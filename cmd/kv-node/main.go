// Command kv-node runs one node of the raft-replicated key-value cluster that
// stores the catalog state.
//
// Bootstrap a new cluster by starting every node with the same
// -initial-cluster. Add a node to a running cluster with "kvctl member add",
// then start it with -join. A node whose data directory has state restarts
// from it. Run with -help to list the flags; each flag can also be set
// through an ECHPOCHMAK_KV_* environment variable.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"echpochmak/internal/config"
	"echpochmak/internal/kv/daemon"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/server"
	"echpochmak/internal/kv/statemachine"
	"echpochmak/internal/logger"
	"echpochmak/internal/metrics"
	"echpochmak/internal/telemetry"
	"echpochmak/internal/version"
)

const serviceName = "kv-node"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadKVNode(os.Args[1:], os.Getenv)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintf(os.Stdout, "Usage of %s:\n%s", serviceName, config.KVNodeUsage())

		return nil
	}

	if err != nil {
		return err
	}

	log := logger.New(os.Stdout, cfg.Log, serviceName)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.InfoContext(ctx, "starting",
		slog.Any(logger.KeyBuild, version.Get()),
		slog.Any(logger.KeyConfig, cfg),
	)

	reg := metrics.NewRegistry(serviceName)

	tracing, err := telemetry.Setup(ctx, serviceName, os.Getenv)
	if err != nil {
		return err
	}

	defer func() {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()

		_ = tracing.Shutdown(flushCtx)
	}()

	d, err := start(ctx, cfg, reg, tracing, log)
	if err != nil {
		return err
	}

	stopHTTP, err := serveHTTP(ctx, cfg, d, reg, log)
	if err != nil {
		_ = d.Shutdown(context.WithoutCancel(ctx))

		return err
	}
	defer stopHTTP()

	select {
	case <-ctx.Done():
		log.InfoContext(ctx, "shutting down")
	case <-d.Done():
		log.InfoContext(ctx, "node stopped on its own; shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()

	if err := d.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutting down: %w", err)
	}

	return nil
}

func start(
	ctx context.Context,
	cfg config.KVNode,
	reg *prometheus.Registry,
	tracing telemetry.Tracing,
	log *slog.Logger,
) (*daemon.Daemon, error) {
	var initial []*kvpb.Member

	if cfg.InitialCluster != "" {
		var err error

		initial, err = config.ParseInitialCluster(cfg.InitialCluster)
		if err != nil {
			return nil, err
		}
	}

	var lc net.ListenConfig

	peer, err := lc.Listen(ctx, "tcp", cfg.ListenPeer)
	if err != nil {
		return nil, fmt.Errorf("listening for peers: %w", err)
	}

	client, err := lc.Listen(ctx, "tcp", cfg.ListenClient)
	if err != nil {
		_ = peer.Close()

		return nil, fmt.Errorf("listening for clients: %w", err)
	}

	d, err := daemon.Start(ctx, daemon.Config{
		Node: node.Config{
			ID:              cfg.ID,
			DataDir:         cfg.DataDir,
			InitialCluster:  initial,
			TickInterval:    cfg.TickInterval,
			ElectionTicks:   cfg.ElectionTicks,
			HeartbeatTicks:  cfg.HeartbeatTicks,
			SnapshotCount:   cfg.SnapshotCount,
			CatchUpEntries:  cfg.CatchUpEntries,
			RequestTimeout:  cfg.RequestTimeout,
			PromoteMaxLag:   cfg.PromoteMaxLag,
			MaxRequestBytes: cfg.MaxRequestBytes,
			StateMachine:    statemachine.Options{DedupWindow: cfg.DedupWindow},
			Tracer:          tracing.Tracer(),
		},
		Peer:          peer,
		Client:        client,
		JoinEndpoints: cfg.Join,
		Server:        server.Options{ReadyMaxLag: cfg.ReadyMaxLag, Zone: cfg.Zone},
		Logger:        log,
		Registry:      reg,
		ClientServerOptions: []grpc.ServerOption{
			grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tracing.Provider))),
		},
	})
	if err != nil {
		_ = peer.Close()
		_ = client.Close()

		return nil, err
	}

	return d, nil
}

// serveHTTP serves /metrics, /healthz and /readyz on the metrics listener,
// unless it is turned off, and returns a function that stops it.
//
// /healthz reports that the process runs; /readyz that the node knows a
// leader and has applied nearly everything committed.
func serveHTTP(
	ctx context.Context,
	cfg config.KVNode,
	d *daemon.Daemon,
	reg *prometheus.Registry,
	log *slog.Logger,
) (func(), error) {
	if cfg.ListenMetrics == "" {
		return func() {}, nil
	}

	maxLag := cfg.ReadyMaxLag
	if maxLag == 0 {
		maxLag = server.DefaultReadyMaxLag
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler(reg))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !server.Ready(d.Node(), maxLag) {
			http.Error(w, "no leader, or applying far behind the commit index", http.StatusServiceUnavailable)

			return
		}

		_, _ = w.Write([]byte("ok\n"))
	})

	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenMetrics)
	if err != nil {
		return nil, fmt.Errorf("listening for metrics: %w", err)
	}

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.ErrorContext(ctx, "metrics server failed", logger.Err(err))
		}
	}()

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutdownCtx)
	}, nil
}
