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
	"os"
	"os/signal"
	"syscall"

	"echpochmak/internal/config"
	"echpochmak/internal/kv/daemon"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/statemachine"
	"echpochmak/internal/logger"
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

	d, err := start(ctx, cfg, log)
	if err != nil {
		return err
	}

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

func start(ctx context.Context, cfg config.KVNode, log *slog.Logger) (*daemon.Daemon, error) {
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
		},
		Peer:          peer,
		Client:        client,
		JoinEndpoints: cfg.Join,
		Logger:        log,
	})
	if err != nil {
		_ = peer.Close()
		_ = client.Close()

		return nil, err
	}

	return d, nil
}
