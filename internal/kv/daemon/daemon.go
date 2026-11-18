// Package daemon wires a KV node to the network: it starts the node with a
// gRPC transport, serves the peer service on one listener and the client
// API on another, and shuts everything down in order. The kv-node command
// and the full-stack tests share it.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/server"
	"echpochmak/internal/kv/transport"
)

const (
	// maxMessageBytes bounds gRPC messages; raft messages are at most about
	// 1 MiB, and requests 1.5 MiB.
	maxMessageBytes = 16 << 20
	joinTimeout     = 30 * time.Second
)

// Config configures a Daemon.
type Config struct {
	// Node configures the node. Its Transport is set by the daemon.
	Node node.Config
	// Peer and Client are the listeners for the peer service and the client
	// API.
	Peer   net.Listener
	Client net.Listener
	// JoinEndpoints are client endpoints of an existing cluster. When set
	// and the data directory is empty, the node joins that cluster: it reads
	// the cluster ID and members from it. The node must have been added with
	// MemberAdd first.
	JoinEndpoints []string
	Server        server.Options
	Logger        *slog.Logger
	// Registry, if set, receives the node's, the transport's and the client
	// API's metrics.
	Registry *prometheus.Registry
	// ClientServerOptions are extra options of the client API's gRPC server,
	// such as a tracing stats handler.
	ClientServerOptions []grpc.ServerOption
}

// Daemon is a running node with its servers.
type Daemon struct {
	node      *node.Node
	transport *transport.Transport
	peer      *grpc.Server
	client    *grpc.Server
	cancel    context.CancelFunc
	serveErr  chan error
	log       *slog.Logger
}

// Start starts the node and its servers.
func Start(ctx context.Context, cfg Config) (*Daemon, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if err := resolveJoin(ctx, &cfg); err != nil {
		return nil, err
	}

	var grpcMetrics *server.GRPCMetrics

	trCfg := transport.Config{Logger: cfg.Logger}

	if cfg.Registry != nil {
		cfg.Node.Metrics = node.NewMetrics(cfg.Registry)
		trCfg.Metrics = transport.NewMetrics(cfg.Registry)
		grpcMetrics = server.NewGRPCMetrics(cfg.Registry)
	}

	tr := transport.New(trCfg)
	cfg.Node.Transport = tr
	cfg.Node.Logger = cfg.Logger

	n, err := node.Start(cfg.Node)
	if err != nil {
		tr.Close()

		return nil, fmt.Errorf("starting node: %w", err)
	}

	tr.SetHandler(n)

	if cfg.Registry != nil {
		cfg.Registry.MustRegister(node.NewCollector(n, cfg.Server.Zone), tr.Collector())
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	d := &Daemon{
		node:      n,
		transport: tr,
		peer:      grpc.NewServer(opts...),
		client:    grpc.NewServer(clientServerOptions(opts, cfg, grpcMetrics)...),
		cancel:    cancel,
		serveErr:  make(chan error, 2),
		log:       cfg.Logger,
	}

	tr.Register(d.peer)
	server.Register(sctx, d.client, n, cfg.Server)

	go func() { d.serveErr <- d.peer.Serve(cfg.Peer) }()
	go func() { d.serveErr <- d.client.Serve(cfg.Client) }()

	d.log.InfoContext(ctx, "kv node serving",
		slog.Uint64("node", n.ID()),
		slog.String("cluster_id", n.ClusterID()),
		slog.String("peer_addr", cfg.Peer.Addr().String()),
		slog.String("client_addr", cfg.Client.Addr().String()),
	)

	return d, nil
}

// clientServerOptions returns the options of the client API's server: the
// common ones, the metrics interceptors and the configured extras.
func clientServerOptions(common []grpc.ServerOption, cfg Config, m *server.GRPCMetrics) []grpc.ServerOption {
	opts := slices.Clone(common)

	if m != nil {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(m.UnaryInterceptor()),
			grpc.ChainStreamInterceptor(m.StreamInterceptor()),
		)
	}

	return append(opts, cfg.ClientServerOptions...)
}

// resolveJoin fills in the cluster ID and members of the cluster to join.
func resolveJoin(ctx context.Context, cfg *Config) error {
	if len(cfg.JoinEndpoints) == 0 {
		return nil
	}

	hasState, err := node.HasState(cfg.Node.FS, cfg.Node.DataDir)
	if err != nil || hasState {
		// A node with state restarts from it and does not need the cluster.
		return err
	}

	c, err := client.New(client.Config{Endpoints: cfg.JoinEndpoints, Logger: cfg.Logger})
	if err != nil {
		return fmt.Errorf("connecting to the cluster to join: %w", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(ctx, joinTimeout)
	defer cancel()

	resp, err := c.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("reading the members of the cluster to join: %w", err)
	}

	isMember := false

	for _, m := range resp.GetMembers() {
		isMember = isMember || m.GetId() == cfg.Node.ID
	}

	if !isMember {
		return fmt.Errorf("node %d is not a member of cluster %s; add it with kvctl member add first",
			cfg.Node.ID, resp.GetClusterId())
	}

	cfg.Node.Join = true
	cfg.Node.ClusterID = resp.GetClusterId()
	cfg.Node.Members = resp.GetMembers()

	return nil
}

// Node returns the running node.
func (d *Daemon) Node() *node.Node {
	return d.node
}

// Done is closed when the node stops, including after its removal.
func (d *Daemon) Done() <-chan struct{} {
	return d.node.Done()
}

// Shutdown stops gracefully: a leader hands over leadership, then the
// servers drain and the node stops.
func (d *Daemon) Shutdown(ctx context.Context) error {
	nodeErr := d.node.Shutdown(ctx)

	d.cancel()
	stopServer(ctx, d.client)
	// Peer streams are long-lived and never finish on their own, so the peer
	// server stops at once.
	d.peer.Stop()
	d.transport.Close()

	var errs []error

	for range 2 {
		if err := <-d.serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errs = append(errs, err)
		}
	}

	return errors.Join(append(errs, nodeErr)...)
}

// stopServer drains a server, giving up on stragglers when ctx ends.
func stopServer(ctx context.Context, s *grpc.Server) {
	done := make(chan struct{})

	go func() {
		s.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		s.Stop()
	}
}
