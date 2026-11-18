package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/server"
	"echpochmak/internal/kv/statemachine"
	"echpochmak/internal/logger"
)

// KVEnvPrefix prefixes the environment variable of every kv-node flag.
const KVEnvPrefix = "ECHPOCHMAK_KV_"

// KVNode is the kv-node configuration.
type KVNode struct {
	ID      uint64
	DataDir string

	// ListenPeer and ListenClient are the addresses of the peer service and
	// the client API. The address other nodes use to reach the peer service
	// is the node's peer URL in the initial cluster, or the one given to
	// "kvctl member add".
	ListenPeer   string
	ListenClient string
	// ListenMetrics is the address of the HTTP listener for /metrics,
	// /healthz and /readyz; empty turns it off.
	ListenMetrics string
	// Zone labels the node's placement, such as an availability zone. It is
	// reported, not replicated.
	Zone string
	// ReadyMaxLag is how many entries the applied index may trail the
	// commit index for the node to report itself ready.
	ReadyMaxLag uint64

	// InitialCluster bootstraps a new cluster: "id=peer-url,...".
	InitialCluster string
	// Join lists client endpoints of an existing cluster to join.
	Join []string

	TickInterval    time.Duration
	ElectionTicks   int
	HeartbeatTicks  int
	SnapshotCount   uint64
	CatchUpEntries  uint64
	DedupWindow     int64
	RequestTimeout  time.Duration
	MaxRequestBytes int
	PromoteMaxLag   uint64
	ShutdownTimeout time.Duration

	Log logger.Config
}

// LoadKVNode parses kv-node flags (without the program name) and falls back
// to environment variables for flags not given.
func LoadKVNode(args []string, getenv func(string) string) (KVNode, error) {
	var cfg KVNode

	fs := flag.NewFlagSet("kv-node", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	registerKVNode(fs, &cfg)

	if err := fs.Parse(args); err != nil {
		return KVNode{}, fmt.Errorf("parsing flags: %w", err)
	}

	if err := applyEnv(fs, KVEnvPrefix, getenv); err != nil {
		return KVNode{}, err
	}

	if err := cfg.Validate(); err != nil {
		return KVNode{}, err
	}

	return cfg, nil
}

// KVNodeUsage returns the help text of all kv-node flags.
func KVNodeUsage() string {
	var cfg KVNode

	return usage("kv-node", func(fs *flag.FlagSet) { registerKVNode(fs, &cfg) })
}

// KVEnvName returns the environment variable that backs the kv-node flag
// named flagName.
func KVEnvName(flagName string) string {
	return envName(KVEnvPrefix, flagName)
}

func registerKVNode(fs *flag.FlagSet, cfg *KVNode) {
	fs.Uint64Var(&cfg.ID, "id", 0, "node ID: a positive number, never reused in the cluster")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "directory of the node's database")
	fs.StringVar(&cfg.ListenPeer, "listen-peer", ":2380", "listen address of the peer service")
	fs.StringVar(&cfg.ListenClient, "listen-client", ":2379", "listen address of the client API")
	fs.StringVar(&cfg.ListenMetrics, "listen-metrics", ":2381",
		"listen address of /metrics, /healthz and /readyz; empty turns it off")
	fs.StringVar(&cfg.Zone, "zone", "", "placement label of the node, such as its availability zone")
	fs.Uint64Var(&cfg.ReadyMaxLag, "ready-max-lag", server.DefaultReadyMaxLag,
		"entries the applied index may trail the commit index for the node to be ready")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "",
		"bootstrap a new cluster with these voters: id=peer-url,... (ignored when data-dir has state)")
	fs.Func("join", "join the cluster at these client endpoints, comma-separated (ignored when data-dir has state)",
		func(v string) error {
			cfg.Join = splitList(v)

			return nil
		})

	fs.DurationVar(&cfg.TickInterval, "tick-interval", 100*time.Millisecond, "raft tick")
	fs.IntVar(&cfg.ElectionTicks, "election-ticks", node.DefaultElectionTicks,
		"ticks without a leader before an election")
	fs.IntVar(&cfg.HeartbeatTicks, "heartbeat-ticks", node.DefaultHeartbeatTicks, "ticks between leader heartbeats")
	fs.Uint64Var(&cfg.SnapshotCount, "snapshot-count", node.DefaultSnapshotCount,
		"entries applied between log compactions")
	fs.Uint64Var(&cfg.CatchUpEntries, "catch-up-entries", node.DefaultCatchUpEntries,
		"applied entries compaction keeps for slow followers")
	fs.Int64Var(&cfg.DedupWindow, "dedup-window", statemachine.DefaultDedupWindow,
		"revisions a write's result is kept for retries")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", node.DefaultRequestTimeout, "proposal and read timeout")
	fs.IntVar(&cfg.MaxRequestBytes, "max-request-bytes", node.DefaultMaxRequestBytes, "maximum size of a write")
	fs.Uint64Var(&cfg.PromoteMaxLag, "promote-max-lag", node.DefaultPromoteMaxLag,
		"entries a learner may trail the leader by to be promoted")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "graceful shutdown timeout")

	fs.TextVar(&cfg.Log.Level, "log-level", slog.LevelInfo, "log level: debug, info, warn, error")
	fs.TextVar(&cfg.Log.Format, "log-format", logger.FormatJSON, "log format: json, text")
	fs.BoolVar(&cfg.Log.AddSource, "log-add-source", false, "add source file and line to log records")
}

// Validate reports every invalid setting.
func (c KVNode) Validate() error {
	var errs []error

	if c.ID == 0 {
		errs = append(errs, errors.New("id is required and must be positive"))
	}

	if c.DataDir == "" {
		errs = append(errs, errors.New("data-dir is required"))
	}

	if c.InitialCluster != "" && len(c.Join) > 0 {
		errs = append(errs, errors.New("initial-cluster and join are mutually exclusive"))
	}

	if c.InitialCluster != "" {
		if _, err := ParseInitialCluster(c.InitialCluster); err != nil {
			errs = append(errs, err)
		}
	}

	positive := map[string]bool{
		"tick-interval":     c.TickInterval > 0,
		"election-ticks":    c.ElectionTicks > 0,
		"heartbeat-ticks":   c.HeartbeatTicks > 0,
		"snapshot-count":    c.SnapshotCount > 0,
		"dedup-window":      c.DedupWindow > 0,
		"request-timeout":   c.RequestTimeout > 0,
		"max-request-bytes": c.MaxRequestBytes > 0,
		"shutdown-timeout":  c.ShutdownTimeout > 0,
	}
	for name, ok := range positive {
		if !ok {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}

	if c.ElectionTicks <= c.HeartbeatTicks {
		errs = append(errs, errors.New("election-ticks must be greater than heartbeat-ticks"))
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	return nil
}

// ParseInitialCluster parses "id=peer-url,..." into members.
func ParseInitialCluster(s string) ([]*kvpb.Member, error) {
	members := []*kvpb.Member{}
	seen := map[uint64]bool{}

	for _, part := range splitList(s) {
		idStr, url, ok := strings.Cut(part, "=")
		if !ok || url == "" {
			return nil, fmt.Errorf("initial-cluster entry %q must be id=peer-url", part)
		}

		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("initial-cluster entry %q has an invalid ID", part)
		}

		if seen[id] {
			return nil, fmt.Errorf("initial-cluster lists node %d twice", id)
		}

		seen[id] = true
		members = append(members, &kvpb.Member{Id: id, PeerUrl: url})
	}

	return members, nil
}

func splitList(s string) []string {
	out := []string{}

	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}

	return out
}

// LogValue implements [slog.LogValuer].
func (c KVNode) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Uint64("id", c.ID),
		slog.String("data_dir", c.DataDir),
		slog.String("listen_peer", c.ListenPeer),
		slog.String("listen_client", c.ListenClient),
		slog.String("listen_metrics", c.ListenMetrics),
		slog.String("zone", c.Zone),
		slog.String("initial_cluster", c.InitialCluster),
		slog.Any("join", c.Join),
		slog.Duration("tick_interval", c.TickInterval),
		slog.Int("election_ticks", c.ElectionTicks),
		slog.Uint64("snapshot_count", c.SnapshotCount),
		slog.Duration("request_timeout", c.RequestTimeout),
	)
}
