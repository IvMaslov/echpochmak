// Package node runs one member of the KV cluster: it drives the raft
// library, persists its log, applies committed entries to the state machine,
// and serves proposals, linearizable reads and membership changes.
//
// The node is transport-agnostic. It sends raft messages through a
// [Transport] and receives them through [Node.Step] and
// [Node.ReceiveSnapshot], so tests can connect nodes in memory.
package node

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/raftlog"
	"echpochmak/internal/kv/statemachine"
)

// dbDir is the Pebble database inside the data directory.
const dbDir = "db"

// keyClusterID stores the cluster ID. It is local to the node: it is not part
// of the replicated state, and snapshots do not overwrite it.
const keyClusterID = "l/cluster_id"

// Defaults for [Config].
const (
	DefaultElectionTicks   = 10
	DefaultHeartbeatTicks  = 1
	DefaultSnapshotCount   = 10_000
	DefaultCatchUpEntries  = 5_000
	DefaultRequestTimeout  = 5 * time.Second
	DefaultPromoteMaxLag   = 1_000
	DefaultMaxRequestBytes = 1536 << 10
)

// Transport sends raft messages to other nodes. Send must not block: a
// transport that cannot deliver a message drops it and reports the peer
// unreachable, and raft retransmits.
type Transport interface {
	Send(msgs []*raftpb.Message)
	AddPeer(id uint64, url string)
	RemovePeer(id uint64)
}

// Config configures a Node.
type Config struct {
	// ID is the node's raft ID. IDs are never reused within a cluster.
	ID uint64
	// DataDir holds the node's Pebble database and snapshot staging files.
	DataDir string
	// FS is the filesystem for DataDir; tests use an in-memory one.
	// Defaults to the operating system's.
	FS        vfs.FS
	Transport Transport

	// InitialCluster bootstraps a new cluster with these voters, which must
	// include this node. Ignored when DataDir already holds state.
	InitialCluster []*kvpb.Member
	// Join starts this node as a new member of an existing cluster: it was
	// added with MemberAdd, and the leader will contact it. Members and
	// ClusterID describe the cluster it joins. Ignored when DataDir already
	// holds state.
	Join      bool
	Members   []*kvpb.Member
	ClusterID string

	// TickInterval is the raft tick. Zero disables the internal ticker, and
	// the caller must call [Node.Tick].
	TickInterval   time.Duration
	ElectionTicks  int
	HeartbeatTicks int

	// SnapshotCount is how many entries are applied between log compactions;
	// CatchUpEntries is how many applied entries compaction keeps for
	// followers that are slightly behind.
	SnapshotCount  uint64
	CatchUpEntries uint64

	RequestTimeout  time.Duration
	PromoteMaxLag   uint64
	MaxRequestBytes int

	StateMachine statemachine.Options
	Logger       *slog.Logger
	// Metrics records events and timings; nil records nothing.
	Metrics *Metrics
	// Tracer traces proposals and reads; nil traces nothing.
	Tracer trace.Tracer
}

func (c *Config) setDefaults() {
	if c.FS == nil {
		c.FS = vfs.Default
	}

	c.ElectionTicks = cmp.Or(c.ElectionTicks, DefaultElectionTicks)
	c.HeartbeatTicks = cmp.Or(c.HeartbeatTicks, DefaultHeartbeatTicks)
	c.SnapshotCount = cmp.Or(c.SnapshotCount, DefaultSnapshotCount)
	c.CatchUpEntries = cmp.Or(c.CatchUpEntries, DefaultCatchUpEntries)
	c.RequestTimeout = cmp.Or(c.RequestTimeout, DefaultRequestTimeout)
	c.PromoteMaxLag = cmp.Or(c.PromoteMaxLag, DefaultPromoteMaxLag)
	c.MaxRequestBytes = cmp.Or(c.MaxRequestBytes, DefaultMaxRequestBytes)

	if c.Logger == nil {
		c.Logger = slog.Default()
	}

	if c.Tracer == nil {
		c.Tracer = noop.NewTracerProvider().Tracer("")
	}
}

// Node is a running member of the cluster.
type Node struct {
	cfg       Config
	log       *slog.Logger
	db        *pebble.DB
	storage   *raftlog.Storage
	sm        *statemachine.StateMachine
	raft      raft.Node
	transport Transport
	clusterID string
	metrics   *Metrics

	leader atomic.Uint64
	term   atomic.Uint64
	state  atomic.Value // raft.StateType

	applied     *progress
	proposals   *waiters[*kvpb.Result]
	reads       *waiters[uint64]
	confChanges *waiters[struct{}]
	readSeq     atomic.Uint64
	confPending atomic.Bool

	lastCompactCheck uint64

	stopc    chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	stopErr  error
}

// Start opens or creates the node's state and starts it.
func Start(cfg Config) (*Node, error) {
	cfg.setDefaults()

	if cfg.ID == 0 {
		return nil, errors.New("node ID must not be zero")
	}

	if err := checkRestoreMarker(cfg.FS, cfg.DataDir); err != nil {
		return nil, err
	}

	if err := createDataDir(cfg.FS, cfg.DataDir); err != nil {
		return nil, err
	}

	db, err := pebble.Open(filepath.Join(cfg.DataDir, dbDir), &pebble.Options{FS: cfg.FS, Logger: pebbleLogger{}})
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	n, err := newNode(cfg, db)
	if err != nil {
		_ = db.Close()

		return nil, err
	}

	return n, nil
}

func newNode(cfg Config, db *pebble.DB) (*Node, error) {
	sm, err := statemachine.Open(db, cfg.StateMachine)
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:         cfg,
		log:         cfg.Logger.With(slog.Uint64("node", cfg.ID)),
		db:          db,
		sm:          sm,
		transport:   cfg.Transport,
		metrics:     cfg.Metrics,
		applied:     newProgress(sm.AppliedIndex()),
		proposals:   newWaiters[*kvpb.Result](),
		reads:       newWaiters[uint64](),
		confChanges: newWaiters[struct{}](),
		stopc:       make(chan struct{}),
		done:        make(chan struct{}),
	}
	n.state.Store(raft.StateFollower)

	storage, err := raftlog.Open(db, n)
	if err != nil {
		return nil, err
	}

	n.storage = storage
	n.lastCompactCheck = sm.AppliedIndex()

	if err := n.startRaft(); err != nil {
		return nil, err
	}

	cleanSnapshotDir(cfg.FS, cfg.DataDir, n.log)

	go n.run()

	if cfg.TickInterval > 0 {
		go n.tickLoop()
	}

	return n, nil
}

// startRaft restarts raft from stored state, or bootstraps or joins a cluster
// when there is none.
func (n *Node) startRaft() error {
	stored, err := n.storedClusterID()
	if err != nil {
		return err
	}

	rc := &raft.Config{
		ID:                        n.cfg.ID,
		ElectionTick:              n.cfg.ElectionTicks,
		HeartbeatTick:             n.cfg.HeartbeatTicks,
		Storage:                   n.storage,
		Applied:                   n.sm.AppliedIndex(),
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 64 << 20,
		CheckQuorum:               true,
		PreVote:                   true,
		ReadOnlyOption:            raft.ReadOnlySafe,
		StepDownOnRemoval:         true,
		Logger:                    raftLogger{log: n.log},
	}

	// Bootstrapping and joining store the cluster ID before raft persists
	// anything, so a node that crashed in between has an ID but an empty log:
	// it bootstraps or joins again.
	switch {
	case !n.storage.IsEmpty():
		return n.restart(rc, stored)
	case len(n.cfg.InitialCluster) > 0:
		return n.bootstrap(rc, stored)
	case n.cfg.Join || stored != "":
		return n.join(rc, stored)
	default:
		return errors.New("empty data directory: give an initial cluster to bootstrap, or join an existing one")
	}
}

func (n *Node) restart(rc *raft.Config, clusterID string) error {
	if n.cfg.ClusterID != "" && n.cfg.ClusterID != clusterID {
		return fmt.Errorf("data directory belongs to cluster %s, not %s", clusterID, n.cfg.ClusterID)
	}

	n.clusterID = clusterID
	n.raft = raft.RestartNode(rc)

	// Configured members cover a joined node that has not applied its
	// membership yet; applied members are authoritative once known.
	for _, m := range n.cfg.Members {
		n.addPeer(m)
	}

	for _, m := range n.sm.Applied().GetMembers() {
		n.addPeer(m)
	}

	n.log.Info("node restarted",
		slog.String("cluster_id", clusterID),
		slog.Uint64("applied", n.sm.AppliedIndex()),
	)

	return nil
}

func (n *Node) bootstrap(rc *raft.Config, stored string) error {
	members := n.cfg.InitialCluster
	if !slices.ContainsFunc(members, func(m *kvpb.Member) bool { return m.GetId() == n.cfg.ID }) {
		return fmt.Errorf("the initial cluster does not include this node (%d)", n.cfg.ID)
	}

	n.clusterID = clusterIDFor(members)
	if stored != "" && stored != n.clusterID {
		return fmt.Errorf("data directory belongs to cluster %s, but the initial cluster is %s", stored, n.clusterID)
	}

	if err := n.storeClusterID(); err != nil {
		return err
	}

	peers := make([]raft.Peer, 0, len(members))

	for _, m := range members {
		raw, err := proto.Marshal(&kvpb.ConfChangeContext{Member: m})
		if err != nil {
			return fmt.Errorf("encoding member: %w", err)
		}

		peers = append(peers, raft.Peer{ID: m.GetId(), Context: raw})
		n.addPeer(m)
	}

	n.raft = raft.StartNode(rc, peers)

	n.log.Info("cluster bootstrapped",
		slog.String("cluster_id", n.clusterID),
		slog.Int("members", len(members)),
	)

	return nil
}

func (n *Node) join(rc *raft.Config, stored string) error {
	n.clusterID = cmp.Or(n.cfg.ClusterID, stored)

	switch {
	case n.clusterID == "":
		return errors.New("joining needs the cluster ID")
	case stored != "" && stored != n.clusterID:
		return fmt.Errorf("data directory belongs to cluster %s, not %s", stored, n.clusterID)
	}

	if err := n.storeClusterID(); err != nil {
		return err
	}

	for _, m := range n.cfg.Members {
		n.addPeer(m)
	}

	// With empty storage, raft waits for the leader to send the log or a
	// snapshot, which makes this node a member.
	n.raft = raft.RestartNode(rc)

	n.log.Info("joining cluster", slog.String("cluster_id", n.clusterID))

	return nil
}

// HasState reports whether dataDir already holds a node's database. A node
// with state restarts from it and ignores bootstrap and join settings.
func HasState(fs vfs.FS, dataDir string) (bool, error) {
	if fs == nil {
		fs = vfs.Default
	}

	names, err := fs.List(filepath.Join(dataDir, dbDir))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("reading data directory: %w", err)
	}

	return len(names) > 0, nil
}

// createDataDir creates the data directory and its subdirectories, and syncs
// the directories that contain them. Without the syncs, a crash soon after
// the first start can lose the new directory entries, and with them the whole
// database, even though every write inside was synced.
func createDataDir(fs vfs.FS, dataDir string) error {
	for _, sub := range []string{dbDir, snapshotDir} {
		if err := fs.MkdirAll(filepath.Join(dataDir, sub), 0o755); err != nil {
			return fmt.Errorf("creating data directory: %w", err)
		}
	}

	for _, dir := range []string{dataDir, filepath.Dir(filepath.Clean(dataDir))} {
		if err := syncDir(fs, dir); err != nil {
			return err
		}
	}

	return nil
}

func syncDir(fs vfs.FS, dir string) error {
	d, err := fs.OpenDir(dir)
	if err != nil {
		return fmt.Errorf("opening %s: %w", dir, err)
	}

	if err := d.Sync(); err != nil {
		_ = d.Close()

		return fmt.Errorf("syncing %s: %w", dir, err)
	}

	if err := d.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", dir, err)
	}

	return nil
}

// clusterIDFor derives the cluster ID from the initial members, so that every
// node bootstrapping the same cluster computes the same ID.
func clusterIDFor(members []*kvpb.Member) string {
	parts := make([]string, 0, len(members))
	for _, m := range members {
		parts = append(parts, strconv.FormatUint(m.GetId(), 10)+"="+m.GetPeerUrl())
	}

	slices.Sort(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, ",")))

	return hex.EncodeToString(sum[:8])
}

func (n *Node) storedClusterID() (string, error) {
	raw, closer, err := n.db.Get([]byte(keyClusterID))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", nil
	}

	if err != nil {
		return "", fmt.Errorf("reading cluster ID: %w", err)
	}
	defer closer.Close()

	return string(raw), nil
}

func (n *Node) storeClusterID() error {
	if err := n.db.Set([]byte(keyClusterID), []byte(n.clusterID), pebble.Sync); err != nil {
		return fmt.Errorf("storing cluster ID: %w", err)
	}

	return nil
}

func (n *Node) addPeer(m *kvpb.Member) {
	if m.GetId() != n.cfg.ID {
		n.transport.AddPeer(m.GetId(), m.GetPeerUrl())
	}
}

// ID returns the node's raft ID.
func (n *Node) ID() uint64 {
	return n.cfg.ID
}

// ClusterID returns the ID of the cluster the node belongs to.
func (n *Node) ClusterID() string {
	return n.clusterID
}

// Leader returns the current leader, or 0 if none is known.
func (n *Node) Leader() uint64 {
	return n.leader.Load()
}

// Term returns the node's current raft term.
func (n *Node) Term() uint64 {
	return n.term.Load()
}

// IsLeader reports whether this node is the leader.
func (n *Node) IsLeader() bool {
	return n.Leader() == n.cfg.ID
}

// Members returns the members of the cluster as of the applied state.
func (n *Node) Members() []*kvpb.Member {
	return n.sm.Applied().GetMembers()
}

// Limits returns the request limits of the state machine.
func (n *Node) Limits() statemachine.Limits {
	return n.sm.Limits()
}

// Done is closed when the node has stopped, including after its own removal
// from the cluster.
func (n *Node) Done() <-chan struct{} {
	return n.done
}

// Tick advances the raft clock by one tick. Use it only when the node was
// configured without a tick interval.
func (n *Node) Tick() {
	select {
	case <-n.stopc:
	default:
		n.raft.Tick()
	}
}

func (n *Node) tickLoop() {
	t := time.NewTicker(n.cfg.TickInterval)
	defer t.Stop()

	for {
		select {
		case <-n.stopc:
			return
		case <-t.C:
			n.raft.Tick()
		}
	}
}

// Shutdown stops the node gracefully: a leader first hands leadership to a
// follower, so the cluster does not wait for an election timeout.
func (n *Node) Shutdown(ctx context.Context) error {
	if n.IsLeader() && len(n.voters()) > 1 {
		if err := n.TransferLeadership(ctx, 0); err != nil {
			n.log.WarnContext(ctx, "leadership transfer before shutdown failed", slog.Any("error", err))
		}
	}

	return n.Stop()
}

// Stop stops the node immediately and closes its database. Requests in
// flight fail with [ErrUnavailable] or [ErrOutcomeUnknown].
func (n *Node) Stop() error {
	n.stopOnce.Do(func() {
		close(n.stopc)
		n.raft.Stop()
		<-n.done

		if err := n.db.Close(); err != nil {
			n.stopErr = fmt.Errorf("closing database: %w", err)
		}

		n.log.Info("node stopped")
	})

	return n.stopErr
}

// voters returns the voting members as of the applied state.
func (n *Node) voters() []uint64 {
	ids := []uint64{}

	for _, m := range n.Members() {
		if !m.GetLearner() {
			ids = append(ids, m.GetId())
		}
	}

	return ids
}

// pebbleLogger keeps Pebble's informational output out of the node's logs.
type pebbleLogger struct{}

func (pebbleLogger) Infof(string, ...any) {}

func (pebbleLogger) Errorf(format string, args ...any) {
	slog.Error(fmt.Sprintf("pebble: "+format, args...))
}

func (pebbleLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}
