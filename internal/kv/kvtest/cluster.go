package kvtest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
)

// Options configures a test cluster.
type Options struct {
	// Seed makes network faults reproducible.
	Seed uint64
	// Configure adjusts each node's configuration before it starts.
	Configure func(*node.Config)
	// DiskDir puts the nodes on the real filesystem, under this directory,
	// so fsync costs what it costs; benchmarks need that. Such a cluster
	// cannot Crash, which needs the in-memory filesystem.
	DiskDir string
}

// Cluster is a set of nodes in one process.
type Cluster struct {
	t    testing.TB
	opts Options
	net  *Network

	mu        sync.Mutex
	nodes     map[uint64]*member
	clusterID string
}

type member struct {
	node *node.Node
	fs   *vfs.MemFS
	join bool
}

const dataDir = "/data"

// NewCluster bootstraps a cluster with voters 1..size and waits for a
// leader. The cluster is stopped when the test ends.
func NewCluster(tb testing.TB, size int, opts Options) *Cluster {
	tb.Helper()

	c := &Cluster{t: tb, opts: opts, net: NewNetwork(opts.Seed), nodes: map[uint64]*member{}}

	initial := make([]*kvpb.Member, 0, size)
	for i := range size {
		id := uint64(i + 1)
		initial = append(initial, &kvpb.Member{Id: id, PeerUrl: fmt.Sprintf("mem://%d", id)})
	}

	for _, m := range initial {
		c.start(m.GetId(), vfs.NewCrashableMem(), func(cfg *node.Config) {
			cfg.InitialCluster = initial
		})
	}

	tb.Cleanup(c.Close)

	c.clusterID = c.Node(1).ClusterID()
	c.WaitLeader()

	return c
}

// Network returns the cluster's network, for fault injection.
func (c *Cluster) Network() *Network {
	return c.net
}

// IDs returns the IDs of the running nodes.
func (c *Cluster) IDs() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	ids := make([]uint64, 0, len(c.nodes))

	for id, m := range c.nodes {
		if m.node != nil {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)

	return ids
}

// Node returns a running node.
func (c *Cluster) Node(id uint64) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()

	m, ok := c.nodes[id]
	if !ok || m.node == nil {
		c.t.Fatalf("node %d is not running", id)
	}

	return m.node
}

func (c *Cluster) start(id uint64, fs *vfs.MemFS, configure func(*node.Config)) {
	c.t.Helper()

	cfg := node.Config{
		ID:             id,
		DataDir:        dataDir,
		FS:             fs,
		Transport:      c.net.Transport(id),
		TickInterval:   10 * time.Millisecond,
		RequestTimeout: 3 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
	}

	if configure != nil {
		configure(&cfg)
	}

	if c.opts.Configure != nil {
		c.opts.Configure(&cfg)
	}

	if c.opts.DiskDir != "" {
		cfg.FS = vfs.Default
		cfg.DataDir = filepath.Join(c.opts.DiskDir, strconv.FormatUint(id, 10))
	}

	n, err := node.Start(cfg)
	if err != nil {
		c.t.Fatalf("starting node %d: %v", id, err)
	}

	c.mu.Lock()
	c.nodes[id] = &member{node: n, fs: fs, join: cfg.Join}
	c.mu.Unlock()

	c.net.Attach(id, n)
}

// Crash stops a node abruptly, losing every write it had not synced, and
// starts it again from what a real disk would hold after the crash.
func (c *Cluster) Crash(id uint64) {
	c.t.Helper()

	if c.opts.DiskDir != "" {
		c.t.Fatal("Crash needs the in-memory filesystem; this cluster is on disk")
	}

	c.mu.Lock()
	m := c.nodes[id]
	c.mu.Unlock()

	// Cut the node off first: a real crash stops everything at once, so the
	// instance must not acknowledge anything after the disk image is taken.
	c.net.Detach(id)

	crashed := m.fs.CrashClone(vfs.CrashCloneCfg{})
	c.stop(id)
	c.start(id, crashed, c.joinConfig(m.join))
}

// Restart stops a node cleanly and starts it again on the same disk.
func (c *Cluster) Restart(id uint64) {
	c.t.Helper()

	c.mu.Lock()
	m := c.nodes[id]
	c.mu.Unlock()

	c.stop(id)
	c.start(id, m.fs, c.joinConfig(m.join))
}

// Kill stops a node for good.
func (c *Cluster) Kill(id uint64) {
	c.t.Helper()
	c.stop(id)
}

func (c *Cluster) stop(id uint64) {
	c.mu.Lock()
	m := c.nodes[id]
	c.mu.Unlock()

	if m == nil || m.node == nil {
		return
	}

	c.net.Detach(id)
	_ = m.node.Stop()

	c.mu.Lock()
	m.node = nil
	c.mu.Unlock()
}

func (c *Cluster) joinConfig(join bool) func(*node.Config) {
	if !join {
		return nil
	}

	return func(cfg *node.Config) {
		cfg.Join = true
		cfg.ClusterID = c.clusterID
	}
}

// StartJoining starts a node that joins the cluster; add it with
// [node.Node.AddMember] on the leader first.
func (c *Cluster) StartJoining(id uint64) *node.Node {
	c.t.Helper()

	members := c.Node(c.WaitLeader()).Members()

	c.start(id, vfs.NewCrashableMem(), func(cfg *node.Config) {
		cfg.Join = true
		cfg.ClusterID = c.clusterID
		cfg.Members = members
	})

	return c.Node(id)
}

// Leader returns the leader some running node knows of, or 0. Unlike
// [Cluster.WaitLeader] it never fails the test, so it is safe in goroutines.
func (c *Cluster) Leader() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, m := range c.nodes {
		if m.node == nil {
			continue
		}

		if l := m.node.Leader(); l != 0 {
			if lm := c.nodes[l]; lm != nil && lm.node != nil {
				return l
			}
		}
	}

	return 0
}

// TryNode returns a running node, or nil. It never fails the test.
func (c *Cluster) TryNode(id uint64) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()

	if m := c.nodes[id]; m != nil {
		return m.node
	}

	return nil
}

// WaitLeader waits until the running nodes agree on a leader and returns it.
func (c *Cluster) WaitLeader() uint64 {
	c.t.Helper()

	var leader uint64

	c.Eventually(10*time.Second, func() error {
		leader = 0

		for _, id := range c.IDs() {
			l := c.Node(id).Leader()

			switch {
			case l == 0:
				return fmt.Errorf("node %d knows no leader", id)
			case leader != 0 && l != leader:
				return fmt.Errorf("nodes disagree on the leader: %d and %d", leader, l)
			}

			leader = l
		}

		if !slices.Contains(c.IDs(), leader) {
			return fmt.Errorf("leader %d is not running", leader)
		}

		return nil
	})

	return leader
}

// Eventually retries fn until it succeeds or timeout passes.
func (c *Cluster) Eventually(timeout time.Duration, fn func() error) {
	c.t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		err := fn()
		if err == nil {
			return
		}

		if time.Now().After(deadline) {
			c.t.Fatalf("condition not met after %s: %v", timeout, err)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// Close stops every node and the network.
func (c *Cluster) Close() {
	for _, id := range c.IDs() {
		c.stop(id)
	}

	c.net.Close()
}

// Put writes a key through any node, retrying while the cluster has no
// leader.
func (c *Cluster) Put(ctx context.Context, via uint64, key, value string) (*kvpb.PutResponse, error) {
	for {
		res, err := c.Node(via).Propose(ctx, &kvpb.Proposal{
			Operation: &kvpb.Proposal_Put{Put: &kvpb.PutRequest{Key: []byte(key), Value: []byte(value)}},
		})

		switch {
		case err == nil:
			return res.GetPut(), nil
		case errors.Is(err, node.ErrUnavailable) && ctx.Err() == nil:
			time.Sleep(20 * time.Millisecond)
		default:
			return nil, err
		}
	}
}
