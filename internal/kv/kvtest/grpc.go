package kvtest

import (
	"context"
	"log/slog"
	"maps"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/daemon"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
)

// GRPCCluster runs full kv-node daemons in one process, on localhost ports,
// connected over real gRPC. Each node keeps its data on an in-memory
// filesystem that survives Stop and Restart. The cluster is stopped when the
// test ends.
type GRPCCluster struct {
	tb      testing.TB
	members []*kvpb.Member

	mu    sync.Mutex
	nodes map[uint64]*grpcNode
}

type grpcNode struct {
	peerAddr   string
	clientAddr string
	fs         vfs.FS
	daemon     *daemon.Daemon
	// Listeners reserved for a node that has not started yet.
	peer, client net.Listener
}

// NewGRPCCluster bootstraps a cluster of voters 1..size.
func NewGRPCCluster(tb testing.TB, size int) *GRPCCluster {
	tb.Helper()

	c := &GRPCCluster{tb: tb, nodes: map[uint64]*grpcNode{}}

	for i := range size {
		id := uint64(i + 1)
		n := c.reserve(id)
		c.members = append(c.members, &kvpb.Member{Id: id, PeerUrl: "http://" + n.peerAddr})
	}

	for _, id := range slices.Sorted(maps.Keys(c.nodes)) {
		c.start(id, nil)
	}

	tb.Cleanup(func() {
		for _, id := range c.running() {
			c.Stop(id)
		}
	})

	return c
}

func (c *GRPCCluster) listen(addr string) net.Listener {
	c.tb.Helper()

	l, err := (&net.ListenConfig{}).Listen(c.tb.Context(), "tcp", addr)
	if err != nil {
		c.tb.Fatalf("listening on %s: %v", addr, err)
	}

	return l
}

// reserve opens the listeners of a new node.
func (c *GRPCCluster) reserve(id uint64) *grpcNode {
	n := &grpcNode{
		peer:   c.listen("127.0.0.1:0"),
		client: c.listen("127.0.0.1:0"),
		fs:     vfs.NewMem(),
	}
	n.peerAddr, n.clientAddr = n.peer.Addr().String(), n.client.Addr().String()

	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()

	return n
}

func (c *GRPCCluster) start(id uint64, join []string) {
	c.tb.Helper()

	c.mu.Lock()
	n := c.nodes[id]
	c.mu.Unlock()

	if n.peer == nil {
		n.peer, n.client = c.listen(n.peerAddr), c.listen(n.clientAddr)
	}

	cfg := daemon.Config{
		Node: node.Config{
			ID:             id,
			DataDir:        "/data",
			FS:             n.fs,
			TickInterval:   10 * time.Millisecond,
			RequestTimeout: 2 * time.Second,
			SnapshotCount:  20,
			CatchUpEntries: 5,
		},
		Peer:          n.peer,
		Client:        n.client,
		JoinEndpoints: join,
		Logger:        slog.New(slog.DiscardHandler),
	}

	if join == nil {
		cfg.Node.InitialCluster = c.members
	}

	d, err := daemon.Start(c.tb.Context(), cfg)
	if err != nil {
		c.tb.Fatalf("starting node %d: %v", id, err)
	}

	c.mu.Lock()
	n.daemon, n.peer, n.client = d, nil, nil
	c.mu.Unlock()
}

// Stop shuts a node down gracefully. Its data is kept for Restart.
func (c *GRPCCluster) Stop(id uint64) {
	c.mu.Lock()
	n := c.nodes[id]
	d := n.daemon
	n.daemon = nil
	c.mu.Unlock()

	if d == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = d.Shutdown(ctx)
}

// Restart starts a stopped node again, on the same addresses and data.
func (c *GRPCCluster) Restart(id uint64) {
	c.tb.Helper()
	c.start(id, nil)
}

// AddJoining reserves addresses for a new node and returns its peer URL, to
// pass to MemberAdd before calling [GRPCCluster.StartJoining].
func (c *GRPCCluster) AddJoining(id uint64) string {
	return "http://" + c.reserve(id).peerAddr
}

// StartJoining starts a node reserved with AddJoining; it joins through the
// running nodes.
func (c *GRPCCluster) StartJoining(id uint64) {
	c.tb.Helper()

	running := c.running()
	join := make([]string, 0, len(running))

	for _, other := range running {
		join = append(join, c.Endpoint(other))
	}

	c.start(id, join)
}

// Node returns a running node.
func (c *GRPCCluster) Node(id uint64) *node.Node {
	c.tb.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.nodes[id]
	if n == nil || n.daemon == nil {
		c.tb.Fatalf("node %d is not running", id)
	}

	return n.daemon.Node()
}

// Endpoint returns the client address of a node.
func (c *GRPCCluster) Endpoint(id uint64) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nodes[id].clientAddr
}

// Endpoints returns the client addresses of every node, running or not.
func (c *GRPCCluster) Endpoints() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	eps := make([]string, 0, len(c.nodes))
	for _, id := range slices.Sorted(maps.Keys(c.nodes)) {
		eps = append(eps, c.nodes[id].clientAddr)
	}

	return eps
}

func (c *GRPCCluster) running() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	ids := []uint64{}

	for id, n := range c.nodes {
		if n.daemon != nil {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)

	return ids
}

// Client returns a client of every node, closed when the test ends.
func (c *GRPCCluster) Client() *client.Client {
	c.tb.Helper()

	cl, err := client.New(client.Config{
		Endpoints:      c.Endpoints(),
		AttemptTimeout: 2 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
	})
	if err != nil {
		c.tb.Fatal(err)
	}

	c.tb.Cleanup(func() { _ = cl.Close() })

	return cl
}

// Leader waits for a running node to lead and returns its ID.
func (c *GRPCCluster) Leader() uint64 {
	c.tb.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		for _, id := range c.running() {
			c.mu.Lock()
			d := c.nodes[id].daemon
			c.mu.Unlock()

			if d != nil && d.Node().IsLeader() {
				return id
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	c.tb.Fatal("no leader")

	return 0
}
