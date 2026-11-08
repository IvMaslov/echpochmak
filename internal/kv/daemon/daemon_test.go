package daemon_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/daemon"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
)

// cluster runs daemons on localhost, connected over real gRPC.
type cluster struct {
	t       *testing.T
	members []*kvpb.Member
	addrs   map[uint64][2]string // peer, client
	fs      map[uint64]vfs.FS
	daemons map[uint64]*daemon.Daemon
}

func listen(t *testing.T, addr string) net.Listener {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("listening on %s: %v", addr, err)
	}

	return l
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()

	c := &cluster{
		t:       t,
		addrs:   map[uint64][2]string{},
		fs:      map[uint64]vfs.FS{},
		daemons: map[uint64]*daemon.Daemon{},
	}

	peers := map[uint64]net.Listener{}
	clients := map[uint64]net.Listener{}

	for i := range size {
		id := uint64(i + 1)
		peers[id] = listen(t, "127.0.0.1:0")
		clients[id] = listen(t, "127.0.0.1:0")
		c.addrs[id] = [2]string{peers[id].Addr().String(), clients[id].Addr().String()}
		c.members = append(c.members, &kvpb.Member{Id: id, PeerUrl: "http://" + c.addrs[id][0]})
		c.fs[id] = vfs.NewMem()
	}

	for id := range peers {
		c.start(id, peers[id], clients[id], nil)
	}

	t.Cleanup(func() {
		for id := range c.daemons {
			c.stop(id)
		}
	})

	return c
}

func (c *cluster) start(id uint64, peer, cl net.Listener, join []string) {
	c.t.Helper()

	cfg := daemon.Config{
		Node: node.Config{
			ID:             id,
			DataDir:        "/data",
			FS:             c.fs[id],
			TickInterval:   10 * time.Millisecond,
			RequestTimeout: 2 * time.Second,
			SnapshotCount:  20,
			CatchUpEntries: 5,
		},
		Peer:          peer,
		Client:        cl,
		JoinEndpoints: join,
		Logger:        slog.New(slog.DiscardHandler),
	}

	if join == nil {
		cfg.Node.InitialCluster = c.members
	}

	d, err := daemon.Start(c.t.Context(), cfg)
	if err != nil {
		c.t.Fatalf("starting node %d: %v", id, err)
	}

	c.daemons[id] = d
}

func (c *cluster) stop(id uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = c.daemons[id].Shutdown(ctx)
	delete(c.daemons, id)
}

// restart starts a stopped node again on the same addresses and disk.
func (c *cluster) restart(id uint64) {
	c.t.Helper()
	c.start(id, listen(c.t, c.addrs[id][0]), listen(c.t, c.addrs[id][1]), nil)
}

func (c *cluster) endpoints() []string {
	eps := make([]string, 0, len(c.addrs))
	for _, id := range slices.Sorted(maps.Keys(c.addrs)) {
		eps = append(eps, c.addrs[id][1])
	}

	return eps
}

func (c *cluster) client() *client.Client {
	c.t.Helper()

	cl, err := client.New(client.Config{
		Endpoints:      c.endpoints(),
		AttemptTimeout: 2 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
	})
	if err != nil {
		c.t.Fatal(err)
	}

	c.t.Cleanup(func() { _ = cl.Close() })

	return cl
}

func (c *cluster) leader() uint64 {
	c.t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		for _, d := range c.daemons {
			if d.Node().IsLeader() {
				return d.Node().ID()
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	c.t.Fatal("no leader")

	return 0
}

func put(t *testing.T, cl *client.Client, key, value string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	if _, err := cl.Put(ctx, []byte(key), []byte(value)); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func mustGet(t *testing.T, cl *client.Client, key, want string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	kv, err := cl.Get(ctx, []byte(key))
	if err != nil || string(kv.GetValue()) != want {
		t.Fatalf("get %s = %v, %v; want %q", key, kv, err, want)
	}
}

func TestClusterOverGRPC(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	cl := c.client()

	put(t, cl, "a", "1")
	mustGet(t, cl, "a", "1")

	// Compare-and-swap succeeds once, then fails on the stale revision.
	kv, err := cl.Get(t.Context(), []byte("a"))
	if err != nil {
		t.Fatal(err)
	}

	swap := func(rev int64) bool {
		resp, err := cl.Txn(t.Context(), &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{client.CompareModRevision([]byte("a"), kvpb.Compare_RESULT_EQUAL, rev)},
			Success: []*kvpb.RequestOp{client.OpPut([]byte("a"), []byte("2"))},
		})
		if err != nil {
			t.Fatal(err)
		}

		return resp.GetSucceeded()
	}

	if !swap(kv.GetModRevision()) || swap(kv.GetModRevision()) {
		t.Fatal("compare-and-swap must succeed once and then fail")
	}

	// An invalid request fails for good instead of being retried.
	if _, err := cl.Put(t.Context(), nil, []byte("v")); !errors.Is(err, client.ErrInvalidArgument) {
		t.Fatalf("empty key: error %v, want ErrInvalidArgument", err)
	}

	// The client fails over when the leader stops.
	c.stop(c.leader())

	put(t, cl, "after-leader", "v")
	mustGet(t, cl, "after-leader", "v")
}

// A follower that misses compacted entries catches up from a snapshot sent
// over the gRPC snapshot stream.
func TestSnapshotOverGRPC(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	cl := c.client()
	put(t, cl, "first", "v")

	lagging := slices.DeleteFunc([]uint64{1, 2, 3}, func(id uint64) bool { return id == c.leader() })[0]
	c.stop(lagging)

	for i := range 100 {
		put(t, cl, fmt.Sprintf("k%03d", i), strconv.Itoa(i))
	}

	c.restart(lagging)

	deadline := time.Now().Add(15 * time.Second)

	for {
		st := c.daemons[lagging].Node().Status()
		if st.Applied >= c.daemons[c.leader()].Node().Status().Applied {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("lagging node stuck at %d", st.Applied)
		}

		time.Sleep(50 * time.Millisecond)
	}

	only, err := client.New(client.Config{Endpoints: []string{c.addrs[lagging][1]}})
	if err != nil {
		t.Fatal(err)
	}
	defer only.Close()

	resp, err := only.Range(t.Context(), &kvpb.RangeRequest{Key: []byte("k099"), Serializable: true})
	if err != nil || len(resp.GetKvs()) != 1 {
		t.Fatalf("lagging node after snapshot: %v, %v", resp, err)
	}
}

func TestJoinOverGRPC(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	cl := c.client()
	put(t, cl, "k", "v")

	peer, clientL := listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")
	c.addrs[4] = [2]string{peer.Addr().String(), clientL.Addr().String()}
	c.fs[4] = vfs.NewMem()

	ctx := t.Context()

	if _, err := cl.MemberAdd(ctx, 4, "http://"+c.addrs[4][0]); err != nil {
		t.Fatalf("MemberAdd: %v", err)
	}

	c.start(4, peer, clientL, c.endpoints()[:3])

	deadline := time.Now().Add(15 * time.Second)

	for {
		_, err := cl.MemberPromote(ctx, 4)
		if err == nil {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("MemberPromote: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	resp, err := cl.MemberList(ctx)
	if err != nil {
		t.Fatal(err)
	}

	voters := 0

	for _, m := range resp.GetMembers() {
		if !m.GetLearner() {
			voters++
		}
	}

	if voters != 4 {
		t.Fatalf("%d voters after promotion, want 4", voters)
	}
}
