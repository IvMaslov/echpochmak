package daemon_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3/raftpb"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/snapshot"
)

// follower returns the endpoint of a running node that is not the leader.
func follower(t *testing.T, c *kvtest.GRPCCluster, ids ...uint64) string {
	t.Helper()

	leader := c.Leader()

	for _, id := range ids {
		if id != leader {
			return c.Endpoint(id)
		}
	}

	t.Fatal("no follower")

	return ""
}

func exportSnapshot(t *testing.T, cl *client.Client, endpoint string) ([]byte, *kvpb.SnapshotHeader) {
	t.Helper()

	var buf bytes.Buffer

	h, err := cl.ExportSnapshot(t.Context(), endpoint, client.To(&buf))
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	return buf.Bytes(), h
}

func countKeys(t *testing.T, cl *client.Client, prefix string) int {
	t.Helper()

	resp, err := cl.Range(t.Context(), &kvpb.RangeRequest{
		Key:      []byte(prefix),
		RangeEnd: []byte(prefix + "\xff"),
	})
	if err != nil || resp.GetMore() {
		t.Fatalf("Range = more %v, %v", resp.GetMore(), err)
	}

	return len(resp.GetKvs())
}

// A snapshot served by a follower, while writes continue, holds every write
// acknowledged before the call.
func TestSnapshotHoldsEveryAcknowledgedWrite(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	cl := c.Client()

	for i := range 50 {
		put(t, cl, fmt.Sprintf("k/%03d", i), "v")
	}

	ctx, cancel := context.WithCancel(t.Context())

	var (
		wg      sync.WaitGroup
		written atomic.Int64
	)

	wg.Go(func() {
		for i := 50; ctx.Err() == nil; i++ {
			if _, err := cl.Put(ctx, []byte(fmt.Sprintf("k/%03d", i)), []byte("v")); err == nil {
				written.Store(int64(i + 1))
			}
		}
	})

	time.Sleep(50 * time.Millisecond)
	acked := written.Load()

	file, h := exportSnapshot(t, cl, follower(t, c, 1, 2, 3))

	cancel()
	wg.Wait()

	info, err := snapshot.Verify(bytes.NewReader(file))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if info.Header.GetIndex() != h.GetIndex() || h.GetClusterId() == "" || len(h.GetMembers()) != 3 {
		t.Fatalf("header %v, file header %v", h, info.Header)
	}

	restored := kvtest.NewRestoredGRPCCluster(t, []uint64{1}, file, 0)

	if got := countKeys(t, restored.Client(), "k/"); int64(got) < acked {
		t.Fatalf("the snapshot holds %d keys, but %d writes were acknowledged before it", got, acked)
	}
}

// A cluster destroyed entirely comes back from its snapshot on new nodes, as
// a new cluster: the data is there, the revision jumped, writes work.
func TestRestoreOntoNewNodes(t *testing.T) {
	t.Parallel()

	old := kvtest.NewGRPCCluster(t, 3)
	cl := old.Client()

	for i := range 30 {
		put(t, cl, fmt.Sprintf("k/%02d", i), strings.Repeat("x", i))
	}

	kv, err := cl.Get(t.Context(), []byte("k/07"))
	if err != nil {
		t.Fatal(err)
	}

	file, h := exportSnapshot(t, cl, old.Endpoint(1))
	oldClusterID := old.Node(1).ClusterID()

	for id := uint64(1); id <= 3; id++ {
		old.Stop(id)
	}

	for _, ids := range [][]uint64{{11, 12, 13}, {21, 22, 23, 24, 25}, {31}} {
		t.Run(fmt.Sprintf("%d nodes", len(ids)), func(t *testing.T) {
			t.Parallel()

			const bump = 1000

			c := kvtest.NewRestoredGRPCCluster(t, ids, file, bump)
			rc := c.Client()

			if got := countKeys(t, rc, "k/"); got != 30 {
				t.Fatalf("%d keys after the restore, want 30", got)
			}

			got, err := rc.Get(t.Context(), []byte("k/07"))
			if err != nil || string(got.GetValue()) != "xxxxxxx" || got.GetModRevision() != kv.GetModRevision() {
				t.Fatalf("k/07 = %v, %v; want the value at its old mod revision %d", got, err, kv.GetModRevision())
			}

			put(t, rc, "after", "restore")

			after, err := rc.Get(t.Context(), []byte("after"))
			if err != nil || after.GetModRevision() <= h.GetRevision()+bump {
				t.Fatalf("new write at revision %d, want above %d", after.GetModRevision(), h.GetRevision()+bump)
			}

			leader := c.Node(c.Leader())
			if leader.ClusterID() == oldClusterID || len(leader.Members()) != len(ids) {
				t.Fatalf("cluster %s with %d members; want a new ID and %d members",
					leader.ClusterID(), len(leader.Members()), len(ids))
			}
		})
	}
}

// The unsafe recovery: a stopped node's data directory is exported offline,
// then restored as a new cluster.
func TestRestoreFromAStoppedNodesDataDir(t *testing.T) {
	t.Parallel()

	old := kvtest.NewGRPCCluster(t, 3)
	cl := old.Client()

	for i := range 10 {
		put(t, cl, fmt.Sprintf("k/%d", i), "v")
	}

	survivor := follower(t, old, 1, 2, 3)

	var id uint64

	for i := uint64(1); i <= 3; i++ {
		if old.Endpoint(i) == survivor {
			id = i
		}
	}

	// The follower has applied everything once a linearizable read through
	// it returns.
	mustGetVia(t, old, id, "k/9", "v")

	for i := uint64(1); i <= 3; i++ {
		old.Stop(i)
	}

	var body bytes.Buffer

	exported, err := node.ExportDataDir(old.FS(id), kvtest.DataDir, &body)
	if err != nil {
		t.Fatalf("ExportDataDir: %v", err)
	}

	var file bytes.Buffer

	w, err := snapshot.NewWriter(&file, &kvpb.SnapshotHeader{
		ClusterId: exported.ClusterID,
		Index:     exported.State.GetIndex(),
		Term:      exported.State.GetTerm(),
		Revision:  exported.State.GetRevision(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.Write(body.Bytes()); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	c := kvtest.NewRestoredGRPCCluster(t, []uint64{41, 42, 43}, file.Bytes(), 0)

	if got := countKeys(t, c.Client(), "k/"); got != 10 {
		t.Fatalf("%d keys after the recovery, want 10", got)
	}
}

func mustGetVia(t *testing.T, c *kvtest.GRPCCluster, id uint64, key, want string) {
	t.Helper()

	cl, err := client.New(client.Config{Endpoints: []string{c.Endpoint(id)}})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	mustGet(t, cl, key, want)
}

// A restore that was interrupted can neither start nor be restored over: an
// operator must empty the directory first.
func TestInterruptedRestoreIsRefused(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	if err := fs.MkdirAll("/data", 0o755); err != nil {
		t.Fatal(err)
	}

	f, err := fs.Create("/data/RESTORE_IN_PROGRESS", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}

	_ = f.Close()

	members := []*kvpb.Member{{Id: 1, PeerUrl: "http://127.0.0.1:1"}}

	_, err = node.Start(node.Config{
		ID:             1,
		DataDir:        "/data",
		FS:             fs,
		InitialCluster: members,
		Transport:      nopTransport{},
	})
	if err == nil || !strings.Contains(err.Error(), "interrupted restore") {
		t.Fatalf("Start = %v, want a refusal", err)
	}

	_, err = node.Restore(node.RestoreConfig{ID: 1, DataDir: "/data", FS: fs, Members: members}, bytes.NewReader(nil))
	if err == nil || !strings.Contains(err.Error(), "interrupted restore") {
		t.Fatalf("Restore = %v, want a refusal", err)
	}
}

// A corrupt file restores nothing: the data directory stays empty.
func TestCorruptSnapshotRestoresNothing(t *testing.T) {
	t.Parallel()

	old := kvtest.NewGRPCCluster(t, 1)
	cl := old.Client()
	put(t, cl, "a", "1")

	file, _ := exportSnapshot(t, cl, old.Endpoint(1))
	file[len(file)/2] ^= 0xff

	fs := vfs.NewMem()

	_, err := snapshot.Restore(bytes.NewReader(file), node.RestoreConfig{
		ID:      1,
		DataDir: "/data",
		FS:      fs,
		Members: []*kvpb.Member{{Id: 1, PeerUrl: "http://127.0.0.1:1"}},
	})
	if !errors.Is(err, snapshot.ErrCorrupt) {
		t.Fatalf("Restore = %v, want ErrCorrupt", err)
	}

	if has, _ := node.HasState(fs, "/data"); has {
		t.Fatal("a corrupt snapshot left a database behind")
	}
}

// nopTransport drops every message.
type nopTransport struct{}

func (nopTransport) Send([]*raftpb.Message) {}
func (nopTransport) AddPeer(uint64, string) {}
func (nopTransport) RemovePeer(uint64)      {}
