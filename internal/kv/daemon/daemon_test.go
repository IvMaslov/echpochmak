package daemon_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/kvtest"
)

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

	c := kvtest.NewGRPCCluster(t, 3)
	cl := c.Client()

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
	c.Stop(c.Leader())

	put(t, cl, "after-leader", "v")
	mustGet(t, cl, "after-leader", "v")
}

// A follower that misses compacted entries catches up from a snapshot sent
// over the gRPC snapshot stream.
func TestSnapshotOverGRPC(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	cl := c.Client()
	put(t, cl, "first", "v")

	leader := c.Leader()
	lagging := slices.DeleteFunc([]uint64{1, 2, 3}, func(id uint64) bool { return id == leader })[0]
	c.Stop(lagging)

	for i := range 100 {
		put(t, cl, fmt.Sprintf("k%03d", i), strconv.Itoa(i))
	}

	c.Restart(lagging)

	deadline := time.Now().Add(15 * time.Second)

	for {
		st := c.Node(lagging).Status()
		if st.Applied >= c.Node(c.Leader()).Status().Applied {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("lagging node stuck at %d", st.Applied)
		}

		time.Sleep(50 * time.Millisecond)
	}

	only, err := client.New(client.Config{Endpoints: []string{c.Endpoint(lagging)}})
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

	c := kvtest.NewGRPCCluster(t, 3)
	cl := c.Client()
	put(t, cl, "k", "v")

	ctx := t.Context()

	if _, err := cl.MemberAdd(ctx, 4, c.AddJoining(4)); err != nil {
		t.Fatalf("MemberAdd: %v", err)
	}

	c.StartJoining(4)

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
