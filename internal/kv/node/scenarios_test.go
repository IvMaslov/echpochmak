package node_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/kv/node"
)

// smallLog makes compaction happen after a few entries, so snapshot paths
// are exercised by short tests.
func smallLog(cfg *node.Config) {
	cfg.SnapshotCount = 20
	cfg.CatchUpEntries = 5
}

func others(ids []uint64, except uint64) []uint64 {
	return slices.DeleteFunc(slices.Clone(ids), func(id uint64) bool { return id == except })
}

// waitApplied waits until every running node has applied at least index.
func waitApplied(t *testing.T, c *kvtest.Cluster, index uint64) {
	t.Helper()

	c.Eventually(10*time.Second, func() error {
		for _, id := range c.IDs() {
			if got := c.Node(id).Status().Applied; got < index {
				return fmt.Errorf("node %d applied %d, want %d", id, got, index)
			}
		}

		return nil
	})
}

// Scenario 3: the leader is cut off. The majority elects a new leader and
// keeps serving; the old leader steps down, and a write it accepted while
// isolated does not report success.
func TestLeaderIsolated(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{Configure: func(cfg *node.Config) {
		cfg.RequestTimeout = time.Second
	}})

	old := c.WaitLeader()
	put(t, c, old, "before", "1")

	c.Network().Isolate(old, c.IDs())

	// A write through the isolated leader cannot commit.
	_, err := c.Node(old).Propose(t.Context(), &kvpb.Proposal{
		Operation: &kvpb.Proposal_Put{Put: &kvpb.PutRequest{Key: []byte("lost"), Value: []byte("x")}},
	})
	if !errors.Is(err, node.ErrOutcomeUnknown) && !errors.Is(err, node.ErrUnavailable) {
		t.Fatalf("write on the isolated leader: error %v, want unknown or unavailable", err)
	}

	majority := others(c.IDs(), old)

	c.Eventually(5*time.Second, func() error {
		l := c.Node(majority[0]).Leader()
		if l == 0 || l == old {
			return fmt.Errorf("majority leader is %d", l)
		}

		return nil
	})

	put(t, c, majority[0], "during", "2")

	// CheckQuorum makes the old leader step down.
	c.Eventually(5*time.Second, func() error {
		if c.Node(old).IsLeader() {
			return errors.New("the isolated node still believes it leads")
		}

		return nil
	})

	c.Network().Heal()
	c.WaitLeader()

	for _, id := range c.IDs() {
		mustGet(t, c.Node(id), "during", "2")

		// The write the isolated leader accepted never committed, so it was
		// overwritten by the new leader's log.
		if kv, err := get(t, c.Node(id), "lost", false); err != nil || kv != nil {
			t.Fatalf("node %d: lost write = %v, %v; want absent", id, kv, err)
		}
	}
}

// Scenario 4: a follower misses so many entries that the leader compacts
// them away; it catches up from a snapshot.
func TestSnapshotCatchUp(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{Configure: smallLog})
	leader := c.WaitLeader()
	lagging := others(c.IDs(), leader)[0]

	c.Network().Isolate(lagging, c.IDs())

	for i := range 100 {
		put(t, c, leader, fmt.Sprintf("k%03d", i), strconv.Itoa(i))
	}

	c.Network().Heal()

	waitApplied(t, c, c.Node(leader).Status().Applied)

	for _, key := range []string{"k000", "k050", "k099"} {
		kv, err := get(t, c.Node(lagging), key, true)
		if err != nil || kv == nil {
			t.Fatalf("lagging node: %s = %v, %v", key, kv, err)
		}
	}

	// It keeps replicating normally after the snapshot.
	put(t, c, leader, "after", "snapshot")
	mustGet(t, c.Node(lagging), "after", "snapshot")
}

// Scenario 5: a node crashes during writes and loses everything it had not
// synced. No acknowledged write is lost.
func TestCrashDuringWrites(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{Configure: smallLog})

	acked := make(chan string, 1000)
	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}

			key := fmt.Sprintf("w%04d", i)

			n := c.TryNode(c.Leader())
			if n == nil {
				time.Sleep(10 * time.Millisecond)

				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := n.Propose(ctx, &kvpb.Proposal{
				Operation: &kvpb.Proposal_Put{Put: &kvpb.PutRequest{Key: []byte(key), Value: []byte("v")}},
			})

			cancel()

			if err == nil {
				acked <- key
			}
		}
	})

	time.Sleep(200 * time.Millisecond)
	c.Crash(c.WaitLeader())
	time.Sleep(200 * time.Millisecond)
	c.Crash(others(c.IDs(), c.WaitLeader())[0])
	time.Sleep(200 * time.Millisecond)

	close(stop)
	wg.Wait()
	close(acked)

	c.WaitLeader()

	n := 0

	for key := range acked {
		for _, id := range c.IDs() {
			mustGet(t, c.Node(id), key, "v")
		}

		n++
	}

	if n == 0 {
		t.Fatal("no write was acknowledged")
	}
}

// Scenario 6: every node crashes at once. State and revision survive.
func TestWholeClusterCrash(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{Configure: smallLog})
	leader := c.WaitLeader()

	var rev int64
	for i := range 50 {
		rev = put(t, c, leader, fmt.Sprintf("k%d", i), strconv.Itoa(i))
	}

	for _, id := range c.IDs() {
		c.Crash(id)
	}

	leader = c.WaitLeader()

	for _, id := range c.IDs() {
		mustGet(t, c.Node(id), "k49", "49")
	}

	if next := put(t, c, leader, "next", "v"); next != rev+1 {
		t.Fatalf("revision after the crash = %d, want %d", next, rev+1)
	}
}

// Scenario 7: a node joins as a learner, catches up and is promoted; then a
// voter, the leader itself, is removed.
func TestMembership(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{Configure: smallLog})
	leader := c.WaitLeader()

	for i := range 50 {
		put(t, c, leader, fmt.Sprintf("k%d", i), "v")
	}

	ctx := t.Context()

	if _, err := c.Node(leader).AddMember(ctx, 4, "mem://4"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}

	if _, err := c.Node(leader).AddMember(ctx, 4, "mem://4"); !errors.Is(err, node.ErrMember) {
		t.Fatalf("adding a member twice: error %v, want ErrMember", err)
	}

	follower := others(c.IDs(), leader)[0]
	if _, err := c.Node(follower).PromoteMember(ctx, 4); err == nil {
		t.Fatal("a follower promoted a member")
	}

	joined := c.StartJoining(4)
	waitApplied(t, c, c.Node(leader).Status().Applied)

	if kv, err := get(t, joined, "k49", true); err != nil || kv == nil {
		t.Fatalf("learner has not caught up: %v, %v", kv, err)
	}

	members, err := c.Node(leader).PromoteMember(ctx, 4)
	if err != nil {
		t.Fatalf("PromoteMember: %v", err)
	}

	if voters := countVoters(members); voters != 4 {
		t.Fatalf("%d voters after promotion, want 4", voters)
	}

	// Remove the leader: the rest elect a new one and keep serving.
	if _, err := c.Node(leader).RemoveMember(ctx, leader); err != nil {
		t.Fatalf("RemoveMember(leader): %v", err)
	}

	select {
	case <-c.Node(leader).Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the removed leader did not stop")
	}

	c.Kill(leader)

	next := c.WaitLeader()
	put(t, c, next, "after", "removal")

	for _, id := range c.IDs() {
		mustGet(t, c.Node(id), "after", "removal")
	}

	if voters := countVoters(c.Node(next).Members()); voters != 3 {
		t.Fatalf("%d voters after removal, want 3", voters)
	}
}

func countVoters(members []*kvpb.Member) int {
	n := 0

	for _, m := range members {
		if !m.GetLearner() {
			n++
		}
	}

	return n
}

// Scenario 8: a write proposed twice with the same request ID, including
// through a new leader, is applied once, and both calls see one result.
func TestDedupAcrossLeaderChange(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{})
	leader := c.WaitLeader()

	p := &kvpb.Proposal{
		RequestId: []byte("retry-me"),
		Operation: &kvpb.Proposal_Txn{Txn: &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{{
				Key:    []byte("counter"),
				Result: kvpb.Compare_RESULT_EQUAL,
				Target: &kvpb.Compare_ModRevision{ModRevision: 0},
			}},
			Success: []*kvpb.RequestOp{{Request: &kvpb.RequestOp_RequestPut{
				RequestPut: &kvpb.PutRequest{Key: []byte("counter"), Value: []byte("1")},
			}}},
		}},
	}

	first, err := c.Node(leader).Propose(t.Context(), p)
	if err != nil || !first.GetTxn().GetSucceeded() {
		t.Fatalf("first attempt = %v, %v", first, err)
	}

	if err := c.Node(leader).TransferLeadership(t.Context(), 0); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}

	next := c.WaitLeader()
	if next == leader {
		t.Fatal("leadership did not move")
	}

	// The retry goes through a follower of the new leader. Executed again,
	// the compare would fail; deduplicated, it reports the first result.
	retry, err := c.Node(others(c.IDs(), next)[0]).Propose(t.Context(), p)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}

	if !proto.Equal(first, retry) {
		t.Fatalf("retry result %v, want the first result %v", retry, first)
	}

	kv, err := get(t, c.Node(next), "counter", false)
	if err != nil || kv.GetVersion() != 1 {
		t.Fatalf("counter = %v, %v; want one write", kv, err)
	}
}

// Scenario 9: after a random workload with faults, every node holds the same
// state, byte for byte.
func TestDeterministicState(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{Seed: 9, Configure: smallLog})
	c.Network().SetDropRate(0.05)

	rng := rand.New(rand.NewPCG(9, 9))

	for i := range 150 {
		via := c.IDs()[rng.IntN(3)]
		key := fmt.Sprintf("k%d", rng.IntN(10))

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)

		var err error

		if rng.IntN(4) == 0 {
			_, err = c.Node(via).Propose(ctx, &kvpb.Proposal{Operation: &kvpb.Proposal_DeleteRange{
				DeleteRange: &kvpb.DeleteRangeRequest{Key: []byte(key)},
			}})
		} else {
			_, err = c.Put(ctx, via, key, strconv.Itoa(i))
		}

		cancel()

		// Lost messages may leave a write's outcome unknown; that is fine
		// here, the test compares states, not results.
		expected := err == nil || errors.Is(err, node.ErrUnavailable) || errors.Is(err, node.ErrOutcomeUnknown)
		if !expected {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	c.Network().SetDropRate(0)
	put(t, c, c.WaitLeader(), "barrier", "v")
	waitApplied(t, c, c.Node(c.WaitLeader()).Status().Applied)

	states := map[uint64][]byte{}

	for _, id := range c.IDs() {
		snap, err := c.Node(id).OpenSnapshot()
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if _, err := snap.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}

		snap.Close()

		states[id] = buf.Bytes()
	}

	for _, id := range c.IDs()[1:] {
		if !bytes.Equal(states[id], states[c.IDs()[0]]) {
			t.Fatalf("node %d's state differs from node %d's", id, c.IDs()[0])
		}
	}
}
