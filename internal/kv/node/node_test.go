package node_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/statemachine"
)

func put(t *testing.T, c *kvtest.Cluster, via uint64, key, value string) int64 {
	t.Helper()

	resp, err := c.Put(t.Context(), via, key, value)
	if err != nil {
		t.Fatalf("put %s via node %d: %v", key, via, err)
	}

	return resp.GetHeader().GetRevision()
}

// get reads a key through a node, linearizably unless serializable is set.
func get(t *testing.T, n *node.Node, key string, serializable bool) (*kvpb.KeyValue, error) {
	t.Helper()

	var kv *kvpb.KeyValue

	err := n.Read(t.Context(), serializable, func(v *statemachine.View) error {
		resp, err := v.Range(&kvpb.RangeRequest{Key: []byte(key)})
		if err != nil {
			return err
		}

		if len(resp.GetKvs()) > 0 {
			kv = resp.GetKvs()[0]
		}

		return nil
	})

	return kv, err
}

func mustGet(t *testing.T, n *node.Node, key, want string) {
	t.Helper()

	kv, err := get(t, n, key, false)
	if err != nil {
		t.Fatalf("node %d: reading %s: %v", n.ID(), key, err)
	}

	if string(kv.GetValue()) != want {
		t.Fatalf("node %d: %s = %q, want %q", n.ID(), key, kv.GetValue(), want)
	}
}

func TestSingleNodeWriteAndRestart(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 1, kvtest.Options{})

	for i := range 20 {
		put(t, c, 1, fmt.Sprintf("k%d", i), strconv.Itoa(i))
	}

	mustGet(t, c.Node(1), "k7", "7")

	c.Restart(1)
	c.WaitLeader()

	mustGet(t, c.Node(1), "k19", "19")

	if rev := put(t, c, 1, "after", "restart"); rev != 21 {
		t.Fatalf("revision after restart = %d, want 21", rev)
	}
}

func TestThreeNodesReplicate(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{})

	// Writes through any node, including followers, are replicated, and a
	// linearizable read on any node sees every completed write.
	for _, via := range c.IDs() {
		key := fmt.Sprintf("via-%d", via)
		put(t, c, via, key, "v")

		for _, id := range c.IDs() {
			mustGet(t, c.Node(id), key, "v")
		}
	}
}

func TestInvalidProposalIsRejectedBeforeRaft(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 1, kvtest.Options{})

	_, err := c.Node(1).Propose(t.Context(), &kvpb.Proposal{
		Operation: &kvpb.Proposal_Put{Put: &kvpb.PutRequest{}},
	})
	if !errors.Is(err, node.ErrInvalidRequest) {
		t.Fatalf("empty-key put error = %v, want ErrInvalidRequest", err)
	}
}

func TestReadWithoutLeaderIsUnavailable(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{Configure: func(cfg *node.Config) {
		cfg.RequestTimeout = 500 * time.Millisecond
	}})

	leader := c.WaitLeader()
	put(t, c, leader, "k", "v")
	waitApplied(t, c, c.Node(leader).Status().Applied)

	// Cut every node off from every other: there is no quorum anywhere.
	net := c.Network()
	for _, id := range c.IDs() {
		net.Isolate(id, c.IDs())
	}

	time.Sleep(300 * time.Millisecond)

	for _, id := range c.IDs() {
		if _, err := get(t, c.Node(id), "k", false); !errors.Is(err, node.ErrUnavailable) {
			t.Fatalf("node %d: linearizable read without quorum: error %v, want ErrUnavailable", id, err)
		}

		// A serializable read is served locally.
		if kv, err := get(t, c.Node(id), "k", true); err != nil || string(kv.GetValue()) != "v" {
			t.Fatalf("node %d: serializable read = %v, %v", id, kv, err)
		}
	}

	net.Heal()
	c.WaitLeader()
	mustGet(t, c.Node(1), "k", "v")
}

func TestStatus(t *testing.T) {
	t.Parallel()

	c := kvtest.NewCluster(t, 3, kvtest.Options{})
	leader := c.WaitLeader()
	put(t, c, leader, "k", "v")

	c.Eventually(5*time.Second, func() error {
		st := c.Node(leader).Status()

		switch {
		case st.Leader != leader || st.RaftState != "StateLeader":
			return fmt.Errorf("status = %+v", st)
		case len(st.Followers) != 2 || len(st.Members) != 3:
			return fmt.Errorf("leader sees %d followers and %d members, want 2 and 3",
				len(st.Followers), len(st.Members))
		case st.ClusterID == "" || st.Applied < 4:
			return fmt.Errorf("status = %+v", st)
		}

		return nil
	})
}
