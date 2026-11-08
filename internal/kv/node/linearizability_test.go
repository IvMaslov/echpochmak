package node_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/statemachine"
)

// nodeKV runs workload operations on random running nodes of a cluster.
type nodeKV struct {
	c   *kvtest.Cluster
	mu  sync.Mutex
	rng *rand.Rand
}

func (k *nodeKV) pick() *node.Node {
	k.mu.Lock()
	defer k.mu.Unlock()

	ids := k.c.IDs()
	if len(ids) == 0 {
		return nil
	}

	return k.c.TryNode(ids[k.rng.IntN(len(ids))])
}

var errNoNode = errors.New("no running node")

func (k *nodeKV) Get(ctx context.Context, key string) (string, int64, bool, error) {
	n := k.pick()
	if n == nil {
		return "", 0, false, errNoNode
	}

	var kv *kvpb.KeyValue

	err := n.Read(ctx, false, func(v *statemachine.View) error {
		resp, err := v.Range(&kvpb.RangeRequest{Key: []byte(key)})
		if err == nil && len(resp.GetKvs()) > 0 {
			kv = resp.GetKvs()[0]
		}

		return err
	})

	return string(kv.GetValue()), kv.GetModRevision(), kv != nil, err
}

func (k *nodeKV) Put(ctx context.Context, key, value string) (int64, error) {
	n := k.pick()
	if n == nil {
		return 0, errNoNode
	}

	res, err := n.Propose(ctx, &kvpb.Proposal{
		Operation: &kvpb.Proposal_Put{Put: &kvpb.PutRequest{Key: []byte(key), Value: []byte(value)}},
	})

	return res.GetPut().GetHeader().GetRevision(), err
}

func (k *nodeKV) CAS(ctx context.Context, key string, expected int64, value string) (bool, int64, error) {
	n := k.pick()
	if n == nil {
		return false, 0, errNoNode
	}

	res, err := n.Propose(ctx, &kvpb.Proposal{
		Operation: &kvpb.Proposal_Txn{Txn: &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{{
				Key:    []byte(key),
				Result: kvpb.Compare_RESULT_EQUAL,
				Target: &kvpb.Compare_ModRevision{ModRevision: expected},
			}},
			Success: []*kvpb.RequestOp{{Request: &kvpb.RequestOp_RequestPut{
				RequestPut: &kvpb.PutRequest{Key: []byte(key), Value: []byte(value)},
			}}},
		}},
	})

	return res.GetTxn().GetSucceeded(), res.GetTxn().GetHeader().GetRevision(), err
}

// injectFaults disturbs the cluster at random until ctx is done: it isolates
// the leader, partitions, drops messages and crashes nodes, then heals.
func injectFaults(ctx context.Context, c *kvtest.Cluster, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed+1))
	net := c.Network()
	all := c.IDs()

	for ctx.Err() == nil {
		switch rng.IntN(5) {
		case 0:
			if l := c.Leader(); l != 0 {
				net.Isolate(l, all)
			}
		case 1:
			victim := all[rng.IntN(len(all))]
			net.Partition([]uint64{victim}, others(all, victim))
		case 2:
			net.SetDropRate(0.2)
		case 3:
			c.Crash(all[rng.IntN(len(all))])
		default:
		}

		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(100+rng.IntN(300)) * time.Millisecond):
		}

		net.Heal()
		net.SetDropRate(0)

		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(100+rng.IntN(200)) * time.Millisecond):
		}
	}
}

// tortureSettings returns the seed and duration of the linearizability run:
// short and fixed by default, or set with KV_TORTURE_SEED and
// KV_TORTURE_DURATION for long randomized runs (make kv-torture).
func tortureSettings(t *testing.T) (uint64, time.Duration) {
	t.Helper()

	seed, duration := uint64(2), 4*time.Second

	if v := os.Getenv("KV_TORTURE_SEED"); v != "" {
		s, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("KV_TORTURE_SEED: %v", err)
		}

		seed = s
	}

	if v := os.Getenv("KV_TORTURE_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("KV_TORTURE_DURATION: %v", err)
		}

		duration = d
	}

	return seed, duration
}

// The milestone check, in process: under leader isolation, partitions,
// message loss and crashes, the history of reads, writes and
// compare-and-swaps is linearizable.
func TestLinearizableUnderFaults(t *testing.T) {
	t.Parallel()

	seed, duration := tortureSettings(t)
	t.Logf("seed %d, duration %s", seed, duration)

	c := kvtest.NewCluster(t, 3, kvtest.Options{Seed: seed, Configure: func(cfg *node.Config) {
		smallLog(cfg)
		cfg.RequestTimeout = time.Second
	}})

	ctx, cancel := context.WithCancel(t.Context())

	var faults sync.WaitGroup

	faults.Go(func() { injectFaults(ctx, c, seed) })

	history := kvtest.Workload{
		KV: &nodeKV{c: c, rng: rand.New(rand.NewPCG(seed, 0))},
		Unknown: func(err error) bool {
			return errors.Is(err, node.ErrOutcomeUnknown)
		},
		Clients:  4,
		Keys:     3,
		Duration: duration,
		Seed:     seed,
		Timeout:  2 * time.Second,
	}.Run(t.Context())

	cancel()
	faults.Wait()

	completed := 0

	for _, op := range history {
		if op.Return != 1<<63-1 {
			completed++
		}
	}

	if completed < 50 {
		t.Fatalf("only %d operations completed; the workload did not exercise the cluster", completed)
	}

	result, info := porcupine.CheckOperationsVerbose(kvtest.Model(), history, time.Minute)
	if result != porcupine.Ok {
		path := t.TempDir() + "/history.html"
		_ = porcupine.VisualizePath(kvtest.Model(), info, path)
		t.Fatalf("history of %d operations is %s; visualization in %s", len(history), result, path)
	}

	t.Logf("history of %d operations (%d completed) is linearizable", len(history), completed)
}
