package node_test

import (
	"strconv"
	"sync/atomic"
	"testing"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/statemachine"
)

// benchCluster is a 3-node cluster on the real filesystem, so every write
// pays for its fsyncs. The network is in memory, so these numbers are a
// lower bound on latency, not a prediction for a real deployment.
func benchCluster(b *testing.B) (*kvtest.Cluster, *node.Node) {
	b.Helper()

	c := kvtest.NewCluster(b, 3, kvtest.Options{DiskDir: b.TempDir()})
	leader := c.Node(c.WaitLeader())

	return c, leader
}

func BenchmarkPut(b *testing.B) {
	_, leader := benchCluster(b)
	value := make([]byte, 256)

	b.ResetTimer()

	for i := range b.N {
		_, err := leader.Propose(b.Context(), &kvpb.Proposal{Operation: &kvpb.Proposal_Put{
			Put: &kvpb.PutRequest{Key: []byte("k" + strconv.Itoa(i%1000)), Value: value},
		}})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutParallel(b *testing.B) {
	_, leader := benchCluster(b)
	value := make([]byte, 256)

	var seq atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := []byte("k" + strconv.FormatInt(seq.Add(1)%1000, 10))

			_, err := leader.Propose(b.Context(), &kvpb.Proposal{Operation: &kvpb.Proposal_Put{
				Put: &kvpb.PutRequest{Key: key, Value: value},
			}})
			if err != nil {
				b.Error(err)

				return
			}
		}
	})
}

// BenchmarkCAS measures the catalog's write pattern: compare a key's
// revision and replace its value.
func BenchmarkCAS(b *testing.B) {
	_, leader := benchCluster(b)

	var rev int64

	b.ResetTimer()

	for i := range b.N {
		res, err := leader.Propose(b.Context(), &kvpb.Proposal{Operation: &kvpb.Proposal_Txn{Txn: &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{{
				Key:    []byte("pointer"),
				Result: kvpb.Compare_RESULT_EQUAL,
				Target: &kvpb.Compare_ModRevision{ModRevision: rev},
			}},
			Success: []*kvpb.RequestOp{{Request: &kvpb.RequestOp_RequestPut{
				RequestPut: &kvpb.PutRequest{Key: []byte("pointer"), Value: []byte(strconv.Itoa(i))},
			}}},
		}}})
		if err != nil || !res.GetTxn().GetSucceeded() {
			b.Fatalf("CAS %d: %v, %v", i, res, err)
		}

		rev = res.GetTxn().GetHeader().GetRevision()
	}
}

func BenchmarkLinearizableRead(b *testing.B) {
	c, leader := benchCluster(b)

	if _, err := c.Put(b.Context(), leader.ID(), "k", "v"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for range b.N {
		err := leader.Read(b.Context(), false, func(v *statemachine.View) error {
			_, err := v.Range(&kvpb.RangeRequest{Key: []byte("k")})

			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
