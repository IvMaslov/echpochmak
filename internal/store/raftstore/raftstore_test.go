package raftstore_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/store"
	"echpochmak/internal/store/raftstore"
	"echpochmak/internal/store/storetest"
)

// TestContract runs the Store contract suite, the same one memstore passes,
// against raftstore on a 3-node cluster over real gRPC. Every store gets its
// own key prefix, so the suite's parallel subtests share one cluster.
func TestContract(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	cl := c.Client()

	var n atomic.Int64

	storetest.Run(t, func(*testing.T) store.Store {
		return raftstore.New(cl, raftstore.Options{Prefix: fmt.Sprintf("test/%d/", n.Add(1))})
	})
}
