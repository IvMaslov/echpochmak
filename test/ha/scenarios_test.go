//go:build ha

package ha_test

import (
	"sync"
	"testing"
	"time"
)

func TestHA(t *testing.T) {
	s := newStack(t)

	t.Run("1 baseline", func(t *testing.T) {
		s := s.in(t)

		s.createTable("baseline")
		s.exec("INSERT INTO %s.baseline VALUES ('trino', 0), ('trino', 1)", s.schema)

		py := s.pyWriter("baseline", "py", 0)
		s.check("baseline", map[string]*writes{"trino": {acked: []int64{0, 1}}, "py": py})
	})

	t.Run("2 catalog replica loss", func(t *testing.T) {
		s := s.in(t)

		s.underLoad("replica_loss", func() {
			t.Log("killing catalog-a")
			docker(t, "kill", container("catalog-a"))
			time.Sleep(5 * time.Second)
			docker(t, "start", container("catalog-a"))
		})
	})

	t.Run("3 raft leader loss", func(t *testing.T) {
		s := s.in(t)

		s.underLoad("leader_loss", func() {
			leader := s.raftLeader()
			t.Logf("killing %s (raft leader)", leader)
			docker(t, "kill", container(leader))
			time.Sleep(5 * time.Second)
			docker(t, "start", container(leader))
		})
	})

	t.Run("4 replica and leader at once", func(t *testing.T) {
		s := s.in(t)

		s.underLoad("both", func() {
			leader := s.raftLeader()
			t.Logf("killing catalog-b and %s together", leader)
			docker(t, "kill", container("catalog-b"), container(leader))
			time.Sleep(5 * time.Second)
			docker(t, "start", container("catalog-b"), container(leader))
		})
	})

	t.Run("5 rolling restart", func(t *testing.T) {
		s := s.in(t)

		// Separate tables, so the two writers never conflict: any error here
		// is a request the rolling restart failed.
		s.createTable("rolling_py")

		stop := make(chan struct{})

		var trino *writes

		var wg sync.WaitGroup

		s.createTable("rolling_trino")
		wg.Go(func() { trino = s.trinoWriter("rolling_trino", "trino", 0, stop) })

		var py *writes

		wg.Go(func() {
			py = s.pyWriter("rolling_py", "py", 0)

			close(stop)
		})

		time.Sleep(2 * time.Second)

		for _, replica := range []struct{ name, port string }{{"catalog-a", "18181"}, {"catalog-b", "18182"}} {
			t.Logf("restarting %s", replica.name)
			docker(t, "restart", "--time", "15", container(replica.name))
			s.waitReady(replica.port)
			// A rolling restart waits for the load balancer too: HAProxy
			// routes to a replica again after two passing checks, 1 s
			// apart (deployments/compose/haproxy/haproxy.cfg).
			time.Sleep(lbRiseTime)
		}

		wg.Wait()
		s.check("rolling_trino", map[string]*writes{"trino": trino})
		s.check("rolling_py", map[string]*writes{"py": py})

		for _, w := range []*writes{trino, py} {
			if len(w.errors) > 0 {
				t.Errorf("errors during a rolling restart: %q", w.errors)
			}
		}
	})

	t.Run("6 idempotent retry through the other replica", func(t *testing.T) {
		s := s.in(t)

		testIdempotentRetry(t, s)
	})

	t.Run("7 two-table transactions under faults", func(t *testing.T) {
		s := s.in(t)

		testTransactionsUnderFaults(t, s)
	})

	t.Run("8 purge finished by the other replica", func(t *testing.T) {
		s := s.in(t)

		testPurgeAcrossReplicas(t, s)
	})
}
