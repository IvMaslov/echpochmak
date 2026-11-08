//go:build kvchaos

// Package kvchaos checks the KV cluster from the compose stack for
// linearizability while docker kills the leader, disconnects nodes from the
// network and restarts them. Run it with make kv-chaos, which starts the
// cluster and removes it afterwards.
//
// KV_CHAOS_DURATION sets the length of the run (default 30s).
package kvchaos_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/kvtest"
)

const (
	network  = "echpochmak_default"
	nodes    = 3
	seed     = 7
	opTimout = 10 * time.Second
)

func container(id uint64) string { return fmt.Sprintf("echpochmak-kv%d-1", id) }

func endpoints() []string {
	eps := make([]string, 0, nodes)
	for i := 1; i <= nodes; i++ {
		eps = append(eps, fmt.Sprintf("localhost:2379%d", i))
	}

	return eps
}

func docker(t *testing.T, args ...string) {
	t.Helper()

	out, err := exec.CommandContext(context.Background(), "docker", args...).CombinedOutput()
	if err != nil {
		t.Logf("docker %v: %v: %s", args, err, out)
	}
}

// clientKV runs workload operations through the Go client.
type clientKV struct {
	c *client.Client
}

func (k clientKV) Get(ctx context.Context, key string) (string, int64, bool, error) {
	kv, err := k.c.Get(ctx, []byte(key))

	return string(kv.GetValue()), kv.GetModRevision(), kv != nil, err
}

func (k clientKV) Put(ctx context.Context, key, value string) (int64, error) {
	resp, err := k.c.Put(ctx, []byte(key), []byte(value))

	return resp.GetHeader().GetRevision(), err
}

func (k clientKV) CAS(ctx context.Context, key string, expected int64, value string) (bool, int64, error) {
	resp, err := k.c.Txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{client.CompareModRevision([]byte(key), kvpb.Compare_RESULT_EQUAL, expected)},
		Success: []*kvpb.RequestOp{client.OpPut([]byte(key), []byte(value))},
	})

	return resp.GetSucceeded(), resp.GetHeader().GetRevision(), err
}

// leader returns the leader some node reports, or 0.
func leader(ctx context.Context, c *client.Client) uint64 {
	statuses, _ := c.Status(ctx)
	for _, st := range statuses {
		if l := st.GetLeaderId(); l != 0 {
			return l
		}
	}

	return 0
}

// chaos disturbs the cluster until ctx is done, logging each fault.
func chaos(ctx context.Context, t *testing.T, c *client.Client) {
	t.Helper()

	rng := rand.New(rand.NewPCG(seed, seed))

	for ctx.Err() == nil {
		victim := uint64(rng.IntN(nodes) + 1)

		switch rng.IntN(3) {
		case 0:
			if l := leader(ctx, c); l != 0 {
				victim = l
			}

			t.Logf("killing node %d (leader)", victim)
			docker(t, "kill", container(victim))
			pause(ctx, rng)
			docker(t, "start", container(victim))
		case 1:
			t.Logf("disconnecting node %d", victim)
			docker(t, "network", "disconnect", network, container(victim))
			pause(ctx, rng)
			docker(t, "network", "connect", "--alias", fmt.Sprintf("kv%d", victim), network, container(victim))
		default:
			t.Logf("restarting node %d", victim)
			docker(t, "restart", "--time", "5", container(victim))
		}

		pause(ctx, rng)
	}
}

func pause(ctx context.Context, rng *rand.Rand) {
	select {
	case <-ctx.Done():
	case <-time.After(time.Duration(1000+rng.IntN(2000)) * time.Millisecond):
	}
}

func TestLinearizableUnderChaos(t *testing.T) {
	duration := 30 * time.Second

	if v := os.Getenv("KV_CHAOS_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("KV_CHAOS_DURATION: %v", err)
		}

		duration = d
	}

	c, err := client.New(client.Config{
		Endpoints:      endpoints(),
		AttemptTimeout: 2 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ready, cancelReady := context.WithTimeout(t.Context(), time.Minute)
	defer cancelReady()

	if _, err := c.Put(ready, []byte("ready"), []byte("1")); err != nil {
		t.Fatalf("cluster not ready: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup

	wg.Go(func() { chaos(ctx, t, c) })

	history := kvtest.Workload{
		KV: clientKV{c: c},
		Unknown: func(err error) bool {
			return errors.Is(err, client.ErrOutcomeUnknown)
		},
		Clients:  4,
		Keys:     3,
		Duration: duration,
		Seed:     seed,
		Timeout:  opTimout,
	}.Run(t.Context())

	cancel()
	wg.Wait()

	// Heal everything, then check that every node serves again. Nodes the
	// chaos already restored refuse to connect twice; that is fine.
	for id := uint64(1); id <= nodes; id++ {
		_ = exec.CommandContext(t.Context(), "docker", "network", "connect",
			"--alias", fmt.Sprintf("kv%d", id), network, container(id)).Run()
		docker(t, "start", container(id))
	}

	healed, cancelHealed := context.WithTimeout(t.Context(), time.Minute)
	defer cancelHealed()

	if _, err := c.Put(healed, []byte("healed"), []byte("1")); err != nil {
		t.Fatalf("cluster did not recover: %v", err)
	}

	unknown := 0

	for _, op := range history {
		if op.Return == 1<<63-1 {
			unknown++
		}
	}

	result, info := porcupine.CheckOperationsVerbose(kvtest.Model(), history, 5*time.Minute)
	if result != porcupine.Ok {
		path := t.TempDir() + "/history.html"
		_ = porcupine.VisualizePath(kvtest.Model(), info, path)
		t.Fatalf("history of %d operations is %s; visualization in %s", len(history), result, path)
	}

	t.Logf("history of %d operations (%d with unknown outcome) is linearizable", len(history), unknown)
}
