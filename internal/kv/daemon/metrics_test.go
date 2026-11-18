package daemon_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/daemon"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/server"
)

func listen(t *testing.T) net.Listener {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	return l
}

func TestDaemonMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	peer, clientL := listen(t), listen(t)

	d, err := daemon.Start(t.Context(), daemon.Config{
		Node: node.Config{
			ID:             1,
			DataDir:        "/data",
			FS:             vfs.NewMem(),
			TickInterval:   10 * time.Millisecond,
			InitialCluster: []*kvpb.Member{{Id: 1, PeerUrl: "http://" + peer.Addr().String()}},
		},
		Peer:     peer,
		Client:   clientL,
		Server:   server.Options{Zone: "zone-a"},
		Logger:   slog.New(slog.DiscardHandler),
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = d.Shutdown(ctx)
	})

	cl, err := client.New(client.Config{Endpoints: []string{clientL.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	put(t, cl, "a", "1")
	mustGet(t, cl, "a", "1")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	present := map[string]bool{}
	for _, f := range families {
		present[f.GetName()] = true
	}

	for _, name := range []string{
		"echpochmak_kv_node_info",
		"echpochmak_kv_raft_term",
		"echpochmak_kv_raft_is_leader",
		"echpochmak_kv_raft_applied_index",
		"echpochmak_kv_proposals_total",
		"echpochmak_kv_wal_fsync_seconds",
		"echpochmak_kv_read_index_duration_seconds",
		"echpochmak_kv_grpc_requests_total",
		"echpochmak_kv_pebble_disk_usage_bytes",
	} {
		if !present[name] {
			t.Errorf("metric %s is missing", name)
		}
	}

	if got := testutil.ToFloat64(gaugeNamed(t, reg, "echpochmak_kv_raft_is_leader")); got != 1 {
		t.Errorf("is_leader = %v, want 1 on a single-node cluster", got)
	}
}

// gaugeNamed returns a collector of the single metric called name in reg.
func gaugeNamed(t *testing.T, reg *prometheus.Registry, name string) prometheus.Collector {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range families {
		if f.GetName() == name && len(f.GetMetric()) == 1 {
			g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "copy"})
			g.Set(f.GetMetric()[0].GetGauge().GetValue())

			return g
		}
	}

	t.Fatalf("no single metric %s", name)

	return nil
}
