package conformance_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"

	"echpochmak/internal/api"
	"echpochmak/internal/catalog"
	"echpochmak/internal/fileio"
	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/daemon"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/logger"
	"echpochmak/internal/metrics"
	"echpochmak/internal/store/instrumented"
	"echpochmak/internal/store/raftstore"
)

// syncBuffer is a log destination shared by concurrent writers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// observedStack is a catalog server on a one-node KV cluster with every
// metric and trace hook connected, as catalog-server and kv-node connect
// them.
type observedStack struct {
	url   string
	spans *tracetest.SpanRecorder
	logs  *syncBuffer
}

func newObservedStack(t *testing.T) observedStack {
	t.Helper()

	// The gRPC instrumentation propagates through the global propagator, as
	// telemetry.Setup configures it in the binaries.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	tracer := tp.Tracer("test")
	reg := metrics.NewRegistry("test")

	listen := func() net.Listener {
		l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		return l
	}

	peer, clientL := listen(), listen()

	d, err := daemon.Start(t.Context(), daemon.Config{
		Node: node.Config{
			ID:             1,
			DataDir:        "/data",
			FS:             vfs.NewMem(),
			TickInterval:   10 * time.Millisecond,
			InitialCluster: []*kvpb.Member{{Id: 1, PeerUrl: "http://" + peer.Addr().String()}},
			Tracer:         tracer,
		},
		Peer:   peer,
		Client: clientL,
		Logger: slog.New(slog.DiscardHandler),
		ClientServerOptions: []grpc.ServerOption{
			grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tp))),
		},
	})
	if err != nil {
		t.Fatalf("starting the KV node: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = d.Shutdown(ctx)
	})

	kv, err := client.New(client.Config{
		Endpoints: []string{clientL.Addr().String()},
		Logger:    slog.New(slog.DiscardHandler),
		Metrics:   client.NewMetrics(reg),
		DialOptions: []grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(tp))),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = kv.Close() })

	logs := &syncBuffer{}
	log := logger.New(logs, logger.Config{Level: slog.LevelInfo, Format: logger.FormatJSON}, "test")
	fileMetrics := fileio.NewMetrics(reg)
	st := instrumented.New(raftstore.New(kv, raftstore.Options{}), instrumented.NewMetrics(reg), tracer)

	svc := catalog.New(catalog.Options{
		Store:         st,
		Files:         fileio.NewInstrumented(fileio.NewIceberg(context.Background(), nil), fileMetrics, tracer),
		Warehouse:     "mem://observability/" + strings.ReplaceAll(t.Name(), "/", "_"),
		CommitRetries: catalog.DefaultCommitRetries,
		Logger:        log,
		Metrics:       catalog.NewMetrics(reg),
		Tracer:        tracer,
	})

	srv := httptest.NewServer(api.NewHandler(api.Options{
		Logger:         log,
		Ready:          svc,
		Server:         api.NewServer(svc, "main"),
		Metrics:        api.NewMetrics(reg),
		MetricsHandler: metrics.Handler(reg),
		TracerProvider: tp,
	}))
	t.Cleanup(srv.Close)

	return observedStack{url: srv.URL, spans: spans, logs: logs}
}

// spansByTrace groups the names of ended spans by trace.
func (s observedStack) spansByTrace() map[string][]string {
	traces := map[string][]string{}

	for _, span := range s.spans.Ended() {
		id := span.SpanContext().TraceID().String()
		traces[id] = append(traces[id], span.Name())
	}

	return traces
}

func TestCommitIsTracedAndMeasured(t *testing.T) {
	t.Parallel()

	stack := newObservedStack(t)
	ctx := t.Context()
	cat := newClient(t, stack.url)

	if err := cat.CreateNamespace(ctx, table.Identifier{"db"}, nil); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	tbl, err := cat.CreateTable(ctx, table.Identifier{"db", "t"}, dataSchema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tx := tbl.NewTransaction()
	if err := tx.SetProperties(map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// One trace runs from the commit request through the catalog, object
	// storage and the store down to the raft proposal on the node.
	want := []string{
		"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
		"catalog.commit.table",
		"fileio.write",
		"store.CommitTables",
		"echpochmak.kv.v1.KVService/Txn",
		"kv.propose",
	}

	found := false

	for _, names := range stack.spansByTrace() {
		if containsAll(names, want) {
			found = true
		}
	}

	if !found {
		t.Fatalf("no trace holds all of %v; traces: %v", want, stack.spansByTrace())
	}

	body := get(t, stack.url+"/metrics")

	for _, series := range []string{
		`echpochmak_catalog_commits_total{kind="table",outcome="committed"} 1`,
		`echpochmak_http_requests_total{method="POST",` +
			`route="/v1/{prefix}/namespaces/{namespace}/tables/{table}",status="200"} 1`,
		`echpochmak_store_call_duration_seconds_count{op="CreateIntent"}`,
		`echpochmak_catalog_metadata_io_seconds_count{op="write"}`,
		`echpochmak_kv_client_attempts_total{result="ok"}`,
	} {
		if !strings.Contains(body, series) {
			t.Errorf("/metrics lacks %s", series)
		}
	}

	if !strings.Contains(stack.logs.String(), `"trace_id"`) {
		t.Error("request logs carry no trace ID")
	}
}

func containsAll(have, want []string) bool {
	set := map[string]bool{}
	for _, name := range have {
		set[name] = true
	}

	for _, name := range want {
		if !set[name] {
			return false
		}
	}

	return true
}

func get(t *testing.T, url string) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}
