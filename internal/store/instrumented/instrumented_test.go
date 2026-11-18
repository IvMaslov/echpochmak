package instrumented_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"echpochmak/internal/store"
	"echpochmak/internal/store/instrumented"
	"echpochmak/internal/store/memstore"
	"echpochmak/internal/store/storetest"
)

// The decorator must not change the store's semantics.
func TestContract(t *testing.T) {
	t.Parallel()

	storetest.Run(t, func(*testing.T) store.Store {
		return instrumented.New(memstore.New(), instrumented.NewMetrics(prometheus.NewRegistry()), nil)
	})
}

func TestMetricsAndSpans(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	spans := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)).Tracer("test")
	s := instrumented.New(memstore.New(), instrumented.NewMetrics(reg), tracer)

	if err := s.CreateNamespace(t.Context(), store.Namespace{"db"}, nil); err != nil {
		t.Fatal(err)
	}

	_, err := s.GetTable(t.Context(), store.Ident{Namespace: store.Namespace{"db"}, Name: "missing"})
	if !errors.Is(err, store.ErrTableNotFound) {
		t.Fatalf("GetTable = %v, want ErrTableNotFound", err)
	}

	if err := s.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	if n := testutil.CollectAndCount(reg, "echpochmak_store_call_duration_seconds"); n != 3 {
		t.Errorf("%d duration series, want 3 (CreateNamespace, GetTable, Ping)", n)
	}

	want := `
# HELP echpochmak_store_call_errors_total Store calls that returned an error, by operation and error class.
# TYPE echpochmak_store_call_errors_total counter
echpochmak_store_call_errors_total{error="not_found",op="GetTable"} 1
`
	err = testutil.CollectAndCompare(reg, strings.NewReader(want), "echpochmak_store_call_errors_total")
	if err != nil {
		t.Error(err)
	}

	ended := spans.Ended()
	names := make([]string, 0, len(ended))

	for _, span := range ended {
		names = append(names, span.Name())
	}

	if len(names) != 2 || names[0] != "store.CreateNamespace" || names[1] != "store.GetTable" {
		t.Errorf("spans %v, want CreateNamespace and GetTable, and none for Ping", names)
	}
}

func TestErrorClass(t *testing.T) {
	t.Parallel()

	tests := map[error]string{
		nil:                                   "",
		store.OutcomeUnknown(errors.New("x")): "outcome_unknown",
		store.ErrUnavailable:                  "unavailable",
		&store.CommitConflictError{}:          "conflict",
		store.ErrIntentChanged:                "changed",
		store.ErrNamespaceNotEmpty:            "not_empty",
		errors.New("disk on fire"):            "other",
	}

	for err, want := range tests {
		if got := instrumented.ErrorClass(err); got != want {
			t.Errorf("ErrorClass(%v) = %q, want %q", err, got, want)
		}
	}
}
