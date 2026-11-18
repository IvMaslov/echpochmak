// Package instrumented decorates a [store.Store] with metrics and traces:
// every call is timed and counted by operation and error class, and gets a
// span. The decorator works for any implementation, so memstore and
// raftstore stay free of instrumentation, and the contract suite covers the
// wrapped store too. See arch/phase5/plan.md (D8, D9).
package instrumented

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"echpochmak/internal/metrics"
	"echpochmak/internal/store"
)

// Metrics are the store call metrics. A nil *Metrics records nothing.
type Metrics struct {
	duration *prometheus.HistogramVec
	errors   *prometheus.CounterVec
}

// NewMetrics registers the store call metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "store_call_duration_seconds",
			Help:      "Duration of store calls, by operation.",
			Buckets:   metrics.FastBuckets(),
		}, []string{"op"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "store_call_errors_total",
			Help:      "Store calls that returned an error, by operation and error class.",
		}, []string{"op", "error"}),
	}

	reg.MustRegister(m.duration, m.errors)

	return m
}

// Store wraps a store.Store with metrics and traces.
type Store struct {
	next    store.Store
	metrics *Metrics
	tracer  trace.Tracer
}

var _ store.Store = (*Store)(nil)

// New wraps next. A nil tracer traces nothing; nil metrics record nothing.
func New(next store.Store, m *Metrics, tracer trace.Tracer) *Store {
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("")
	}

	return &Store{next: next, metrics: m, tracer: tracer}
}

// call runs fn as the store operation op, timed, counted and traced.
func call[T any](ctx context.Context, s *Store, op string, fn func(context.Context) (T, error)) (T, error) {
	ctx, span := s.tracer.Start(ctx, "store."+op, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	v, err := fn(ctx)
	class := ErrorClass(err)

	if s.metrics != nil {
		s.metrics.duration.WithLabelValues(op).Observe(time.Since(start).Seconds())

		if err != nil {
			s.metrics.errors.WithLabelValues(op, class).Inc()
		}
	}

	if err != nil {
		span.SetAttributes(attribute.String("store.error", class))

		if isFailure(class) {
			span.SetStatus(codes.Error, err.Error())
		}
	}

	return v, err
}

// callErr is call for operations that only return an error.
func callErr(ctx context.Context, s *Store, op string, fn func(context.Context) error) error {
	_, err := call(ctx, s, op, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})

	return err
}

// ErrorClass names the kind of a store error, for metric labels: a bounded
// set of values.
func ErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, store.ErrOutcomeUnknown):
		return "outcome_unknown"
	case errors.Is(err, store.ErrUnavailable):
		return "unavailable"
	case errors.Is(err, store.ErrCommitConflict):
		return "conflict"
	case errors.Is(err, store.ErrIntentChanged), errors.Is(err, store.ErrPurgeTaskChanged):
		return "changed"
	case errors.Is(err, store.ErrNamespaceNotFound),
		errors.Is(err, store.ErrTableNotFound),
		errors.Is(err, store.ErrViewNotFound):
		return "not_found"
	case errors.Is(err, store.ErrNamespaceAlreadyExists),
		errors.Is(err, store.ErrTableAlreadyExists),
		errors.Is(err, store.ErrViewAlreadyExists):
		return "already_exists"
	case errors.Is(err, store.ErrNamespaceNotEmpty):
		return "not_empty"
	case errors.Is(err, store.ErrInvalidArgument):
		return "invalid_argument"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "other"
	}
}

// isFailure reports whether an error class means the store failed, rather
// than answered with a normal outcome such as "not found".
func isFailure(class string) bool {
	return class == "outcome_unknown" || class == "unavailable" || class == "other"
}

// Ping implements [store.Store]. Readiness probes call it every second, so
// it is timed but not traced.
func (s *Store) Ping(ctx context.Context) error {
	start := time.Now()
	err := s.next.Ping(ctx)

	if s.metrics != nil {
		s.metrics.duration.WithLabelValues("Ping").Observe(time.Since(start).Seconds())

		if err != nil {
			s.metrics.errors.WithLabelValues("Ping", ErrorClass(err)).Inc()
		}
	}

	return err
}

// CreateNamespace implements [store.Store].
func (s *Store) CreateNamespace(ctx context.Context, ns store.Namespace, props map[string]string) error {
	return callErr(ctx, s, "CreateNamespace", func(ctx context.Context) error {
		return s.next.CreateNamespace(ctx, ns, props)
	})
}

// GetNamespace implements [store.Store].
func (s *Store) GetNamespace(ctx context.Context, ns store.Namespace) (store.NamespaceRecord, error) {
	return call(ctx, s, "GetNamespace", func(ctx context.Context) (store.NamespaceRecord, error) {
		return s.next.GetNamespace(ctx, ns)
	})
}

// ListNamespaces implements [store.Store].
func (s *Store) ListNamespaces(
	ctx context.Context,
	parent store.Namespace,
	req store.PageRequest,
) (store.Page[store.Namespace], error) {
	return call(ctx, s, "ListNamespaces", func(ctx context.Context) (store.Page[store.Namespace], error) {
		return s.next.ListNamespaces(ctx, parent, req)
	})
}

// UpdateNamespaceProperties implements [store.Store].
func (s *Store) UpdateNamespaceProperties(
	ctx context.Context,
	ns store.Namespace,
	upd store.PropertiesUpdate,
) (store.PropertiesUpdateResult, error) {
	return call(ctx, s, "UpdateNamespaceProperties", func(ctx context.Context) (store.PropertiesUpdateResult, error) {
		return s.next.UpdateNamespaceProperties(ctx, ns, upd)
	})
}

// DropNamespace implements [store.Store].
func (s *Store) DropNamespace(ctx context.Context, ns store.Namespace) error {
	return callErr(ctx, s, "DropNamespace", func(ctx context.Context) error {
		return s.next.DropNamespace(ctx, ns)
	})
}

// GetTable implements [store.Store].
func (s *Store) GetTable(ctx context.Context, id store.Ident) (store.Pointer, error) {
	return call(ctx, s, "GetTable", func(ctx context.Context) (store.Pointer, error) {
		return s.next.GetTable(ctx, id)
	})
}

// GetTables implements [store.Store].
func (s *Store) GetTables(ctx context.Context, ids []store.Ident) ([]store.Pointer, error) {
	return call(ctx, s, "GetTables", func(ctx context.Context) ([]store.Pointer, error) {
		return s.next.GetTables(ctx, ids)
	})
}

// ListTables implements [store.Store].
func (s *Store) ListTables(
	ctx context.Context,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	return call(ctx, s, "ListTables", func(ctx context.Context) (store.Page[store.Ident], error) {
		return s.next.ListTables(ctx, ns, req)
	})
}

// CommitTables implements [store.Store].
func (s *Store) CommitTables(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
	return call(ctx, s, "CommitTables", func(ctx context.Context) ([]store.Pointer, error) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int("store.commits", len(commits)))

		return s.next.CommitTables(ctx, commits)
	})
}

// RenameTable implements [store.Store].
func (s *Store) RenameTable(ctx context.Context, from, to store.Ident) error {
	return callErr(ctx, s, "RenameTable", func(ctx context.Context) error {
		return s.next.RenameTable(ctx, from, to)
	})
}

// DropTable implements [store.Store].
func (s *Store) DropTable(ctx context.Context, id store.Ident, opts store.DropOptions) (store.Pointer, error) {
	return call(ctx, s, "DropTable", func(ctx context.Context) (store.Pointer, error) {
		return s.next.DropTable(ctx, id, opts)
	})
}

// GetView implements [store.Store].
func (s *Store) GetView(ctx context.Context, id store.Ident) (store.Pointer, error) {
	return call(ctx, s, "GetView", func(ctx context.Context) (store.Pointer, error) {
		return s.next.GetView(ctx, id)
	})
}

// ListViews implements [store.Store].
func (s *Store) ListViews(
	ctx context.Context,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	return call(ctx, s, "ListViews", func(ctx context.Context) (store.Page[store.Ident], error) {
		return s.next.ListViews(ctx, ns, req)
	})
}

// CommitView implements [store.Store].
func (s *Store) CommitView(ctx context.Context, commit store.Commit) (store.Pointer, error) {
	return call(ctx, s, "CommitView", func(ctx context.Context) (store.Pointer, error) {
		return s.next.CommitView(ctx, commit)
	})
}

// RenameView implements [store.Store].
func (s *Store) RenameView(ctx context.Context, from, to store.Ident) error {
	return callErr(ctx, s, "RenameView", func(ctx context.Context) error {
		return s.next.RenameView(ctx, from, to)
	})
}

// DropView implements [store.Store].
func (s *Store) DropView(ctx context.Context, id store.Ident) error {
	return callErr(ctx, s, "DropView", func(ctx context.Context) error {
		return s.next.DropView(ctx, id)
	})
}

// ListPurgeTasks implements [store.Store].
func (s *Store) ListPurgeTasks(ctx context.Context, limit int) ([]store.PurgeTask, error) {
	return call(ctx, s, "ListPurgeTasks", func(ctx context.Context) ([]store.PurgeTask, error) {
		return s.next.ListPurgeTasks(ctx, limit)
	})
}

// ClaimPurgeTask implements [store.Store].
func (s *Store) ClaimPurgeTask(
	ctx context.Context,
	task store.PurgeTask,
	claim store.PurgeClaim,
) (store.PurgeTask, error) {
	return call(ctx, s, "ClaimPurgeTask", func(ctx context.Context) (store.PurgeTask, error) {
		return s.next.ClaimPurgeTask(ctx, task, claim)
	})
}

// CompletePurgeTask implements [store.Store].
func (s *Store) CompletePurgeTask(ctx context.Context, task store.PurgeTask) error {
	return callErr(ctx, s, "CompletePurgeTask", func(ctx context.Context) error {
		return s.next.CompletePurgeTask(ctx, task)
	})
}

// CreateIntent implements [store.Store].
func (s *Store) CreateIntent(ctx context.Context, intent store.Intent) (store.Intent, error) {
	return call(ctx, s, "CreateIntent", func(ctx context.Context) (store.Intent, error) {
		return s.next.CreateIntent(ctx, intent)
	})
}

// ListIntents implements [store.Store].
func (s *Store) ListIntents(ctx context.Context, limit int) ([]store.Intent, error) {
	return call(ctx, s, "ListIntents", func(ctx context.Context) ([]store.Intent, error) {
		return s.next.ListIntents(ctx, limit)
	})
}

// CondemnIntent implements [store.Store].
func (s *Store) CondemnIntent(ctx context.Context, ref store.IntentRef) (store.Intent, error) {
	return call(ctx, s, "CondemnIntent", func(ctx context.Context) (store.Intent, error) {
		return s.next.CondemnIntent(ctx, ref)
	})
}

// ReleaseIntent implements [store.Store].
func (s *Store) ReleaseIntent(ctx context.Context, ref store.IntentRef) error {
	return callErr(ctx, s, "ReleaseIntent", func(ctx context.Context) error {
		return s.next.ReleaseIntent(ctx, ref)
	})
}
