package fileio

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"echpochmak/internal/metrics"
)

// Metrics are the metadata IO metrics. A nil *Metrics records nothing.
type Metrics struct {
	duration *prometheus.HistogramVec
	errors   *prometheus.CounterVec
	cache    *prometheus.CounterVec
}

// NewMetrics registers the metadata IO metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "catalog_metadata_io_seconds",
			Help:      "Duration of metadata file reads, writes and deletes in object storage, by operation.",
			Buckets:   metrics.SlowBuckets(),
		}, []string{"op"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "catalog_metadata_io_errors_total",
			Help:      "Failed metadata file operations, by operation.",
		}, []string{"op"}),
		cache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "catalog_metadata_cache_requests_total",
			Help:      "Metadata file reads, by whether the cache had the file.",
		}, []string{"result"}),
	}

	reg.MustRegister(m.duration, m.errors, m.cache)

	return m
}

// ObserveLookup counts a cache lookup; pass it to [WithLookupObserver].
func (m *Metrics) ObserveLookup(hit bool) {
	if m == nil {
		return
	}

	result := "miss"
	if hit {
		result = "hit"
	}

	m.cache.WithLabelValues(result).Inc()
}

// Instrumented decorates a FileIO with metrics and traces.
type Instrumented struct {
	next    FileIO
	metrics *Metrics
	tracer  trace.Tracer
}

var _ FileIO = (*Instrumented)(nil)

// NewInstrumented wraps next. A nil tracer traces nothing; nil metrics record
// nothing.
func NewInstrumented(next FileIO, m *Metrics, tracer trace.Tracer) *Instrumented {
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("")
	}

	return &Instrumented{next: next, metrics: m, tracer: tracer}
}

// Read implements [FileIO].
func (f *Instrumented) Read(ctx context.Context, location string) ([]byte, error) {
	var data []byte

	err := f.do(ctx, "read", location, func(ctx context.Context) error {
		var err error

		data, err = f.next.Read(ctx, location)

		return err
	})

	return data, err
}

// Write implements [FileIO].
func (f *Instrumented) Write(ctx context.Context, location string, data []byte) error {
	return f.do(ctx, "write", location, func(ctx context.Context) error {
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int("fileio.bytes", len(data)))

		return f.next.Write(ctx, location, data)
	})
}

// Delete implements [FileIO].
func (f *Instrumented) Delete(ctx context.Context, location string) error {
	return f.do(ctx, "delete", location, func(ctx context.Context) error {
		return f.next.Delete(ctx, location)
	})
}

func (f *Instrumented) do(ctx context.Context, op, location string, fn func(context.Context) error) error {
	ctx, span := f.tracer.Start(ctx, "fileio."+op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("fileio.location", location)),
	)
	defer span.End()

	start := time.Now()
	err := fn(ctx)

	if f.metrics != nil {
		f.metrics.duration.WithLabelValues(op).Observe(time.Since(start).Seconds())

		if err != nil {
			f.metrics.errors.WithLabelValues(op).Inc()
		}
	}

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
