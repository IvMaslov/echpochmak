// Package telemetry sets up OpenTelemetry tracing for a binary from the
// standard OTEL_* environment variables, so nothing project-specific has to
// be configured: OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME,
// OTEL_TRACES_SAMPLER and the rest work as everywhere else.
//
// Tracing is off unless an OTLP endpoint is set or OTEL_TRACES_EXPORTER asks
// for OTLP. Off means a no-op provider, so instrumented code costs nothing.
// See arch/phase5/plan.md (D9).
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"echpochmak/internal/version"
)

// InstrumentationName names the tracers of this project.
const InstrumentationName = "echpochmak"

// Tracing is the configured tracer provider of a binary.
type Tracing struct {
	Provider trace.TracerProvider
	// Enabled reports whether spans are exported.
	Enabled  bool
	shutdown func(context.Context) error
}

// Tracer returns the project's tracer.
func (t Tracing) Tracer() trace.Tracer {
	return t.Provider.Tracer(InstrumentationName)
}

// Shutdown flushes buffered spans and stops the exporter.
func (t Tracing) Shutdown(ctx context.Context) error {
	return t.shutdown(ctx)
}

// Setup configures tracing for service from the environment, read with
// getenv, and installs the provider and the W3C trace context propagator
// as OpenTelemetry's globals, which the HTTP and gRPC instrumentation use.
func Setup(ctx context.Context, service string, getenv func(string) string) (Tracing, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if !enabled(getenv) {
		tp := noop.NewTracerProvider()
		otel.SetTracerProvider(tp)

		return Tracing{Provider: tp, shutdown: func(context.Context) error { return nil }}, nil
	}

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return Tracing{}, fmt.Errorf("creating the OTLP trace exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.NewSchemaless(
			semconv.ServiceName(service),
			semconv.ServiceVersion(version.Get().Version),
		),
		resource.Environment(), // OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES win.
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return Tracing{}, fmt.Errorf("building the trace resource: %w", err)
	}

	// The sampler comes from OTEL_TRACES_SAMPLER; the SDK's default is
	// parent-based always-on, so a sampled caller is always followed.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return Tracing{Provider: tp, Enabled: true, shutdown: tp.Shutdown}, nil
}

// enabled reports whether the environment asks for exported traces.
func enabled(getenv func(string) string) bool {
	if strings.EqualFold(getenv("OTEL_SDK_DISABLED"), "true") {
		return false
	}

	switch strings.ToLower(getenv("OTEL_TRACES_EXPORTER")) {
	case "none":
		return false
	case "otlp":
		return true
	}

	return getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}
