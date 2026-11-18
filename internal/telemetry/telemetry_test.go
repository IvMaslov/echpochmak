package telemetry_test

import (
	"testing"

	"echpochmak/internal/telemetry"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestTracingIsOffByDefault(t *testing.T) {
	t.Parallel()

	tracing, err := telemetry.Setup(t.Context(), "test", env(nil))
	if err != nil || tracing.Enabled {
		t.Fatalf("Setup without OTEL_* = enabled %v, %v; want off", tracing.Enabled, err)
	}

	_, span := tracing.Tracer().Start(t.Context(), "x")
	if span.SpanContext().IsValid() {
		t.Fatal("a disabled tracer produced a real span")
	}

	span.End()
}

func TestTracingIsOnWithAnEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vars map[string]string
		want bool
	}{
		{vars: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4317"}, want: true},
		{vars: map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}, want: true},
		{vars: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://j:4317", "OTEL_SDK_DISABLED": "true"}},
		{vars: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://j:4317", "OTEL_TRACES_EXPORTER": "none"}},
	}

	for _, tt := range tests {
		tracing, err := telemetry.Setup(t.Context(), "test", env(tt.vars))
		if err != nil || tracing.Enabled != tt.want {
			t.Errorf("Setup(%v) = enabled %v, %v; want %v", tt.vars, tracing.Enabled, err, tt.want)
		}

		_ = tracing.Shutdown(t.Context())
	}
}
