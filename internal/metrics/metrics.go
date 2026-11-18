// Package metrics holds what every binary's Prometheus metrics share: the
// namespace, a registry with the Go runtime, process and build information
// collectors, and the HTTP handler that serves it.
//
// There is no global registry. Each binary builds one in main and passes it
// to the packages that export metrics; each of those takes its metrics as an
// optional dependency that does nothing when nil, so tests need none. See
// arch/phase5/plan.md (D8).
package metrics

import (
	"net/http"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"echpochmak/internal/version"
)

// Namespace prefixes every metric name.
const Namespace = "echpochmak"

// NewRegistry returns a registry with the Go runtime and process collectors,
// and echpochmak_build_info for the named component.
func NewRegistry(component string) *prometheus.Registry {
	reg := prometheus.NewRegistry()

	info := version.Get()
	build := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "build_info",
		Help:      "Build information; always 1.",
		ConstLabels: prometheus.Labels{
			"component":  component,
			"version":    info.Version,
			"revision":   info.Commit,
			"go_version": runtime.Version(),
		},
	})
	build.Set(1)

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		build,
	)

	return reg
}

// Handler serves the metrics of reg in the Prometheus text format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// FastBuckets returns histogram buckets for KV reads and writes: 100 µs to
// 2.5 s.
func FastBuckets() []float64 {
	return []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}
}

// SlowBuckets returns histogram buckets for requests and commits: 1 ms to
// 60 s.
func SlowBuckets() []float64 {
	return []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
}
