package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"echpochmak/internal/metrics"
)

// pathMetrics serves Prometheus metrics, outside the Iceberg API.
const pathMetrics = "/metrics"

// Idempotency outcomes, recorded per request.
const (
	idemFirst       = "first"
	idemReplayed    = "replayed"
	idemMismatch    = "mismatch"
	idemBusy        = "busy"
	idemUnavailable = "unavailable"
)

// Metrics are the HTTP layer's metrics. A nil *Metrics records nothing.
type Metrics struct {
	requests    *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	idempotency *prometheus.CounterVec
	ready       prometheus.Gauge
}

// NewMetrics registers the HTTP layer's metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "http_requests_total",
			Help:      "HTTP requests, by route template, method and status.",
		}, []string{"route", "method", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "http_request_duration_seconds",
			Help:      "Duration of HTTP requests, by route template and method.",
			Buckets:   metrics.SlowBuckets(),
		}, []string{"route", "method"}),
		idempotency: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "idempotency_requests_total",
			Help:      "Requests with an Idempotency-Key, by what the protocol decided.",
		}, []string{"result"}),
		ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Name:      "catalog_ready",
			Help:      "1 if the last readiness probe succeeded, 0 if it failed.",
		}),
	}

	reg.MustRegister(m.requests, m.duration, m.idempotency, m.ready)

	return m
}

// observe records a served request. pattern is the mux pattern, which
// includes the method; the label keeps the path template only.
func (m *Metrics) observe(r *http.Request, pattern string, status int, d time.Duration, idem string) {
	if m == nil || pattern == pathMetrics {
		return
	}

	route := pattern
	if _, path, ok := strings.Cut(pattern, " "); ok {
		route = path
	}

	m.requests.WithLabelValues(route, r.Method, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(route, r.Method).Observe(d.Seconds())

	if idem != "" {
		m.idempotency.WithLabelValues(idem).Inc()
	}
}

func (m *Metrics) setReady(ready bool) {
	if m == nil {
		return
	}

	v := 0.0
	if ready {
		v = 1
	}

	m.ready.Set(v)
}
