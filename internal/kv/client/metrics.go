package client

import (
	"github.com/prometheus/client_golang/prometheus"

	"echpochmak/internal/metrics"
)

// Attempt results counted by [Metrics].
const (
	attemptOK      = "ok"
	attemptRetried = "retried"
	attemptUnknown = "unknown"
	attemptFailed  = "failed"
)

// Metrics count the client's attempts. A nil *Metrics records nothing.
type Metrics struct {
	attempts *prometheus.CounterVec
}

// NewMetrics registers the client's metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "kv_client_attempts_total",
			Help: "Attempts of KV calls on one node, by result: ok; retried on another node; " +
				"unknown outcome; failed for good.",
		}, []string{"result"}),
	}

	reg.MustRegister(m.attempts)

	return m
}

func (m *Metrics) attempt(result string) {
	if m != nil {
		m.attempts.WithLabelValues(result).Inc()
	}
}
