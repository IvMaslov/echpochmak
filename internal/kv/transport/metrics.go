package transport

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"echpochmak/internal/metrics"
)

// Metrics are the transport's counters. A nil *Metrics records nothing.
type Metrics struct {
	dropped   *prometheus.CounterVec
	snapshots *prometheus.CounterVec
}

// NewMetrics registers the transport's counters with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "kv_transport_dropped_total",
			Help:      "Raft messages dropped because a peer's queue was full.",
		}, []string{"peer"}),
		snapshots: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "kv_snapshots_sent_total",
			Help:      "Snapshots sent to followers that fell behind, by result.",
		}, []string{"result"}),
	}

	reg.MustRegister(m.dropped, m.snapshots)

	return m
}

func (m *Metrics) droppedMessage(peer uint64) {
	if m != nil {
		m.dropped.WithLabelValues(strconv.FormatUint(peer, 10)).Inc()
	}
}

func (m *Metrics) snapshotSent(err error) {
	if m == nil {
		return
	}

	result := "ok"
	if err != nil {
		result = "failed"
	}

	m.snapshots.WithLabelValues(result).Inc()
}

// queueCollector reports the length of every peer's queue on scrape.
type queueCollector struct {
	t    *Transport
	desc *prometheus.Desc
}

// Collector returns a collector of the peers' queue lengths.
func (t *Transport) Collector() prometheus.Collector {
	return &queueCollector{
		t: t,
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(metrics.Namespace, "", "kv_transport_queue_length"),
			"Raft messages waiting to be sent to a peer.",
			[]string{"peer"},
			nil,
		),
	}
}

// Describe implements [prometheus.Collector].
func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements [prometheus.Collector].
func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	c.t.mu.RLock()
	defer c.t.mu.RUnlock()

	for id, p := range c.t.peers {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(len(p.queue)),
			strconv.FormatUint(id, 10))
	}
}
