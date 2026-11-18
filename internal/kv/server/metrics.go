package server

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"echpochmak/internal/kv/node"
	"echpochmak/internal/metrics"
)

// GRPCMetrics counts and times the client API's calls by method and code.
type GRPCMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewGRPCMetrics registers the client API's metrics with reg.
func NewGRPCMetrics(reg prometheus.Registerer) *GRPCMetrics {
	m := &GRPCMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "kv_grpc_requests_total",
			Help:      "Calls to the client API, by method and gRPC code.",
		}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "kv_grpc_duration_seconds",
			Help:      "Duration of calls to the client API, by method.",
			Buckets:   metrics.FastBuckets(),
		}, []string{"method"}),
	}

	reg.MustRegister(m.requests, m.duration)

	return m
}

func (m *GRPCMetrics) record(method string, start time.Time, err error) {
	m.requests.WithLabelValues(method, status.Code(err).String()).Inc()
	m.duration.WithLabelValues(method).Observe(time.Since(start).Seconds())
}

// UnaryInterceptor records unary calls.
func (m *GRPCMetrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		m.record(info.FullMethod, start, err)

		return resp, err
	}
}

// StreamInterceptor records streaming calls.
func (m *GRPCMetrics) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		m.record(info.FullMethod, start, err)

		return err
	}
}

// Ready reports whether n can serve: it knows a leader and has applied
// nearly everything committed. The health service and /readyz share it.
func Ready(n *node.Node, maxLag uint64) bool {
	st := n.Status()

	return st.Leader != 0 && st.Applied+maxLag >= st.Commit
}
