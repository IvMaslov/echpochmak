package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"echpochmak/internal/logger"
	"echpochmak/internal/metrics"
)

// unhealthyAfter is how many intervals without a successful backup make the
// service report itself unhealthy.
const unhealthyAfter = 3

// loopMetrics are the service's metrics; see arch/phase5/plan.md (D8).
type loopMetrics struct {
	lastSuccess  prometheus.Gauge
	lastRevision prometheus.Gauge
	lastBytes    prometheus.Gauge
	duration     prometheus.Histogram
	failures     *prometheus.CounterVec
	skipped      prometheus.Counter
	verified     prometheus.Gauge
	retained     prometheus.Gauge
}

func newLoopMetrics(reg prometheus.Registerer) *loopMetrics {
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metrics.Namespace, Name: name, Help: help})
		reg.MustRegister(g)

		return g
	}

	m := &loopMetrics{
		lastSuccess: gauge("backup_last_success_timestamp_seconds",
			"When the last backup succeeded, or found the latest one current."),
		lastRevision: gauge("backup_last_revision", "KV revision of the latest backup."),
		lastBytes:    gauge("backup_bytes", "Size of the latest backup uploaded."),
		verified: gauge("backup_verify_last_success_timestamp_seconds",
			"When a test restore of the newest backup last succeeded."),
		retained: gauge("backup_retained", "Complete backups of the cluster kept by retention."),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "backup_duration_seconds",
			Help:      "Time to take and store one backup.",
			Buckets:   metrics.SlowBuckets(),
		}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "backup_failures_total",
			Help:      "Failed backup steps.",
		}, []string{"stage"}),
		skipped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "backup_skipped_total",
			Help:      "Backups skipped because the cluster had not changed.",
		}),
	}

	reg.MustRegister(m.duration, m.failures, m.skipped)

	return m
}

// runLoop is the service: back up, expire, verify, until ctx is done.
func (c *command) runLoop(ctx context.Context) error {
	reg := metrics.NewRegistry(serviceName)
	mx := newLoopMetrics(reg)

	var lastSuccess atomic.Int64

	started := time.Now()

	srv, err := c.serveMetrics(ctx, reg, func() bool {
		limit := unhealthyAfter * c.cfg.Interval
		last := time.Unix(0, lastSuccess.Load())

		return time.Since(started) < limit || time.Since(last) < limit
	})
	if err != nil {
		return err
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutdownCtx)
	}()

	var lastVerify time.Time

	for {
		if c.backupOnce(ctx, mx) {
			lastSuccess.Store(time.Now().UnixNano())
		}

		if c.cfg.VerifyInterval > 0 && time.Since(lastVerify) >= c.cfg.VerifyInterval {
			if c.verifyNewest(ctx, mx) {
				lastVerify = time.Now()
			}
		}

		t := time.NewTimer(jitter(c.cfg.Interval))

		select {
		case <-ctx.Done():
			t.Stop()

			return nil
		case <-t.C:
		}
	}
}

// backupOnce takes a backup and applies retention, and reports whether the
// backup succeeded.
func (c *command) backupOnce(ctx context.Context, mx *loopMetrics) bool {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	start := time.Now()

	res, err := c.m.Backup(ctx)
	if err != nil {
		mx.failures.WithLabelValues("backup").Inc()

		if ctx.Err() == nil || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.log.WarnContext(ctx, "backup failed", logger.Err(err))
		}

		return false
	}

	mx.lastSuccess.SetToCurrentTime()
	mx.lastRevision.Set(float64(res.Manifest.Revision))

	if res.Skipped {
		mx.skipped.Inc()
		c.log.DebugContext(ctx, "backup skipped: the cluster has not changed",
			slog.Int64("revision", res.Manifest.Revision))

		return true
	}

	mx.duration.Observe(time.Since(start).Seconds())
	mx.lastBytes.Set(float64(res.Manifest.Size))

	c.log.InfoContext(ctx, "backup stored",
		slog.String(logger.KeyLocation, res.Manifest.Snapshot),
		slog.Int64("revision", res.Manifest.Revision),
		slog.Int64("bytes", res.Manifest.Size),
		slog.String("endpoint", res.Endpoint),
		slog.Duration(logger.KeyDuration, time.Since(start)),
	)

	kept, err := c.m.Retain(ctx, res.Manifest.ClusterID)
	if err != nil {
		mx.failures.WithLabelValues("retention").Inc()
		c.log.WarnContext(ctx, "retention failed", logger.Err(err))

		return true
	}

	mx.retained.Set(float64(kept.Retained))

	return true
}

// verifyNewest test-restores the newest backup and reports whether it could.
func (c *command) verifyNewest(ctx context.Context, mx *loopMetrics) bool {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	manifest, err := c.m.Find(ctx, time.Time{})
	if err == nil {
		err = c.m.Verify(ctx, manifest)
	}

	if err != nil {
		mx.failures.WithLabelValues("verify").Inc()
		c.log.ErrorContext(ctx, "backup verification failed", logger.Err(err))

		return false
	}

	mx.verified.SetToCurrentTime()
	c.log.InfoContext(ctx, "backup verified", slog.String(logger.KeyLocation, manifest.Snapshot))

	return true
}

// serveMetrics serves /metrics and /healthz until shut down.
func (c *command) serveMetrics(
	ctx context.Context,
	reg *prometheus.Registry,
	healthy func() bool,
) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler(reg))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !healthy() {
			http.Error(w, "no successful backup recently", http.StatusServiceUnavailable)

			return
		}

		_, _ = w.Write([]byte("ok\n"))
	})

	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", c.cfg.ListenMetrics)
	if err != nil {
		return nil, err //nolint:wrapcheck // the address is in the error.
	}

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.log.ErrorContext(ctx, "metrics server failed", logger.Err(err))
		}
	}()

	return srv, nil
}

// jitter returns d scaled by a random factor in [0.9, 1.1).
func jitter(d time.Duration) time.Duration {
	spread := rand.Int64N(int64(d)/5 + 1) //nolint:gosec // G404: jitter needs no cryptographic randomness.

	return d*9/10 + time.Duration(spread)
}

// probe checks the health endpoint of a running service, so a container
// image without a shell or curl can have a health check.
func (c *command) probe(ctx context.Context) error {
	addr := c.cfg.ListenMetrics
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/healthz", http.NoBody)
	if err != nil {
		return fmt.Errorf("building the probe: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("probing %s: %w", addr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: %s", resp.Status)
	}

	return nil
}
