package catalog

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"echpochmak/internal/metrics"
	"echpochmak/internal/store"
)

// Kinds of commit, a label of the commit metrics.
const (
	kindTable       = "table"
	kindTransaction = "transaction"
	kindView        = "view"
	kindRegister    = "register"
)

// labelKind labels commit metrics with the kind of commit.
const labelKind = "kind"

// Metrics are the catalog's metrics: commits, background workers and file
// cleanup. A nil *Metrics records nothing.
type Metrics struct {
	commits        *prometheus.CounterVec
	commitDuration *prometheus.HistogramVec
	commitAttempts *prometheus.HistogramVec
	deleteFailures prometheus.Counter

	purgesDone       prometheus.Counter
	purgeFiles       prometheus.Counter
	purgeFailures    prometheus.Counter
	purgeOldest      prometheus.Gauge
	orphansPending   prometheus.Gauge
	orphansOldest    prometheus.Gauge
	orphanFiles      prometheus.Counter
	orphanFailures   prometheus.Counter
	orphanIntentsRun prometheus.Counter
}

// NewMetrics registers the catalog's metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: metrics.Namespace, Name: name, Help: help})
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metrics.Namespace, Name: name, Help: help})
	}

	m := &Metrics{
		commits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "catalog_commits_total",
			Help:      "Commits, by kind and outcome.",
		}, []string{labelKind, "outcome"}),
		commitDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "catalog_commit_duration_seconds",
			Help:      "Duration of commits, retries included, by kind.",
			Buckets:   metrics.SlowBuckets(),
		}, []string{labelKind}),
		commitAttempts: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "catalog_commit_attempts",
			Help:      "Attempts per commit, by kind; more than one means lost races.",
			Buckets:   []float64{1, 2, 3, 5, 8, 13},
		}, []string{labelKind}),
		deleteFailures: counter("catalog_expired_metadata_delete_failures_total",
			"Metadata files the catalog failed to delete after they were replaced or expired."),
		purgesDone:    counter("purge_tasks_completed_total", "Purge tasks completed by this replica."),
		purgeFiles:    counter("purge_files_deleted_total", "Files deleted by purges on this replica."),
		purgeFailures: counter("purge_failures_total", "Purge task runs that failed and will be retried."),
		purgeOldest: gauge("purge_queue_oldest_seconds",
			"Age of the oldest queued purge task at the last scan; 0 when the queue is empty."),
		orphansPending: gauge("orphan_intents_pending",
			"Commit intents seen by the last janitor sweep: in flight or abandoned."),
		orphansOldest: gauge("orphan_intents_oldest_seconds",
			"Age of the oldest commit intent at the last sweep; 0 when there is none."),
		orphanFiles: counter("orphan_files_deleted_total",
			"Metadata files of commits that never landed, deleted."),
		orphanFailures: counter("orphan_delete_failures_total",
			"Abandoned intents the janitor could not clean up yet."),
		orphanIntentsRun: counter("orphan_intents_cleaned_total", "Abandoned commit intents cleaned up."),
	}

	reg.MustRegister(
		m.commits, m.commitDuration, m.commitAttempts, m.deleteFailures,
		m.purgesDone, m.purgeFiles, m.purgeFailures, m.purgeOldest,
		m.orphansPending, m.orphansOldest, m.orphanFiles, m.orphanFailures, m.orphanIntentsRun,
	)

	return m
}

// commit records a commit of kind that ended with err after attempts.
func (m *Metrics) commit(kind string, attempts int, d time.Duration, err error) {
	if m == nil {
		return
	}

	m.commits.WithLabelValues(kind, commitOutcome(err)).Inc()
	m.commitDuration.WithLabelValues(kind).Observe(d.Seconds())
	m.commitAttempts.WithLabelValues(kind).Observe(float64(attempts))
}

// commitOutcome names how a commit ended.
func commitOutcome(err error) string {
	switch {
	case err == nil:
		return "committed"
	case errors.Is(err, ErrCommitStateUnknown):
		return "state_unknown"
	case errors.Is(err, ErrRequirementFailed):
		return "requirement_failed"
	case errors.Is(err, store.ErrCommitConflict):
		return "conflict"
	case errors.Is(err, store.ErrUnavailable):
		return "unavailable"
	default:
		return "rejected"
	}
}

func (m *Metrics) deleteFailed() {
	if m != nil {
		m.deleteFailures.Inc()
	}
}

// ObserveSweep records a janitor sweep; pass it as [JanitorOptions.OnSweep].
func (m *Metrics) ObserveSweep(res SweepResult) {
	if m == nil {
		return
	}

	m.orphansPending.Set(float64(res.Pending))
	m.orphansOldest.Set(age(res.Oldest))
	m.orphanFiles.Add(float64(res.FilesDeleted))
	m.orphanFailures.Add(float64(res.Failed))
	m.orphanIntentsRun.Add(float64(res.Cleaned))
}

func (m *Metrics) purgeScanned(oldest time.Time) {
	if m != nil {
		m.purgeOldest.Set(age(oldest))
	}
}

func (m *Metrics) purged(files int, err error) {
	if m == nil {
		return
	}

	if err != nil {
		m.purgeFailures.Inc()

		return
	}

	m.purgesDone.Inc()
	m.purgeFiles.Add(float64(files))
}

// age returns the seconds since t, or 0 for a zero t.
func age(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}

	return time.Since(t).Seconds()
}
