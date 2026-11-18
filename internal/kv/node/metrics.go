package node

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"echpochmak/internal/metrics"
)

// Proposal results counted by [Metrics].
const (
	proposalApplied  = "applied"
	proposalDropped  = "dropped"
	proposalTimedOut = "timed_out"
	proposalInvalid  = "invalid"
)

// Metrics are a node's event counters and timings. State that the node
// already holds, such as its term or applied index, is read on scrape by
// [NewCollector] instead. A nil *Metrics records nothing.
type Metrics struct {
	leaderChanges     prometheus.Counter
	proposals         *prometheus.CounterVec
	proposalDuration  prometheus.Histogram
	readIndexDuration prometheus.Histogram
	walFsync          prometheus.Histogram
	applyBatch        prometheus.Histogram
	readyLoop         prometheus.Histogram
	compactions       prometheus.Counter
	snapshotsApplied  prometheus.Counter
}

// NewMetrics registers a node's metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	histogram := func(name, help string) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      name,
			Help:      help,
			Buckets:   metrics.FastBuckets(),
		})
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: metrics.Namespace, Name: name, Help: help})
	}

	m := &Metrics{
		leaderChanges: counter("kv_raft_leader_changes_total", "Changes of leader observed by this node."),
		proposals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "kv_proposals_total",
			Help:      "Writes proposed through this node, by result.",
		}, []string{"result"}),
		proposalDuration: histogram("kv_proposal_duration_seconds",
			"Time from proposing a write to applying it on this node."),
		readIndexDuration: histogram("kv_read_index_duration_seconds",
			"Time to confirm a linearizable read with a quorum and catch up to it."),
		walFsync: histogram("kv_wal_fsync_seconds",
			"Time to persist the hard state and new entries of a Ready, synced when raft requires it."),
		applyBatch:       histogram("kv_apply_batch_seconds", "Time to apply the committed entries of a Ready."),
		readyLoop:        histogram("kv_ready_loop_seconds", "Time to handle one raft Ready."),
		compactions:      counter("kv_log_compactions_total", "Raft log compactions."),
		snapshotsApplied: counter("kv_snapshots_received_total", "Snapshots received from a leader and applied."),
	}

	reg.MustRegister(
		m.leaderChanges, m.proposals, m.proposalDuration, m.readIndexDuration,
		m.walFsync, m.applyBatch, m.readyLoop, m.compactions, m.snapshotsApplied,
	)

	return m
}

func (m *Metrics) leaderChanged() {
	if m != nil {
		m.leaderChanges.Inc()
	}
}

func (m *Metrics) proposal(result string, start time.Time) {
	if m == nil {
		return
	}

	m.proposals.WithLabelValues(result).Inc()

	if result == proposalApplied {
		m.proposalDuration.Observe(time.Since(start).Seconds())
	}
}

func (m *Metrics) observe(h func(*Metrics) prometheus.Histogram, start time.Time) {
	if m != nil {
		h(m).Observe(time.Since(start).Seconds())
	}
}

func (m *Metrics) compacted() {
	if m != nil {
		m.compactions.Inc()
	}
}

func (m *Metrics) snapshotApplied() {
	if m != nil {
		m.snapshotsApplied.Inc()
	}
}

// collector reads a node's state on scrape.
type collector struct {
	n    *Node
	zone string

	info, term, leader, isLeader, commit, applied, logEntries *prometheus.Desc
	followerLag                                               *prometheus.Desc
	diskUsage, walSize, compactionDebt, readAmp               *prometheus.Desc
}

// NewCollector returns a collector of n's raft state and Pebble database,
// read when Prometheus scrapes: term, leader, commit and applied index,
// follower lag, disk usage. zone labels the node.
func NewCollector(n *Node, zone string) prometheus.Collector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(metrics.Namespace, "", name), help, labels, nil)
	}

	return &collector{
		n:    n,
		zone: zone,
		info: desc("kv_node_info", "The node's ID and zone; always 1.", "node", "zone"),
		term: desc("kv_raft_term", "Current raft term."),
		leader: desc("kv_raft_leader_id",
			"ID of the leader this node knows; 0 when it knows none."),
		isLeader:   desc("kv_raft_is_leader", "1 if this node is the leader."),
		commit:     desc("kv_raft_commit_index", "Raft commit index."),
		applied:    desc("kv_raft_applied_index", "Index of the last entry applied to the state machine."),
		logEntries: desc("kv_log_entries", "Entries in the raft log since the last compaction."),
		followerLag: desc("kv_raft_follower_match_lag",
			"Entries a follower trails the leader's last index by; only on the leader.", "follower"),
		diskUsage:      desc("kv_pebble_disk_usage_bytes", "Disk space used by the node's database."),
		walSize:        desc("kv_pebble_wal_bytes", "Size of the database's write-ahead log."),
		compactionDebt: desc("kv_pebble_compaction_debt_bytes", "Estimated bytes compactions still have to rewrite."),
		readAmp:        desc("kv_pebble_read_amplification", "Read amplification of the database."),
	}
}

// Describe implements [prometheus.Collector].
func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.info, c.term, c.leader, c.isLeader, c.commit, c.applied, c.logEntries,
		c.followerLag, c.diskUsage, c.walSize, c.compactionDebt, c.readAmp,
	} {
		ch <- d
	}
}

// Collect implements [prometheus.Collector].
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}

	st := c.n.Status()
	isLeader := 0.0

	if st.Leader == st.ID {
		isLeader = 1
	}

	gauge(c.info, 1, strconv.FormatUint(st.ID, 10), c.zone)
	gauge(c.term, float64(st.Term))
	gauge(c.leader, float64(st.Leader))
	gauge(c.isLeader, isLeader)
	gauge(c.commit, float64(st.Commit))
	gauge(c.applied, float64(st.Applied))

	if first, err := c.n.storage.FirstIndex(); err == nil {
		if last, err := c.n.storage.LastIndex(); err == nil && last+1 >= first {
			gauge(c.logEntries, float64(last+1-first))
		}
	}

	if isLeader == 1 {
		for _, f := range st.Followers {
			lag := float64(0)
			if st.Commit > f.Match {
				lag = float64(st.Commit - f.Match)
			}

			gauge(c.followerLag, lag, strconv.FormatUint(f.ID, 10))
		}
	}

	m := c.n.db.Metrics()
	gauge(c.diskUsage, float64(m.DiskSpaceUsage()))
	gauge(c.walSize, float64(m.WAL.PhysicalSize))
	gauge(c.compactionDebt, float64(m.Compact.EstimatedDebt))
	gauge(c.readAmp, float64(m.ReadAmp()))
}
