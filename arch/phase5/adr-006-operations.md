# ADR-006: Disaster resistance and operations

- **Status:** accepted
- **Date:** 2026-11-15
- **Context:** Phase 5 (`arch/phase5/plan.md`)

## Context

After Phase 4 the catalog serves the whole spec from a replicated KV
cluster, and survives losing a catalog replica or the raft leader. It did
not survive losing the cluster itself, its failure targets were not written
down, nothing showed what it was doing, and commits that never landed left
metadata files in S3. This ADR records how Phase 5 closes those gaps. The
operator's side, with every command, is in `docs/operations.md`.

## Decisions

### 1. Backups are snapshot files in S3, taken by a separate service

- **A consistent snapshot over the client API.** `MaintenanceService.
  ExportSnapshot` runs a ReadIndex barrier, then streams one Pebble
  snapshot. A snapshot holds every write acknowledged before the call,
  whichever node serves it. Followers serve it, to spare the leader.
- **A self-describing file** (`internal/kv/snapshot`): magic, format
  version, a protobuf header (cluster ID, index, term, revision, members),
  the state's record stream compressed with zstd, and a trailer with the
  body's size and a SHA-256 of everything before it. Truncation or a
  flipped bit anywhere is detected; a test flips every byte of a file.
- **`kv-backup`**, its own binary, so kv-node needs no S3 code or
  credentials. Every 5 minutes it uploads a snapshot unless the revision
  has not moved, writes the manifest last (a snapshot without one is
  incomplete), applies retention (newest 48, newest per day for 14 days,
  always the newest of all, only its own cluster's), and every 24 hours
  restores the newest backup into memory to prove it can be restored. Its
  `/healthz` fails after three intervals without a successful backup.
- **Changed from the plan:**
  - The trailer holds the body's size instead of a record count: the
    export does not count records, and size plus checksum detect the same
    damage.
  - `-location s3://bucket/prefix` replaces separate `-bucket` and
    `-prefix` flags.
  - No `-sse` flag: iceberg-go's S3 layer has no server-side encryption
    setting. The guide asks for the bucket's default encryption instead.
  - The client call takes a callback that receives the header before any
    byte: the object's name contains the revision, and an unchanged
    revision ends the stream without uploading.

### 2. A restore builds a new cluster

`kvctl snapshot restore` is offline and per node, like
`etcdutl snapshot restore`: the same file and member list on every node.
1. It verifies the whole file before it writes anything.
2. It imports the records, rewrites the members and the conf state, and
   moves the revision forward by 1,000,000,000.
3. It resets the raft log to an empty log after the snapshot's index and
   term, with a hard state committed to that index. All in one synced
   batch.
4. It stores a new cluster ID, derived from the members and the file's
   checksum.

- **The revision bump** keeps the `Store` contract: writes after the
  backup are lost, but their revisions were handed out, so the new cluster
  continues above them.
- **The new cluster ID** keeps the old cluster out: a node of it that
  comes back is rejected by the existing peer check. A drill starts an old
  disk next to the restored cluster: it never finds a leader.
- **A `RESTORE_IN_PROGRESS` marker** exists while a restore runs. A node
  refuses to start with it, and a restore refuses its directory. Without
  it, an interrupted restore left an empty directory, which a node with
  `-initial-cluster` set would have bootstrapped as a new, empty cluster.
- **The unsafe recovery** for a quorum lost for good: `kvctl snapshot save
  -data-dir` exports a stopped node's state, which is restored the same
  way. It may lack the last entries the lost majority committed.
- The plan's Step 0 question, whether `raft.RestartNode` continues from a
  log reset to a snapshot with a new conf state, is answered by the
  in-process tests: restores onto 1, 3 and 5 new nodes elect a leader and
  take writes.

### 3. After a restore, tables roll forward

The restored pointers lag; the metadata files of later commits are still
in S3, and each names its predecessor last in its metadata log.
`catalogctl restore-report` follows those chains per table and reports
`ahead` (a single chain: roll forward), `ambiguous` (two files continue
one parent: a person chooses), `missing-metadata`, `missing-files` (with
`-deep`) and `unregistered` directories (with `-scan-warehouse`). It only
reads; roll-forward is the Phase 4 `register` with `overwrite`, and
`-emit-commands` prints the calls.

The orphan janitor must not run before the report: restored intents may
name exactly the files to roll forward to. The runbook starts the catalogs
with `-orphan-cleanup=false` until the report is done.

**A bug the tests found:** the report compared directories with
`path.Dir`, which turns `s3://bucket/x` into `s3:/bucket/x`, so no file
ever matched. Locations are URIs; the code now cuts at the last slash.

### 4. Orphans are tracked with commit intents

Before writing the metadata files of an attempt, the catalog records an
intent naming them (`cat/v1/intent/<UUIDv7>`). The commit's KV transaction
compares the intent's revision and deletes it, so the pointer moves and the
intent disappears together, or neither does. An intent that outlives its
attempt names files no pointer leads to.

The janitor on every replica takes intents older than the grace period
(twice the write timeout), **condemns** each (a compare-and-swap that
changes its revision, so a commit still holding it fails and retries with a
new intent), deletes its files, then removes it. Safety needs no clock: a
file is deleted only after no commit can make it current. A janitor that
dies after condemning leaves a condemned intent, which the next sweep
finishes.

- **Rejected:** sweeping S3 for files the current metadata does not
  reference. It costs a listing per table, depends on clocks, and after a
  restore deletes exactly the files roll-forward needs.
- **Costs:**
  - One more KV write per attempt. `BenchmarkStore` (3 nodes, gRPC,
    in-memory disks): a commit takes 0.89 ms, a commit with its intent
    1.43 ms. On a real disk that is about one more fsync, roughly 2 ms
    (ADR-003); an S3 write takes tens of milliseconds.
  - One compare of the 128 per transaction: 127 updates, or 42 creates.
  - Three store calls per attempt instead of two. The startup check is now
    `http-write-timeout > (commit-retries + 1) × 3 × kv-request-timeout`,
    and the default write timeout rose from 60 s to 90 s; HAProxy's
    timeouts from 90 s to 120 s. The idempotency lease, twice the write
    timeout, is now 180 s, and the HA suite's transaction deadline rose
    from 150 s to 200 s to outlast it.
- **A commit without an intent still works**, so replicas of the two
  versions run side by side during a rolling upgrade.
- **Not orphans:** committed files that aged out of the metadata log
  (`write.metadata.previous-versions-max`, 100) and whose delete failed or
  was never asked for. The first orphan drill flagged exactly those, the 7
  oldest of 108 files; the drill now ignores files older than the log.

### 5. Definite store failures are told apart from unknown ones

`store.ErrOutcomeUnknown` is new; errors that match it also match
`store.ErrUnavailable`, so existing callers keep working. A KV failure
that certainly applied nothing (no leader, the proposal dropped) is
`ErrUnavailable` alone: the catalog deletes the new file at once and
answers `503 ServiceUnavailableException`. Only an unknown outcome answers
`CommitStateUnknown` and leaves the file to the janitor. The KV client
reports an unknown outcome if any attempt's was, even when a later attempt
failed definitely.

### 6. Zones are a label, placement a rule

`kv-node -zone` labels a node; the zone is reported by `Status`,
`kvctl status` (which warns when one zone holds a majority of the nodes
listed) and `echpochmak_kv_node_info`. It is not replicated: placement is
an operator's rule (an odd number of voters, at most `(n-1)/2` per zone),
not something the cluster enforces. The compose zone overlay runs 5 nodes
in 3 zones, 2/2/1.

### 7. Metrics and traces

- **Prometheus metrics** with `client_golang`, no global registry: each
  binary builds one in `main` and passes it down; every package's metrics
  are optional and do nothing when nil. Labels come from a bounded
  allow-list, enforced by a test that describes every registered metric,
  so a table name never becomes a label.
- **State is read on scrape**, events are counted where they happen: a
  node's term, leader, indexes, follower lag and Pebble statistics come
  from a collector that reads them when Prometheus asks; leader changes,
  proposals and fsync times are counted in the raft loop.
- **kv-node got an HTTP listener** (`:2381`) for `/metrics`, `/healthz`
  and `/readyz`.
- **OpenTelemetry traces**, configured only with the standard `OTEL_*`
  variables, off without an OTLP endpoint. `otelhttp` on the API (probes
  and `/metrics` excluded), spans for commits and purges, decorators for
  the store and file IO, `otelgrpc` between the catalog and the KV nodes,
  and `kv.propose` and `kv.read_index` on the node. An in-process test
  checks that one commit is one trace from the HTTP span to `kv.propose`.
  Log lines inside a traced request carry `trace_id` and `span_id`.
- **The commit retry loop now passes its context** to each attempt;
  before, attempts used the caller's context, and store and file spans
  would not have nested under the commit's span.
- **No alerting**, by decision: no alert rules, no Alertmanager. The
  Grafana dashboard shows what the alerts would have watched, and the
  guide names the series per failure.

### 8. Tools ship in the images

The images are distroless. `kvctl` and `kv-backup` are in the `kv-node`
image, `catalogctl` in the `catalog-server` image, so operators run them
in place. `kv-backup probe` checks the service's `/healthz`, for container
health checks without `curl`.

## Verification

| Check | Result |
|-------|--------|
| Store contract suite with intents and the new error, on `memstore`, `raftstore` and the instrumented decorator | Pass |
| Catalog: a replica dying before the swap; an unknown outcome that landed and one that did not; an intent condemned during its commit; an interrupted janitor; a definite KV failure | Pass |
| Snapshot format: round trip; truncation; a flipped bit at every byte; an unknown version | Pass |
| Restore in process: onto 1, 3 and 5 new nodes; from a stopped node's disk; a corrupt file; an interrupted restore | Pass |
| `kv-backup`: skipped unchanged backups; an interrupted upload; retention over a synthetic history; verification of a corrupted object | Pass |
| One commit traced from HTTP to `kv.propose`; metrics and log trace IDs | Pass |
| Metric labels on the allow-list | Pass |
| All Go tests with the race detector, lint | Pass, 0 issues |
| **Drills, `make dr-e2e`** | Each drill passed, but not yet in one full run: see below |

The drills, on 5 kv-nodes in 3 zones, two catalogs, Trino and PyIceberg:

| Drill | Result |
|-------|--------|
| 1. Zone `a` (2 of 5 nodes) killed under load | No acknowledged row lost or duplicated, 0 client errors. Then a third node killed: both catalogs unready; ready again 1–5 s after the nodes restarted |
| 2. A catalog replica and the raft leader killed during commits | After the grace period, every metadata file current or in its table's log (77 and 119 files) |
| 3. Observability | Key series present in Prometheus; 3 zones in the metrics; a Jaeger trace with spans of the catalog and a kv-node; with no quorum, leader 0 and both catalogs unready |
| 4. Backups | Backups listed; retention with keep-last 2; `kv-backup verify` passes |
| 5. Every node and disk destroyed, restored from the newest backup | **RTO 11–13 s** to the first commit. The table had its 10 rows of the backup; the report found the 10 later commits (`ahead`) and the table created after the backup (`unregistered`); after roll-forward, 20 rows and the new table; the namespace property set after the backup is lost, as documented; the janitor, back on, deleted nothing a pointer needs; an old node's disk started next to the cluster never found a leader |
| 6. Unsafe recovery from one stopped node | **RTO 9–10 s**; no row lost |

**What is not verified:** `make dr-e2e` did not pass as one full run.
Across three runs every drill passed at least once, and the last run passed
all but drill 5, which failed only on a container left by the previous
run's cleanup. That cleanup is fixed but not re-run: the two confirming
runs were stopped by request. The first run had a failure that did not
recur: a kv-node not running when a drill expected it, and a catalog not
ready afterwards; its logs were lost, and `dr-e2e` now saves all logs to
`build/dr-logs.txt` on failure. `make e2e`, `make ha-e2e` and the RCK were
not re-run in this phase; the HA suite's deadline was adjusted for the new
lease. The runbooks of `docs/operations.md` were exercised by the scripted
drills, not followed by hand.

## Known limitations

- **No TLS or authentication** (decision 3): a reachable client port can
  read and write the KV store, and can stream a snapshot of it.
- **A restore loses** namespace changes, view changes and table drops made
  after the backup; roll-forward recovers table commits and creates only.
- **Ambiguous chains need a person:** after a restore, a commit that never
  landed and one that did can continue the same file; the report cannot
  tell them apart.
- **Placement is not enforced:** nothing stops an operator from putting a
  majority in one zone; `kvctl status` and the dashboard only show it.
- **Snapshots are in memory during an offline save** (`kvctl snapshot save
  -data-dir`): fine for a catalog's state, which is small.
- **Zone loss on one machine** proves placement and quorum logic, not
  network behaviour between data centres.
