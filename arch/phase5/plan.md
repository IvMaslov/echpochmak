# Phase 5: Disaster resistance and operations

**Goal:** the catalog survives the loss of a node, of an availability zone
and of the whole KV cluster, with a recovery point and a recovery time that
are written down and measured. Operators can see what the system is doing:
every component exports Prometheus metrics, requests can be traced across
the catalog and the KV cluster, and a dashboard shows the state of the
whole system at a glance. Files
written by commits that never landed no longer pile up in S3.

**Milestone (from `arch/plan.md`):**
- Periodic snapshots of the KV cluster land in S3. A whole cluster, all of
  its disks destroyed, is rebuilt from the latest one with one documented
  procedure, and Trino reads the restored tables.
- A 5-node cluster spread over 3 zones keeps serving when one zone is lost.
  The RPO and RTO of every failure class are documented, and the drills
  measure them.
- Prometheus scrapes every component; a trace of a table commit shows the
  HTTP request, the catalog, S3 and the KV cluster down to the raft
  proposal.
- After commits are interrupted by killed replicas and lost KV leaders, no
  metadata file that the catalog wrote is left unreferenced once the grace
  period has passed.

**Builds on:**
- **Phase 2:** the KV node's Pebble database, its consistent exports
  (`statemachine.Export`, used today to send snapshots to slow followers),
  `raftlog.Storage.ResetToSnapshot`, and the cluster ID check between peers.
- **Phase 3:** `raftstore`, the timeout budgets of ADR-004, readiness and
  draining, and the HA stack with its test harness (`test/ha`).
- **Phase 4:** the purge queue and its leased worker, which the orphan
  janitor copies; `register` with `overwrite`, which a restore uses to roll
  tables forward.

---

## Scope

### In scope

| Area | What |
|------|------|
| **Backups** | A consistent snapshot RPC on every kv-node; `kv-backup`, a service that uploads snapshots to S3 on a schedule, keeps a retention policy and test-restores the latest backup |
| **Restore** | `kvctl snapshot save / status / restore`: rebuild a cluster from a snapshot file, with a new cluster identity; recovery from the data directory of a single surviving node when quorum is lost |
| **After a restore** | `catalogctl restore-report`: tables whose S3 metadata moved past the restored pointer, and pointers whose files are gone; roll-forward through `register` with `overwrite` |
| **Zones** | A `-zone` label on kv-nodes, a placement check, a 5-node 3-zone compose overlay, the RPO/RTO table |
| **Orphan cleanup** | Commit intents in the KV store, released in the commit's own transaction; a janitor on every replica deletes the files of intents that were never released |
| **Precise store failures** | `store.ErrOutcomeUnknown` split from `store.ErrUnavailable`, so a definitely failed commit deletes its file at once |
| **Metrics** | Prometheus metrics on `catalog-server`, `kv-node` and `kv-backup` |
| **Tracing** | OpenTelemetry traces across HTTP, the catalog, S3 calls, the KV client and the KV server, exported with OTLP |
| **Dashboard** | One Grafana dashboard; a compose overlay with Prometheus, Grafana and Jaeger |
| **Operations guide** | `docs/operations.md`: runbooks for every failure class, the metrics reference, backup and restore |

### Out of scope

| Item | Why |
|------|-----|
| TLS and mTLS between nodes, catalogs and clients | Decision 3 of `arch/plan.md`: trusted network only. ADR-003 listed TLS under Phase 5; it moves to a later security phase together with authentication, because TLS without authentication protects little here |
| Continuous log shipping (point-in-time recovery between snapshots) | The S3 metadata files already form a log of table commits, and roll-forward (D7) recovers them. What remains between snapshots (namespace properties, creates and drops) does not justify shipping the raft log |
| Backing up the S3 data itself | Table files live in the operator's bucket. The guide requires bucket versioning with a noncurrent-version retention at least as long as the backup retention (D7), and leaves replication to the storage layer |
| Kubernetes manifests | No cluster to test them on; untested manifests would mislead. The guide describes the placement rules (one node per zone, odd count) in terms that map to topology spread constraints |
| Orphan *data* files (written by engines for commits that failed) | The catalog never writes or tracks them. Engines clean them up: Trino's `ALTER TABLE ... EXECUTE remove_orphan_files`, Spark's `remove_orphan_files` |
| Automatic cross-region failover | A restore is a deliberate operator action: it loses the commits after the snapshot, and only a person can decide that the old region is gone |
| Alerting (alert rules, Alertmanager) | User decision: not needed for this project. The metrics and the dashboard show every condition an alert would have watched; the guide names the series to look at per failure class |
| Leader placement by zone, follower reads | No latency problem to solve on one machine; revisit with real zones |

---

## How the pieces fit

```
 Trino · PyIceberg ── traceparent ──┐
                                    ▼
 catalog-server ×2 ── /metrics ─────────────► Prometheus ──► Grafana
  ├─ otelhttp, catalog spans  ── OTLP ──────► Jaeger            ▲
  ├─ orphan janitor ──► S3: delete unreleased files             │
  └─ store (instrumented) ── gRPC + trace ──┐                   │
                                            ▼                   │
 kv-node ×5, zones a/b/c ── :2381 /metrics ─┴───────────────────┤
  └─ Maintenance.Snapshot (consistent export)                   │
            ▲                                                   │
 kv-backup ─┴─► S3: s3://<backup-bucket>/kv/<cluster>/…  ── /metrics
            (schedule, retention, daily test restore)

 Disaster:  kv-backup fetch ─► kvctl snapshot restore (each node)
            ─► start nodes ─► catalogctl restore-report ─► register overwrite
```

---

## Design decisions

### D1: A consistent snapshot RPC and a self-describing file

Snapshots already exist for one purpose: the transport streams a Pebble
export to a follower that fell behind (ADR-003). Backups reuse the same
export, through a new client-facing service, so a backup tool never touches
a node's disk.

**The RPC.** A new service in `api/kv/proto/.../maintenance.proto`:

```proto
service Maintenance {
  // Streams a consistent snapshot of the replicated state.
  rpc Snapshot(SnapshotRequest) returns (stream SnapshotChunk);
}
```

1. **Freshness:** the node first runs a ReadIndex barrier and waits until it
   has applied that index. The snapshot then contains every write
   acknowledged before the call, whichever node serves it.
2. **Consistency:** it opens one Pebble snapshot and streams the export from
   it, so data and applied state match even while writes continue.
3. **Chunks** of 1 MiB. The first message carries the header, the last the
   trailer.
4. Any node serves it. `kv-backup` prefers a follower, to keep the export's
   I/O off the leader.

Adding a service is a compatible change: `make buf-breaking` stays green.

**The file format** (`internal/kv/snapshot`), versioned, readable without a
running node:

| Part | Content |
|------|---------|
| Magic | `ECHKVSNP` |
| Format version | `uint32`, starts at 1 |
| Header | length-prefixed protobuf `SnapshotHeader`: cluster ID, serving node ID, raft index and term, KV revision, members with their peer URLs, creation time, `kv-node` version |
| Body | the existing record stream (`statemachine.Export.WriteTo`), zstd-compressed (`klauspost/compress`, already a dependency) |
| Trailer | record count and SHA-256 of everything before the trailer |

`kvctl snapshot status <file>` checks the trailer and prints the header.
A truncated or corrupt file is always detected before anything is written.

### D2: `kv-backup` uploads snapshots to S3

A separate binary, `cmd/kv-backup`, so `kv-node` stays free of S3 code and
credentials. It runs as one more container on the HA stack.

**The loop** (`kv-backup run`):
1. Every `-interval` (default 5 min, jittered by ±10%), call
   `Maintenance.Snapshot` on a follower.
2. If the revision has not moved since the last backup, upload nothing and
   record the check as a success: the last backup is still current.
3. Otherwise stream the snapshot to S3 with a multipart upload, without a
   temporary file, at
   `s3://<bucket>/<prefix>/<cluster-id>/<UTC time>-r<revision>.snap`.
4. Write the **manifest** last, next to it: `….snap.json` with the header
   fields, size and SHA-256. A backup without a manifest is incomplete and is
   ignored by `list`, `fetch` and retention.

**Retention** (`-keep-last 48`, `-keep-daily 14`): keep the newest 48
backups (4 hours at the default interval) and the newest backup of each of
the last 14 days. Retention:
- deletes only backups under its own cluster ID prefix;
- never deletes the newest complete backup;
- runs after a successful upload, never after a failed one.

**Verification** (`-verify-interval 24h`): download the newest backup,
check its trailer, import it into an in-memory Pebble database, and compare
the applied state with the header. A backup that has never been restored is
not a backup; this makes restoring a daily event.

**Commands:**

| Command | Purpose |
|---------|---------|
| `kv-backup run` | The service: schedule, retention, verification, `/metrics`, and `/healthz`, which fails after 3 intervals without a successful backup, so the container shows unhealthy |
| `kv-backup now` | One backup, for an operator before a risky change |
| `kv-backup list` | Complete backups, newest first: time, revision, size |
| `kv-backup fetch [-at <time>] <file>` | Download the newest backup, or the newest at or before a time, and check it |
| `kv-backup verify [<key>]` | The daily verification, on demand |

**Where backups go.** A separate bucket (`-bucket`, compose creates
`echpochmak-backups`), so a mistaken lifecycle rule on the warehouse cannot
take the backups with it. In production it belongs in another region or
account; the guide says so. Server-side encryption is passed through
(`-sse`); there is no client-side encryption.

### D3: Restore builds a new cluster from a snapshot

A restore is offline and per node, like `etcdutl snapshot restore`: the
operator runs it once for each node of the new cluster, with the same file
and the same member list, then starts the nodes.

```
kvctl snapshot restore backup.snap \
  -id 1 -data-dir /data \
  -initial-cluster 1=http://kv1:2380,2=http://kv2:2380,3=http://kv3:2380
```

1. **Refuse** a non-empty data directory, and a file whose trailer does not
   match.
2. **Import** the records into a fresh Pebble database
   (`statemachine.ImportSnapshot`).
3. **Rewrite the applied state:**
   - members and conf state become the new member list: the old cluster's
     peer URLs mean nothing in the new one;
   - the revision jumps forward by `-revision-bump` (default 1,000,000,000).
4. **Reset the raft log** to a snapshot at the backup's index and term with
   the new conf state (`raftlog.Storage.ResetToSnapshot`), and set the hard
   state's commit to that index.
5. **Store a new cluster ID**, derived from the new member list and the
   snapshot's SHA-256, so every node computes the same one.
6. Fsync everything, the directories included (the lesson of ADR-003's
   first bug).

Two choices inside it:
- **A new cluster ID.** A node of the old cluster that comes back, or an old
  data directory started by mistake, is rejected by the peer check that
  already exists. It can never mix old log entries into the new cluster.
- **The revision bump.** Writes after the snapshot are lost, but their
  revisions were already handed out. The `Store` contract says a version is
  never reused, so the restored cluster continues far above anything the
  old one issued. Pointers keep their stored `mod_revision`. etcd's restore
  has the same option for the same reason.

**When quorum is lost but a node survives** (two of three zones gone for
good), the same restore path recovers from that node's disk:

```
kvctl snapshot save -data-dir /data/kv1 kv1.snap     # offline, node stopped
```

It exports the node's applied state without a running cluster. That node
may lack the last entries that the lost majority committed, so this is the
*unsafe* recovery, named so in the guide: its RPO is the node's replication
lag, usually milliseconds, but not zero. Waiting for a lost zone to return
is always safer, and the runbook asks for that first.

### D4: Zones, and the targets per failure class

**Placement.** `kv-node -zone <name>` labels a node. The zone is node-local:
it appears in `Status` (one new field) and on the `echpochmak_kv_node_info`
metric, and it does not change the replicated state. `kvctl status -all`
queries every endpoint it is given and warns when one zone holds a majority
of the voters, because losing that zone loses quorum. The dashboard shows
the voters per zone from the same metric.

The rules, in the guide:
- an odd number of voters, at most `(n-1)/2` per zone;
- 3 nodes in 3 zones survive one zone; 5 nodes in 3 zones (2/2/1) survive
  one zone, or any two nodes;
- 2 zones are never enough: losing the larger one loses quorum.

**Targets.** RPO is what is lost, RTO is how long writes are refused.

| Failure | RPO | RTO target | How |
|---------|-----|------------|-----|
| One kv-node, or the raft leader | 0 | ≤ 3 s | Election (1 s at the defaults) plus client retries; the Phase 3 milestone measures it |
| One zone (3 zones) | 0 | ≤ 3 s | As above, for up to two nodes at once |
| A catalog replica | 0 | 0 for new requests | HAProxy health checks; in-flight requests retried by clients |
| Quorum lost, nodes come back | 0 | Until the zone returns | Nodes restart from their disks |
| Quorum lost for good, one node left | Replication lag of that node (ms) | ≤ 15 min | Unsafe recovery from its data directory (D3) |
| Every kv-node and disk lost | Table commits: ≈ 0 after roll-forward (D7). Everything else: ≤ one backup interval + upload time (default ≤ 6 min) | ≤ 15 min | Restore from the newest S3 backup |
| Operator error (a wrong drop) | Changes after the chosen backup | ≤ 30 min | Restore an older backup within retention, then roll forward the tables that should keep their changes |

The drills (Step 7) measure the RTO of the last three rows on the local
stack. The 15-minute target is for a manual runbook; ADR-006 records the
measured times, which on one machine will be far smaller.

### D5: Orphan metadata files, tracked by commit intents

**Where orphans come from today.** A commit writes the new metadata file
first and swaps the pointer second. The file is orphaned when:
- the swap's outcome is unknown and it did not land (ADR-004 keeps the file,
  on purpose);
- the replica dies between the write and the swap, or during the swap;
- a cleanup delete after a lost race fails, and is only logged.

**Rejected: sweeping S3.** List every table's `metadata/` directory and
delete files that the current metadata does not reference and that are older
than a grace period. It costs a listing per table, it depends on clocks
(a commit slower than the grace period loses its file), and after a restore
it would delete exactly the files that roll-forward needs (D7).

**Chosen: an intent per commit attempt**, released atomically by the commit
itself.
1. **Before writing** the metadata files of an attempt, the catalog creates
   an intent: `cat/v1/intent/<UUIDv7>` → `{locations, identifiers,
   created, replica}`. One intent covers every file of the attempt, so a
   multi-table transaction needs one, not N.
2. **The commit transaction** (`CommitTables`, `CommitView`) gains one
   compare (the intent is still at its version) and one operation (delete
   the intent). The pointer moves and the intent disappears in the same
   write, or neither happens.
3. **After a lost race or a definite rejection,** the catalog deletes the
   files and then releases the intent. If a delete fails, the intent stays
   and the janitor retries it.
4. **After an unknown outcome,** both stay. If the commit landed, the intent
   is already gone; if it did not, the janitor finds it.

**The janitor**, on every replica, every `-orphan-interval` (1 min,
jittered), for intents older than `-orphan-grace` (default twice the HTTP
write timeout):
1. **Condemn** the intent: compare-and-swap it to "condemned". Its version
   changes, so a commit still holding it now fails its compare, and is
   retried as a lost race with a fresh intent.
2. **Delete the files.** Deleting a missing file succeeds.
3. **Remove the intent**, with a compare on the condemned version.

A janitor that dies after step 1 leaves a condemned intent, which the next
pass finishes. Safety needs no clock: a file is only deleted after the
intent's version moved, and from then on no commit can make it current. The
grace period only avoids condemning commits that are merely slow.

**Costs and budgets:**
- **One more KV write per commit attempt** (≈ 1–2 ms, measured in
  `BenchmarkStore`), small next to the S3 write.
- **The transaction budget** (ADR-005: 128 compares per branch) loses one
  compare: up to 127 table updates per transaction, or 42 creates.
- **The timeout budget of ADR-004** grows from two store calls per attempt
  to three. `catalog-server` refuses to start unless
  `http-write-timeout > (commit-retries + 1) × 3 × kv-request-timeout`.
  With the defaults that is 5 × 3 × 5 s = 75 s, so the default
  `http-write-timeout` rises from 60 s to 90 s, and HAProxy's client and
  server timeouts from 90 s to 120 s. Step 0 checks the clients' own
  timeouts against 90 s.
- **Rolling upgrades:** a commit without an intent still works, so old and
  new replicas can run side by side during the upgrade.

**Store changes:**
- `CreateIntent(ctx, Intent) (IntentRef, error)`
- `Commit` and the view commit take an optional `IntentRef`.
- `ListIntents(ctx, createdBefore, limit)`, `CondemnIntent(ctx, ref)`,
  `ReleaseIntent(ctx, ref)`.
- Both backends, and contract tests: release in the commit; a condemned
  intent fails the commit; commits without intents.

**What it does not cover:** metadata files that dropped out of the metadata
log and whose delete failed (`write.metadata.delete-after-commit.enabled`).
They are referenced by nothing, but they were committed once, so they are
not orphans of a failed commit. Their failed deletes are counted in a
metric, and the guide points to engine maintenance.

### D6: Definite store failures are told apart from unknown ones

ADR-004 maps both `client.ErrUnavailable` (certainly not applied) and
`client.ErrOutcomeUnknown` to `store.ErrUnavailable`, and the catalog
answers "commit state unknown" for both. It keeps the file in both cases.

Phase 5 splits them:

| KV client error | `Store` error | Catalog after a failed commit |
|-----------------|---------------|-------------------------------|
| `ErrUnavailable` | `store.ErrUnavailable` | Delete the files, release the intent, answer `503 ServiceUnavailableException` |
| `ErrOutcomeUnknown` | `store.ErrOutcomeUnknown` (new) | Keep both, answer `503 CommitStateUnknownException` |

To a client both are a 5xx after which it must not clean up; Step 0
confirms how Java, PyIceberg and Trino treat a `503` on commit. The server
side is what changes: the common definite failure (no leader, the proposal
dropped) no longer leaves anything for the janitor. Reads and other writes
keep mapping both to `503`.

### D7: After a restore: the report and roll-forward

A restored KV cluster points every table at the metadata current at backup
time. S3 is not restored, so two kinds of drift exist:
- **Commits after the backup:** their metadata files are still in S3, and
  each one names its predecessor in its metadata log. The pointer can be
  moved forward to them.
- **Deletions after the backup:** a purge, an expired metadata file, or an
  engine's snapshot expiry may have deleted files that the restored metadata
  still references.

`cmd/catalogctl` (new, talks to the KV store like `catalog-server`) has a
read-only command:

```
catalogctl restore-report [-tables db.t,…] [-scan-warehouse]
```

For every table it reads the pointer's metadata file and lists the table's
`metadata/` directory, then reports:

| Finding | Meaning | Suggested action |
|---------|---------|------------------|
| `ahead` | A chain of newer metadata files continues from the current one, each naming the previous in its metadata log | Roll forward to the newest file of the chain |
| `ambiguous` | Two newer files continue from the same parent (one was a commit that lost its race or never landed) | Choose by hand; the report shows both with their snapshot summaries |
| `missing-metadata` | The pointer's metadata file is gone | Recover it from bucket versioning, or drop the table |
| `missing-files` | With `-deep`, a manifest list or manifest the current snapshot needs is gone | Recover from bucket versioning, or roll back the table's snapshot |
| `unregistered` | With `-scan-warehouse`, a metadata directory under the warehouse that no pointer leads to: a table created after the backup | Register it |

**Roll-forward** is the Phase 4 `register` with `overwrite=true`, pointed at
the newest file of the chain. `catalogctl restore-report -emit-commands`
prints the `curl` calls; it never runs them itself.

The orphan janitor must not run before the report: intents restored from
the backup may name exactly the files that roll-forward needs. So the
runbook starts the catalogs with `-orphan-cleanup=false` after a restore,
runs the report and the roll-forward, and then turns the janitor on.

**Bucket versioning.** The deletion findings can only be repaired if S3
kept the deleted versions. The guide requires versioning on the warehouse
bucket with a noncurrent-version retention at least as long as the backup
retention (14 days by default). The compose stack turns it on in MinIO.

### D8: Metrics

**Library:** `github.com/prometheus/client_golang`, used directly. No
global registry: each component builds a `prometheus.Registry` in `main` and
passes it down, and every package takes its metrics as an optional
dependency that is a no-op when nil, so tests do not need it. This keeps
`gochecknoglobals` happy and tests isolated.

**Conventions:**
- Prefix `echpochmak_`; units in names (`_seconds`, `_bytes`, `_total`).
- **No unbounded labels:** never a table, namespace, request ID or key.
  Routes are the route templates the API layer already computes
  (`withRoute`). Per-table detail belongs in traces and logs.
- Histograms for latencies, with buckets chosen per metric (sub-millisecond
  for KV reads, up to tens of seconds for commits).
- Go runtime and process collectors on every binary, and
  `echpochmak_build_info{version, revision, go_version}`.

**catalog-server**, served at `GET /metrics` on the API listener, outside
`/v1` and excluded from request logs like the probes:

| Metric | Labels |
|--------|--------|
| `http_requests_total`, `http_request_duration_seconds` | `route`, `method`, `status` |
| `catalog_commits_total` | `kind` (table, view, transaction), `outcome` (committed, conflict, requirement_failed, rejected, unavailable, state_unknown) |
| `catalog_commit_duration_seconds`, `catalog_commit_attempts` | `kind` |
| `catalog_metadata_io_seconds`, `catalog_metadata_io_errors_total` | `op` (read, write, delete) |
| `catalog_metadata_cache_requests_total` | `result` (hit, miss) |
| `store_call_duration_seconds`, `store_call_errors_total` | `op`, `error` |
| `kv_client_attempts_total` | `result` (ok, retried, failed_over, unavailable, unknown) |
| `idempotency_requests_total` | `result` (first, replayed, mismatch, busy, takeover) |
| `purge_tasks_completed_total`, `purge_files_deleted_total`, `purge_failures_total`, `purge_queue_oldest_seconds` | — |
| `orphan_intents_pending`, `orphan_intents_oldest_seconds`, `orphan_files_deleted_total`, `orphan_delete_failures_total` | — |
| `catalog_expired_metadata_delete_failures_total` | — |
| `readiness` (1 ready, 0 not) | `reason` |

The queue gauges are sampled by the purge worker and the janitor on each
pass, not on scrape, so a scrape never reaches the KV store.

**kv-node**, on a new HTTP listener `-listen-metrics :2381`, which also
serves `/healthz` (the process runs) and `/readyz` (a leader is known and
this node's applied index is within `-ready-max-lag` entries of its commit
index):

| Metric | Meaning |
|--------|---------|
| `kv_node_info{zone, node}` | Constant 1, for joins and the placement panel |
| `kv_raft_term`, `kv_raft_leader_id`, `kv_raft_is_leader` | Raft state |
| `kv_raft_leader_changes_total` | Counted on every observed change of leader |
| `kv_raft_commit_index`, `kv_raft_applied_index` | Their difference is the apply lag |
| `kv_raft_follower_match_lag` (leader only, label `follower`) | Entries each follower trails by; bounded by the member count |
| `kv_proposals_total{result}` | applied, dropped, timed_out, deduplicated |
| `kv_proposal_duration_seconds`, `kv_read_index_duration_seconds` | Propose to apply; ReadIndex round |
| `kv_wal_fsync_seconds` | The synced batch of hard state and entries in the Ready loop |
| `kv_apply_batch_seconds`, `kv_ready_loop_seconds` | Time spent per Ready |
| `kv_snapshots_sent_total`, `kv_snapshots_received_total`, `kv_snapshot_bytes_total` | Follower catch-up |
| `kv_log_compactions_total`, `kv_log_entries` | Log size |
| `kv_transport_queue_length{peer}`, `kv_transport_dropped_total{peer}` | Back-pressure |
| `kv_grpc_requests_total{method, code}`, `kv_grpc_duration_seconds{method}` | Client API, through a small interceptor written here |
| `kv_pebble_*` | A collector over `pebble.DB.Metrics()` on scrape: disk usage, WAL size, compaction debt, read amplification |

**kv-backup**, on `-listen-metrics`:
`backup_last_success_timestamp_seconds`, `backup_last_revision`,
`backup_duration_seconds`, `backup_bytes`, `backup_failures_total{stage}`,
`backup_verify_last_success_timestamp_seconds`, `backup_retained`.

### D9: Tracing

**Library:** the OpenTelemetry Go SDK with the OTLP exporter. Configuration
uses the standard `OTEL_*` environment variables (endpoint, service name,
sampler), so nothing project-specific is invented. Tracing is off, with a
no-op provider and no cost, unless `OTEL_EXPORTER_OTLP_ENDPOINT` or
`OTEL_TRACES_EXPORTER` is set. The default sampler is
`parentbased_traceidratio` at 1.0: a client that sends a sampled
`traceparent` is always followed.

**Spans:**

| Where | Span | Attributes |
|-------|------|------------|
| HTTP server (`otelhttp`, outermost) | `HTTP <method> <route>` | route, status, request ID, idempotency key present |
| Catalog | `catalog.CommitTable`, `catalog.CommitTransaction`, `catalog.CommitView`, `catalog.Purge` | identifiers, attempts, outcome |
| Store decorator | `store.<Method>` | result |
| File IO decorator | `fileio.Read`, `fileio.Write`, `fileio.Delete` | location, bytes, cache hit |
| KV client and server (`otelgrpc` stats handlers) | `kv.v1.KV/Txn` and so on | endpoint, retries |
| kv-node | `kv.propose` (wait from proposal to apply), `kv.read_index` | index, deduplicated |

Raft internals (the Ready loop, peer messages) are not traced: they are
shared by all requests, and metrics describe them better.

**Instrumentation as decorators.** The store and file IO spans and metrics
come from wrappers (`internal/store/instrumented`,
`internal/fileio/instrumented`) around any implementation, so `memstore`,
`raftstore` and the S3 adapter stay unchanged and the conformance suite
covers the wrapped versions too.

**Logs and traces join.** A `slog` handler wrapper adds `trace_id` and
`span_id` to every record logged with a traced context. The request ID stays,
and is also a span attribute.

### D10: The dashboard and the observability stack

**The overlay** `deployments/compose/docker-compose.obs.yaml`, on top of
the HA overlay:
- **Prometheus**, scraping both catalogs, all kv-nodes and `kv-backup`.
- **Jaeger** (all-in-one), receiving OTLP from every component.
- **Grafana**, with Prometheus and Jaeger as provisioned data sources and
  one provisioned dashboard: request rate, errors and latency per route;
  commit outcomes; raft leader, term and apply lag per node; fsync latency;
  backup freshness and the last verification; voters per zone; orphan and
  purge queues.

All three images are pinned by digest, as HAProxy was. They are pulled by
hand, like `haproxy:3.4` and the JDK image; Step 0 lists
them.

### D11: The zone stack

`deployments/compose/docker-compose.zones.yaml`, on top of the HA overlay:
- 5 kv-nodes: `kv1`, `kv2` in zone `a`; `kv3`, `kv4` in zone `b`; `kv5` in
  zone `c`.
- `kv-backup`, writing to the `echpochmak-backups` bucket, every 30 s so
  the drills don't wait.
- MinIO with versioning on the warehouse bucket.

Docker has no zones, so "losing a zone" means killing every container of
that zone and disconnecting its volumes; that is what a zone outage looks
like to the rest of the cluster.

### D12: Configuration

**catalog-server:**

| Setting | Default | Purpose |
|---------|---------|---------|
| `http-write-timeout` | `90s` (was 60 s) | The budget of D5 |
| `metrics` | `true` | Serves `/metrics` |
| `orphan-cleanup` | `true` | Runs the janitor; `false` after a restore until the report is done (D7) |
| `orphan-interval` | `1m` | Janitor pass, jittered |
| `orphan-grace` | `0` = twice `http-write-timeout` | Age before an intent is condemned |

**kv-node:**

| Setting | Default | Purpose |
|---------|---------|---------|
| `zone` | empty | Placement label (D4) |
| `listen-metrics` | `:2381` | `/metrics`, `/healthz`, `/readyz`; empty turns it off |
| `ready-max-lag` | `1000` | Apply lag above which `/readyz` fails |

**kv-backup:** `endpoints`, `bucket`, `prefix` (`kv`), `interval` (`5m`),
`keep-last` (`48`), `keep-daily` (`14`), `verify-interval` (`24h`), `sse`,
`listen-metrics` (`:2382`), and the S3 settings shared with
`catalog-server` (endpoint, region, path style, credentials).

**Tracing:** the standard `OTEL_*` variables on all three (D9).

---

## Step 0: Check the dependencies

Short spikes; results go in the implementation notes at the end.
- **Go 1.26.2 compatibility** of `prometheus/client_golang`, the OTel SDK,
  the OTLP gRPC exporter, `otelhttp` and `otelgrpc`: pick the newest
  releases whose `go.mod` accepts 1.26.2, as was needed for `buf`. Several
  are already indirect dependencies.
- **Restore mechanics:** in a scratch test, reset a node's raft log to a
  snapshot at index *i*, term *t*, with a new conf state, and confirm that
  `raft.RestartNode` elects a leader and continues from *i* + 1 on three
  nodes. Check what the hard state's term must be.
- **Client behaviour on commit errors:** confirm that Java's REST client,
  PyIceberg 0.12.0 and Trino 484 treat a `503 ServiceUnavailable` on a
  commit as "state unknown" and never delete their files (D6).
- **Client timeouts:** confirm that Java's and PyIceberg's REST clients wait
  at least 90 s for a commit response, or record what they do (D5).
- **S3 checksums:** whether MinIO accepts `x-amz-checksum-sha256` on
  multipart uploads, so the upload itself is verified; otherwise verify by
  reading back.
- **Pebble metrics:** which fields of `pebble.Metrics` in v2.1.7 give disk
  usage, WAL size and compaction debt.
- **Images to pull by hand,** then pinned by digest: Prometheus 3.x (3.16.0),
  Grafana 13.x (13.3.1), Jaeger 2.x all-in-one (2.23.0). The user pulls them, as with HAProxy.

## Step 1: Snapshots and restore in the KV layer

- `internal/kv/snapshot`: the file format of D1, its writer and reader.
- `Maintenance.Snapshot` in the proto, the server and the Go client.
- `kvctl snapshot save` (from a cluster, or offline with `-data-dir`),
  `status`, `restore` (D3).
- Tests:
  - The format: round trip; truncation and a flipped byte anywhere are
    rejected; an unknown format version is rejected.
  - The RPC returns every write acknowledged before the call, served by a
    follower, while writes continue.
  - **Restore, in process** (`kvtest`): write to a 3-node cluster, save,
    destroy every node, restore onto 3 new data directories with new IDs,
    start them. The data matches, the revision jumped, the cluster ID is
    new, writes work, and a node of the old cluster is rejected by the peer
    check.
  - Restore onto 5 nodes from a 3-node snapshot, and onto 1.
  - The offline save from a stopped node's directory.
  - A crash in the middle of a restore leaves a directory that the next
    restore refuses, never a half-restored node that starts.

## Step 2: `kv-backup`

- `cmd/kv-backup` with `run`, `now`, `list`, `fetch`, `verify` (D2).
- The S3 side: streaming multipart upload, manifests last, listing by
  manifest. `FileIO` (read, write, delete whole files) does not fit
  streaming uploads or listings, so `kv-backup` uses a small interface of
  its own (`PutStream`, `Get`, `List`, `Delete`) with an S3 implementation
  on `aws-sdk-go-v2` and an in-memory fake.
- Tests against the fake and, in the drills, against MinIO:
  - A backup is skipped when the revision has not moved.
  - Retention: keep-last and keep-daily over a synthetic history; the newest
    complete backup survives every policy; incomplete uploads are ignored
    and eventually removed.
  - Verification detects a corrupted object.
  - `fetch -at` picks the right backup.

## Step 3: Orphan cleanup and precise store failures

- **Store** (D5, D6): intents in `memstore` and `raftstore`;
  `store.ErrOutcomeUnknown`; contract tests for both.
- **Catalog:** create an intent before every metadata write (table commit,
  create, transaction, view create and replace); pass it to the commit;
  release it after cleanup; the new classification of D6.
- **The janitor** in `internal/catalog/orphans.go`, started in `main` like
  the purge worker.
- **Budgets:** the new timeout check; the transaction budget minus one;
  `http-write-timeout` 90 s; HAProxy 120 s.
- Tests:
  - A replica "dies" between writing the file and the swap (a fault-injecting
    store): after the grace period the file is gone.
  - An unknown outcome that landed: the intent was released by the commit,
    and the file survives every janitor pass.
  - An unknown outcome that did not land: the janitor deletes the file.
  - The janitor condemns an intent while its commit is in flight: the commit
    fails its compare and succeeds on retry with a new file; the condemned
    file never becomes current.
  - A janitor killed after condemning: the next pass finishes.
  - A definite KV failure deletes the file at once and answers
    `ServiceUnavailable`.
  - A transaction of 127 updates commits; 128 is rejected.
  - Old and new commits (with and without intent) interleave.
- **Benchmark:** `BenchmarkStore` and a catalog commit benchmark, before and
  after intents.

## Step 4: Metrics

- Registries in the three `main` functions; metric structs in each package,
  nil-safe (D8).
- The store and file IO decorators (shared with Step 5).
- `kv-node`'s metrics listener with `/healthz` and `/readyz`.
- Tests with `prometheus/testutil`:
  - Each counter moves on its event: a commit of each outcome, a replay,
    a purge, a leader change, a dropped proposal, a snapshot sent.
  - `/metrics` is served and is not logged; probes remain unaffected.
  - A lint-like test that walks the registry and fails on any label outside
    an allow-list, so a table name never becomes a label.

## Step 5: Tracing

- The provider setup from `OTEL_*`, shared by the three binaries
  (`internal/telemetry`); the `slog` wrapper.
- `otelhttp` on the API, `otelgrpc` on the KV client and server, the spans
  of D9.
- Tests with the SDK's in-memory exporter:
  - One table commit produces a single trace from the HTTP span through the
    catalog, S3 and store spans to the `kv.propose` span on the node.
  - An incoming `traceparent` is continued.
  - With no `OTEL_*` set, no spans are recorded and no exporter starts.
  - Log records inside a request carry the trace ID.

## Step 6: `catalogctl restore-report`

- `cmd/catalogctl` with `restore-report` (D7): `-tables`, `-deep`,
  `-scan-warehouse`, `-emit-commands`, JSON output.
- `FileIO` gains `List(ctx, prefix)`, implemented by the S3 adapter and the
  catalog's in-memory test double; the report needs it to find newer
  metadata files.
- Tests on MinIO fixtures and in memory:
  - `ahead` with a chain of three commits, rolled forward with the emitted
    command, then committed to by Trino.
  - `ambiguous` with two children of one parent.
  - `missing-metadata`, `missing-files` and `unregistered`.
  - A table with nothing to report.

## Step 7: Stacks and drills

- **Compose:** the zone overlay (D11) and the observability overlay (D10);
  bucket versioning and the backup bucket in `minio-init`.
- **Make targets:**

  | Target | What |
  |--------|------|
  | `make obs-up`, `make obs-down` | HA + zones + observability, for manual use |
  | `make dr-e2e` | The drills below, then remove every container and volume |

- **Drills** (`test/dr`, build tag `dr`, per-run schema as in `test/ha`):
  1. **Zone loss:** PyIceberg and Trino write continuously; kill zone `a`
     (2 of 5 nodes). No errors beyond retries, no lost or duplicated rows.
     Then kill one more node in zone `b`: writes stop (quorum lost) and
     `/readyz` turns 503 on both catalogs. Bring the nodes back: writes
     resume, every acknowledged row present once.
  2. **Backups:** backups appear every interval with manifests; retention
     with a small `keep-last` removes old ones; `kv-backup verify` passes.
  3. **Total loss and restore:** write a known state, wait for a backup,
     commit more (including a new table and a namespace property change),
     then kill every kv-node and delete their volumes. Follow the runbook
     step by step, as scripted commands: `kv-backup fetch`, `kvctl snapshot
     restore` for each node, start the nodes, start the catalogs with
     `-orphan-cleanup=false`. Measure the RTO from the kill to the first
     successful commit.
  4. **Report and roll-forward:** `restore-report` finds the tables with
     commits after the backup as `ahead` and the new table as
     `unregistered`; roll them forward and register it; Trino reads every
     acknowledged row. The namespace property change is the documented
     loss. Turn the janitor on: no file that a pointer needs is deleted.
  5. **The old cluster stays out:** start one old kv-node data directory
     (kept from before the deletion, in a copy) against the restored
     cluster: it is rejected and the restored cluster is unaffected.
  6. **Unsafe recovery:** stop all nodes, keep one node's volume, recover
     from it with `kvctl snapshot save -data-dir`, restore a 3-node cluster;
     data present up to that node's applied index.
  7. **Orphans under faults:** the Phase 3 kill scenarios (catalog replica,
     raft leader) with commits in flight; after the grace period, every
     metadata file under each table's `metadata/` directory is the current
     one or in its metadata log.
  8. **Observability:** scrape every target and check that the key series
     exist and moved (commits, leader changes, backups); fetch from Jaeger's
     API a trace of a Trino commit that contains catalog and kv-node spans;
     stop a majority and see `kv_raft_leader_id` drop to 0 on the survivor
     and `readiness` turn 0 on both catalogs.
- `make ha-e2e` keeps passing with the new timeouts and intents.

## Step 8: Documentation

- **`arch/phase5/adr-006-operations.md`:** the decisions above; measured
  RPO and RTO per drill; the commit latency cost of intents; any change
  from the plan.
- **`docs/operations.md`** (new), the operator's guide:
  - deployment rules: zones, node counts, the backup bucket, bucket
    versioning;
  - a runbook per failure class of D4, each written as commands to run, in
    order, with how to verify the result and the metrics that show it;
  - backups: schedule, retention, verification, `list` and `fetch`;
  - restore, the report and roll-forward;
  - the metrics reference and the trace layout;
  - orphan cleanup: what it removes and what it never removes.
- **`README.md`:** the new binaries, the overlays and make targets, the
  configuration table.
- **`arch/plan.md`:** mark Phase 5 done; record TLS as moved out.

---

## Order of work

1. Step 0 spikes; adjust the plan where a dependency falls short.
2. Precise store failures and intents, with their contract and catalog tests
   (Step 3): they change the commit path, so they go in first and every later
   test runs on them.
3. The snapshot format, the RPC and `kvctl snapshot` with the in-process
   restore tests (Step 1).
4. `kv-backup` (Step 2).
5. Metrics (Step 4), then tracing (Step 5); the decorators once, for both.
6. `catalogctl restore-report` (Step 6).
7. The compose overlays and the dashboard (Step 7).
8. The drills, fixing what they find, each fix with a Go test (Step 7).
9. ADR-006 and the operator's guide, written while running the runbooks by
   hand once (Step 8).

## Definition of done

- `make ci` passes; the contract suite covers intents and the new error on
  both backends.
- `make e2e` and `make ha-e2e` pass with intents and the new timeouts.
- `make dr-e2e` passes twice in a row, and removes its containers and
  volumes.
- Every runbook in `docs/operations.md` has been followed by hand once on
  the local stack, and the measured RPO and RTO are in ADR-006.
- A trace of a Trino commit in Jaeger shows the path from the HTTP request
  to the raft proposal.
- ADR-006 is written; `README.md` and `arch/plan.md` are updated.

## Risks

| Risk | Mitigation |
|------|------------|
| A restore that starts but is subtly wrong (old members, stale conf state, revision going backwards) | Step 0 spike on the raft mechanics first; in-process restore tests check members, cluster ID and revision; the drill restores daily through `kv-backup verify` |
| An old node rejoins a restored cluster and mixes logs | A new cluster ID per restore; the existing peer check rejects it; drill 5 |
| Backups silently stop or become unreadable | Backup freshness and the last verification on the dashboard and in `kv-backup list`; `/healthz` of `kv-backup` fails after 3 missed intervals; daily test restore; manifests written last |
| Retention deletes the only good backup | The newest complete backup is never deleted; retention runs only after a successful upload; unit tests over synthetic histories |
| The janitor deletes a file a commit still needs | Condemn first: a commit with a condemned intent cannot land; no clock involved; tests for the race and for the interrupted janitor |
| The janitor deletes files that roll-forward needs after a restore | `-orphan-cleanup=false` in the runbook until the report is done; drill 4 |
| An extra KV write per commit slows commits | Measured before and after; ≈ 1–2 ms against an S3 write of tens of ms |
| The longer write timeout breaks a client that gives up sooner | Step 0 checks the clients' timeouts; the budget formula and its defaults are documented in ADR-006 |
| Restored metadata references files deleted after the backup | Bucket versioning required; `restore-report -deep` finds them |
| Metric cardinality grows with tables | No table labels; the registry test enforces an allow-list |
| Tracing adds latency or allocations when off | A no-op provider unless configured; a benchmark with tracing off and on |
| Observability images can't be pulled | Pulled by hand and pinned by digest, as with HAProxy and the JDK image |
| "Zone loss" on one machine is not a real zone outage | Documented: the drills prove the logic of placement and quorum, not network behaviour between data centres |

---

## Implementation notes (2026-11-15)

Phase 5 is implemented; the decisions and results are in
`adr-006-operations.md`, the operator's side in `docs/operations.md`. What
Step 0 found, and what changed from the plan:

- **Step 0 results:**
  - The newest releases accept Go 1.26.2: `prometheus/client_golang`
    v1.25.0, OpenTelemetry v1.47.0, `otelhttp` and `otelgrpc` v0.72.0.
  - Restore mechanics: a log reset to the snapshot's index and term, with
    the hard state committed there and a new conf state, restarts with
    `raft.RestartNode` and elects a leader; proven by in-process restores
    onto 1, 3 and 5 nodes.
  - Pebble v2.1.7 gives `DiskSpaceUsage()`, `WAL.PhysicalSize`,
    `Compact.EstimatedDebt` and `ReadAmp()`.
  - iceberg-go's S3 layer streams multipart uploads and lists with
    `WalkDir`, so `kv-backup` and `catalogctl` use it instead of the AWS SDK
    directly, and tests use its `mem://` store. It has no server-side
    encryption setting, so there is no `-sse` flag.
  - Images pinned: Prometheus 3.16.0, Grafana 13.3.1, Jaeger 2.23.0 (whose
    query API is `/api/v3`).
  - Not checked: how Java, PyIceberg and Trino treat a `503
    ServiceUnavailable` on commit, and their commit timeouts against 90 s.
- **Changed from the plan:**
  - The snapshot trailer holds the body's size, not a record count.
  - `kv-backup -location` replaces `-bucket` and `-prefix`.
  - `kvctl status` gained the zone column and the placement warning; there
    is no separate `-all` flag, since it already queries every endpoint.
  - `readiness{reason}` became `catalog_ready`, without a label.
  - Idempotency metrics have no `takeover` result: the takeover happens
    inside the guard, which the HTTP layer does not see.
  - `kv-backup` runs from the `kv-node` image; `kv-backup probe` serves the
    container health check.
- **Bugs found by the tests:** directories of `s3://` locations compared
  with `path.Dir`, which collapses the `//`; commit attempts that did not
  receive the commit span's context; the HA suite's transaction deadline,
  shorter than the new idempotency lease.
- **Drills:** every drill passed, but `make dr-e2e` was not completed as one
  full run, and not twice in a row; the confirming runs were stopped on
  request. See ADR-006, Verification.
