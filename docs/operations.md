# Operating echpochmak

This guide is for the people who run the catalog: how to deploy it so it
survives failures, what to do when one happens, and how to see what the
system is doing. The design behind it is in `arch/phase5/plan.md` and
`arch/phase5/adr-006-operations.md`.

The commands are written for the compose stack of this repository
(`make obs-up`: 5 kv-nodes in 3 zones, two catalogs behind HAProxy,
`kv-backup`, Prometheus, Grafana and Jaeger). On other platforms the same
binaries take the same flags; only the way to start a container differs.

```
DR="docker compose -f deployments/compose/docker-compose.yaml \
  -f deployments/compose/docker-compose.ha.yaml \
  -f deployments/compose/docker-compose.zones.yaml \
  -f deployments/compose/docker-compose.obs.yaml \
  --env-file deployments/compose/.env --profile kv"
FIVE=1=http://kv1:2380,2=http://kv2:2380,3=http://kv3:2380,4=http://kv4:2380,5=http://kv5:2380
```

`kvctl` and `kv-backup` are in the `kv-node` image, `catalogctl` in the
`catalog-server` image. The images have no shell: run the binaries directly,
for example `docker exec echpochmak-kv-backup-1 /kv-backup list`, or
`$DR run --rm --no-deps --entrypoint /kvctl kv1 …` for a node that is
stopped.

---

## 1. Deployment rules

### The KV cluster

- **An odd number of voters**: 3 or 5. A cluster of `n` voters keeps
  working while a majority, `n/2 + 1`, is up.
- **At most `(n-1)/2` voters per zone**, so no single zone holds a majority:
  - 3 nodes in 3 zones survive the loss of one zone;
  - 5 nodes in 3 zones (2/2/1) survive the loss of one zone, or of any two
    nodes;
  - **2 zones are never enough**: losing the larger one loses the quorum.
- Label each node with its zone, `kv-node -zone <name>`. `kvctl status`
  prints a warning when one zone holds a majority of the nodes listed, and
  Grafana's *Voters per zone* panel shows the same.
- On Kubernetes the same rules are a topology spread constraint over
  `topology.kubernetes.io/zone` with `maxSkew: 1` and a pod disruption budget
  of one.

### Backups

- Run **one `kv-backup` per cluster**. Two would only take duplicate backups.
- Give it **its own bucket**, in another region or account than the
  warehouse in production, so a mistaken lifecycle rule or a lost region
  takes one of the two, not both. Turn on the bucket's default encryption:
  `kv-backup` relies on it rather than encrypting itself.
- Keep the **warehouse bucket versioned**, with noncurrent versions kept at
  least as long as the backup retention (14 days by default). A restored
  catalog points at the metadata of the backup's time; versioning keeps the
  files that were deleted since then recoverable (section 4).

### Catalogs

- Two or more replicas behind a load balancer that checks `/readyz`. A
  replica that loses the KV quorum reports `503` there and is taken out.
- `http-write-timeout` must exceed `(commit-retries + 1) × 3 ×
  kv-request-timeout`; the server refuses to start otherwise. The defaults
  (90 s; 4; 5 s) satisfy it. The load balancer's timeouts must be longer
  (HAProxy here: 120 s).

---

## 2. Targets

RPO is what a failure loses; RTO is how long writes are refused.

| Failure | RPO | RTO target | Runbook |
|---------|-----|------------|---------|
| One kv-node, or the raft leader | 0 | ≤ 3 s | 3.1 |
| One zone | 0 | ≤ 3 s | 3.1 |
| A catalog replica | 0 | 0 for new requests | 3.2 |
| Quorum lost, nodes come back | 0 | until they are back | 3.3 |
| Quorum lost for good, one node left | that node's replication lag (ms) | ≤ 15 min | 3.4 |
| Every kv-node and disk lost | table commits ≈ 0 after roll-forward; the rest ≤ one backup interval | ≤ 15 min | 3.5 |
| Operator error, such as a wrong drop | changes after the chosen backup | ≤ 30 min | 3.6 |

ADR-006 records what the drills (`make dr-e2e`) measured on one machine.

---

## 3. Runbooks

Each runbook starts with the signs, then lists the steps in order, then how
to check the result.

### 3.1 A kv-node or a zone is down

**Signs.** `kvctl status` shows the node with an error;
`echpochmak_kv_raft_leader_changes_total` moved; *Nodes up* in Grafana
dropped. Writes continue.

1. Nothing is urgent while a majority is up. Find out why the node stopped
   (`docker logs echpochmak-kv1-1`).
2. Start it again: `docker start echpochmak-kv1-1`. It restarts from its
   disk and catches up from the leader, with a snapshot if it fell far
   behind (`echpochmak_kv_snapshots_received_total`).
3. If its disk is lost, replace the node: remove it with
   `kvctl member remove <id>`, add a new one with a new ID
   (`kvctl member add <id> <peer-url>`), start it with `-join`, and promote
   it once it has caught up (`kvctl member promote <id>`).

**Check.** `kvctl status` lists every node with the same leader and the
applied index close to the commit index; *Apply lag* is near zero.

### 3.2 A catalog replica is down

**Signs.** HAProxy routes around it; `echpochmak_catalog_ready` of that
replica is 0 or missing.

1. Restart it. It holds no state.
2. Requests that were running on it fail; clients retry them. Retries with
   an `Idempotency-Key` are answered by the other replica, after the dead
   replica's claim expires (twice the write timeout) if it held one.
3. Files of commits it was running are cleaned up by the janitor of any
   replica after the orphan grace period (section 6).

**Check.** `curl localhost:18181/readyz` answers `{"status":"ok"}`.

### 3.3 The quorum is lost, the nodes will come back

**Signs.** Both catalogs answer `503` on `/readyz`; writes and reads fail;
`echpochmak_kv_raft_leader_id` is 0 on the nodes that are up.

1. **Do not restore from a backup.** Every acknowledged write is on the
   nodes' disks; a restore would lose the newest ones.
2. Bring enough nodes back for a majority, in any order.
3. Nothing else: the nodes elect a leader, the catalogs report ready again.

**Check.** `kvctl status` shows a leader; `/readyz` answers `200` on both
catalogs.

### 3.4 The quorum is lost for good, one node is left (unsafe recovery)

Use this only when the other nodes will never return, for example two of
three zones are gone. The surviving node may lack the last entries the lost
majority committed: this recovery can lose acknowledged writes, usually
milliseconds' worth.

1. Stop the surviving node, then export its state offline:

   ```
   docker stop echpochmak-kv3-1
   mkdir -m 777 /tmp/restore
   $DR run --rm --no-deps -v /tmp/restore:/restore --entrypoint /kvctl kv3 \
     snapshot save /restore/kv3.snap -data-dir /data
   ```

2. Build a new cluster from the file: section 3.5, from step 3, with
   `kv3.snap` instead of `latest.snap`.

**Check.** As in 3.5.

### 3.5 Every kv-node and disk is lost: restore from a backup

**Signs.** The KV cluster is gone; the catalogs are unready.

1. **Stop the backup service**, so it does not back up the empty cluster
   you are about to build: `docker stop echpochmak-kv-backup-1`.
2. **Fetch the newest backup** and check it:

   ```
   mkdir -m 777 /tmp/restore
   $DR run --rm --no-deps -v /tmp/restore:/restore --entrypoint /kv-backup \
     kv-backup fetch /restore/latest.snap
   ```

   `kv-backup list` shows every backup; `fetch -at 2026-11-15T10:00:00Z`
   picks the newest one at or before a time.
3. **Remove what is left of the old nodes**, containers and volumes:

   ```
   $DR rm -fsv kv1 kv2 kv3 kv4 kv5
   docker volume rm echpochmak_kv1-data echpochmak_kv2-data \
     echpochmak_kv3-data echpochmak_kv4-data echpochmak_kv5-data
   ```

4. **Restore every node** from the same file, with the same member list:

   ```
   for id in 1 2 3 4 5; do
     $DR run --rm --no-deps -v /tmp/restore:/restore:ro --entrypoint /kvctl kv$id \
       snapshot restore /restore/latest.snap -id $id -data-dir /data -initial-cluster $FIVE
   done
   ```

   The restored cluster gets a **new cluster ID**: a node of the old
   cluster that comes back is rejected by its peers and can never mix old
   entries in. Its **revision jumps** by 1,000,000,000, so no revision the
   old cluster handed out is reused. A restore that is interrupted leaves a
   `RESTORE_IN_PROGRESS` marker; such a node refuses to start, and the next
   restore refuses the directory until it is emptied.
5. **Start the nodes**: `$DR up -d --no-deps kv1 kv2 kv3 kv4 kv5`.
6. **Start the catalogs without the orphan janitor**, so it cannot delete
   files that the roll-forward needs:

   ```
   ORPHAN_CLEANUP=false $DR up -d --no-deps --force-recreate catalog-a catalog-b
   ```

7. **Run the restore report** and roll forward (section 4).
8. **Turn the janitor back on**:
   `$DR up -d --no-deps --force-recreate catalog-a catalog-b`.
9. **Start the backup service** again: `docker start echpochmak-kv-backup-1`.
   Its backups now go under the new cluster ID.

**Check.** `kvctl status` shows 5 nodes and a leader; `/readyz` answers
`200`; the report of step 7, run again, finds nothing; Trino reads the
tables.

### 3.6 An operator error: restore an older state

A table dropped by mistake, a namespace emptied: restore the backup from
before the mistake, then roll forward the tables that should keep the
changes made since.

1. Find the backup: `docker exec echpochmak-kv-backup-1 /kv-backup list`.
2. Follow 3.5 with `fetch -at <time just before the mistake>`.
3. In the restore report, roll forward every table **except** the ones the
   mistake concerned.

A purge (drop with `purgeRequested`) deletes the table's files; only bucket
versioning brings them back.

---

## 4. After a restore: the report and roll-forward

A restored catalog points every table at its metadata as of the backup. The
metadata files of the commits since are still in object storage, and each
names its predecessor, so the catalog can be moved forward to them.

```
docker exec echpochmak-catalog-a-1 /catalogctl restore-report -scan-warehouse
```

| Finding | Meaning | What to do |
|---------|---------|------------|
| `ahead` | newer metadata files continue the current one in a single chain | roll forward to the newest |
| `ambiguous` | several files continue the same one; one was a commit that never landed | choose by the snapshot summaries in the files, then register the one you chose |
| `missing-metadata` | the pointer's metadata file is gone | recover it from bucket versioning, or drop the table |
| `missing-files` (with `-deep`) | a manifest list or manifest of the current snapshot is gone | recover from bucket versioning, or roll the table back to an older snapshot |
| `unregistered` (with `-scan-warehouse`) | a metadata directory no pointer leads to: a table created after the backup | register it under its name |

`-emit-commands -catalog-url http://catalog:8181` prints the `curl` calls
for the `ahead` and `unregistered` findings; the report itself never
changes anything. A roll-forward is the Iceberg `register` call with
`overwrite`:

```
curl -fsS -X POST http://localhost:8181/v1/main/namespaces/db/register \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","metadata-location":"s3://…/00012-….metadata.json","overwrite":true}'
```

What a restore loses for good: namespace creates, drops and property
changes, view changes, and table drops made after the backup.

---

## 5. Backups

`kv-backup run` backs up every `-interval` (5 min by default, jittered):

1. It takes a consistent snapshot from a follower, holding every write
   acknowledged before the call.
2. If the revision has not moved since the latest backup, it uploads nothing.
3. It streams the snapshot to `<location>/<cluster-id>/<time>-r<revision>.snap`
   and then writes the manifest next to it, `….snap.json`. A snapshot
   without a manifest is incomplete: ignored, and deleted after an hour.
4. Retention keeps the newest `-keep-last` backups (48) and the newest of
   each of the last `-keep-daily` days (14), always the newest of all, and
   only touches its own cluster's backups.
5. Every `-verify-interval` (24 h) it downloads the newest backup and
   restores it into memory, to prove it can be restored.

Commands: `kv-backup now` (one backup, before a risky change), `list`,
`fetch [-at TIME] FILE`, `verify [SNAPSHOT]`. `kvctl snapshot save FILE`
takes a snapshot without the service, and `kvctl snapshot status FILE`
checks a file and prints its header.

**Watch**: *Since last backup* and *Since last verification* in Grafana, or
`time() - echpochmak_backup_last_success_timestamp_seconds`. The service's
`/healthz` fails after three intervals without a successful backup, so its
container turns unhealthy.

---

## 6. Orphan cleanup

A commit writes the new metadata file first and moves the pointer second.
Before writing, it records an **intent** naming the file; the commit
removes the intent in the same atomic write that moves the pointer. An
intent that outlives its commit names a file no pointer leads to.

The janitor on every replica sweeps every `-orphan-interval` (1 min). An
intent older than `-orphan-grace` (twice the write timeout) is first
**condemned**, which makes a commit still holding it fail, then its files
are deleted, then the intent is removed. No clock decides safety: a file is
deleted only after no commit can make it current.

What it removes: metadata files of commits that lost their outcome, whose
replica died, or whose cleanup failed. What it never removes: data files
(engines clean those, for example Trino's
`ALTER TABLE … EXECUTE remove_orphan_files`), and metadata files that
dropped out of a table's metadata log after a failed delete
(`echpochmak_catalog_expired_metadata_delete_failures_total` counts those).

**Watch**: `echpochmak_orphan_intents_pending` stays small;
`echpochmak_orphan_intents_oldest_seconds` stays below a few grace periods.
Turn the janitor off with `-orphan-cleanup=false` only after a restore,
until the report is done.

---

## 7. Metrics

Every metric is prefixed `echpochmak_`. Labels are bounded: never a table
name, a key or a request ID; those are in traces and logs.

### catalog-server (`GET /metrics` on the API port)

| Metric | Labels | Meaning |
|--------|--------|---------|
| `http_requests_total`, `http_request_duration_seconds` | route, method, status | requests by route template |
| `catalog_commits_total` | kind, outcome | commits: `committed`, `conflict`, `requirement_failed`, `rejected`, `unavailable`, `state_unknown` |
| `catalog_commit_duration_seconds`, `catalog_commit_attempts` | kind | retries included; more than one attempt means lost races |
| `catalog_metadata_io_seconds`, `catalog_metadata_io_errors_total` | op | metadata reads, writes, deletes in object storage |
| `catalog_metadata_cache_requests_total` | result | metadata cache hits and misses |
| `store_call_duration_seconds`, `store_call_errors_total` | op, error | store calls |
| `kv_client_attempts_total` | result | KV attempts: `ok`, `retried`, `unknown`, `failed` |
| `idempotency_requests_total` | result | `first`, `replayed`, `mismatch`, `busy`, `unavailable` |
| `catalog_ready` | | 1 if the last readiness probe passed |
| `purge_*`, `orphan_*` | | the purge queue and the janitor |
| `catalog_expired_metadata_delete_failures_total` | | old metadata files that could not be deleted |

### kv-node (`GET /metrics` on `-listen-metrics`, :2381)

| Metric | Meaning |
|--------|---------|
| `kv_node_info{node, zone}` | the node's ID and zone |
| `kv_raft_term`, `kv_raft_leader_id`, `kv_raft_is_leader` | raft state; leader 0 means none known |
| `kv_raft_leader_changes_total` | leader changes this node observed |
| `kv_raft_commit_index`, `kv_raft_applied_index` | their difference is the apply lag |
| `kv_raft_follower_match_lag{follower}` | on the leader: how far each follower trails |
| `kv_proposals_total{result}`, `kv_proposal_duration_seconds` | writes: `applied`, `dropped`, `timed_out`, `invalid` |
| `kv_read_index_duration_seconds` | linearizable read confirmation |
| `kv_wal_fsync_seconds`, `kv_apply_batch_seconds`, `kv_ready_loop_seconds` | the raft loop |
| `kv_snapshots_sent_total{result}`, `kv_snapshots_received_total` | catching up slow followers |
| `kv_log_entries`, `kv_log_compactions_total` | the raft log |
| `kv_transport_queue_length{peer}`, `kv_transport_dropped_total{peer}` | back-pressure to peers |
| `kv_grpc_requests_total{method, code}`, `kv_grpc_duration_seconds{method}` | the client API |
| `kv_pebble_disk_usage_bytes`, `kv_pebble_wal_bytes`, `kv_pebble_compaction_debt_bytes`, `kv_pebble_read_amplification` | the database |

The listener also serves `/healthz` (the process runs) and `/readyz` (a
leader is known and the node has applied nearly everything committed).

### kv-backup (`GET /metrics` on `-listen-metrics`, :2382)

`backup_last_success_timestamp_seconds`, `backup_last_revision`,
`backup_bytes`, `backup_duration_seconds`, `backup_failures_total{stage}`,
`backup_skipped_total`, `backup_verify_last_success_timestamp_seconds`,
`backup_retained`.

Every binary also exports Go runtime and process metrics and
`echpochmak_build_info{component, version, revision, go_version}`.

### What to look at per failure

| Question | Look at |
|----------|---------|
| Is a node down? | `up{job="kv"}`, *Nodes up* |
| Is there a leader? | `kv_raft_leader_id` |
| Is the cluster unstable? | `rate(kv_raft_leader_changes_total[10m])` |
| Is a node falling behind? | apply lag, `kv_raft_follower_match_lag` |
| Is the disk slow? | `kv_wal_fsync_seconds` p99 |
| Are commits failing? | `catalog_commits_total` by outcome; any `state_unknown` |
| Are clients racing? | `outcome="conflict"`, `catalog_commit_attempts` |
| Are backups current? | *Since last backup*, *Since last verification* |
| Are files piling up? | `orphan_intents_oldest_seconds`, `purge_queue_oldest_seconds` |

---

## 8. Traces

Tracing uses the standard OpenTelemetry environment variables; it is off
unless an OTLP endpoint is set:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
OTEL_EXPORTER_OTLP_INSECURE=true
OTEL_TRACES_SAMPLER=parentbased_traceidratio   # optional
OTEL_TRACES_SAMPLER_ARG=0.1                    # optional; the default samples everything
```

A table commit is one trace:

```
POST /v1/{prefix}/namespaces/{namespace}/tables/{table}      catalog-server
└─ catalog.commit.table   (attempts, outcome)
   ├─ store.GetTable ─ echpochmak.kv.v1.KVService/Range ─ kv.read_index   kv-node
   ├─ store.CreateIntent ─ KVService/Txn ─ kv.propose
   ├─ fileio.read, fileio.write   (location, bytes)
   └─ store.CommitTables ─ KVService/Txn ─ kv.propose
```

An incoming W3C `traceparent` is continued. Every log line written while
handling a traced request carries `trace_id` and `span_id`, next to the
`request_id`, so a log line leads to its trace and back. Probes and
`/metrics` are not traced.
