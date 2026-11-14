# ADR-004: The catalog on the raft KV store, with replicas

- **Status:** accepted
- **Date:** 2026-11-11
- **Context:** Phase 3 (`arch/phase3/plan.md`)

## Context

Phase 1 kept the catalog's state in memory, so it was lost on restart and
only one replica could run. Phase 2 built a replicated, linearizable KV
cluster. Phase 3 connects the two:
- `raftstore` implements the catalog's `Store` interface on the KV client.
- Two or more stateless catalog replicas run behind a load balancer.
- `Idempotency-Key` retries work whichever replica they reach.

## Decisions

### Key layout

Every catalog key is under `cat/v1/`. The version leaves room for a later
migration. A key puts `\x00` between the parent namespace (levels joined by
`\x1f`) and the name.

| Entity | Key | Value |
|--------|-----|-------|
| Namespace | `cat/v1/ns/<parent>\x00<name>` | `{"properties": {...}}` |
| Table | `cat/v1/tbl/<namespace>\x00<name>` | `{"metadata_location": "..."}` |
| View | `cat/v1/view/<namespace>\x00<name>` | `{"metadata_location": "..."}` |
| Idempotency record | `cat/v1/idem/<key>` | record JSON |
| Idempotency expiry index | `cat/v1/idemexp/<expiry, big-endian ns>/<key>` | empty |

- **Children are one prefix scan.** The direct children of a namespace `P`
  are exactly the keys with prefix `ns/<P>\x00`. Validated names contain no
  control characters, so grandchildren, and siblings such as `ab` next to
  `a`, never match. A unit test pins these boundaries.
- **Versions come from the KV store.** A `Store` version is the key's
  `mod_revision`: store-wide, only growing, never reused.
- **Values are JSON**, readable with `kvctl get cat/v1/ -prefix`.

### One transaction per write, failures read in the same transaction

Every `Store` write is one KV `Txn`:
- Its **compares** are the operation's preconditions.
- Its **failure branch reads** the keys involved.

So when a write fails, the adapter knows from the very state that made it
fail whether the namespace was missing, the name taken by a table or a view,
or a version stale. It never needs a second read, which could race. Failures
are classified in the same order as `memstore`. For example, the first
failing commit of a multi-table commit is the one reported.

| Operation | Compares | On failure |
|-----------|----------|-----------|
| Create namespace | the namespace key is absent; the parent exists | exists → already exists; otherwise parent not found |
| Drop namespace | the key exists; `range_count == 0` over child namespaces, tables and views | missing → not found; otherwise not empty |
| Update properties | read, merge, then compare-and-swap on `mod_revision` | retried up to 8 times on a lost race |
| `CommitTables` | per commit — create: table key and view key absent, namespace present; update: `mod_revision == expected` | the first failing commit, in order |
| Rename | source at the revision read; target namespace present; target table and view keys absent | retried while only the source moved |
| List | the parent exists, in a **read-only** `Txn` with the range | parent missing → not found |

Two consequences:
- **Emptiness is atomic.** "Drop a namespace only if it is empty" cannot
  race with a concurrent create: the three range counts are evaluated in the
  transaction that deletes it.
- **Transaction size limit.** One `CommitTables` call carries at most
  `MaxCommitsPerTxn` = 42 commits: a create needs 3 compares, and the KV
  store allows 128 per branch. Larger calls get `ErrInvalidArgument`.
  Phase 4's multi-table endpoint must respect this or split.

### Errors and timeout budgets

| KV client error | `Store` error |
|-----------------|---------------|
| `ErrOutcomeUnknown` | `ErrUnavailable` (wrapping it) → the catalog answers `CommitStateUnknown` and keeps the metadata file |
| `ErrUnavailable` | `ErrUnavailable` — reported as "state unknown" too, which is safe; at worst it leaves an orphaned metadata file for Phase 5 |
| `ErrInvalidArgument` | `ErrInvalidArgument` |

Three layers retry: the KV client, the catalog's commit retries, and the
engine. Their budgets must nest, or the server's eventual success reaches a
client that has already given up. `catalog-server` refuses to start unless:

```
http-write-timeout > (commit-retries + 1) × 2 store calls × kv-request-timeout
```

With the defaults that is 60 s > 5 × 2 × 5 s. The plan proposed
`kv-request-timeout` = 15 s and `kv-attempt-timeout` = 5 s. Those don't nest
under the 60 s write timeout, so the defaults became 5 s and 2 s. A KV
leader election takes about a second, well inside both.

### Readiness and draining

- **`/readyz`** is a linearizable read of the KV store. A replica cut off
  from the KV quorum reports 503, and HAProxy stops routing to it.
- **`/healthz`** never touches the store, so a KV outage doesn't make an
  orchestrator restart healthy catalog processes.
- **On SIGTERM** a replica:
  1. Reports 503 on `/readyz` for `http-drain-delay` (3 s in the HA stack;
     HAProxy checks every 1 s and needs 2 failures).
  2. Keeps serving during that time.
  3. Then drains in-flight requests.

### Idempotency-Key, shared across replicas

A retry may reach a different replica, so records live in the KV store
(`internal/idempotency`).

1. **Claim:** the first attempt creates an "in progress" record with a
   lease of twice the HTTP write timeout (`mod_revision == 0` compare).
2. **Another attempt** that finds the record:
   - **Finalized:** it replays the stored response, marked
     `Idempotent-Replay: true`.
   - **In progress, lease live:** it gets `503`, so it retries later.
   - **Lease expired:** it takes the claim over, because the replica that
     held it died.
3. **Key reuse:** a key reused with a different method, path or body gets
   `422`.
4. **Finishing:**
   - **Final** responses (2xx, and 4xx other than 408 and 429) are stored.
   - **5xx** responses delete the claim, so a retry runs again.
   - A janitor on every replica deletes expired records.

Decisions inside the protocol:
- **Table metadata is not stored.** Create and commit responses carry the
  whole table metadata, which can be megabytes and exceed what a KV entry
  should hold. Their replay serves a fresh `GET` of the table instead. The
  spec allows this: a replay "may reflect a newer state of the catalog".
  Other responses are stored verbatim, up to 64 KiB; a larger one is not
  finalized.
- **Re-running after a takeover is safe** for every operation in scope:
  - Creates and drops fail definitively when repeated.
  - Commits re-check their requirements, so a commit that already landed
    fails with 409 instead of applying twice.
- **Staged creates bypass the protocol:** they have no effect to repeat.
- **Placement:** the middleware runs on the raw request, before the
  generated decoder. A malformed request's 400 is final and replayed,
  which is deterministic.

### The HA stack

`deployments/compose/docker-compose.ha.yaml` overlays the base stack:
- 3 kv-nodes.
- Two catalog replicas on the raft backend (`catalog-a` on 18181,
  `catalog-b` on 18182).
- HAProxy 3.4, pinned by digest.
- PyIceberg 0.12.0.

HAProxy answers to the network name **`catalog`** on port 8181, so Trino's
configuration and the Phase 1 Trino suite reach the load balancer
unchanged. HAProxy re-resolves replicas through Docker's DNS, because a
restarted replica gets a new address. It retries a request on the other
replica only when the connection failed: then the request certainly never
reached a catalog.

The plan proposed a compose profile. An overlay file was chosen because it
can switch off the base file's single in-memory catalog and redirect Trino,
which a profile alone cannot.

## Defects found by the tests

1. **Draining cancelled the requests it was meant to finish.** The HTTP
   server's base context was the signal context, so on SIGTERM every
   request in flight, and every request served during the drain delay, had
   its context cancelled. They failed with 500s. The rolling-restart
   scenario found it. Request contexts are now independent of the signal;
   `Shutdown` alone ends them.
2. **Writer starvation, not a catalog fault.** One HA run failed: PyIceberg
   could not commit for 120 s while Trino committed to the same table every
   ~300 ms. Every PyIceberg commit lost the compare-and-swap, and the
   catalog correctly answered 409. Optimistic concurrency gives no fairness
   between writers. The fault scenarios now give each writer its own table;
   contention on one table stays covered by the baseline scenario, the
   conformance suite and the Trino suite.
3. **Test failures reported on the wrong test.** The HA suite's helpers
   reported failures on the top-level test, so a failing scenario showed as
   passing while the parent failed. The helpers are now bound to each
   scenario.

## Verification

| Check | Result |
|-------|--------|
| `storetest` contract suite on `raftstore` (3 nodes, real gRPC) | Pass: the same 71 subtests as `memstore` |
| `raftstore` fault tests: leader stop mid-commit, two replicas racing, error precedence, transaction size | Pass |
| iceberg-go `catalogtest` conformance, plus append-and-scan and concurrent appends | Pass on both the memory and raft backends |
| Idempotency: store contract (memory and KV), protocol, HTTP across two replicas | Pass |
| **Milestone, `make ha-e2e`:** baseline; catalog replica killed; raft leader killed; both at once; rolling restart; idempotent retry through the other replica | **Pass in 2 consecutive runs:** every acknowledged row present exactly once, no duplicates, the pointer in the KV store names a metadata file in MinIO. The rolling restart had zero client errors; the kill scenarios had at most one retried error per writer. |
| Phase 1 Trino suite through HAProxy on the HA stack | Pass |
| Readiness by hand: KV quorum lost, then restored | 503 while lost, 200 afterwards; data intact |

`make ha-e2e` removes every container and volume afterwards.

## Latency

`BenchmarkStore` in `internal/store/raftstore`: a 3-node cluster over gRPC on
localhost, with in-memory disks, so fsync costs nothing. ADR-003 has the
cost on a real disk, about 2.3 ms per compare-and-swap.

| Operation | `memstore` | `raftstore` |
|-----------|-----------|-------------|
| Commit (pointer swap) | 1.2 µs | 1.10 ms |
| Get table pointer (linearizable) | 0.4 µs | 0.80 ms |

A table commit also writes a metadata file to S3, which dominates either
way.

## Known limitations

- **Orphaned metadata files:** a definite KV failure is reported as "state
  unknown", so the metadata file written before it is kept as an orphan.
  Phase 5 cleans up orphans; a narrower `Store` error could let the catalog
  delete the file right away.
- **No fairness between writers on one table:** a slow writer can lose
  every race against a fast one. Clients' retry limits bound the damage.
- **The idempotency janitor relies on clocks:** a record's expiry is
  wall-clock time on the replica that created it. Clock skew between
  replicas only shifts when records are deleted, by the skew.
- **No retry on server errors at the load balancer:** HAProxy retries only
  connection failures. A 5xx goes to the client, which retries itself, with
  an `Idempotency-Key` if it sent one.
