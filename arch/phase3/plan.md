# Phase 3: raftstore adapter and high availability

**Goal:** the catalog keeps its state in the Phase 2 KV cluster instead of
memory, and runs as several stateless replicas, so the system survives losing
any one catalog replica or any minority of KV nodes, with no data loss.

**Milestone (from `arch/plan.md`):** Trino and PyIceberg end-to-end tests pass
against MinIO, 3 kv-nodes and 2 catalog servers. Killing a catalog replica or
the raft leader mid-test causes no errors beyond retries.

**Builds on:**
- **Phase 1:** the catalog service, the `Store` interface and its contract
  suite (`internal/store/storetest`).
- **Phase 2:** the KV cluster and its Go client (`internal/kv/client`). The
  contract it offers Phase 3 is listed in
  `arch/phase2/adr-003-kv-cluster.md`, under "What Phase 3 can rely on".

---

## Scope

### In scope
- **`raftstore`:** an implementation of `store.Store` on the KV client that
  passes the same contract suite as `memstore`.
- **A `raft` store backend for `catalog-server`:** configuration, readiness
  and shutdown.
- **`Idempotency-Key`:** support for the mutations implemented so far, shared
  across replicas, and advertised in `/v1/config`.
- **A high-availability local stack:** 3 kv-nodes, 2 catalog replicas behind
  a load balancer, MinIO, Trino and PyIceberg.
- **End-to-end and chaos tests** for the milestone.

### Out of scope

| Item | Where |
|------|-------|
| Views, multi-table `transactions/commit`, register, purge | Phase 4. `Store` already supports multi-table commits and views; Phase 4 adds the HTTP endpoints. |
| Cleanup of orphaned metadata files | Phase 5 |
| Metrics, tracing, TLS | Phase 5 |
| Migrating data from `memstore` | Not needed: `memstore` holds nothing that survives a restart |

---

## How the pieces fit

```
Trino · PyIceberg
          │ Iceberg REST
          ▼
   load balancer (HAProxy), health-checks /readyz
      │                 │
 catalog-server 1   catalog-server 2     stateless; S3 for metadata files
      │ raftstore         │ raftstore
      └──── KV client ────┘  failover, request-ID retries
                │ gRPC :2379
     kv-node 1 · kv-node 2 · kv-node 3   (Phase 2)
```

Any replica can serve any request: there is no session or cache whose
correctness depends on hitting the same replica. The metadata-file cache is
safe to keep per replica because metadata files never change (Phase 1, D5).

---

## Design decisions

### D1: Key layout in the KV store

Every catalog key lives under a layout-versioned prefix, `cat/v1/`, which
leaves room for a later migration. A namespace path is encoded with the
`\x1f` level separator, as in Phase 1, and keys put a `\x00` byte between
the **parent** and the **name**. Validated names never contain control
characters, so these keys are unambiguous.

| Entity | Key | Value (JSON) |
|--------|-----|--------------|
| Namespace | `cat/v1/ns/<parent>\x00<name>` | `{"properties": {...}}` |
| Table pointer | `cat/v1/tbl/<namespace>\x00<name>` | `{"metadata_location": "..."}` |
| View pointer | `cat/v1/view/<namespace>\x00<name>` | `{"metadata_location": "..."}` |
| Idempotency record | `cat/v1/idem/<key>` | see D6 |
| Idempotency expiry index | `cat/v1/idemexp/<expiry, big-endian>/<key>` | empty |
| Readiness probe key | `cat/v1/ping` | never written |

Why this shape:
- **Direct children are one prefix scan.** The children of a namespace `P`
  are exactly the keys with the prefix `ns/<P>\x00`. Grandchildren have a
  different parent path, so they never match. The same holds for the tables
  and views of a namespace.
- **"Is this namespace empty?" is three `range_count == 0` compares** (child
  namespaces, tables and views), evaluated atomically in the transaction
  that deletes it.
- **`Version` is the key's `mod_revision`.** It is store-wide, only grows,
  and is never reused, which is exactly the `Store` contract.
- **Values are JSON** so they can be read with `kvctl get`. Each JSON
  document tolerates unknown fields, so later phases can add fields.

### D2: How each `Store` operation maps to the KV API

Every write is **one** `Txn`. Its compares encode the operation's
preconditions. Its failure branch *reads* the keys involved, so the adapter
can tell the caller exactly which precondition failed, without a second,
racy read.

| Operation | Compares (all must hold) | Success branch | Classifying a failure |
|-----------|--------------------------|----------------|-----------------------|
| `CreateNamespace` | the namespace key doesn't exist; the parent key exists (nested only) | put the namespace | exists → `ErrNamespaceAlreadyExists`; parent missing → `ErrNamespaceNotFound` |
| `DropNamespace` | the namespace key exists; `range_count == 0` over child namespaces, tables and views | delete the namespace | missing → `ErrNamespaceNotFound`; otherwise `ErrNamespaceNotEmpty` |
| `UpdateNamespaceProperties` | the namespace's `mod_revision` equals the value read | put the merged properties | a lost race is retried (read, merge, compare-and-swap), up to a few times; missing → `ErrNamespaceNotFound` |
| `CommitTables` | per commit — create: the table key and the view key don't exist, and the namespace exists; update: the table's `mod_revision == ExpectedVersion` | put every table pointer | checked in commit order, as `memstore` does: `ErrNamespaceNotFound`, `ErrTableAlreadyExists`, `ErrViewAlreadyExists`, `ErrTableNotFound`, or `CommitConflictError{Expected, Actual}` |
| `RenameTable` / `RenameView` | the source's `mod_revision` equals the value read; the target table and view keys don't exist; the target namespace exists | delete the source, put the target with the same location | if only the source moved (a concurrent commit), retry; otherwise the matching error |
| `DropTable` / `DropView` | the key exists | delete it | `ErrTableNotFound` / `ErrViewNotFound` |
| `CommitView` | as `CommitTables`, for one view | put | as `CommitTables` |
| `Get*` | — | a linearizable `Range` of one key | missing → the matching not-found error |
| `List*` | the parent namespace exists (the root always does) | `Range` over the prefix, after the page token, with the page size | parent missing → `ErrNamespaceNotFound` |
| `Ping` | — | a linearizable `Range` of `cat/v1/ping` | the read failing → `ErrUnavailable` |

Notes:
- **Lists are read-only transactions,** so the existence check and the scan
  see one consistent state. The KV server serves them as reads, not through
  raft.
- **Page tokens** are opaque, as in `memstore`: they hold the last name
  returned, and the next page starts at `<prefix><last name>\x00`, the
  smallest key after it. A listing stays stable while items are inserted
  between pages.
- **Transaction size limit:** the KV store allows 128 compares and 128
  operations per transaction branch. A create needs 3 compares, so one
  `CommitTables` call can carry at least 42 tables. Beyond that the adapter
  returns `ErrInvalidArgument`. Phase 4's multi-table endpoint must document
  this limit or split transactions.

### D3: Errors

| KV client error | `Store` error | What the catalog does |
|-----------------|---------------|-----------------------|
| `client.ErrOutcomeUnknown` | `store.ErrUnavailable`, wrapped with "outcome unknown" | A failed pointer swap is "state unknown": `503 CommitStateUnknownException`, and the metadata file is kept (Phase 1, D4). |
| `client.ErrUnavailable` | `store.ErrUnavailable` | The same. A definite failure is also reported as "unknown" for now, which is safe; at worst it leaves an orphaned metadata file for Phase 5 to clean up. |
| `client.ErrInvalidArgument` | `store.ErrInvalidArgument` | `400` |
| context cancelled or deadline exceeded | the context error, wrapped | The request ends |

A one-line improvement can come later: a separate sentinel for "certainly
not applied" would let the catalog delete the orphaned file right away. It's
deliberately left out of this phase so the `Store` interface stays as the
contract suite defines it.

### D4: Configuration of `catalog-server`

| Setting | Default | Purpose |
|---------|---------|---------|
| `store-backend` | `memory` | Also accepts `raft` |
| `kv-endpoints` | — | KV client endpoints, comma-separated; required for `raft` |
| `kv-attempt-timeout` | `5s` | Per-attempt timeout of the KV client |
| `kv-request-timeout` | `15s` | Upper bound for one store call, retries included; it must fit inside the HTTP write timeout |
| `idempotency-lifetime` | `30m` | Advertised as `idempotency-key-lifetime`; `0` disables idempotency support |

Readiness (`/readyz`) calls `Store.Ping`. For `raft` that's a linearizable
read, so a replica cut off from the KV quorum reports itself unready and the
load balancer stops sending it traffic. Liveness (`/healthz`) does not touch
the store, so a KV outage doesn't make the orchestrator restart healthy
catalog processes.

### D5: Timeouts and retries across layers

Three layers retry, and their budgets must nest. Otherwise a client gives up
while the server is still working, and the server's eventual success is
reported as a failure.

| Layer | Retries | Budget |
|-------|---------|--------|
| KV client | Failover and same-request-ID retries | `kv-attempt-timeout` per attempt, `kv-request-timeout` overall |
| Catalog commit | CAS conflicts with re-checked requirements (Phase 1, D4) | `commit-retries` × one store round |
| HTTP | — | `http-write-timeout` (60 s) must exceed the worst case of the two above; validated at startup |
| Engines | Their own 409 and 5xx handling | Their configuration (documented for Trino and PyIceberg) |

### D6: `Idempotency-Key`, shared by all replicas

The spec says: with the same key, the server applies no additional effects,
and replays the final response of a finalized request.
- **Finalized** means 200/201/204 and deterministic terminal 4xx responses.
- 5xx responses are not finalized.
- Keys are UUIDv7 strings, and must be honoured for at least the advertised
  lifetime.

A retry may reach a different replica, so records live in the KV store
(D1), not in memory.

1. **Claim.** Before executing, the replica creates the record in an
   "in progress" state with a lease of twice the request budget, using a
   `mod_revision == 0` compare. The record holds a fingerprint of the
   request: method, route, path parameters and a hash of the body.
2. **Found finalized:** replay the stored status and body. If the
   fingerprint differs, the client reused a key for another operation:
   reject with `422 UnprocessableEntityException`.
3. **Found in progress, lease still valid:** another attempt is running.
   Answer `503` (not finalized), so the client retries later.
4. **Found in progress, lease expired:** the replica that held it died. Take
   over the claim with a compare on the record's `mod_revision`, then execute.
   Re-execution is safe for the in-scope operations:
   - Creates and drops fail definitively when repeated, as `AlreadyExists`
     and `NotFound`.
   - Commits re-check their requirements (Phase 1, D4), so a commit that
     already landed fails with 409 instead of applying twice.
5. **Finalize.** Store the response with a compare on the claim's
   `mod_revision`. A 5xx result deletes the claim instead, so the client's
   retry runs again.
6. **Expiry.** Each record also has an entry in the expiry index. A janitor
   on every replica deletes expired records in small batches, each delete
   conditional on the record being unchanged. Several replicas running the
   janitor at once is harmless.

- **In-scope operations:** create, drop and update-properties of namespaces;
  create, commit, drop and rename of tables.
- **Advertising:** `/v1/config` advertises `idempotency-key-lifetime` only
  when the feature is enabled.
- **Memory backend:** with the memory backend, the same records live in a
  small in-memory implementation behind the same interface, so tests run
  without a cluster.

### D7: The high-availability stack

- **Two `catalog-server` replicas** behind **HAProxy 3.4**, which
  health-checks `/readyz` and retries idempotent requests on another replica
  when a connection fails. The image is `haproxy:3.4` (currently 3.4.7).
  Compose pins it by digest, so a rebuilt tag cannot change the test stack.
- **The three kv-nodes** from the Phase 2 `kv` profile, which the catalogs
  use through `kv-endpoints`.
- **Clients talk to the load balancer only:**
  - **Trino** is the only SQL engine: the existing catalog configuration,
    pointed at the load balancer. Spark was considered and dropped: it is
    too heavy for the test stack, and Trino covers the SQL side.
  - **PyIceberg 0.12.0**, a pinned Python image running the test scripts,
    covers a second, non-SQL client implementation.
- **A new compose profile, `ha`,** starts the whole stack, so the Phase 1
  `compose-up` stays light.

### D8: Graceful shutdown with a load balancer

On SIGTERM a catalog replica:
1. Turns `/readyz` to 503.
2. Waits one load-balancer health-check interval, so new requests go
   elsewhere.
3. Drains in-flight requests within `http-shutdown-timeout`.
4. Closes the KV client.

A rolling restart then drops no requests.

---

## Step 1: `raftstore` (`internal/store/raftstore`)

- **Keys and values** (D1): small helpers that build keys and encode and
  decode the JSON values. Unit tests check prefix boundaries: a child scan
  never returns grandchildren, and the prefixes of `a` and `ab` don't
  overlap.
- **Operations** (D2): one file per area (namespaces, tables, views, shared
  pointer logic), with tables and views sharing the pointer code.
- **Error mapping** (D3), in one function, with a unit test per row.
- **Construction:** the adapter takes the KV client and a key prefix, so
  tests can isolate catalogs in one cluster.

## Step 2: A reusable in-process gRPC cluster

Move the localhost cluster helper from `internal/kv/daemon/daemon_test.go`
into `kvtest`. It becomes a gRPC cluster: real daemons on `127.0.0.1` ports
with in-memory filesystems, plus helpers to stop, restart and find the
leader. The KV full-stack tests and the `raftstore` tests then share it.

## Step 3: Contract and fault tests for `raftstore`

- **The contract suite:** `storetest.Run` against `raftstore` on a 3-node
  in-process cluster, with a fresh key prefix per subtest, so the suite runs
  in parallel on one cluster. This is the main proof that `raftstore` and
  `memstore` behave the same.
- **Fault tests:**
  - **Leader stops during `CommitTables`:** the call either succeeds, or
    returns `ErrUnavailable`. Afterwards the pointer is at the old version or
    at the new one, never anywhere else.
  - **Concurrent conflicts across two `raftstore` instances**, standing in
    for two catalog replicas: exactly one commit wins each round. This
    repeats the contract's concurrency test across processes.
  - **Every compare fails at once:** the error names the first failing
    commit in order, as `memstore` does.
  - **Transaction size:** at the limit it works; past it, `ErrInvalidArgument`.

## Step 4: Idempotency (`internal/idempotency`, used by `internal/api`)

- **An interface for records** — claim, load, finalize, release, expire —
  with KV and memory implementations, and a shared contract test.
- **HTTP middleware** that wraps the in-scope routes only:
  1. Reads `Idempotency-Key` and validates it as a UUID string.
  2. Fingerprints the request.
  3. Runs the steps in D6.
  4. Captures the response for finalization.

  It sits *after* request decoding, so malformed requests are rejected
  without claiming a key.
- **The janitor:** one goroutine per replica, with a jittered interval.
- **`/v1/config`** gains `idempotency-key-lifetime`.
- **Tests:**
  - Replays of 200, 204 and 409 responses.
  - No replay of 5xx.
  - Fingerprint mismatch → 422.
  - In-progress → 503.
  - Takeover after an expired lease.
  - Two replicas, one shared store: a key used through replica A replays
    through replica B.
  - The janitor removes expired records and nothing else.

## Step 5: `catalog-server` wiring

- **Configuration** (D4) and **timeout validation** (D5).
- **Backend selection** in `cmd/catalog-server`, closing the KV client on
  shutdown.
- **Readiness and drain on shutdown** (D8).
- **Startup log:** the backend, the endpoints and the catalog key prefix.
- **Conformance:** the in-process suite (`test/conformance`: iceberg-go
  `catalogtest` plus the append-and-scan tests) runs once per backend: memory
  and raft. The raft run uses the in-process gRPC cluster from Step 2.

## Step 6: The high-availability stack (D7)

- **The `ha` compose profile:** 3 kv-nodes, 2 catalog replicas
  (`catalog-a`, `catalog-b`) with `ECHPOCHMAK_STORE_BACKEND=raft`, HAProxy,
  MinIO, Trino and PyIceberg.
- **HAProxy configuration** in `deployments/compose/haproxy/`, with health
  checks on `/readyz` and retries on connection failure.
- **Trino's catalog configuration** pointed at the load balancer instead of
  a single catalog replica.
- **The PyIceberg image** (`build/pyiceberg/Dockerfile`) with pinned
  packages, plus the test scripts.
- **Make targets:**
  - `make ha-up` / `make ha-down` start and stop the stack.
  - `make ha-e2e` runs the milestone suite and removes every container and
    volume afterwards, as `make kv-chaos` does.

## Step 7: Milestone tests (`test/ha`, build tag `ha`)

A Go test drives the scenario. It talks to the clients (Trino over its HTTP
API, PyIceberg scripts via `docker exec`), injects faults with the docker
CLI, and checks the results.

1. **Baseline:**
   - Trino creates a namespace and a partitioned table, inserts data, and
     reads it back.
   - PyIceberg appends Arrow data to the same table and scans it.
   - Trino reads the result.
   - Both clients see the same rows.
2. **Replica loss:**
   - Writers in Trino and PyIceberg insert in a loop while `catalog-a` is
     killed and restarted.
   - Every acknowledged insert is present at the end, with no duplicates.
   - Engine-side errors are only retried ones.
3. **Raft leader loss:**
   - The same writers run while the kv-node that leads is killed and
     restarted.
   - The same assertions hold.
4. **Both at once:** a catalog replica and the raft leader are killed
   together.
5. **Rolling restart:** each catalog replica is restarted in turn under load,
   and no request fails (D8).
6. **Idempotency over HTTP:** an `updateTable` sent twice with one
   `Idempotency-Key`, once through each replica, is applied once and
   replayed once.

The assertions count rows per writer and check the table's snapshot log
(every commit is accounted for). They also check that the pointer in the KV
store, read with `kvctl`, names a metadata file that exists in MinIO.

The Trino suite from Phase 1 (`make e2e`) runs against the `ha` stack too,
through the load balancer.

## Step 8: Documentation

- **`arch/phase3/adr-004-raftstore.md`:**
  - The key layout (D1).
  - The operation mapping (D2).
  - The error model across layers (D3, D5).
  - The idempotency design (D6).
  - The test results, and the catalog's write latency on the raft backend
    against `memstore`.
- **`README.md`:** running the `ha` stack, the `raft` backend settings, and
  reading catalog state with `kvctl`.
- **`arch/plan.md`:** mark Phase 3 done, and record any scope that moved.

---

## Order of work

1. `raftstore` keys, values and namespaces, then tables and views (Step 1).
2. The in-process gRPC cluster (Step 2). Run the contract suite against
   `raftstore` and fix it until it passes (Step 3).
3. Fault tests for `raftstore` (Step 3).
4. `catalog-server` wiring, and conformance on both backends (Step 5).
5. Idempotency: the interface and its two implementations, the middleware,
   the janitor (Step 4).
6. The `ha` compose profile, the HAProxy configuration, and the PyIceberg
   image (Step 6).
7. Milestone scenarios 1–6 (Step 7). Fix what they find.
8. ADR-004 and documentation (Step 8).

## Definition of done

- `make ci` passes, with `storetest` run against both `memstore` and
  `raftstore`.
- The conformance suite passes on both backends.
- `make ha-e2e` passes scenarios 1–6. Containers and volumes are removed
  afterwards.
- `make e2e` (Trino) passes against the `ha` stack.
- `/v1/config` advertises `idempotency-key-lifetime`, and an idempotent
  retry through another replica is replayed, not re-executed.
- ADR-004 is written; `README.md` and `arch/plan.md` are updated.

## Risks

| Risk | Mitigation |
|------|------------|
| `raftstore` differs subtly from `memstore` (error precedence, version semantics, pagination) | The shared contract suite is the arbiter; failure branches read the keys, so classification never depends on a second, racy read |
| A definite KV failure reported as "state unknown" leaves orphaned metadata files | Accepted for this phase (D3); Phase 5 adds orphan cleanup, and the narrower sentinel is a small follow-up |
| Retry budgets that don't nest (KV client, catalog, HTTP, engine) turn successes into reported failures | Budgets validated at startup (D5); the chaos scenarios measure them |
| Idempotency re-executes after a replica dies holding a claim | Takeover only after the lease expires; in-scope operations fail safely when repeated (D6); a test covers it |
| The PyIceberg image can't be built (network behind a VPN) | Versions pinned; image built with `BUILD_NETWORK`; dependencies baked in at build time, never downloaded at test time |
| Staged create is not exercised by any engine: Trino 484 and PyIceberg both create tables directly | It stays covered by the in-process conformance and catalog tests, which now run on both backends |
| Larger transactions hit the KV limits | The limit is documented in D2 with a clear error; Phase 4 decides how to split |
| The janitor competes with live traffic | Small batches, conditional deletes, jittered interval; the records are tiny |

---

## Implementation notes (2026-11-11)

Phase 3 is implemented and the milestone is met: `make ha-e2e` passed all
six scenarios in two consecutive runs, and the Phase 1 Trino suite passes
through the load balancer. Details are in `adr-004-raftstore.md`. Changes
from the plan:

- **KV timeouts** default to 2 s per attempt and 5 s per store call, not
  5 s and 15 s: the planned values do not nest under the 60 s HTTP write
  timeout, which the server now checks at startup (D5).
- **`raftstore.New`** takes `Options{Prefix, CallTimeout}`.
- **The HA stack is an overlay file** (`docker-compose.ha.yaml`), not a
  profile.
  - It disables the base file's in-memory catalog.
  - HAProxy takes the network name `catalog`, so Trino's configuration and
    the Trino suite reach it unchanged.
  - The replicas are also exposed on 18181 and 18182 for tests that must
    pick one.
- **Idempotency details (D6):**
  - The middleware works on the raw request, before the generated decoder.
  - Successful table creates and commits replay through a fresh GET of the
    table, because their metadata is too large to store; the spec allows a
    newer state.
  - Other responses over 64 KiB are not finalized.
  - Staged creates bypass the protocol.
- **Fault scenarios 2–4 give each writer its own table.** Two writers on one
  table starved the slower one for minutes, which is correct optimistic
  concurrency but made the test about the race instead of the fault.
  Contention stays covered by scenario 1, the conformance suite and the
  Trino suite.
- **A drain bug was found and fixed:** request contexts were cancelled by
  the shutdown signal, so draining failed the requests it was meant to
  finish.
- **The gRPC cluster helper** moved to `kvtest.GRPCCluster`, as planned.

