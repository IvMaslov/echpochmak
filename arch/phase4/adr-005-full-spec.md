# ADR-005: Full spec coverage — views, transactions, register, purge

- **Status:** accepted
- **Date:** 2026-11-14
- **Context:** Phase 4 (`arch/phase4/plan.md`)

## Context

After Phase 3 the catalog serves namespaces and tables from a replicated KV
store, behind a load balancer. Engines also need:
- **Views**, which Trino creates through the REST catalog.
- **Atomic multi-table commits** (`transactions/commit`).
- **Registering metadata** written elsewhere, and unregistering it.
- **Dropping a table with its files** (purge).

Iceberg's REST Compatibility Kit (RCK) tests a server through Java's
reference client. A Swagger UI makes manual testing easy.

`Store` already holds view pointers and commits several tables in one
transaction (ADR-004). This ADR records how the remaining pieces are built
on it.

## Decisions

### 1. Views use the table pointer model

A view is a pointer to an immutable metadata file, swapped with
compare-and-swap.
- Create, replace, rename and drop follow the table code paths.
- Metadata is built with iceberg-go's `view.MetadataBuilder`. A view's
  first version is always version 1, as in Java.
- View metadata is written gzipped by default, as Java does.
- Views have a version log but no log of metadata files, so a commit never
  deletes an older file. Dropping a view keeps its files: the spec has no
  purge for views.
- Responses are the stored JSON.
- A view and a table never share a name; `Store` enforces this.

**Alternative rejected:** storing view metadata in the KV store itself.
Views are small, but separate storage would mean a second commit model,
and the spec's `metadata-location` would point nowhere.

### 2. A transaction is one store write, never split

`transactions/commit` runs the single-table commit flow for N tables:
1. One read of all N pointers, through a new `Store.GetTables`.
2. Requirements checked for every table before anything is written.
3. N metadata files written in parallel.
4. One `CommitTables` call that swaps every pointer or none.

A lost race retries everything, with every requirement checked again.

- **The size limit is the KV transaction budget:** 128 compares and 128
  reads in the failure branch, where an update costs 1 of each and a
  create 3. It replaces Phase 3's fixed cap of 42 commits. An update's
  failure branch now reads only its own key; before, every commit read
  three keys, which capped updates at 42 as well. Over the budget, the
  request fails with `400`.
- **A table with requirements but no updates** is checked against the
  state read at the start of the attempt. Its pointer is not written, so a
  commit to it that lands during the attempt goes unnoticed. Writing the
  pointer unchanged would close the gap, but it would make every other
  writer of that table conflict.
- **Why not split** large transactions: the spec promises atomicity, and
  two KV transactions are not atomic together.
- **Why `GetTables`:** one read per attempt keeps the timeout budget of
  ADR-004 valid for any transaction size.

### 3. Register writes nothing; unregister returns exactly what it removed

- **Register** reads and validates the metadata file, then creates the
  pointer. With `overwrite` it swaps an existing table's pointer instead. It
  never writes or deletes the registered files.
- **Unregister** reads and deletes the pointer in one KV transaction, and
  answers with the metadata file that pointer named. So the response
  includes every commit made before the unregister, as the spec requires.
  To support this, `Store.DropTable` returns the pointer it removed.

### 4. Purge is a durable queue in the KV store

`DELETE ...?purgeRequested=true` does two things in **one** store write:
- removes the pointer;
- enqueues a purge task, a key under `cat/v1/purge/`.

Then it answers `204`. A worker on every replica claims tasks with a lease
and deletes the table's files:
- metadata files;
- manifest lists and manifests;
- data and delete files;
- statistics files.

Then it removes the task. The metadata file the task names goes last, so
a worker that takes over a half-done task can still collect the files.

- **Why not delete within the request:** a large table has thousands of
  files. That would blow the request deadline, and a replica dying halfway
  would leave the job half done with no record of it.
- **Why a lease:** a dead worker's task is taken over. Deleting a missing
  file succeeds, so running a task twice is harmless. The lease is renewed
  between batches of 100 deletions.
- **Why not iceberg-go's `Table.PurgeFiles`:** it deletes everything under
  the table location. A table created under a namespace `location`
  property has no UUID in its path, so a recreated table shares the
  directory and would lose its files. The worker deletes only the files
  the dropped metadata references.
- **The drop with purge** reads the pointer, then deletes it at that
  revision, because the task must name the metadata file. A concurrent
  commit makes it retry, with jitter: a test found that without the jitter
  it starved under back-to-back commits.
- **Safety:**
  - Only files under the table's location are deleted.
  - `gc.enabled=false` makes the drop fail with `400`, rather than drop the
    table and silently keep the files.
  - Two entries registered on the same files cannot be detected cheaply.
    Purging one of them is the client's responsibility, as with Java's
    catalogs.

### 5. Every new mutation is idempotent

The Phase 3 `Idempotency-Key` middleware covers every new mutation.
- **Replays of responses that carry metadata** (view create, view replace,
  register) serve a fresh `GET`, as table commits already do.
- **Unregister** stores the metadata location and replays by reading that
  file: the table no longer exists to `GET`.

### 6. Iceberg's own test kit is an optional check

The RCK runs Java's `CatalogTests` and `ViewCatalogTests` through Java's
`RESTCatalog` against the server:
- pinned to the Iceberg release that matches our spec;
- in a container, with its dependencies baked in at build time;
- against both store backends: `make rck` and `make rck-ha`.

- **Why run it:** every other test checks our own reading of the spec; the
  RCK checks the reference reading. It is also the only independent client
  for register, register-view and Java's transaction paths.
- **Why optional:** it needs a Java toolchain, and some failures come from
  the Java client rather than the spec. It runs once before Phase 4 is
  closed, then by hand after API or `iceberg-go` changes. It is never part
  of `make ci`.
- **Exclusions** are explicit, in `test/rck/exclusions.txt`. Tests that
  contradict a recorded decision, such as no credential vending or no
  server-side scan planning, are excluded with a written reason. Any other
  failure is a bug.
- **Bugs it finds** each get a Go regression test, so they stay covered
  without the RCK.

**Alternative rejected:** making it a required check. The build time and
upkeep of a Java toolchain don't pay off on every run.

### 7. Errors are worded as the reference implementation words them

Java clients, Trino among them, show the server's error messages to users,
and the RCK checks them. So the messages are Java's:
- "Table does not exist: db.t", "Namespace already exists: db",
  "View with same name already exists: db.v", "Cannot rename db.a to db.b.
  Table already exists".
- Failed requirements start with Java's text, for example "Requirement
  failed: current schema changed: expected id 0 != 1". The client adds
  "Commit failed: " in front.
- A well-formed request that Iceberg rejects, such as an invalid update or
  two SQL representations for one dialect, answers
  `IllegalArgumentException`, which Java clients raise as such. Malformed
  requests stay `BadRequestException`.

The store's sentinel errors carry the wording, so both backends produce
the same messages; the catalog rewords the cases that depend on what was
being created.

### 8. The Swagger UI is embedded, and shows only what is served

- **Embedded assets:** `swagger-ui-dist` 5.34.1, checked against its npm
  integrity hash and embedded with `go:embed`, so it works without a CDN,
  behind the VPN. `make swagger-ui-update` refreshes it.
- **A filtered spec:** the served spec is the pinned spec, reduced at
  startup to the advertised endpoints, so "Try it out" never offers an
  operation that answers `501`.
- **The served spec also drops authentication** and points at the server's
  own origin, with the catalog prefix filled in.
- **Routes:** `/docs` and `/openapi.json`, outside `/v1`. The `-docs` flag
  turns them off.

### Out of scope

| Item | Reason |
|------|--------|
| OAuth tokens | Decision 3 of `arch/plan.md`: no authentication |
| Credential vending, S3 request signing | Decision 4: clients bring their own credentials |
| Server-side scan planning | No engine in the stack needs it |
| Functions (UDFs) | No engine in the stack reads them |

These keep answering `501` and are not advertised in `/v1/config`.

## Consequences

- **`Store` changes:**
  - `GetTables` is new.
  - `DropTable` takes `DropOptions` and returns the removed pointer.
  - A purge queue is added.
  - The `CommitTables` limit becomes a compare budget.

  Both backends implement the changes, and the contract suite covers them.
- **The KV key layout** of ADR-004 gains `cat/v1/purge/<task id>`.
- **Configuration:** `purge-workers`, `purge-lease`, `purge-interval` and
  `docs`.
- **Delayed file deletion:** a purged table's files outlive the `204` by a
  few seconds, or longer if every replica is down. The task survives
  either way.
- **A Java build in the test tooling:** the RCK image is built only for
  `make rck` and `make rck-ha`, and nothing else depends on it.

## The REST Compatibility Kit

Iceberg 1.12.0's kit, the release closest to our pinned spec: the spec
commit is 86 commits after it, and the two specs differ only in one
column-statistics field.
- **First run, against the Phase 3 server:** 160 tests, 83 failed. 49 of
  the failures were views, not served yet. Most of the rest were error
  wording.
- **Now:** every test passes except the exclusions below. The kit skips 10
  more itself: nested and empty namespaces and names with slashes, which
  it cannot switch on, and one test it disables. Names with dots and the
  `variant` type are switched on.

The kit found three server bugs, each fixed with a Go test:
1. Error messages did not use the reference wording (decision 7).
2. Validation failures answered `BadRequestException` instead of
   `IllegalArgumentException`.
3. A create commit that sets format version 1 failed: the server built from
   empty v2 metadata, and an upgrade cannot go down. It now starts from the
   requested version, as Java does.

**Exclusions** (`test/rck/exclusions.txt`):

| Tests | Reason |
|-------|--------|
| `testCompleteCreateTransaction`, `…V2`, `…MultipleSchemas`, `testCompleteCreateOrReplaceTransactionCreate`, `…Replace`, `testCompleteReplaceTransaction` | The tests ask for a `file:/tmp` table location. The server honours requested locations, and the client's S3FileIO cannot write there: client and server share only S3 (decision 2 of `arch/plan.md`) |
| `testLoadTableWithMissingMetadataFile` | The test deletes the metadata file through the local file system (decision 2) |
| `testCreateTableWithVariantColumn` | Outside the spec: iceberg-go numbers nested field IDs depth-first, Java breadth-first. The spec fixes no order, and clients read the IDs back from the returned metadata |

## Verification

| Check | Result |
|-------|--------|
| Contract suite with the new store methods (`GetTables`, `DropTable`, the purge queue) | Pass on `memstore` and `raftstore` |
| `raftstore` fault tests: transaction budget for creates and updates, mixed failures classified, a drop racing commits, a purge drop under commits | Pass |
| Catalog unit tests: views, transactions (including atomicity under 12 concurrent writers), register, unregister, purge (takeover, failure, files outside the location, `gc.enabled`) | Pass |
| HTTP tests: every new endpoint and error, idempotent replays of transactions, view creates, registers and unregisters across two replicas, the served spec | Pass |
| Conformance suite through iceberg-go, on both backends: views, a two-table transaction of real appends, register, a purge of real data files and manifests | Pass |
| Trino (`make e2e`): views (create, replace, rename, drop, name clash), a multi-table transaction read back, unregister and register, `DROP TABLE` purging every file | Pass |
| HA (`make ha-e2e`): the six Phase 3 scenarios, two-table transactions under a replica and leader kill, a purge finished by the other replica, the Trino suite through HAProxy | Pass: all 8 scenarios, then the Trino suite through HAProxy. 107 transactions acknowledged under the kill, each on both tables and none on only one; the purge was finished by `catalog-b` within 3 s |
| RCK: `make rck` (memory backend) and `make rck-ha` (raft backend, through HAProxy) | Pass on both: 152 tests, 0 failures, 10 skipped by the kit, 8 excluded |
| `make ci`: lint, tests with `-race`, generated code, build | Pass: 0 lint issues; `go mod tidy` and `go generate` change nothing |

All stacks are removed after their runs.

## Known limitations

- **A key held by a dead replica waits out its lease.** If a replica dies
  while it executes a request with an `Idempotency-Key`, retries with that
  key answer `503` until the claim's lease expires: twice the HTTP write
  timeout, 2 minutes by default. Then another replica takes the claim over
  and runs the request. The HA scenario with the transaction writer showed
  it: one transaction waited out the full lease. A shorter lease would need
  a shorter write timeout, and that timeout must still hold the nested
  retry budgets of ADR-004.
- **Requirements without updates** in a transaction are checked against the
  state read at the start of the attempt (decision 2).
- **Shared files between registered tables** are not detected; purging one
  can destroy the other's data (decision 4).
- **Nested field IDs** of a new table are numbered as iceberg-go numbers
  them, not as Java does. Both are valid (see the RCK exclusions).
