# Phase 4: Full spec coverage

**Goal:** the catalog serves every part of the pinned Iceberg REST spec that
an engine needs for tables and views: views, atomic multi-table
transactions, registering and unregistering existing metadata, and dropping
a table with its files. An independent client implementation, Iceberg's own
REST Compatibility Kit, confirms that the server behaves as the reference
catalog does.

**Milestone (from `arch/plan.md`):**
- Views, `transactions/commit`, register and purge work end to end: from
  Trino and over HTTP, on both store backends and on the HA stack.
- Iceberg's REST Compatibility Kit (RCK), an optional check, passes once
  against both backends before the phase is closed, except for tests
  excluded with a written reason.
- A Swagger UI is served for manual testing.

**Builds on:**
- **Phase 1:** the catalog service, its commit flow (load → check
  requirements → apply updates → write the metadata file → compare-and-swap
  the pointer) and the error mapping.
- **Phase 3:** `raftstore`, the `Idempotency-Key` protocol, the HA stack
  and its test harness (`test/ha`). `Store` already holds view pointers and
  commits several tables in one transaction. Phase 4 adds the HTTP and
  catalog layers on top.

---

## Scope

### In scope

| Area | Endpoints |
|------|-----------|
| **Views** | list, create, load, exists (`HEAD`), replace (commit), drop, rename |
| **Multi-table transactions** | `POST /v1/{prefix}/transactions/commit` |
| **Register** | `POST .../namespaces/{namespace}/register` (with `overwrite`), `POST .../namespaces/{namespace}/register-view` |
| **Unregister** | `POST .../tables/{table}/unregister` |
| **Purge** | `DELETE .../tables/{table}?purgeRequested=true` deletes the table's files |
| **Compatibility** | Iceberg's REST Compatibility Kit: an optional check, run in a container against the server (D6) |
| **Swagger UI** | `GET /docs` and the served spec at `GET /openapi.json` |

Every new mutation honours `Idempotency-Key`, and every new endpoint is
advertised in `/v1/config`.

### Out of scope

| Item | Why |
|------|-----|
| `GET /v1/oauth/tokens` | Decision 3 of `arch/plan.md`: no authentication |
| `credentials` (credential vending), `sign` (S3 remote signing) | Decision 4: clients use their own S3 credentials, and signing requests is credential vending in another form |
| Server-side scan planning (`plan`, `tasks`) | A large feature that no engine in the stack needs: Trino and PyIceberg plan scans themselves. A candidate for a later phase |
| Functions (`functions`, UDFs) | Recent in the spec; no engine in the stack reads them |
| Orphan cleanup, metrics, tracing | Phase 5 |

These operations keep returning `501 NotImplemented` and stay out of
`/v1/config`, so clients never call them.

---

## How the pieces fit

```
Trino · PyIceberg · Java RCK · Swagger UI (browser)
                  │ Iceberg REST
                  ▼
catalog-server
 ├─ api       new handlers: views, transactions, register, unregister;
 │            /docs, /openapi.json
 ├─ catalog   views.go, transactions.go, register.go, purge.go
 ├─ purge worker (every replica)   ──► S3 / MinIO: deletes the files
 └─ store.Store                    ──► KV: pointers + the purge queue
```

The new catalog code reuses the Phase 1 commit flow. A view commit is the
table commit with view metadata. A multi-table transaction runs the same
steps for N tables and swaps all N pointers in one `Store.CommitTables`
call.

---

## Design decisions

### D1: Views reuse the table pointer model

A view is a pointer to an immutable metadata file, exactly like a table.
`Store` already has `GetView`, `ListViews`, `CommitView`, `RenameView` and
`DropView`, and the contract suite already covers them.

- **Metadata:** iceberg-go v0.7.0 has the view types, the update and
  requirement parsers, and a metadata builder (`view.MetadataBuilder`). The
  catalog uses them as it uses `table.UpdateTableMetadata` for tables.
- **Create:**
  1. Validate the request: schema, view version and SQL representations.
  2. Assign a view UUID.
  3. Build the metadata with `SetCurrentVersion(version, schema)`.
  4. Write `<location>/metadata/00000-<uuid>.gz.metadata.json`.
  5. Create the pointer (`ExpectedVersion` 0).
- **Replace:** load → check the requirements (`assert-view-uuid`) → apply
  the updates → write the next metadata file → compare-and-swap. It is
  retried on a lost race as table commits are, with the same retry budget.
- **Default location:** like tables, under the namespace's `location`
  property or the warehouse, with the view UUID in the last path segment.
- **Names are shared with tables:** a view and a table can never have the
  same identifier. `Store` already enforces this in both directions.
- **Old metadata files** are deleted after a commit when the view sets
  `write.metadata.delete-after-commit.enabled`, as for tables, bounded by
  `version.history.num-entries`.
- **Responses** are written from the stored JSON, as for tables, so clients
  get every field exactly as stored.

### D2: Multi-table transactions are one store transaction

`POST /v1/{prefix}/transactions/commit` carries a list of table changes,
each with requirements and updates. The spec requires all or nothing.

1. **Validate:** at least one change, no table twice, every identifier
   valid. Anything else is a `400`.
2. **Load every pointer in one read** with a new `Store.GetTables(ids)`.
   One linearizable read keeps the store calls per attempt at two, so the
   timeout budget of ADR-004 (`http-write-timeout > (commit-retries + 1) ×
   2 × kv-request-timeout`) still holds.
3. **For each table:** read the current metadata, check the requirements
   and apply the updates, as the single-table commit does. A failing
   requirement fails the whole request with `409 CommitFailedException`
   before anything is written.
4. **Write the N new metadata files** in parallel, at most 8 at a time.
5. **Swap all N pointers** with one `CommitTables` call.
6. **On a lost race,** delete the N new files and retry from step 2. Every
   requirement is checked again, so a retry succeeds only if all of the
   client's assumptions still hold.
7. **On an unknown outcome,** keep all N files and answer `503
   CommitStateUnknownException`, as the single-table commit does.

Creates are allowed in a transaction (a change with `assert-create`), as
the spec and Java's staged-create transactions use them.

**Size limit.** Splitting a transaction would break atomicity, so it is
never split. The limit comes from the KV store: 128 compares and 128
operations per transaction branch. Phase 3 caps a `CommitTables` call at a
fixed 42 commits, the number of creates that fit. Phase 4 replaces that cap
with the real budget:
- An update costs 1 compare; a create costs 3 (the table key and the view
  key are absent, and the namespace exists).
- The sum must be at most 128, so a transaction can update up to 128 tables,
  or create up to 42.
- A larger transaction gets `400 BadRequestException` naming the limit.

### D3: Register and unregister

**Register table** (`POST .../register`, body `name`, `metadata-location`,
`overwrite`):
1. Read and parse the metadata file. A missing file or invalid metadata is
   `400`.
2. **Without `overwrite`:** create the pointer (`ExpectedVersion` 0). A
   taken name is `409 AlreadyExistsException`.
3. **With `overwrite`:** if a table exists, swap its pointer from the
   version just read. A view of that name is still `409`, because overwrite
   replaces tables only.
4. Answer `200` with the metadata, as a table load does.

The catalog never writes or deletes the registered files. Later commits
continue the file numbering; names that don't follow the Iceberg
convention already fall back to the metadata log's length (Phase 1). The
previous metadata of an overwritten table is left in place: it belongs to
whatever wrote it.

**Register view** (`POST .../register-view`) is the same for views, without
`overwrite`: the spec has none.

**Unregister table** (`POST .../tables/{table}/unregister`) removes the
pointer and leaves every file. The response must include all commits before
the unregister. So the pointer is deleted with a compare on the
`mod_revision` just read, and the response is the metadata file that this
pointer named. A commit that lands in between makes the delete fail, and it
is retried. To support this, `Store.DropTable` returns the pointer it
removed (D4).

### D4: Purge is a durable queue, worked by every replica

A purge can delete thousands of files. That is too slow for a request, and
a replica can die halfway. So `DELETE ...?purgeRequested=true`:
1. **In one store transaction,** removes the pointer and enqueues a purge
   task naming the table's last metadata location. A task is never lost,
   and never exists without the drop.
2. **Answers `204`** right away. Afterwards the table is gone from the
   catalog; its files go shortly after.

A **purge worker** on every replica claims tasks with a lease, like the
idempotency janitor:
1. **Claim:** compare-and-swap the task to "claimed until T". An expired
   claim can be taken over, because its owner died.
2. **Collect the files,** starting from the metadata file:
   - every metadata file in the metadata log, and the current one;
   - every snapshot's manifest list, every manifest, and every data and
     delete file those manifests list;
   - statistics and partition statistics files.
3. **Delete them** in batches, renewing the lease between batches.
   Deleting a missing file succeeds, so a task that was taken over halfway
   simply runs again.
4. **Remove the task** with a compare on the claim.

Safety rules:
- **Only files under the table's location are deleted.** A file referenced
  from elsewhere, for example by a registered table that points into
  another table's directory, is logged and kept.
- **`gc.enabled=false` blocks purging:** the drop is rejected with `400`
  and nothing is changed, so the client learns that its files would
  survive.
- **Shared files:** two catalog entries registered on the same files
  destroy each other's data when one is purged. The catalog cannot detect
  this cheaply. It is the client's responsibility, as with the Java
  catalogs, and it is documented.
- **Views** have no purge in the spec. Dropping a view never deletes
  files.

Store changes:
- `DropTable(ctx, id, DropOptions{Purge bool}) (Pointer, error)` returns the
  removed pointer and, with `Purge`, enqueues the task in the same write.
- A small queue: `ListPurgeTasks`, `ClaimPurgeTask`, `CompletePurgeTask`.
  Under `raftstore` the tasks are keys `cat/v1/purge/<task id>`, so the
  ADR-004 key layout gains one entry.
- Both backends implement it, and the contract suite covers it.

### D5: Idempotency for the new mutations

Every new mutation joins the Phase 3 middleware:

| Route | Replay |
|-------|--------|
| `transactions/commit` | `204`, stored verbatim |
| Create, replace and register view | A fresh `GET` of the view: its metadata can be large, as for tables |
| Register table | A fresh `GET` of the table |
| Drop and rename view | Stored verbatim |
| Unregister table | The metadata location is stored, and the replay reads that file again: the table is gone, so a `GET` would fail. If the file was deleted meanwhile, the replay is `404 NoSuchTableException`, which is what re-running would answer |

Re-running after a lease takeover stays safe:
- Transactions re-check every requirement.
- Registers and creates fail with `409` when they already happened.
- Drops, renames and unregisters fail with `404`.

### D6: The REST Compatibility Kit, an optional check

**What it is.** Iceberg's Java repository has a module, `open-api`, with a
test suite meant for third-party REST servers: the REST Compatibility Kit
(RCK). Its entry point is `RESTCompatibilityKitSuite`. It runs two suites
that every Java catalog implementation must pass, through Java's own
`RESTCatalog` client, against a server given by URL:
- **`CatalogTests`:**
  - namespaces: create, nested, properties, list, drop, drop when not
    empty;
  - tables: create, load, list, rename within and across namespaces, drop;
  - schema, partition spec, sort order and property evolution;
  - appends and concurrent commits that must conflict;
  - create and replace transactions;
  - register table;
  - the exact exception type for each failure.
- **`ViewCatalogTests`:**
  - views: create, load, list, replace, rename, drop;
  - version history and SQL dialects;
  - name clashes between views and tables;
  - register view.

**Why we run it.** Every other test in the project checks our own reading
of the spec. The RCK checks the reference reading. It is also the only
independent client for features no engine in our stack uses: register,
register-view, and the transaction paths of Java's client.

**Why it is optional.** It needs a Java toolchain and a Gradle build, and a
failure can come from the Java client rather than the spec. So it is not
part of `make ci`, `make e2e` or `make ha-e2e`. It runs:
- **once before Phase 4 is closed**, against both backends;
- **afterwards by hand**, after changes to the API layer or to
  `iceberg-go`, or before pinning a new spec version.

**The image (`build/rck/Dockerfile`):**
- **Base:** a JDK 21 image, pinned by digest, like HAProxy in Phase 3.
- **Source:** the Apache source release of Iceberg, `apache-iceberg-<v>`,
  checked against its published SHA-512. The source release carries
  `version.txt`, so the Gradle build needs no git history. The release is
  the one whose spec matches our pinned spec commit
  (`api/iceberg/SPEC_VERSION`); Step 0 picks it.
- **Build:** at image build time, with `BUILD_NETWORK`, Gradle downloads
  its distribution and every dependency and compiles the suite. A test run
  then works with `--offline`: nothing is downloaded while testing.
- **The S3 client:** the suite writes manifests and data files itself, as
  engines do, so it needs `S3FileIO`. A Gradle init script in the image
  adds `iceberg-aws` and the AWS SDK to the suite's test classpath, so the
  Iceberg sources stay unpatched.
- **Exclusions:** the same init script reads `test/rck/exclusions.txt` and
  turns each line into a Gradle `excludeTestsMatching` filter.

**Configuration of a run.** The suite reads its catalog properties from
environment variables with the `CATALOG_` prefix: `__` becomes `-`, and `_`
becomes `.`. It reads its switches from `rck.*` system properties. Step 0
confirms the exact names for the chosen release.

| Setting | Value | Meaning |
|---------|-------|---------|
| `rck.local` | `false` | Test a remote server instead of starting Java's own |
| `rck.requires-namespace-create` | `true` | The server does not create namespaces implicitly |
| `CATALOG_URI` | `http://catalog:8181` | The single catalog, or HAProxy on the HA stack (same name) |
| `CATALOG_WAREHOUSE` | `s3://<bucket>/rck` | A separate prefix, so the RCK's files are easy to inspect and remove |
| `CATALOG_IO__IMPL` | `org.apache.iceberg.aws.s3.S3FileIO` | The client's file IO |
| `CATALOG_S3_ENDPOINT`, `CATALOG_S3_PATH__STYLE__ACCESS` | MinIO, `true` | Reach MinIO |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | the MinIO dev credentials from `.env` | Credentials, as engines get them: never from the catalog (decision 4) |

**Exclusions (`test/rck/exclusions.txt`).**
- One test-name pattern per line, each followed by a comment that names the
  decision it contradicts. For example, a test that needs vended
  credentials points to decision 4 of `arch/plan.md`.
- A failure that no recorded decision explains is a server bug: it is
  fixed, not excluded.
- ADR-005 lists the final exclusions.

**Make targets:**

| Target | Stack |
|--------|-------|
| `make rck` | The base stack: MinIO and one catalog on the memory backend |
| `make rck-ha` | The HA stack: 3 kv-nodes, 2 catalogs on the raft backend, HAProxy |

Each target:
1. Builds the image if it is missing.
2. Starts the stack and waits for `/readyz`.
3. Runs the suite in a one-off container on the compose network.
4. Copies the JUnit XML and HTML reports to `build/rck-reports/<target>/`.
5. Removes every container and volume, whether the run passed or failed,
   and exits with the suite's result.

**Reading the results.** Gradle prints the failed tests with their
assertion messages. The HTML report shows each test's output and the
client's log, including the HTTP requests. Each failure is then sorted
into one of three outcomes:
- **A server bug:** fixed, with a Go test that reproduces it, so the fix
  stays covered by `make ci` without the RCK.
- **A recorded decision:** excluded, with its reason.
- **A Java client quirk outside the spec:** excluded, with a link to the
  client code, and noted in ADR-005.

### D7: The Swagger UI is embedded and shows only what is served

- **Assets:** `swagger-ui-dist`, pinned to one version, vendored into the
  repository and embedded with `go:embed`. The page then works without
  network access to a CDN, which matters behind the VPN, and the version
  cannot change under us.
- **The served spec** is built at startup from the embedded spec. It keeps
  only the operations in `Endpoints()` plus `/v1/config`, and sets the
  `prefix` parameter's default to the server's prefix. Swagger then offers
  only what works, and a test keeps the served spec and `Endpoints()` in
  step.
- **Routes:** `GET /docs` serves the UI, `GET /openapi.json` the spec. They
  sit outside `/v1`, like the probes.
- **Configuration:** `-docs` (default `true`) turns both off. The server has
  no authentication anyway (decision 3), so "Try it out" exposes nothing
  that `curl` doesn't.

### D8: Configuration

| Setting | Default | Purpose |
|---------|---------|---------|
| `purge-workers` | `2` | Purge tasks run at once per replica; `0` turns the worker off (tasks wait for another replica) |
| `purge-lease` | `2m` | How long a claim lasts; renewed between delete batches |
| `purge-interval` | `10s` | How often an idle worker looks for tasks, jittered |
| `docs` | `true` | Serves `/docs` and `/openapi.json` |

---

## Step 0: Check the dependencies

Short spikes, to settle the facts the plan depends on. The results go in
the implementation notes at the end of this file.
- **iceberg-go views:** check that v0.7.0's view builder covers create and
  replace as the spec defines them, and that its JSON round-trips Trino's
  and Java's view metadata unchanged. Gaps become a local workaround or an
  upstream issue.
- **Trino 484 views:** check that Trino uses the REST view endpoints when
  `/v1/config` advertises them, and which operations it sends.
- **PyIceberg 0.12.0 views:** check what it supports: list, load, create,
  drop.
- **RCK (D6):**
  - Pick the Iceberg release whose spec matches ours.
  - Confirm the suite's class names, its `rck.*` switches and its
    `CATALOG_` variables for that release.
  - Confirm that the source release builds without git history.
  - Confirm that the init script can add `iceberg-aws` to the test
    classpath, and that `excludeTestsMatching` works on the suite.
  - Make a first, unfiltered run against the Phase 3 server. It shows
    which failures to expect before any Phase 4 code exists.

## Step 1: Store changes

- `GetTables(ctx, ids)`: one read for several pointers (D2). Under
  `raftstore` it is one read-only `Txn`.
- `CommitTables`: the compare budget replaces the 42-commit cap (D2).
- `DropTable` returns the removed pointer and takes `DropOptions` (D3, D4).
- The purge queue (D4).
- The contract suite grows to cover all of these, and both backends pass
  it. New fault tests for `raftstore`: a transaction exactly at the
  budget, and one over it.

## Step 2: Views in the catalog (`internal/catalog/views.go`)

- Create, load, exists, list, replace, rename and drop (D1).
- View metadata I/O, sharing the table code (compression, file naming,
  caching).
- Unit tests: create and load; replace with requirements that hold and
  that fail; a lost race that is retried; a name clash with a table; drop
  then recreate; deleting old metadata files.

## Step 3: Multi-table transactions (`internal/catalog/transactions.go`)

- The flow in D2, sharing the per-table steps with `CommitTable`.
- Unit tests:
  - Two tables committed together.
  - One requirement failing: nothing is written, and no metadata file is
    left behind.
  - A lost race on one table: the whole transaction retries.
  - An unknown outcome: all files kept, `503`.
  - Duplicate tables; the budget limit.
  - A create inside a transaction.
- A concurrency test: many goroutines run overlapping transactions on
  shared tables. Every transaction either lands entirely or not at all.

## Step 4: Register and unregister (`internal/catalog/register.go`)

- Register table with and without `overwrite`, register view, unregister
  (D3).
- Unit tests:
  - Registering metadata written by PyIceberg and by Java (fixtures in
    `testdata`), and a file name that doesn't follow the convention.
  - Committing to a registered table afterwards.
  - A missing or invalid metadata file.
  - Overwrite of a table, and of a view (rejected).
  - Unregister racing a commit: the response includes the commit.

## Step 5: Purge (`internal/catalog/purge.go`, worker in `cmd/catalog-server`)

- File collection and deletion with the safety rules (D4), and the worker
  with its lease.
- Unit tests:
  - A table with several snapshots, deletes and statistics: every file
    under its location is removed.
  - A file outside the location is kept.
  - `gc.enabled=false` is rejected.
  - A worker killed halfway: another worker finishes the task.
  - Two workers racing for one task: one claims it.

## Step 6: HTTP handlers (`internal/api`)

- **Handlers** for the new endpoints, replacing the stubs. View requests
  are decoded with iceberg-go from the raw body, as table commits are, so
  no field is lost.
- **Error mapping:**
  - `NoSuchViewException`, `ViewAlreadyExistsException`.
  - `409 CommitFailedException` and `CommitStateUnknownException` for view
    commits and transactions.
  - `400` for an over-budget transaction.
- **Idempotency:** the new routes and their replay rules (D5).
- **`/v1/config`:** the new endpoints in `Endpoints()`.
- **Swagger UI** and the served spec (D7).
- **Tests:** a handler test per endpoint and per error, plus idempotent
  replays of each new route.
- **Conformance:** extend `test/conformance` with iceberg-go's view and
  transaction support where its REST client has them, on both backends.

## Step 7: End-to-end tests

- **Trino suite (`test/e2e`):**
  - `CREATE VIEW` and query through it.
  - `CREATE OR REPLACE VIEW`.
  - `ALTER VIEW ... RENAME TO`.
  - `DROP VIEW`.
  - A view and a table cannot share a name.
  - `DROP TABLE` with purge: the files disappear from MinIO.
- **HTTP scenarios (Go, against the compose stack):**
  - A transaction over two tables, read back by Trino.
  - Register a table written by PyIceberg into a second namespace and
    query it in Trino.
  - Unregister it, then register it again.
- **HA (`test/ha`), new scenarios:**
  - **Transactions under faults:** a writer runs two-table transactions
    in a loop while the raft leader and a catalog replica are killed. At
    the end, every acknowledged transaction changed both tables, and none
    changed only one.
  - **Purge across replicas:** drop with purge through replica A, then
    kill A at once. Replica B finishes the purge.
- **RCK (optional, D6):**
  - `build/rck/Dockerfile` with its Gradle init script, and
    `test/rck/exclusions.txt`.
  - `make rck` and `make rck-ha`.
  - Run both once at the end of the phase; sort every failure into a bug,
    a decision or a client quirk.
  - Each fixed bug gets a Go regression test.

## Step 8: Documentation

- **`arch/phase4/adr-005-full-spec.md`:** the decisions above, with the
  test results, and the RCK exclusion list with reasons. Supplement doc if needs
- **`README.md`:** views, transactions, register and purge; the Swagger UI;
  running the RCK.
- **`arch/plan.md`:** mark Phase 4 done, and record any scope that moved.

---

## Order of work

1. Step 0 spikes; adjust the plan if a dependency falls short.
2. Store changes and their contract tests (Step 1).
3. Views in the catalog, then their handlers, Trino tests and idempotency
   (Steps 2, 6, 7).
4. Multi-table transactions, the same way (Steps 3, 6, 7).
5. Register and unregister (Steps 4, 6, 7).
6. Purge and its worker (Steps 5, 6, 7).
7. Swagger UI (Step 6).
8. RCK: the image, the targets and the exclusion list. Run it on both
   backends and fix what it finds (D6, Step 7).
9. HA scenarios (Step 7).
10. ADR-005 and documentation (Step 8).

## Definition of done

- `make ci` passes; the contract suite covers the new store methods on both
  backends.
- The conformance suite passes on both backends.
- `make e2e` passes with the view and purge tests.
- `make ha-e2e` passes the Phase 3 scenarios and the new ones.
- `make rck` and `make rck-ha` have each passed once, with every exclusion
  documented. They are not part of `make ci`.
- `/docs` shows exactly the advertised endpoints, and "Try it out" works
  against a local stack.
- ADR-005 is written; `README.md` and `arch/plan.md` are updated.

## Risks

| Risk | Mitigation |
|------|------------|
| iceberg-go's view support is incomplete or differs from Java's | Step 0 checks it first; responses are the stored JSON, so a field iceberg-go doesn't model is still served |
| The RCK is heavy to build behind the VPN (Gradle, Maven dependencies) | Dependencies baked in at image build time with `BUILD_NETWORK`; base image pinned by digest; pinned, checksummed source release; runs use `--offline` |
| RCK tests assume behaviour the server deliberately lacks | An explicit exclusion list, each entry with its reason; anything else counts as a bug |
| RCK failures come from the Java client, not the spec | Sorted into "client quirk" with a link to the client code; the RCK stays optional, so it never blocks `make ci` |
| A bug the RCK found comes back unnoticed, because the RCK is not run often | Every fixed bug gets a Go regression test that runs in `make ci` |
| A purge deletes files that another entry still uses | Only files under the table's location; `gc.enabled=false` honoured; shared registrations documented as the client's responsibility |
| A purge worker dies halfway | Lease and takeover; deleting a missing file succeeds, so re-running is harmless |
| Large transactions hit the KV limits | The exact budget, a clear `400`, and the limit documented in ADR-005 |
| A transaction's N metadata files make commits slow | Parallel writes, at most 8 at a time; the store swap is still one write |
| Trino sends view operations differently than expected | Step 0 records what Trino 484 sends before the handlers are written |
| Swagger UI assets add size to the repository | Only the minified distribution files, pinned and vendored once |

---

## Implementation notes (2026-11-14)

Phase 4 is implemented; results are in `adr-005-full-spec.md`. What Step 0
found, and what changed from the plan:

- **Step 0 results:**
  - **iceberg-go v0.7.0** covers views fully: the builder, requirements
    and updates. It also has a REST client for views, transactions,
    register and purge, so the in-process conformance suite exercises all
    of them.
  - **Trino 484** uses the REST view endpoints once they are advertised.
    Its `DROP TABLE` asks the catalog to purge.
  - **PyIceberg 0.12.0** can create, load, list and drop views, register
    tables and views, and purge.
  - **The RCK:** Iceberg 1.12.0 is the release closest to our pinned spec.
    The spec commit is 86 commits after it, and the two specs differ only
    in one column-statistics field. The kit reads `rck.*` system properties
    and `CATALOG_*` variables, as planned.
- **The RCK image differs from D6 in three details:**
  - `iceberg-aws-bundle` is already on the suite's classpath, so the init
    script only applies exclusions.
  - The two test classes are selected directly, not through
    `RESTCompatibilityKitSuite`: Gradle's exclude filters do not apply
    inside a JUnit suite.
  - Reports are copied out with `docker cp`, because the image's user
    cannot write to a host directory.

  The kit needs a catalog without its test namespaces, so each run starts
  a fresh stack. Names with dots and the v3 `variant` type are switched on;
  nested and empty namespaces cannot be switched on in this kit.
- **The RCK found these, all fixed with Go tests:**
  - Error messages now use the Java reference wording ("Table does not
    exist: db.t", "Requirement failed: current schema changed: …"). Java
    clients show these messages, and the kit checks them.
  - Validation failures answer `IllegalArgumentException`, as Java's server
    does; malformed requests stay `BadRequestException`.
  - A create commit that sets format version 1 failed: the server built
    from empty v2 metadata. It now starts from the requested version.
- **Views (D1):** views have a version log but no metadata-file log, so
  nothing expires after a commit. A view's first version is always 1, as
  in Java.
- **Transactions (D2):**
  - The failure branch of an update now reads only its own key. Before, it
    read three keys per commit, which capped updates at 42 too.
  - A table listed with requirements but no updates is checked against the
    state read at the start of the attempt; its pointer is not written.
- **Drops (D3, D4):**
  - A plain drop reads and deletes the pointer in one transaction.
  - A drop with purge reads first, because the task must name the metadata
    file, and retries with jitter. A test found that without the jitter it
    starved under back-to-back commits.
- **Purge (D4):** iceberg-go's `Table.PurgeFiles` is not used. It deletes
  everything under the table location, and a table created under a
  namespace `location` has no UUID in its path, so a recreated table would
  lose its files. The worker deletes only referenced files under the
  location, the metadata file last.
- **Idempotency (D5):** as planned. A replayed unregister reads the stored
  metadata file again.
- **Swagger UI (D7):** `swagger-ui-dist` 5.34.1, checked against its npm
  integrity hash. `make swagger-ui-update` refreshes it.
- **Compose:** catalogs scan the purge queue every 2 s. The HA replicas use
  a 10 s lease, so a takeover is quick enough to test.
