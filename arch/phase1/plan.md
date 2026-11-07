# Phase 1: Catalog server on memstore

**Goal:** a working Iceberg REST catalog for namespaces and tables, backed by
`memstore` and S3/MinIO, that real engines can use.

**Milestone (from `arch/plan.md`):** Trino can create a table, append data and
read it back through the server.

**Builds on Phase 0:**
- Routing and types generated from the pinned spec (`internal/api/gen`).
- The `Store` interface and its contract suite, plus `memstore`.
- Error mapping to `ErrorModel`, the logger, config and the compose stack.

---

## Scope

### In scope

| Area | Endpoints |
|------|-----------|
| Config | `GET /v1/config` |
| Namespaces | list, create, load, exists (`HEAD`), update properties, drop |
| Tables | list, create (including **staged create**), load, exists (`HEAD`), commit (`POST .../tables/{table}`), rename, drop |
| Metrics | `POST .../tables/{table}/metrics`: accept and log |

Staged create was planned for Phase 4. It moves here because Trino creates
tables through it. See "How Trino talks to the catalog" below.

### Out of scope (later phases)

| Feature | Phase | Behaviour in Phase 1 |
|---------|-------|----------------------|
| Views (all view endpoints) | 4 | 501; not advertised in `/v1/config` |
| `POST /transactions/commit` (multi-table) | 4 | 501; not advertised |
| Register / unregister table | 4 | 501; not advertised |
| Purge on drop (`purgeRequested=true`) | 4 | Drop the pointer only; log that files were kept |
| `Idempotency-Key` semantics | 3 | Header accepted and ignored; `idempotency-key-lifetime` not advertised |
| Scan planning, credentials, request signing, functions, OAuth tokens | not planned | 501; not advertised |
| `raftstore` | 3 | Only `memstore`; state is lost on restart |

---

## How Trino talks to the catalog

Trino's Iceberg connector uses the Iceberg Java REST client. The Phase 1 design
follows from how that client behaves:

1. **Startup:** `GET /v1/config`. The client merges `defaults` and `overrides`
   into its properties. It takes `prefix` from `overrides` and builds every
   later path as `/v1/{prefix}/...`. If `endpoints` is present, the client
   calls **only** those endpoints. Features we don't list (views, for example)
   are skipped instead of failing, so `SHOW TABLES` does not hit the 501 view
   endpoints.
2. **`CREATE SCHEMA`:** `POST /namespaces`, optionally with a `location`
   property.
3. **`CREATE TABLE` / `CREATE TABLE AS`:** a create *transaction*:
   - `POST /namespaces/{ns}/tables` with `stage-create: true`. The server
     returns metadata but persists nothing.
   - Then `POST /namespaces/{ns}/tables/{t}` with the `assert-create`
     requirement and the full list of updates: `assign-uuid`,
     `upgrade-format-version`, `add-schema`, `set-current-schema`, `add-spec`,
     `set-default-spec`, `add-sort-order`, `set-default-sort-order`,
     `set-location`, `set-properties`, plus `add-snapshot` and
     `set-snapshot-ref` for CTAS.

   So **commit must be able to create a table** from an empty base.
4. **`INSERT` / `UPDATE` / `DELETE` / `MERGE`:** Trino writes data, delete
   and manifest files to S3 itself, with its own credentials. Then it commits
   with `assert-ref-snapshot-id` (and often `assert-table-uuid`) plus
   `add-snapshot` and `set-snapshot-ref`.
5. **`SELECT`, `$snapshots` and other metadata tables:** `GET .../tables/{t}`.
   Trino then reads the manifests from S3 itself. The client may send
   `If-None-Match` and the `snapshots` query parameter.
6. **Conflicts:** on `409 CommitFailedException` the client refreshes the
   table and retries the operation itself.

Steps 3 and 4 are the core of this phase. The e2e suite (Step 9) confirms
every point on the pinned Trino version, so no assumption here goes
unchecked.

---

## Design decisions

### D1: The prefix and `/v1/config`

- Add a config setting `catalog-name`, default `main`. `/v1/config` returns
  `overrides: {"prefix": <catalog-name>}`.
- Every `/v1/{prefix}/...` handler checks the prefix. A mismatch returns
  `404 NotFoundException` naming the expected prefix, which turns a client
  misconfiguration into a clear error.
- The `warehouse` query parameter of `/v1/config` is accepted and ignored:
  there is one catalog per server.
- `endpoints` lists exactly what this phase implements, using the spec's
  `"<VERB> <path>"` format. A test keeps this list in sync with the routes
  that don't return 501.
- `defaults` is empty. Clients use their own S3 configuration (decision 4 in
  `arch/plan.md`).

### D2: Who owns which types

| Layer | Types | Why |
|-------|-------|-----|
| HTTP (`internal/api`) | generated `gen.*` request types; **custom response objects** | The generated router decodes requests. Responses must carry the exact metadata JSON. |
| Catalog (`internal/catalog`) | `iceberg-go` `table.Metadata`, `table.Update`, `table.Requirement`, `iceberg.Schema` | Its update and requirement logic implements the spec semantics, so we don't re-implement them (ADR-001). |
| Store | `store.*` pointers | Unchanged from Phase 0 |

- **Requests:** the generated union types (`TableUpdate`, `TableRequirement`)
  wrap raw JSON. The handler re-encodes them and decodes them with
  `iceberg-go` (`table.Updates`, `table.Requirements`). Schema, partition spec
  and sort order in create requests go through the same path.
- **Responses that contain table metadata** (create, load, commit) don't use
  the generated `TableMetadata` struct. A struct that drops one field the
  model doesn't know would silently corrupt metadata for clients. Instead, the
  handler returns its own types that implement the generated
  `...ResponseObject` interfaces and write `iceberg-go`'s JSON directly.
- **Main risk: requests can lose fields** in the round trip through the
  generated structs. Mitigation: golden round-trip tests with real payloads
  (Step 8). If a case loses data, add a middleware that keeps the raw body
  for the three table-write endpoints only.

### D3: Table location and metadata files

- **Location of a new table**, first match wins:
  1. `location` in the create request.
  2. The namespace's `location` property, plus `/<table>`.
  3. `<warehouse>/<ns level 1>/.../<ns level n>/<table>-<uuid>`.

  The UUID suffix in rule 3 matters because purge is out of scope: a table
  that is dropped and recreated under the same name must not share a
  directory with the old table's files.
- **Metadata file names:** `<location>/metadata/<NNNNN>-<uuid>.metadata.json`,
  produced by `iceberg-go`'s location provider. It honours
  `write.metadata.path` and `write.metadata.compression-codec`.
- **Next version:** the current file name's version plus 1. If the name
  can't be parsed (for example a table registered later from an arbitrary
  file), fall back to the metadata-log length plus 1.
- **Never overwrite:** every write goes to a new file. The UUID in the name
  makes collisions impossible, so a failed commit never damages a file a
  reader might be using.
- **Reading:** plain JSON and `.gz.metadata.json`.

### D4: The commit algorithm (the heart of this phase)

For `POST .../tables/{t}` with requirements R and updates U:

1. **Load** the pointer from the store.
   - If it's missing and R contains `assert-create`, the base is empty
     metadata (create path).
   - If it's missing otherwise: `404 NoSuchTableException`.
2. **Read** the current metadata file through the cache (D5).
3. **Validate** every requirement against the current metadata. A failure is
   `409 CommitFailedException`, with the requirement's message.
4. **Apply** U with `iceberg-go`'s metadata builder. Invalid updates (unknown
   schema ID, bad ref, …) are `400 BadRequestException`.
5. **Write** the new metadata file (D3).
6. **Swap the pointer** with `CommitTables`, at the expected version from
   step 1, or version 0 when creating.
7. **On `ErrCommitConflict`:**
   - Delete the file from step 5 (best effort, logged).
   - Go back to step 1, up to `commit-retries` times (default 4), with short
     jittered backoff.
   - Requirements are re-checked against the new state, so a retry succeeds
     only when the client's assumptions still hold. This is why the spec
     makes requirements explicit.
   - When retries run out: `409 CommitFailedException`.
8. **On any other store error after step 5** (timeout, unavailable): the
   outcome is unknown. Return `503` with **`CommitStateUnknownException`**,
   and do **not** delete the file, because the commit may have landed.
   `memstore` never fails this way, but the mapping must be right before
   `raftstore` (Phase 3) can produce it.
9. **On success:**
   - Delete metadata files that dropped out of the metadata log, if
     `write.metadata.delete-after-commit.enabled` is set (best effort).
   - Return `metadata-location` and `metadata` with an `ETag`.

**Create (non-staged)** runs the same pipeline with an empty base and updates
synthesised from the request. Using the same code path for both creates is
what keeps them consistent.

**Staged create** builds the same metadata but doesn't write a file or touch
the store. It returns `LoadTableResult` without `metadata-location`. The
later commit (with `assert-create`) does the persisting.

### D5: Metadata cache

Metadata files are immutable and are addressed by their location, so cache
the parsed metadata in a size-bounded LRU keyed by location. No invalidation
is ever needed. This removes the S3 read from most loads and commits. It is
on by default (`metadata-cache-size`, default 1000 entries), and size 0
disables it.

### D6: Pagination compatibility

The spec says a client signals pagination support by sending `pageToken`,
even an empty one. A request **without** `pageToken` must get the full
listing. The handler walks the store's pages until done in that case;
otherwise it passes `pageToken` and `pageSize` through.

### D7: ETags

- Load, create and commit responses carry `ETag`: a hash of
  `metadata-location` plus the `snapshots` mode.
- `GET` with a matching `If-None-Match` returns `304` with no body.
- The `snapshots=refs` parameter is accepted, but Phase 1 always returns all
  snapshots. That's a valid superset; filtering is a later optimisation.

---

## Step 1: Dependencies and FileIO (`internal/fileio`)

- Add `github.com/apache/iceberg-go` (pinned, v0.7.0 or newer) and register
  its S3 implementation with a blank import of `io/gocloud/s3` in
  `cmd/catalog-server` only (the style guide keeps blank imports in `main`).
- Define a small interface, owned by the catalog: read a metadata file, write
  a new one, delete one. This is the boundary to object storage; the catalog
  never touches `iceberg-go`'s IO registry directly.
- Two implementations:
  - **S3**, through `iceberg-go`'s IO. Settings map from our config:
    endpoint, region, credentials, and path-style addressing (which becomes
    `s3.force-virtual-addressing=false`).
  - **In-memory**, through `iceberg-go`'s `mem://` IO, for unit and
    conformance tests.
- Add the metadata LRU cache (D5) as a decorator on the read path. Use a
  small hand-written LRU unless a dependency pays for itself.
- Errors: map "not found" to a typed error. A pointer to a missing metadata
  file is a 500 with a clear log line; it means data loss or manual tampering.
- Tests: write/read round trip, gzip reading, cache hits and eviction, and a
  delete of a missing file being harmless.

## Step 2: Configuration

New settings, each with a flag and an `ECHPOCHMAK_*` environment variable:

| Setting | Default | Purpose |
|---------|---------|---------|
| `catalog-name` | `main` | The prefix returned by `/v1/config` (D1) |
| `commit-retries` | `4` | Server-side retries on CAS conflict (D4) |
| `metadata-cache-size` | `1000` | LRU entries; `0` disables (D5) |
| `http-max-body-bytes` | `16 MiB` | Request body limit; large commits can carry many snapshot updates |

Changes to existing settings:
- `s3-warehouse` also accepts `file://` and `mem://`, for local runs without
  MinIO and for tests. The error message lists the allowed schemes.
- The startup "effective config" log includes the new settings. Secrets stay
  redacted.

## Step 3: Catalog service (`internal/catalog`)

This package holds the business logic and has no HTTP types. It depends on
`store.Store`, the FileIO interface, a clock and a UUID source (both
injectable, for deterministic tests).

| Operation | Behaviour |
|-----------|-----------|
| Create namespace | Validate; store; return the namespace and properties |
| Load / exists namespace | Store lookup |
| List namespaces | Validate the parent; page through the store |
| Update namespace properties | Reject a key that's both updated and removed with **422** (new error, see Step 5); otherwise call the store |
| Drop namespace | The store enforces emptiness → `409 NamespaceNotEmptyException` |
| Create table | Location (D3); build metadata with fresh IDs (the server reassigns field, spec and sort-order IDs, as the spec requires); write; CAS create |
| Stage create | As create, without the write and the CAS |
| Load table | Pointer → cached metadata → result with ETag |
| Commit table | D4 |
| Rename table | Store rename (location unchanged) |
| Drop table | Store drop; log when `purgeRequested` is set (files kept) |
| Table exists | Store lookup |
| Report metrics | Validate the table exists; log the report at debug level |

Logging: one line per mutating operation at info level, with namespace,
table, old and new version, metadata location, retry count and duration.
Conflicts and retries are logged at warn level.

## Step 4: HTTP handlers (`internal/api`)

Replace the 501 stubs for in-scope operations. Each handler:
1. Checks the prefix (D1).
2. Parses the namespace from the path (`%1F` separators) with
   `store.ParseNamespace`.
3. Converts the generated request types to catalog inputs (D2).
4. Calls the service.
5. Returns a typed success response, or an error that `writeError` maps.

| Operation | Success | Notable errors | Notes |
|-----------|---------|----------------|-------|
| `GET /v1/config` | 200 | none | D1 |
| `GET /namespaces` | 200 | 404 parent | `parent`, `pageToken`, `pageSize`; D6 |
| `POST /namespaces` | 200 | 409, 404 parent | Echo namespace and properties |
| `GET /namespaces/{ns}` | 200 | 404 | |
| `HEAD /namespaces/{ns}` | 204 | 404 | No body |
| `POST /namespaces/{ns}/properties` | 200 | 404, 422 | `updated` / `removed` / `missing` |
| `DELETE /namespaces/{ns}` | 204 | 404, 409 | |
| `GET /namespaces/{ns}/tables` | 200 | 404 | D6 |
| `POST /namespaces/{ns}/tables` | 200 | 404 ns, 409, 400 | `stage-create`; ETag |
| `GET /namespaces/{ns}/tables/{t}` | 200 / 304 | 404 | `If-None-Match`, `snapshots`; ETag (D7) |
| `HEAD /namespaces/{ns}/tables/{t}` | 204 | 404 | |
| `POST /namespaces/{ns}/tables/{t}` | 200 | 404, 409, 400, 503 state unknown | D4; ETag |
| `DELETE /namespaces/{ns}/tables/{t}` | 204 | 404 | `purgeRequested` |
| `POST /tables/rename` | 204 | 404 source / ns, 409 | |
| `POST /namespaces/{ns}/tables/{t}/metrics` | 204 | 404 | |

Also:
- Wrap request bodies in `http.MaxBytesReader` with `http-max-body-bytes`.
  Exceeding it returns `413` with an `ErrorModel`.
- Empty lists encode as `[]`, never `null`.

## Step 5: Error mapping

Additions to `internal/api/errors.go` and the catalog errors:

| Error | Status | Iceberg type |
|-------|--------|--------------|
| Requirement failed / CAS retries exhausted | 409 | `CommitFailedException` (exists) |
| Store failure after the metadata write | 503 | `CommitStateUnknownException` |
| Invalid update, schema or spec | 400 | `BadRequestException` |
| Property key both updated and removed | 422 | `UnprocessableEntityException` |
| Wrong prefix / unknown route | 404 | `NotFoundException` |
| Body too large | 413 | `BadRequestException` |
| Pointer to a missing metadata file | 500 | `InternalServerError` (logged with the location) |

Messages name the offending namespace, table, requirement or update, so a
Trino error is actionable without reading server logs.

## Step 6: Wiring

- `cmd/catalog-server`: build the FileIO from the S3 config, wrap it with the
  cache, build the catalog service, and pass it to `api.NewServer`.
- `/readyz` keeps checking the store only. Checking S3 there would mark every
  replica unready during an S3 outage that clients would hit anyway.

## Step 7: Local environment

- Compose: no change to services. The catalog already receives the MinIO
  settings.
- Add `make e2e`: brings the stack up, runs the e2e suite (Step 9), and
  leaves the stack running for debugging.
- README: update the status, and add a "try it" section with the Trino SQL
  from the milestone.

## Step 8: Tests

### Unit tests (`internal/catalog`)
Run on `memstore` plus in-memory FileIO, with a fixed clock and UUIDs.
- **Create:** location rules (D3), fresh-ID assignment, a missing namespace,
  a duplicate table, and a table/view name clash.
- **Staged create then commit:** produces the same table as a direct create.
- **Commit:**
  - Each requirement type fails correctly.
  - Updates apply.
  - Version and metadata-log progression.
  - An invalid update returns 400 and writes no file.
- **Concurrency and retries** (with a store wrapper that injects conflicts):
  - A conflict whose requirements still hold is retried and succeeds.
  - A conflict that breaks a requirement fails with 409.
  - Retries are bounded.
  - Orphan files from failed attempts are deleted.
  - A store error after the write gives "state unknown", and the file is
    **not** deleted.
- **Delete-after-commit:** honours `write.metadata.previous-versions-max`.

### HTTP tests (`internal/api`)
- Every row of the Step 4 table, including the error statuses, HEAD
  responses without a body, 304, ETag stability and the prefix check.
- Pagination with and without `pageToken`.
- `/v1/config` `endpoints` equals the set of implemented routes.

### Round-trip golden tests (risk in D2)
- Fixtures: real request bodies covering every update type, nested and v3
  schemas, partition specs with each transform, and sort orders. They're
  captured from the Java and iceberg-go clients, and checked into
  `internal/api/testdata`.
- Each fixture goes through the generated types and the `iceberg-go` decode,
  and the result must equal decoding the original JSON directly.

### Conformance (in process)
- Run `iceberg-go`'s `catalog/catalogtest` suite through `iceberg-go`'s REST
  client, against our handler on `httptest`, with `memstore` and a `mem://`
  warehouse.
- Add an in-process end-to-end test with the same client: create a table,
  append Arrow data, scan it back, then append concurrently from two clients.
  This proves the commit path without Docker.

### End-to-end with Trino (`test/e2e`, build tag `e2e`)
A Go test against the compose stack, using the Trino HTTP API through
`trino-go-client` (a test-only dependency). Scenario:
1. `CREATE SCHEMA`, `SHOW SCHEMAS`.
2. `CREATE TABLE` with a partition spec, then `INSERT`, `SELECT`, and
   `INSERT` again.
3. `CREATE TABLE AS SELECT` from `tpch.tiny.nation`.
4. `ALTER TABLE ADD COLUMN`, `UPDATE`, `DELETE`, `MERGE`.
5. `SELECT` from `$snapshots`, then time travel with `FOR VERSION AS OF`.
6. `ALTER TABLE RENAME`, `SHOW TABLES` (exercises endpoint gating for views).
7. Two concurrent `INSERT`s into one table: both succeed, with no lost rows.
8. `DROP TABLE`, `DROP SCHEMA`.

Assertions cover the SQL results and the server state: pointer versions, and
the metadata files in MinIO.

## Step 9: Documentation

- `arch/phase1/adr-002-metadata.md`:
  - The type boundary (D2) and the outcome of the round-trip tests.
  - The location layout (D3).
  - The commit algorithm (D4), including the "state unknown" rule that
    Phase 3 relies on.
- Update `arch/plan.md`: staged create moved into Phase 1; the Phase 1
  milestone is met.

---

## Order of work

1. Config additions and the FileIO layer with its tests (Steps 1–2).
2. Catalog service: namespaces first, then create, load and commit, then
   rename, drop and metrics (Step 3), each with unit tests.
3. Round-trip golden tests (Step 8). Do these **before** the table handlers,
   because a failure changes how the handlers read bodies (D2).
4. Handlers and error mapping (Steps 4–5), endpoint by endpoint, each with
   HTTP tests; `/v1/config` last, once its endpoint list is final.
5. Wiring (Step 6), then the conformance suite and the in-process
   end-to-end test.
6. `make e2e` with Trino (Steps 7–8). Fix what it finds.
7. ADR-002 and the plan and README updates (Step 9).

## Definition of done

- `make ci` passes: lint clean, tests green with `-race`, generated code up
  to date.
- `iceberg-go`'s `catalogtest` suite passes in process.
- `make e2e` passes the whole Trino scenario against MinIO, including the
  concurrent-insert check.
- `/v1/config` advertises exactly the implemented endpoints. Everything else
  still returns `501` with an `ErrorModel`.
- ADR-002 is written, and `arch/plan.md` reflects the scope change.

## Risks

| Risk | Mitigation |
|------|------------|
| The generated request structs drop fields that `iceberg-go` needs (D2) | Golden round-trip tests first; fallback is raw-body capture for three endpoints |
| `iceberg-go` misses or differs from Java on an update's semantics | The Trino e2e suite exercises the updates Trino sends; fix upstream or patch locally, with a test |
| Trino's client behaves differently from the description above | Every point is checked by the e2e scenario; both create paths are implemented anyway |
| Orphaned metadata files from failed commits | Deleted on CAS failure; full cleanup is Phase 5 |
| Dropped tables leave data in MinIO (no purge) | Accepted for Phase 1; UUID-suffixed locations prevent collisions (D3) |
| `memstore` loses everything on restart | Expected until Phase 3; documented in the README |
| Large metadata files make commits slow | The cache removes reads; writes are unavoidable; measure in e2e and revisit with compression |

---

## Implementation notes (2026-10-19)

Phase 1 is implemented and the milestone is met: `make e2e` runs the full
Trino scenario against MinIO, including concurrent inserts, and passes.
Details are in `adr-002-metadata.md`. Changes from the plan:

- **D2's fallback was needed:** the generated `TableRequirement` keeps only
  its `type`, so create and commit bodies are decoded from the raw body with
  `iceberg-go`, via a body-capturing middleware. This was found by
  inspecting the generated code before writing the handlers, and a test pins
  it. The golden test covers all 23 updates and 8 requirements through this
  path.
- **Non-staged create** builds metadata directly with
  `table.NewMetadataWithUUID` instead of synthesising updates for the commit
  pipeline. Staged create plus the `assert-create` commit does go through
  the commit pipeline.
- **The metadata cache holds file bytes, not parsed metadata,** so no parsed
  object is shared between requests (D5).
- **No clock injection:** `iceberg-go`'s builder reads the system clock
  itself, so tests use relative timestamps instead.
- **Test dependency:** `trino-go-client` is pinned to v1.1.0. Newer releases
  require Go 1.27, while the project, Dockerfile and lint toolchain are on
  Go 1.26.2.
- **Lint config:**
  - `govet`'s `shadow` is off, because it conflicts with the scoped
    `if err := …` idiom the style guide prefers.
  - `goconst` is off in tests.
  - e2e files are linted (`build-tags: e2e`), with `paralleltest` and
    `tparallel` off for the sequential scenario.
- **Found by e2e and fixed:** the access log recorded `route=unmatched` for
  every request with a body, because the body middleware replaced the
  request. The matched route now travels back through the context.
- **Observed Trino traffic:**
  - **Correction (2026-11-07):** Trino 484 does *not* use staged create.
    Every `CREATE TABLE` / CTAS is a direct create (`00000-*` metadata file),
    then ordinary commits. The "How Trino talks to the catalog" section above
    was wrong on this point; staged create stays, for other clients.
  - Appends assert the branch head; conflicting inserts get 409 and Trino
    retries them.
  - The server retried one pure CAS conflict itself.
  - Trino reports metrics after every scan and commit (accepted with 204).
