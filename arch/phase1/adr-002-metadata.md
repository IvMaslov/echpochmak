# ADR-002: Table metadata handling

- **Status:** accepted
- **Date:** 2026-10-19
- **Context:** Phase 1 (`arch/phase1/plan.md`, decisions D2–D5)

## 1. Type boundary: raw bodies in, stored bytes out

**Finding.** The generated `gen.TableRequirement` is `struct { Type string }`.
Decoding a commit request through the generated types silently drops every
requirement field: `uuid`, `ref`, `snapshot-id`, and so on. This is exactly
the risk D2 named, and it is real. `TestGeneratedRequirementLosesFields`
pins the behaviour; if it ever fails, the generator has improved.

**Decision.**
- **Requests that change tables** (create and commit) are decoded from the
  raw body straight into `iceberg-go` types: `iceberg.Schema`,
  `iceberg.PartitionSpec`, `table.SortOrder`, `table.Requirements` and
  `table.Updates`. A middleware (`withRawBody`) reads the body once,
  enforces `http-max-body-bytes` (413 above it), keeps a copy in the request
  context, and hands the generated decoder an identical body. That decoder
  still runs, so routing and parameter binding stay generated.
- **Every other request** uses the generated types; they are lossless for
  namespaces, rename and metrics.
- **Responses that carry table metadata** (create, load, commit) are custom
  types that implement the generated `...ResponseObject` interfaces. They
  write `metadata` as the exact JSON stored in the metadata file, never
  re-encoded. Load serves the file's bytes; create and commit serve the
  bytes they just wrote. Clients therefore see every field, including fields
  this server does not model.

**Verified by:**
- `TestDecodeAllCommitTypes`: all 23 update types and all 8 requirement types
  of the spec decode, and their fields survive.
- The `iceberg-go` conformance suite.
- The Trino e2e scenario.

## 2. Table locations and metadata files (D3)

A new table's location, first match wins:
1. `location` from the create request.
2. The namespace's `location` property, plus `/<table>`.
3. `<warehouse>/<ns level 1>/.../<table>-<table uuid>`.

The UUID suffix in rule 3 keeps a recreated table out of the directory of a
dropped one. That matters because purge is not implemented yet.

Metadata files:
- Named `<location>/metadata/<NNNNN>-<uuid>[.gz|.zstd].metadata.json` by
  `iceberg-go`'s location provider. It honours `write.metadata.path` and
  `write.metadata.compression-codec`.
- The version is the current file's version plus 1. Non-conforming names
  fall back to the metadata-log length plus 1.
- Compressed files are recognised by magic bytes, not by extension.
- A file is **never overwritten**; each commit writes a new, uniquely named
  file.

## 3. The commit algorithm (D4)

Implemented in `internal/catalog/commit.go`:

1. **Load** the pointer.
   - Missing, and the commit carries `assert-create`: start from empty
     metadata (create path).
   - Missing otherwise: 404.
2. **Read** the current metadata through the cache.
3. **Check every requirement.** Only `assert-create` can be evaluated when
   there is no metadata; any other requirement on a missing table fails.
   This also avoids nil dereferences inside `iceberg-go`.
4. **Apply the updates** with `table.UpdateTableMetadata`. Invalid updates
   are 400. A commit with no updates on an existing table returns it
   unchanged.
5. **Write** the new metadata file.
6. **Swap the pointer** with `CommitTables` at the version loaded in step 1.
7. **Classify a failed swap:**
   - **Lost a race** (`ErrCommitConflict`, a table created or dropped
     concurrently): delete the new file, then retry from step 1, up to
     `commit-retries` times with jittered backoff. Requirements are checked
     again on every attempt, so a retry never overwrites a change the client
     did not see.
   - **Rejected** (view name clash, namespace gone, invalid argument): delete
     the file and return the error.
   - **Anything else: state unknown.** Keep the file and return
     **503 `CommitStateUnknownException`**, so the client does not delete
     files that may now be live. `memstore` never fails this way. Phase 3's
     `raftstore` must map leader loss and timeouts *after* a proposal to
     errors that land here, not to `ErrCommitConflict`.
8. **On success:** if `write.metadata.delete-after-commit.enabled` is set,
   delete the files that dropped out of the metadata log (best effort).

**Non-staged create** builds the metadata directly with
`table.NewMetadataWithUUID`, which assigns fresh IDs. It then writes it and
swaps the pointer at version 0. **Staged create** builds the same metadata and
persists nothing; the client then creates the table with an `assert-create`
commit, which runs the commit pipeline on an empty base.

The plan predicted that Trino creates tables this way. The e2e logs
(2026-11-07, Trino 484) show otherwise: every `CREATE TABLE` and CTAS is a
non-staged create that writes `00000-*.metadata.json`, followed by ordinary
commits from `00001` on (CTAS adds its snapshot in the first commit). No
table was created by an `assert-create` commit. Staged create stays
implemented and covered by unit and conformance tests, since the spec
defines it and other clients (Spark, the Java `createTransaction` API) use it.

Server-side retries only help when the client's requirements still hold after
the race. Engines that assert the branch head (`assert-ref-snapshot-id`, as
every append does) get 409 and retry on their side; Trino does so by default.
iceberg-go retries only when `commit.retry.num-retries` is set, because its
default is 0.

## 4. Metadata cache (D5)

`fileio.Cached` keeps the **bytes** of recently read files in an LRU keyed by
location (`metadata-cache-size`, default 1000). Because files are never
rewritten, entries never go stale. Bytes are cached rather than parsed
`table.Metadata`, so no parsed object is shared between concurrent requests:
`iceberg-go` metadata objects are not documented as safe for concurrent use.
Each request parses its own copy.

## 5. File IO

`fileio.Iceberg` wraps `iceberg-go`'s IO registry, with one client per
scheme and bucket, created on first use. It runs on a context that is not
cancelled at shutdown, so draining requests can still write. The S3 backend
is registered by a blank import in `cmd/catalog-server` only. `mem://` and
`file://` warehouses work out of the box for tests and local runs.

## Consequences

- **Binary size:** `iceberg-go` brings in Arrow and cloud SDKs. The stripped
  binary is about 60 MiB and the image 65 MB. That's acceptable for now;
  revisit only if it becomes a deployment problem.
- **Phase 3:** `raftstore` must preserve the error classes above. The
  definite failures are conflict, already-exists, not-found and invalid
  argument; everything else becomes "state unknown". The contract suite
  should gain a test for this once a store can fail that way.
