# Phase 0: Foundations

**Goal:** a buildable, lintable, testable skeleton that every later phase builds on. It includes the pinned Iceberg REST spec with generated server types, the project layout, the application logger, lint and Makefile tooling, a local MinIO + Trino environment, and the `Store` interface with its contract tests and an in-memory implementation.

**Out of scope:** real catalog endpoint logic (Phase 1) and anything raft-related (Phase 2).

---

## Step 1: Project layout

Follow the community [golang-standards/project-layout](https://github.com/golang-standards/project-layout) conventions. Use only the directories we actually need, and keep everything private under `internal/` unless there is a real reason to export it.

```
cmd/
  catalog-server/      main package: wiring only (config → logger → store → http server)
  kv-node/             placeholder main for Phase 2 (prints version, exits)
api/
  iceberg/             pinned rest-catalog-open-api.yaml + codegen config + SPEC_VERSION note
internal/
  api/                 generated server code (do not edit) + handler stubs
  catalog/             catalog service (empty in Phase 0, filled in Phase 1)
  config/              config loading for the binaries
  logger/              slog setup and helpers
  store/               Store interface, domain types, errors, key/ident helpers
  store/storetest/     reusable contract test suite
  store/memstore/      in-memory Store implementation
  version/             build info injected via -ldflags
build/
  catalog-server/      Dockerfile
deployments/
  compose/             docker-compose.yaml + MinIO init + Trino catalog config
scripts/               helper scripts used by the Makefile (if any)
test/
  e2e/                 placeholder for end-to-end scenarios (Phase 1+)
arch/                  design docs and phase plans (already exists)
```

Tasks:
- Move the root `main.go` into `cmd/catalog-server/` and delete the root file.
- Keep the module path `echpochmak`.
- Extend `.gitignore` with `bin/`, coverage output, local compose volumes and IDE files.
- Add a short `README.md` covering the project purpose, prerequisites and the main `make` targets.

---

## Step 2: Pin the OpenAPI spec and generate code

**Facts from the spec as of 2026-10-09** (commit `79bc69a5`, file `open-api/rest-catalog-open-api.yaml`):
- The spec is **OpenAPI 3.1.1** and about 6,400 lines long.
- It has 66 uses of `const`, 8 discriminators, 16 `oneOf`, 6 `anyOf` and 60 `allOf`. Many Go generators handle only OpenAPI 3.0 well, so these constructs are the risk.
- It covers config, oauth, namespaces, tables, views, functions, scan planning, credentials, sign, metrics, transactions and `Idempotency-Key`.

Tasks:
1. **Download and pin the spec.** Save it to `api/iceberg/rest-catalog-open-api.yaml`. Record the source URL, commit SHA and download date in `api/iceberg/SPEC_VERSION`, and never edit the copy by hand. Add a `make spec-update` target that re-downloads it, so updates show up as reviewable diffs.
2. **Spike: pick a generator (timebox: 1 day).** Try two candidates against the pinned spec:
   - **ogen** (`github.com/ogen-go/ogen`): typed server interface, good `oneOf`/discriminator support, emits an "unimplemented" handler.
   - **oapi-codegen v2** (`github.com/oapi-codegen/oapi-codegen`): net/http (`std-http`) server mode, a smaller runtime, but weaker OpenAPI 3.1 support.

   Pass/fail criteria for each:
   - Generates without editing the spec.
   - Compiles.
   - Models `TableUpdate`/`TableRequirement` as usable tagged unions.
   - Serves through a plain `net/http` handler, so we can add our own middleware.
   - Tolerates unknown fields in requests.

   **Fallback** if neither passes cleanly: keep a small overlay or patch file for the spec, applied by the generate target. The pinned original stays untouched. If even that fails, hand-write the transport types, reusing `github.com/apache/iceberg-go` types for table metadata, updates and requirements.

   Record the outcome in `arch/phase0/adr-001-codegen.md`.
3. **Wire up generation.**
   - Add the chosen generator as a Go tool dependency (`tool` directive in `go.mod`, via `go get -tool`), so `go generate` works with a pinned version and needs no global install.
   - Generated code goes in `internal/api/gen/` and is committed. It's excluded from linting.
   - `make generate` regenerates it, and CI fails if the regenerated output differs from what's committed.
4. **Handler stub.** Every operation returns `501 Not Implemented` with an Iceberg `ErrorModel` body. This proves the generated server plugs into our HTTP stack. Phase 1 replaces the stubs one by one.
5. **Check `iceberg-go` now.** Evaluate its metadata builder and update/requirement application, since Phase 1 depends on it. Note its coverage gaps in the same ADR.

---

## Step 3: Configuration and the application logger

### Configuration (`internal/config`)
- Use the standard library only: `flag` with environment-variable fallbacks (`ECHPOCHMAK_*`). Avoid heavier libraries until there's a need.
- Phase 0 settings:
  - HTTP listen address; read/write/idle timeouts; shutdown grace period.
  - Log level, log format (`json`/`text`) and whether to add the source location.
  - Store backend (`memory` only for now).
  - S3 settings (endpoint, region, bucket/warehouse, path-style access). Phase 0 only parses and validates them; Phase 1 uses them.
- Validate the config at startup and fail fast with a clear error. Never log secrets: redact S3 keys in the startup "effective config" log line.

### Logger (`internal/logger`)
- Expose one constructor that builds a `*slog.Logger` from config:
  - JSON handler for deployments, text handler for local development.
  - The level is backed by `slog.LevelVar`, so it can be changed at runtime later.
- Attach base attributes to every record: `service`, `version`, `instance` (hostname or pod name).
- Call `slog.SetDefault` once, in `main` only. Library packages receive a `*slog.Logger` explicitly; there's no global logger lookup inside `internal/`.
- Add context helpers so request-scoped attributes travel with the request:
  - Store a logger enriched with `request_id` in the context, and retrieve it (falling back to the base logger).
  - HTTP middleware generates or propagates `X-Request-ID`, and writes one access-log line per request: method, route, status, duration, bytes.
- Keep key names consistent. Define the common attribute keys as constants (`err`, `namespace`, `table`, `request_id`, …).
- Tests: level filtering, the base attributes are present, and the request ID comes back out of the context.

### catalog-server wiring (`cmd/catalog-server`)
- Startup order: config → logger → store (memstore) → HTTP mux.
- Routes on the mux:
  - The generated API under `/v1`.
  - `/healthz` (liveness) and `/readyz` (readiness, which checks the store).
- Graceful shutdown on SIGINT/SIGTERM via `signal.NotifyContext`: stop accepting connections, then drain within the grace period.
- Set the version, commit and build date with `-ldflags` into `internal/version`, and log them at startup.

---

## Step 4: golangci-lint v2

- Pin golangci-lint **v2.x**, using the latest stable release at implementation time. Install it into `./bin` via the Makefile with the official install script or a pinned `go install`, and don't rely on a global binary. (golangci-lint advises against installing it with `go tool`, because its dependencies would mix with ours.)
- Write `.golangci.yml` in the **v2 format** (`version: "2"`), modelled on the community-recommended "golden config" by maratori and adjusted to this project:
  - **`linters.default: standard`**, plus these enabled linters:
    - Bugs and correctness: `errorlint`, `wrapcheck`, `nilerr`, `nilnil`, `bodyclose`, `contextcheck`, `noctx`, `rowserrcheck`, `sqlclosecheck`, `copyloopvar`, `durationcheck`, `exhaustive`, `forcetypeassert`, `makezero`, `musttag`.
    - Quality: `gocritic`, `revive`, `gosec`, `cyclop` / `gocognit` (generous limits), `funlen` (generous), `nestif`, `goconst`, `unconvert`, `unparam`, `usestdlibvars`, `perfsprint`, `prealloc`, `predeclared`, `misspell`, `nolintlint` (require a reason and a specific linter name), `errname`, `sloglint` (enforce key-value consistency and the snake_case key style), `testifylint` (if testify is used), `paralleltest` / `tparallel`, `gochecknoglobals` and `gochecknoinits`.
  - **`formatters`:** `gofumpt` and `goimports` with the local prefix `echpochmak`. `golines` is optional.
  - **Exclusions:**
    - Generated code: `generated: strict`, plus an explicit path exclusion for `internal/api/gen/`.
    - Relax `funlen`, `gochecknoglobals`, `wrapcheck` and `gosec` in `_test.go` files.
    - Relax `gochecknoglobals` for `internal/version`.
  - **Settings:** take the rules in `.claude/skills/go-code-style.md` into account (line length about 120, named booleans for complex conditions, no nil slices or maps), using `lll` with a limit of 120 where a linter can enforce them.
- Add `make lint` and `make fmt`. The goal is a zero-issue baseline at the end of Phase 0, so lint can be a hard CI gate from day one.

---

## Step 5: Makefile

Write a self-documenting Makefile: `make help` lists the targets, generated from `##` comments. Tool versions are pinned in variables at the top, and tools install into `./bin`.

| Target | Purpose |
|--------|---------|
| `help` | List the targets |
| `tools` | Install pinned tools into `./bin` (golangci-lint) |
| `spec-update` | Re-download the Iceberg OpenAPI spec and update `SPEC_VERSION` |
| `generate` | Run codegen (`go generate ./...`) |
| `check-generate` | Regenerate and fail on a diff (for CI) |
| `build` | Build `catalog-server` and `kv-node` into `./bin` with version ldflags |
| `run` | Build and run `catalog-server` locally with dev defaults (text logs, debug level) |
| `test` | `go test -race -count=1 ./...` |
| `test-short` | Fast unit tests only (`-short`) |
| `cover` | Write a coverage profile and an HTML report |
| `lint` | Run golangci-lint |
| `fmt` | Run golangci-lint formatters with `--fix` |
| `tidy` | `go mod tidy` and verify |
| `docker-build` | Build the catalog-server image (`build/catalog-server/Dockerfile`) |
| `compose-up` / `compose-down` / `compose-logs` | Manage the local environment |
| `trino-cli` | Open the Trino CLI inside the container |
| `ci` | `tidy`, `check-generate`, `lint`, `test`, `build`. The single target CI runs |
| `clean` | Remove `./bin` and coverage files |

**Dockerfile (`build/catalog-server/Dockerfile`):** a multi-stage build. The builder stage is the official Go image matching `go.mod`. The final stage is distroless static, running as a non-root user, with a `HEALTHCHECK`-friendly `/healthz` endpoint.

---

## Step 6: Local environment (docker compose)

Create `deployments/compose/docker-compose.yaml`. Pin every image tag to a specific version, never `latest`.

| Service | Image | Notes |
|---------|-------|-------|
| `minio` | `minio/minio` | Ports 9000 (API) and 9001 (console). Fixed dev credentials from an `.env` file (committed as `.env.example`). Named volume. Healthcheck. |
| `minio-init` | `minio/mc` | One-shot job: wait for MinIO, create the `warehouse` bucket, then exit. |
| `catalog` | built from `build/catalog-server` | Port 8181 (the conventional Iceberg REST port). Depends on `minio-init`. Its S3 env points at `http://minio:9000`. |
| `trino` | `trinodb/trino` | Port 8080. Depends on `catalog`. |

Trino catalog file `deployments/compose/trino/catalog/iceberg.properties`:
- `connector.name=iceberg`
- `iceberg.catalog.type=rest`
- `iceberg.rest-catalog.uri=http://catalog:8181`
- Native S3 filesystem: `fs.native-s3.enabled=true`, `s3.endpoint=http://minio:9000`, `s3.path-style-access=true`, `s3.region`, plus the access and secret keys from env.

Notes:
- The catalog stays in the compose file even though its endpoints return 501 in Phase 0. This proves networking, health checks and config end to end.
- Phase 0 is verified by Trino starting and `SHOW CATALOGS` listing `iceberg`. Queries against it are expected to fail until Phase 1.
- Add a compose profile or override so the catalog can also run on the host (`make run`) while MinIO and Trino run in Docker. That gives a fast edit-run loop.
- Later phases add the `kv-node` services (3 nodes) to the same file.

---

## Step 7: Store interface, domain types and errors (`internal/store`)

### Domain types
- **`Namespace`:** an ordered list of levels, with helpers:
  - Encode and decode the URL form (levels separated by `\x1f`, the spec's unit separator).
  - Get the parent; check for an "is prefix of" relationship.
  - Validate: no empty levels, no control characters.
- **`TableIdent` / `ViewIdent`:** namespace plus name.
- **`NamespaceRecord`:** properties and a version.
- **`TableRecord` / `ViewRecord`:** `MetadataLocation` and a `Version` (a monotonically increasing number owned by the store, used for CAS).
- **`PropsUpdate`:** keys to set and keys to remove. The result reports which keys were updated, removed and missing, because the spec's response needs all three.
- **`Page` / `PageToken`:** an opaque token plus a page size, with a default and a maximum. Tokens are opaque to callers and must stay stable under concurrent inserts (they encode the last returned key).
- **`TableCommit`:**
  - Identifier, expected version and new metadata location.
  - A `Create` flag, meaning "the table must not exist" (expected version 0).
  - A pointer-only design: the catalog layer writes metadata files to S3, and the store only swaps pointers.

### Interface
These are the methods from `arch/plan.md`, with their semantics pinned down:
- **Namespaces:**
  - **Create:** fails if the namespace exists, or if a nested namespace's parent doesn't exist.
  - **Drop:** fails if the namespace has child namespaces, tables or views.
  - **List:** returns direct children of the parent only, in sorted order and paginated.
- **Tables:**
  - **Get:** returns the record.
  - **List:** returns tables in one namespace, sorted and paginated.
  - **Drop:** removes only the pointer. Purging files is the catalog layer's job.
  - **Rename:** atomic. The source must exist; the target must not exist as a table **or a view**, and its namespace must exist.
- **`CommitTables`:** applies all commits atomically or none. Each commit's version must match the current one exactly. On a mismatch it returns a conflict error naming the identifier. A single-table commit is the same call with one element.
- **Views:** mirror tables, including registering a view and the shared table/view namespace for name collisions.
- **`Ping` / health:** used by `/readyz`.
- Every method takes `context.Context` first and respects cancellation.

### Errors
Sentinel errors, checked with `errors.Is`. Each maps 1:1 to an Iceberg error type in the HTTP layer:
- `ErrNamespaceNotFound`, `ErrNamespaceAlreadyExists`, `ErrNamespaceNotEmpty`
- `ErrTableNotFound`, `ErrTableAlreadyExists`, `ErrViewNotFound`, `ErrViewAlreadyExists`
- `ErrCommitConflict`: wrapped with the conflicting identifier and the expected and actual versions.
- `ErrUnavailable`: retryable, for when the raft store has no leader or times out. It maps to 503.
- `ErrInvalidArgument`: bad identifiers or page tokens.

### Documentation
Add a package doc comment that states the consistency contract every implementation must meet:
- Linearizable reads and writes.
- Atomic multi-table commits.
- Versions that never go backwards.

`raftstore` must meet the same contract in Phase 3.

---

## Step 8: Contract test suite (`internal/store/storetest`)

- Expose `storetest.Run(t, newStore func(t) store.Store)`. Every implementation calls it from its own `_test.go`.
- Cover each area with table-driven subtests:
  - **Namespace lifecycle:** create, get, list, update properties, drop, plus every error path.
  - **Nesting:** a missing parent; dropping a non-empty namespace (one with children, tables or views).
  - **Listing:** sorted order; pagination with page sizes 1, 2 and larger than the total; tokens staying stable while new items are inserted between pages; empty results come back as `[]`, not nil.
  - **Tables:** create via `CommitTables` with `Create`, get, list, drop, and rename (within and across namespaces, onto an existing table or view, into a missing namespace).
  - **CAS:** a stale version gets `ErrCommitConflict`; versions increase strictly.
  - **Multi-table commit:** if one stale entry is present, *nothing* is applied; the conflict identifies the stale table.
  - **Concurrency:** N goroutines commit to the same table from the same base version, and exactly one succeeds per round. Run under `-race`.
  - **Context:** a cancelled context returns an error promptly.
  - **Isolation:** mutating returned maps or slices doesn't change stored state (defensive copies).

---

## Step 9: In-memory store (`internal/store/memstore`)

- A single `sync.RWMutex` guards ordered data: maps plus a sorted key list, or a B-tree if it's simpler. It's built for correctness and readability, not speed.
- Use the same key encoding helpers that `raftstore` will use later, so pagination tokens behave the same in both.
- `CommitTables` checks every precondition under the write lock before applying anything.
- It must pass `storetest.Run` under `-race`.
- It's used by the catalog-server in Phase 1, and by unit tests permanently.

---

## Step 10: CI (optional, if a remote is added)

GitHub Actions workflow: check out the code, set up Go (version from `go.mod`), cache modules, then run `make ci`. Add a separate job for `make docker-build`. For now the repository has no remote, so `make ci` serves as the local gate.

---

## Order of work

1. Layout, `.gitignore`, README and the moved `main.go` (Step 1).
2. Makefile skeleton plus golangci-lint config and install (Steps 4–5). Lint is green on the empty project.
3. Config and logger, then wire up catalog-server with health endpoints and graceful shutdown (Step 3).
4. Spec pin and generator spike, then the ADR, then generated code and 501 stubs mounted under `/v1` (Step 2).
5. Store types, errors and interface, then the contract suite, then memstore (Steps 7–9).
6. Dockerfile and compose with MinIO, Trino and the catalog (Step 6).
7. `make ci` green; first commit.

## Definition of done

- `make ci` passes: tidy, generated code up to date, zero lint issues, tests green with `-race`, both binaries build.
- `make run` starts catalog-server:
  - `/healthz` and `/readyz` return 200.
  - `GET /v1/config` returns 501 with a valid Iceberg `ErrorModel`.
  - Logs are structured and carry a request ID.
- `make compose-up` brings up MinIO (with the `warehouse` bucket), the catalog and Trino; Trino lists the `iceberg` catalog.
- `memstore` passes the full `storetest` contract suite.
- `arch/phase0/adr-001-codegen.md` records the generator choice and the `iceberg-go` findings.

## Risks

| Risk | Mitigation |
|------|------------|
| OpenAPI 3.1 features (`const`, discriminated unions) break the generators | Timeboxed spike, then a spec overlay, then hand-written types plus `iceberg-go` |
| The upstream spec on `main` changes (it's unreleased) | Pin by commit SHA; updates only through `make spec-update` with a reviewed diff |
| The `Store` interface misses something Phase 1 or 3 needs | The contract suite is the specification; changes to the interface go through it first |
| Trino config differs between versions | Pin the Trino version; keep connector properties in one file |

---

## Implementation notes (2026-10-17)

Phase 0 is implemented. Changes from the plan above:

- **Code generation:** oapi-codegen v2.8.0 won the spike; ogen skips 13 core
  operations. The spec needs two fixes, kept in `api/iceberg/overlay.yaml`.
  They are applied by `internal/tools/applyoverlay` *before* oapi-codegen
  runs, because oapi-codegen's built-in overlay support runs after spec
  validation, which the upstream spec fails. See `adr-001-codegen.md`.
- **MinIO image:** official MinIO images are no longer published on Docker
  Hub or quay.io. Compose uses `pgsty/minio`, a community build of the same
  AGPL code, pinned to a release. It also provides `mc` for bucket setup.
- **Store API:**
  - One `Ident` type serves both tables and views; there are no separate
    `TableIdent`/`ViewIdent`.
  - A commit with `ExpectedVersion` 0 means "create"; there is no separate
    `Create` flag.
  - Versions are store-wide revisions, like etcd's, so a dropped and
    recreated table never reuses a version.
  - Views commit one at a time (`CommitView`); only tables have multi-commit.
- **Logger:** the level is fixed at startup (no `slog.LevelVar` yet).
  `FromContext` falls back to `slog.Default()`, which `main` sets.
- **Lint:**
  - `contextcheck` is off: it reports false positives on standard
    `net/http` middleware.
  - `sloglint`'s `no-raw-keys` is off: it would need a constant for every
    nested `LogValue` key. Common keys are still constants in
    `internal/logger`.
- **Local environment workarounds:** `BUILD_NETWORK` and `COMPOSE_SUBNET` in
  `deployments/compose/.env` work around Docker networking behind a VPN that
  routes the whole address space.
- **Not created:** empty placeholder directories (`scripts/`, `test/e2e/`,
  `internal/catalog/`), since git doesn't track empty directories, and the
  CI workflow, since the repository has no remote. They arrive with the phase
  that fills them.
- **`make check-generate`** needs the generated files to be committed. Until
  the first commit, generation was checked by hand: running `go generate`
  twice produces no diff.
