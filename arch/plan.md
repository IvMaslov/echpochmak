# Development plan: Iceberg REST catalog on a raft KV store

## Decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | KV store | **Build our own KV store on `go.etcd.io/raft/v3`.** No etcd server and no etcd adapter. |
| 2 | Object storage | **S3-compatible only** (AWS S3 / MinIO). |
| 3 | Auth / multi-tenancy | **Not needed.** The server is trusted-network only, with a single catalog. |
| 4 | Credential vending | **No.** Clients use their own S3 credentials. `/v1/config` returns only static defaults/overrides. |

## Analysis

The three-part split in `prompt.md` matches how production REST catalogs are built: a stateless API layer over a consistent store. Three things shape the design:

1. **The catalog has to write metadata files to S3.** In the Iceberg REST protocol, the client sends `requirements` and `updates` on commit. The **server** checks the requirements, applies the updates, writes a new `vN.metadata.json` to S3/MinIO, and then swaps the table's pointer. The KV store holds only the pointers and namespace properties, not the table data.
2. **Correct commits depend on compare-and-swap.** Every table commit is optimistic concurrency: "swap the pointer only if it's still at version X." The multi-table endpoint `POST /v1/transactions/commit` needs this across several keys at once. So our KV store must support **multi-key transactions with compare conditions** (`Txn{compares, success ops, failure ops}`), not just get and put.
3. **`go.etcd.io/raft/v3` is only the consensus algorithm.** We write everything else ourselves: the WAL, snapshots and log compaction, peer-to-peer network transport, the state machine, linearizable reads (ReadIndex), membership changes and the client protocol. This is the largest and riskiest part of the project, so the plan isolates it behind the `Store` interface. An in-memory store lets the catalog be built and tested in parallel.

## Architecture

```
 Trino / PyIceberg
          │  Iceberg REST (OpenAPI)
          ▼
 ┌──────────────────────────┐   N stateless replicas behind a load balancer
 │ catalog-server           │
 │  api (generated handlers)│
 │  catalog service         │──► S3 / MinIO (metadata.json files)
 │  store.Store interface   │
 └───────────┬──────────────┘
             │ adapter: memstore (tests) | raftstore (gRPC client)
             ▼
 ┌──────────────────────────┐   3 or 5 nodes
 │ kv-node (raft cluster)   │
 └──────────────────────────┘
```

### Adapter interface

Domain-level, so the HTTP layer never sees keys:

```go
type Store interface {
    CreateNamespace(ctx context.Context, ns Namespace, props map[string]string) error
    GetNamespace(ctx context.Context, ns Namespace) (NamespaceRecord, error)
    ListNamespaces(ctx context.Context, parent Namespace, page PageToken) ([]Namespace, PageToken, error)
    UpdateNamespaceProps(ctx context.Context, ns Namespace, upd PropsUpdate) error
    DropNamespace(ctx context.Context, ns Namespace) error

    GetTable(ctx context.Context, id TableIdent) (TableRecord, error) // metadata location + version
    ListTables(ctx context.Context, ns Namespace, page PageToken) ([]TableIdent, PageToken, error)
    // Atomic: every pointer must still be at its expected version, or nothing is written.
    CommitTables(ctx context.Context, commits []TableCommit) error
    RenameTable(ctx context.Context, from, to TableIdent) error
    DropTable(ctx context.Context, id TableIdent) error
    // views: same shape as tables
}
```

`raftstore` translates these calls into KV transactions over this key layout:

- `ns/<ns-path>` → properties
- `tbl/<ns-path>/<name>` → `{metadata_location, version}`
- `view/...` → the same shape as tables

Namespace levels are separated by `\x1f`, the same separator the spec uses in URLs.

## Proposed layout

```
cmd/catalog-server/      cmd/kv-node/
api/openapi.yaml         (pinned Iceberg REST spec)
internal/api/            generated server (oapi-codegen) + handlers, error mapping
internal/catalog/        business logic: validate requirements, apply updates, write metadata
internal/fileio/         S3/MinIO writer/reader (aws-sdk-go-v2 or minio-go)
internal/store/          Store interface, error types, key encoding, contract tests
internal/store/memstore  internal/store/raftstore
internal/kv/             raft node, WAL, snapshots, state machine, transport, gRPC KV service
deploy/                  docker-compose (MinIO, 3 kv-nodes, 2 servers, Trino), k8s manifests
test/e2e/                Trino/PyIceberg scenarios
```

**Reuse:** `github.com/apache/iceberg-go` has the table-metadata types and update/requirement logic (a metadata builder). Using it saves weeks of re-implementing the spec's update semantics, so check its coverage first.

## Phased plan

Phases 1 and 2 are independent and can run in parallel.

### Phase 0: Foundations (about 2–3 days)
- Pin the Iceberg REST OpenAPI spec version and generate server types with oapi-codegen.
- Set up the repo layout, config, `slog` logging, golangci-lint and Makefile.
- Write a `docker-compose` with MinIO and Trino.
- Define the `Store` interface, its errors and the contract test suite, and implement `memstore`.

### Phase 1: Catalog server on memstore (done, 2026-10-19)
- Endpoints: `GET /v1/config`, namespace CRUD, table create (including staged create, moved here from Phase 4 on the assumption that Trino needs it; e2e showed Trino 484 creates tables directly, but staged create stays for other clients)/load/list/drop/rename/exists, `POST .../tables/{t}` commit, and metrics reporting.
- Commit flow: load current metadata → check requirements → apply updates → write the new metadata file to S3 → CAS the pointer. On a CAS conflict, return `409 CommitFailedException`.
- Map errors to the spec's `ErrorModel` and status codes.
- No auth: the server doesn't advertise the OAuth endpoints.
- **Milestone:** Trino can create a table, append data and read it back through the server.

### Phase 2: Raft KV cluster (`kv-node`) (done, 2026-11-08)
1. **State machine:** an ordered KV store (Pebble or bbolt) with MVCC revisions; Range/Put/Delete/`Txn{compares, success, failure}`; deterministic apply.
2. **Single raft node:** the `raft.Node` Ready loop; a WAL that persists `HardState` and entries before sending messages; apply committed entries.
3. **Snapshots and compaction:** snapshot the state machine every N entries, truncate the WAL, and restore on startup.
4. **Peer transport:** gRPC streams between nodes; handle `MsgSnap` delivery to followers that fall behind.
5. **Client gRPC API:** Range/Put/Delete/Txn. Non-leaders forward writes to the leader, and linearizable reads use ReadIndex.
6. **Membership changes:** `ConfChange` to add or remove nodes, a join flow for new nodes, and leader transfer for graceful shutdown.
7. **Deduplication:** a request ID on each write, so a write retried after a leader change is applied only once.
- **Milestone:** the 3-node cluster survives leader kills and network partitions, and `porcupine` reports the operation history as linearizable.

### Phase 3: raftstore adapter and high availability (done, 2026-11-11)
- `raftstore`: a gRPC client with leader discovery, retries with backoff, and the `Store` → KV `Txn` mapping.
- It must pass the same contract tests as `memstore`.
- Run 2 or more stateless `catalog-server` replicas behind a load balancer.
- Add health/readiness probes, graceful shutdown, timeouts, and `Idempotency-Key` support where the spec version has it.
- **Milestone:** Trino + PyIceberg end-to-end tests pass against MinIO + 3 kv-nodes + 2 servers. Killing a server replica or the raft leader mid-test causes no errors beyond retries.

### Phase 4: Full spec coverage (done, 2026-11-14)
- Views, `transactions/commit` (multi-table atomic), `register` table, purge on drop. (Staged create and metrics reporting were done in Phase 1.) Register-view and unregister, both in the pinned spec, were added too.
- Run Iceberg's REST compatibility tests (Java `RESTCatalog` test kit / `iceberg-rest-fixture`) against the server. Done with the REST Compatibility Kit of Iceberg 1.12.0, as an optional check (`make rck`, `make rck-ha`).
- Swagger UI for rest api. For easy manual testing.
- Not served, by decision: OAuth tokens, credential vending, S3 request signing, server-side scan planning, functions.

### Phase 5: Disaster resistance and operations
- Periodic raft snapshots exported to S3, plus a tool that rebuilds a cluster from a snapshot.
- Spread the cluster across availability zones (odd node count). Document the RPO/RTO you're targeting.
- Prometheus metrics (commit latency, conflicts, raft term/leader changes, apply lag, WAL fsync time) and OpenTelemetry tracing.
- Orphan-metadata cleanup for files written by commits that later failed CAS.

## Testing strategy

- **Contract tests for `Store`** that both `memstore` and `raftstore` must pass. This is the main tool for keeping the interface honest.
- **Concurrency tests:** many goroutines commit to the same table, and exactly one wins each version.
- **Raft KV tests:**
  - Unit tests for the state machine and WAL replay.
  - An in-process multi-node cluster with a fault-injecting transport that drops, delays and partitions messages.
  - `porcupine` linearizability checks.
  - Crash-recovery tests: kill a node mid-write, then restart it from the WAL and snapshot.
- **End-to-end:** Trino and PyIceberg in docker-compose with MinIO. Trino is the only SQL engine; Spark is too heavy for the test stack.
