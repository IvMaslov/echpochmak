# echpochmak

An [Apache Iceberg REST catalog](https://iceberg.apache.org/spec/#iceberg-rest-catalog)
server in Go, with its state in a **raft-replicated key-value store built on
[`go.etcd.io/raft/v3`](https://github.com/etcd-io/raft)** — no etcd, no
database: the WAL, snapshots, transport, membership and client protocol are
part of this project.

Engines such as Trino, Spark and PyIceberg use it as their catalog: they
create namespaces, tables and views through it and commit changes with
optimistic concurrency. The catalog writes each table's metadata file to
S3-compatible storage and swaps the table's pointer with compare-and-swap in
the KV store.

```
 Trino · PyIceberg · Java RESTCatalog
                 │  Iceberg REST
                 ▼
 HAProxy ──► catalog-server × N      stateless; any replica serves any request
                 │         └───────► S3 / MinIO   table metadata files
                 ▼ gRPC
 kv-node × 3 or 5                    raft consensus, Pebble per node
                 ▲
 kv-backup ──────┘ ─────────────────► S3          scheduled snapshots
```

The full architecture, layer by layer, with diagrams of every flow, is in
[`arch/phase5/architecture.pdf`](arch/phase5/architecture.pdf).

## Features

- **The Iceberg REST spec for tables and views**: namespaces, tables (also
  staged creates), views, atomic multi-table transactions
  (`transactions/commit`), register, register-view, unregister, rename, and
  `DROP TABLE … purge`, which a worker on every replica carries out from a
  durable queue.
- **Tested against real clients**: Trino 484 and PyIceberg 0.12 end to end,
  iceberg-go's REST client in process, and Iceberg's own Java REST
  Compatibility Kit (with a short, documented list of exclusions).
- **High availability**: any number of stateless catalog replicas behind a
  load balancer; a 3- or 5-node KV cluster that survives losing nodes, the
  raft leader or a whole zone. `Idempotency-Key` retries work across
  replicas.
- **A KV store checked for linearizability** with
  [porcupine](https://github.com/anishathalye/porcupine), under leader kills,
  partitions and message loss, in process and in Docker.
- **Disaster recovery**: scheduled, verified backups to S3; a restore that
  builds a new cluster from a snapshot; a report that rolls tables forward
  to the commits made after the backup; an unsafe recovery from one
  surviving node.
- **No leaked files**: commit intents and a janitor delete the metadata files
  of commits that never landed.
- **Observability**: Prometheus metrics on every binary, OpenTelemetry
  traces from the HTTP request down to the raft proposal, trace IDs in the
  logs, and a Grafana dashboard.
- **A Swagger UI** at `/docs` that shows exactly the served operations.

### Binaries

| Binary | Role |
|--------|------|
| `catalog-server` | The Iceberg REST API. Stateless; with `-store-backend raft` its state is in the KV cluster |
| `kv-node` | One node of the raft KV cluster |
| `kvctl` | Administers the cluster: keys, membership, leader transfer, snapshots and restore |
| `kv-backup` | Backs the cluster up to S3 on a schedule, with retention and test restores |
| `catalogctl` | Compares the catalog with object storage after a restore |

## Status

This is a working, heavily tested implementation, built in phases
(`arch/plan.md`). It is not hardened for production:

- **No authentication and no TLS**: run it on a trusted network only. Anyone
  who reaches a port can read and write the catalog.
- **One catalog per deployment**, no multi-tenancy, and **no credential
  vending**: engines bring their own S3 credentials.
- Not served, and not advertised to clients: OAuth tokens, credential
  vending, S3 request signing, server-side scan planning and functions.
- With the default in-memory backend, catalog state is lost on restart; use
  `-store-backend raft` for anything that should survive.

How to run it, and what to do when something fails, is in
[`docs/operations.md`](docs/operations.md). Known limitations of each phase
are listed in its ADR.

## Requirements

- Go 1.26.2 (see `go.mod`) and GNU Make
- Docker with Compose v2, for the local environment and the end-to-end
  suites

```sh
git clone <this repository> && cd echpochmak
make build        # binaries in ./bin
make test         # unit, contract and in-process cluster tests; no Docker needed
make compose-up   # MinIO, a catalog and Trino; creates deployments/compose/.env from .env.example
```

The compose files use local development credentials only (MinIO's
`minioadmin`); change them in `deployments/compose/.env`, which is not
tracked.

## Common tasks

```sh
make help          # list all targets
make build         # build bin/: catalog-server, catalogctl, kv-node, kvctl, kv-backup
make test          # run tests with the race detector
make lint          # golangci-lint v2 (installed into ./bin on first use)
make ci            # tidy, generated-code check, lint, test, build
make e2e           # compose stack + Trino end-to-end tests
make generate      # regenerate the API code and the KV protocol
make kv-chaos      # 3-node KV cluster in docker, linearizability under chaos
make kv-torture    # longer in-process linearizability run, random seed
make ha-up         # HA stack: 3 kv-nodes, 2 catalogs behind HAProxy, Trino, PyIceberg
make ha-e2e        # HA milestone suite under failures, then remove the stack
make obs-up        # HA stack with 5 kv-nodes in 3 zones, kv-backup, Prometheus, Grafana, Jaeger
make dr-e2e        # disaster drills on that stack, then remove it
make rck           # optional: Iceberg's REST Compatibility Kit, one in-memory catalog
make rck-ha        # optional: the same against the HA stack
```

## Try it

```sh
make compose-up
make trino-cli
```

```sql
CREATE SCHEMA iceberg.demo;
CREATE TABLE iceberg.demo.t (id bigint, name varchar)
  WITH (partitioning = ARRAY['bucket(id, 4)']);
INSERT INTO iceberg.demo.t VALUES (1, 'a'), (2, 'b');
SELECT * FROM iceberg.demo.t;
SELECT * FROM iceberg.demo."t$snapshots";
```

```sql
CREATE VIEW iceberg.demo.v AS SELECT id FROM iceberg.demo.t WHERE id > 1;
SELECT * FROM iceberg.demo.v;
DROP TABLE iceberg.demo.t;  -- the catalog purges the table's files
```

`make e2e` runs the full scenario automatically: CTAS, row-level changes,
time travel, rename, concurrent inserts, views, a multi-table transaction,
unregister and register, and purge.

The API can also be tried in a browser: the Swagger UI at
http://localhost:8181/docs lists exactly the operations the server
implements, with the catalog prefix filled in. Its spec is served at
`/openapi.json`; `-docs=false` turns both off.

## Local environment

```sh
make compose-up    # MinIO + catalog-server + Trino
make trino-cli     # Trino shell; the catalog is called "iceberg"
make compose-down
```

| Service        | URL                                             |
|----------------|-------------------------------------------------|
| catalog-server | http://localhost:8181 (`/healthz`, `/readyz`, `/v1/...`, `/docs`) |
| MinIO API      | http://localhost:9000                           |
| MinIO console  | http://localhost:9001 (minioadmin / minioadmin) |
| Trino          | http://localhost:8080                           |

To run the catalog on the host instead (faster edit-run loop), start only
MinIO in Docker and use `make run`:

```sh
docker compose -f deployments/compose/docker-compose.yaml --env-file deployments/compose/.env up -d minio minio-init
make run
```

If Docker builds cannot reach the Go module proxy, or compose fails with
"all predefined address pools have been fully subnetted" (common behind a
VPN), set `BUILD_NETWORK=host` and `COMPOSE_SUBNET` in
`deployments/compose/.env`. See `.env.example`.

## KV cluster

Start three nodes on one machine; each needs its own data directory and
ports:

```sh
IC=1=http://127.0.0.1:23801,2=http://127.0.0.1:23802,3=http://127.0.0.1:23803
for i in 1 2 3; do
  bin/kv-node -id $i -data-dir /tmp/kv$i -initial-cluster $IC \
    -listen-peer 127.0.0.1:2380$i -listen-client 127.0.0.1:2379$i &
done

export ECHPOCHMAK_KV_ENDPOINTS=127.0.0.1:23791,127.0.0.1:23792,127.0.0.1:23793
bin/kvctl put greeting hello
bin/kvctl get greeting
bin/kvctl status
```

Add a fourth node: register it as a learner, start it with `-join`, then
promote it once it has caught up.

```sh
bin/kvctl member add 4 http://127.0.0.1:23804
bin/kv-node -id 4 -data-dir /tmp/kv4 -join $ECHPOCHMAK_KV_ENDPOINTS \
  -listen-peer 127.0.0.1:23804 -listen-client 127.0.0.1:23794 &
bin/kvctl member promote 4
```

`kvctl` also has `del`, `get -prefix`, `member list|remove`,
`leader transfer` and `-json` output. A node whose data directory has state
restarts from it and ignores `-initial-cluster` and `-join`. In compose, the
same cluster runs under the `kv` profile
(`docker compose --profile kv up`), on host ports 23791–23793.

## High availability

`make ha-up` starts the HA stack:
- 3 kv-nodes.
- Two catalog replicas on the raft backend: `catalog-a` on host port
  18181, `catalog-b` on 18182.
- HAProxy on port 8181, which health-checks `/readyz`.
- MinIO, Trino and a PyIceberg container.

Clients use `localhost:8181` exactly as with the single catalog. Inspect the
catalog's state in the KV store with `kvctl`:

```sh
export ECHPOCHMAK_KV_ENDPOINTS=localhost:23791,localhost:23792,localhost:23793
bin/kvctl get cat/v1/ -prefix
```

`make ha-e2e` writes through Trino and PyIceberg while it kills catalog
replicas and the raft leader, and checks that no acknowledged row is lost or
duplicated. `make ha-down` removes the stack, its containers and its
volumes.

## Disaster resistance and observability

`make obs-up` starts the HA stack with two more overlays: five kv-nodes in
three zones (`docker-compose.zones.yaml`) with `kv-backup`, and Prometheus,
Grafana and Jaeger (`docker-compose.obs.yaml`):

| | |
|---|---|
| Grafana, the *echpochmak overview* dashboard | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

```sh
docker exec echpochmak-kv-backup-1 /kv-backup list      # backups in the bucket
bin/kvctl -endpoints localhost:23791,localhost:23795 status   # zones, leader
bin/kvctl snapshot save backup.snap                     # a snapshot by hand
docker exec echpochmak-catalog-a-1 /catalogctl restore-report
```

`make dr-e2e` runs the disaster drills on that stack: losing a zone, then
the quorum; orphans under faults; metrics and traces; backups and
retention; destroying every kv-node and restoring from the newest backup,
with the restore report and roll-forward; and the unsafe recovery from one
surviving node. The runbooks for each failure are in
[`docs/operations.md`](docs/operations.md).

## REST Compatibility Kit

The RCK is Iceberg's own test suite for REST servers: Java's `CatalogTests`
and `ViewCatalogTests`, run through Java's `RESTCatalog`. It checks the
server against the reference implementation, and is the only independent
client for register and Java's transaction paths. It is optional: it is not
part of `make ci`, `make e2e` or `make ha-e2e`.

```sh
make rck      # against one in-memory catalog
make rck-ha   # against the HA stack, through HAProxy
```

Each target builds the `echpochmak/rck:1.12.0` image if it is missing
(`build/rck/Dockerfile`, Iceberg 1.12.0 on JDK 21, about 7 minutes the first
time, with `BUILD_NETWORK`), starts a fresh stack, runs the kit, copies the
reports to `build/rck-reports/<target>/` and removes every container and
volume. Tests the server deliberately fails are listed with their reasons in
`test/rck/exclusions.txt`; anything else that fails is a server bug.

## Configuration

`catalog-server -help`, `kv-node -help` and `kv-backup -help` list every
flag. Each flag can also be set with an environment variable: `-http-addr`
becomes `ECHPOCHMAK_HTTP_ADDR` for catalog-server, `-data-dir` becomes
`ECHPOCHMAK_KV_DATA_DIR` for kv-node, and `-location` becomes
`ECHPOCHMAK_BACKUP_LOCATION` for kv-backup. A flag on the command line wins
over the environment. Tracing is configured with the standard `OTEL_*`
variables and is off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

Notable settings:

| Flag | Default | Meaning |
|------|---------|---------|
| `-catalog-name` | `main` | URL prefix returned to clients by `/v1/config` |
| `-s3-warehouse` | `s3://warehouse/` | Base location of new tables (`s3://`, `file://` or `mem://`) |
| `-commit-retries` | `4` | Server-side retries of a commit after a concurrent change |
| `-metadata-cache-size` | `1000` | Metadata files cached in memory (`0` disables) |
| `-http-max-body-bytes` | `16 MiB` | Request body limit |
| `-store-backend` | `memory` | `memory`, or `raft` for the replicated KV store |
| `-kv-endpoints` | — | KV client endpoints, comma-separated (raft backend) |
| `-kv-attempt-timeout` / `-kv-request-timeout` | `2s` / `5s` | One KV attempt / one store call with retries; must nest under `-http-write-timeout` (checked at startup) |
| `-http-write-timeout` | `90s` | Must exceed `(commit-retries + 1) × 3 × kv-request-timeout` |
| `-idempotency-lifetime` | `30m` | How long an `Idempotency-Key` is honoured; `0` disables it |
| `-http-drain-delay` | `0` | On shutdown, report unready this long before draining; set it to the load balancer's health-check window |
| `-purge-workers` | `2` | Purge tasks this replica runs at once; `0` leaves them to other replicas |
| `-purge-lease` / `-purge-interval` | `2m` / `10s` | A purge worker's claim on a task; how often an idle worker scans the queue |
| `-docs` | `true` | Serve the Swagger UI at `/docs` and the spec at `/openapi.json` |
| `-metrics` | `true` | Serve Prometheus metrics at `/metrics` |
| `-orphan-cleanup` | `true` | Run the janitor that deletes files of commits that never landed; off after a restore until the report is done |
| `-orphan-interval` / `-orphan-grace` | `1m` / twice the write timeout | Janitor sweeps; age at which an intent counts as abandoned |
| kv-node `-zone` | — | Placement label, reported by `kvctl status` and the metrics |
| kv-node `-listen-metrics` | `:2381` | `/metrics`, `/healthz`, `/readyz` |
| kv-backup `-location` | — | Where backups go, such as `s3://backups/kv` |
| kv-backup `-interval` / `-keep-last` / `-keep-daily` | `5m` / `48` / `14` | Schedule and retention |

## Layout

```
cmd/                  binaries (wiring only)
api/iceberg/          pinned Iceberg OpenAPI spec, overlay, codegen config
internal/api/         HTTP layer; gen/ holds generated code, swaggerui/ the
                      vendored Swagger UI
internal/catalog/     catalog operations: tables, views, transactions,
                      register, the purge worker
internal/fileio/      object storage access, the metadata cache, its metrics
                      and traces
internal/store/       Store interface, types, errors; storetest/ contract suite
internal/store/memstore/
internal/store/raftstore/  Store on the KV cluster
internal/store/instrumented/  Store decorator with metrics and traces
internal/backup/      KV backups in object storage: schedule, retention,
                      verification
internal/idempotency/ Idempotency-Key protocol, memory and KV stores
api/kv/               KV cluster protocol (protobuf, buf config)
internal/kv/          KV cluster: kvpb (generated), statemachine, raftlog,
                      node, transport, server, client, daemon, kvtest,
                      snapshot (the backup file format)
internal/config/      flags and environment
internal/logger/      slog setup, request-scoped loggers, trace IDs in logs
internal/metrics/     the Prometheus registry every binary shares
internal/telemetry/   OpenTelemetry setup from OTEL_* variables
internal/version/     build info (set via -ldflags)
internal/tools/       code-generation helpers
test/conformance/     in-process tests through iceberg-go's REST client
test/e2e/             Trino end-to-end tests (build tag e2e)
test/kvchaos/         KV linearizability under docker chaos (build tag kvchaos)
test/ha/              HA milestone suite (build tag ha), disaster drills
                      (build tag dr), PyIceberg writer
test/rck/             REST Compatibility Kit exclusions
build/                Dockerfiles
deployments/compose/  local environment
arch/                 design docs, plans, ADRs
docs/                 operator's guide
```

## Documentation

| Document | Contents |
|----------|----------|
| [`arch/phase5/architecture.pdf`](arch/phase5/architecture.pdf) | The whole system by layers, with diagrams of every flow |
| [`docs/operations.md`](docs/operations.md) | Deployment rules, runbooks per failure, backups and restore, metrics and traces |
| [`arch/plan.md`](arch/plan.md) | Decisions, architecture and the phased plan |
| `arch/phase0` … `arch/phase5` | Each phase's detailed plan and its ADR (ADR-001 … ADR-006) |

## Contributing

Issues and pull requests are welcome. Before sending a change:

```sh
make ci    # tidy, generated-code check, protocol compatibility, lint, tests, build
```

Changes to the KV protocol go in `api/kv/proto` and need `make generate`;
`make buf-breaking` rejects incompatible changes. Changes that touch
replication or failure handling should also pass `make ha-e2e`, and those
that touch backups or restore `make dr-e2e`; both need Docker. The code
follows `.claude/skills/go-code-style.md`, and golangci-lint must report
no issues.

## License

[MIT](LICENSE). Third-party code keeps its own licence: the vendored
Swagger UI in `internal/api/swaggerui/dist` is under the Apache License 2.0
(its `LICENSE` and `NOTICE` are next to it), and
`.claude/skills/go-code-style.md` is MIT-licensed by its author, as its
header states.

