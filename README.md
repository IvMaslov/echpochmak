# echpochmak

An [Apache Iceberg REST catalog](https://iceberg.apache.org/spec/#iceberg-rest-catalog)
server, with catalog state kept in a raft-replicated key-value store.

- **catalog-server** is a stateless HTTP server implementing the Iceberg REST
  API. Run as many replicas as you like behind a load balancer.
- **kv-node** is one node of a raft cluster, built on `go.etcd.io/raft/v3`,
  that stores namespaces and table/view pointers. (Phase 2)
- **Store** is the interface between them (`internal/store`). An in-memory
  implementation is used for tests and local development.

Table metadata files live in S3-compatible object storage (AWS S3 or MinIO).

Design docs and phase plans are in [`arch/`](arch/).

## Status

Phase 0 (foundations) is done. The server runs and routes the full Iceberg
REST API, but every operation returns `501` until Phase 1.

## Requirements

- Go 1.26.2 (see `go.mod`)
- Docker with Compose v2, for the local environment

## Common tasks

```sh
make help          # list all targets
make build         # build bin/catalog-server and bin/kv-node
make test          # run tests with the race detector
make lint          # golangci-lint v2 (installed into ./bin on first use)
make ci            # tidy, generated-code check, lint, test, build
make generate      # regenerate the API code from the pinned spec
```

## Local environment

```sh
make compose-up    # MinIO + catalog-server + Trino
make trino-cli     # Trino shell; the catalog is called "iceberg"
make compose-down
```

| Service        | URL                                             |
|----------------|-------------------------------------------------|
| catalog-server | http://localhost:8181 (`/healthz`, `/readyz`, `/v1/...`) |
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

## Configuration

`catalog-server -help` lists every flag. Each flag can also be set with an
environment variable: `-http-addr` becomes `ECHPOCHMAK_HTTP_ADDR`. A flag on
the command line wins over the environment.

## Layout

```
cmd/                  binaries (wiring only)
api/iceberg/          pinned Iceberg OpenAPI spec, overlay, codegen config
internal/api/         HTTP layer; gen/ holds generated code
internal/store/       Store interface, types, errors; storetest/ contract suite
internal/store/memstore/
internal/config/      flags and environment
internal/logger/      slog setup and request-scoped loggers
internal/version/     build info (set via -ldflags)
internal/tools/       code-generation helpers
build/                Dockerfiles
deployments/compose/  local environment
arch/                 design docs, plans, ADRs
```
