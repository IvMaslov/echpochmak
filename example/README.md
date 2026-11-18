# Examples: local deployments

One guide per docker compose file in `deployments/compose/`. Each starts its
stack, then walks through every tool in it, with the local URLs. The stacks
build on each other: every overlay is layered on the files before it.

| Guide | Compose files | What you get |
|-------|---------------|--------------|
| [01-base.md](01-base.md) | `docker-compose.yaml` | MinIO, one in-memory catalog, Trino; optional 3-node KV cluster and the REST Compatibility Kit |
| [02-ha.md](02-ha.md) | + `docker-compose.ha.yaml` | two catalog replicas on the raft store behind HAProxy, 3 kv-nodes, PyIceberg |
| [03-zones.md](03-zones.md) | + `docker-compose.zones.yaml` | 5 kv-nodes in 3 zones, kv-backup, backups and restore |
| [04-observability.md](04-observability.md) | + `docker-compose.obs.yaml` | Prometheus, Grafana and Jaeger on top of all of the above |

All commands run from the repository root. They need Docker with Compose v2;
the first start builds the images (a few minutes). The `Makefile` copies
`deployments/compose/.env.example` to `deployments/compose/.env` on first use;
to do it by hand:

```sh
cp deployments/compose/.env.example deployments/compose/.env
```

The credentials in it (`minioadmin` / `minioadmin`) are for local use only.

## Every local URL

| URL | Service | Stack |
|-----|---------|-------|
| http://localhost:8181 | the catalog (base), or HAProxy in front of the replicas (HA and up) | all |
| http://localhost:8181/docs | Swagger UI of the catalog API | all |
| http://localhost:9000 | MinIO S3 API | all |
| http://localhost:9001 | MinIO console | all |
| http://localhost:8080 | Trino web UI | all |
| http://localhost:18181, http://localhost:18182 | catalog-a, catalog-b directly | HA and up |
| http://localhost:8404/stats | HAProxy statistics | HA and up |
| `localhost:23791` … `localhost:23795` | kv-node client API (gRPC, for `kvctl`) | kv profile, HA and up |
| http://localhost:9090 | Prometheus | observability |
| http://localhost:3000 | Grafana | observability |
| http://localhost:16686 | Jaeger | observability |

## Cleaning up

Each guide ends with its own `down -v`. To remove everything any guide
started, whatever is running:

```sh
make obs-down
docker rm -f echpochmak-kv-old 2>/dev/null; docker volume rm echpochmak_kv1-old 2>/dev/null
```
