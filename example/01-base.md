# 01 · Base stack: MinIO, one catalog, Trino

`deployments/compose/docker-compose.yaml` runs the smallest useful setup:

| Service | Local URL | What it is |
|---------|-----------|------------|
| `catalog` | http://localhost:8181 | `catalog-server` on the **in-memory** store: state is lost when it stops |
| `minio` | http://localhost:9000 (S3), http://localhost:9001 (console) | object storage for metadata and data files |
| `minio-init` | — | creates the buckets `warehouse` and `echpochmak-backups`, then exits |
| `trino` | http://localhost:8080 | Trino 484 with an `iceberg` catalog pointing at the server |
| `kv1`–`kv3` | `localhost:23791`–`23793` | optional 3-node KV cluster, profile `kv` |
| `rck` | — | optional REST Compatibility Kit, profile `rck` |

## Start and stop

```sh
make compose-up        # build and start, wait until healthy
make compose-logs      # follow the logs
make compose-down      # stop, keep the data
make compose-clean     # stop and delete the data
```

The same with plain compose:

```sh
BASE="docker compose -f deployments/compose/docker-compose.yaml --env-file deployments/compose/.env"
$BASE up -d --build --wait
$BASE ps
$BASE down -v
```

## Trino

Open the CLI; the catalog is called `iceberg`:

```sh
make trino-cli
```

```sql
CREATE SCHEMA iceberg.demo;
CREATE TABLE iceberg.demo.orders (id bigint, customer varchar, amount double)
  WITH (partitioning = ARRAY['bucket(id, 4)']);
INSERT INTO iceberg.demo.orders VALUES (1, 'ann', 10.5), (2, 'bob', 20.0);
UPDATE iceberg.demo.orders SET amount = 25.0 WHERE id = 2;

SELECT * FROM iceberg.demo.orders;
SELECT snapshot_id, operation FROM iceberg.demo."orders$snapshots";
SELECT * FROM iceberg.demo.orders FOR VERSION AS OF <snapshot_id>;   -- time travel

CREATE VIEW iceberg.demo.big_orders AS SELECT * FROM iceberg.demo.orders WHERE amount > 15;
SELECT * FROM iceberg.demo.big_orders;

ALTER TABLE iceberg.demo.orders RENAME TO iceberg.demo.orders_v2;
DROP TABLE iceberg.demo.orders_v2;   -- the catalog purges the table's files
```

One-off queries without the interactive shell:

```sh
docker exec echpochmak-trino-1 trino --execute "SHOW SCHEMAS FROM iceberg"
```

The Trino web UI at http://localhost:8080 shows running and finished queries
(any user name, no password).

## The catalog's REST API

Everything Trino did goes through these endpoints; curl works the same way.
The URL prefix is the catalog name, `main`:

```sh
curl -s localhost:8181/v1/config | jq .                       # defaults, prefix, endpoints
curl -s -X POST localhost:8181/v1/main/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace":["sales"],"properties":{"owner":"ann"}}'    # create a namespace
curl -s localhost:8181/v1/main/namespaces | jq .              # list namespaces
curl -s localhost:8181/v1/main/namespaces/sales | jq .        # load one
curl -s -X POST localhost:8181/v1/main/namespaces/sales/properties \
  -H 'Content-Type: application/json' \
  -d '{"updates":{"team":"finance"},"removals":["owner"]}'     # update properties
curl -s localhost:8181/v1/main/namespaces/demo/tables | jq .  # list tables
curl -s localhost:8181/v1/main/namespaces/demo/tables/orders | jq '.["metadata-location"]'
curl -s -o /dev/null -w '%{http_code}\n' -I localhost:8181/v1/main/namespaces/demo/tables/orders  # exists?
curl -s -X DELETE localhost:8181/v1/main/namespaces/sales     # drop an empty namespace
```

Nested namespaces put `%1F` between levels: `/v1/main/namespaces/a%1Fb`.

Create a table with a schema and then commit a change to it:

```sh
curl -s -X POST localhost:8181/v1/main/namespaces/demo/tables \
  -H 'Content-Type: application/json' -d '{
    "name": "events",
    "schema": {"type": "struct", "schema-id": 0, "fields": [
      {"id": 1, "name": "id", "type": "long", "required": true},
      {"id": 2, "name": "kind", "type": "string", "required": false}]}
  }' | jq '.["metadata-location"]'

curl -s -X POST localhost:8181/v1/main/namespaces/demo/tables/events \
  -H 'Content-Type: application/json' -d '{
    "requirements": [],
    "updates": [{"action": "set-properties", "updates": {"owner": "ann"}}]
  }' | jq '.metadata.properties'
```

Retries with an `Idempotency-Key` are applied at most once; the replay
carries `Idempotent-Replay: true`:

```sh
KEY=$(uuidgen)
for i in 1 2; do
  curl -s -D - -o /dev/null -X POST localhost:8181/v1/main/namespaces \
    -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' \
    -d '{"namespace":["once"]}' | grep -iE '^HTTP|idempotent-replay'
done
```

### Swagger UI, probes and metrics

| URL | What |
|-----|------|
| http://localhost:8181/docs | Swagger UI: every served operation, "Try it out" works |
| http://localhost:8181/openapi.json | the served OpenAPI spec |
| http://localhost:8181/healthz | the process runs |
| http://localhost:8181/readyz | the store answers; `503` otherwise |
| http://localhost:8181/metrics | Prometheus metrics |

```sh
curl -s localhost:8181/readyz
curl -s localhost:8181/metrics | grep -E '^echpochmak_(catalog_commits|http_requests)_total'
```

Every response has an `X-Request-ID`; send your own to find a request in the
logs:

```sh
curl -s -H 'X-Request-ID: my-test-1' localhost:8181/v1/main/namespaces >/dev/null
docker logs echpochmak-catalog-1 2>&1 | grep my-test-1
```

## MinIO

The console at http://localhost:9001 (`minioadmin` / `minioadmin`) shows the
buckets: under `warehouse/` every table has a `metadata/` directory with its
`0000N-<uuid>.metadata.json` files, and a `data/` directory written by
Trino. The warehouse bucket is versioned, so deleted files stay recoverable.

The `mc` client ships in the MinIO image:

```sh
MC='docker exec echpochmak-minio-1 sh -c'
$MC 'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null; mc ls local'
$MC 'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null; mc ls --recursive local/warehouse/demo'
$MC 'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null; mc cat local/warehouse/demo/<table-dir>/metadata/<file>.metadata.json' | jq '.["current-snapshot-id"]'
$MC 'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null; mc ls --versions --recursive local/warehouse | head'
```

Any S3 client works against http://localhost:9000 with path-style access, for
example `aws --endpoint-url http://localhost:9000 s3 ls s3://warehouse/`.

## The catalog on the host instead

For a quicker edit-run loop, run only MinIO in Docker and the server from
source:

```sh
docker compose -f deployments/compose/docker-compose.yaml --env-file deployments/compose/.env \
  up -d minio minio-init
make run          # bin/catalog-server against localhost:9000, debug logs
```

## The KV cluster on its own (profile `kv`)

The base file also defines a 3-node KV cluster; nothing uses it until the HA
overlay (02), but it can be explored alone:

```sh
docker compose -f deployments/compose/docker-compose.yaml --env-file deployments/compose/.env \
  --profile kv up -d --build kv1 kv2 kv3

make build
export ECHPOCHMAK_KV_ENDPOINTS=localhost:23791,localhost:23792,localhost:23793
bin/kvctl status                         # leader, term, commit and applied index per node
bin/kvctl put greeting hello
bin/kvctl get greeting
bin/kvctl get "" -prefix                 # every key
bin/kvctl member list
bin/kvctl leader transfer 2              # hand leadership to node 2
bin/kvctl -json status
```

`kvctl` is also inside the image: `docker exec echpochmak-kv1-1 /kvctl
-endpoints kv1:2379,kv2:2379,kv3:2379 status`.

## REST Compatibility Kit (profile `rck`)

Iceberg's own Java test suite against a fresh catalog; builds its image on
first use (about 7 minutes) and removes its stack afterwards:

```sh
make rck
xdg-open build/rck-reports/rck/html/index.html     # the test report
```

## Clean up

```sh
make compose-clean
docker compose -f deployments/compose/docker-compose.yaml --env-file deployments/compose/.env \
  --profile kv down -v      # if you started the KV cluster too
```
