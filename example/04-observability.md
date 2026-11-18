# 04 · Observability: Prometheus, Grafana, Jaeger

`deployments/compose/docker-compose.obs.yaml` goes on top of all three
other files ([01](01-base.md), [02](02-ha.md), [03](03-zones.md)). It turns
on tracing in every catalog and kv-node and adds:

| Service | Local URL | What it is |
|---------|-----------|------------|
| `prometheus` | http://localhost:9090 | scrapes catalog-a, catalog-b, kv1–kv5 and kv-backup every 5 s |
| `grafana` | http://localhost:3000 | the *echpochmak overview* dashboard; anonymous admin, local use only |
| `jaeger` | http://localhost:16686 | traces, received over OTLP on `jaeger:4317` |

## Start and stop

```sh
make obs-up       # the whole stack: base, HA, zones and observability
make obs-down     # remove containers and volumes
```

With plain compose:

```sh
OBS="docker compose -f deployments/compose/docker-compose.yaml \
  -f deployments/compose/docker-compose.ha.yaml \
  -f deployments/compose/docker-compose.zones.yaml \
  -f deployments/compose/docker-compose.obs.yaml \
  --env-file deployments/compose/.env --profile kv"
$OBS up -d --build --wait
```

Make some traffic first, so there is something to see:

```sh
docker exec echpochmak-trino-1 trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.obs"
docker exec echpochmak-trino-1 trino --execute "CREATE TABLE IF NOT EXISTS iceberg.obs.t (id bigint)"
for i in $(seq 20); do
  docker exec echpochmak-trino-1 trino --execute "INSERT INTO iceberg.obs.t VALUES ($i)"
done
```

## Grafana

http://localhost:3000 opens on the dashboard
(http://localhost:3000/d/echpochmak-overview), in three rows:

- **Catalog**: ready replicas, commits per second, commits in unknown state,
  pending commit intents; requests, errors and p99 latency per route; commit
  outcomes; store and metadata IO latency; orphan and purge queues; KV client
  attempts.
- **KV cluster**: nodes up, leader, term, leader changes; voters per zone;
  apply lag; WAL fsync p99; proposals; follower lag; disk usage.
- **Backups**: time since the last backup and the last verification, backups
  kept, failures, size and duration.

Kill the raft leader and watch *Leader*, *Term* and *Leader changes* move:

```sh
make build
bin/kvctl -endpoints localhost:23791,localhost:23795 status
docker kill echpochmak-kv1-1; sleep 15; docker start echpochmak-kv1-1     # if kv1 led
```

Grafana's *Explore* view (http://localhost:3000/explore) queries both data
sources, Prometheus and Jaeger, which are provisioned.

## Prometheus

http://localhost:9090/targets lists the eight scrape targets and their
health; http://localhost:9090/graph runs PromQL. Queries to try:

| Question | PromQL |
|----------|--------|
| Commits per second, by outcome | `sum by (outcome) (rate(echpochmak_catalog_commits_total[1m]))` |
| p99 request latency per route | `histogram_quantile(0.99, sum by (le, route) (rate(echpochmak_http_request_duration_seconds_bucket[5m])))` |
| Who leads | `echpochmak_kv_raft_leader_id` |
| Nodes per zone | `count by (zone) (echpochmak_kv_node_info)` |
| Apply lag per node | `echpochmak_kv_raft_commit_index - echpochmak_kv_raft_applied_index` |
| WAL fsync p99 | `histogram_quantile(0.99, sum by (le, instance) (rate(echpochmak_kv_wal_fsync_seconds_bucket[5m])))` |
| Seconds since the last backup | `time() - echpochmak_backup_last_success_timestamp_seconds` |
| Pending commit intents | `max(echpochmak_orphan_intents_pending)` |
| Ready replicas | `sum(echpochmak_catalog_ready)` |

The same through the HTTP API:

```sh
curl -s 'localhost:9090/api/v1/query' --data-urlencode \
  'query=sum by (kind, outcome) (echpochmak_catalog_commits_total)' | jq '.data.result'
curl -s 'localhost:9090/api/v1/targets' | jq -r '.data.activeTargets[] | .labels.instance + " " + .health'
```

The raw metrics of the catalogs are also on their own ports:
http://localhost:18181/metrics and http://localhost:18182/metrics. The
kv-nodes' (`:2381`) and kv-backup's (`:2382`) are only inside the network;
see [03-zones.md](03-zones.md) for reading them, or use Prometheus.

## Jaeger

http://localhost:16686 → *Service* `catalog-server` → *Find Traces*. A
commit's trace (operation `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`)
shows the catalog's commit span (`catalog.commit.table`), the S3 reads and
writes, the store calls (`store.GetTable`, `store.CreateIntent`, `store.CommitTables`),
and under them the gRPC calls into `kv-node` down to `kv.propose`. Search
`kv-node` to see the cluster's side alone.

Direct link: http://localhost:16686/search?service=catalog-server

The query API (Jaeger 2 serves `/api/v3`):

```sh
curl -s localhost:16686/api/v3/services | jq .
START=$(date -u -d '-15 min' +%Y-%m-%dT%H:%M:%SZ); END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -s -G localhost:16686/api/v3/traces \
  --data-urlencode query.service_name=catalog-server \
  --data-urlencode 'query.operation_name=POST /v1/{prefix}/namespaces/{namespace}/tables/{table}' \
  --data-urlencode query.search_depth=5 \
  --data-urlencode query.start_time_min=$START --data-urlencode query.start_time_max=$END \
  | jq '[.result.resourceSpans[].scopeSpans[].spans[].name] | unique'
```

Without the operation filter, most traces are the background workers'
periodic store calls (`store.ListIntents`, `store.ListPurgeTasks`), one
every few seconds per replica.

## From a log line to its trace

Every log line written inside a traced request carries `trace_id`, next to
`request_id`:

```sh
docker logs echpochmak-catalog-a-1 2>&1 | grep 'http request' | grep trace_id | tail -1
```

Paste the `trace_id` into Jaeger's search box (top left) to open that
request's trace.

## Tracing outside this stack

Tracing uses the standard OpenTelemetry variables and is off unless an
endpoint is set. To trace a catalog run on the host into this Jaeger, publish
its OTLP port first (add `"4317:4317"` to the `jaeger` service), then:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 OTEL_EXPORTER_OTLP_INSECURE=true make run
```

## Clean up

```sh
make obs-down
```
