# 03 · Zones and backups: 5 kv-nodes, kv-backup, restore

`deployments/compose/docker-compose.zones.yaml`, on top of the base and HA
files, grows the KV cluster to five nodes in three zones and adds the backup
service:

| Service | Local URL | What it is |
|---------|-----------|------------|
| `kv1`, `kv2` | `localhost:23791`, `23792` | zone `a` |
| `kv3`, `kv4` | `localhost:23793`, `23794` | zone `b` |
| `kv5` | `localhost:23795` | zone `c` |
| `kv-backup` | — (`:2382` inside the network) | backs the cluster up to `s3://echpochmak-backups/kv` every 30 s |
| everything from [02-ha.md](02-ha.md) | | the catalogs now use all five nodes |

Losing any one zone leaves at least three of five voters, a majority. The
janitor timings are short here (grace 20 s, sweep every 5 s), so orphan
cleanup is visible within seconds.

## Start and stop

The Make targets start this overlay together with the observability one
([04-observability.md](04-observability.md)); to run it without
Prometheus, Grafana and Jaeger:

```sh
ZONES="docker compose -f deployments/compose/docker-compose.yaml \
  -f deployments/compose/docker-compose.ha.yaml \
  -f deployments/compose/docker-compose.zones.yaml \
  --env-file deployments/compose/.env --profile kv"
$ZONES up -d --build --wait
$ZONES down -v --remove-orphans
```

`make obs-up` / `make obs-down` start and remove the same stack plus
observability.

## Zones and placement

```sh
make build
export ECHPOCHMAK_KV_ENDPOINTS=localhost:23791,localhost:23792,localhost:23793,localhost:23794,localhost:23795
bin/kvctl status          # a ZONE column; a warning if one zone holds a majority
bin/kvctl member list
```

Lose zone `a`; writes continue:

```sh
docker kill echpochmak-kv1-1 echpochmak-kv2-1
bin/kvctl -endpoints localhost:23793,localhost:23794,localhost:23795 status
docker exec echpochmak-trino-1 trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.zones"
docker exec echpochmak-trino-1 trino --execute \
  "CREATE TABLE iceberg.zones.t AS SELECT 1 AS id"       # still works with 3 of 5
docker start echpochmak-kv1-1 echpochmak-kv2-1
```

Lose one more node and the quorum is gone: both catalogs answer `503` on
`/readyz` until a third node is back.

Each node's own metrics and probes are on port 2381 inside the network; the
images have no shell, so read them through the PyIceberg container:

```sh
docker exec echpochmak-pyiceberg-1 python -c "
import urllib.request as u
print(u.urlopen('http://kv1:2381/readyz').read().decode())
print('\n'.join(l for l in u.urlopen('http://kv1:2381/metrics').read().decode().splitlines()
                if l.startswith('echpochmak_kv_raft')))"
```

## kv-backup

The service runs `kv-backup run`: a backup every 30 s (skipped when nothing
changed), retention, and a test restore of the newest backup every 2
minutes. Its tools run in the same container:

```sh
docker logs echpochmak-kv-backup-1 2>&1 | tail -5              # "backup stored" / "backup verified"
docker exec echpochmak-kv-backup-1 /kv-backup list             # complete backups, newest first
docker exec echpochmak-kv-backup-1 /kv-backup now              # one backup now, then retention
docker exec echpochmak-kv-backup-1 /kv-backup verify           # restore the newest into memory
docker exec echpochmak-kv-backup-1 /kv-backup probe && echo healthy
docker inspect -f '{{.State.Health.Status}}' echpochmak-kv-backup-1
```

The backups in MinIO, each a `.snap` file and its `.snap.json` manifest:

```sh
docker exec echpochmak-minio-1 sh -c \
  'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null; mc ls --recursive local/echpochmak-backups'
```

or in the console at http://localhost:9001, bucket `echpochmak-backups`.

Fetch a backup to the host and inspect it with `kvctl`:

```sh
mkdir -m 777 -p /tmp/restore
$ZONES run --rm --no-deps -v /tmp/restore:/restore --entrypoint /kv-backup \
  kv-backup fetch /restore/latest.snap                         # or: fetch -at 2026-11-18T10:00:00Z
bin/kvctl snapshot status /tmp/restore/latest.snap             # cluster ID, index, revision, checksum
```

`kvctl` can also take a snapshot directly, from a follower:

```sh
bin/kvctl snapshot save /tmp/restore/manual.snap
```

## Restore the whole cluster

Destroy every node and its disk, then rebuild the cluster from the newest
backup — the runbook of `docs/operations.md`, section 3.5:

```sh
FIVE=1=http://kv1:2380,2=http://kv2:2380,3=http://kv3:2380,4=http://kv4:2380,5=http://kv5:2380

docker stop echpochmak-kv-backup-1                             # 1. pause backups
$ZONES run --rm --no-deps -v /tmp/restore:/restore --entrypoint /kv-backup \
  kv-backup fetch /restore/latest.snap                         # 2. the newest backup

$ZONES rm -fsv kv1 kv2 kv3 kv4 kv5                             # 3. the disaster
docker volume rm echpochmak_kv1-data echpochmak_kv2-data echpochmak_kv3-data \
  echpochmak_kv4-data echpochmak_kv5-data

for id in 1 2 3 4 5; do                                        # 4. restore every node
  $ZONES run --rm --no-deps -v /tmp/restore:/restore:ro --entrypoint /kvctl kv$id \
    snapshot restore /restore/latest.snap -id $id -data-dir /data -initial-cluster $FIVE
done

$ZONES up -d --no-deps kv1 kv2 kv3 kv4 kv5                     # 5. start the new cluster
ORPHAN_CLEANUP=false $ZONES up -d --no-deps --force-recreate catalog-a catalog-b   # 6. janitor off
```

`kvctl status` now shows a new cluster; the data is as of the backup.

## The restore report and roll-forward

Commits made after the backup still have their metadata files in MinIO.
`catalogctl` (in the catalog image) finds them:

```sh
docker exec echpochmak-catalog-a-1 /catalogctl restore-report -scan-warehouse
docker exec echpochmak-catalog-a-1 /catalogctl restore-report -json -deep
docker exec echpochmak-catalog-a-1 /catalogctl restore-report -emit-commands \
  -catalog-url http://localhost:8181
```

`-emit-commands` prints one `curl` call per table that is ahead (register
with `overwrite`) and per table created after the backup (fill in its
namespace and name). Run them from the host, then:

```sh
$ZONES up -d --no-deps --force-recreate catalog-a catalog-b    # 7. janitor back on
docker start echpochmak-kv-backup-1                            # 8. backups again
```

## Unsafe recovery from one node

If only one node's disk survives, export it offline and restore from that
file the same way:

```sh
docker stop echpochmak-kv1-1 echpochmak-kv2-1 echpochmak-kv3-1 echpochmak-kv4-1 echpochmak-kv5-1
$ZONES run --rm --no-deps -v /tmp/restore:/restore --entrypoint /kvctl kv3 \
  snapshot save /restore/kv3.snap -data-dir /data
# then steps 3-6 above, with kv3.snap instead of latest.snap
```

## Orphan cleanup

Every commit records its new metadata file as an intent first; a janitor on
each replica deletes the files of intents that outlived their commit:

```sh
bin/kvctl get cat/v1/intent/ -prefix                            # intents in flight or abandoned
curl -s localhost:18181/metrics | grep -E '^echpochmak_orphan'
docker logs echpochmak-catalog-a-1 2>&1 | grep 'orphans:'
```

## Tests on this stack

```sh
make dr-e2e     # zone loss, orphans, observability, backups, total loss and restore, unsafe recovery
```

## Clean up

```sh
$ZONES down -v --remove-orphans
rm -rf /tmp/restore
```
