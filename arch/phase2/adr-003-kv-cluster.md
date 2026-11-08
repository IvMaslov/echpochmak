# ADR-003: The raft KV cluster

- **Status:** accepted
- **Date:** 2026-11-08
- **Context:** Phase 2 (`arch/phase2/plan.md`)

## Context

The catalog needs a store with linearizable reads and writes, atomic
multi-key compare-and-swap, and a revision that only grows (the `Store`
contract in `internal/store`). Decision 1 in `arch/plan.md` was to build that
store on `go.etcd.io/raft/v3` rather than run etcd. This ADR records how the
cluster was built, what testing found, and what Phase 3 can rely on.

## Decisions

### Storage: one Pebble database per node

The raft log, the hard state and the state machine all live in one
Pebble (`github.com/cockroachdb/pebble/v2` v2.1.7) database per node, under
separate prefixes (`r/` for raft, `s/` for replicated state, `l/` for
node-local values such as the cluster ID).

- The raft log and hard state are written in one batch and **synced before
  the messages of the same Ready are sent**.
- Applying committed entries writes the data, the new applied index, the
  revision and dedup records in **one unsynced batch**. After a crash, entries
  past the last persisted applied index are applied again from the synced
  log, which is safe because applying is deterministic.
- **Rejected alternatives:**
  - `bbolt` syncs a B+tree on every transaction, which is slow for log
    appends.
  - etcd's `wal` and `snap` packages live inside `go.etcd.io/etcd/server/v3`,
    a very heavy dependency, and using them would defeat the point of
    decision 1.

### Data model

- Each key carries `create_revision`, `mod_revision` and `version`.
- A store-wide revision grows by one for each write transaction that
  changes something, and every key the transaction writes gets that revision.
- There is no history: only the latest value is kept, with no watches and no
  historical reads, because `Store` needs neither.
- `Txn` compares `mod_revision`, `version`, `value`, or **`range_count`** of a
  key range. `range_count` makes "drop a namespace only if it is empty"
  atomic. A count only goes one past its target, which is enough to decide
  any comparison.
- **Determinism** is tested directly: applying the same log twice gives
  byte-identical state, and after a randomized workload with message loss
  every node's state is byte-identical.

### Snapshots: taken when sent, not stored

There are no snapshot files. The state machine is durable, so the log is
compacted every 10,000 applied entries, keeping 5,000 for slow followers.

When a follower needs entries that are gone, the transport opens a **Pebble
snapshot at send time**:
- The header carries the metadata read *inside that view*: index, term and
  conf state, all stored alongside the data.
- That index may be newer than the one raft asked for. That's safe, because
  the data is all committed and the metadata describes exactly what is sent.
- The data streams in 1 MiB chunks with a CRC-32C trailer.
- The follower stages it to a synced file and only passes the snapshot to
  raft once the checksum matches.

### Writes: any node, at most once

- Any node accepts writes, and raft forwards the proposal to the leader.
- The node that received a write completes it when it applies the entry with
  the matching request ID.
- **Deduplication:** each write's result is kept under its request ID for
  100,000 revisions. Pruning goes by revision, not time, so every node
  prunes the same records.
- **Outcomes** are reported precisely:
  - `ErrUnavailable` means the write was certainly not applied (for example,
    the proposal was dropped because there was no leader).
  - `ErrOutcomeUnknown` means it was proposed and its fate is unknown.
  - Over gRPC both are `UNAVAILABLE`; the difference travels in an
    `ErrorDetail`.
- The Go client retries both with the **same request ID**, so an unknown
  outcome usually resolves into a known one. In the chaos run below, not a
  single operation ended unknown.

### Reads: ReadIndex, no leases

Linearizable reads use `ReadIndex` with `ReadOnlySafe`: a heartbeat round
confirms leadership, with no reliance on clocks. The cost is about 0.14 ms
locally. A `serializable` flag serves stale local reads for debugging.

### Membership

- **Bootstrap:** the initial voters are given on every node. The cluster ID
  is derived from that list, so every node computes the same one.
- **Join:** `kvctl member add` adds a learner, the new node starts with
  `-join`, and `kvctl member promote` succeeds once the learner is within
  1,000 entries of the leader.
- **Removal:** a removed node stops itself, and removing the leader works.
- **Leader transfer:** on SIGTERM a leader hands over leadership before it
  stops. It took 20 ms in the smoke test.
- Peer URLs are part of replicated state, so snapshots carry them.

### Transport

- **Two ports per node:** a peer port (2380) and a client port (2379).
- **Raft messages:** one long-lived gRPC client stream per peer, carrying
  protobuf-encoded `raftpb.Message` values.
- **Back-pressure without blocking:** each peer has a bounded queue of 4,096
  messages. When it's full, messages are dropped and the peer is reported
  unreachable, so the Ready loop never blocks.
- **Cluster ID check:** every peer call carries the cluster ID in metadata,
  and nodes reject calls from another cluster.
- **Protocol tooling:** the protocol lives in `api/kv/proto`. Code is
  generated with `buf` v1.72.0 (newer releases need Go ≥ 1.26.7; the project
  stays on 1.26.2) and the protoc plugins, all pinned as `go tool`
  dependencies.

## Bugs found by the tests

1. **The data directory itself was not durable.** The crash tests use
   Pebble's crashable in-memory filesystem, which drops anything not synced,
   directory entries included. A node that crashed soon after its first start
   lost its *entire database*, even though every write inside it had been
   synced: the `data/` and `db/` directory entries never were.

   The same can happen on a real disk after a power loss. `node.Start` now
   creates the directories itself and fsyncs their parents.
2. **A harness bug that looked like a raft bug.** A raft panic
   ("tocommit(140) is out of range [lastIndex(139)]") came from the test
   harness: `Crash` took the disk image *before* cutting the node off, so the
   doomed instance acknowledged an entry that the restarted instance never
   had.

   The in-memory network now tags messages with an instance epoch and drops
   any from a crashed instance. Real crashes stop everything at once; the
   harness now does too.
3. **A test that raced the followers.** `TestReadWithoutLeaderIsUnavailable`
   isolated the nodes before every follower had applied the write. The test
   now waits for that first.

## Verification

| Check | Result |
|-------|--------|
| State machine, raft log and node unit tests | Pass, 10 consecutive runs under `-race` |
| Node scenarios 1–9 from the plan: replication, leader isolation, snapshot catch-up, crash during writes, whole-cluster crash, membership, dedup across a leader change, determinism | Pass, 10 consecutive runs under `-race` |
| Full stack over real gRPC (`internal/kv/daemon`): compare-and-swap, client failover, snapshot stream, join | Pass, 5 consecutive runs |
| In-process linearizability: porcupine, 4 clients, faults (leader isolation, partitions, 20% message loss, crash-restarts) | Ok, about 2,000 operations per run, 6 runs; `make kv-torture` for longer randomized runs |
| The model itself rejects bad histories (stale read, double compare-and-swap, revision going backwards, phantom value) | Pass |
| **Milestone: `make kv-chaos`**, 3 `kv-node` containers, 30 s of `docker kill` on the leader, `docker network disconnect` and `docker restart` | **Ok: 20,630 operations linearizable, 0 with unknown outcome.** Containers and volumes are removed afterwards |
| Real processes by hand: SIGKILL the leader, restart, join and promote a 4th node, SIGTERM the leader | Works |

## Benchmark baseline

3 nodes on one machine, on the real filesystem (laptop NVMe, AMD Ryzen 5
4600H), with an in-memory network. This is a baseline for spotting
regressions, not a prediction for a deployment, where network round trips
add to every write.

| Benchmark | Time per operation |
|-----------|--------------------|
| `BenchmarkPut`: one writer | 2.29 ms (about 435 writes/s) |
| `BenchmarkPutParallel`: 12 writers, batched by raft | 0.56 ms (about 1,800 writes/s) |
| `BenchmarkCAS`: the catalog's commit pattern | 2.26 ms |
| `BenchmarkLinearizableRead` | 0.14 ms |

A single writer pays for two fsyncs in series: the leader's, then the
followers'. Phase 3 commits are compare-and-swaps from many catalog replicas,
so they get the batched rate.

## What Phase 3 can rely on

- `Version` = `mod_revision`. It is store-wide, only grows, and is never
  reused after a drop and recreate.
- `CommitTables` = one `Txn` with one `mod_revision` compare per table.
  Creating a table compares `mod_revision == 0`.
- "Namespace has no children" = a `range_count == 0` compare inside the same
  `Txn`.
- `client.ErrOutcomeUnknown` must become `ErrUnavailable` in `Store`, so the
  catalog answers `CommitStateUnknown` and keeps the metadata file.
  `client.ErrUnavailable` is a definite failure.
- Reads through the client are linearizable by default.

## Known limitations

- **Whole-cluster shutdown is slow:** when every node stops at once, the last
  leader waits out its request timeout (5 s) trying to hand leadership to
  nodes that are stopping too. It is bounded by `-shutdown-timeout`.
- **Lost read confirmations are not retried:** a `ReadIndex` request lost
  during an election is not re-sent. The read times out with
  `ErrUnavailable` and the client retries elsewhere.
- **Conflicting membership changes:** only the leader's own in-flight change
  is detected. A second change proposed while raft still has one pending is
  dropped by raft and reported as an unknown outcome.
- **No raft log entry cache:** the plan mentioned one, but the benchmarks
  don't show `Entries` reads mattering yet.
- **No TLS and no metrics:** both are Phase 5.
