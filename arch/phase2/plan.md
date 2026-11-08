# Phase 2: Raft KV cluster (`kv-node`)

**Goal:** a replicated, linearizable key-value store, built on
`go.etcd.io/raft/v3`, that `raftstore` (Phase 3) can implement the catalog's
`Store` interface on.

**Milestone (from `arch/plan.md`):** a 3-node cluster survives leader kills
and network partitions, and `porcupine` reports the operation history as
linearizable.

**Relation to other phases:**
- Phase 2 does not touch the catalog server. It produces the `kv-node`
  binary, a Go client package, and an admin CLI.
- Phase 3 writes `raftstore` on the Go client. This phase's API is shaped by
  what `raftstore` needs (see "What Phase 3 needs").

---

## Scope

### In scope
- **State machine:** an ordered key-value store with revisions, and atomic
  transactions with compare conditions.
- **Raft integration:** the Ready loop, durable raft log and hard state,
  applying committed entries, log compaction and snapshots.
- **Peer transport:** gRPC message streams between nodes, and streamed
  snapshots for followers that fall behind.
- **Client API (gRPC):** Range, Put, Delete and Txn on any node, with
  linearizable reads through ReadIndex.
- **Cluster API (gRPC):** member list, add, promote, remove; leader
  transfer; node status.
- **Write deduplication:** a request ID on each write, so a retried write is
  applied once.
- **Tools:** `cmd/kv-node`, `cmd/kvctl` (admin CLI), and an `internal/kv/client`
  Go client.
- **Tests:** unit tests, an in-process cluster with fault injection,
  linearizability checks, crash-recovery tests, and a 3-node docker compose
  run with chaos.

### Out of scope

| Feature | Where |
|---------|-------|
| The catalog using the cluster (`raftstore`) | Phase 3 |
| Watches and historical (MVCC) reads | Not needed by `Store`; not planned |
| TLS / mTLS between nodes and clients | Trusted network (decision 3); revisit in Phase 5 |
| Prometheus metrics, tracing | Phase 5 (Phase 2 exposes a status RPC and logs) |
| Snapshot export to S3, restore tooling | Phase 5 |
| Multi-raft / sharding | Not planned: catalog state is small |

---

## What Phase 3 needs

`raftstore` must implement the `Store` contract from `internal/store`. That
fixes these requirements on the KV API:

| `Store` requirement | KV feature |
|---------------------|------------|
| Linearizable reads and writes | Writes through raft; reads through ReadIndex, on any node |
| `Version` strictly increases, never reused after drop/recreate | A store-wide **revision**, bumped once per write transaction; each key keeps the revision of its last write (`mod_revision`) |
| `CommitTables`: all-or-nothing across many tables | `Txn` with several compares and several puts, applied atomically |
| Create only if absent | Compare `mod_revision == 0` |
| Drop namespace only if it has no children, tables or views | Compare **"range is empty"** on a key prefix, evaluated atomically at apply time |
| Create a nested namespace only if the parent exists | Compare `mod_revision > 0` on the parent key |
| Sorted, paginated listings | `Range` over `[key, range_end)` with a limit, in key order, with a "more" flag |
| `ErrUnavailable` vs. definite failure, for `CommitStateUnknown` | The client gets a definite result, or a distinct "outcome unknown" error (timeout, leader lost) |
| Safe retries of writes whose outcome was unknown | Request-ID deduplication in the state machine |

The KV keyspace layout for catalog data (namespace keys, pointer keys) is
Phase 3's job. Phase 2 is a generic KV store.

---

## Design decisions

### D1: One Pebble database per node, for both the raft log and the state

Every node keeps one Pebble (`github.com/cockroachdb/pebble/v2`) database
under its data directory, with separate key prefixes:

| Prefix | Content |
|--------|---------|
| `r/log/<index>` | raft log entries (big-endian index, so iteration is ordered) |
| `r/hardstate` | raft `HardState` (term, vote, commit) |
| `r/truncated` | index and term of the last compacted entry |
| `s/data/<key>` | state machine: user key → value with revisions |
| `s/meta/applied` | applied index, term, conf state, current revision, members |
| `s/dedup/<request-id>` | recent write results, for deduplication (D7) |

Why:
- **Pure Go and production-grade.** It's the engine CockroachDB runs on,
  with no cgo.
- **One engine.** Batches are atomic and can be synced, so durability rules
  are easy to state and test. There is no separate WAL to get right.
- **Crash testing.** Pebble's crashable in-memory filesystem
  (`vfs.NewCrashableMem`, `CrashClone`) drops writes that were never synced.
  That gives honest crash-recovery tests without killing processes.
- **The alternatives were worse.** `bbolt` syncs a B+tree on every
  transaction, which is slow for log appends. etcd's own `wal` and `snap`
  packages live inside `go.etcd.io/etcd/server/v3`, which is a very heavy
  dependency, and using them would defeat the point of decision 1.

### D2: Durability rules

These follow the `raft` package's contract:
- **Raft log and `HardState`** are written in one batch and **synced before
  any message from the same Ready is sent**, whenever `Ready.MustSync` is
  set.
- **Applying committed entries** writes the state-machine changes, the new
  applied index, the revision and dedup records in **one batch**, so applying
  is atomic and idempotent. This batch is not synced: after a crash, entries
  past the persisted applied index are simply re-applied from the durable
  raft log.
- **On startup** the node reads `HardState`, conf state and applied index
  from Pebble, and passes the applied index as `raft.Config.Applied`. Raft
  then replays only the entries the state machine hasn't seen.

### D3: The Ready loop

There is one goroutine per node, driving `raft.Node` in the documented order:

1. Persist `HardState`, entries and any incoming snapshot (D2).
2. Send `Messages` through the transport. A `MsgSnap` triggers a snapshot
   stream (D5).
3. Apply an incoming snapshot to the state machine, if any.
4. Apply `CommittedEntries`: normal entries go to the state machine; conf
   changes go to `ApplyConfChange` and update the member list.
5. Complete waiting reads whose `ReadState` index has been applied (D6).
6. Call `Advance`.

A separate ticker calls `Tick` every `tick-interval` (default 100 ms). The
synchronous mode (`AsyncStorageWrites=false`) keeps the order simple. The
async mode is an optimisation to measure later, not a starting point.

Raft configuration:

| Setting | Value | Why |
|---------|-------|-----|
| `ElectionTick` | 10 (1 s) | Standard |
| `HeartbeatTick` | 1 | Standard |
| `CheckQuorum` | true | A leader cut off from the majority steps down |
| `PreVote` | true | A rejoining partitioned node doesn't disrupt the cluster |
| `ReadOnlyOption` | `ReadOnlySafe` | ReadIndex without relying on clocks (D6) |
| `MaxSizePerMsg` | 1 MiB | |
| `MaxInflightMsgs` | 256 | |
| `MaxUncommittedEntriesSize` | 64 MiB | Back-pressure on proposals when followers lag |
| `StepDownOnRemoval` | true | A removed leader steps down |

### D4: The data model and the operations

Each user key stores its value, `create_revision`, `mod_revision` and
`version` (the number of writes since creation). A store-wide `revision`
counter increases by one for each write transaction that changes anything.
Every key written by that transaction gets that revision as its
`mod_revision`. There is no history: only the latest value of each key is
kept. Watches and historical reads are not needed.

| Operation | Behaviour |
|-----------|-----------|
| `Range(key, range_end, limit)` | Keys in `[key, range_end)` in byte order; `range_end` empty means one key. Returns the key-values, a `more` flag and the current revision. |
| `Put(key, value)` | Creates or replaces a key |
| `DeleteRange(key, range_end)` | Deletes one key or a range; returns the number deleted |
| `Txn(compares, success, failure)` | Evaluates all compares against the current state; runs the `success` operations if all hold, otherwise the `failure` operations. Operations are put, delete and range, so a failed CAS can return the current state. Atomic. |

Compare targets, each with `=`, `≠`, `<` or `>`:
- `mod_revision` of a key (0 means absent).
- `version` of a key.
- `value` of a key.
- **`range_count`** of `[key, range_end)`. This is used for "no children"
  checks.

Puts and deletes go through raft. Reads don't; they use D6. A `Txn` that
contains any write goes through raft as one entry; a read-only `Txn` is
served like a read.

Every apply is **deterministic**: it uses no clocks, no randomness and no map
iteration order. Results depend only on the state and the entry. A test
applies the same log twice and compares the results byte for byte.

**Limits** (configurable): request size 1.5 MiB, key size 4 KiB, 128
operations per transaction, and a range limit of 10,000 keys per call.

### D5: Snapshots and log compaction

- **Compaction:** the state machine is durable in Pebble, so the raft log is
  only needed for followers that are catching up. After every
  `snapshot-count` applied entries (default 10,000), truncate the log up to
  `applied − catch-up-entries` (default 5,000). Truncation is one range
  delete plus an update of `r/truncated`.
- **No snapshot files:** there are no periodic snapshot files. When raft asks
  `Storage.Snapshot()` for a follower that needs data the log no longer has,
  the node takes a **Pebble snapshot**: a consistent read view.
  - Its metadata (index, term, conf state) comes from `s/meta/applied`
    inside that view, so data and metadata always match.
  - The raft `Snapshot` message carries only the metadata plus a snapshot ID.
- **Streaming:** the data goes over a separate gRPC stream (`Peer.Snapshot`)
  in chunks of 1 MiB or less, with a checksum. Snapshot size then isn't
  limited by gRPC message size. When the stream completes, the leader calls
  `ReportSnapshot`. Failures report `SnapshotFailure`, and raft retries.
- **Receiving:** the follower writes the stream into a staging area, then
  hands raft the `MsgSnap`. When the snapshot shows up in `Ready.Snapshot`,
  the follower replaces its `s/` keyspace, the applied index and the log
  truncation point in one synced batch.
- **Concurrency:** at most one outgoing snapshot per follower, and a
  configurable limit on concurrent snapshots per node.

### D6: Linearizable reads

- A client read (`Range`, or a read-only `Txn`) on any node calls
  `ReadIndex` with a unique request context. It waits until the node has
  applied up to the returned index, then reads from local Pebble.
- **No leader leases:** with `ReadOnlySafe`, ReadIndex confirms leadership
  with a heartbeat round instead of trusting clocks. That costs one round
  trip and is always safe.
- A per-request `serializable` flag allows a local read without ReadIndex,
  which may be stale. It's for debugging and for `kvctl`; `raftstore` never
  sets it.
- If no ReadIndex answer arrives before the timeout, for example during an
  election, the read fails with `Unavailable`, and the client retries on
  another node.

### D7: Writes, waiting, and deduplication

- **Any node accepts writes.** It proposes the request and raft forwards the
  proposal to the leader. Every node applies every entry, so the node that
  received the request completes it when it applies the entry with the
  matching request ID. A registry of waiters, keyed by request ID, connects
  the two.
- **Request IDs:** 16 random bytes, generated by the client. A client retries
  with the **same** ID.
- **Deduplication:** applying a write stores its result under
  `s/dedup/<id>`. If an entry arrives with an ID already there, it isn't
  executed again; the stored result is returned. So a retry after "outcome
  unknown" either finds the earlier result or executes once.
- **Pruning dedup records** must be deterministic, so it can't use time.
  Each record keeps the revision it was written at, and records older than
  `dedup-window` revisions (default 100,000) are removed during apply. A
  client must not retry an ID after that many writes have passed; at catalog
  write rates, that's hours.
- **Outcomes reported to the client:**

| Situation | Result |
|-----------|--------|
| Applied | Success, or a `Txn` with `succeeded=false` |
| Rejected before proposing (too large, invalid) | `InvalidArgument` |
| No leader, or the proposal was dropped before commit was confirmed, or a timeout | **`Unavailable` with "outcome unknown"**: the client may retry with the same ID |
| The node is shutting down | `Unavailable` ("outcome unknown" if already proposed) |

This is the distinction Phase 3 needs: a definite conflict maps to `409`, and
"outcome unknown" maps to `CommitStateUnknown`.

### D8: Membership

- **Node IDs** are configured `uint64` values, never reused.
- **Bootstrap:** the first nodes start with `-initial-cluster
  id=peer-url,...` and an empty data directory. Raft is bootstrapped with
  those voters.
- **Join flow:**
  1. `kvctl member add` proposes `ConfChangeAddLearnerNode`. The new node's
     peer URL travels in the conf-change context.
  2. The new node is started with `-join`. It catches up as a learner, via a
     snapshot if needed.
  3. `kvctl member promote` proposes `ConfChangeAddNode`, but only once the
     learner is within `promote-max-lag` entries of the leader.

  Learners never vote, so adding one can't hurt quorum.
- **Remove:** `kvctl member remove` proposes `ConfChangeRemoveNode`. A
  removed node stops itself when it applies its own removal.
- **Member addresses** are part of replicated state (in `s/meta`), so every
  node, and every snapshot, knows how to reach every peer.
- **Leader transfer:** on SIGTERM a leader transfers leadership to the most
  up-to-date follower before stopping, so planned restarts don't wait for an
  election timeout. `kvctl leader transfer` triggers the same by hand.
- Only one conf change can be in flight at a time, which raft enforces.
  `kvctl` reports a clear error if another is pending.

### D9: Transport

- **Two listeners per node:** a **peer** port for raft traffic and snapshots,
  and a **client** port for the KV and cluster APIs. This matches etcd's
  2380/2379 split, so peer traffic can be firewalled separately.
- **Peer messages:** one long-lived gRPC client stream per peer
  (`Peer.Raft`), carrying `raftpb.Message` values. raft v3.7 uses
  protobuf-go, so they can be embedded directly.
- **Never block the Ready loop:** each peer has a bounded send queue. When
  the queue is full, messages are dropped and `ReportUnreachable` is called;
  raft retransmits. A peer that can't be reached is also reported unreachable
  and redialed with backoff.
- **Protocol definitions** live in `.proto` files under `api/kv/`. Code is
  generated with `buf`, `protoc-gen-go` and `protoc-gen-go-grpc`, all pinned
  as `go tool` dependencies, so no system `protoc` is needed. Generated code
  is committed, and `make check-generate` covers it.

### D10: The Go client (`internal/kv/client`)

- Takes a list of endpoints, keeps connections, and sends each call to one
  node. On `Unavailable` it fails over to the next node with jittered
  backoff, until the context deadline.
- Generates request IDs and reuses them on retry (D7).
- Returns errors that keep the definite/unknown distinction: `ErrUnavailable`
  for a read or an unproposed write, and `ErrOutcomeUnknown` for a write that
  may have been applied.
- Offers typed helpers for building `Txn` compares and operations, so
  `raftstore` code reads well.

---

## Step 1: Protocol definitions (`api/kv/`)

Three services, in `.proto` files:
- **`KV`:** Range, Put, DeleteRange and Txn. Messages: `KeyValue`,
  `Compare`, `RequestOp`, `ResponseOp`, and a `ResponseHeader` that carries
  the revision and the ID and term of the serving node.
- **`Cluster`:** MemberList, MemberAdd, MemberPromote, MemberRemove,
  LeaderTransfer and Status. Status reports node ID, leader ID, term, commit
  index, applied index, revision, raft state, and each follower's progress
  when the node is leader.
- **`Peer`** (internal): `Raft` (client stream of `raftpb.Message`) and
  `Snapshot` (client stream of chunks: a header with metadata and a
  checksum, then data).

Internal messages (not exposed): the raft entry payload (`Request` with ID
and one write operation or `Txn`), dedup records, stored key-values and the
member list.

Tooling:
- `buf.yaml` / `buf.gen.yaml`, with generated code under `internal/kv/kvpb`.
- `buf lint` and `buf breaking` (against the last commit) run in `make ci`.
- Update `make generate` and `check-generate` to cover these files.

## Step 2: State machine (`internal/kv/statemachine`)

- Applies one decoded entry to a Pebble batch: the operation, the revision
  bump, the dedup record and dedup pruning, plus the applied index and term.
- Serves reads against a Pebble reader: the DB for current reads, or a
  Pebble snapshot.
- Exports and imports the whole `s/` keyspace for snapshots (D5).
- No raft types inside except entry index and term, so it can be tested on
  its own.
- Tests: table-driven `Txn` semantics for each compare target and result,
  `range_count`, ranges and limits; determinism (same log twice gives
  identical bytes); dedup and pruning; snapshot export/import round trip;
  every limit in D4.

## Step 3: Raft storage (`internal/kv/raftlog`)

- Implements `raft.Storage` on Pebble: `InitialState`, `Entries` (respecting
  `maxSize`), `Term`, `FirstIndex`, `LastIndex`, `Snapshot` (D5).
- Write side: append entries (overwriting a conflicting suffix), set the hard
  state, truncate up to an index, apply an incoming snapshot.
- A small in-memory cache of recent entries (by index) keeps `Entries` calls
  from the leader off disk in the common case.
- Tests: port the behavioural cases of raft's own `MemoryStorage` tests so the
  two implementations agree, plus restart tests (close, reopen, same
  answers), and crash tests with the crashable filesystem (unsynced appends
  vanish; synced ones survive).

## Step 4: Node (`internal/kv/node`)

- Owns `raft.Node`, the Ready loop (D3), the ticker, the waiter registry
  (D7), ReadIndex handling (D6), conf-change application (D8), compaction
  scheduling (D5) and leader transfer on shutdown.
- Exposes a small Go API (propose and wait, linearizable read, status,
  membership) that the gRPC server and the in-process test harness both use.
- Startup:
  - **New cluster:** bootstrap from `-initial-cluster`.
  - **Join:** start without bootstrapping and wait to be contacted.
  - **Restart:** load state from Pebble.

  The node refuses to start when the flags contradict the data directory,
  for example `-initial-cluster` given but data already present for a
  different cluster ID.
- **Cluster ID:** generated at bootstrap and stored in `s/meta`. Peers check
  it on every connection, so a node can't join the wrong cluster by
  mistake.
- Logging: raft's logger adapted to `slog`, plus one line per leader change,
  conf change, snapshot sent or received, and compaction.

## Step 5: Transport (`internal/kv/transport`)

- The peer gRPC service, receiving `Raft` streams and `Snapshot` streams and
  handing them to the node.
- Per-peer senders: a dial with backoff, a bounded queue, unreachable
  reporting, and snapshot streaming with a concurrency limit (D9, D5).
- An interface between node and transport, so the in-process test harness
  can swap in a fault-injecting implementation (Step 9).

## Step 6: Client-facing server (`internal/kv/server`)

- gRPC implementations of `KV` and `Cluster` over the node API.
- Maps node results to gRPC status codes: `InvalidArgument`,
  `FailedPrecondition` (for example a conf change already pending),
  `Unavailable` with an "outcome unknown" detail, and `DeadlineExceeded`.
- The standard gRPC health service (`grpc.health.v1`):
  - `SERVING` when the node knows a leader and its applied index is within
    `ready-max-lag` of the commit index.
  - `NOT_SERVING` otherwise.
- Request size limits enforced at the gRPC layer.

## Step 7: Configuration and binaries

`kv-node` flags, each with an `ECHPOCHMAK_KV_*` environment variable. The
flag+env mechanism moves out of the catalog-specific code in
`internal/config` so both binaries share it.

| Setting | Default | Purpose |
|---------|---------|---------|
| `id` | required | Node ID (`uint64`, never reused) |
| `data-dir` | required | Pebble directory |
| `listen-peer`, `advertise-peer` | `:2380` | Raft traffic |
| `listen-client`, `advertise-client` | `:2379` | KV and cluster API |
| `initial-cluster` | — | `id=peer-url,...` for bootstrap |
| `join` | false | Start as a new member of an existing cluster |
| `tick-interval` | `100ms` | Raft tick |
| `election-ticks`, `heartbeat-ticks` | 10, 1 | D3 |
| `snapshot-count`, `catch-up-entries` | 10,000, 5,000 | D5 |
| `dedup-window` | 100,000 revisions | D7 |
| `request-timeout` | `5s` | Proposal and read timeout |
| `max-request-bytes` | 1.5 MiB | D4 |
| `log-*` | as catalog-server | Shared logger |

- **`cmd/kv-node`:** wiring and graceful shutdown (transfer leadership, stop
  raft, close Pebble).
- **`cmd/kvctl`:** `get`, `put`, `del`, `range` (with `--serializable`),
  `member list|add|promote|remove`, `leader transfer`, `status`. It's a thin
  layer over `internal/kv/client`, with output as a table or `--json`.

## Step 8: Go client (`internal/kv/client`)

As D10. Tests run against an in-process cluster from Step 9: failover when
the node in use dies, retries reusing request IDs, and the unknown-outcome
error when a node dies mid-proposal.

## Step 9: Testing

### In-process cluster harness (`internal/kv/kvtest`)
- Starts N nodes in one process, each with a Pebble database on the
  crashable in-memory filesystem.
- Ticks are driven manually, so time is controllable.
- A fault-injecting transport can drop, delay, duplicate and reorder
  messages, partition the cluster into groups, and isolate the leader.
- Operations:
  - **Crash a node:** take `CrashClone` of its filesystem and restart from
    the clone, losing unsynced writes.
  - **Restart cleanly.**
  - **Add and remove members.**
- Used by the node, client and linearizability tests.

### Scenarios (each with a test)
1. A single node bootstraps, writes, restarts, and has the same data.
2. Three nodes elect a leader; writes on any node are visible to
   linearizable reads on every node.
3. Leader isolated: the majority elects a new leader and keeps serving. The
   old leader steps down (CheckQuorum), and its pending proposals fail with
   "outcome unknown". After healing it rejoins without disruption (PreVote).
4. A follower is partitioned away past compaction, then healed: it catches
   up through a snapshot stream.
5. A node crashes mid-write, using the crash clone: after restart it
   recovers from the synced log, and no acknowledged write is lost.
6. All nodes crash and restart: state and revision are intact.
7. Membership: add a learner, catch up, promote; remove a voter (including
   the leader); quorum holds throughout.
8. Dedup: the same request ID proposed twice, including across a leader
   change, is applied once, and both calls see the same result.
9. Determinism: every node's state-machine bytes are identical after a
   randomized workload.

### Linearizability (milestone)
- A `porcupine` (`github.com/anishathalye/porcupine`) model of the KV store:
  per-key state with value and `mod_revision`, plus `Txn` with compares.
- **Workload:** concurrent clients doing reads, puts and CAS transactions
  over a small key set, so contention is high. Faults run meanwhile: leader
  kills, partitions, message drops and crash-restarts, on a randomized
  schedule.
- **Unknown outcomes** are recorded as "may or may not have happened", as
  porcupine supports. Every recorded history must check as linearizable.
- Runs in CI with a fixed seed and short duration. A longer randomized run
  is a separate make target (`make kv-torture`) for local use.

### Multi-process run (docker compose)
- Add three `kv-node` services to the compose stack, under a `kv` profile,
  each with a named volume. The catalog doesn't use them until Phase 3.
- `make kv-chaos` runs the linearizability workload from the host through
  `internal/kv/client`. Meanwhile a script kills the leader
  (`docker kill`), disconnects a node from the network
  (`docker network disconnect`), and restarts nodes. It checks the history
  with porcupine at the end.
- This is the milestone run on real processes, disks and networks.

### Benchmarks
`go test -bench` for single-key CAS throughput and latency, and
`kvctl`-driven load against the compose cluster. Recorded in the ADR as a
baseline, not as targets.

## Step 10: Documentation

- `arch/phase2/adr-003-kv-cluster.md`:
  - D1 (storage engine), D5 (snapshot design), D7 (writes and dedup) and D9
    (transport), with the alternatives that were rejected.
  - The benchmark baseline.
- `README.md`: running a cluster, adding a member, and `kvctl` usage.
- `arch/plan.md`: mark Phase 2 done; note any API change Phase 3 depends on.

---

## Order of work

1. Protocol definitions and generation tooling (Step 1).
2. State machine with its tests (Step 2). It has no raft dependency, so it
   can be finished and trusted first.
3. Raft storage with its tests, including crash tests (Step 3).
4. Node on a trivial in-memory transport: single-node bootstrap, propose,
   apply, ReadIndex, restart (Step 4, scenario 1).
5. The test harness with the fault-injecting transport (Step 9). Then
   scenarios 2–3 and 8–9.
6. Compaction and snapshot streaming (D5). Then scenarios 4–6.
7. Membership and leader transfer (D8). Then scenario 7.
8. gRPC transport and server, Go client, and binaries (Steps 5–8).
9. Linearizability tests in process, then the compose cluster and
   `make kv-chaos`.
10. Benchmarks and documentation (Step 10).

## Definition of done

- `make ci` passes, including the generated-code check and `buf lint`.
- Every scenario in Step 9 has a passing test; the in-process
  linearizability check passes under `-race`.
- **Milestone:** `make kv-chaos` against the 3-node compose cluster, with
  leader kills, partitions and restarts, ends with a history that porcupine
  confirms as linearizable, and no acknowledged write lost.
- `kvctl` can bootstrap-check, add, promote and remove members on the compose
  cluster.
- ADR-003 is written; `README.md` and `arch/plan.md` are updated.

## Risks

| Risk | Mitigation |
|------|------------|
| Getting the raft contract wrong: Ready order, sync before send, applied index on restart | Follow D2/D3 to the letter; crash tests with the crashable filesystem; porcupine under faults |
| Snapshot and compaction bugs (off-by-one at the truncation point, metadata not matching data) | Metadata read inside the same Pebble view as the data; scenario 4 and a dedicated test of compaction at every boundary |
| Unknown outcomes reported as definite failures, which would make Phase 3 lie to clients | The outcome table in D7, tested in scenario 3 and in the client tests; porcupine treats unknown results correctly |
| Non-deterministic apply | Scenario 9 compares state bytes across nodes |
| Pebble is a sizeable dependency | Accepted: it replaces a hand-written WAL; it's pure Go |
| Learner promotion or removal while partitioned | Promotion gated on lag; removal tested with the leader as the target; one conf change at a time |
| The in-process harness hides real-network problems | `make kv-chaos` on real processes and networks is the milestone, not the harness |
| Linearizability checks get slow with long histories | Keep CI runs short with a fixed seed; long runs stay in `make kv-torture` |

---

## Implementation notes (2026-11-08)

Phase 2 is implemented and the milestone is met: `make kv-chaos` checked
20,630 operations as linearizable through leader kills, network
disconnects and restarts of 3 containers. Details, test results and the
benchmark baseline are in `adr-003-kv-cluster.md`. Changes from the plan:

- **`internal/kv/daemon`** (not in the plan) wires listeners, gRPC servers,
  transport, node and services. `cmd/kv-node` and the full-stack tests both
  use it, so they cannot drift apart.
- **No `advertise-*` flags:** a node's peer URL comes from `-initial-cluster`
  or `kvctl member add`, and members carry no client URL. Clients are
  configured with endpoints.
- **`kvctl range` became `kvctl get -prefix`.**
- **No raft log entry cache** (Step 3): the benchmarks do not show a need
  yet.
- **`buf breaking`** runs in `make ci` once `api/kv` has been committed;
  until then there is nothing to compare against.
- **The test harness can put nodes on disk** (`kvtest.Options.DiskDir`) for
  the benchmarks; crash tests stay on the in-memory filesystem.
- **Lint config:**
  - `wrapcheck` ignores gRPC status errors, which handlers must return
    unwrapped.
  - `exhaustive` treats a `default` case as exhaustive, so switches over
    gRPC's codes stay readable.
  - The `kvchaos` build tag is linted.
- **Found by tests and fixed:**
  - The data directory was not fsynced, so a crash right after the first
    start lost the whole database.
  - A harness bug faked a raft log loss.
  - One flaky test raced the followers.

  See the ADR for details.

