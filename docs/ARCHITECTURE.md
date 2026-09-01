# Architecture and Engineering Decisions

This document explains the current v5 design (`node_raft_sharded.py`) as an engineering
system: what guarantees each request path attempts to provide, which failures it handles,
where those guarantees stop, and why the present trade-offs were chosen.

It is intentionally stricter than a feature list. A distributed system is defined as much
by the failures it refuses to hide as by its successful requests.

## Scope and non-goals

The project is a learning-oriented, dependency-free implementation of a sharded replicated
key-value store. Its purpose is to make consensus, persistence, transactions, failure
injection and observability inspectable in one repository.

Current scope:

- A fixed three-node cluster with one Raft group per shard.
- String `set`, `get` and `delete` operations.
- Cross-shard multi-key writes through a deliberately limited 2PC coordinator.
- Process-crash recovery through an optional WAL and atomic checkpoint backend.
- Prometheus text metrics without an external client library.

Non-goals today:

- Dynamic membership, shard migration or automatic leader balancing.
- A production-compatible wire protocol, authentication or multi-tenancy.
- Failure-safe distributed transactions.
- A claim of complete Raft correctness while C3-C8 and C10 remain open in
  [`RAFT_CORRECTNESS.md`](RAFT_CORRECTNESS.md).

## System model

Every process hosts every shard, and every shard owns an independent `ShardRaft` state
machine. A deterministic `MD5(key) % NUM_SHARDS` mapping selects the shard. Each shard may
elect a different leader, allowing independent replication rounds when leaders are spread
across machines.

```text
client
  |
  | HTTP/JSON to any node
  v
request router -- hash(key) --> shard Raft group
  |                               |
  | follower                      | leader
  +---- forward to leader --------+
                                  |
                                  +--> replicate to a majority
                                  +--> persist committed state
                                  +--> apply to local state machine
                                  +--> respond
```

All nodes currently keep the complete logical key space. Sharding parallelizes leadership
and consensus work; it does not partition storage capacity.

## Write path

An ordinary `set` or `delete` follows this path:

1. Any node accepts the HTTP request and hashes the key to a shard.
2. A follower forwards the request to its cached leader for that shard.
3. The leader's per-shard batch queue groups up to 20 operations for at most 5 ms.
4. Under the shard lock, the leader appends entries using its current term.
5. Replication RPCs run concurrently to the other nodes.
6. The request may succeed only after a majority acknowledges the round.
7. Committed entries are applied in order to the in-memory store and passed to the selected
   storage engine while holding the store lock.
8. Waiting client requests receive the result of their shared replication round.

The batch window is a deliberate throughput/latency control. It amortizes consensus and
storage work under concurrency, while adding queueing latency and increasing tail latency.
The benchmark records this trade-off instead of presenting one peak throughput number.

### Client outcome boundary

A timeout does not prove that a write was aborted. It means the outcome is unknown: a
majority may have accepted the entry after the client stopped waiting. Safe automatic
retries therefore require a durable request ID and deduplication table, which are not yet
implemented. The current API must not be treated as exactly-once.

## Read path

Reads route to the shard leader. Before returning its local value, the leader probes a
majority in its current term. If it cannot confirm a majority, it refuses the read. This
prevents the demonstrated stale-old-leader failure in which a partitioned former leader
continues serving old state.

This barrier is narrower than the Raft ReadIndex protocol. It does not independently prove
that the leader has applied every committed entry before reading. Complete linearizability
still depends on the log replication, commit and apply invariants tracked in
`RAFT_CORRECTNESS.md`.

## Transaction path

The `/txn` endpoint groups keys by shard and runs two phases:

1. **Prepare:** find the current leader for each shard, acquire in-memory key locks and
   retain the staged operations under one transaction ID.
2. **Commit or abort:** contact the exact participant that accepted prepare. Commit writes
   each staged operation through that shard's Raft path; abort releases the locks.

Prepare safely follows a `not_leader` hint or tries another known node without changing the
transaction ID. A deterministic lock conflict is a business result, not a routing failure,
and is not retried on arbitrary nodes.

This is not failure-safe atomic 2PC. Coordinator decisions and prepared intents are not
durable. A coordinator crash can leave locks until the ten-second lease expires, and a
participant crash can lose phase state. Metrics consequently use `reported_ok`, not a label
that overstates atomicity.

## Persistence boundaries

The implementation has three different durable artifacts. They solve different problems
and must not be conflated.

| Artifact | Durable state | Purpose | Current boundary |
|---|---|---|---|
| Raft hard state | `currentTerm`, `votedFor` per shard | Prevent double voting across restarts | Full Raft log durability is still open (C3). |
| Raft snapshot | Compacted log boundary and state used for follower catch-up | Bound in-memory log growth | Snapshot isolation and apply ordering still have tracked follow-ups. |
| Storage WAL + checkpoint | Committed key/value state and per-shard applied index | Recover the local state machine after a crash | This does not make the Raft log durable. |

The WAL uses length-prefixed, checksummed frames. Recovery ignores and truncates only a
partial final frame; a bad checksum, impossible length or broken frame boundary fails
closed. Checkpoint publication is ordered as `write temp -> fsync -> atomic replace ->
fsync directory -> truncate WAL`, so a crash on either side of publication leaves at least
one replayable source of truth.

## Failure semantics

| Failure | Expected behavior | Verification |
|---|---|---|
| One follower process stops | A leader can still commit with the remaining majority. | Three-process integration suite. |
| Current leader stops | The remaining nodes elect a replacement after the randomized timeout. | Integration and read-quorum suites. |
| Old leader is isolated | It refuses reads when it cannot confirm a current-term majority. | `SIGSTOP` regression in `test_read_quorum.py`. |
| Process dies after committed state is written | WAL replay reconstructs committed state. | Two complete `SIGKILL` cycles in `test_wal.py`. |
| WAL ends with a torn frame | Replay preserves the valid prefix and truncates the invalid tail. | Storage corruption tests. |
| WAL/checkpoint has interior corruption | Node fails closed instead of silently skipping data. | Checksum and framing tests. |
| Coordinator dies during 2PC | Atomic completion is not guaranteed; locks expire. | Documented limitation, not claimed as solved. |
| All nodes lose volatile Raft logs | Leader completeness is not guaranteed yet. | Open correctness case C3. |

## Concurrency and lock ordering

The server uses one thread per HTTP request plus background election, heartbeat, batch and
transaction-cleanup threads. The key shared locks are:

- One lock per shard for Raft state.
- One global store lock for state-machine data.
- One condition per shard for the batch queue.
- Internal locks inside the metrics registry and storage engine.

Disk and network I/O are kept outside shard locks where possible. The committed-state
persistence path uses the order `store_lock -> storage engine lock`. Code that needs a
snapshot copies store state without retaining a shard lock, then revalidates the shard
boundary before truncating. These rules limit lock hold time and avoid a shard/store lock
cycle.

## Scaling model and measured bottlenecks

Independent shard leaders create the possibility of parallel writes, but the local
benchmark deliberately reports that three shards did not outperform one shard on one
laptop. Three server processes and the client compete for the same CPU, the Python GIL
serializes work inside each process, leaders may concentrate on one machine, and spreading
requests reduces batch depth.

The current bottlenecks are therefore implementation boundaries, not evidence that Raft
itself is the limiting factor:

- HTTP/1.0 creates a connection per request.
- JSON encoding and Python threads add CPU and scheduling overhead.
- The legacy backend rewrites the full store on every commit.
- WAL checkpoints briefly hold the store lock while writing O(N) state.
- Every node stores every key, so storage capacity does not scale with shard count.

A meaningful horizontal-scaling experiment would deploy nodes on separate machines,
control leader placement, keep the replica factor constant, record CPU/disk/network
saturation and compare one hot shard against a balanced multi-shard workload.

## Decision record

### Standard library HTTP before gRPC

**Decision:** use `http.server` and `urllib` first.

**Reason:** zero dependencies keep the consensus and failure tests easy to reproduce and
inspect. The cost is per-request connections, weak schemas and performance far below a
production transport. gRPC/HTTP2 is a logical transport experiment only after correctness
boundaries are explicit.

### Modulo hashing before consistent hashing

**Decision:** use deterministic modulo hashing.

**Reason:** it makes shard ownership obvious and stable for a fixed topology. It does not
support incremental resizing; changing the shard count remaps almost every key. Dynamic
membership requires an explicit migration protocol, not a new hash function alone.

### Correctness-first hard-state persistence

**Decision:** atomically rewrite all shards' term/vote state before sending dependent RPCs.

**Reason:** for three shards, the simple whole-file strategy makes the election invariant
auditable. It creates O(N) write amplification per mutation and should become a per-shard
journal before large shard counts.

### Explicitly limited 2PC

**Decision:** expose the failure-free transaction path while naming its crash boundary.

**Reason:** it demonstrates cross-shard coordination and leader rediscovery without claiming
the durability of a transaction manager that the implementation does not have. The next
step is durable intents and coordinator decisions, followed by recovery tests.

## Prioritized engineering roadmap

The order is safety before speed:

1. **Durable Raft log and recovery (C3).** Persist log entries and snapshot metadata before
   acknowledging RPCs; add restart and power-loss-oriented regressions.
2. **Standard `nextIndex`/`matchIndex` replication (C4) and commit rule (C5).** Stop shipping
   whole logs, repair divergent suffixes and commit only entries proven safe by the
   current-term rule.
3. **Ordered apply and shard-scoped snapshots (C7-C8).** Make the state-machine boundary
   precise before expanding the read guarantee.
4. **Complete ReadIndex semantics.** Add an apply barrier and history-based linearizability
   testing after the underlying Raft invariants hold.
5. **Durable request deduplication and transaction recovery.** Define retry semantics and
   recover coordinator/participant decisions.
6. **Transport and deployment experiment.** Add persistent connections or gRPC, multi-host
   benchmarks and controlled leader placement only after the safety work above.

This roadmap is intentionally test-shaped: every change should begin with a reproducible
failure, state the invariant it restores, and land with a regression that fails on the old
implementation.
