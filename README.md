# Distributed KV Store

[![CI](https://github.com/96528025/distributed-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/96528025/distributed-kv/actions/workflows/ci.yml)

A distributed key-value store written from scratch in Python, evolving from a simple
3-node replicated store to per-shard Raft consensus groups — the architecture used by
CockroachDB and TiKV. The current version, `node_raft_sharded.py`, runs an independent
Raft group per shard, with log snapshot compaction, two-phase commit transactions,
quorum-validated leader reads, batch writes, and an optional write-ahead log backend.

The core v5 consensus, persistence, routing, read-quorum, transaction and observability
paths are covered by 115 automated checks that run in CI on Python 3.12 and 3.14, against
real 3-node clusters killed with `SIGKILL` and `SIGSTOP`. The deployment history, benchmark
figures and load-test numbers further down are recorded results, not test-verified claims.

The v1 store and its chat layer were deployed across three AWS EC2 regions (Virginia,
Oregon, Ireland). v5 supports the same cross-machine `IP:PORT` setup and is verified
locally as a 3-node cluster.

## 90-second engineering tour

If you are reviewing the project quickly, these are the highest-signal entry points:

| Question | Evidence |
|---|---|
| Does it run a real distributed protocol? | `test_raft_sharded.py` starts three OS processes, elects per-shard leaders, injects failures, and exercises replication, snapshots, transactions and recovery. |
| Are safety claims tested rather than assumed? | `test_raft_correctness.py` maps regressions to named Raft invariants; `docs/RAFT_CORRECTNESS.md` separates fixed, partial and pending cases. |
| Can it reject a stale read? | `test_read_quorum.py` pauses the old leader with `SIGSTOP` and verifies that it refuses to serve without a majority. |
| Does committed state survive process crashes? | `test_wal.py` covers framed checksummed WAL replay, atomic checkpoints, torn tails, corruption and two full-cluster `SIGKILL` cycles. |
| Are performance claims reproducible? | `benchmark_raft_sharded.py` and `benchmark_storage.py` preserve raw CSV/JSON results and document environment, variance and bottlenecks. |
| Are design trade-offs explicit? | [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) traces request paths and records failure semantics, persistence boundaries, scaling constraints and next decisions. |

The shortest useful verification is:

```bash
python3 test_raft_correctness.py
python3 test_read_quorum.py
python3 test_wal.py
```

Those suites target the project's hardest claims. The complete six-suite CI gate is
listed below.

---

## Verified: 115 automated checks across 6 suites, all run in CI

The badge above is green only when all 115 pass. There are no external dependencies; the
suites start real clusters and fail them deliberately.

| Suite | Checks | What it covers |
|---|---|---|
| `test_raft_sharded.py` | 56 | Election, replication, snapshot compaction and recovery, `install_snapshot`, 2PC, leader-routed reads, batch writes |
| `test_raft_correctness.py` | 24 | Raft safety invariants, each mapped to a named case in `docs/RAFT_CORRECTNESS.md` |
| `test_wal.py` | 17 | WAL framing and checksum replay, atomic checkpoint, two `SIGKILL` rounds on a live cluster |
| `test_metrics.py` | 9 | Metrics primitives, bounded labels, HTTP/Raft instrumentation, live Prometheus endpoint |
| `test_txn_routing.py` | 5 | Leader hint, unreachable fallback, lock conflict, repeated `txn_id`, phase-2 participants |
| `test_read_quorum.py` | 4 | Quorum-validated leader read; an isolated old leader must refuse the stale read |

The largest suite reproduces in one command, starting a real 3-node cluster on
`localhost:5001-5003`:

```console
$ python3 test_raft_sharded.py
  ✅ PASS  cluster started and elected leaders
  ✅ PASS  write hello=world
  ✅ PASS  data readable after forwarding
  ✅ PASS  log truncated (max log_length=20 < 60)
  ✅ PASS  k60 still readable after the snapshot
  ✅ PASS  cluster still writable after restart
  ✅ PASS  manual prepare locked alice
  ✅ PASS  the conflicting transaction aborted
  ✅ PASS  bob updated to after_timeout
  ...
───────────────────────────────────────────────────────
  🎉 all passed: 56/56
───────────────────────────────────────────────────────
```

The read-quorum suite adds three logic tests plus a real three-process failure-injection
test:

```console
$ python3 -m unittest test_read_quorum
Ran 4 tests in 5.527s

OK
```

It verifies that an isolated old leader rejects a read without quorum and converges to the
majority's committed value once communication resumes. That closes the stale-old-leader
path; it is not a complete Raft ReadIndex proof.

The transaction-routing suite covers leader changes during prepare:

```console
$ python3 -m unittest test_txn_routing
Ran 5 tests in 0.025s

OK
```

Prepare follows `not_leader` hints, or tries other known nodes when the cached leader is
unreachable, always reusing the same `txn_id`. Lock conflicts stop routing retries, and
commit/abort target the node that actually handled prepare.

---

## Architecture

Each shard runs its own Raft consensus group, so three shards may have three different
leaders at once:

```
shard 0: Raft group -> leader may be on 5001, 5002 or 5003
shard 1: Raft group -> leader may be on a different node
shard 2: Raft group -> leader may be on a different node

Every node stores the full data set. Writes route to the shard's leader; reads route to
the leader and confirm a majority before answering from local state.
```

### Features

- **Replication** — write to any node; the write routes to the shard leader and replicates.
- **Raft leader election** per shard (v5); v1 used a simple lowest-port rule.
- **Log snapshot compaction** — the log truncates past a threshold, and a restart restores
  from the local snapshot file.
- **Follower catch-up** through `install_snapshot` when a follower falls too far behind.
- **Automatic forwarding** — a non-leader forwards reads and writes to the shard leader.
- **Quorum-validated leader reads** reject an isolated old leader. This is not yet a
  complete ReadIndex implementation.
- **Multi-key transactions via 2PC**, coordinating prepare and commit across shards on the
  failure-free path. Crash recovery is incomplete; see Known Limitations.
- **Prepare follows leader changes** and records the participant that actually handled it,
  so commit and abort reach the right node.
- **Batch writes** — concurrent requests merge into one Raft round.
- **Optional append-only WAL** with atomic checkpoints; the legacy JSON backend stays the
  default.
- **Disk persistence** — committed state survives restart.

---

## Observability

Each v5 node exposes dependency-free Prometheus text metrics on `GET /metrics`. Phase A
provides thread-safe counter, gauge and histogram primitives, bounded HTTP route labels,
request totals and latency, and the scrape endpoint. Phase B connects the distributed-system
paths: election attempts, leader transitions, read-quorum results, replication-round latency
and outcomes, snapshot create and install operations, transaction coordinator responses, and
scrape-time Raft state gauges.

```bash
curl http://localhost:5001/metrics
python3 test_metrics.py   # 9 checks, including a real HTTP endpoint
```

Metric labels deliberately exclude keys, values, request IDs and transaction IDs. The
transaction outcome `reported_ok` describes the client-visible response; it does not claim
the coordinator is failure-safe atomic 2PC. Counters are process-local and reset on restart.
Metric definitions and suggested first alerts are in
[`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md).

---

## Persistence: WAL + checkpoint

The legacy JSON full-store rewrite remains the default for backward compatibility. The WAL
backend is explicit opt-in with `--backend=wal`.

Only operations that reached a Raft majority and are being applied to the state machine
enter the storage WAL: committed sets, deletes, batch entries, transaction commits, and
follower applies. Installed snapshot state is published through an atomic storage checkpoint
instead. Uncommitted in-memory or Raft-log entries are never replayed as state-machine data.

WAL records use a framed format:

```text
MAGIC(4) | payload length(4) | versioned JSON payload | crc32(4)
payload = shard id + absolute applied index + term + op + key + value
```

Recovery loads the latest valid checkpoint, then replays only records whose per-shard index
is newer than the checkpoint's `applied` index. Replay is idempotent. A partial final frame
is discarded and the bad tail truncated before future appends; checksum, frame alignment or
checkpoint corruption raises an explicit `StorageCorruptionError`.

Checkpoint publication follows a crash-safe order:

1. Write the full state plus per-shard applied indexes to a temporary file.
2. `flush()` and `fsync()` that file.
3. Publish it with atomic `os.replace()`, and fsync the directory where supported.
4. Only after publication succeeds, truncate and rotate the WAL.

The Raft snapshot and the storage checkpoint stay separate mechanisms:

| Mechanism | Responsibility | State-machine authority with WAL enabled |
|---|---|---|
| Raft snapshot | Raft log compaction and follower catch-up metadata | Does not overwrite newer storage-WAL values during startup |
| Storage checkpoint + WAL | Single-node committed state-machine crash recovery | Restores the store plus per-shard applied indexes |

By default each committed WAL batch is flushed to the OS page cache, which is what the
`SIGKILL` tests exercise. Use `--fsync` (or `KV_FSYNC=1`) to fsync per commit for stronger
power-loss durability; checkpoints are always fsynced.

Storage microbenchmark methodology and preserved raw results are in
[`benchmarks/storage_benchmark.md`](benchmarks/storage_benchmark.md).

---

## How to run

### The v5 cluster

```bash
# Start a 3-node cluster, one Raft group per shard
python3 node_raft_sharded.py 5001 5002 5003 &
python3 node_raft_sharded.py 5002 5001 5003 &
python3 node_raft_sharded.py 5003 5001 5002 &
sleep 5

curl "http://localhost:5001/set" -d '{"key":"hello","value":"world"}'
curl "http://localhost:5002/get?key=hello"     # forwarded to the shard leader
curl "http://localhost:5001/health"            # per-shard Raft state
curl "http://localhost:5001/metrics"           # Prometheus text metrics
```

Backend and durability options:

```bash
# Default: legacy JSON backend
python3 node_raft_sharded.py 5001 5002 5003

# WAL backend
python3 node_raft_sharded.py 5001 5002 5003 --backend=wal

# Optional configuration (CLI overrides environment)
--data-dir=PATH        # KV_DATA_DIR
--fsync                # KV_FSYNC=1
--rotate-records=1000  # KV_ROTATE_RECORDS
```

### The test suites

Exactly what CI runs, 115 checks in about 55 seconds:

```bash
python3 test_metrics.py            #  9
python3 test_raft_sharded.py       # 56
python3 test_raft_correctness.py   # 24
python3 test_txn_routing.py        #  5
python3 test_read_quorum.py        #  4
python3 test_wal.py                # 17

python3 benchmark_storage.py --quick --no-save   # storage benchmark smoke test
```

### The chat demo (v1)

An earlier demonstration built on the v1 store, kept because it is what ran on AWS. Its
console output is still in Chinese.

```bash
bash start.sh                  # 3 v1 KV nodes on 5001-5003
python3 chat_server.py 9001 &  # stateless WebSocket servers
python3 chat_server.py 9002 &
python3 chat_server.py 9003 &
python3 chat_client.py         # connect as a user
python3 load_test.py           # load test
```

For the AWS layout, each EC2 instance runs one `node.py` and one `chat_server.py`, with
security-group ports 22, 5001-5003 and 9001-9003 open.

---

## Project structure

### KV node versions

Five versions built in order, each fixing the core defect of the one before it:

| File | Version | What it does |
|---|---|---|
| `node.py` | v1 | The simplest version. The leader is the live node with the lowest port, which is not consensus. Only the leader accepts writes and syncs them to the others. Supports string and list types and a split-brain demo (`/isolate`, `/heal`). **The chat system runs on this.** |
| `node_sharded.py` | v2 | Adds modulo sharding: `MD5(key) % 3` decides which node owns a key, so every node owns its own keys and writes go in parallel. Defect: when a node dies, the keys it owns become unreadable. |
| `node_raft.py` | v3 | Implements real Raft consensus — randomized election timeouts, log replication, heartbeats and terms. But there is a single global Raft group and no sharding, so writes are still bottlenecked on one leader. |
| `node_replicated.py` | v4 | Sharding plus full replicas: each shard has a primary, every node stores all data, and reads work from any node. Defect: the primary is chosen by a simple liveness check rather than Raft, so writes not yet synced when it dies are lost. |
| `node_raft_sharded.py` | **v5** | An independent Raft group per shard, with log snapshot compaction, 2PC transactions, quorum-validated leader reads, batch writes, `/delete`, and an optional WAL backend. The most complete version. |

### Chat system

| File | What it does |
|---|---|
| `chat_server.py` | Stateless WebSocket chat server. Messages are stored in the v1 KV through `/lpush`; a new client is sent the last 50 through `/lrange`. Several run in parallel, and losing one does not affect the others. |
| `chat_client.py` | Chat client. Two concurrent async tasks (send and receive) detect a dropped connection immediately and reconnect to another chat server. |
| `load_test.py` | Load test simulating up to 1000 concurrent users, reporting success rate, throughput and average latency. |

### Tools and documentation

| File | What it does |
|---|---|
| `client.py` | Interactive CLI for the v1 store (`get`, `set`, `lpush`, `lrange`, ...). |
| `metrics.py` | Thread-safe counter, gauge and histogram plus Prometheus text export, standard library only, with a fixed label schema. |
| `storage.py` | `StorageEngine` abstraction, legacy JSON backend, append-only WAL, checksum replay and atomic checkpoint. |
| `benchmark_storage.py` | Storage microbenchmark comparing full JSON rewrite against WAL append; results in `benchmarks/storage_benchmark.md`. |
| `docs/RAFT_CORRECTNESS.md` | Raft safety properties checked against the code, with a status table covering every case: fixed, partial and pending. |
| `docs/LESSON_01_READ_QUORUM.md` | Leader-only reads versus quorum-validated leader reads: the failure scenarios and where ReadIndex begins. |
| `docs/LESSON_02_TXN_LEADER_CHANGES.md` | Where prepare can safely rediscover a leader, and why phase 2 still needs durable recovery. |
| `docs/OBSERVABILITY.md` | Phase A/B boundary, the full metric contract, cardinality notes and suggested alerts. |
| `docs/ARCHITECTURE.md` | Request flows, failure semantics, persistence boundaries, scaling model and design trade-offs. |
| `docs/BUILD_LOG.md` | Day-by-day development record (Chinese). |
| `start.sh` / `start_chat.sh` | Start 3 v1 KV nodes (5001-5003) / 3 chat servers (9001-9003). |

---

## API endpoints

### v5: `node_raft_sharded.py`

| Method | Path | Description |
|---|---|---|
| GET | `/get?key=<k>` | Route to the shard leader and confirm quorum before the local read. Not yet full ReadIndex. |
| GET | `/all` | Dump all data |
| GET | `/health` | Per-shard Raft state (role, term, leader, log_length, ...) |
| GET | `/metrics` | Prometheus text metrics for HTTP and distributed-system paths |
| POST | `/set` | Write a key-value pair (batched, Raft-replicated) |
| POST | `/delete` | Delete a key (Raft-replicated) |
| POST | `/txn` | 2PC coordinator with prepare-time participant leader discovery |
| POST | `/txn_prepare` | Lock keys and stage intent — shard leader, internal |
| POST | `/txn_commit` | Commit the transaction through Raft — shard leader, internal |
| POST | `/txn_abort` | Abort the transaction and release locks — internal |
| POST | `/install_snapshot` | Follower requests a full snapshot from the leader — internal |
| POST | `/append_entries` | Raft log replication RPC — internal |
| POST | `/vote` | Raft vote request RPC — internal |

### v1: `node.py` and the chat system

| Method | Path | Description |
|---|---|---|
| GET | `/get?key=<k>` | Read a string value |
| GET | `/lrange?key=<k>&start=0&end=49` | Read a list range |
| GET | `/all` | Dump all data |
| GET | `/health` | Health check |
| GET | `/snapshot` | Full data dump for peer recovery |
| GET | `/leader` | Current leader info |
| GET | `/isolate` / `/heal` | Enter or leave isolated mode (split-brain demo) |
| POST | `/set` | Write a string value |
| POST | `/lpush` | Append to a list |
| POST | `/internal` | Receive replicated data from peers |

---

## Load test results

> These numbers are for the **v1 KV store plus the chat layer**, not v5. v5 adds Raft
> consensus and batch writes, and has different throughput characteristics.

| Users | Success rate | Throughput | Average latency |
|---|---|---|---|
| 50 | 100% | 41.5 msg/s | 0.04 ms |
| 200 | 100% | 163 msg/s | 0.04 ms |
| 1000 | 25% | 200 msg/s | 0.03 ms |

The bottleneck is connections per chat server (around 80 concurrent), not latency. Scaling
is linear: adding a server adds capacity. KV write throughput is limited by the single
leader, which is what sharding addresses.

---

## Build log

The day-by-day development record, from the first single-node store through to per-shard
Raft groups, is in [docs/BUILD_LOG.md](docs/BUILD_LOG.md). It is kept as a historical
record and is written in Chinese.

---

## Known limitations

These apply to the current version, `node_raft_sharded.py`. Earlier versions have
additional limitations, described in the build log.

- **Fixed cluster size** — adding or removing nodes requires a restart; there is no dynamic
  membership change.
- **Modulo sharding, not consistent hashing** — keys are placed by `MD5(key) % NUM_SHARDS`,
  and `NUM_SHARDS` derives from the node count. Changing the cluster size therefore remaps
  nearly every key, so this design does *not* have the minimal-remapping property of
  consistent hashing: no hash ring, no virtual nodes.
- **The read barrier is not full ReadIndex** — it rejects an isolated old leader, but
  complete linearizability still depends on the remaining Raft election, log, commit/apply
  and durability invariants.
- **2PC coordinator crash** — if the coordinator dies between prepare and commit, the
  affected shards stay locked until the 10-second timeout expires.
- **2PC phase-2 recovery is not durable** — routing commit/abort to the in-memory prepare
  participant does not recover a decision after a participant or coordinator crash.
- **`txn_commit` is not batched** — transaction commits still use one Raft round per key;
  only ordinary `/set` and `/delete` benefit from batching.
- **WAL is opt-in and fsync-per-commit is off by default** — default JSON behaviour is
  preserved. The WAL `flush()` covers the process crashes under test; power-loss durability
  requires `--fsync`.
- **Checkpoints pause local writes briefly** — rotation writes one O(N) full-state
  checkpoint while holding `store_lock`. The threshold trades write amplification against
  replay length.
- **Storage durability does not complete Raft durability** — `currentTerm` and `votedFor`
  are persisted, but full Raft log recovery is unfinished. See
  `docs/RAFT_CORRECTNESS.md` for the complete fixed, partial and pending status.
- **Metrics are process-local** — counters reset on restart. No Prometheus server, retention,
  dashboard, alert manager, tracing or authentication is bundled.
- **No `/keys` endpoint** — there is no way to list all existing keys.
