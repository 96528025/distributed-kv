"""Sharded replicated key-value node with selected Raft mechanisms.

Keys map to shards through ``MD5(key) % NUM_SHARDS``. Each shard runs an
independent Raft group and may elect a different leader, while every node keeps
a full replica of the logical key space.

The implementation includes snapshot-based log compaction, quorum-validated
leader reads, batched writes, delete operations, an optional WAL/checkpoint
state-machine backend, and a deliberately limited two-phase commit path. The
2PC coordinator and prepared intents are not durable, so crash-safe atomicity is
outside the current guarantee.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver
import json
import sys
import urllib.request
import threading
import hashlib
import time
import random
import os

import metrics as metrics_mod
import storage as storage_mod

# Startup arguments
# Local:       python3 node_raft_sharded.py 5001 5002 5003
# Cross-host:  python3 node_raft_sharded.py 5001 54.x.x.x:5002 54.x.x.x:5003
# Persistence options:
#   --backend=json|wal    (KV_BACKEND; default: json)
#   --data-dir=PATH       (KV_DATA_DIR; default: current directory)
#   --fsync               (KV_FSYNC=1; fsync after each committed batch)
#   --rotate-records=N    (KV_ROTATE_RECORDS; default: 1000 WAL records)
# Network option:
#   --host=HOST           (KV_HOST; default: 127.0.0.1)
_flags = {}
_positional = []
for arg in sys.argv[1:]:
    if arg.startswith("--"):
        if "=" in arg:
            key, value = arg[2:].split("=", 1)
        else:
            key, value = arg[2:], "1"
        _flags[key] = value
    else:
        _positional.append(arg)

MY_PORT  = int(_positional[0])
PEER_MAP = {}   # {port: host}, used to construct cross-host URLs
for arg in _positional[1:]:
    if ":" in arg:
        host, port = arg.rsplit(":", 1)
        PEER_MAP[int(port)] = host
    else:
        PEER_MAP[int(arg)] = "localhost"
PEER_PORTS = list(PEER_MAP.keys())
ALL_PORTS  = sorted([MY_PORT] + PEER_PORTS)

def _cfg(flag, env, default):
    if flag in _flags:
        return _flags[flag]
    return os.environ.get(env, default)


DATA_DIR = _cfg("data-dir", "KV_DATA_DIR", ".")
BACKEND = _cfg("backend", "KV_BACKEND", "json")
BIND_HOST = _cfg("host", "KV_HOST", "127.0.0.1")
FSYNC = str(_cfg("fsync", "KV_FSYNC", "0")).lower() in ("1", "true", "yes")
ROTATE_RECORDS = int(_cfg("rotate-records", "KV_ROTATE_RECORDS", "1000"))
DISK_FILE = os.path.join(DATA_DIR, f"data_raft_sharded_{MY_PORT}.json")

storage = storage_mod.create_storage_engine(storage_mod.StorageConfig(
    backend=BACKEND,
    data_dir=DATA_DIR,
    port=MY_PORT,
    fsync=FSYNC,
    rotate_records=ROTATE_RECORDS,
))

# Test mode
# RAFT_TEST_MODE=1 enables introspection and timer overrides. It is disabled by
# default because /debug/raft exposes the complete internal Raft state.
TEST_MODE = os.environ.get("RAFT_TEST_MODE") == "1"

# The shard count defaults to the node count, a documented limitation. Tests
# may override it to reason about a single Raft group without changing the
# default placement model. Every node must use the same value.
NUM_SHARDS = int(os.environ.get("RAFT_NUM_SHARDS") or len(ALL_PORTS))

FOLLOWER  = "follower"
CANDIDATE = "candidate"
LEADER    = "leader"

HEARTBEAT_INTERVAL = 0.5

# Timer overrides are restricted to test mode so tests can control election
# order without exposing unsafe production configuration.
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0
if TEST_MODE:
    ELECTION_TIMEOUT_MIN = float(os.environ.get("RAFT_ELECTION_TIMEOUT_MIN", ELECTION_TIMEOUT_MIN))
    ELECTION_TIMEOUT_MAX = float(os.environ.get("RAFT_ELECTION_TIMEOUT_MAX", ELECTION_TIMEOUT_MAX))

def new_election_timeout():
    return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
SNAPSHOT_THRESHOLD = 20   # Compact after more than 20 log entries.
BATCH_MAX_SIZE     = 20   # Maximum operations merged into one batch.
BATCH_TIMEOUT      = 0.005  # Maximum 5 ms batching window.
READ_QUORUM_TIMEOUT = 0.5   # Maximum wait for a leader-read quorum.


# In-process, bounded-cardinality Prometheus metrics
metrics_registry = metrics_mod.Registry()

HTTP_REQUESTS = metrics_registry.counter(
    "distributed_kv_http_requests_total",
    "HTTP responses produced by this node.",
    ("method", "route", "status"),
)
HTTP_REQUEST_DURATION = metrics_registry.histogram(
    "distributed_kv_http_request_duration_seconds",
    "End-to-end HTTP handler duration in seconds.",
    (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
    ("method", "route"),
)
RAFT_ELECTIONS = metrics_registry.counter(
    "distributed_kv_raft_elections_total",
    "Election attempts started by this node.",
    ("shard",),
)
RAFT_LEADER_TRANSITIONS = metrics_registry.counter(
    "distributed_kv_raft_leader_transitions_total",
    "Times this node became Leader.",
    ("shard",),
)
READ_QUORUM_CHECKS = metrics_registry.counter(
    "distributed_kv_read_quorum_checks_total",
    "Leader read-barrier checks grouped by result.",
    ("shard", "outcome"),
)
REPLICATION_ROUND_DURATION = metrics_registry.histogram(
    "distributed_kv_raft_replication_round_duration_seconds",
    "Time spent waiting for a replication majority.",
    (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0),
    ("shard", "outcome"),
)
SNAPSHOT_OPERATIONS = metrics_registry.counter(
    "distributed_kv_snapshot_operations_total",
    "Successful local snapshot creates and follower installs.",
    ("shard", "operation"),
)
TRANSACTION_RESULTS = metrics_registry.counter(
    "distributed_kv_transaction_coordinator_results_total",
    "Client-visible transaction coordinator results; reported_ok is not proof of atomic commit.",
    ("outcome",),
)

NODE_INFO = metrics_registry.gauge(
    "distributed_kv_node_info",
    "Static process identity and configured storage backend.",
    ("port", "backend"),
)
RAFT_TERM = metrics_registry.gauge(
    "distributed_kv_raft_term",
    "Current in-memory term by shard.",
    ("shard",),
)
RAFT_COMMIT_INDEX = metrics_registry.gauge(
    "distributed_kv_raft_commit_index",
    "Current in-memory commit index by shard.",
    ("shard",),
)
RAFT_LOG_ENTRIES = metrics_registry.gauge(
    "distributed_kv_raft_log_entries",
    "Number of entries in the current in-memory log window.",
    ("shard",),
)
RAFT_ROLE = metrics_registry.gauge(
    "distributed_kv_raft_role",
    "One-hot current role by shard.",
    ("shard", "role"),
)
PENDING_TRANSACTIONS = metrics_registry.gauge(
    "distributed_kv_pending_transactions",
    "Prepared in-memory transactions by shard.",
    ("shard",),
)

NODE_INFO.set(1, port=MY_PORT, backend=BACKEND)


# Global KV state machine shared by all shards
store      = {}
store_lock = threading.Lock()


# Helpers
def peer_host(port):
    """Return the configured host for a port, or localhost for this node."""
    if port == MY_PORT:
        return "localhost"
    return PEER_MAP.get(port, "localhost")

def send_rpc(port, path, data, timeout=0.5):
    """Send an HTTP RPC, returning ``None`` on failure."""
    try:
        url  = f"http://{peer_host(port)}:{port}{path}"
        body = json.dumps(data).encode()
        req  = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-type", "application/json")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return None

def send_get_rpc(port, path, timeout=0.5):
    """Send an HTTP GET RPC, returning ``None`` on failure."""
    try:
        url = f"http://{peer_host(port)}:{port}{path}"
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return None

def majority():
    return len(ALL_PORTS) // 2 + 1

def send_txn_prepare_with_discovery(txn_id, shard_id, ops, initial_leader):
    """Locate the current shard Leader and send phase-1 prepare.

    A stale Leader hint or an unreachable cached Leader may be retried on other known
    nodes because phase 1 has not made a global commit decision. Every attempt uses the
    same txn_id. Deterministic participant results such as ``ready`` and ``locked`` are
    returned immediately instead of being mistaken for routing failures.

    Returns ``(result, participant_port)``. This only hardens phase-1 routing; prepared
    intents and the coordinator decision are still not durable or failure-safe.
    """
    candidates = []
    if initial_leader is not None:
        candidates.append(initial_leader)
    candidates.extend(port for port in ALL_PORTS if port != initial_leader)
    attempted = set()

    while candidates:
        port = candidates.pop(0)
        if port in attempted:
            continue
        attempted.add(port)

        result = send_rpc(port, "/txn_prepare", {
            "txn_id": txn_id,
            "shard_id": shard_id,
            "ops": ops,
        })
        if result is None:
            continue
        if result.get("status") == "not_leader":
            hinted_leader = result.get("leader")
            if hinted_leader in ALL_PORTS and hinted_leader not in attempted:
                candidates.insert(0, hinted_leader)
            continue
        return result, port

    return {"status": "unreachable"}, initial_leader

def apply_entry(entry):
    """Apply one log entry while the caller holds ``store_lock``."""
    if entry.get("op") == "delete":
        store.pop(entry["key"], None)
    else:
        store[entry["key"]] = entry["value"]


# Per-shard Raft state
class ShardRaft:
    def __init__(self, shard_id):
        self.shard_id = shard_id
        self.lock     = threading.Lock()

        # Core Raft state.
        self.term           = 0
        self.voted_for      = None
        self.role           = FOLLOWER
        self.leader_id      = None
        self.votes_received = set()

        # Log; log_offset is the absolute index represented by log[0].
        self.log          = []   # [{"term": int, "key": str, "value": str}]
        self.commit_index = -1   # Absolute index; -1 means no committed entry.
        self.log_offset   = 0    # Absolute index of log[i] is i + log_offset.

        # Snapshot boundary.
        self.snapshot_index = -1  # Absolute index of the last snapshot entry.
        self.snapshot_term  = 0   # Term of the last snapshot entry.

        # Election timing.
        self.last_heartbeat   = time.time()
        self.election_timeout = new_election_timeout()

        # Volatile 2PC state.
        self.pending_txns = {}   # {txn_id: [{"key": ..., "value": ...}]}
        self.key_locks    = {}   # {key: txn_id}
        self.lock_expiry  = {}   # {txn_id: expire_time}

        # Batch queue with its own condition lock, separate from shard.lock.
        self.batch_queue = []              # [{"key", "value", "op", "event", "result"}]
        self.batch_cv    = threading.Condition()


# One Raft state object per shard.
shards = [ShardRaft(i) for i in range(NUM_SHARDS)]


def refresh_state_metrics():
    """Refresh scrape-time gauges without holding Raft and metric locks together."""
    snapshots = []
    for shard in shards:
        with shard.lock:
            snapshots.append({
                "shard": shard.shard_id,
                "term": shard.term,
                "commit_index": shard.commit_index,
                "log_entries": len(shard.log),
                "role": shard.role,
                "pending_transactions": len(shard.pending_txns),
            })

    for state in snapshots:
        shard_label = state["shard"]
        RAFT_TERM.set(state["term"], shard=shard_label)
        RAFT_COMMIT_INDEX.set(state["commit_index"], shard=shard_label)
        RAFT_LOG_ENTRIES.set(state["log_entries"], shard=shard_label)
        PENDING_TRANSACTIONS.set(
            state["pending_transactions"], shard=shard_label
        )
        for role in (FOLLOWER, CANDIDATE, LEADER):
            RAFT_ROLE.set(
                1 if state["role"] == role else 0,
                shard=shard_label,
                role=role,
            )


# State-machine persistence
# Only committed operations entering the state machine may use this path. The
# caller holds store_lock but no shard lock; lock order is store -> storage engine.
def persist_committed(records):
    storage.commit(store, records)


def snapshot_path(shard_id):
    return os.path.join(DATA_DIR, f"snapshot_{MY_PORT}_shard{shard_id}.json")


# Raft hard-state persistence
# Of the persistent state in Raft Figure 2, this file stores only currentTerm
# and votedFor. Durable Raft log recovery remains open as C3.
#
# This hard state and the storage WAL are semantically separate:
#   hard state  -- persisted before dependent RPCs to prevent duplicate votes
#   storage WAL -- persisted after commit to recover committed state
HARD_STATE_FILE    = os.path.join(DATA_DIR, f"raft_hardstate_{MY_PORT}.json")
HARD_STATE_VERSION = 1
_hard_state_lock   = threading.Lock()


def _fsync_dir(path):
    """Fsync the directory after ``os.replace`` to persist the rename."""
    fd = os.open(path or ".", os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def persist_hard_state():
    """Atomically persist ``(currentTerm, votedFor)`` for every shard.

    Callers must hold no shard lock, and persistence must finish before any RPC
    response that depends on this state. State is read under each shard lock;
    disk I/O occurs after those locks are released. ``_hard_state_lock``
    serializes complete latest-wins snapshots so an older term cannot overwrite
    a newer one.
    """
    with _hard_state_lock:
        state = {}
        for shard in shards:
            with shard.lock:
                state[str(shard.shard_id)] = {
                    "term":      shard.term,
                    "voted_for": shard.voted_for,
                }

        tmp = HARD_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            # num_shards is part of the topology identity. Changing it changes
            # the Raft group represented by a shard ID, invalidating term/vote state.
            json.dump({"version":    HARD_STATE_VERSION,
                       "num_shards": NUM_SHARDS,
                       "shards":     state}, f)
            f.flush()
            os.fsync(f.fileno())      # A granted vote must survive power loss.
        os.replace(tmp, HARD_STATE_FILE)
        _fsync_dir(os.path.dirname(os.path.abspath(HARD_STATE_FILE)))


def load_hard_state():
    """Recover ``(currentTerm, votedFor)`` during startup."""
    if not os.path.exists(HARD_STATE_FILE):
        return
    try:
        with open(HARD_STATE_FILE) as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        # Atomic publication cannot produce a partial final file. Fail closed so
        # corruption cannot erase a previously granted vote.
        raise SystemExit(
            f"⛔ Raft hard state is corrupt; refusing to start: {HARD_STATE_FILE} ({e})"
        )

    # Validate shard topology identity and fail closed.
    # Silently dropping shards after a 3 -> 1 -> 3 topology change would erase
    # durable votes and permit another vote in the same term. Without membership
    # migration semantics, the safe behavior is to refuse startup.
    persisted_n = payload.get("num_shards")
    if persisted_n != NUM_SHARDS:
        raise SystemExit(
            f"⛔ Raft shard topology does not match persisted state; refusing to start:\n"
            f"   {HARD_STATE_FILE} records num_shards={persisted_n}; "
            f"this process uses NUM_SHARDS={NUM_SHARDS}\n"
            "   Discarding any shard's term/votedFor could permit a second vote "
            "in the same term.\n"
            "   Shard migration is not implemented; clear this node's Raft state "
            "before changing the shard count."
        )

    restored = {}
    for sid_str, s in payload.get("shards", {}).items():
        sid = int(sid_str)
        if not (0 <= sid < NUM_SHARDS):
            raise SystemExit(
                f"⛔ Raft hard state contains out-of-range shard {sid} "
                f"(num_shards={NUM_SHARDS}); refusing to start: "
                f"{HARD_STATE_FILE}"
            )
        shards[sid].term      = s.get("term", 0)
        shards[sid].voted_for = s.get("voted_for")
        restored[sid]         = s.get("term", 0)
    print(f"  🗳️  restored Raft hard state (currentTerm per shard = {restored})")


def _last_log_locked(shard):
    """Return ``(lastLogTerm, lastLogIndex)`` while holding ``shard.lock``.

    Both candidate and voter use this definition. An empty compacted log falls
    back to the snapshot boundary so the election restriction remains valid.
    """
    if shard.log:
        return shard.log[-1]["term"], shard.log_offset + len(shard.log) - 1
    return shard.snapshot_term, shard.snapshot_index



def load_from_disk():
    recovered = storage.load()
    if recovered:
        store.update(recovered)
        print(f"  💾 [{BACKEND}] recovered {len(store)} entries from the storage engine")

    # Raft snapshots restore log-compaction metadata. The legacy JSON backend
    # retains its snapshot-store merge behavior. With WAL enabled, only the
    # storage checkpoint and WAL restore state-machine data, preventing an older
    # Raft snapshot from overwriting a newer WAL value.
    for shard in shards:
        fname = snapshot_path(shard.shard_id)
        if os.path.exists(fname):
            with open(fname, "r") as f:
                snap = json.load(f)
            if BACKEND == "json":
                with store_lock:
                    store.update(snap["store"])
            with shard.lock:
                shard.snapshot_index = snap["snapshot_index"]
                shard.snapshot_term  = snap["snapshot_term"]
                shard.log_offset     = snap["log_offset"]
                shard.commit_index   = snap["snapshot_index"]
            print(f"  📸 shard {shard.shard_id} recovered from snapshot (snapshot_index={snap['snapshot_index']})")

    # Storage applied indexes are state-machine recovery metadata, not Raft snapshots.
    # They prevent WAL record index reuse after restart without pretending to recover
    # Raft term/vote/log state.
    for sid, applied_index in storage.applied_indices().items():
        if 0 <= sid < len(shards):
            with shards[sid].lock:
                shards[sid].commit_index = max(shards[sid].commit_index, applied_index)
                if not shards[sid].log:
                    shards[sid].log_offset = max(
                        shards[sid].log_offset, applied_index + 1
                    )


# Shard routing
def get_shard(key):
    """Map a key by deterministic modulo hashing, not consistent hashing.

    Changing ``NUM_SHARDS`` remaps nearly every key.
    """
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_SHARDS


# Snapshot compaction
def maybe_snapshot(shard):
    """Compact an oversized committed log; call without ``shard.lock``."""
    with shard.lock:
        if len(shard.log) <= SNAPSHOT_THRESHOLD:
            return

    # Copy the store without holding shard.lock to avoid a lock cycle.
    with store_lock:
        store_copy = dict(store)

    with shard.lock:
        ci  = shard.commit_index
        lo  = shard.log_offset
        cut = ci - lo + 1   # Number of committed entries to compact.
        if cut <= 0 or cut > len(shard.log):
            return
        snap_entry = shard.log[cut - 1]

    # Write the snapshot without holding the shard lock.
    fname = snapshot_path(shard.shard_id)
    snapshot_data = {
        "snapshot_index": ci,
        "snapshot_term":  snap_entry["term"],
        "log_offset":     ci + 1,
        "store":          store_copy,
    }
    with open(fname, "w") as f:
        json.dump(snapshot_data, f)

    # Reacquire the lock and guard against a concurrent compaction.
    with shard.lock:
        if shard.log_offset != lo:
            return   # Another thread already compacted the log.
        shard.snapshot_index = ci
        shard.snapshot_term  = snap_entry["term"]
        shard.log            = shard.log[cut:]
        shard.log_offset     = ci + 1

    SNAPSHOT_OPERATIONS.inc(shard=shard.shard_id, operation="create")
    print(f"  📸 shard {shard.shard_id} snapshot saved"
          f" (snapshot_index={ci}, {len(shard.log)} log entries remaining)")


# Expired 2PC lock cleanup
def txn_cleanup_loop():
    """Release expired volatile transaction locks once per second."""
    while True:
        time.sleep(1.0)
        now = time.time()
        for shard in shards:
            with shard.lock:
                expired = [
                    txn_id for txn_id, exp in shard.lock_expiry.items()
                    if now > exp
                ]
                for txn_id in expired:
                    ops = shard.pending_txns.pop(txn_id, [])
                    for op in ops:
                        shard.key_locks.pop(op["key"], None)
                    shard.lock_expiry.pop(txn_id, None)
                    print(f"  ⏱️  shard {shard.shard_id}: transaction {txn_id} timed out; lock released")


# Batched write loop
def batch_loop(shard):
    """Collect a shard's queued writes into shared replication rounds.

    Only a leader processes a batch. Callers are notified together after the
    round commits or fails.
    """
    sid = shard.shard_id
    while True:
        # Wait up to BATCH_TIMEOUT for queued work.
        with shard.batch_cv:
            shard.batch_cv.wait_for(
                lambda: len(shard.batch_queue) > 0,
                timeout=BATCH_TIMEOUT,
            )
            if not shard.batch_queue:
                continue
            batch = shard.batch_queue[:BATCH_MAX_SIZE]
            del shard.batch_queue[:BATCH_MAX_SIZE]

        # Verify leadership and append the entire batch to the log.
        with shard.lock:
            if shard.role != LEADER:
                for item in batch:
                    item["result"][0] = (False, "not leader")
                    item["event"].set()
                continue

            t = shard.term
            base_abs = shard.log_offset + len(shard.log)
            new_entries = []
            for item in batch:
                entry = {"term": t, "op": item["op"], "key": item["key"]}
                if item["op"] == "set":
                    entry["value"] = item["value"]
                new_entries.append(entry)
                shard.log.append(entry)

            last_abs = shard.log_offset + len(shard.log) - 1   # Absolute tail index.

        print(f"\n📦 [shard {sid} batch] merged {len(batch)} writes into one Raft round")

        # Replicate to all followers concurrently.
        replication_started = time.monotonic()
        acks      = [MY_PORT]
        ack_lock  = threading.Lock()
        ack_event = threading.Event()
        if len(acks) >= majority():   # A single-node group already has a majority.
            ack_event.set()

        def replicate_to(port):
            with shard.lock:
                r_term    = shard.term
                r_entries = list(shard.log)
                r_ci      = shard.commit_index
                r_lo      = shard.log_offset
                r_snap    = shard.snapshot_term
            result = send_rpc(port, "/append_entries", {
                "shard_id":       sid,
                "term":           r_term,
                "leader_id":      MY_PORT,
                "entries":        r_entries,
                "commit_index":   r_ci,
                "log_offset":     r_lo,
                "prev_log_index": r_lo - 1,
                "prev_log_term":  r_snap,
            })
            if result and result.get("success"):
                with ack_lock:
                    acks.append(port)
                    if len(acks) >= majority():
                        ack_event.set()

        threads = [
            threading.Thread(target=replicate_to, args=(p,), daemon=True)
            for p in PEER_PORTS
        ]
        for th in threads:
            th.start()
        ack_event.wait(timeout=1.0)

        if len(acks) >= majority():
            REPLICATION_ROUND_DURATION.observe(
                time.monotonic() - replication_started,
                shard=sid,
                outcome="success",
            )
            with shard.lock:
                shard.commit_index = last_abs
            with store_lock:
                records = []
                for offset, entry in enumerate(new_entries):
                    apply_entry(entry)
                    records.append(storage_mod.WalRecord(
                        shard_id=sid,
                        index=base_abs + offset,
                        term=t,
                        op=entry["op"],
                        key=entry["key"],
                        value=entry.get("value"),
                    ))
                persist_committed(records)
            threading.Thread(target=maybe_snapshot, args=(shard,), daemon=True).start()
            print(f"  🎉 shard {sid} batch committed ({len(batch)} writes)")
            for item in batch:
                item["result"][0] = (True, None)
                item["event"].set()
        else:
            REPLICATION_ROUND_DURATION.observe(
                time.monotonic() - replication_started,
                shard=sid,
                outcome="quorum_unavailable",
            )
            err = f"majority not reached ({len(acks)}/{majority()})"
            for item in batch:
                item["result"][0] = (False, err)
                item["event"].set()


# Elections
def start_election(shard):
    """Start an election for one shard; call without ``shard.lock``."""
    with shard.lock:
        shard.term          += 1
        shard.role           = CANDIDATE
        shard.voted_for      = MY_PORT
        shard.votes_received = {MY_PORT}
        term = shard.term
        last_log_term, last_log_index = _last_log_locked(shard)
        sid  = shard.shard_id

    # Persist before requesting votes. Otherwise a crash could erase this
    # candidacy and allow the node to vote for another candidate in the same term.
    persist_hard_state()

    RAFT_ELECTIONS.inc(shard=sid)
    print(f"\n🗳️  [shard {sid} term {term}] node {MY_PORT} started an election")

    for port in PEER_PORTS:
        def request_vote(p, t, lli, llt):
            result = send_rpc(p, "/vote", {
                "shard_id":      sid,
                "term":          t,
                "candidate_id":  MY_PORT,
                "last_log_index": lli,
                "last_log_term":  llt,
            })
            if result is None:
                return

            stepped_down = False
            with shard.lock:
                if result.get("term", 0) > shard.term:
                    shard.term      = result["term"]
                    shard.role      = FOLLOWER
                    shard.voted_for = None
                    stepped_down    = True
            if stepped_down:
                persist_hard_state()
                return

            with shard.lock:
                if (result.get("vote_granted") and
                        shard.role == CANDIDATE and
                        result.get("term") == shard.term):
                    shard.votes_received.add(p)
                    cnt = len(shard.votes_received)
                    print(f"   ✅ shard {sid}: vote received from node {p} ({cnt}/{majority()})")
                    if cnt >= majority():
                        _become_leader_locked(shard)

        threading.Thread(
            target=request_vote,
            args=(port, term, last_log_index, last_log_term),
            daemon=True
        ).start()


def _become_leader_locked(shard):
    """Become leader and send an immediate heartbeat while holding ``shard.lock``."""
    shard.role      = LEADER
    shard.leader_id = MY_PORT
    RAFT_LEADER_TRANSITIONS.inc(shard=shard.shard_id)
    print(f"\n👑 [shard {shard.shard_id} term {shard.term}] node {MY_PORT} elected leader")
    threading.Thread(target=send_heartbeats, args=(shard,), daemon=True).start()


# Heartbeats
def send_heartbeats(shard):
    """Send leader heartbeats to all followers; call without ``shard.lock``."""
    with shard.lock:
        term      = shard.term
        ci        = shard.commit_index
        entries   = list(shard.log)
        lo        = shard.log_offset
        snap_term = shard.snapshot_term   # Read consistently with log_offset.
        sid       = shard.shard_id

    for port in PEER_PORTS:
        def hb(p, t, e, c, offset, pt):
            result = send_rpc(p, "/append_entries", {
                "shard_id":       sid,
                "term":           t,
                "leader_id":      MY_PORT,
                "entries":        e,
                "commit_index":   c,
                "log_offset":     offset,
                "prev_log_index": offset - 1,   # Absolute index before this log window.
                "prev_log_term":  pt,            # Term at the snapshot boundary.
            })
            if result and result.get("term", 0) > t:
                stepped_down = False
                with shard.lock:
                    if result["term"] > shard.term:
                        shard.term      = result["term"]
                        shard.role      = FOLLOWER
                        shard.voted_for = None   # No vote has been cast in the new term.
                        stepped_down    = True
                if stepped_down:
                    persist_hard_state()

        threading.Thread(target=hb, args=(port, term, entries, ci, lo, snap_term), daemon=True).start()


def heartbeat_loop():
    """Send periodic heartbeats for every shard led by this node."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        for shard in shards:
            with shard.lock:
                is_leader = (shard.role == LEADER)
            if is_leader:
                threading.Thread(target=send_heartbeats, args=(shard,), daemon=True).start()


def confirm_read_quorum(shard, timeout=READ_QUORUM_TIMEOUT):
    """Confirm that a majority still recognizes this node as leader.

    Current-term AppendEntries probes gate the local read. A higher peer term
    causes this node to step down. This barrier rejects an isolated old leader,
    but it is not a complete Raft ReadIndex implementation; full linearizability
    also depends on the remaining election, log-matching, commit, and apply
    invariants.
    """
    with shard.lock:
        if shard.role != LEADER:
            READ_QUORUM_CHECKS.inc(shard=shard.shard_id, outcome="not_leader")
            return False
        term      = shard.term
        ci        = shard.commit_index
        entries   = list(shard.log)
        lo        = shard.log_offset
        snap_term = shard.snapshot_term
        sid       = shard.shard_id

    if majority() == 1:
        READ_QUORUM_CHECKS.inc(shard=sid, outcome="success")
        return True

    responses = []
    response_lock = threading.Lock()

    def probe(port):
        result = send_rpc(port, "/append_entries", {
            "shard_id":       sid,
            "term":           term,
            "leader_id":      MY_PORT,
            "entries":        entries,
            "commit_index":   ci,
            "log_offset":     lo,
            "prev_log_index": lo - 1,
            "prev_log_term":  snap_term,
        }, timeout=timeout)
        with response_lock:
            responses.append(result)

    threads = [
        threading.Thread(target=probe, args=(port,), daemon=True)
        for port in PEER_PORTS
    ]
    for thread in threads:
        thread.start()

    deadline = time.monotonic() + timeout
    for thread in threads:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        thread.join(timeout=remaining)

    with response_lock:
        peer_responses = list(responses)

    higher_term = max(
        (result.get("term", 0) for result in peer_responses if result),
        default=term,
    )
    hard_state_dirty = False
    leadership_changed = False
    with shard.lock:
        if higher_term > shard.term:
            shard.term      = higher_term
            shard.role      = FOLLOWER
            shard.voted_for = None
            shard.leader_id = None
            hard_state_dirty = True
        if shard.role != LEADER or shard.term != term:
            outcome = "higher_term" if higher_term > term else "leadership_changed"
            leadership_changed = True

    # A higher-term read probe is still a Raft term transition. Persist it before
    # reporting the failed barrier, and never call persist_hard_state under shard.lock.
    if hard_state_dirty:
        persist_hard_state()
    if leadership_changed:
        READ_QUORUM_CHECKS.inc(shard=sid, outcome=outcome)
        return False

    acknowledgements = 1 + sum(
        1 for result in peer_responses
        if result and result.get("success") and result.get("term") == term
    )
    if acknowledgements >= majority():
        READ_QUORUM_CHECKS.inc(shard=sid, outcome="success")
        return True
    READ_QUORUM_CHECKS.inc(shard=sid, outcome="unavailable")
    return False


def election_timer():
    """Start elections for shards whose follower timers expire."""
    while True:
        time.sleep(0.1)
        for shard in shards:
            with shard.lock:
                is_leader = (shard.role == LEADER)
                elapsed   = time.time() - shard.last_heartbeat
                timeout   = shard.election_timeout

            if not is_leader and elapsed > timeout:
                with shard.lock:
                    shard.election_timeout = new_election_timeout()
                start_election(shard)


# HTTP handlers
METRIC_ROUTES = frozenset({
    "/get",
    "/all",
    "/health",
    "/metrics",
    "/debug/raft",
    "/set",
    "/delete",
    "/vote",
    "/append_entries",
    "/install_snapshot",
    "/txn",
    "/txn_prepare",
    "/txn_commit",
    "/txn_abort",
})


def metric_route(path):
    """Return a bounded route label; never expose keys or arbitrary paths."""
    route = path.split("?", 1)[0]
    return route if route in METRIC_ROUTES else "unknown"


class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self._begin_request()
        if self.path.startswith("/get"):
            key   = self.path.split("=")[-1]
            sid   = get_shard(key)
            shard = shards[sid]

            with shard.lock:
                role   = shard.role
                leader = shard.leader_id

            # Followers route to the known leader, which performs the quorum barrier.
            if role != LEADER:
                if leader is not None:
                    result = send_get_rpc(leader, f"/get?key={key}")
                    if result is not None:
                        result["forwarded_by"] = MY_PORT
                        self._respond(200 if "value" in result else 404, result)
                    else:
                        self._respond(503, {"error": "leader unreachable", "shard": sid})
                else:
                    self._respond(503, {"error": "no leader yet for shard", "shard": sid})
                return

            # Leader routing alone cannot exclude an isolated old leader.
            if not confirm_read_quorum(shard):
                self._respond(503, {
                    "error": "leadership quorum unavailable",
                    "shard": sid,
                    "retryable": True,
                })
                return

            with store_lock:
                value = store.get(key)
            if value is None:
                self._respond(404, {"error": f"key '{key}' not found"})
            else:
                self._respond(200, {
                    "key":          key,
                    "value":        value,
                    "from_node":    MY_PORT,
                    "shard":        sid,
                    "shard_leader": MY_PORT,
                })

        elif self.path == "/all":
            with store_lock:
                self._respond(200, {"node": MY_PORT, "data": dict(store)})

        elif self.path == "/health":
            shard_info = {}
            for shard in shards:
                with shard.lock:
                    shard_info[shard.shard_id] = {
                        "role":           shard.role,
                        "term":           shard.term,
                        "leader":         shard.leader_id,
                        "log_length":     len(shard.log),
                        "commit_index":   shard.commit_index,
                        "log_offset":     shard.log_offset,
                        "snapshot_index": shard.snapshot_index,
                        "pending_txns":   len(shard.pending_txns),
                    }
            self._respond(200, {"node": MY_PORT, "shards": shard_info})

        elif self.path == "/metrics":
            self._respond_metrics()

        elif self.path.startswith("/debug/raft"):
            # Test-only endpoint that exposes complete internal state and logs.
            if not TEST_MODE:
                self._respond(404, {"error": "unknown endpoint"})
                return

            sid = None
            if "?" in self.path:
                query = self.path.split("?", 1)[1]
                for pair in query.split("&"):
                    if pair.startswith("shard="):
                        try:
                            sid = int(pair.split("=", 1)[1])
                        except ValueError:
                            self._respond(400, {"error": "shard must be an integer"})
                            return
            if sid is not None and not (0 <= sid < NUM_SHARDS):
                self._respond(404, {"error": f"no such shard {sid}", "num_shards": NUM_SHARDS})
                return

            targets = shards if sid is None else [shards[sid]]
            out = {}
            for shard in targets:
                with shard.lock:
                    out[shard.shard_id] = {
                        "role":           shard.role,
                        # Persistent state defined by the Raft paper.
                        "current_term":   shard.term,
                        "voted_for":      shard.voted_for,
                        "log":            list(shard.log),
                        "log_offset":     shard.log_offset,
                        # volatile state
                        "commit_index":   shard.commit_index,
                        "leader_id":      shard.leader_id,
                        "snapshot_index": shard.snapshot_index,
                        "snapshot_term":  shard.snapshot_term,
                        # Planned fields: last_applied, next_index, and match_index.
                    }
            self._respond(200, {"node": MY_PORT, "num_shards": NUM_SHARDS, "shards": out})

        else:
            self._respond(404, {"error": "unknown endpoint"})

    def do_POST(self):
        self._begin_request()
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length))

        if   self.path == "/set":               self._handle_set(body)
        elif self.path == "/delete":            self._handle_delete(body)
        elif self.path == "/vote":              self._handle_vote(body)
        elif self.path == "/append_entries":    self._handle_append_entries(body)
        elif self.path == "/install_snapshot":  self._handle_install_snapshot(body)
        elif self.path == "/txn":               self._handle_txn(body)
        elif self.path == "/txn_prepare":       self._handle_txn_prepare(body)
        elif self.path == "/txn_commit":        self._handle_txn_commit(body)
        elif self.path == "/txn_abort":         self._handle_txn_abort(body)
        else:
            self._respond(404, {"error": "unknown endpoint"})

    # Core replication operation used by the leader
    def _do_raft_op(self, shard, key, value=None, op="set"):
        """Replicate one set or delete through the shard leader.

        Return ``(True, None)`` on commit or ``(False, error_message)`` on
        failure. The caller must hold no lock.
        """
        sid = shard.shard_id
        with shard.lock:
            if shard.role != LEADER:
                return False, "not leader"
            t     = shard.term
            entry = {"term": t, "op": op, "key": key}
            if op == "set":
                entry["value"] = value
            shard.log.append(entry)
            log_index = len(shard.log) - 1 + shard.log_offset   # Absolute index.

        label = f"{key} = {value}" if op == "set" else f"DELETE {key}"
        print(f"\n📝 [shard {sid} leader] appended log[{log_index}]: {label}")

        replication_started = time.monotonic()
        acks      = [MY_PORT]
        ack_lock  = threading.Lock()
        ack_event = threading.Event()

        def replicate_to(port):
            with shard.lock:
                rep_term      = shard.term
                rep_entries   = list(shard.log)
                rep_ci        = shard.commit_index
                rep_lo        = shard.log_offset
                rep_snap_term = shard.snapshot_term   # Read consistently with rep_lo.
            result = send_rpc(port, "/append_entries", {
                "shard_id":       sid,
                "term":           rep_term,
                "leader_id":      MY_PORT,
                "entries":        rep_entries,
                "commit_index":   rep_ci,
                "log_offset":     rep_lo,
                "prev_log_index": rep_lo - 1,
                "prev_log_term":  rep_snap_term,
            })
            if result and result.get("success"):
                with ack_lock:
                    acks.append(port)
                    print(f"  ✅ shard {sid}: node {port} acknowledged ({len(acks)}/{majority()} nodes)")
                    if len(acks) >= majority():
                        ack_event.set()

        threads = [
            threading.Thread(target=replicate_to, args=(p,), daemon=True)
            for p in PEER_PORTS
        ]
        for th in threads:
            th.start()

        ack_event.wait(timeout=1.0)

        if len(acks) >= majority():
            REPLICATION_ROUND_DURATION.observe(
                time.monotonic() - replication_started,
                shard=sid,
                outcome="success",
            )
            with shard.lock:
                shard.commit_index = log_index
            with store_lock:
                apply_entry(entry)
                persist_committed([storage_mod.WalRecord(
                    shard_id=sid,
                    index=log_index,
                    term=t,
                    op=op,
                    key=key,
                    value=value if op == "set" else None,
                )])
            print(f"  🎉 shard {sid} committed: {label}")
            # Trigger compaction asynchronously without delaying this request.
            threading.Thread(target=maybe_snapshot, args=(shard,), daemon=True).start()
            return True, None
        else:
            REPLICATION_ROUND_DURATION.observe(
                time.monotonic() - replication_started,
                shard=sid,
                outcome="quorum_unavailable",
            )
            return False, f"failed to reach majority ({len(acks)}/{majority()})"

    def _handle_set(self, body):
        key   = body.get("key")
        value = body.get("value")
        sid   = get_shard(key)
        shard = shards[sid]

        with shard.lock:
            r = shard.role
            l = shard.leader_id

        if r != LEADER:
            if l is not None:
                print(f"\n↪️  leader for shard {sid} is {l}; forwarding...")
                result = send_rpc(l, "/set", {"key": key, "value": value})
                if result:
                    result["forwarded_by"] = MY_PORT
                    self._respond(200, result)
                else:
                    self._respond(503, {"error": "leader unreachable", "shard": sid})
            else:
                self._respond(503, {"error": "no leader yet for this shard", "shard": sid})
            return

        # The leader queues the request for the batch loop.
        event  = threading.Event()
        result = [None]
        with shard.batch_cv:
            shard.batch_queue.append(
                {"key": key, "value": value, "op": "set", "event": event, "result": result}
            )
            shard.batch_cv.notify()
        event.wait(timeout=2.0)

        if result[0] and result[0][0]:
            with shard.lock:
                t = shard.term
            self._respond(200, {
                "status":     "ok",
                "key":        key,
                "value":      value,
                "shard":      sid,
                "written_to": MY_PORT,
                "term":       t,
            })
        else:
            self._respond(500, {"error": result[0][1] if result[0] else "timeout", "shard": sid})

    def _handle_delete(self, body):
        key   = body.get("key")
        sid   = get_shard(key)
        shard = shards[sid]

        with shard.lock:
            r = shard.role
            l = shard.leader_id

        if r != LEADER:
            if l is not None:
                print(f"\n↪️  leader for shard {sid} is {l}; forwarding the delete...")
                result = send_rpc(l, "/delete", {"key": key})
                if result:
                    result["forwarded_by"] = MY_PORT
                    self._respond(200, result)
                else:
                    self._respond(503, {"error": "leader unreachable", "shard": sid})
            else:
                self._respond(503, {"error": "no leader yet for this shard", "shard": sid})
            return

        # The leader queues the request for the batch loop.
        event  = threading.Event()
        result = [None]
        with shard.batch_cv:
            shard.batch_queue.append(
                {"key": key, "value": None, "op": "delete", "event": event, "result": result}
            )
            shard.batch_cv.notify()
        event.wait(timeout=2.0)

        if result[0] and result[0][0]:
            with shard.lock:
                t = shard.term
            self._respond(200, {
                "status":     "ok",
                "key":        key,
                "deleted":    True,
                "shard":      sid,
                "written_to": MY_PORT,
                "term":       t,
            })
        else:
            self._respond(500, {"error": result[0][1] if result[0] else "timeout", "shard": sid})

    def _handle_vote(self, body):
        sid = body.get("shard_id", 0)
        if not (0 <= sid < NUM_SHARDS):
            self._respond(400, {"error": f"unknown shard {sid}", "num_shards": NUM_SHARDS})
            return
        shard = shards[sid]

        candidate_term = body.get("term", 0)
        candidate_id   = body.get("candidate_id")
        # The candidate's last-log term/index is distinct from its current term.
        # Confusing them makes the freshness check nearly always pass.
        cand_last_term  = body.get("last_log_term", 0)
        cand_last_index = body.get("last_log_index", -1)

        dirty = False
        with shard.lock:
            if candidate_term > shard.term:
                shard.term      = candidate_term
                shard.role      = FOLLOWER
                shard.voted_for = None
                dirty           = True

            # Raft §5.4.1: grant a vote only if the candidate's log is at
            # least as up to date, comparing term before index.
            my_last_term, my_last_index = _last_log_locked(shard)
            up_to_date = (
                cand_last_term > my_last_term or
                (cand_last_term == my_last_term and cand_last_index >= my_last_index)
            )

            vote_granted = (
                candidate_term >= shard.term and
                (shard.voted_for is None or shard.voted_for == candidate_id) and
                up_to_date
            )

            if vote_granted:
                shard.voted_for      = candidate_id
                shard.last_heartbeat = time.time()
                dirty                = True
            resp_term = shard.term

        # Persist before responding so a granted vote survives a crash.
        if dirty:
            persist_hard_state()

        if vote_granted:
            print(f"  🗳️  shard {sid}: voted for node {candidate_id} (term {candidate_term})")
        elif not up_to_date:
            print(f"  🚫 shard {sid}: refused {candidate_id} -- stale log"
                  f" (candidate last log ({cand_last_term}, {cand_last_index})"
                  f" < this node ({my_last_term}, {my_last_index}))")

        self._respond(200, {"term": resp_term, "vote_granted": vote_granted})

    def _handle_append_entries(self, body):
        sid = body.get("shard_id", 0)
        if not (0 <= sid < NUM_SHARDS):
            self._respond(400, {"error": f"unknown shard {sid}", "num_shards": NUM_SHARDS})
            return
        shard      = shards[sid]
        term       = body.get("term", 0)
        lid        = body.get("leader_id")
        entries    = body.get("entries", [])
        new_commit = body.get("commit_index", -1)
        leader_lo  = body.get("log_offset", 0)

        to_apply      = []
        need_snapshot = False
        snap_leader   = None
        prev_log_index = body.get("prev_log_index", -1)
        prev_log_term  = body.get("prev_log_term", 0)
        hard_state_dirty = False
        conflict_resp    = None

        with shard.lock:
            if term < shard.term:
                self._respond(200, {"term": shard.term, "success": False})
                return

            shard.last_heartbeat = time.time()
            if term > shard.term:
                shard.term      = term
                shard.voted_for = None
                hard_state_dirty = True

            shard.role      = FOLLOWER
            shard.leader_id = lid

            # Verify prevLogIndex consistency.
            # Verify prevLogIndex before accepting new entries.
            if entries and prev_log_index >= 0:
                if prev_log_index >= shard.log_offset:
                    # The previous entry falls inside this log window.
                    rel_i = prev_log_index - shard.log_offset
                    if rel_i < len(shard.log):
                        if shard.log[rel_i]["term"] != prev_log_term:
                            # Truncate at a conflicting term and reject this request.
                            shard.log = shard.log[:rel_i]
                            print(f"  ⚠️  shard {sid}: prevLog conflict (index={prev_log_index}), "
                                  f"truncating at rel_i={rel_i}")
                            conflict_resp = {"term": shard.term, "success": False,
                                             "conflict_index": prev_log_index}
                    # A previous entry beyond this window is handled by snapshot recovery.
                # Entries before log_offset fall within the compacted snapshot range.

            if conflict_resp is None:
                # Synchronize the log window.
                if entries:
                    if leader_lo > shard.log_offset + len(shard.log):
                        # This follower needs a snapshot to catch up.
                        need_snapshot = True
                        snap_leader   = lid
                    else:
                        shard.log        = list(entries)
                        shard.log_offset = leader_lo

                # Collect newly committed entries, converting absolute indexes.
                if not need_snapshot and new_commit > shard.commit_index:
                    start_abs = shard.commit_index + 1
                    end_abs   = min(new_commit + 1, len(shard.log) + shard.log_offset)
                    for abs_i in range(start_abs, end_abs):
                        rel_i = abs_i - shard.log_offset
                        if 0 <= rel_i < len(shard.log):
                            to_apply.append((abs_i, shard.log[rel_i]))
                    shard.commit_index = new_commit

            resp_term = shard.term

        # Persist term/votedFor before every response path, including conflicts.
        if hard_state_dirty:
            persist_hard_state()

        if conflict_resp is not None:
            self._respond(200, conflict_resp)
            return

        # Perform network and disk I/O outside shard.lock.
        if need_snapshot:
            snap = send_rpc(snap_leader, "/install_snapshot",
                            {"shard_id": sid, "requester": MY_PORT},
                            timeout=2.0)
            if snap and "snapshot_index" in snap:
                with store_lock:
                    store.update(snap["store"])
                    storage.checkpoint(store, {sid: snap["snapshot_index"]})
                with shard.lock:
                    shard.snapshot_index = snap["snapshot_index"]
                    shard.snapshot_term  = snap["snapshot_term"]
                    shard.log_offset     = snap["log_offset"]
                    shard.commit_index   = snap["snapshot_index"]
                    shard.log            = snap.get("tail_log", [])
                # Persist the installed snapshot locally.
                fname = snapshot_path(sid)
                with open(fname, "w") as f:
                    json.dump({
                        "snapshot_index": snap["snapshot_index"],
                        "snapshot_term":  snap["snapshot_term"],
                        "log_offset":     snap["log_offset"],
                        "store":          snap["store"],
                    }, f)
                SNAPSHOT_OPERATIONS.inc(shard=sid, operation="install")
                print(f"  📥 shard {sid} installing a snapshot from leader {snap_leader}"
                      f" (snapshot_index={snap['snapshot_index']})")
        elif to_apply:
            with store_lock:
                records = []
                for abs_i, entry in to_apply:
                    apply_entry(entry)
                    records.append(storage_mod.WalRecord(
                        shard_id=sid,
                        index=abs_i,
                        term=entry["term"],
                        op=entry.get("op", "set"),
                        key=entry["key"],
                        value=entry.get("value"),
                    ))
                persist_committed(records)
            # Followers also compact logs that exceed the threshold.
            threading.Thread(target=maybe_snapshot, args=(shard,), daemon=True).start()

        self._respond(200, {"term": resp_term, "success": True})

    def _handle_install_snapshot(self, body):
        """Return the leader's snapshot and post-snapshot log to a follower."""
        sid   = body.get("shard_id", 0)
        shard = shards[sid]

        with shard.lock:
            if shard.role != LEADER:
                self._respond(200, {"error": "not leader"})
                return
            snap_index = shard.snapshot_index
            snap_term  = shard.snapshot_term
            log_offset = shard.log_offset
            tail_log   = list(shard.log)

        with store_lock:
            store_copy = dict(store)

        self._respond(200, {
            "snapshot_index": snap_index,
            "snapshot_term":  snap_term,
            "log_offset":     log_offset,
            "store":          store_copy,
            "tail_log":       tail_log,
        })

    # Deliberately limited multi-key transactions (2PC)
    def _handle_txn(self, body):
        """Coordinate the non-durable two-phase commit path."""
        ops = body.get("ops", [])
        if not ops:
            TRANSACTION_RESULTS.inc(outcome="invalid")
            self._respond(400, {"error": "ops is empty"})
            return

        # Group operations by shard.
        shard_ops = {}
        for op in ops:
            sid = get_shard(op["key"])
            shard_ops.setdefault(sid, []).append(op)

        txn_id = f"{MY_PORT}-{time.time()}"

        # Resolve the current leader for each shard.
        shard_leaders = {}
        for sid in shard_ops:
            with shards[sid].lock:
                leader = shards[sid].leader_id
            if leader is None:
                TRANSACTION_RESULTS.inc(outcome="unavailable")
                self._respond(503, {"error": f"no leader for shard {sid}", "txn_id": txn_id})
                return
            shard_leaders[sid] = leader

        # ── Phase 1: Prepare ──────────────────────────────
        prepare_results      = {}
        prepared_participants = {}
        prep_lock            = threading.Lock()

        def do_prepare(sid, leader, ops_list):
            result, participant = send_txn_prepare_with_discovery(
                txn_id, sid, ops_list, leader
            )
            with prep_lock:
                prepare_results[sid] = result
                prepared_participants[sid] = participant

        prep_threads = [
            threading.Thread(
                target=do_prepare,
                args=(sid, shard_leaders[sid], ops_list),
                daemon=True
            )
            for sid, ops_list in shard_ops.items()
        ]
        for t in prep_threads:
            t.start()
        for t in prep_threads:
            # Each discovery attempt is bounded by send_rpc's timeout. Waiting for all
            # results avoids starting phase 2 against stale cached addresses.
            t.join()

        all_ready = all(
            prepare_results.get(sid, {}).get("status") == "ready"
            for sid in shard_ops
        )

        # Phase 2: commit or abort.
        action = "/txn_commit" if all_ready else "/txn_abort"

        def do_action(leader, sid):
            send_rpc(leader, action, {"txn_id": txn_id, "shard_id": sid}, timeout=2.0)

        action_threads = [
            threading.Thread(
                target=do_action,
                args=(prepared_participants.get(sid, shard_leaders[sid]), sid),
                daemon=True,
            )
            for sid in shard_ops
        ]
        for t in action_threads:
            t.start()
        for t in action_threads:
            t.join(timeout=3.0)

        if all_ready:
            # The existing coordinator does not validate every phase-2 result.  Keep
            # the metric honest: this is the response reported to the client, not a
            # claim that all participants durably committed.
            TRANSACTION_RESULTS.inc(outcome="reported_ok")
            self._respond(200, {"status": "ok", "txn_id": txn_id})
        else:
            TRANSACTION_RESULTS.inc(outcome="aborted")
            failed = [sid for sid in shard_ops
                      if prepare_results.get(sid, {}).get("status") != "ready"]
            self._respond(200, {
                "status": "aborted",
                "txn_id": txn_id,
                "reason": f"prepare failed for shards {failed}",
                "details": {str(sid): prepare_results.get(sid) for sid in failed},
            })

    def _handle_txn_prepare(self, body):
        """Lock keys and stage volatile write intents on the shard leader."""
        txn_id = body.get("txn_id")
        sid    = body.get("shard_id")
        ops    = body.get("ops", [])
        shard  = shards[sid]

        with shard.lock:
            if shard.role != LEADER:
                self._respond(200, {
                    "status": "not_leader",
                    "leader": shard.leader_id,
                })
                return

            # Reject keys locked by another transaction.
            for op in ops:
                key = op["key"]
                if key in shard.key_locks and shard.key_locks[key] != txn_id:
                    self._respond(200, {
                        "status":    "locked",
                        "key":       key,
                        "locked_by": shard.key_locks[key],
                    })
                    return

            # Lock the keys and stage their intents in memory.
            for op in ops:
                shard.key_locks[op["key"]] = txn_id
            shard.pending_txns[txn_id] = ops
            shard.lock_expiry[txn_id]  = time.time() + 10

        print(f"  🔒 shard {sid}: transaction {txn_id} PREPARE"
              f" (keys={[op['key'] for op in ops]})")
        self._respond(200, {"status": "ready"})

    def _handle_txn_commit(self, body):
        """Replicate each prepared operation through the shard leader."""
        txn_id = body.get("txn_id")
        sid    = body.get("shard_id")
        shard  = shards[sid]

        with shard.lock:
            ops  = shard.pending_txns.pop(txn_id, [])
            role = shard.role
            for op in ops:
                shard.key_locks.pop(op["key"], None)
            shard.lock_expiry.pop(txn_id, None)

        if role != LEADER:
            self._respond(200, {"status": "not_leader"})
            return

        print(f"  ✅ shard {sid}: transaction {txn_id} COMMIT ({len(ops)} writes)")
        for op in ops:
            success, err = self._do_raft_op(shard, op["key"], op.get("value"), op.get("op", "set"))
            if not success:
                self._respond(500, {"status": "commit_failed", "error": err, "key": op["key"]})
                return

        self._respond(200, {"status": "ok"})

    def _handle_txn_abort(self, body):
        """Discard staged intents and release their locks."""
        txn_id = body.get("txn_id")
        sid    = body.get("shard_id")
        shard  = shards[sid]

        with shard.lock:
            ops = shard.pending_txns.pop(txn_id, [])
            for op in ops:
                shard.key_locks.pop(op["key"], None)
            shard.lock_expiry.pop(txn_id, None)

        print(f"  ❌ shard {sid}: transaction {txn_id} ABORT")
        self._respond(200, {"status": "ok"})

    def _respond(self, code, data):
        self._record_request(code)
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _begin_request(self):
        self._metrics_started = time.monotonic()
        self._metrics_route = metric_route(self.path)
        self._metrics_recorded = False

    def _record_request(self, code):
        if getattr(self, "_metrics_recorded", False):
            return
        self._metrics_recorded = True
        method = self.command
        route = getattr(self, "_metrics_route", metric_route(self.path))
        started = getattr(self, "_metrics_started", time.monotonic())
        HTTP_REQUESTS.inc(method=method, route=route, status=code)
        HTTP_REQUEST_DURATION.observe(
            max(0.0, time.monotonic() - started),
            method=method,
            route=route,
        )

    def _respond_metrics(self):
        # Count the scrape before rendering so the first scrape proves that generic
        # request instrumentation is connected to the endpoint.
        self._record_request(200)
        refresh_state_metrics()
        body = metrics_registry.render().encode("utf-8")
        self.send_response(200)
        self.send_header(
            "Content-type", "text/plain; version=0.0.4; charset=utf-8"
        )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, f, *a):
        pass


# Startup
if __name__ == "__main__":
    print(f"🚀 sharded Raft node starting on port {MY_PORT}")
    print(f"   bind: {BIND_HOST}:{MY_PORT}   cluster: {ALL_PORTS}   shards: {NUM_SHARDS}")
    load_hard_state()   # Recover currentTerm/votedFor before starting election timers.
    load_from_disk()

    print(f"\n📊 shard plan (each shard elects its own leader):")
    for s in range(NUM_SHARDS):
        print(f"   shard {s}: Raft group = {ALL_PORTS} (election pending)")

    print(f"\n✅ node {MY_PORT} ready\n")

    threading.Thread(target=election_timer,   daemon=True).start()
    threading.Thread(target=heartbeat_loop,   daemon=True).start()
    threading.Thread(target=txn_cleanup_loop, daemon=True).start()
    for shard in shards:
        threading.Thread(target=batch_loop, args=(shard,), daemon=True).start()

    class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
        """Use one thread per request so coordinator self-RPCs cannot deadlock."""
        daemon_threads = True

    ThreadedHTTPServer((BIND_HOST, MY_PORT), Handler).serve_forever()
