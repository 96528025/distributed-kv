"""
Automated tests for node_raft_sharded.py (snapshot compaction + multi-key transactions)

Usage:
  python3 test_raft_sharded.py

Coverage (section numbers match the run output):
  0. Environment setup: clear old files, start the 3-node cluster, wait for a leader per shard
  1. Basic read/write
  2. Leader forwarding (writing to a non-leader node)
  3. Log snapshot compaction (60 writes trigger a snapshot; verify the file and log truncation)
  4. Snapshot recovery / follower catch-up (restart a node through install_snapshot, no data lost)
  5. Multi-key transactions (2PC, normal commit)
  6. Transaction lock conflict (prepare conflict -> abort)
  7. Transaction lock timeout release (waits for cleanup_loop)
  8. /delete endpoint (forwarding, idempotency, rewrite after delete)
  9. Linearizable reads (/get routed to the leader)
 10. Batch writes (10 concurrent sets + 5 concurrent deletes merged into one Raft round)

This is an integration script rather than a unittest/pytest suite: the 11 sections above produce
56 assertions (checks) at run time, and a full pass prints 56/56.
"""

import json
import subprocess
import sys
import time
import urllib.request
import urllib.error
import os
import glob
import threading
import atexit

# ── configuration ─────────────────────────────────────────
PORTS   = [5001, 5002, 5003]
BASE    = os.path.dirname(os.path.abspath(__file__))
SCRIPT  = os.path.join(BASE, "node_raft_sharded.py")

PASS = "\033[92m✅ PASS\033[0m"
FAIL = "\033[91m❌ FAIL\033[0m"
INFO = "\033[94mℹ️ \033[0m"

results = []   # [(name, ok, detail)]


# ── helpers ─────────────────────────────────────────────────
def http_get(port, path, timeout=3):
    try:
        with urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        return None

def http_post(port, path, data, timeout=3):
    try:
        body = json.dumps(data).encode()
        req  = urllib.request.Request(
            f"http://localhost:{port}{path}", data=body, method="POST"
        )
        req.add_header("Content-type", "application/json")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        return None

def check(name, ok, detail=""):
    tag = PASS if ok else FAIL
    print(f"  {tag}  {name}")
    if detail:
        print(f"         {detail}")
    results.append((name, ok, detail))
    return ok

def section(title):
    print(f"\n{'─'*55}")
    print(f"  {title}")
    print(f"{'─'*55}")

def wait_for_cluster(timeout=12):
    """Wait until all three nodes are up and every shard has elected a leader."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            all_ready = True
            for port in PORTS:
                h = http_get(port, "/health", timeout=1)
                if h is None:
                    all_ready = False
                    break
                for sid, info in h["shards"].items():
                    if info["leader"] is None:
                        all_ready = False
                        break
            if all_ready:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False

def start_node(port, peers):
    peer_args = [str(p) for p in peers if p != port]
    proc = subprocess.Popen(
        [sys.executable, SCRIPT, str(port)] + peer_args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=BASE,
    )
    return proc

def stop_node(port):
    subprocess.run(["pkill", "-f", f"node_raft_sharded.py {port}"],
                   capture_output=True)
    time.sleep(0.5)

def stop_all():
    subprocess.run(["pkill", "-f", "node_raft_sharded.py"],
                   capture_output=True)
    time.sleep(1)

def clean_files():
    for pattern in ("snapshot_*.json", "data_raft_sharded_*.json",
                    "raft_hardstate_*.json", "raft_hardstate_*.json.tmp"):
        for f in glob.glob(os.path.join(BASE, pattern)):
            os.remove(f)


def _final_cleanup():
    """Kill the node processes and clean up generated data/snapshot files on every exit path:
    normal completion, test failure, an exception, or Ctrl-C. Without this hook the error paths
    leave three node_raft_sharded.py processes holding ports 5001-5003."""
    stop_all()
    clean_files()

atexit.register(_final_cleanup)

def get_shard_leader(key):
    """Find the leader of the shard owning a key, asking any node."""
    r = http_get(PORTS[0], f"/get?key={key}")
    if r and "shard_leader" in r:
        return r["shard"], r["shard_leader"]
    # if the key does not exist, look it up via /health
    import hashlib
    sid = int(hashlib.md5(key.encode()).hexdigest(), 16) % len(PORTS)
    h = http_get(PORTS[0], "/health")
    if h:
        return sid, h["shards"][str(sid)]["leader"]
    return None, None


def health(port=None, attempts=6, delay=0.5):
    """Fetch /health, retrying briefly before giving up.

    http_get returns None on any error, including a timeout while the machine is
    loaded. Indexing ["shards"] straight off that None turns a transient blip into
    a TypeError that aborts the whole run with a stack trace, instead of a failure
    the summary can report.
    """
    target = PORTS[0] if port is None else port
    for _ in range(attempts):
        h = http_get(target, "/health")
        if h and h.get("shards"):
            return h
        time.sleep(delay)
    check("/health reachable", False,
          f"node {target} did not answer /health after {attempts} attempts")
    stop_all()
    sys.exit(1)


# ══════════════════════════════════════════════════════════
#  tests begin
# ══════════════════════════════════════════════════════════
print("\n🚀 Automated tests: node_raft_sharded.py")
print(f"   script: {SCRIPT}")

# ── 0. environment setup ────────────────────────────────
section("0. Environment setup (clear old files, start the cluster)")
stop_all()
clean_files()
print(f"  {INFO} old files cleared")

procs = []
for port in PORTS:
    peers = [p for p in PORTS if p != port]
    procs.append(start_node(port, peers))
print(f"  {INFO} three nodes started, waiting for elections...")

ok = wait_for_cluster(timeout=15)
check("cluster started and elected leaders", ok)
if not ok:
    print("\n⛔ cluster failed to start; aborting")
    stop_all()
    sys.exit(1)


# ── 1. basic read/write ─────────────────────────────────
section("1. Basic read/write")

r = http_post(PORTS[0], "/set", {"key": "hello", "value": "world"})
check("write hello=world", r and r.get("status") == "ok", str(r))

time.sleep(0.3)
for port in PORTS:
    r = http_get(port, "/get?key=hello")
    check(f"node {port} reads hello", r and r.get("value") == "world",
          f"value={r.get('value') if r else 'None'}")


# ── 2. leader forwarding ────────────────────────────────
section("2. Leader forwarding (write to a non-leader)")

# find a node that is not the leader for key "forward_test"
import hashlib
sid_ft = int(hashlib.md5("forward_test".encode()).hexdigest(), 16) % len(PORTS)
h = health()
leader_ft = h["shards"][str(sid_ft)]["leader"]
non_leader = next((p for p in PORTS if p != leader_ft), None)

if non_leader:
    r = http_post(non_leader, "/set", {"key": "forward_test", "value": "forwarded"})
    check(f"write to non-leader {non_leader} (forwarded to {leader_ft})",
          r and r.get("status") == "ok" and "forwarded_by" in r,
          str(r))
    time.sleep(0.3)
    r2 = http_get(PORTS[0], "/get?key=forward_test")
    check("data readable after forwarding", r2 and r2.get("value") == "forwarded")
else:
    check("leader forwarding", False, "no non-leader node found")


# ── 3. log snapshot compaction ──────────────────────────
section("3. Log snapshot compaction (60 writes trigger a snapshot)")

print(f"  {INFO} writing 60 keys (k1..k60)...")
for i in range(1, 61):
    http_post(PORTS[0], "/set", {"key": f"k{i}", "value": f"v{i}"})
time.sleep(1)  # wait for the async snapshot to finish

snap_files = glob.glob(os.path.join(BASE, "snapshot_*.json"))
check("snapshot file created", len(snap_files) > 0,
      f"found {len(snap_files)} snapshot file(s): {[os.path.basename(f) for f in snap_files]}")

# verify the snapshot format
if snap_files:
    with open(snap_files[0]) as f:
        snap = json.load(f)
    required_keys = {"snapshot_index", "snapshot_term", "log_offset", "store"}
    check("snapshot file format is correct", required_keys.issubset(snap.keys()),
          f"snapshot_index={snap.get('snapshot_index')}, log_offset={snap.get('log_offset')}")

# verify the leader's log was truncated (log_length < 60)
h = health()
max_log = max(info["log_length"] for info in h["shards"].values())
check(f"log truncated (max log_length={max_log} < 60)", max_log < 60,
      "snapshot compaction took effect and old entries were removed")

# data must remain readable after the snapshot
r = http_get(PORTS[0], "/get?key=k1")
check("k1 still readable after the snapshot", r and r.get("value") == "v1")
r = http_get(PORTS[0], "/get?key=k60")
check("k60 still readable after the snapshot", r and r.get("value") == "v60")


# ── 4. snapshot recovery (restart a node) ───────────────
section("4. Snapshot recovery (restart one node)")

target = PORTS[1]  # restart 5002
print(f"  {INFO} stopping node {target}...")
stop_node(target)
time.sleep(2)

print(f"  {INFO} restarting node {target}...")
peers = [p for p in PORTS if p != target]
new_proc = start_node(target, peers)
procs.append(new_proc)

print(f"  {INFO} waiting for the node to rejoin the cluster...")
time.sleep(6)

# data must remain readable after the restart
r = http_get(target, "/get?key=k1")
check(f"node {target} reads k1 after restart", r and r.get("value") == "v1",
      f"value={r.get('value') if r else 'None'}")
r = http_get(target, "/get?key=k30")
check(f"node {target} reads k30 after restart", r and r.get("value") == "v30",
      f"value={r.get('value') if r else 'None'}")
r = http_get(target, "/get?key=k60")
check(f"node {target} reads k60 after restart", r and r.get("value") == "v60",
      f"value={r.get('value') if r else 'None'}")

# the cluster must still accept writes after the restart
r = http_post(PORTS[0], "/set", {"key": "after_restart", "value": "yes"})
check("cluster still writable after restart", r and r.get("status") == "ok")


# ── 5. multi-key transaction (normal commit) ────────────
section("5. Multi-key transaction (normal commit)")

r = http_post(PORTS[0], "/txn", {
    "ops": [
        {"key": "alice", "value": "100"},
        {"key": "bob",   "value": "200"},
    ]
}, timeout=10)
check("transaction committed", r and r.get("status") == "ok", f"txn_id={r.get('txn_id') if r else None}")

time.sleep(0.5)
ra = http_get(PORTS[0], "/get?key=alice")
rb = http_get(PORTS[0], "/get?key=bob")
check("alice=100 written correctly", ra and ra.get("value") == "100",
      f"value={ra.get('value') if ra else 'None'}")
check("bob=200 written correctly",   rb and rb.get("value") == "200",
      f"value={rb.get('value') if rb else 'None'}")

# both keys must be readable from different nodes
ra2 = http_get(PORTS[1], "/get?key=alice")
rb2 = http_get(PORTS[2], "/get?key=bob")
check("alice readable on node 5002", ra2 and ra2.get("value") == "100")
check("bob readable on node 5003",   rb2 and rb2.get("value") == "200")


# ── 6. transaction lock conflict (prepare -> abort) ─────
section("6. Transaction lock conflict (concurrent txns -> one aborts)")

# find the leader of alice's shard and lock it with a manual prepare
sid_alice, leader_alice = get_shard_leader("alice")
print(f"  {INFO} alice is on shard {sid_alice}, leader={leader_alice}")

# Phase 1: manual prepare locks alice
r_prep = http_post(leader_alice, "/txn_prepare", {
    "txn_id":   "test-conflict-001",
    "shard_id": sid_alice,
    "ops":      [{"key": "alice", "value": "locked"}],
})
check("manual prepare locked alice", r_prep and r_prep.get("status") == "ready",
      str(r_prep))

# a second transaction touching alice must now abort
r_txn = http_post(PORTS[0], "/txn", {
    "ops": [{"key": "alice", "value": "conflict"}]
}, timeout=5)
check("the conflicting transaction aborted",
      r_txn and r_txn.get("status") == "aborted",
      f"reason={r_txn.get('reason') if r_txn else None}")

locked_detail = (r_txn or {}).get("details", {})
check("the abort reason mentions the lock",
      any(v.get("status") == "locked" for v in locked_detail.values()),
      str(locked_detail))

# alice's value must be unchanged
time.sleep(0.3)
r = http_get(PORTS[0], "/get?key=alice")
check("alice unchanged by the conflicting transaction", r and r.get("value") == "100",
      f"value={r.get('value') if r else 'None'}")

# manual abort releases the lock
r_abort = http_post(leader_alice, "/txn_abort", {
    "txn_id":   "test-conflict-001",
    "shard_id": sid_alice,
})
check("manual abort released the lock", r_abort and r_abort.get("status") == "ok")

# a new transaction can succeed once the lock is released
r_after = http_post(PORTS[0], "/txn", {
    "ops": [{"key": "alice", "value": "after_unlock"}]
}, timeout=10)
check("new transaction succeeds after the lock is released", r_after and r_after.get("status") == "ok")


# ── 7. transaction lock timeout release ─────────────────
section("7. Transaction lock timeout release (waits for cleanup_loop)")

print(f"  {INFO} note: the lock timeout is 10s, so this test waits about 12s...")

# find the leader of bob's shard
sid_bob, leader_bob = get_shard_leader("bob")
print(f"  {INFO} bob is on shard {sid_bob}, leader={leader_bob}")

# prepare locks bob but never commits or aborts, simulating a crashed coordinator
r_prep2 = http_post(leader_bob, "/txn_prepare", {
    "txn_id":   "test-timeout-002",
    "shard_id": sid_bob,
    "ops":      [{"key": "bob", "value": "will_timeout"}],
})
check("prepare succeeded (bob locked)", r_prep2 and r_prep2.get("status") == "ready")

# an immediate attempt to modify bob must be blocked
r_blocked = http_post(PORTS[0], "/txn",
                      {"ops": [{"key": "bob", "value": "blocked"}]}, timeout=5)
check("transaction aborts while bob is locked",
      r_blocked and r_blocked.get("status") == "aborted")

# wait for the automatic timeout release (10s plus buffer)
print(f"  {INFO} waiting for the lock to time out (12s)...")
time.sleep(12)

# the write must succeed once the lock has timed out
r_after2 = http_post(PORTS[0], "/txn", {
    "ops": [{"key": "bob", "value": "after_timeout"}]
}, timeout=10)
check("new transaction succeeds after the lock times out", r_after2 and r_after2.get("status") == "ok",
      str(r_after2))

time.sleep(0.5)
r_bob = http_get(PORTS[0], "/get?key=bob")
check("bob updated to after_timeout",
      r_bob and r_bob.get("value") == "after_timeout",
      f"value={r_bob.get('value') if r_bob else 'None'}")


# ── 8. /delete endpoint ─────────────────────────────────
section("8. /delete endpoint")

# write a key, then delete it
r = http_post(PORTS[0], "/set", {"key": "to_delete", "value": "bye"})
check("write to_delete before deleting", r and r.get("status") == "ok")
time.sleep(0.3)

r = http_post(PORTS[0], "/delete", {"key": "to_delete"})
check("DELETE to_delete returns ok",
      r and r.get("status") == "ok" and r.get("deleted") is True, str(r))
time.sleep(0.5)

# after deletion the key must be gone on all three nodes
for port in PORTS:
    r = http_get(port, "/get?key=to_delete")
    check(f"node {port} no longer returns the deleted key",
          r is None or r.get("error") is not None or "value" not in r,
          str(r))

# deleting a missing key must still return ok (idempotent)
r = http_post(PORTS[0], "/delete", {"key": "nonexistent_xyz"})
check("deleting a missing key returns ok (idempotent)", r and r.get("status") == "ok")

# the same key can be written again after deletion
r = http_post(PORTS[0], "/set", {"key": "to_delete", "value": "reborn"})
check("the same key can be rewritten after deletion", r and r.get("status") == "ok")
time.sleep(0.3)
r = http_get(PORTS[0], "/get?key=to_delete")
check("the rewritten value is readable", r and r.get("value") == "reborn")

# send delete to a non-leader to verify forwarding
import hashlib
sid_del = int(hashlib.md5("to_delete".encode()).hexdigest(), 16) % len(PORTS)
h = health()
leader_del = h["shards"][str(sid_del)]["leader"]
non_leader_del = next((p for p in PORTS if p != leader_del), None)
if non_leader_del:
    r = http_post(non_leader_del, "/delete", {"key": "to_delete"})
    check(f"delete sent to non-leader {non_leader_del} is forwarded",
          r and r.get("status") == "ok" and "forwarded_by" in r, str(r))


# ── 9. linearizable reads (routed to the leader) ────────
section("9. Linearizable reads (/get routed to the leader)")

# write a key
r = http_post(PORTS[0], "/set", {"key": "linear_key", "value": "v1"})
check("write linear_key=v1", r and r.get("status") == "ok")
time.sleep(0.3)

# all three nodes must return v1; non-leaders forward to the leader
for port in PORTS:
    r = http_get(port, "/get?key=linear_key")
    check(f"node {port} reads linear_key = v1 (linearizable)",
          r and r.get("value") == "v1",
          f"value={r.get('value') if r else 'None'}, forwarded_by={r.get('forwarded_by') if r else '-'}")

# a non-leader read response must carry forwarded_by
import hashlib
sid_lk = int(hashlib.md5("linear_key".encode()).hexdigest(), 16) % len(PORTS)
h = health()
leader_lk = h["shards"][str(sid_lk)]["leader"]
non_leaders = [p for p in PORTS if p != leader_lk]
if non_leaders:
    r = http_get(non_leaders[0], "/get?key=linear_key")
    check(f"non-leader {non_leaders[0]} read includes forwarded_by",
          r and "forwarded_by" in r,
          str(r))

# a direct leader read must not carry forwarded_by
r = http_get(leader_lk, "/get?key=linear_key")
check(f"leader {leader_lk} reads directly, without forwarded_by",
      r and "forwarded_by" not in r,
      str(r))

# write, then read immediately from any node: the latest value must be visible (linearizability)
r = http_post(PORTS[0], "/set", {"key": "linear_key", "value": "v2"})
check("update linear_key=v2", r and r.get("status") == "ok")
for port in PORTS:
    r = http_get(port, "/get?key=linear_key")
    check(f"node {port} immediately reads the latest value v2",
          r and r.get("value") == "v2",
          f"value={r.get('value') if r else 'None'}")


# ── 10. batch writes (concurrent requests merged) ───────
section("10. Batch writes (10 concurrent requests)")

batch_results = [None] * 10

def do_batch_set(i):
    batch_results[i] = http_post(PORTS[0], "/set",
                                 {"key": f"batch_k{i}", "value": f"batch_v{i}"})

threads = [threading.Thread(target=do_batch_set, args=(i,)) for i in range(10)]
for th in threads:
    th.start()
for th in threads:
    th.join()

ok_count = sum(1 for r in batch_results if r and r.get("status") == "ok")
check(f"all 10 concurrent writes succeeded ({ok_count}/10)", ok_count == 10,
      str([r.get("status") if r else "None" for r in batch_results]))

time.sleep(0.5)
for i in [0, 4, 9]:
    r = http_get(PORTS[0], f"/get?key=batch_k{i}")
    check(f"batch_k{i} written correctly", r and r.get("value") == f"batch_v{i}",
          f"value={r.get('value') if r else 'None'}")

# concurrent deletes must batch as well
del_results = [None] * 5

def do_batch_del(i):
    del_results[i] = http_post(PORTS[0], "/delete", {"key": f"batch_k{i}"})

threads = [threading.Thread(target=do_batch_del, args=(i,)) for i in range(5)]
for th in threads:
    th.start()
for th in threads:
    th.join()

del_ok = sum(1 for r in del_results if r and r.get("status") == "ok")
check(f"all 5 concurrent deletes succeeded ({del_ok}/5)", del_ok == 5)

time.sleep(0.5)
r = http_get(PORTS[0], "/get?key=batch_k0")
check("batch_k0 was deleted", r is None or "error" in (r or {}))


# ══════════════════════════════════════════════════════════
#  summary
# ══════════════════════════════════════════════════════════
section("Summary")
stop_all()

total  = len(results)
passed = sum(1 for _, ok, _ in results if ok)
failed = total - passed

for name, ok, detail in results:
    tag = PASS if ok else FAIL
    print(f"  {tag}  {name}")

print(f"\n{'─'*55}")
if failed == 0:
    print(f"  \033[92m🎉 all passed: {passed}/{total}\033[0m")
else:
    print(f"  \033[91m⚠️  {passed}/{total} passed, {failed} failed\033[0m")
    print("\n  Failures:")
    for name, ok, detail in results:
        if not ok:
            print(f"    • {name}: {detail}")
print(f"{'─'*55}\n")

sys.exit(0 if failed == 0 else 1)
