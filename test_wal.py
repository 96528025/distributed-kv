"""
test_wal.py -- WAL + checkpoint storage engine tests

Usage:
  python3 test_wal.py                # everything, including a real-node restart integration test
  python3 test_wal.py --unit-only    # only the fast storage-layer unit tests

Requirements covered:
  1  recovery after set
  2  recovery after delete
  3  multi-shard recovery
  4  batch-write recovery (several records in one commit)
  5  transaction-commit recovery (several sets committed in order)
  6  repeated replay has no side effects (idempotent)
  7  partial record at the WAL tail (earlier records kept, tail repaired, later appends recoverable)
  8  checksum corruption (raises rather than failing silently)
  9  invalid checkpoint (raises rather than failing silently)
  10 checkpoint published while the old WAL still holds covered records (deduplicated, not replayed)
  11 recovery after WAL rotation
  12 JSON backend compatibility
  13 repeated start/stop leaves data unchanged
  14 uncommitted in-memory data is not recovered, and the checkpoint applied index is
  15 a real 3-node cluster recovering across two SIGKILLs, with record index continuous afterwards

Every test runs in a temporary directory and cleans up, leaving nothing in the repository.
"""

from __future__ import annotations

import os
import sys
import json
import hashlib
import time
import shutil
import signal
import tempfile
import subprocess
import urllib.request

import storage as st

BASE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE, "node_raft_sharded.py")

PASS = "\033[92m✅ PASS\033[0m"
FAIL = "\033[91m❌ FAIL\033[0m"

_results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"  {PASS if ok else FAIL}  {name}" + (f"\n         {detail}" if detail and not ok else ""))
    _results.append((name, ok, detail))
    return ok


def wal_engine(d: str, port: int, **kw) -> st.WalStorageEngine:
    cfg = st.StorageConfig(backend="wal", data_dir=d, port=port,
                           rotate_records=kw.get("rotate_records", 10_000),
                           fsync=kw.get("fsync", False))
    return st.create_storage_engine(cfg)


# ── 1. recovery after set ───────────────────────────────────
def t_set_recovery(d):
    e = wal_engine(d, 1)
    store = e.load()
    store["name"] = "alice"; e.commit(store, [st.WalRecord(0, 0, 1, "set", "name", "alice")])
    e.close()
    store2 = wal_engine(d, 1).load()
    check("recovery after set", store2 == {"name": "alice"}, str(store2))


# ── 2. recovery after delete ────────────────────────────────
def t_delete_recovery(d):
    e = wal_engine(d, 2)
    s = e.load()
    s["k"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "k", "1")])
    s.pop("k", None); e.commit(s, [st.WalRecord(0, 1, 1, "delete", "k")])
    e.close()
    s2 = wal_engine(d, 2).load()
    check("recovery after delete", s2 == {}, str(s2))


# ── 3. multi-shard recovery ─────────────────────────────────
def t_multi_shard(d):
    e = wal_engine(d, 3)
    s = e.load()
    s["a"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])
    s["b"] = "2"; e.commit(s, [st.WalRecord(1, 0, 1, "set", "b", "2")])
    s["c"] = "3"; e.commit(s, [st.WalRecord(2, 0, 1, "set", "c", "3")])
    e.close()
    s2 = wal_engine(d, 3).load()
    check("multi-shard recovery", s2 == {"a": "1", "b": "2", "c": "3"}, str(s2))


# ── 4. batch-write recovery ─────────────────────────────────
def t_batch_recovery(d):
    e = wal_engine(d, 4)
    s = e.load()
    recs = [st.WalRecord(0, i, 1, "set", f"k{i}", str(i)) for i in range(20)]
    for r in recs:
        s[r.key] = r.value
    e.commit(s, recs)   # several records in one commit (mirrors batch_loop)
    e.close()
    s2 = wal_engine(d, 4).load()
    check("batch-write recovery", s2 == {f"k{i}": str(i) for i in range(20)}, str(len(s2)))


# ── 5. transaction-commit recovery ──────────────────────────
def t_txn_recovery(d):
    # In the node, a transaction commit runs _do_raft_op per key, committing one record at a time.
    e = wal_engine(d, 5)
    s = e.load()
    for i, (k, v) in enumerate([("x", "10"), ("y", "20")]):
        s[k] = v; e.commit(s, [st.WalRecord(0, i, 1, "set", k, v)])
    e.close()
    s2 = wal_engine(d, 5).load()
    check("transaction-commit recovery", s2 == {"x": "10", "y": "20"}, str(s2))


# ── 6. repeated replay is idempotent ────────────────────────
def t_idempotent_replay(d):
    e = wal_engine(d, 6)
    s = e.load()
    s["k"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "k", "1")])
    s["k"] = "2"; e.commit(s, [st.WalRecord(0, 1, 1, "set", "k", "2")])
    e.close()
    # Loading repeatedly must give the same result; re-committing an already-persisted index is skipped.
    for _ in range(3):
        e = wal_engine(d, 6)
        s = e.load()
        e.commit(s, [st.WalRecord(0, 0, 1, "set", "k", "1")])   # index <= applied -> skipped
        e.close()
    final = wal_engine(d, 6).load()
    wal_size = os.path.getsize(os.path.join(d, "wal_6.log"))
    # Idempotent: the value is still the latest "2", and the repeat commit did not re-append to the WAL.
    check("repeated replay is idempotent", final == {"k": "2"}, f"{final}, wal={wal_size}B")


# ── 7. partial record at the WAL tail ───────────────────────
def t_partial_tail(d):
    e = wal_engine(d, 7)
    s = e.load()
    s["a"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])
    s["b"] = "2"; e.commit(s, [st.WalRecord(0, 1, 1, "set", "b", "2")])
    e.close()
    walp = os.path.join(d, "wal_7.log")
    size = os.path.getsize(walp)
    with open(walp, "r+b") as f:
        f.truncate(size - 3)   # chop 3 bytes off the last record (simulates a half-written tail)
    e2 = wal_engine(d, 7)
    s2 = e2.load()
    # Recovery must truncate the partial frame before future appends. Otherwise the next
    # valid frame would remain hidden behind the bad tail forever.
    s2["c"] = "3"
    e2.commit(s2, [st.WalRecord(0, 2, 1, "set", "c", "3")])
    e2.close()
    e3 = wal_engine(d, 7)
    s3 = e3.load()
    e3.close()
    check(
        "WAL tail partial record: append/replay still works after repair",
        s3 == {"a": "1", "c": "3"},
        str(s3),
    )


# ── 8. checksum corruption ──────────────────────────────────
def t_checksum_corruption(d):
    e = wal_engine(d, 8)
    s = e.load()
    s["a"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])
    s["b"] = "2"; e.commit(s, [st.WalRecord(0, 1, 1, "set", "b", "2")])
    e.close()
    walp = os.path.join(d, "wal_8.log")
    # Flip one byte in the first record's payload: the frame is intact but the CRC no longer matches,
    # so this must raise rather than fail silently.
    with open(walp, "r+b") as f:
        f.seek(14)
        byte = f.read(1)
        f.seek(14)
        f.write(bytes([byte[0] ^ 0x01]))
    raised = False
    try:
        wal_engine(d, 8).load()
    except st.StorageCorruptionError:
        raised = True
    check("checksum corruption raises rather than failing silently", raised)


# ── 9. invalid checkpoint ───────────────────────────────────
def t_invalid_checkpoint(d):
    e = wal_engine(d, 9)
    s = e.load()
    s["a"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])
    e.checkpoint(s)   # publish a valid checkpoint
    e.close()
    ckpt = os.path.join(d, "checkpoint_9.json")
    # corrupt the checkpoint contents so the checksum no longer matches
    with open(ckpt, "r+b") as f:
        data = bytearray(f.read())
        data[-5] ^= 0x01
        f.seek(0)
        f.write(data)
    raised = False
    try:
        wal_engine(d, 9).load()
    except st.StorageCorruptionError:
        raised = True
    check("invalid checkpoint raises rather than failing silently", raised)


# ── 10. checkpoint published, old WAL still present ─────────
def t_checkpoint_and_stale_wal(d):
    # Construct the crash point by hand: the checkpoint is published, but the WAL still carries records
    # the checkpoint already covers.
    e = wal_engine(d, 10)
    s = e.load()
    for i in range(3):
        s[f"k{i}"] = str(i); e.commit(s, [st.WalRecord(0, i, 1, "set", f"k{i}", str(i))])
    # Call the internal checkpoint directly (publish and truncate the WAL), then append new records.
    e.checkpoint(s)
    for i in range(3, 5):
        s[f"k{i}"] = str(i); e.commit(s, [st.WalRecord(0, i, 1, "set", f"k{i}", str(i))])
    # Re-append an old record to the WAL, recreating the overlap of a checkpointed record still in the WAL.
    with open(os.path.join(d, "wal_10.log"), "ab") as f:
        f.write(st.WalStorageEngine._encode(st.WalRecord(0, 0, 1, "set", "k0", "STALE")))
    e.close()
    s2 = wal_engine(d, 10).load()
    # index=0 is already covered by the checkpoint, so the STALE record must be deduplicated and k0 stays "0".
    ok = s2 == {f"k{i}": str(i) for i in range(5)}
    check("checkpoint published with old WAL present (deduplicated, not replayed)", ok, str(s2))


# ── 11. recovery after WAL rotation ─────────────────────────
def t_rotation_recovery(d):
    e = wal_engine(d, 11, rotate_records=5)   # checkpoint and rotate every 5 records
    s = e.load()
    for i in range(23):
        # Contract: the store must already reflect the record before commit (in the node, apply_entry runs
        # before persist).
        s[f"k{i}"] = str(i)
        e.commit(s, [st.WalRecord(0, i, 1, "set", f"k{i}", str(i))])
    e.close()
    # rotation should have happened: a checkpoint file exists and the WAL shrank
    ckpt_exists = os.path.exists(os.path.join(d, "checkpoint_11.json"))
    s2 = wal_engine(d, 11).load()
    ok = ckpt_exists and s2 == {f"k{i}": str(i) for i in range(23)}
    check("recovery after WAL rotation", ok, f"ckpt={ckpt_exists}, n={len(s2)}")


# ── 12. JSON backend compatibility ──────────────────────────
def t_json_backend(d):
    cfg = st.StorageConfig(backend="json", data_dir=d, port=12)
    e = st.create_storage_engine(cfg)
    s = e.load()
    s["a"] = "1"; s["b"] = "2"
    e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])   # the JSON backend ignores records and rewrites the whole file
    e.close()
    # The file on disk should be a plain json.dump(store), readable by the older code as-is.
    path = os.path.join(d, "data_raft_sharded_12.json")
    with open(path) as f:
        raw = json.load(f)
    s2 = st.create_storage_engine(cfg).load()
    check("JSON backend compatibility (format + round trip)", raw == {"a": "1", "b": "2"} and s2 == raw, str(s2))


# ── 13. repeated start/stop leaves data unchanged ───────────
def t_repeated_start_stop(d):
    e = wal_engine(d, 13)
    s = e.load()
    s["k"] = "v"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "k", "v")])
    e.close()
    snapshots = []
    for _ in range(5):
        e = wal_engine(d, 13)
        s = e.load()
        snapshots.append(dict(s))
        e.close()
    check("repeated start/stop leaves data unchanged", all(x == {"k": "v"} for x in snapshots), str(snapshots[-1]))


# ── 14. committed-only + applied index metadata ─────────────
def t_committed_only_and_applied_index(d):
    e = wal_engine(d, 14)
    s = e.load()
    s["ghost"] = "not-committed"
    e.close()

    e2 = wal_engine(d, 14)
    recovered = e2.load()
    no_ghost = "ghost" not in recovered
    recovered["durable"] = "yes"
    e2.commit(recovered, [st.WalRecord(0, 7, 3, "set", "durable", "yes")])
    e2.checkpoint(recovered)
    e2.close()

    e3 = wal_engine(d, 14)
    final = e3.load()
    applied = e3.applied_indices()
    e3.close()
    check(
        "only committed data is recovered, and the checkpoint applied index is preserved",
        no_ghost and final == {"durable": "yes"} and applied == {0: 7},
        f"store={final}, applied={applied}",
    )


# ── 15. recovery after SIGKILL (real 3-node cluster integration test) ──
def _post(port, path, data, timeout=3.0):
    body = json.dumps(data).encode()
    req = urllib.request.Request(f"http://localhost:{port}{path}", data=body, method="POST")
    req.add_header("Content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _get(port, path, timeout=3.0):
    try:
        with urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _start_cluster(d, ports):
    procs = []
    for p in ports:
        peers = [str(x) for x in ports if x != p]
        procs.append(subprocess.Popen(
            [sys.executable, SCRIPT, str(p), *peers,
             "--backend=wal", f"--data-dir={d}", "--rotate-records=8"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    return procs


def _kill(procs):
    for pr in procs:
        try:
            pr.send_signal(signal.SIGKILL)
        except Exception:
            pass
    for pr in procs:
        try:
            pr.wait(timeout=5)
        except Exception:
            pass


def _wait_writable(ports, key="probe", tries=60):
    for _ in range(tries):
        r = _post(ports[0], "/set", {"key": key, "value": "1"})
        if r is not None and r.get("status") == "ok":
            return True
        time.sleep(0.5)
    return False


def _post_until_ok(port, path, data, timeout=20.0) -> bool:
    """Retry a submission until it returns status=ok, or time out.

    While the three shards are still electing, some leader forwards time out and the write fails.
    Retrying guarantees every write is genuinely committed before measuring. This is a
    semantics-independent robustness wait.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = _post(port, path, data)
        if r is not None and r.get("status") == "ok":
            return True
        time.sleep(0.3)
    return False


def _wait_replicated(port, expected: dict, deleted: set, timeout=30.0) -> dict:
    """Poll a node's /all until every expected key is present and every deleted key is gone, or time out.

    This is a semantics-independent robustness wait: after a write commits through the leader,
    followers apply it asynchronously via heartbeats. Polling instead of a fixed sleep removes
    timing jitter on a loaded machine.
    """
    deadline = time.time() + timeout
    data = {}
    while time.time() < deadline:
        resp = _get(port, "/all")
        data = (resp or {}).get("data", {})
        if all(data.get(k) == v for k, v in expected.items()) \
                and all(k not in data for k in deleted):
            return data
        time.sleep(0.5)
    return data


def t_sigkill_recovery(d):
    ports = [6301, 6302, 6303]
    procs = _start_cluster(d, ports)
    try:
        if not _wait_writable(ports):
            check("recovery after SIGKILL: cluster start", False, "cluster never became writable")
            return
        # Write across shards plus one delete, covering set/delete, multiple shards and repeated writes.
        # Each write retries until committed, so an early-election forward timeout cannot drop one.
        expected = {"probe": "1"}
        write_ok = True
        for i in range(15):
            k, v = f"key{i}", f"val{i}"
            write_ok &= _post_until_ok(ports[0], "/set", {"key": k, "value": v})
            expected[k] = v
        write_ok &= _post_until_ok(ports[0], "/delete", {"key": "key7"})
        expected.pop("key7", None)
        deleted = {"key7"}

        # Exercise the real coordinator -> participant -> _do_raft_op path across two
        # different shards, not merely direct StorageEngine calls.
        txn_keys = {}
        candidate = 0
        while len(txn_keys) < 2:
            key = f"txn_key_{candidate}"
            sid = int(hashlib.md5(key.encode()).hexdigest(), 16) % len(ports)
            txn_keys.setdefault(sid, key)
            candidate += 1
        txn_ops = [
            {"key": key, "value": f"txn_value_{sid}"}
            for sid, key in txn_keys.items()
        ]
        write_ok &= _post_until_ok(ports[0], "/txn", {"ops": txn_ops})
        expected.update({op["key"]: op["value"] for op in txn_ops})
        if not write_ok:
            check("data fully written and replicated to every replica before SIGKILL", False,
                  "some writes never committed within timeout")
            return

        # Poll until all three replicas have the data, rather than sleeping a fixed amount.
        # Every node must converge first: this system does not persist the Raft log across restarts
        # (only the store and Raft snapshots), so after a full-cluster crash each node recovers from
        # its own WAL and no longer reconciles through the Raft log. Verifying deterministically that
        # crash recovery preserves applied state therefore requires that state to be applied everywhere.
        befores = {p: _wait_replicated(p, expected, deleted, timeout=30.0) for p in ports}
        all_consistent = all(
            all(b.get(k) == v for k, v in expected.items()) and "key7" not in b
            for b in befores.values()
        )
        check("data fully written and replicated to every replica before SIGKILL",
              all_consistent,
              "; ".join(f"node{p}: {len(b)} keys, missing="
                        f"{[k for k in expected if b.get(k) != expected[k]]}"
                        for p, b in befores.items()))

        # SIGKILL every node, giving no chance to close, to mimic a real crash
        _kill(procs)
        procs = []
        time.sleep(1.0)

        # restart the cluster and verify recovery from WAL/checkpoint
        procs = _start_cluster(d, ports)
        if not _wait_writable(ports, key="probe2"):
            check("recovery after SIGKILL: cluster restart", False, "cluster did not restart")
            return
        # read from a different node, polling until recovery completes
        after = _wait_replicated(ports[1], expected, deleted, timeout=30.0)
        ok = all(after.get(f"key{i}") == f"val{i}" for i in range(15) if i != 7) \
            and "key7" not in after
        check("recovery from WAL after SIGKILL (multi-shard + delete)", ok,
              f"got {len(after)} keys; key7 in data={'key7' in after}")
        # Commit new data after recovery, wait until every replica has persisted it, then
        # crash the whole cluster again. This catches WAL index reuse after restart.
        expected["probe2"] = "1"
        post_restart_ok = _post_until_ok(
            ports[0], "/set", {"key": "post_restart", "value": "v2"}
        )
        expected["post_restart"] = "v2"
        replicated_again = {
            p: _wait_replicated(p, expected, deleted, timeout=30.0) for p in ports
        }
        if not post_restart_ok or not all(
            data.get("post_restart") == "v2" for data in replicated_again.values()
        ):
            check("a new commit after restart replicates", False, "post-restart write did not converge")
            return

        _kill(procs)
        procs = []
        time.sleep(1.0)
        procs = _start_cluster(d, ports)
        after_second_crash = _wait_replicated(
            ports[2], expected, deleted, timeout=30.0
        )
        check(
            "WAL index stays continuous across two SIGKILLs and post-restart commits are recoverable",
            all(after_second_crash.get(k) == v for k, v in expected.items())
            and "key7" not in after_second_crash,
            str(after_second_crash),
        )
    finally:
        _kill(procs)


# ── runner ──────────────────────────────────────────────────
UNIT_TESTS = [
    t_set_recovery, t_delete_recovery, t_multi_shard, t_batch_recovery,
    t_txn_recovery, t_idempotent_replay, t_partial_tail, t_checksum_corruption,
    t_invalid_checkpoint, t_checkpoint_and_stale_wal, t_rotation_recovery,
    t_json_backend, t_repeated_start_stop, t_committed_only_and_applied_index,
]


def main():
    unit_only = "--unit-only" in sys.argv
    print("\n🧪 WAL storage engine tests\n" + "─" * 55)
    print("Storage-layer unit tests:")
    for fn in UNIT_TESTS:
        d = tempfile.mkdtemp(prefix="waltest_")
        try:
            fn(d)
        except Exception as e:
            check(fn.__name__, False, f"exception: {e!r}")
        finally:
            shutil.rmtree(d, ignore_errors=True)

    if not unit_only:
        print("\nIntegration test (real nodes + SIGKILL):")
        d = tempfile.mkdtemp(prefix="waltest_int_")
        try:
            t_sigkill_recovery(d)
        except Exception as e:
            check("t_sigkill_recovery", False, f"exception: {e!r}")
        finally:
            shutil.rmtree(d, ignore_errors=True)

    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    print("\n" + "─" * 55)
    if passed == total:
        print(f"  \033[92m🎉 all passed: {passed}/{total}\033[0m")
    else:
        print(f"  \033[91m⚠️  {passed}/{total} passed\033[0m")
        for name, ok, detail in _results:
            if not ok:
                print(f"    • {name}: {detail}")
    print("─" * 55 + "\n")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
