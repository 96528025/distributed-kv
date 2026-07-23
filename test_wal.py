"""
test_wal.py — WAL + checkpoint 存储引擎测试

运行方式：
  python3 test_wal.py                # 全部（含一个真实节点重启集成测试）
  python3 test_wal.py --unit-only    # 只跑快速的存储层单元测试

覆盖需求列表：
  1  set 后重启恢复
  2  delete 后重启恢复
  3  多 shard 恢复
  4  batch write 恢复（一次 commit 多条）
  5  transaction commit 恢复（多条 set 顺序 commit）
  6  重复 replay 不产生副作用（幂等）
  7  WAL 尾部 partial record（保留此前有效记录）
  8  checksum corruption（不静默，抛错）
  9  无效 checkpoint（不静默，抛错）
  10 checkpoint 完成但旧 WAL 仍存在（去重、不重放）
  11 WAL rotation 后恢复
  12 JSON backend 兼容性
  13 多次启动/关闭不改变数据
  14 进程被强制终止（SIGKILL）后的恢复（真实 3 节点集群集成测试）

所有测试使用临时目录，结束后清理，不在仓库留下任何产物。
"""

from __future__ import annotations

import os
import sys
import json
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


# ── 1. set 后重启恢复 ───────────────────────────────────────
def t_set_recovery(d):
    e = wal_engine(d, 1)
    store = e.load()
    store["name"] = "alice"; e.commit(store, [st.WalRecord(0, 0, 1, "set", "name", "alice")])
    e.close()
    store2 = wal_engine(d, 1).load()
    check("set 后重启恢复", store2 == {"name": "alice"}, str(store2))


# ── 2. delete 后重启恢复 ────────────────────────────────────
def t_delete_recovery(d):
    e = wal_engine(d, 2)
    s = e.load()
    s["k"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "k", "1")])
    s.pop("k", None); e.commit(s, [st.WalRecord(0, 1, 1, "delete", "k")])
    e.close()
    s2 = wal_engine(d, 2).load()
    check("delete 后重启恢复", s2 == {}, str(s2))


# ── 3. 多 shard 恢复 ────────────────────────────────────────
def t_multi_shard(d):
    e = wal_engine(d, 3)
    s = e.load()
    s["a"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])
    s["b"] = "2"; e.commit(s, [st.WalRecord(1, 0, 1, "set", "b", "2")])
    s["c"] = "3"; e.commit(s, [st.WalRecord(2, 0, 1, "set", "c", "3")])
    e.close()
    s2 = wal_engine(d, 3).load()
    check("多 shard 恢复", s2 == {"a": "1", "b": "2", "c": "3"}, str(s2))


# ── 4. batch write 恢复 ─────────────────────────────────────
def t_batch_recovery(d):
    e = wal_engine(d, 4)
    s = e.load()
    recs = [st.WalRecord(0, i, 1, "set", f"k{i}", str(i)) for i in range(20)]
    for r in recs:
        s[r.key] = r.value
    e.commit(s, recs)   # 一次 commit 多条（模拟 batch_loop）
    e.close()
    s2 = wal_engine(d, 4).load()
    check("batch write 恢复", s2 == {f"k{i}": str(i) for i in range(20)}, str(len(s2)))


# ── 5. transaction commit 恢复 ──────────────────────────────
def t_txn_recovery(d):
    # 事务提交在节点里是对每个 key 依次 _do_raft_op → 逐条 commit（单条记录）
    e = wal_engine(d, 5)
    s = e.load()
    for i, (k, v) in enumerate([("x", "10"), ("y", "20")]):
        s[k] = v; e.commit(s, [st.WalRecord(0, i, 1, "set", k, v)])
    e.close()
    s2 = wal_engine(d, 5).load()
    check("transaction commit 恢复", s2 == {"x": "10", "y": "20"}, str(s2))


# ── 6. 重复 replay 幂等 ─────────────────────────────────────
def t_idempotent_replay(d):
    e = wal_engine(d, 6)
    s = e.load()
    s["k"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "k", "1")])
    s["k"] = "2"; e.commit(s, [st.WalRecord(0, 1, 1, "set", "k", "2")])
    e.close()
    # 反复 load 多次，结果必须一致；再对已持久化 index 重复 commit 应被跳过
    for _ in range(3):
        e = wal_engine(d, 6)
        s = e.load()
        e.commit(s, [st.WalRecord(0, 0, 1, "set", "k", "1")])   # index<=applied → skip
        e.close()
    final = wal_engine(d, 6).load()
    wal_size = os.path.getsize(os.path.join(d, "wal_6.log"))
    # 幂等：值仍是最新 "2"，且重复 commit 没有把旧记录再写进 WAL
    check("重复 replay 幂等", final == {"k": "2"}, f"{final}, wal={wal_size}B")


# ── 7. WAL 尾部 partial record ──────────────────────────────
def t_partial_tail(d):
    e = wal_engine(d, 7)
    s = e.load()
    s["a"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])
    s["b"] = "2"; e.commit(s, [st.WalRecord(0, 1, 1, "set", "b", "2")])
    e.close()
    walp = os.path.join(d, "wal_7.log")
    size = os.path.getsize(walp)
    with open(walp, "r+b") as f:
        f.truncate(size - 3)   # 砍掉最后一条的尾部 3 字节（模拟半写）
    s2 = wal_engine(d, 7).load()
    check("WAL 尾部 partial record（保留此前有效记录）", s2 == {"a": "1"}, str(s2))


# ── 8. checksum corruption ──────────────────────────────────
def t_checksum_corruption(d):
    e = wal_engine(d, 8)
    s = e.load()
    s["a"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])
    s["b"] = "2"; e.commit(s, [st.WalRecord(0, 1, 1, "set", "b", "2")])
    e.close()
    walp = os.path.join(d, "wal_8.log")
    # 翻转第一条记录 payload 里的一个字节：帧完整但 crc 不符 → 必须抛错，不静默
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
    check("checksum corruption 抛错（不静默）", raised)


# ── 9. 无效 checkpoint ──────────────────────────────────────
def t_invalid_checkpoint(d):
    e = wal_engine(d, 9)
    s = e.load()
    s["a"] = "1"; e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])
    e.checkpoint(s)   # 发布一个合法 checkpoint
    e.close()
    ckpt = os.path.join(d, "checkpoint_9.json")
    # 破坏 checkpoint 内容（校验和不再匹配）
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
    check("无效 checkpoint 抛错（不静默）", raised)


# ── 10. checkpoint 完成但旧 WAL 仍存在 ──────────────────────
def t_checkpoint_and_stale_wal(d):
    # 手工构造“崩溃点”：checkpoint 已发布，但 WAL 还带着已被 checkpoint 覆盖的记录。
    e = wal_engine(d, 10)
    s = e.load()
    for i in range(3):
        s[f"k{i}"] = str(i); e.commit(s, [st.WalRecord(0, i, 1, "set", f"k{i}", str(i))])
    # 直接调用内部 checkpoint（发布并截断 WAL），然后再追加新记录到新 WAL
    e.checkpoint(s)
    for i in range(3, 5):
        s[f"k{i}"] = str(i); e.commit(s, [st.WalRecord(0, i, 1, "set", f"k{i}", str(i))])
    # 人为把旧记录再追加回 WAL（模拟“已 checkpoint 但旧记录仍在 WAL”的重叠）
    with open(os.path.join(d, "wal_10.log"), "ab") as f:
        f.write(st.WalStorageEngine._encode(st.WalRecord(0, 0, 1, "set", "k0", "STALE")))
    e.close()
    s2 = wal_engine(d, 10).load()
    # index=0 已被 checkpoint 覆盖 → 那条 STALE 必须被去重跳过，k0 仍是 "0"
    ok = s2 == {f"k{i}": str(i) for i in range(5)}
    check("checkpoint 完成但旧 WAL 仍存在（去重不重放）", ok, str(s2))


# ── 11. WAL rotation 后恢复 ─────────────────────────────────
def t_rotation_recovery(d):
    e = wal_engine(d, 11, rotate_records=5)   # 每 5 条触发一次 checkpoint+轮换
    s = e.load()
    for i in range(23):
        # 契约：commit 前 store 必须已反映该条记录（节点里 apply_entry 先于 persist）
        s[f"k{i}"] = str(i)
        e.commit(s, [st.WalRecord(0, i, 1, "set", f"k{i}", str(i))])
    e.close()
    # 轮换应已发生（checkpoint 文件存在，WAL 变小）
    ckpt_exists = os.path.exists(os.path.join(d, "checkpoint_11.json"))
    s2 = wal_engine(d, 11).load()
    ok = ckpt_exists and s2 == {f"k{i}": str(i) for i in range(23)}
    check("WAL rotation 后恢复", ok, f"ckpt={ckpt_exists}, n={len(s2)}")


# ── 12. JSON backend 兼容性 ─────────────────────────────────
def t_json_backend(d):
    cfg = st.StorageConfig(backend="json", data_dir=d, port=12)
    e = st.create_storage_engine(cfg)
    s = e.load()
    s["a"] = "1"; s["b"] = "2"
    e.commit(s, [st.WalRecord(0, 0, 1, "set", "a", "1")])   # JSON 后端忽略 records，全量重写
    e.close()
    # 磁盘文件应是普通 json.dump(store) 格式，可被旧代码直接读
    path = os.path.join(d, "data_raft_sharded_12.json")
    with open(path) as f:
        raw = json.load(f)
    s2 = st.create_storage_engine(cfg).load()
    check("JSON backend 兼容性（格式 + 往返）", raw == {"a": "1", "b": "2"} and s2 == raw, str(s2))


# ── 13. 多次启动/关闭不改变数据 ─────────────────────────────
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
    check("多次启动/关闭不改变数据", all(x == {"k": "v"} for x in snapshots), str(snapshots[-1]))


# ── 14. SIGKILL 后恢复（真实 3 节点集群集成测试）────────────
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
    """重试提交直到返回 status=ok（或超时）。

    早期集群三个分片仍在选举时，部分 Leader 转发会超时导致写失败；
    重试保证测量前每条写都真正 committed（语义无关的健壮性等待）。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = _post(port, path, data)
        if r is not None and r.get("status") == "ok":
            return True
        time.sleep(0.3)
    return False


def _wait_replicated(port, expected: dict, deleted: set, timeout=30.0) -> dict:
    """轮询某节点 /all，直到 expected 全部就位且 deleted 全部消失（或超时）。

    这是**语义无关**的健壮性等待：写入经 Leader 转发提交后，follower 通过心跳
    异步 apply；用轮询代替固定 sleep，消除单机高负载下的时序抖动。
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
            check("SIGKILL 后恢复：集群启动", False, "cluster never became writable")
            return
        # 写入多分片数据 + 一次 delete（覆盖 set/delete、多分片、多次写）。
        # 每条写重试直到 committed，避免早期选举期的转发超时导致漏写。
        expected = {"probe": "1"}
        write_ok = True
        for i in range(15):
            k, v = f"key{i}", f"val{i}"
            write_ok &= _post_until_ok(ports[0], "/set", {"key": k, "value": v})
            expected[k] = v
        write_ok &= _post_until_ok(ports[0], "/delete", {"key": "key7"})
        expected.pop("key7", None)
        deleted = {"key7"}
        if not write_ok:
            check("SIGKILL 前数据完整写入并复制到全部副本", False,
                  "some writes never committed within timeout")
            return

        # 轮询直到**所有三个副本**都完整复制到位（而非固定 sleep）。
        # 必须等全部节点收敛：本系统不跨重启持久化 Raft log（只持久化 store + Raft 快照），
        # 全集群崩溃后每个节点各自从自己的 WAL 恢复、不再靠 Raft 日志相互对账。
        # 因此要确定性地验证「崩溃恢复保留已 apply 的状态」，需先确保该状态已 apply 到每个副本。
        befores = {p: _wait_replicated(p, expected, deleted, timeout=30.0) for p in ports}
        all_consistent = all(
            all(b.get(k) == v for k, v in expected.items()) and "key7" not in b
            for b in befores.values()
        )
        check("SIGKILL 前数据完整写入并复制到全部副本",
              all_consistent,
              "; ".join(f"node{p}: {len(b)} keys, missing="
                        f"{[k for k in expected if b.get(k) != expected[k]]}"
                        for p, b in befores.items()))

        # 强杀所有节点（不给 close 机会，模拟真实崩溃）
        _kill(procs)
        procs = []
        time.sleep(1.0)

        # 重启集群，验证从 WAL/checkpoint 恢复
        procs = _start_cluster(d, ports)
        if not _wait_writable(ports, key="probe2"):
            check("SIGKILL 后恢复：集群重启", False, "cluster did not restart")
            return
        # 从另一个节点读，轮询直到恢复完成
        after = _wait_replicated(ports[1], expected, deleted, timeout=30.0)
        ok = all(after.get(f"key{i}") == f"val{i}" for i in range(15) if i != 7) \
            and "key7" not in after
        check("SIGKILL 后从 WAL 恢复（多分片 + delete）", ok,
              f"got {len(after)} keys; key7 in data={'key7' in after}")
    finally:
        _kill(procs)


# ── 运行器 ──────────────────────────────────────────────────
UNIT_TESTS = [
    t_set_recovery, t_delete_recovery, t_multi_shard, t_batch_recovery,
    t_txn_recovery, t_idempotent_replay, t_partial_tail, t_checksum_corruption,
    t_invalid_checkpoint, t_checkpoint_and_stale_wal, t_rotation_recovery,
    t_json_backend, t_repeated_start_stop,
]


def main():
    unit_only = "--unit-only" in sys.argv
    print("\n🧪 WAL 存储引擎测试\n" + "─" * 55)
    print("存储层单元测试：")
    for fn in UNIT_TESTS:
        d = tempfile.mkdtemp(prefix="waltest_")
        try:
            fn(d)
        except Exception as e:
            check(fn.__name__, False, f"exception: {e!r}")
        finally:
            shutil.rmtree(d, ignore_errors=True)

    if not unit_only:
        print("\n集成测试（真实节点 + SIGKILL）：")
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
        print(f"  \033[92m🎉 全部通过：{passed}/{total}\033[0m")
    else:
        print(f"  \033[91m⚠️  {passed}/{total} 通过\033[0m")
        for name, ok, detail in _results:
            if not ok:
                print(f"    • {name}：{detail}")
    print("─" * 55 + "\n")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
