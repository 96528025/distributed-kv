"""
自动化测试：node_raft_sharded.py（快照压缩 + 多 key 事务）

运行方式：
  python3 test_raft_sharded.py

测试覆盖：
  1. 基础读写
  2. Leader 转发（向非 Leader 节点写入）
  3. 日志快照压缩（写够 60 条触发快照，验证文件生成 + log 截断）
  4. 快照恢复（重启节点，验证数据不丢）
  5. Follower 落后拉快照（install_snapshot 流程）
  6. 多 key 事务（正常提交）
  7. 事务锁冲突（prepare 冲突 → abort）
  8. 事务锁超时自动释放（等待 cleanup_loop）
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

# ── 配置 ──────────────────────────────────────────────────
PORTS   = [5001, 5002, 5003]
BASE    = os.path.dirname(os.path.abspath(__file__))
SCRIPT  = os.path.join(BASE, "node_raft_sharded.py")

PASS = "\033[92m✅ PASS\033[0m"
FAIL = "\033[91m❌ FAIL\033[0m"
INFO = "\033[94mℹ️ \033[0m"

results = []   # [(name, ok, detail)]


# ── 工具函数 ────────────────────────────────────────────────
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
    """等待三个节点全部就绪且所有分片都选出 Leader"""
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
    for f in glob.glob(os.path.join(BASE, "snapshot_*.json")):
        os.remove(f)
    for f in glob.glob(os.path.join(BASE, "data_raft_sharded_*.json")):
        os.remove(f)

def get_shard_leader(key):
    """通过任意节点找到 key 所在分片的 Leader"""
    r = http_get(PORTS[0], f"/get?key={key}")
    if r and "shard_leader" in r:
        return r["shard"], r["shard_leader"]
    # key 不存在时，从 health 里查
    import hashlib
    sid = int(hashlib.md5(key.encode()).hexdigest(), 16) % len(PORTS)
    h = http_get(PORTS[0], "/health")
    if h:
        return sid, h["shards"][str(sid)]["leader"]
    return None, None


# ══════════════════════════════════════════════════════════
#  测试开始
# ══════════════════════════════════════════════════════════
print("\n🚀 开始自动化测试：node_raft_sharded.py")
print(f"   脚本路径：{SCRIPT}")

# ── 0. 环境准备 ─────────────────────────────────────────
section("0. 准备环境（清理旧文件 + 启动集群）")
stop_all()
clean_files()
print(f"  {INFO} 旧文件已清理")

procs = []
for port in PORTS:
    peers = [p for p in PORTS if p != port]
    procs.append(start_node(port, peers))
print(f"  {INFO} 三个节点已启动，等待选举...")

ok = wait_for_cluster(timeout=15)
check("集群启动并完成选举", ok)
if not ok:
    print("\n⛔ 集群无法启动，终止测试")
    stop_all()
    sys.exit(1)


# ── 1. 基础读写 ─────────────────────────────────────────
section("1. 基础读写")

r = http_post(PORTS[0], "/set", {"key": "hello", "value": "world"})
check("写入 hello=world", r and r.get("status") == "ok", str(r))

time.sleep(0.3)
for port in PORTS:
    r = http_get(port, "/get?key=hello")
    check(f"节点 {port} 读取 hello", r and r.get("value") == "world",
          f"value={r.get('value') if r else 'None'}")


# ── 2. Leader 转发 ─────────────────────────────────────
section("2. Leader 转发（向非 Leader 写入）")

# 找一个对 key "forward_test" 来说不是 Leader 的节点
import hashlib
sid_ft = int(hashlib.md5("forward_test".encode()).hexdigest(), 16) % len(PORTS)
h = http_get(PORTS[0], "/health")
leader_ft = h["shards"][str(sid_ft)]["leader"]
non_leader = next((p for p in PORTS if p != leader_ft), None)

if non_leader:
    r = http_post(non_leader, "/set", {"key": "forward_test", "value": "forwarded"})
    check(f"向非 Leader {non_leader} 写入（转发到 {leader_ft}）",
          r and r.get("status") == "ok" and "forwarded_by" in r,
          str(r))
    time.sleep(0.3)
    r2 = http_get(PORTS[0], "/get?key=forward_test")
    check("转发后可读到数据", r2 and r2.get("value") == "forwarded")
else:
    check("Leader 转发", False, "找不到非 Leader 节点")


# ── 3. 日志快照压缩 ─────────────────────────────────────
section("3. 日志快照压缩（写 60 条触发快照）")

print(f"  {INFO} 写入 60 条 key（k1~k60）...")
for i in range(1, 61):
    http_post(PORTS[0], "/set", {"key": f"k{i}", "value": f"v{i}"})
time.sleep(1)  # 等快照异步完成

snap_files = glob.glob(os.path.join(BASE, "snapshot_*.json"))
check("快照文件已生成", len(snap_files) > 0,
      f"找到 {len(snap_files)} 个快照文件：{[os.path.basename(f) for f in snap_files]}")

# 验证快照内容格式
if snap_files:
    with open(snap_files[0]) as f:
        snap = json.load(f)
    required_keys = {"snapshot_index", "snapshot_term", "log_offset", "store"}
    check("快照文件格式正确", required_keys.issubset(snap.keys()),
          f"snapshot_index={snap.get('snapshot_index')}, log_offset={snap.get('log_offset')}")

# 验证 Leader 的日志已截断（log_length < 60）
h = http_get(PORTS[0], "/health")
max_log = max(info["log_length"] for info in h["shards"].values())
check(f"日志已截断（最大 log_length={max_log} < 60）", max_log < 60,
      "快照压缩生效，旧日志已删除")

# 验证快照后数据仍可读
r = http_get(PORTS[0], "/get?key=k1")
check("快照后 k1 仍可读", r and r.get("value") == "v1")
r = http_get(PORTS[0], "/get?key=k60")
check("快照后 k60 仍可读", r and r.get("value") == "v60")


# ── 4. 快照恢复（重启节点）───────────────────────────────
section("4. 快照恢复（重启一个节点）")

target = PORTS[1]  # 重启 5002
print(f"  {INFO} 关闭节点 {target}...")
stop_node(target)
time.sleep(2)

print(f"  {INFO} 重启节点 {target}...")
peers = [p for p in PORTS if p != target]
new_proc = start_node(target, peers)
procs.append(new_proc)

print(f"  {INFO} 等待节点重新加入集群...")
time.sleep(6)

# 验证重启后数据仍可读
r = http_get(target, "/get?key=k1")
check(f"节点 {target} 重启后读 k1", r and r.get("value") == "v1",
      f"value={r.get('value') if r else 'None'}")
r = http_get(target, "/get?key=k30")
check(f"节点 {target} 重启后读 k30", r and r.get("value") == "v30",
      f"value={r.get('value') if r else 'None'}")
r = http_get(target, "/get?key=k60")
check(f"节点 {target} 重启后读 k60", r and r.get("value") == "v60",
      f"value={r.get('value') if r else 'None'}")

# 重启后集群仍可写
r = http_post(PORTS[0], "/set", {"key": "after_restart", "value": "yes"})
check("重启后集群仍可写入", r and r.get("status") == "ok")


# ── 5. 多 key 事务（正常提交）─────────────────────────────
section("5. 多 key 事务（正常提交）")

r = http_post(PORTS[0], "/txn", {
    "ops": [
        {"key": "alice", "value": "100"},
        {"key": "bob",   "value": "200"},
    ]
}, timeout=10)
check("事务提交成功", r and r.get("status") == "ok", f"txn_id={r.get('txn_id') if r else None}")

time.sleep(0.5)
ra = http_get(PORTS[0], "/get?key=alice")
rb = http_get(PORTS[0], "/get?key=bob")
check("alice=100 写入正确", ra and ra.get("value") == "100",
      f"value={ra.get('value') if ra else 'None'}")
check("bob=200 写入正确",   rb and rb.get("value") == "200",
      f"value={rb.get('value') if rb else 'None'}")

# 两个 key 从不同节点都能读到
ra2 = http_get(PORTS[1], "/get?key=alice")
rb2 = http_get(PORTS[2], "/get?key=bob")
check("alice 在节点 5002 可读", ra2 and ra2.get("value") == "100")
check("bob 在节点 5003 可读",   rb2 and rb2.get("value") == "200")


# ── 6. 事务锁冲突（prepare 冲突 → abort）─────────────────
section("6. 事务锁冲突（并发事务 → 其中一个 abort）")

# 找 alice 所在分片的 Leader，手动 prepare 锁住它
sid_alice, leader_alice = get_shard_leader("alice")
print(f"  {INFO} alice 在分片 {sid_alice}，Leader={leader_alice}")

# Phase 1: 手动 prepare 锁住 alice
r_prep = http_post(leader_alice, "/txn_prepare", {
    "txn_id":   "test-conflict-001",
    "shard_id": sid_alice,
    "ops":      [{"key": "alice", "value": "locked"}],
})
check("手动 prepare 成功锁住 alice", r_prep and r_prep.get("status") == "ready",
      str(r_prep))

# 此时发起另一个事务修改 alice → 应该 abort
r_txn = http_post(PORTS[0], "/txn", {
    "ops": [{"key": "alice", "value": "conflict"}]
}, timeout=5)
check("冲突事务被 abort",
      r_txn and r_txn.get("status") == "aborted",
      f"reason={r_txn.get('reason') if r_txn else None}")

locked_detail = (r_txn or {}).get("details", {})
check("abort 原因包含 locked 信息",
      any(v.get("status") == "locked" for v in locked_detail.values()),
      str(locked_detail))

# alice 的值未被修改
time.sleep(0.3)
r = http_get(PORTS[0], "/get?key=alice")
check("alice 值未被冲突事务修改", r and r.get("value") == "100",
      f"value={r.get('value') if r else 'None'}")

# 手动 abort 释放锁
r_abort = http_post(leader_alice, "/txn_abort", {
    "txn_id":   "test-conflict-001",
    "shard_id": sid_alice,
})
check("手动 abort 释放锁", r_abort and r_abort.get("status") == "ok")

# 释放后新事务可以成功
r_after = http_post(PORTS[0], "/txn", {
    "ops": [{"key": "alice", "value": "after_unlock"}]
}, timeout=10)
check("释放锁后新事务成功", r_after and r_after.get("status") == "ok")


# ── 7. 事务锁超时自动释放 ──────────────────────────────
section("7. 事务锁超时自动释放（等待 cleanup_loop）")

print(f"  {INFO} 注意：锁超时设为 10s，此测试需要等待约 12s...")

# 找 bob 的分片 Leader
sid_bob, leader_bob = get_shard_leader("bob")
print(f"  {INFO} bob 在分片 {sid_bob}，Leader={leader_bob}")

# prepare 锁住 bob，但不 commit 也不 abort（模拟崩溃的协调者）
r_prep2 = http_post(leader_bob, "/txn_prepare", {
    "txn_id":   "test-timeout-002",
    "shard_id": sid_bob,
    "ops":      [{"key": "bob", "value": "will_timeout"}],
})
check("prepare 成功（锁住 bob）", r_prep2 and r_prep2.get("status") == "ready")

# 立刻尝试修改 bob → 应该被锁
r_blocked = http_post(PORTS[0], "/txn",
                      {"ops": [{"key": "bob", "value": "blocked"}]}, timeout=5)
check("bob 被锁时事务 abort",
      r_blocked and r_blocked.get("status") == "aborted")

# 等待超时自动释放（10s + buffer）
print(f"  {INFO} 等待锁超时（12s）...")
time.sleep(12)

# 超时后应能成功写入
r_after2 = http_post(PORTS[0], "/txn", {
    "ops": [{"key": "bob", "value": "after_timeout"}]
}, timeout=10)
check("锁超时后新事务成功", r_after2 and r_after2.get("status") == "ok",
      str(r_after2))

time.sleep(0.5)
r_bob = http_get(PORTS[0], "/get?key=bob")
check("bob 值已更新为 after_timeout",
      r_bob and r_bob.get("value") == "after_timeout",
      f"value={r_bob.get('value') if r_bob else 'None'}")


# ── 8. /delete 端点 ────────────────────────────────────────
section("8. /delete 端点")

# 先写一个 key，再删除
r = http_post(PORTS[0], "/set", {"key": "to_delete", "value": "bye"})
check("delete 前先写入 to_delete", r and r.get("status") == "ok")
time.sleep(0.3)

r = http_post(PORTS[0], "/delete", {"key": "to_delete"})
check("DELETE to_delete 返回 ok",
      r and r.get("status") == "ok" and r.get("deleted") is True, str(r))
time.sleep(0.5)

# 删除后三个节点都读不到
for port in PORTS:
    r = http_get(port, "/get?key=to_delete")
    check(f"节点 {port} 读不到已删除的 key",
          r is None or r.get("error") is not None or "value" not in r,
          str(r))

# 删除不存在的 key 也应该正常返回 ok（幂等）
r = http_post(PORTS[0], "/delete", {"key": "nonexistent_xyz"})
check("删除不存在的 key 幂等返回 ok", r and r.get("status") == "ok")

# 删除后重新写入同一个 key
r = http_post(PORTS[0], "/set", {"key": "to_delete", "value": "reborn"})
check("删除后可以重新写入同一个 key", r and r.get("status") == "ok")
time.sleep(0.3)
r = http_get(PORTS[0], "/get?key=to_delete")
check("重新写入后可以读到新值", r and r.get("value") == "reborn")

# 向非 Leader 发 delete，验证转发
import hashlib
sid_del = int(hashlib.md5("to_delete".encode()).hexdigest(), 16) % len(PORTS)
h = http_get(PORTS[0], "/health")
leader_del = h["shards"][str(sid_del)]["leader"]
non_leader_del = next((p for p in PORTS if p != leader_del), None)
if non_leader_del:
    r = http_post(non_leader_del, "/delete", {"key": "to_delete"})
    check(f"向非 Leader {non_leader_del} 发 delete 自动转发",
          r and r.get("status") == "ok" and "forwarded_by" in r, str(r))


# ── 9. 线性化读（读路由到 Leader）─────────────────────────
section("9. 线性化读（/get 路由到 Leader）")

# 写入一个 key
r = http_post(PORTS[0], "/set", {"key": "linear_key", "value": "v1"})
check("写入 linear_key=v1", r and r.get("status") == "ok")
time.sleep(0.3)

# 从三个节点读，结果都是 v1（非 Leader 会被转发到 Leader）
for port in PORTS:
    r = http_get(port, "/get?key=linear_key")
    check(f"节点 {port} 读 linear_key = v1（线性化）",
          r and r.get("value") == "v1",
          f"value={r.get('value') if r else 'None'}, forwarded_by={r.get('forwarded_by') if r else '-'}")

# 验证非 Leader 节点的读响应包含 forwarded_by
import hashlib
sid_lk = int(hashlib.md5("linear_key".encode()).hexdigest(), 16) % len(PORTS)
h = http_get(PORTS[0], "/health")
leader_lk = h["shards"][str(sid_lk)]["leader"]
non_leaders = [p for p in PORTS if p != leader_lk]
if non_leaders:
    r = http_get(non_leaders[0], "/get?key=linear_key")
    check(f"非 Leader {non_leaders[0]} 读取包含 forwarded_by 字段",
          r and "forwarded_by" in r,
          str(r))

# Leader 直接读，不含 forwarded_by
r = http_get(leader_lk, "/get?key=linear_key")
check(f"Leader {leader_lk} 直接读，不含 forwarded_by",
      r and "forwarded_by" not in r,
      str(r))

# 写入 → 立即从任意节点读，保证读到最新值（线性化保证）
r = http_post(PORTS[0], "/set", {"key": "linear_key", "value": "v2"})
check("更新 linear_key=v2", r and r.get("status") == "ok")
for port in PORTS:
    r = http_get(port, "/get?key=linear_key")
    check(f"节点 {port} 立即读到最新值 v2",
          r and r.get("value") == "v2",
          f"value={r.get('value') if r else 'None'}")


# ── 10. 批量写入（并发请求合并为一次 Raft round）──────────
section("10. 批量写入（10 个并发请求）")

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
check(f"10 个并发写入全部成功（{ok_count}/10）", ok_count == 10,
      str([r.get("status") if r else "None" for r in batch_results]))

time.sleep(0.5)
for i in [0, 4, 9]:
    r = http_get(PORTS[0], f"/get?key=batch_k{i}")
    check(f"batch_k{i} 写入正确", r and r.get("value") == f"batch_v{i}",
          f"value={r.get('value') if r else 'None'}")

# 验证并发 delete 也能批处理
del_results = [None] * 5

def do_batch_del(i):
    del_results[i] = http_post(PORTS[0], "/delete", {"key": f"batch_k{i}"})

threads = [threading.Thread(target=do_batch_del, args=(i,)) for i in range(5)]
for th in threads:
    th.start()
for th in threads:
    th.join()

del_ok = sum(1 for r in del_results if r and r.get("status") == "ok")
check(f"5 个并发 delete 全部成功（{del_ok}/5）", del_ok == 5)

time.sleep(0.5)
r = http_get(PORTS[0], "/get?key=batch_k0")
check("batch_k0 已被删除", r is None or "error" in (r or {}))


# ══════════════════════════════════════════════════════════
#  汇总结果
# ══════════════════════════════════════════════════════════
section("测试汇总")
stop_all()

total  = len(results)
passed = sum(1 for _, ok, _ in results if ok)
failed = total - passed

for name, ok, detail in results:
    tag = PASS if ok else FAIL
    print(f"  {tag}  {name}")

print(f"\n{'─'*55}")
if failed == 0:
    print(f"  \033[92m🎉 全部通过：{passed}/{total}\033[0m")
else:
    print(f"  \033[91m⚠️  {passed}/{total} 通过，{failed} 失败\033[0m")
    print("\n  失败项：")
    for name, ok, detail in results:
        if not ok:
            print(f"    • {name}：{detail}")
print(f"{'─'*55}\n")

sys.exit(0 if failed == 0 else 1)
