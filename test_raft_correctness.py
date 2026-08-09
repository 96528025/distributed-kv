"""
Raft 正确性回归测试

运行方式：
  python3 test_raft_correctness.py

与 test_raft_sharded.py 的分工：
  test_raft_sharded.py  —— 功能集成测试（happy path + 基本故障），真三节点集群
  test_raft_correctness.py —— 安全性回归测试，确定性构造，每条断言对应
                              docs/RAFT_CORRECTNESS.md 里的一个 invariant

每个测试都不依赖调度时序：真实节点的选举超时被调到极大，它不会自发竞选，
全部状态转换由测试构造的 RPC 驱动。唯一例外是 T1.7（必须观察真实节点主动
发出的 RequestVote），它用 FakePeer 录制，并且断言写成 ">=" 而不是 "==" ，
所以多竞选几轮也不会 flaky。

PR1 覆盖：
  C1 (Election Safety)      —— T1.1, T1.5, T1.6, T1.7
  C2 (Leader Completeness)  —— T1.2, T1.3, T1.4
  测试基础设施本身           —— T1.8, T1.9
"""

import atexit
import subprocess
import sys
import time

from raft_harness import (
    FakePeer, RealNode, append_entries, clean_artifacts, http_get, http_get_status,
    last_log_tuple, make_entries, request_vote,
)

PORT   = 5401
PEERS  = [5402, 5403]
SHARD  = 0

PASS = "\033[92m✅ PASS\033[0m"
FAIL = "\033[91m❌ FAIL\033[0m"
INFO = "\033[94mℹ️ \033[0m"

results = []


def check(name, ok, detail=""):
    print(f"  {PASS if ok else FAIL}  {name}")
    if detail:
        print(f"         {detail}")
    results.append((name, bool(ok), detail))
    return ok


def section(title):
    print(f"\n{'─'*62}\n  {title}\n{'─'*62}")


def _cleanup():
    subprocess.run(["pkill", "-f", f"node_raft_sharded.py {PORT}"], capture_output=True)
    clean_artifacts()

atexit.register(_cleanup)


def fresh_node(**kw):
    """每个测试独立起一个干净节点：先清掉上一轮的持久化产物。"""
    _cleanup()
    time.sleep(0.3)
    kw.setdefault("num_shards", 1)
    return RealNode(PORT, PEERS, **kw).start()


# ══════════════════════════════════════════════════════════════
print("\n🔬 Raft 正确性回归测试（PR1: election correctness）")

# ── C1 · Election Safety ──────────────────────────────────────
section("T1.1  重启后同一 term 不得重复投票  [I-C1.1 / I-C1.3]")

node = fresh_node()
r = request_vote(PORT, term=5, candidate_id=9001, last_log_term=0, last_log_index=-1)
check("term 5 首次投票被接受（候选人 9001）", r is not None and r.get("vote_granted") is True,
      f"resp={r}")

d = node.debug(SHARD)
check("投票后内存状态正确（term=5, votedFor=9001）",
      d and d["current_term"] == 5 and d["voted_for"] == 9001,
      f"debug={d}")

node.kill()
node.start()
d = node.debug(SHARD)
check("SIGKILL 重启后 term/votedFor 仍在（term=5, votedFor=9001）",
      d and d["current_term"] == 5 and d["voted_for"] == 9001,
      f"debug={d}")

r = request_vote(PORT, term=5, candidate_id=9002, last_log_term=0, last_log_index=-1)
check("重启后同一 term 拒绝投给第二个候选人 9002",
      r is not None and r.get("vote_granted") is False,
      f"resp={r} —— 授票会让 term 5 出现两个 Leader")
node.kill()


# ── C2 · Leader Completeness ──────────────────────────────────
section("T1.2 / T1.3  RequestVote 日志新旧检查  [I-C2.1]")

node = fresh_node()
append_entries(PORT, term=5, leader_id=9001, entries=make_entries(3, term=5),
               commit_index=2, log_offset=0, prev_log_index=-1, prev_log_term=0)
d = node.debug(SHARD)
check("前置：投票方已持有 3 条 term=5 的日志（last log = (5, 2)）",
      d and len(d["log"]) == 3 and d["current_term"] == 5,
      f"debug={d}")

r = request_vote(PORT, term=6, candidate_id=9002, last_log_term=4, last_log_index=0)
check("T1.2  日志落后的候选人被拒绝（候选人 last log = (4, 0)）",
      r is not None and r.get("vote_granted") is False,
      f"resp={r} —— 授票会让已提交条目 1..2 被覆盖丢失")

r = request_vote(PORT, term=7, candidate_id=9003, last_log_term=5, last_log_index=2)
check("T1.3  日志一样新的候选人被接受（反向保护，防止规则过严）",
      r is not None and r.get("vote_granted") is True,
      f"resp={r}")
node.kill()


section("T1.4  比较的是 lastLogTerm，不是 currentTerm  [I-C2.2]")

node = fresh_node()
# commit_index=-1：日志灌进去但不提交，避免触发快照压缩把日志截空。
# （快照边界这条路径由 T1.4c 单独覆盖。）
append_entries(PORT, term=5, leader_id=9001, entries=make_entries(51, term=5),
               commit_index=-1, log_offset=0, prev_log_index=-1, prev_log_term=0)
d = node.debug(SHARD)
check("前置：投票方 last log = (term=5, index=50)",
      last_log_tuple(d) == (5, 50),
      f"last_log={last_log_tuple(d)}")

# 候选人日志更短但 term 更高 —— term 优先，应当授票。
r = request_vote(PORT, term=10, candidate_id=9101, last_log_term=6, last_log_index=1)
check("T1.4a  候选人 last log = (6, 1) 胜过投票方 (5, 50) —— lastLogTerm 优先于 index",
      r is not None and r.get("vote_granted") is True,
      f"resp={r}")

# 同 lastLogTerm、index 更小 —— 应当拒绝。
r = request_vote(PORT, term=11, candidate_id=9102, last_log_term=5, last_log_index=49)
check("T1.4b  候选人 last log = (5, 49) 落后于投票方 (5, 50) —— 拒绝",
      r is not None and r.get("vote_granted") is False,
      f"resp={r}")
node.kill()

# T1.4c：日志被快照截空后，投票方必须退回快照边界来比较。
# 这是投票方和候选人两侧最容易算出不同答案的地方 —— 一侧用 log[-1]，
# 另一侧忘了 log 为空的情况，规则就会静默失效。
node = fresh_node()
append_entries(PORT, term=5, leader_id=9001, entries=make_entries(51, term=5),
               commit_index=50, log_offset=0, prev_log_index=-1, prev_log_term=0)
time.sleep(1.0)                     # 等异步快照线程完成截断
d = node.debug(SHARD)
check("前置：快照已把日志截空，last log 退回快照边界 (5, 50)",
      d and d["log"] == [] and last_log_tuple(d) == (5, 50),
      f"last_log={last_log_tuple(d)} log_len={len(d['log']) if d else None}")

r = request_vote(PORT, term=12, candidate_id=9103, last_log_term=5, last_log_index=49)
check("T1.4c  日志为空时仍按快照边界拒绝落后候选人 (5, 49)",
      r is not None and r.get("vote_granted") is False,
      f"resp={r} —— 日志被快照截空不等于「我没有日志」")
node.kill()


# ── C1 · durability ordering ──────────────────────────────────
section("T1.5  回复「投票通过」之前，这张票必须已经落盘  [I-C1.1]")

node = fresh_node()
r = request_vote(PORT, term=3, candidate_id=9201, last_log_term=0, last_log_index=-1)
granted = r is not None and r.get("vote_granted") is True
node.kill()          # 收到响应后立刻 SIGKILL，不给任何清理机会
node.start()
d = node.debug(SHARD)
check("响应已返回 → SIGKILL → 重启后 (term=3, votedFor=9201) 完好",
      granted and d and d["current_term"] == 3 and d["voted_for"] == 9201,
      f"granted={granted} debug={d}")
node.kill()


section("T1.6  currentTerm 跨重启不回退  [I-C1.2]")

node = fresh_node()
append_entries(PORT, term=9, leader_id=9301, entries=[], commit_index=-1)
d = node.debug(SHARD)
check("前置：从 AppendEntries 观察到 term=9", d and d["current_term"] == 9, f"debug={d}")
node.kill()
node.start()
d = node.debug(SHARD)
check("SIGKILL 重启后 currentTerm >= 9",
      d and d["current_term"] >= 9,
      f"debug={d} —— 回退到 0 会让节点在已投过票的 term 里再投一次")
node.kill()


section("T1.7  候选人必须先持久化 term，再发出 RequestVote  [I-C1.1]")

peer_a, peer_b = FakePeer(PEERS[0]), FakePeer(PEERS[1])
peer_a.start(); peer_b.start()
try:
    node = fresh_node(election_timeout=(0.3, 0.5))
    observed = None
    deadline = time.time() + 8
    while time.time() < deadline:
        terms = [t for t in (peer_a.max_vote_term(), peer_b.max_vote_term()) if t]
        if terms:
            observed = max(terms)
            break
        time.sleep(0.1)

    check("前置：FakePeer 收到了真实节点发出的 RequestVote",
          observed is not None, f"observed_term={observed}")

    node.kill()          # 观察到 RPC 后立刻 SIGKILL
    node.election_timeout = None
    node.start()
    d = node.debug(SHARD)
    check(f"候选人竞选到 term={observed}，重启后持久化的 currentTerm >= {observed}",
          observed is not None and d and d["current_term"] >= observed,
          f"debug={d} —— term 丢失会让节点在同一 term 里重新竞选并重新投票")
    node.kill()
finally:
    peer_a.stop(); peer_b.stop()


# ── 测试基础设施本身 ───────────────────────────────────────────
section("T1.8 / T1.9  内省端点必须 gated，NUM_SHARDS override 不改默认行为")

node = fresh_node(test_mode=False)
status = http_get_status(PORT, f"/debug/raft?shard={SHARD}")
check("T1.8  未开 RAFT_TEST_MODE 时 /debug/raft 返回 404",
      status == 404, f"status={status}")
node.kill()

node = fresh_node(num_shards=1)
d = node.debug(SHARD)
check("T1.9a RAFT_NUM_SHARDS=1 生效（单个 Raft group）",
      d is not None, "num_shards override 未生效")
node.kill()

node = fresh_node(num_shards=None)          # 不设 override → 走默认
r = http_get(PORT, "/debug/raft")
check("T1.9b 不设 override 时默认仍为 len(ALL_PORTS)=3（未偷偷改默认行为）",
      r is not None and r.get("num_shards") == 3, f"resp_num_shards={r and r.get('num_shards')}")
node.kill()


section("T1.10  分片数与持久化状态不一致时必须拒绝启动  [I-C1.4]")

# RAFT_NUM_SHARDS 是 PR1 自己引入的、生产可达的配置路径。如果换个分片数启动时
# 静默丢掉超出范围的分片状态，就等于给 I-C1.3 开了一个后门：
#   3 shards（投过票）→ 1 shard 启动（丢弃 shard 1/2）→ 再回 3 shards
#   → shard 1/2 的 votedFor 凭空消失 → 同一 term 里可以再投一次
_cleanup()
time.sleep(0.3)

node = RealNode(PORT, PEERS, num_shards=3).start()
r = request_vote(PORT, term=7, candidate_id=9401, last_log_term=0, last_log_index=-1, shard=2)
d = node.debug(2)
check("前置：以 3 分片启动并在分片 2 投出一票（term=7, votedFor=9401）",
      r is not None and r.get("vote_granted") is True
      and d and d["current_term"] == 7 and d["voted_for"] == 9401,
      f"resp={r} debug={d}")
node.kill()

mismatched = RealNode(PORT, PEERS, num_shards=1)
rc = mismatched.start_expect_exit(timeout=10)
check("T1.10a 换成 1 分片启动 → 节点拒绝启动（非零退出）",
      rc is not None and rc != 0,
      f"exit_code={rc} —— 静默丢弃分片 1/2 的选票会绕开 I-C1.3")
check("T1.10b 拒绝启动时不对外服务",
      http_get_status(PORT, "/health", timeout=1) is None,
      "拒绝启动的节点不应该还在应答请求")

node = RealNode(PORT, PEERS, num_shards=3).start()
d = node.debug(2)
check("T1.10c fail-closed 没有破坏原状态：改回 3 分片后选票完好",
      d and d["current_term"] == 7 and d["voted_for"] == 9401,
      f"debug={d} —— 拒绝启动必须是只读的，不能顺手改写 hard state")
node.kill()


# ══════════════════════════════════════════════════════════════
section("测试汇总")
_cleanup()

total  = len(results)
passed = sum(1 for _, ok, _ in results if ok)
failed = total - passed

for name, ok, _ in results:
    print(f"  {PASS if ok else FAIL}  {name}")

print(f"\n{'─'*62}")
if failed == 0:
    print(f"  \033[92m🎉 全部通过：{passed}/{total}\033[0m")
else:
    print(f"  \033[91m⚠️  {passed}/{total} 通过，{failed} 失败\033[0m")
    print("\n  失败项：")
    for name, ok, detail in results:
        if not ok:
            print(f"    • {name}\n      {detail}")
print(f"{'─'*62}\n")

sys.exit(0 if failed == 0 else 1)
