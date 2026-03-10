"""
分布式 KV 节点 — 分片 Raft 版（含快照压缩 + 多 key 事务 2PC + 批量写入）

架构：
- 每个 key 通过一致性哈希分配到某个分片
- 每个分片独立运行一个 Raft 共识组（独立选 Leader、独立日志）
- 所有节点存全量数据（全量副本）
- 多个分片的 Leader 可以在不同节点上，写入并行不冲突

新增功能：
  1. 日志快照压缩：日志超过 SNAPSHOT_THRESHOLD 条时自动生成快照，截断旧日志
  2. 多 key 事务（2PC）：原子地修改多个 key，使用两阶段提交保证一致性
  3. 删除操作（/delete）：通过 Raft 共识删除 key，所有节点同步
  4. 线性化读（/get 路由到 Leader）：读请求转发给分片 Leader，保证读到最新已提交数据
  5. 批量写入（Batching）：每个分片有独立 batch_loop，积攒多个写请求合并成一次
     Raft round，大幅提高高并发下的吞吐量
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

# ── 启动参数 ──────────────────────────────────────────────
MY_PORT    = int(sys.argv[1])
PEER_PORTS = [int(p) for p in sys.argv[2:]]
ALL_PORTS  = sorted([MY_PORT] + PEER_PORTS)
NUM_SHARDS = len(ALL_PORTS)
DISK_FILE  = f"data_raft_sharded_{MY_PORT}.json"

FOLLOWER  = "follower"
CANDIDATE = "candidate"
LEADER    = "leader"

HEARTBEAT_INTERVAL = 0.5
SNAPSHOT_THRESHOLD = 20   # log 超过 20 条就触发快照（小值方便演示）
BATCH_MAX_SIZE     = 20   # 每批最多合并 20 条写请求
BATCH_TIMEOUT      = 0.005  # 最长等待 5ms 积攒批次


# ── 全局 KV 状态机（所有分片共用）────────────────────────
store      = {}
store_lock = threading.Lock()


# ── 工具函数 ────────────────────────────────────────────────
def send_rpc(port, path, data, timeout=0.5):
    """发送 HTTP RPC，失败返回 None（不抛异常）"""
    try:
        url  = f"http://localhost:{port}{path}"
        body = json.dumps(data).encode()
        req  = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-type", "application/json")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return None

def send_get_rpc(port, path, timeout=0.5):
    """发送 HTTP GET RPC，失败返回 None（不抛异常）"""
    try:
        with urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return None

def majority():
    return len(ALL_PORTS) // 2 + 1

def apply_entry(entry):
    """将一条日志条目应用到 store（调用前必须持有 store_lock）"""
    if entry.get("op") == "delete":
        store.pop(entry["key"], None)
    else:
        store[entry["key"]] = entry["value"]


# ── 每个分片的 Raft 状态 ───────────────────────────────────
class ShardRaft:
    def __init__(self, shard_id):
        self.shard_id = shard_id
        self.lock     = threading.Lock()

        # Raft 核心状态
        self.term           = 0
        self.voted_for      = None
        self.role           = FOLLOWER
        self.leader_id      = None
        self.votes_received = set()

        # 日志（log_offset 表示 log[0] 的绝对 index）
        self.log          = []   # [{"term": int, "key": str, "value": str}]
        self.commit_index = -1   # 绝对 index（-1 = 无提交）
        self.log_offset   = 0    # log[i] 的绝对 index = i + log_offset

        # 快照字段
        self.snapshot_index = -1  # 快照最后一条的绝对 index
        self.snapshot_term  = 0   # 快照最后一条的 term

        # 选举计时
        self.last_heartbeat   = time.time()
        self.election_timeout = random.uniform(1.5, 3.0)

        # 2PC 事务字段
        self.pending_txns = {}   # {txn_id: [{"key": ..., "value": ...}]}
        self.key_locks    = {}   # {key: txn_id}
        self.lock_expiry  = {}   # {txn_id: expire_time}

        # 批量写入队列（batch_loop 专用，独立 Condition 锁，不与 shard.lock 混用）
        self.batch_queue = []              # [{"key", "value", "op", "event", "result"}]
        self.batch_cv    = threading.Condition()


# 所有分片的 Raft 实例
shards = [ShardRaft(i) for i in range(NUM_SHARDS)]


# ── 持久化 ─────────────────────────────────────────────────
def save_to_disk():
    """调用前必须持有 store_lock，且不能持有任何 shard.lock"""
    with open(DISK_FILE, "w") as f:
        json.dump(store, f)

def load_from_disk():
    if os.path.exists(DISK_FILE):
        with open(DISK_FILE, "r") as f:
            store.update(json.load(f))
        print(f"  💾 从磁盘恢复了 {len(store)} 条数据")

    # 加载各分片快照（会覆盖磁盘 KV，因为快照版本更新）
    for shard in shards:
        fname = f"snapshot_{MY_PORT}_shard{shard.shard_id}.json"
        if os.path.exists(fname):
            with open(fname, "r") as f:
                snap = json.load(f)
            with store_lock:
                store.update(snap["store"])
            with shard.lock:
                shard.snapshot_index = snap["snapshot_index"]
                shard.snapshot_term  = snap["snapshot_term"]
                shard.log_offset     = snap["log_offset"]
                shard.commit_index   = snap["snapshot_index"]
            print(f"  📸 分片{shard.shard_id} 从快照恢复（snapshot_index={snap['snapshot_index']}）")


# ── 分片逻辑 ───────────────────────────────────────────────
def get_shard(key):
    """根据 key 决定属于哪个分片（一致性哈希）"""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_SHARDS


# ── 快照压缩 ────────────────────────────────────────────────
def maybe_snapshot(shard):
    """commit 后调用，日志超过阈值则生成快照并截断（在 shard.lock 外调用）"""
    with shard.lock:
        if len(shard.log) <= SNAPSHOT_THRESHOLD:
            return

    # 读 store 副本（持 store_lock，不持 shard.lock，避免死锁）
    with store_lock:
        store_copy = dict(store)

    with shard.lock:
        ci  = shard.commit_index
        lo  = shard.log_offset
        cut = ci - lo + 1   # 要截断的条数（已提交的）
        if cut <= 0 or cut > len(shard.log):
            return
        snap_entry = shard.log[cut - 1]

    # 写快照文件（在锁外做 I/O）
    fname = f"snapshot_{MY_PORT}_shard{shard.shard_id}.json"
    snapshot_data = {
        "snapshot_index": ci,
        "snapshot_term":  snap_entry["term"],
        "log_offset":     ci + 1,
        "store":          store_copy,
    }
    with open(fname, "w") as f:
        json.dump(snapshot_data, f)

    # 截断日志（重新持锁，防止并发截断）
    with shard.lock:
        if shard.log_offset != lo:
            return   # 其他线程已经截断
        shard.snapshot_index = ci
        shard.snapshot_term  = snap_entry["term"]
        shard.log            = shard.log[cut:]
        shard.log_offset     = ci + 1

    print(f"  📸 分片{shard.shard_id} 快照已保存"
          f"（snapshot_index={ci}，日志剩余 {len(shard.log)} 条）")


# ── 2PC 锁超时清理 ─────────────────────────────────────────
def txn_cleanup_loop():
    """每秒扫描所有分片，释放过期事务的锁（防止死锁）"""
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
                    print(f"  ⏱️  分片{shard.shard_id}: 事务 {txn_id} 超时，自动释放锁")


# ── 批量写入循环 ────────────────────────────────────────────
def batch_loop(shard):
    """
    每个分片的后台批量写入线程（只有 Leader 节点实际执行写入）。
    等待 batch_queue 有请求或超过 BATCH_TIMEOUT，将积攒的写请求合并成
    一次 Raft AppendEntries，提交后统一通知所有等待的调用者。
    """
    sid = shard.shard_id
    while True:
        # 等待队列非空，最多等 BATCH_TIMEOUT 秒
        with shard.batch_cv:
            shard.batch_cv.wait_for(
                lambda: len(shard.batch_queue) > 0,
                timeout=BATCH_TIMEOUT,
            )
            if not shard.batch_queue:
                continue
            batch = shard.batch_queue[:BATCH_MAX_SIZE]
            del shard.batch_queue[:BATCH_MAX_SIZE]

        # 检查 Leader 身份，并将所有 ops 一次性追加到日志
        with shard.lock:
            if shard.role != LEADER:
                for item in batch:
                    item["result"][0] = (False, "not leader")
                    item["event"].set()
                continue

            t = shard.term
            new_entries = []
            for item in batch:
                entry = {"term": t, "op": item["op"], "key": item["key"]}
                if item["op"] == "set":
                    entry["value"] = item["value"]
                new_entries.append(entry)
                shard.log.append(entry)

            last_abs = shard.log_offset + len(shard.log) - 1   # 最后一条的绝对 index

        print(f"\n📦 [分片{sid} 批量] 合并 {len(batch)} 条写入为一次 Raft round")

        # 并发复制给所有 Follower
        acks      = [MY_PORT]
        ack_lock  = threading.Lock()
        ack_event = threading.Event()
        if len(acks) >= majority():   # 单节点集群直接满足 majority
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
            with shard.lock:
                shard.commit_index = last_abs
            with store_lock:
                for entry in new_entries:
                    apply_entry(entry)
                save_to_disk()
            threading.Thread(target=maybe_snapshot, args=(shard,), daemon=True).start()
            print(f"  🎉 分片{sid} 批量提交成功（{len(batch)} 条）")
            for item in batch:
                item["result"][0] = (True, None)
                item["event"].set()
        else:
            err = f"majority not reached ({len(acks)}/{majority()})"
            for item in batch:
                item["result"][0] = (False, err)
                item["event"].set()


# ── 选举 ───────────────────────────────────────────────────
def start_election(shard):
    """为某个分片发起选举（在 shard.lock 外调用）"""
    with shard.lock:
        shard.term          += 1
        shard.role           = CANDIDATE
        shard.voted_for      = MY_PORT
        shard.votes_received = {MY_PORT}
        term           = shard.term
        last_log_index = shard.log_offset + len(shard.log) - 1  # 绝对 index
        last_log_term  = shard.log[-1]["term"] if shard.log else shard.snapshot_term
        sid            = shard.shard_id

    print(f"\n🗳️  [分片{sid} Term {term}] 节点 {MY_PORT} 发起选举")

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
            with shard.lock:
                if result.get("term", 0) > shard.term:
                    shard.term      = result["term"]
                    shard.role      = FOLLOWER
                    shard.voted_for = None
                    return
                if (result.get("vote_granted") and
                        shard.role == CANDIDATE and
                        result.get("term") == shard.term):
                    shard.votes_received.add(p)
                    cnt = len(shard.votes_received)
                    print(f"   ✅ 分片{sid}: 收到节点 {p} 的投票（{cnt}/{majority()} 票）")
                    if cnt >= majority():
                        _become_leader_locked(shard)

        threading.Thread(
            target=request_vote,
            args=(port, term, last_log_index, last_log_term),
            daemon=True
        ).start()


def _become_leader_locked(shard):
    """在持有 shard.lock 时调用，升为 Leader 并立刻发心跳"""
    shard.role      = LEADER
    shard.leader_id = MY_PORT
    print(f"\n👑 [分片{shard.shard_id} Term {shard.term}] 节点 {MY_PORT} 当选 Leader！")
    threading.Thread(target=send_heartbeats, args=(shard,), daemon=True).start()


# ── 心跳 ───────────────────────────────────────────────────
def send_heartbeats(shard):
    """Leader 发心跳给所有 Follower（在 shard.lock 外调用）"""
    with shard.lock:
        term      = shard.term
        ci        = shard.commit_index
        entries   = list(shard.log)
        lo        = shard.log_offset
        snap_term = shard.snapshot_term   # 与 log_offset 同一锁块读，保证一致
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
                "prev_log_index": offset - 1,   # 当前日志窗口之前的最后一条绝对 index
                "prev_log_term":  pt,            # 该条目的 term（来自快照）
            })
            if result and result.get("term", 0) > t:
                with shard.lock:
                    if result["term"] > shard.term:
                        shard.term = result["term"]
                        shard.role = FOLLOWER

        threading.Thread(target=hb, args=(port, term, entries, ci, lo, snap_term), daemon=True).start()


def heartbeat_loop():
    """Leader 每隔 HEARTBEAT_INTERVAL 为所有分片发心跳"""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        for shard in shards:
            with shard.lock:
                is_leader = (shard.role == LEADER)
            if is_leader:
                threading.Thread(target=send_heartbeats, args=(shard,), daemon=True).start()


def election_timer():
    """遍历所有分片，超时则发起选举"""
    while True:
        time.sleep(0.1)
        for shard in shards:
            with shard.lock:
                is_leader = (shard.role == LEADER)
                elapsed   = time.time() - shard.last_heartbeat
                timeout   = shard.election_timeout

            if not is_leader and elapsed > timeout:
                with shard.lock:
                    shard.election_timeout = random.uniform(1.5, 3.0)
                start_election(shard)


# ── HTTP 处理 ───────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path.startswith("/get"):
            key   = self.path.split("=")[-1]
            sid   = get_shard(key)
            shard = shards[sid]

            with shard.lock:
                role   = shard.role
                leader = shard.leader_id

            # 非 Leader → 转发给 Leader（线性化读，保证读到最新提交）
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

            # 是 Leader → 直接读（保证看到所有已提交数据）
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

        else:
            self._respond(404, {"error": "unknown endpoint"})

    def do_POST(self):
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

    # ── 核心 Raft 操作（Leader 调用）─────────────────────────
    def _do_raft_op(self, shard, key, value=None, op="set"):
        """
        在当前节点（必须是 shard 的 Leader）执行一次 Raft 操作。
        op="set"    → 写入 key=value
        op="delete" → 删除 key
        返回 (True, None) 成功，或 (False, error_msg) 失败。
        调用前无需持任何锁。
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
            log_index = len(shard.log) - 1 + shard.log_offset   # 绝对 index

        label = f"{key} = {value}" if op == "set" else f"DELETE {key}"
        print(f"\n📝 [分片{sid} Leader] 写入日志[{log_index}]: {label}")

        acks      = [MY_PORT]
        ack_lock  = threading.Lock()
        ack_event = threading.Event()

        def replicate_to(port):
            with shard.lock:
                rep_term      = shard.term
                rep_entries   = list(shard.log)
                rep_ci        = shard.commit_index
                rep_lo        = shard.log_offset
                rep_snap_term = shard.snapshot_term   # 与 rep_lo 同一锁块读
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
                    print(f"  ✅ 分片{sid}: 节点 {port} 确认（{len(acks)}/{majority()} 节点）")
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
            with shard.lock:
                shard.commit_index = log_index
            with store_lock:
                apply_entry(entry)
                save_to_disk()
            print(f"  🎉 分片{sid} 已提交：{label}")
            # 异步触发快照（不阻塞当前请求）
            threading.Thread(target=maybe_snapshot, args=(shard,), daemon=True).start()
            return True, None
        else:
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
                print(f"\n↪️  分片{sid} 的 Leader 是 {l}，转发...")
                result = send_rpc(l, "/set", {"key": key, "value": value})
                if result:
                    result["forwarded_by"] = MY_PORT
                    self._respond(200, result)
                else:
                    self._respond(503, {"error": "leader unreachable", "shard": sid})
            else:
                self._respond(503, {"error": "no leader yet for this shard", "shard": sid})
            return

        # Leader 路径：提交到批量队列，等待 batch_loop 统一处理
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
                print(f"\n↪️  分片{sid} 的 Leader 是 {l}，转发删除...")
                result = send_rpc(l, "/delete", {"key": key})
                if result:
                    result["forwarded_by"] = MY_PORT
                    self._respond(200, result)
                else:
                    self._respond(503, {"error": "leader unreachable", "shard": sid})
            else:
                self._respond(503, {"error": "no leader yet for this shard", "shard": sid})
            return

        # Leader 路径：提交到批量队列，等待 batch_loop 统一处理
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
        sid   = body.get("shard_id", 0)
        shard = shards[sid]

        candidate_term = body.get("term", 0)
        candidate_id   = body.get("candidate_id")

        with shard.lock:
            if candidate_term > shard.term:
                shard.term      = candidate_term
                shard.role      = FOLLOWER
                shard.voted_for = None

            vote_granted = (
                candidate_term >= shard.term and
                (shard.voted_for is None or shard.voted_for == candidate_id)
            )

            if vote_granted:
                shard.voted_for      = candidate_id
                shard.last_heartbeat = time.time()
                print(f"  🗳️  分片{sid}: 投票给节点 {candidate_id}（Term {candidate_term}）")

            self._respond(200, {"term": shard.term, "vote_granted": vote_granted})

    def _handle_append_entries(self, body):
        sid        = body.get("shard_id", 0)
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

        with shard.lock:
            if term < shard.term:
                self._respond(200, {"term": shard.term, "success": False})
                return

            shard.last_heartbeat = time.time()
            if term > shard.term:
                shard.term      = term
                shard.voted_for = None

            shard.role      = FOLLOWER
            shard.leader_id = lid

            # ── prevLogIndex 一致性检查 ─────────────────────────
            # 在接受新条目前，验证"接入点"处的日志是否吻合
            if entries and prev_log_index >= 0:
                if prev_log_index >= shard.log_offset:
                    # prev entry 在当前日志窗口内
                    rel_i = prev_log_index - shard.log_offset
                    if rel_i < len(shard.log):
                        if shard.log[rel_i]["term"] != prev_log_term:
                            # 冲突：该位置 term 不一致，截断到冲突点并拒绝
                            shard.log = shard.log[:rel_i]
                            print(f"  ⚠️  分片{sid}: prevLog 冲突（index={prev_log_index}），"
                                  f"截断至 rel_i={rel_i}")
                            self._respond(200, {"term": shard.term, "success": False,
                                               "conflict_index": prev_log_index})
                            return
                    # else: prev entry 超出当前日志，由下方 need_snapshot 逻辑处理
                # prev_log_index < log_offset：在快照范围内，信任快照 term 一致，跳过

            # 同步日志
            if entries:
                if leader_lo > shard.log_offset + len(shard.log):
                    # 落后太多，需要从 Leader 拉取快照
                    need_snapshot = True
                    snap_leader   = lid
                else:
                    shard.log        = list(entries)
                    shard.log_offset = leader_lo

            # 收集需要 apply 的条目（绝对 index 转换为相对 index）
            if not need_snapshot and new_commit > shard.commit_index:
                start_abs = shard.commit_index + 1
                end_abs   = min(new_commit + 1, len(shard.log) + shard.log_offset)
                for abs_i in range(start_abs, end_abs):
                    rel_i = abs_i - shard.log_offset
                    if 0 <= rel_i < len(shard.log):
                        to_apply.append(shard.log[rel_i])
                shard.commit_index = new_commit

            resp_term = shard.term

        # 在 shard.lock 外执行 I/O
        if need_snapshot:
            snap = send_rpc(snap_leader, "/install_snapshot",
                            {"shard_id": sid, "requester": MY_PORT},
                            timeout=2.0)
            if snap and "snapshot_index" in snap:
                with store_lock:
                    store.update(snap["store"])
                    save_to_disk()
                with shard.lock:
                    shard.snapshot_index = snap["snapshot_index"]
                    shard.snapshot_term  = snap["snapshot_term"]
                    shard.log_offset     = snap["log_offset"]
                    shard.commit_index   = snap["snapshot_index"]
                    shard.log            = snap.get("tail_log", [])
                # 写快照到本地磁盘
                fname = f"snapshot_{MY_PORT}_shard{sid}.json"
                with open(fname, "w") as f:
                    json.dump({
                        "snapshot_index": snap["snapshot_index"],
                        "snapshot_term":  snap["snapshot_term"],
                        "log_offset":     snap["log_offset"],
                        "store":          snap["store"],
                    }, f)
                print(f"  📥 分片{sid} 从 Leader {snap_leader} 安装快照"
                      f"（snapshot_index={snap['snapshot_index']}）")
        elif to_apply:
            with store_lock:
                for entry in to_apply:
                    apply_entry(entry)
                save_to_disk()
            # Follower 也触发快照检查（日志超阈值时生成快照文件，重启不依赖磁盘文件）
            threading.Thread(target=maybe_snapshot, args=(shard,), daemon=True).start()

        self._respond(200, {"term": resp_term, "success": True})

    def _handle_install_snapshot(self, body):
        """Follower 请求时，Leader 返回当前快照内容 + 快照后的日志"""
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

    # ── 多 key 事务（2PC）────────────────────────────────────
    def _handle_txn(self, body):
        """协调者：执行两阶段提交"""
        ops = body.get("ops", [])
        if not ops:
            self._respond(400, {"error": "ops is empty"})
            return

        # 按分片分组
        shard_ops = {}
        for op in ops:
            sid = get_shard(op["key"])
            shard_ops.setdefault(sid, []).append(op)

        txn_id = f"{MY_PORT}-{time.time()}"

        # 找每个分片的 Leader
        shard_leaders = {}
        for sid in shard_ops:
            with shards[sid].lock:
                leader = shards[sid].leader_id
            if leader is None:
                self._respond(503, {"error": f"no leader for shard {sid}", "txn_id": txn_id})
                return
            shard_leaders[sid] = leader

        # ── Phase 1: Prepare ──────────────────────────────
        prepare_results = {}
        prep_lock       = threading.Lock()

        def do_prepare(sid, leader, ops_list):
            result = send_rpc(leader, "/txn_prepare", {
                "txn_id":   txn_id,
                "shard_id": sid,
                "ops":      ops_list,
            })
            with prep_lock:
                prepare_results[sid] = result or {"status": "unreachable"}

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
            t.join(timeout=1.0)

        all_ready = all(
            prepare_results.get(sid, {}).get("status") == "ready"
            for sid in shard_ops
        )

        # ── Phase 2: Commit 或 Abort ──────────────────────
        action = "/txn_commit" if all_ready else "/txn_abort"

        def do_action(leader, sid):
            send_rpc(leader, action, {"txn_id": txn_id, "shard_id": sid}, timeout=2.0)

        action_threads = [
            threading.Thread(target=do_action, args=(shard_leaders[sid], sid), daemon=True)
            for sid in shard_ops
        ]
        for t in action_threads:
            t.start()
        for t in action_threads:
            t.join(timeout=3.0)

        if all_ready:
            self._respond(200, {"status": "ok", "txn_id": txn_id})
        else:
            failed = [sid for sid in shard_ops
                      if prepare_results.get(sid, {}).get("status") != "ready"]
            self._respond(200, {
                "status": "aborted",
                "txn_id": txn_id,
                "reason": f"prepare failed for shards {failed}",
                "details": {str(sid): prepare_results.get(sid) for sid in failed},
            })

    def _handle_txn_prepare(self, body):
        """分片 Leader：锁定 key，暂存写入意图"""
        txn_id = body.get("txn_id")
        sid    = body.get("shard_id")
        ops    = body.get("ops", [])
        shard  = shards[sid]

        with shard.lock:
            if shard.role != LEADER:
                self._respond(200, {"status": "not_leader"})
                return

            # 检查 key 是否已被其他事务锁定
            for op in ops:
                key = op["key"]
                if key in shard.key_locks and shard.key_locks[key] != txn_id:
                    self._respond(200, {
                        "status":    "locked",
                        "key":       key,
                        "locked_by": shard.key_locks[key],
                    })
                    return

            # 锁定 key，暂存
            for op in ops:
                shard.key_locks[op["key"]] = txn_id
            shard.pending_txns[txn_id] = ops
            shard.lock_expiry[txn_id]  = time.time() + 10

        print(f"  🔒 分片{sid}: 事务 {txn_id} PREPARE"
              f"（keys={[op['key'] for op in ops]}）")
        self._respond(200, {"status": "ready"})

    def _handle_txn_commit(self, body):
        """分片 Leader：提交事务，通过 Raft 写入每个 key"""
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

        print(f"  ✅ 分片{sid}: 事务 {txn_id} COMMIT（{len(ops)} 条写入）")
        for op in ops:
            success, err = self._do_raft_op(shard, op["key"], op.get("value"), op.get("op", "set"))
            if not success:
                self._respond(500, {"status": "commit_failed", "error": err, "key": op["key"]})
                return

        self._respond(200, {"status": "ok"})

    def _handle_txn_abort(self, body):
        """分片 Leader：中止事务，释放锁"""
        txn_id = body.get("txn_id")
        sid    = body.get("shard_id")
        shard  = shards[sid]

        with shard.lock:
            ops = shard.pending_txns.pop(txn_id, [])
            for op in ops:
                shard.key_locks.pop(op["key"], None)
            shard.lock_expiry.pop(txn_id, None)

        print(f"  ❌ 分片{sid}: 事务 {txn_id} ABORT")
        self._respond(200, {"status": "ok"})

    def _respond(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, f, *a):
        pass


# ── 启动 ────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 分片 Raft 节点启动：port {MY_PORT}")
    print(f"   集群：{ALL_PORTS}，分片数：{NUM_SHARDS}")
    load_from_disk()

    print(f"\n📊 分片规划（每个分片独立选 Leader）：")
    for s in range(NUM_SHARDS):
        print(f"   分片 {s}: Raft Group = {ALL_PORTS}（待选举）")

    print(f"\n✅ 节点 {MY_PORT} 就绪\n")

    threading.Thread(target=election_timer,   daemon=True).start()
    threading.Thread(target=heartbeat_loop,   daemon=True).start()
    threading.Thread(target=txn_cleanup_loop, daemon=True).start()
    for shard in shards:
        threading.Thread(target=batch_loop, args=(shard,), daemon=True).start()

    class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
        """每个请求在独立线程处理，避免协调者向自身发 RPC 时死锁"""
        daemon_threads = True

    ThreadedHTTPServer(("0.0.0.0", MY_PORT), Handler).serve_forever()
