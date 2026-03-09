"""
分布式 KV 节点 — Raft 共识算法版

核心实现：
1. Leader Election  — 随机超时 + 多数票当选
2. Heartbeat        — Leader 定期发心跳，防止重新选举
3. Log Replication  — 写入先进日志，多数确认后 commit
4. Term             — 任期编号，过期 Leader 自动下台
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import urllib.request
import urllib.error
import threading
import time
import random
import os

# ── 启动参数 ──────────────────────────────────────────────
MY_PORT = int(sys.argv[1])
PEER_PORTS = [int(p) for p in sys.argv[2:]]
ALL_PORTS = sorted([MY_PORT] + PEER_PORTS)

# ── Raft 状态 ──────────────────────────────────────────────
FOLLOWER  = "follower"
CANDIDATE = "candidate"
LEADER    = "leader"

state_lock = threading.Lock()

current_term = 0       # 当前任期
voted_for = None       # 本任期投票给谁
role = FOLLOWER        # 当前角色
leader_id = None       # 当前 Leader 是谁
votes_received = set() # 收到的票数（candidate 时用）

# 日志和状态机
log = []               # [{"term": int, "key": str, "value": str}]
commit_index = -1      # 最后一条已提交的日志索引
store = {}             # 已提交的 KV 状态机

# 选举超时
last_heartbeat = time.time()
ELECTION_TIMEOUT = random.uniform(1.5, 3.0)  # 随机化避免同时选举
HEARTBEAT_INTERVAL = 0.5


# ── 工具函数 ────────────────────────────────────────────────
def send_rpc(port, path, data):
    try:
        url = f"http://localhost:{port}{path}"
        body = json.dumps(data).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-type", "application/json")
        with urllib.request.urlopen(req, timeout=0.5) as resp:
            return json.loads(resp.read())
    except Exception:
        return None

def majority():
    return len(ALL_PORTS) // 2 + 1


# ── 选举 ───────────────────────────────────────────────────
def start_election():
    global current_term, voted_for, role, votes_received, leader_id

    with state_lock:
        current_term += 1
        role = CANDIDATE
        voted_for = MY_PORT
        votes_received = {MY_PORT}
        term = current_term
        log_len = len(log)
        last_log_term = log[-1]["term"] if log else 0

    print(f"\n🗳️  [Term {term}] 节点 {MY_PORT} 发起选举")

    for port in PEER_PORTS:
        def request_vote(p, t, ll, llt):
            result = send_rpc(p, "/vote", {
                "term": t,
                "candidate_id": MY_PORT,
                "last_log_index": ll - 1,
                "last_log_term": llt,
            })
            if result is None:
                return
            with state_lock:
                global current_term, role, votes_received, leader_id
                if result.get("term", 0) > current_term:
                    # 发现更高任期，退回 Follower
                    current_term = result["term"]
                    role = FOLLOWER
                    voted_for = None
                    return
                if result.get("vote_granted") and role == CANDIDATE and result.get("term") == current_term:
                    votes_received.add(p)
                    print(f"   ✅ 收到节点 {p} 的投票（{len(votes_received)}/{majority()} 票）")
                    if len(votes_received) >= majority():
                        become_leader()

        t = threading.Thread(target=request_vote, args=(port, term, log_len, last_log_term), daemon=True)
        t.start()


def become_leader():
    global role, leader_id
    role = LEADER
    leader_id = MY_PORT
    print(f"\n👑 [Term {current_term}] 节点 {MY_PORT} 当选 Leader！")
    # 立刻发一次心跳宣告主权
    threading.Thread(target=send_heartbeats, daemon=True).start()


# ── 心跳 ───────────────────────────────────────────────────
def send_heartbeats():
    """Leader 发心跳给所有 Follower"""
    with state_lock:
        term = current_term
        ci = commit_index
        entries = list(log)

    for port in PEER_PORTS:
        def hb(p, t, e, c):
            result = send_rpc(p, "/append_entries", {
                "term": t,
                "leader_id": MY_PORT,
                "entries": e,
                "commit_index": c,
            })
            if result and result.get("term", 0) > t:
                with state_lock:
                    global current_term, role
                    current_term = result["term"]
                    role = FOLLOWER

        threading.Thread(target=hb, args=(port, term, entries, ci), daemon=True).start()


def heartbeat_loop():
    """Leader 每隔 HEARTBEAT_INTERVAL 发心跳"""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with state_lock:
            is_leader = (role == LEADER)
        if is_leader:
            send_heartbeats()


def election_timer():
    """Follower/Candidate 超时没收到心跳就发起选举"""
    global last_heartbeat, ELECTION_TIMEOUT
    while True:
        time.sleep(0.1)
        with state_lock:
            is_leader = (role == LEADER)
            elapsed = time.time() - last_heartbeat
            timeout = ELECTION_TIMEOUT

        if not is_leader and elapsed > timeout:
            ELECTION_TIMEOUT = random.uniform(1.5, 3.0)  # 重置超时
            start_election()


# ── HTTP 处理 ───────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path.startswith("/get"):
            key = self.path.split("=")[-1]
            with state_lock:
                value = store.get(key)
            if value is None:
                self._respond(404, {"error": f"key '{key}' not found"})
            else:
                self._respond(200, {"key": key, "value": value, "from_node": MY_PORT})

        elif self.path == "/all":
            with state_lock:
                self._respond(200, {"node": MY_PORT, "data": dict(store)})

        elif self.path == "/health":
            with state_lock:
                r = role
                t = current_term
                l = leader_id
            self._respond(200, {
                "node": MY_PORT,
                "role": r,
                "term": t,
                "leader": l,
                "log_length": len(log),
                "commit_index": commit_index,
            })

        else:
            self._respond(404, {"error": "unknown endpoint"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        if self.path == "/set":
            self._handle_set(body)

        elif self.path == "/vote":
            self._handle_vote(body)

        elif self.path == "/append_entries":
            self._handle_append_entries(body)

        else:
            self._respond(404, {"error": "unknown endpoint"})

    def _handle_set(self, body):
        global role, leader_id
        key = body.get("key")
        value = body.get("value")

        with state_lock:
            r = role
            t = current_term
            l = leader_id

        if r != LEADER:
            self._respond(403, {"error": "not the leader", "leader": l})
            return

        # 写入本地日志
        entry = {"term": t, "key": key, "value": value}
        with state_lock:
            log.append(entry)
            log_index = len(log) - 1

        print(f"\n📝 [Leader] 写入日志[{log_index}]: {key} = {value}")

        # 等待多数节点确认
        acks = [MY_PORT]
        ack_lock = threading.Lock()
        ack_event = threading.Event()

        def replicate_to(port):
            result = send_rpc(port, "/append_entries", {
                "term": t,
                "leader_id": MY_PORT,
                "entries": list(log),
                "commit_index": commit_index,
            })
            if result and result.get("success"):
                with ack_lock:
                    acks.append(port)
                    print(f"  ✅ 节点 {port} 确认（{len(acks)}/{majority()} 节点）")
                    if len(acks) >= majority():
                        ack_event.set()

        threads = [threading.Thread(target=replicate_to, args=(p,), daemon=True) for p in PEER_PORTS]
        for t_ in threads:
            t_.start()

        # 等待多数确认（最多 1 秒）
        ack_event.wait(timeout=1.0)

        if len(acks) >= majority():
            # Commit
            with state_lock:
                global commit_index
                commit_index = log_index
                store[key] = value
            print(f"  🎉 已提交：{key} = {value}")
            self._respond(200, {"status": "ok", "key": key, "value": value, "term": t})
        else:
            self._respond(500, {"error": "failed to reach majority", "acks": len(acks)})

    def _handle_vote(self, body):
        global current_term, voted_for, role, last_heartbeat

        candidate_term = body.get("term", 0)
        candidate_id = body.get("candidate_id")

        with state_lock:
            # 发现更高任期，更新自己
            if candidate_term > current_term:
                current_term = candidate_term
                role = FOLLOWER
                voted_for = None

            # 是否投票：任期要够新，且本任期还没投过票
            vote_granted = (
                candidate_term >= current_term and
                (voted_for is None or voted_for == candidate_id)
            )

            if vote_granted:
                voted_for = candidate_id
                last_heartbeat = time.time()
                print(f"  🗳️  投票给节点 {candidate_id}（Term {candidate_term}）")

            self._respond(200, {
                "term": current_term,
                "vote_granted": vote_granted,
            })

    def _handle_append_entries(self, body):
        global current_term, role, leader_id, last_heartbeat, log, commit_index

        term = body.get("term", 0)
        lid = body.get("leader_id")
        entries = body.get("entries", [])
        new_commit = body.get("commit_index", -1)

        with state_lock:
            if term < current_term:
                self._respond(200, {"term": current_term, "success": False})
                return

            # 合法的 Leader 消息，重置心跳计时器
            last_heartbeat = time.time()

            if term > current_term:
                current_term = term
                voted_for = None

            role = FOLLOWER
            leader_id = lid

            # 同步日志
            if entries:
                log = list(entries)

            # 更新 commit，应用到状态机
            if new_commit > commit_index:
                for i in range(commit_index + 1, min(new_commit + 1, len(log))):
                    store[log[i]["key"]] = log[i]["value"]
                commit_index = new_commit

            self._respond(200, {"term": current_term, "success": True})

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
    print(f"🚀 Raft 节点启动：port {MY_PORT}，peers: {PEER_PORTS}")
    print(f"   选举超时：{ELECTION_TIMEOUT:.2f}s")

    threading.Thread(target=election_timer, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    HTTPServer(("0.0.0.0", MY_PORT), Handler).serve_forever()
