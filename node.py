"""
分布式 Key-Value 节点
每个节点：
1. 存储数据（内存字典）
2. 对外提供 HTTP 接口（读写数据）
3. 写入时自动同步给其他节点
4. 启动时从其他节点拉取全量数据（快照恢复）
5. 每次写入同时持久化到磁盘（重启不丢数据）
6. 后台选主：存活节点中端口最小的是 Leader，只有 Leader 接受写入
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import urllib.request
import urllib.error
import threading
import os
import time

# ── 启动参数 ──────────────────────────────────────────────
MY_PORT = int(sys.argv[1])
PEER_PORTS = [int(p) for p in sys.argv[2:]]
ALL_PORTS = sorted([MY_PORT] + PEER_PORTS)

# ── 状态变量 ───────────────────────────────────────────────
store = {}
store_lock = threading.Lock()
DISK_FILE = f"data_{MY_PORT}.json"
isolated = False
leader_port = None  # 当前 Leader 是谁


# ── 持久化：读写磁盘 ───────────────────────────────────────
def save_to_disk():
    with open(DISK_FILE, "w") as f:
        json.dump(store, f)

def load_from_disk():
    if os.path.exists(DISK_FILE):
        with open(DISK_FILE, "r") as f:
            store.update(json.load(f))
        print(f"  💾 从磁盘恢复了 {len(store)} 条数据：{store}")
    else:
        print(f"  💾 没有磁盘文件，从空数据开始")


# ── 选主：存活节点中端口最小的当 Leader ────────────────────
def elect_leader():
    """检查所有节点存活情况，选出 Leader"""
    global leader_port
    alive = []
    for port in ALL_PORTS:
        try:
            url = f"http://localhost:{port}/health"
            urllib.request.urlopen(url, timeout=1)
            alive.append(port)
        except Exception:
            pass

    new_leader = min(alive) if alive else None

    if new_leader != leader_port:
        old = leader_port
        leader_port = new_leader
        role = "👑 LEADER" if leader_port == MY_PORT else "🔄 FOLLOWER"
        print(f"\n🗳️  选主结果：节点 {leader_port} 当选 Leader（原来：{old}）")
        print(f"   本节点 {MY_PORT} 角色：{role}")

def leader_election_loop():
    """后台线程，每 5 秒重新选主"""
    while True:
        time.sleep(5)
        elect_leader()


# ── 同步给其他节点 ─────────────────────────────────────────
def replicate(key, value):
    if isolated:
        print(f"  🚫 节点 {MY_PORT} 已孤立，跳过所有同步")
        return
    for port in PEER_PORTS:
        try:
            url = f"http://localhost:{port}/internal"
            data = json.dumps({"key": key, "value": value}).encode()
            req = urllib.request.Request(url, data=data, method="POST")
            req.add_header("Content-type", "application/json")
            urllib.request.urlopen(req, timeout=2)
            print(f"  ✅ 同步到节点 {port} 成功")
        except urllib.error.URLError:
            print(f"  ⚠️  节点 {port} 不在线，跳过同步")


# ── HTTP 请求处理 ──────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        global isolated

        if self.path.startswith("/get"):
            key = self.path.split("=")[-1]
            with store_lock:
                value = store.get(key)
            if value is None:
                self._respond(404, {"error": f"key '{key}' not found"})
            else:
                self._respond(200, {"key": key, "value": value, "from_node": MY_PORT})

        elif self.path == "/all":
            with store_lock:
                self._respond(200, {"node": MY_PORT, "data": store})

        elif self.path == "/snapshot":
            with store_lock:
                self._respond(200, {"node": MY_PORT, "data": dict(store)})

        elif self.path == "/health":
            role = "leader" if MY_PORT == leader_port else "follower"
            self._respond(200, {"status": "healthy", "node": MY_PORT, "role": role})

        elif self.path == "/leader":
            role = "leader" if MY_PORT == leader_port else "follower"
            self._respond(200, {
                "leader": leader_port,
                "my_port": MY_PORT,
                "my_role": role
            })

        elif self.path == "/isolate":
            isolated = True
            print(f"\n🔴 节点 {MY_PORT} 进入孤立模式（模拟脑裂）")
            self._respond(200, {"status": "isolated", "node": MY_PORT})

        elif self.path == "/heal":
            isolated = False
            print(f"\n🟢 节点 {MY_PORT} 恢复正常（脑裂解除）")
            self._respond(200, {"status": "healed", "node": MY_PORT})

        else:
            self._respond(404, {"error": "unknown endpoint"})

    def do_POST(self):
        global isolated
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        if self.path == "/set":
            key = body.get("key")
            value = body.get("value")

            # 只有 Leader 接受写入
            if leader_port is not None and MY_PORT != leader_port:
                print(f"\n🚫 我不是 Leader，拒绝写入，请发给节点 {leader_port}")
                self._respond(403, {
                    "error": "not the leader",
                    "leader": leader_port
                })
                return

            with store_lock:
                store[key] = value
                save_to_disk()
            print(f"\n📝 [Leader] 写入: {key} = {value}")
            print(f"🔄 开始同步...")
            replicate(key, value)

            self._respond(200, {
                "status": "ok",
                "key": key,
                "value": value,
                "written_to": MY_PORT
            })

        elif self.path == "/internal":
            if isolated:
                print(f"\n🚫 拒绝同步（孤立状态）")
                self._respond(503, {"error": "node is isolated"})
                return
            key = body.get("key")
            value = body.get("value")
            with store_lock:
                store[key] = value
                save_to_disk()
            print(f"\n📨 收到同步: {key} = {value}")
            self._respond(200, {"status": "ok"})

        else:
            self._respond(404, {"error": "unknown endpoint"})

    def _respond(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, f, *a):
        pass


# ── 启动时从其他节点拉取全量数据 ──────────────────────────
def recover_from_peers():
    print(f"🔍 尝试从其他节点恢复数据...")
    recovered = False
    for port in PEER_PORTS:
        try:
            url = f"http://localhost:{port}/snapshot"
            with urllib.request.urlopen(url, timeout=2) as resp:
                result = json.loads(resp.read())
                peer_data = result.get("data", {})
                if peer_data:
                    with store_lock:
                        store.update(peer_data)
                    print(f"  ✅ 从节点 {port} 恢复了 {len(peer_data)} 条数据：{peer_data}")
                    recovered = True
                    break
        except urllib.error.URLError:
            print(f"  ⚠️  节点 {port} 不在线，跳过")
    if not recovered:
        print(f"  ℹ️  没有在线节点，从空数据开始")


# ── 启动服务器 ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 节点启动：port {MY_PORT}，peers: {PEER_PORTS}")
    load_from_disk()
    recover_from_peers()
    elect_leader()  # 启动时立刻选一次主
    print(f"✅ 节点 {MY_PORT} 就绪，当前数据：{store}")

    # 后台线程持续选主
    t = threading.Thread(target=leader_election_loop, daemon=True)
    t.start()

    HTTPServer(("0.0.0.0", MY_PORT), Handler).serve_forever()
