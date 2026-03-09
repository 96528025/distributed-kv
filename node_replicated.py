"""
分布式 KV 节点 — 分片 + 副本版

架构：
- 每个 key 通过一致性哈希分配到某个分片
- 每个分片的数据存在所有节点上（全量副本）
- 每个分片有一个 primary 负责写入
- primary 挂了，下一个存活节点自动接管

分片分配（3节点为例）：
  分片 0: primary=5001, 候补=[5002, 5003]
  分片 1: primary=5002, 候补=[5001, 5003]
  分片 2: primary=5003, 候补=[5001, 5002]
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import urllib.request
import urllib.error
import threading
import hashlib
import time
import os

# ── 启动参数 ──────────────────────────────────────────────
MY_PORT = int(sys.argv[1])
PEER_PORTS = [int(p) for p in sys.argv[2:]]
ALL_PORTS = sorted([MY_PORT] + PEER_PORTS)
NUM_SHARDS = len(ALL_PORTS)
DISK_FILE = f"data_replicated_{MY_PORT}.json"

# ── 状态 ────────────────────────────────────────────────────
store = {}           # 所有分片的数据都存在这里
store_lock = threading.Lock()
alive_nodes = set(ALL_PORTS)   # 当前存活的节点
alive_lock = threading.Lock()


# ── 持久化 ─────────────────────────────────────────────────
def save_to_disk():
    with open(DISK_FILE, "w") as f:
        json.dump(store, f)

def load_from_disk():
    if os.path.exists(DISK_FILE):
        with open(DISK_FILE, "r") as f:
            store.update(json.load(f))
        print(f"  💾 从磁盘恢复了 {len(store)} 条数据")


# ── 分片逻辑 ───────────────────────────────────────────────
def get_shard(key):
    """根据 key 决定属于哪个分片"""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_SHARDS

def get_primary(shard_id):
    """
    获取某个分片当前的 primary。
    优先使用初始分配的节点，挂了就用下一个存活节点。
    """
    with alive_lock:
        alive = set(alive_nodes)
    # 分片 i 的候选列表：从 ALL_PORTS[i] 开始轮转
    for i in range(len(ALL_PORTS)):
        candidate = ALL_PORTS[(shard_id + i) % len(ALL_PORTS)]
        if candidate in alive:
            return candidate
    return None

def i_am_primary(key):
    shard = get_shard(key)
    return get_primary(shard) == MY_PORT


# ── 健康检查：后台监测节点存活 ──────────────────────────────
def health_check_loop():
    while True:
        time.sleep(2)
        # 先在锁外收集 ping 结果，避免持锁期间发 HTTP 请求导致死锁
        new_alive = set()
        for port in ALL_PORTS:
            try:
                url = f"http://localhost:{port}/ping"
                urllib.request.urlopen(url, timeout=0.5)
                new_alive.add(port)
            except Exception:
                pass

        # 再加锁更新状态
        with alive_lock:
            old_alive = set(alive_nodes)
            alive_nodes.clear()
            alive_nodes.update(new_alive)

        # 打印变化（在锁外打印，避免持锁太久）
        went_down = old_alive - new_alive
        came_up = new_alive - old_alive
        for p in went_down:
            print(f"\n💀 节点 {p} 下线，重新分配分片 primary")
            _print_shard_layout()
        for p in came_up:
            if p != MY_PORT:
                print(f"\n🟢 节点 {p} 恢复上线")
                _print_shard_layout()

def _print_shard_layout():
    for s in range(NUM_SHARDS):
        primary = get_primary(s)
        marker = "← 我" if primary == MY_PORT else ""
        print(f"   分片 {s}: primary={primary} {marker}")


# ── 转发请求 ───────────────────────────────────────────────
def forward_to(port, path, data):
    try:
        url = f"http://localhost:{port}{path}"
        body = json.dumps(data).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-type", "application/json")
        with urllib.request.urlopen(req, timeout=2) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}

def replicate_to_peers(key, value):
    """把写入同步给其他所有节点"""
    for port in PEER_PORTS:
        try:
            url = f"http://localhost:{port}/replicate"
            data = json.dumps({"key": key, "value": value}).encode()
            req = urllib.request.Request(url, data=data, method="POST")
            req.add_header("Content-type", "application/json")
            urllib.request.urlopen(req, timeout=1)
            print(f"  ✅ 副本同步到节点 {port}")
        except Exception:
            print(f"  ⚠️  节点 {port} 不在线，跳过副本同步")


# ── HTTP 处理 ───────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/ping":
            self._respond(200, {"ok": True})

        elif self.path.startswith("/get"):
            key = self.path.split("=")[-1]
            # 读取可以从任意节点（都有副本）
            with store_lock:
                value = store.get(key)
            if value is None:
                self._respond(404, {"error": f"key '{key}' not found"})
            else:
                shard = get_shard(key)
                primary = get_primary(shard)
                self._respond(200, {
                    "key": key,
                    "value": value,
                    "from_node": MY_PORT,
                    "shard": shard,
                    "shard_primary": primary,
                })

        elif self.path == "/all":
            with store_lock:
                self._respond(200, {"node": MY_PORT, "data": dict(store)})

        elif self.path == "/shards":
            # 显示当前分片分配
            layout = {}
            for s in range(NUM_SHARDS):
                layout[s] = {
                    "primary": get_primary(s),
                    "all_replicas": ALL_PORTS,
                }
            with alive_lock:
                self._respond(200, {
                    "node": MY_PORT,
                    "alive_nodes": list(alive_nodes),
                    "shards": layout,
                })

        elif self.path == "/health":
            with alive_lock:
                alive = list(alive_nodes)
            self._respond(200, {"node": MY_PORT, "alive": alive})

        else:
            self._respond(404, {"error": "unknown endpoint"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        if self.path == "/set":
            key = body.get("key")
            value = body.get("value")
            shard = get_shard(key)
            primary = get_primary(shard)

            if primary == MY_PORT:
                # 我是这个分片的 primary，直接写入并同步
                with store_lock:
                    store[key] = value
                    save_to_disk()
                print(f"\n✅ 写入分片{shard} [primary]: {key} = {value}")
                replicate_to_peers(key, value)
                self._respond(200, {
                    "status": "ok",
                    "key": key,
                    "value": value,
                    "shard": shard,
                    "written_to": MY_PORT,
                })
            else:
                # 转发给正确的 primary
                print(f"\n↪️  分片{shard} 的 primary 是 {primary}，转发...")
                result = forward_to(primary, "/set", {"key": key, "value": value})
                result["forwarded_by"] = MY_PORT
                self._respond(200, result)

        elif self.path == "/replicate":
            # 接收 primary 发来的副本同步
            key = body.get("key")
            value = body.get("value")
            with store_lock:
                store[key] = value
                save_to_disk()
            print(f"\n📨 副本同步: {key} = {value}")
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


# ── 启动 ────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 分片+副本节点启动：port {MY_PORT}")
    print(f"   集群：{ALL_PORTS}，分片数：{NUM_SHARDS}")
    load_from_disk()

    # 初始分片分配预览
    print(f"\n📊 初始分片分配：")
    for s in range(NUM_SHARDS):
        primary = ALL_PORTS[s % len(ALL_PORTS)]
        marker = "← 我" if primary == MY_PORT else ""
        print(f"   分片 {s}: primary={primary}, 副本={ALL_PORTS} {marker}")

    print(f"\n✅ 节点 {MY_PORT} 就绪\n")

    # 后台健康检查
    threading.Thread(target=health_check_loop, daemon=True).start()

    HTTPServer(("0.0.0.0", MY_PORT), Handler).serve_forever()
