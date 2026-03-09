"""
分布式 KV 节点 — 分片版
核心改动：用一致性哈希决定每个 key 由哪个节点负责
- 写入时：如果这个 key 不属于我，自动转发给正确的节点
- 读取时：如果这个 key 不属于我，自动转发给正确的节点
- 不再有单一 Leader，每个节点都是自己负责的 key 的 Leader
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import urllib.request
import urllib.error
import threading
import os
import hashlib

# ── 启动参数 ──────────────────────────────────────────────
# 用法：python3 node_sharded.py 5001 5002 5003
MY_PORT = int(sys.argv[1])
ALL_PORTS = sorted([int(p) for p in sys.argv[1:]])
PEER_PORTS = [p for p in ALL_PORTS if p != MY_PORT]
DISK_FILE = f"data_sharded_{MY_PORT}.json"

# ── 一致性哈希 ─────────────────────────────────────────────
def get_owner(key):
    """根据 key 的哈希值决定由哪个节点负责"""
    hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
    index = hash_val % len(ALL_PORTS)
    return ALL_PORTS[index]

def i_own(key):
    return get_owner(key) == MY_PORT

# ── 状态 ────────────────────────────────────────────────────
store = {}
store_lock = threading.Lock()


# ── 持久化 ─────────────────────────────────────────────────
def save_to_disk():
    with open(DISK_FILE, "w") as f:
        json.dump(store, f)

def load_from_disk():
    if os.path.exists(DISK_FILE):
        with open(DISK_FILE, "r") as f:
            store.update(json.load(f))
        print(f"  💾 从磁盘恢复了 {len(store)} 条数据")
    else:
        print(f"  💾 没有磁盘文件，从空数据开始")


# ── 转发请求给正确的节点 ────────────────────────────────────
def forward_set(port, key, value):
    try:
        url = f"http://localhost:{port}/set"
        data = json.dumps({"key": key, "value": value}).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-type", "application/json")
        with urllib.request.urlopen(req, timeout=2) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}

def forward_get(port, key):
    try:
        url = f"http://localhost:{port}/get?key={key}"
        with urllib.request.urlopen(url, timeout=2) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


# ── HTTP 处理 ───────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path.startswith("/get"):
            key = self.path.split("=")[-1]
            owner = get_owner(key)

            if i_own(key):
                with store_lock:
                    value = store.get(key)
                if value is None:
                    self._respond(404, {"error": f"key '{key}' not found"})
                else:
                    self._respond(200, {"key": key, "value": value, "served_by": MY_PORT})
            else:
                # 转发给负责这个 key 的节点
                print(f"  ↪️  key '{key}' 不属于我，转发给节点 {owner}")
                result = forward_get(owner, key)
                result["forwarded_by"] = MY_PORT
                self._respond(200, result)

        elif self.path == "/all":
            with store_lock:
                self._respond(200, {"node": MY_PORT, "data": store, "owns_keys": list(store.keys())})

        elif self.path == "/shard_info":
            # 显示分片分配情况
            info = {
                "my_port": MY_PORT,
                "all_nodes": ALL_PORTS,
                "my_keys": list(store.keys()),
                "key_count": len(store),
            }
            self._respond(200, info)

        elif self.path == "/health":
            self._respond(200, {"status": "healthy", "node": MY_PORT})

        elif self.path == "/snapshot":
            with store_lock:
                self._respond(200, {"node": MY_PORT, "data": dict(store)})

        else:
            self._respond(404, {"error": "unknown endpoint"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        if self.path == "/set":
            key = body.get("key")
            value = body.get("value")
            owner = get_owner(key)

            if i_own(key):
                # 这个 key 属于我，直接写入
                with store_lock:
                    store[key] = value
                    save_to_disk()
                print(f"  ✅ 写入: {key} = {value}（我负责这个 key）")
                self._respond(200, {"status": "ok", "key": key, "value": value, "written_to": MY_PORT})
            else:
                # 转发给负责这个 key 的节点
                print(f"  ↪️  key '{key}' 不属于我（属于节点 {owner}），转发...")
                result = forward_set(owner, key, value)
                result["forwarded_by"] = MY_PORT
                self._respond(200, result)

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
    print(f"🚀 分片节点启动：port {MY_PORT}")
    print(f"   集群节点：{ALL_PORTS}")
    load_from_disk()

    # 展示分片分配预览
    sample_keys = ["user:alice", "user:bob", "chat:msg", "session:1", "cache:home", "orders:99"]
    print(f"\n📊 分片示例（key → 负责节点）：")
    for k in sample_keys:
        owner = get_owner(k)
        mine = "← 我负责" if owner == MY_PORT else ""
        print(f"   {k:20s} → 节点 {owner} {mine}")

    print(f"\n✅ 节点 {MY_PORT} 就绪\n")
    HTTPServer(("0.0.0.0", MY_PORT), Handler).serve_forever()
