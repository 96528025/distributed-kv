"""
分布式 Key-Value 节点
每个节点：
1. 存储数据（内存字典）
2. 对外提供 HTTP 接口（读写数据）
3. 写入时自动同步给其他节点
4. 启动时从其他节点拉取全量数据（快照恢复）
5. 每次写入同时持久化到磁盘（重启不丢数据）
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import urllib.request
import urllib.error
import threading
import os

# ── 启动参数 ──────────────────────────────────────────────
# 运行方式: python3 node.py <port> <peer1_port> <peer2_port>
# 例如:     python3 node.py 5001 5002 5003
MY_PORT = int(sys.argv[1])
PEER_PORTS = [int(p) for p in sys.argv[2:]]

# ── 数据存储（内存字典） ───────────────────────────────────
store = {}
store_lock = threading.Lock()  # 防止多个请求同时写入，造成数据混乱
DISK_FILE = f"data_{MY_PORT}.json"  # 每个节点有自己的文件


# ── 持久化：读写磁盘 ───────────────────────────────────────
def save_to_disk():
    """把当前 store 写入 JSON 文件"""
    with open(DISK_FILE, "w") as f:
        json.dump(store, f)

def load_from_disk():
    """从 JSON 文件恢复 store（启动时调用）"""
    if os.path.exists(DISK_FILE):
        with open(DISK_FILE, "r") as f:
            store.update(json.load(f))
        print(f"  💾 从磁盘恢复了 {len(store)} 条数据：{store}")
    else:
        print(f"  💾 没有磁盘文件，从空数据开始")


# ── 同步给其他节点 ─────────────────────────────────────────
def replicate(key, value):
    """把写入操作同步给所有其他节点"""
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

    # GET /get?key=name  → 读取数据
    # GET /all           → 查看所有数据
    def do_GET(self):
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
            # 把自己所有数据返回给请求方（用于新节点恢复）
            with store_lock:
                self._respond(200, {"node": MY_PORT, "data": dict(store)})

        elif self.path == "/health":
            self._respond(200, {"status": "healthy", "node": MY_PORT})

        else:
            self._respond(404, {"error": "unknown endpoint"})

    # POST /set          → 写入数据（同时同步给其他节点）
    # POST /internal     → 接收其他节点同步过来的数据
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        if self.path == "/set":
            key = body.get("key")
            value = body.get("value")

            # 写入本节点
            with store_lock:
                store[key] = value
                save_to_disk()
            print(f"\n📝 写入: {key} = {value}")

            # 同步给其他节点
            print(f"🔄 开始同步...")
            replicate(key, value)

            self._respond(200, {
                "status": "ok",
                "key": key,
                "value": value,
                "written_to": MY_PORT
            })

        elif self.path == "/internal":
            # 来自其他节点的同步请求，只写入，不再转发
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
        pass  # 关掉默认日志，用我们自己的 print


# ── 启动时从其他节点拉取全量数据 ──────────────────────────
def recover_from_peers():
    """启动时向所有在线节点请求快照，合并到本地"""
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
                        store.update(peer_data)  # 合并数据
                    print(f"  ✅ 从节点 {port} 恢复了 {len(peer_data)} 条数据：{peer_data}")
                    recovered = True
                    break  # 拿到一份完整数据就够了
        except urllib.error.URLError:
            print(f"  ⚠️  节点 {port} 不在线，跳过")

    if not recovered:
        print(f"  ℹ️  没有在线节点，从空数据开始")


# ── 启动服务器 ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 节点启动：port {MY_PORT}，peers: {PEER_PORTS}")
    load_from_disk()      # 先从磁盘恢复
    recover_from_peers()  # 再从其他节点补充更新的数据
    print(f"✅ 节点 {MY_PORT} 就绪，当前数据：{store}")
    HTTPServer(("0.0.0.0", MY_PORT), Handler).serve_forever()
