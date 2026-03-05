"""
分布式 Key-Value 节点
每个节点：
1. 存储数据（内存字典）
2. 对外提供 HTTP 接口（读写数据）
3. 写入时自动同步给其他节点
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import urllib.request
import urllib.error
import threading

# ── 启动参数 ──────────────────────────────────────────────
# 运行方式: python3 node.py <port> <peer1_port> <peer2_port>
# 例如:     python3 node.py 5001 5002 5003
MY_PORT = int(sys.argv[1])
PEER_PORTS = [int(p) for p in sys.argv[2:]]

# ── 数据存储（内存字典） ───────────────────────────────────
store = {}
store_lock = threading.Lock()  # 防止多个请求同时写入，造成数据混乱


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


# ── 启动服务器 ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 节点启动：port {MY_PORT}，peers: {PEER_PORTS}")
    HTTPServer(("0.0.0.0", MY_PORT), Handler).serve_forever()
