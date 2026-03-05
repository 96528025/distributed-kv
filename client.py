"""
分布式 KV 客户端
用来跟集群交互：读数据、写数据、查看所有数据
"""

import urllib.request
import urllib.error
import json
import sys

NODES = [5001, 5002, 5003]


def request(method, port, path, data=None):
    url = f"http://localhost:{port}{path}"
    try:
        if data:
            body = json.dumps(data).encode()
            req = urllib.request.Request(url, data=body, method=method)
            req.add_header("Content-type", "application/json")
        else:
            req = urllib.request.Request(url, method=method)
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError:
        return None


def set_key(key, value, port=5001):
    """写入数据到指定节点"""
    print(f"\n📝 写入 [{key} = {value}] 到节点 {port}")
    result = request("POST", port, "/set", {"key": key, "value": value})
    if result:
        print(f"   ✅ 成功：{result}")
    else:
        print(f"   ❌ 节点 {port} 无响应")


def get_key(key, port=5001):
    """从指定节点读取数据"""
    print(f"\n🔍 从节点 {port} 读取 [{key}]")
    result = request("GET", port, f"/get?key={key}")
    if result:
        print(f"   ✅ 结果：{result}")
    else:
        print(f"   ❌ 节点 {port} 无响应")


def show_all():
    """查看所有节点的数据"""
    print("\n📊 所有节点数据：")
    print("=" * 40)
    for port in NODES:
        result = request("GET", port, "/all")
        if result:
            data = result.get("data", {})
            print(f"节点 {port}: {data}")
        else:
            print(f"节点 {port}: ❌ 不在线")
    print("=" * 40)


# ── 交互式命令行 ───────────────────────────────────────────
def main():
    print("🖥️  分布式 KV 客户端")
    print("命令：set <key> <value> [port]  |  get <key> [port]  |  all  |  quit")

    while True:
        try:
            line = input("\n> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n👋 退出")
            break

        if not line:
            continue

        parts = line.split()
        cmd = parts[0].lower()

        if cmd == "set" and len(parts) >= 3:
            key = parts[1]
            value = parts[2]
            port = int(parts[3]) if len(parts) > 3 else 5001
            set_key(key, value, port)

        elif cmd == "get" and len(parts) >= 2:
            key = parts[1]
            port = int(parts[2]) if len(parts) > 2 else 5001
            get_key(key, port)

        elif cmd == "all":
            show_all()

        elif cmd == "quit":
            print("👋 退出")
            break

        else:
            print("用法：set <key> <value> [port]  |  get <key> [port]  |  all  |  quit")


if __name__ == "__main__":
    main()
