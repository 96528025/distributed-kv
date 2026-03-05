"""
分布式 KV 客户端
"""

import urllib.request
import urllib.error
import json

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
    except urllib.error.HTTPError as e:
        # 403 等错误状态码，读取响应体（里面有 leader 信息）
        return json.loads(e.read())
    except urllib.error.URLError:
        return None


def set_key(key, value, port=5001):
    """写入数据，如果目标节点不是 Leader 自动转发"""
    result = request("POST", port, "/set", {"key": key, "value": value})
    if result is None:
        print(f"   ❌ 节点 {port} 无响应")
        return
    if result.get("error") == "not the leader":
        leader = result.get("leader")
        print(f"   ↪️  节点 {port} 不是 Leader，自动转发到节点 {leader}")
        result = request("POST", leader, "/set", {"key": key, "value": value})
        if result:
            print(f"   ✅ 成功：{result}")
        else:
            print(f"   ❌ Leader 节点 {leader} 无响应")
    else:
        print(f"   ✅ 成功：{result}")


def get_key(key, port=5001):
    print(f"\n🔍 从节点 {port} 读取 [{key}]")
    result = request("GET", port, f"/get?key={key}")
    if result:
        print(f"   ✅ 结果：{result}")
    else:
        print(f"   ❌ 节点 {port} 无响应")


def show_all():
    print("\n📊 所有节点数据：")
    print("=" * 40)
    for port in NODES:
        result = request("GET", port, "/all")
        if result:
            data = result.get("data", {})
            # 查角色
            health = request("GET", port, "/health")
            role = health.get("role", "?") if health else "?"
            role_icon = "👑" if role == "leader" else "🔄"
            print(f"节点 {port} {role_icon} {role}: {data}")
        else:
            print(f"节点 {port}: ❌ 不在线")
    print("=" * 40)


def show_leader():
    print("\n🗳️  当前 Leader：")
    for port in NODES:
        result = request("GET", port, "/leader")
        if result:
            print(f"   节点 {port} 认为 Leader 是：{result.get('leader')}，自己角色：{result.get('my_role')}")
            break
    else:
        print("   ❌ 没有节点在线")


def main():
    print("🖥️  分布式 KV 客户端（选主版）")
    print("命令：set <key> <value> [port]  |  get <key> [port]  |  all  |  leader  |  isolate <port>  |  heal <port>  |  quit")

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
            print(f"\n📝 写入 [{key} = {value}] 到节点 {port}")
            set_key(key, value, port)

        elif cmd == "get" and len(parts) >= 2:
            key = parts[1]
            port = int(parts[2]) if len(parts) > 2 else 5001
            get_key(key, port)

        elif cmd == "all":
            show_all()

        elif cmd == "leader":
            show_leader()

        elif cmd == "isolate" and len(parts) >= 2:
            port = int(parts[1])
            result = request("GET", port, "/isolate")
            print(f"   🔴 节点 {port} 已孤立：{result}")

        elif cmd == "heal" and len(parts) >= 2:
            port = int(parts[1])
            result = request("GET", port, "/heal")
            print(f"   🟢 节点 {port} 已恢复：{result}")

        elif cmd == "quit":
            print("👋 退出")
            break

        else:
            print("用法：set  |  get  |  all  |  leader  |  isolate <port>  |  heal <port>  |  quit")


if __name__ == "__main__":
    main()
