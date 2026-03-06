"""
分布式聊天室服务器
- 用分布式 KV 集群存储消息历史
- 多台 Chat Server 可以同时运行，任何一台挂了不影响
- 新用户连接时自动推送历史记录
"""

import asyncio
import websockets
import urllib.request
import urllib.error
import json
import sys

# ── 配置 ─────────────────────────────────────────────────
MY_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9001
KV_NODES = [5001, 5002, 5003]   # 分布式 KV 集群
HISTORY_KEY = "chat:messages"   # 消息存在 KV 里的 key
MAX_HISTORY = 50                # 最多保存多少条历史

# ── 本地连接管理 ───────────────────────────────────────
clients = {}  # { websocket: 昵称 }


# ── 和 KV 集群通信 ────────────────────────────────────
def kv_request(method, path, data=None):
    """找到 Leader 节点，发请求"""
    for port in KV_NODES:
        try:
            url = f"http://localhost:{port}{path}"
            if data:
                body = json.dumps(data).encode()
                req = urllib.request.Request(url, data=body, method=method)
                req.add_header("Content-type", "application/json")
            else:
                req = urllib.request.Request(url, method=method)
            with urllib.request.urlopen(req, timeout=2) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            result = json.loads(e.read())
            # 被转发到 leader
            if result.get("error") == "not the leader":
                leader = result.get("leader")
                try:
                    url = f"http://localhost:{leader}{path}"
                    body = json.dumps(data).encode()
                    req = urllib.request.Request(url, data=body, method=method)
                    req.add_header("Content-type", "application/json")
                    with urllib.request.urlopen(req, timeout=2) as resp:
                        return json.loads(resp.read())
                except Exception:
                    pass
        except Exception:
            continue
    return None


def save_message(msg):
    """把消息存入 KV 集群"""
    return kv_request("POST", "/lpush", {"key": HISTORY_KEY, "value": msg})


def load_history():
    """从 KV 集群读取历史消息"""
    result = kv_request("GET", f"/lrange?key={HISTORY_KEY}&start=0&end={MAX_HISTORY-1}")
    if result:
        return result.get("items", [])
    return []


# ── WebSocket 处理 ────────────────────────────────────
async def broadcast(message, sender=None):
    for ws in list(clients):
        if ws != sender:
            try:
                await ws.send(message)
            except Exception:
                pass


async def handle(websocket):
    # 第一条消息是昵称
    name = await websocket.recv()
    clients[websocket] = name
    print(f"[+] {name} 加入（Chat Server {MY_PORT}，当前 {len(clients)} 人）")

    # 推送历史记录
    history = load_history()
    if history:
        await websocket.send(f"📜 --- 历史消息（最近 {len(history)} 条）---")
        for msg in history:
            await websocket.send(f"  {msg}")
        await websocket.send(f"📜 --- 历史结束 ---")

    await broadcast(f"🟢 {name} 加入了聊天室", sender=websocket)

    try:
        async for message in websocket:
            full_msg = f"{name}: {message}"
            print(f"  {full_msg}")
            # 存入 KV 集群（持久化）
            save_message(full_msg)
            # 广播给所有人
            await broadcast(full_msg, sender=websocket)
            await websocket.send(f"你: {message}")
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        del clients[websocket]
        print(f"[-] {name} 离开（当前 {len(clients)} 人）")
        await broadcast(f"🔴 {name} 离开了聊天室")


async def main():
    print(f"🚀 Chat Server 启动：ws://localhost:{MY_PORT}")
    print(f"   依赖 KV 集群：{KV_NODES}")
    async with websockets.serve(handle, "localhost", MY_PORT):
        await asyncio.Future()


asyncio.run(main())
