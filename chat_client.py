"""
分布式聊天室客户端
- 支持连接任意一台 Chat Server
- 断线后自动重连其他服务器
"""

import asyncio
import websockets

CHAT_SERVERS = [9001, 9002, 9003]


async def connect_to_server():
    """依次尝试连接各台 Chat Server，返回 (websocket, port)"""
    for port in CHAT_SERVERS:
        try:
            print(f"🔌 尝试连接 Chat Server {port}...")
            ws = await websockets.connect(f"ws://localhost:{port}")
            return ws, port
        except Exception:
            print(f"   ❌ {port} 不可用")
    return None, None


async def receive_loop(websocket):
    """持续接收消息，连接断了就返回"""
    async for message in websocket:
        print(f"\r💬 {message}\n> ", end="", flush=True)


async def send_loop(websocket):
    """持续发送消息，quit 或断线就返回"""
    loop = asyncio.get_event_loop()
    while True:
        message = await loop.run_in_executor(None, lambda: input("> "))
        if message.strip().lower() == "quit":
            return "quit"
        if message.strip():
            try:
                await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                return "disconnected"


async def main():
    name = input("请输入你的昵称：").strip()
    if not name:
        print("昵称不能为空")
        return

    while True:
        ws, port = await connect_to_server()
        if ws is None:
            print("❌ 所有 Chat Server 不可用，退出")
            return

        print(f"✅ 已连接到 Chat Server {port}！输入消息按回车发送，quit 退出\n")

        try:
            await ws.send(name)

            # 两个任务同时跑：接收 和 发送
            # 哪个先结束（断线/quit）就取消另一个
            recv_task = asyncio.create_task(receive_loop(ws))
            send_task = asyncio.create_task(send_loop(ws))

            done, pending = await asyncio.wait(
                [recv_task, send_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

            # 如果是用户主动 quit
            if send_task in done and send_task.result() == "quit":
                print("👋 再见！")
                await ws.close()
                return

        except Exception:
            pass

        print(f"⚠️  Chat Server {port} 断线，正在重连...")
        await asyncio.sleep(1)


asyncio.run(main())
