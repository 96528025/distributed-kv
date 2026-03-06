"""
压力测试：模拟多个用户同时连接并发消息
测量：
- 连接成功率
- 消息发送成功率
- 每秒处理消息数（throughput）
- 平均延迟
"""

import asyncio
import websockets
import time
import random

CHAT_SERVERS = [9001, 9002, 9003]
NUM_USERS = 1000        # 模拟用户数
MESSAGES_PER_USER = 10  # 每个用户发几条消息

# 统计数据
stats = {
    "connected": 0,
    "failed_connect": 0,
    "messages_sent": 0,
    "messages_failed": 0,
    "latencies": [],
}
stats_lock = asyncio.Lock()


async def simulate_user(user_id):
    """模拟一个用户：连接 → 发消息 → 断开"""
    name = f"User{user_id}"
    port = CHAT_SERVERS[user_id % len(CHAT_SERVERS)]  # 均匀分配到各台服务器

    try:
        async with websockets.connect(f"ws://localhost:{port}", open_timeout=3) as ws:
            await ws.send(name)

            # 读掉历史消息（不计入延迟）
            try:
                async with asyncio.timeout(0.5):
                    async for _ in ws:
                        pass
            except (asyncio.TimeoutError, Exception):
                pass

            async with stats_lock:
                stats["connected"] += 1

            # 发消息并测延迟
            for i in range(MESSAGES_PER_USER):
                msg = f"msg-{user_id}-{i}"
                t0 = time.time()
                try:
                    await ws.send(msg)
                    latency = (time.time() - t0) * 1000  # ms
                    async with stats_lock:
                        stats["messages_sent"] += 1
                        stats["latencies"].append(latency)
                except Exception:
                    async with stats_lock:
                        stats["messages_failed"] += 1

                await asyncio.sleep(random.uniform(0.05, 0.2))  # 模拟真实用户节奏

    except Exception:
        async with stats_lock:
            stats["failed_connect"] += 1


async def main():
    print(f"🧪 压力测试开始")
    print(f"   用户数：{NUM_USERS}")
    print(f"   每人发消息：{MESSAGES_PER_USER} 条")
    print(f"   目标服务器：{CHAT_SERVERS}")
    print()

    t_start = time.time()

    # 所有用户同时连接
    tasks = [simulate_user(i) for i in range(NUM_USERS)]
    await asyncio.gather(*tasks)

    elapsed = time.time() - t_start

    # 输出结果
    total_msgs = stats["messages_sent"] + stats["messages_failed"]
    throughput = stats["messages_sent"] / elapsed
    latencies = stats["latencies"]
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    max_latency = max(latencies) if latencies else 0

    print("=" * 40)
    print(f"📊 测试结果（耗时 {elapsed:.1f}s）")
    print(f"   连接成功：{stats['connected']} / {NUM_USERS}")
    print(f"   连接失败：{stats['failed_connect']}")
    print(f"   消息发送：{stats['messages_sent']} / {total_msgs}")
    print(f"   吞吐量：  {throughput:.1f} 消息/秒")
    print(f"   平均延迟：{avg_latency:.2f} ms")
    print(f"   最大延迟：{max_latency:.2f} ms")
    print("=" * 40)

    if stats["failed_connect"] > 0:
        print(f"⚠️  有 {stats['failed_connect']} 个用户连接失败 → Chat Server 是瓶颈")
    if avg_latency > 100:
        print(f"⚠️  平均延迟 {avg_latency:.0f}ms 偏高 → KV 集群写入是瓶颈")
    if stats["failed_connect"] == 0 and avg_latency < 50:
        print(f"✅ 系统健康，可以尝试增加用户数再测")


asyncio.run(main())
