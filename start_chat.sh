#!/bin/bash
# 启动完整的分布式聊天系统
# 依赖：先运行 bash start.sh 启动 KV 集群

echo "💬 启动分布式聊天服务器..."
echo "   （请确保 KV 集群已启动：bash start.sh）"
echo ""

# 检查虚拟环境
if [ -d "../chat-room/venv" ]; then
    source ../chat-room/venv/bin/activate
fi

python3 chat_server.py 9001 &
python3 chat_server.py 9002 &
python3 chat_server.py 9003 &

echo "✅ 三台 Chat Server 已启动："
echo "   Chat Server 1: ws://localhost:9001"
echo "   Chat Server 2: ws://localhost:9002"
echo "   Chat Server 3: ws://localhost:9003"
echo ""
echo "运行 'python3 chat_client.py' 开始聊天"
echo "按 Ctrl+C 停止"

wait
