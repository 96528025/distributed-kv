#!/bin/bash
# 一键启动 3 个节点

echo "🚀 启动分布式 KV 集群..."

python3 node.py 5001 5002 5003 &
python3 node.py 5002 5001 5003 &
python3 node.py 5003 5001 5002 &

echo "✅ 三个节点已启动："
echo "   节点 A: localhost:5001"
echo "   节点 B: localhost:5002"
echo "   节点 C: localhost:5003"
echo ""
echo "运行 'python3 client.py' 开始使用"
echo "按 Ctrl+C 停止所有节点"

wait
