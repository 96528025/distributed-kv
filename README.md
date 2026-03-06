# Distributed Key-Value Store + Real-Time Chat

A Redis-inspired distributed key-value store built from scratch in Python, extended into a full distributed chat system. Demonstrates core distributed systems concepts: data replication, fault tolerance, disk persistence, snapshot recovery, leader election, split-brain, and horizontal scaling.

## Architecture

```
Alice ──WebSocket──► Chat Server :9001 ──┐
Bob   ──WebSocket──► Chat Server :9002 ──┼──► KV Cluster :5001/:5002/:5003
Carol ──WebSocket──► Chat Server :9003 ──┘    (message persistence + replication)
```

Each Chat Server is stateless. All message history is stored in the distributed KV cluster. If a Chat Server goes down, clients automatically reconnect to another one — and message history is preserved.

## Features

**Distributed KV Store**
- **Data replication** — write to any node, all nodes sync automatically
- **Disk persistence** — every write saved to `data_<port>.json`, survives full cluster restart
- **Fault tolerance** — cluster keeps working when a node goes down
- **Snapshot recovery** — restarted nodes automatically fetch full data from peers
- **Leader election** — lowest-port alive node becomes leader; only leader accepts writes; auto re-elect on failure
- **Auto redirect** — writing to a follower automatically redirects to the current leader
- **List type** — `lpush` / `lrange` support for storing message history
- **Split-brain demo** — simulate network partition and data conflict with `isolate`/`heal`

**Distributed Chat**
- **Multi-server** — 3 Chat Servers, clients auto-reconnect on failure
- **Message history** — new users receive last 50 messages on connect (stored in KV cluster)
- **Persistent messages** — chat history survives full cluster restart
- **Load tested** — 200 concurrent users: 100% success rate, 163 msg/s, 0.04ms avg latency

## Load Test Results

| Users | Success Rate | Throughput | Avg Latency |
|-------|-------------|------------|-------------|
| 50    | 100%        | 41.5 msg/s | 0.04ms      |
| 200   | 100%        | 163 msg/s  | 0.04ms      |
| 1000  | 25%         | 200 msg/s  | 0.03ms      |

Bottleneck: connection count per Chat Server (~80 concurrent), not latency. Linear scaling — doubling servers doubles capacity.

## How to Run

```bash
# Terminal 1: start the KV cluster
bash start.sh

# Terminal 2: start Chat Servers
source ~/Desktop/chat-room/venv/bin/activate
python3 chat_server.py 9001 &
python3 chat_server.py 9002 &
python3 chat_server.py 9003 &

# Terminal 3+: connect as a user
python3 chat_client.py

# Load test
python3 load_test.py
```

**KV client commands:**
```
set <key> <value> [port]   # write data
get <key> [port]           # read data
all                        # show all nodes' data
leader                     # show current leader
isolate <port>             # isolate a node (split-brain demo)
heal <port>                # reconnect isolated node
quit                       # exit
```

## Project Structure

```
distributed-kv/
├── node.py          # KV node: HTTP server + replication + leader election + list type
├── client.py        # interactive CLI for the KV store
├── chat_server.py   # WebSocket chat server backed by KV cluster
├── chat_client.py   # chat client with auto-reconnect
├── load_test.py     # concurrent load tester
├── start.sh         # start all 3 KV nodes
└── start_chat.sh    # start all 3 Chat Servers
```

## API Endpoints (KV Node)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/get?key=<k>` | read a value |
| GET | `/lrange?key=<k>&start=0&end=49` | read a list range |
| GET | `/all` | dump all data |
| GET | `/health` | health check |
| GET | `/snapshot` | full data dump (used for recovery) |
| GET | `/leader` | current leader info |
| GET | `/isolate` | enter isolated mode (split-brain demo) |
| GET | `/heal` | exit isolated mode |
| POST | `/set` | write a string value (triggers replication) |
| POST | `/lpush` | append to a list (triggers replication) |
| POST | `/internal` | receive replicated data from peers |

## Known Limitations

- **Simple leader election** — based on lowest port number, not Raft/Paxos consensus
- **No conflict resolution** — split-brain recovery uses last-write-wins
- **Single write leader** — KV write throughput limited by one leader node; needs sharding to scale further

Real solutions: Raft consensus, Redis Cluster sharding.

---

## 中文说明

用 Python 从零手写的分布式键值数据库，升级版包含实时聊天室。演示了分布式系统核心概念：数据复制、持久化、故障容忍、快照恢复、脑裂、水平扩展。

### 如何运行

```bash
# 终端 1：启动 KV 集群
bash start.sh

# 终端 2：启动聊天服务器
source ~/Desktop/chat-room/venv/bin/activate
python3 chat_server.py 9001 &
python3 chat_server.py 9002 &
python3 chat_server.py 9003 &

# 终端 3+：启动客户端
python3 chat_client.py

# 压力测试
python3 load_test.py
```

### 压力测试结论

- 200 并发用户：100% 连接成功，163 消息/秒，0.04ms 平均延迟
- 1000 并发用户：25% 连接成功，瓶颈是每台服务器约 80 并发连接上限
- 扩展方案：增加 Chat Server 数量（线性扩展），KV 层需分片突破写入瓶颈
