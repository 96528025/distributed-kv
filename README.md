# Distributed Key-Value Store

A Redis-inspired distributed key-value store built from scratch in Python. Demonstrates core distributed systems concepts: data replication, fault tolerance, disk persistence, snapshot recovery, leader election, and split-brain.

## Architecture

```
  Client
    │
    ▼
┌─────────┐     replicates     ┌─────────┐
│ Node A  │ ─────────────────► │ Node B  │
│ :5001   │ ◄───────────────── │ :5002   │
└─────────┘                    └─────────┘
     │                              │
     └──────────┬───────────────────┘
                ▼
          ┌─────────┐
          │ Node C  │
          │ :5003   │
          └─────────┘
```

Each node stores data independently and replicates writes to all other nodes.

## Features

- **Data replication** — write to any node, all nodes sync automatically
- **Disk persistence** — every write saved to `data_<port>.json`, survives full cluster restart
- **Fault tolerance** — cluster keeps working when a node goes down
- **Snapshot recovery** — restarted nodes automatically fetch full data from peers
- **Leader election** — lowest-port alive node becomes leader; only leader accepts writes; auto re-elect on failure
- **Auto redirect** — writing to a follower automatically redirects to the current leader
- **Split-brain demo** — simulate network partition and data conflict with `isolate`/`heal`

## Demo

**Normal replication** — write once, all nodes get it:
```
> set name Freja
📝 写入 [name = Freja] 到节点 5001
  ✅ 同步到节点 5002 成功
  ✅ 同步到节点 5003 成功

> all
节点 5001: {'name': 'Freja'}
节点 5002: {'name': 'Freja'}
节点 5003: {'name': 'Freja'}
```

**Node failure** — one node goes down, others keep working:
```
节点 5001: {'name': 'Freja', 'city': 'MountainView'}
节点 5002: {'name': 'Freja', 'city': 'MountainView'}
节点 5003: ❌ 不在线
```

**Disk persistence** — full cluster restart, data survives:
```
# All nodes killed (simulating power failure)
# data_5001.json: {"name": "Freja", "city": "MountainView"}

# On restart, each node reads its own disk file first
💾 从磁盘恢复了 2 条数据：{'name': 'Freja', 'city': 'MountainView'}
✅ 节点 5001 就绪，当前数据：{'name': 'Freja', 'city': 'MountainView'}
```

**Snapshot recovery** — node restarts and automatically recovers all historical data:
```
# Node 5003 was offline, missed some writes
# On restart, it fetches a full snapshot from peers

🚀 节点启动：port 5003，peers: [5001, 5002]
🔍 尝试从其他节点恢复数据...
  ✅ 从节点 5001 恢复了 5 条数据

节点 5001: {'name': 'Freja', 'city': 'MountainView', 'avatar': 'GGBond', ...}
节点 5002: {'name': 'Freja', 'city': 'MountainView', 'avatar': 'GGBond', ...}
节点 5003: {'name': 'Freja', 'city': 'MountainView', 'avatar': 'GGBond', ...}  ← fully recovered!
```

## How to Run

```bash
# Terminal 1: start the cluster
bash start.sh

# Terminal 2: interact with the cluster
python3 client.py
```

**Client commands:**
```
set <key> <value> [port]   # write data (default: node 5001)
get <key> [port]           # read data (default: node 5001)
all                        # show all nodes' data
quit                       # exit
```

## Project Structure

```
distributed-kv/
├── node.py      # each node: HTTP server + in-memory store + replication
├── client.py    # interactive CLI client
└── start.sh     # starts all 3 nodes
```

## API Endpoints

Each node exposes:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/get?key=<k>` | read a value |
| GET | `/all` | dump all data |
| GET | `/health` | health check |
| GET | `/snapshot` | return full data dump (used for recovery) |
| POST | `/set` | write a value (triggers replication) |
| POST | `/internal` | receive replicated data from peers |

**Leader election** — leader goes down, cluster elects a new one automatically:
```
节点 5001 👑 leader: {'name': 'Freja'}
节点 5002 🔄 follower: {'name': 'Freja'}
节点 5003 🔄 follower: {'name': 'Freja'}

# kill node 5001 (the leader)
💥 Leader 5001 已挂掉

# 5 seconds later, 5002 becomes the new leader
> leader
节点 5002 认为 Leader 是：5002，自己角色：leader

# writing to a follower auto-redirects to leader
> set city Cupertino 5002
↪️  节点 5002 不是 Leader，自动转发到节点 5001
✅ 成功：{'written_to': 5002}
```

## Known Limitations

- **Simple leader election** — based on lowest port number, not a full Raft/Paxos consensus
- **No conflict resolution** — split-brain recovery uses last-write-wins, data may be silently lost

These are intentional simplifications to focus on core concepts. Real solutions: Raft consensus algorithm, Redis RDB snapshots.

**Split-brain demo** — simulate network partition and data conflict:
```
> set role unknown        # initial state, all nodes in sync
> isolate 5001            # cut node 5001 off from the cluster
> set role leader 5001    # 5001 thinks it's the leader
> set role follower 5002  # the other side disagrees

> all
节点 5001: {'role': 'leader'}    ← isolated side
节点 5002: {'role': 'follower'}  ← other side
节点 5003: {'role': 'follower'}  ← other side

# same key, two different values — this is split-brain
# healing the partition: last write wins, no conflict warning
> heal 5001
> set role resolved 5001
节点 5001: {'role': 'resolved'}
节点 5002: {'role': 'resolved'}
节点 5003: {'role': 'resolved'}  ← 'leader' value silently lost!
```

---

## 中文说明

### 这是什么？

用 Python 从零手写的分布式键值数据库，类似 Redis 集群的简化版。演示了分布式系统的核心概念：数据复制、持久化、故障容忍、快照恢复、脑裂问题。

### 架构

三个节点运行在本地不同端口，互相同步数据：
- 写入任意节点 → 自动同步到其他所有节点
- 节点挂了 → 其他节点继续工作
- 节点重启 → 先从磁盘恢复，再从其他节点补全

### 功能列表

- **数据复制** — 写入一个节点，所有节点自动同步
- **磁盘持久化** — 每次写入同时存入 `data_<port>.json`，全集群重启数据不丢失
- **故障容忍** — 一个节点挂掉，集群继续正常工作
- **快照恢复** — 节点重启时自动从其他节点拉取全量数据
- **选主（Leader Election）** — 存活节点中端口最小的当 Leader，只有 Leader 接受写入，Leader 挂了自动重新选
- **自动转发** — 写入 Follower 时自动重定向到 Leader，客户端无感知
- **脑裂演示** — 模拟网络分区，展示数据不一致问题

### 如何运行

```bash
# 终端 1：启动集群
bash start.sh

# 终端 2：启动客户端
python3 client.py
```

**客户端命令：**
```
set <key> <value> [port]   # 写入数据（默认写到节点 5001）
get <key> [port]           # 读取数据
all                        # 查看所有节点的数据
isolate <port>             # 孤立某个节点（模拟脑裂）
heal <port>                # 恢复某个节点的网络连接
quit                       # 退出
```

### API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/get?key=<k>` | 读取一个值 |
| GET | `/all` | 查看所有数据 |
| GET | `/health` | 健康检查 |
| GET | `/snapshot` | 返回全量数据（用于新节点恢复） |
| GET | `/isolate` | 进入孤立模式（模拟脑裂） |
| GET | `/heal` | 退出孤立模式 |
| POST | `/set` | 写入数据（触发同步） |
| POST | `/internal` | 接收其他节点的同步数据 |

### 已知局限

- **没有选主（Leader Election）** — 任何节点都能接受写入，存在脑裂风险
- **没有冲突解决** — 脑裂恢复后，最后一次写入覆盖其他值，旧数据无警告丢失

真实系统的解决方案：Raft 共识算法、Redis RDB 快照。
