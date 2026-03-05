# Distributed Key-Value Store

A Redis-inspired distributed key-value store built from scratch in Python. Demonstrates core distributed systems concepts: data replication, fault tolerance, snapshot recovery, and consistency challenges.

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
- **Fault tolerance** — cluster keeps working when a node goes down
- **Snapshot recovery** — restarted nodes automatically fetch full data from peers
- **Consistency demo** — shows what happens when nodes miss updates
- **Auto failover** — writes skip offline nodes, warn instead of crash

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

## Known Limitations

- **No leader election** — any node can accept writes (potential split-brain)
- **In-memory only** — data lost if all nodes restart simultaneously
- **No conflict resolution** — if two nodes get different values for the same key while partitioned, last write wins

These are intentional simplifications to focus on core concepts. Real solutions: Raft consensus algorithm, Redis RDB snapshots.
