# Distributed Key-Value Store

A Redis-inspired distributed key-value store built from scratch in Python. Demonstrates core distributed systems concepts: data replication, fault tolerance, and consistency challenges.

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

**Consistency gap** — node rejoins but missed historical data:
```
节点 5001: {'city': 'MountainView', 'name': 'Freja'}
节点 5002: {'city': 'MountainView', 'name': 'Freja'}
节点 5003: {'name': 'Freja'}   ← only has data written after it rejoined
```

This demonstrates **eventual consistency** — a core challenge in distributed systems.

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
| POST | `/set` | write a value (triggers replication) |
| POST | `/internal` | receive replicated data from peers |

## Known Limitations

- **No snapshot/recovery** — new nodes joining the cluster won't get historical data
- **No leader election** — any node can accept writes (potential split-brain)
- **In-memory only** — data lost on restart

These are intentional simplifications to focus on core concepts. Real solutions: Redis RDB snapshots, Raft consensus algorithm.
