# Distributed KV Store + Real-Time Chat / 分布式键值存储 + 实时聊天室

A Redis-inspired distributed key-value store built from scratch in Python, extended into a full distributed chat system deployed across 3 AWS regions. Demonstrates core distributed systems concepts: data replication, fault tolerance, disk persistence, snapshot recovery, leader election, split-brain, horizontal scaling, consistent hashing sharding, and cloud deployment.

用 Python 从零手写的分布式键值数据库，升级为完整的分布式聊天系统，部署在 AWS 三大洲。演示了分布式系统核心概念：数据复制、持久化、故障容忍、快照恢复、选主、脑裂、水平扩展、一致性哈希分片和云端部署。

---

## Architecture / 架构

```
[You / 你]
    │  WebSocket
    ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  Chat Server    │   │  Chat Server    │   │  Chat Server    │
│  Virginia :9001 │   │  Oregon  :9002  │   │  Ireland :9003  │
└────────┬────────┘   └────────┬────────┘   └────────┬────────┘
         │                     │                     │
         └─────────────────────┼─────────────────────┘
                               │ HTTP (lpush / lrange)
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│   KV Node       │◄──│   KV Node       │──►│   KV Node       │
│  Virginia :5001 │   │  Oregon  :5002  │   │  Ireland :5003  │
│  👑 Leader      │   │  🔄 Follower    │   │  🔄 Follower    │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

- **Chat Servers** are stateless — any one can go down, clients auto-reconnect
- **KV Cluster** stores all message history — persisted to disk, replicated across 3 continents
- Write to Virginia → automatically synced to Oregon and Ireland

---

## Features / 功能

**Distributed KV Store**
- Data replication — write to any node, all nodes sync automatically / 写入一个节点，所有节点自动同步
- Disk persistence — every write saved to `data_<port>.json` / 每次写入同时存磁盘，重启不丢数据
- Fault tolerance — cluster keeps working when a node goes down / 节点挂掉，集群继续工作
- Snapshot recovery — restarted nodes fetch full data from peers / 节点重启从其他节点拉取全量数据
- Leader election — lowest-port alive node becomes leader / 存活节点中端口最小的当 Leader
- Auto redirect — follower automatically redirects writes to leader / Follower 自动转发写入到 Leader
- List type — `lpush` / `lrange` for storing message history / 列表类型，用于存储聊天历史
- Split-brain demo — simulate network partition with `isolate`/`heal` / 脑裂演示

**Distributed Chat**
- Multi-server — 3 Chat Servers across 3 regions, clients auto-reconnect on failure / 三大洲三台服务器，断线自动重连
- Message history — new users receive last 50 messages on connect / 新用户连接自动推送历史消息
- Persistent messages — chat history survives full cluster restart / 聊天记录跨重启保存
- Cross-machine peers — KV nodes communicate via real public IPs / KV 节点通过真实公网 IP 互相通信

---

## Load Test Results / 压力测试结果

| Users / 用户数 | Success Rate / 成功率 | Throughput / 吞吐量 | Avg Latency / 平均延迟 |
|---------------|----------------------|--------------------|-----------------------|
| 50            | 100%                 | 41.5 msg/s         | 0.04ms                |
| 200           | 100%                 | 163 msg/s          | 0.04ms                |
| 1000          | 25%                  | 200 msg/s          | 0.03ms                |

**Bottleneck / 瓶颈：** Connection count per Chat Server (~80 concurrent), not latency. Linear scaling — doubling servers doubles capacity. KV write throughput is limited by single leader; needs sharding to scale further.

瓶颈在每台 Chat Server 的连接数（约 80 并发），而非延迟。水平扩展有效：加一台服务器，容量线性增加。KV 写入瓶颈在单 Leader，需分片突破。

---

## How to Run / 如何运行

### Local / 本地运行

```bash
# Terminal 1: start KV cluster / 启动 KV 集群
bash start.sh

# Terminal 2: start Chat Servers / 启动聊天服务器
source ~/Desktop/chat-room/venv/bin/activate
python3 chat_server.py 9001 &
python3 chat_server.py 9002 &
python3 chat_server.py 9003 &

# Terminal 3+: connect as user / 连接聊天室
python3 chat_client.py

# Load test / 压力测试
python3 load_test.py
```

### Cloud (AWS) / 云端运行

```bash
# On each EC2 instance / 每台 EC2 上运行：
git clone https://github.com/96528025/distributed-kv.git
cd distributed-kv
pip3 install websockets

# Virginia (us-east-1)
python3 node.py 5001 <oregon-ip>:5002 <ireland-ip>:5003 &
python3 chat_server.py 9001 <virginia-ip>:5001 <oregon-ip>:5002 <ireland-ip>:5003 &

# Oregon (us-west-2)
python3 node.py 5002 <virginia-ip>:5001 <ireland-ip>:5003 &
python3 chat_server.py 9002 <virginia-ip>:5001 <oregon-ip>:5002 <ireland-ip>:5003 &

# Ireland (eu-west-1)
python3 node.py 5003 <virginia-ip>:5001 <oregon-ip>:5002 &
python3 chat_server.py 9003 <virginia-ip>:5001 <oregon-ip>:5002 <ireland-ip>:5003 &
```

**Required AWS Security Group ports / 需要开放的端口：**
- 22 (SSH), 5001-5003 (KV cluster), 9001-9003 (Chat Servers)

---

## Project Structure / 项目结构

```
distributed-kv/
├── node.py                  # KV node v1: replication + simple leader election (min port)
├── node_sharded.py          # KV node v2: consistent hashing sharding (no single leader)
├── node_raft.py             # KV node v3: Raft consensus (real leader election + log replication)
├── node_replicated.py       # KV node v4: sharding + full replication (per-shard primary failover)
├── node_raft_sharded.py     # KV node v5: per-shard Raft groups (CockroachDB/TiKV architecture)
├── client.py                # interactive CLI for the KV store
├── chat_server.py           # WebSocket chat server backed by KV cluster
├── chat_client.py           # chat client with auto-reconnect
├── load_test.py             # concurrent load tester
├── start.sh                 # start all 3 KV nodes (local)
└── start_chat.sh            # start all 3 Chat Servers (local)
```

---

## API Endpoints / API 接口

| Method | Path | Description |
|--------|------|-------------|
| GET | `/get?key=<k>` | read a value / 读取字符串值 |
| GET | `/lrange?key=<k>&start=0&end=49` | read a list range / 读取列表片段 |
| GET | `/all` | dump all data / 查看所有数据 |
| GET | `/health` | health check / 健康检查 |
| GET | `/snapshot` | full data dump for recovery / 全量快照（用于恢复） |
| GET | `/leader` | current leader info / 查看当前 Leader |
| GET | `/isolate` | enter isolated mode (split-brain demo) / 进入孤立模式 |
| GET | `/heal` | exit isolated mode / 退出孤立模式 |
| POST | `/set` | write a string value / 写入字符串 |
| POST | `/lpush` | append to a list / 列表追加 |
| POST | `/internal` | receive replicated data from peers / 接收同步数据 |

---

## Build Log / 开发日志

### Day 1 — Distributed KV Store / 第一天：分布式 KV 存储

Built a Redis-inspired distributed KV store from scratch with 3 nodes.

从零手写分布式 KV 存储，3个节点互相同步。

**Problems & Solutions / 遇到的问题：**

| Problem / 问题 | Solution / 解决方法 |
|---------------|-------------------|
| `Address already in use` | `pkill -9 -f "node.py"` 清理残留进程 |
| Writing to follower showed "no response" | urllib 把 403 当 HTTPError 抛出，单独 catch 处理 |
| `global isolated` syntax error | 同一方法两处声明，改为方法顶部声明一次 |
| Split-brain not working | 孤立只阻止发出，忘了阻止接收，在 `/internal` 加孤立检查 |

---

### Day 2 — WebSocket Chat + Distributed Upgrade / 第二天：WebSocket 聊天室 + 分布式升级

Added list type to KV store, built Chat Servers on top, load tested.

给 KV 存储加列表类型，在上面建聊天服务器，压力测试。

**Key decisions / 关键设计决策：**
- Chat Servers are stateless — all state lives in KV cluster
- Messages stored with `lpush`, history fetched with `lrange`
- Client uses two concurrent async tasks (recv + send) so disconnect is detected immediately

**Problems & Solutions / 遇到的问题：**

| Problem / 问题 | Solution / 解决方法 |
|---------------|-------------------|
| Client didn't auto-reconnect on disconnect | `input()` blocks — switched to two concurrent asyncio tasks; whichever finishes first triggers reconnect |
| Load test: 17/50 connections failed with 2 servers | Started 3rd Chat Server → 50/50 success |
| Load test bottleneck at 1000 users | ~80 concurrent connections per server; fix = add more servers |

---

### Day 3 — AWS Cloud Deployment + Sharding + Raft / 第三天：AWS 云端部署 + 分片 + Raft

**Consistent Hashing Sharding (`node_sharded.py`) / 一致性哈希分片**

Each key is assigned to a node via `MD5(key) % num_nodes`. Writes and reads automatically forwarded to the correct node. No single leader bottleneck — all nodes handle writes in parallel.

每个 key 通过 `MD5(key) % 节点数` 分配到固定节点。写入和读取自动转发给正确节点，三台都能处理写入，无单点瓶颈。

```
set user:alice  → hash → node 5001 (direct write)
set chat:msg    → hash → node 5002 (forwarded)
set cache:home  → hash → node 5003 (forwarded)
```

**Raft Consensus Algorithm (`node_raft.py`) / Raft 共识算法**

Replaces fake "min port = leader" with real consensus:
- **Terms**: monotonically increasing epoch; stale leaders step down automatically
- **Election**: randomized timeouts → first to timeout requests votes → majority wins
- **Log replication**: writes appended to log first, committed only after majority ACK
- **Heartbeats**: leader sends periodic heartbeats to prevent unnecessary re-elections

替换掉"最小端口当 Leader"的假方案，实现真正的共识：
- **任期**：单调递增，过期 Leader 自动下台
- **选举**：随机超时，先超时的发起投票，多数票当选
- **日志复制**：写入先进日志，多数节点确认后才 commit
- **心跳**：Leader 定期发心跳，防止不必要的重新选举

**Problems & Solutions / 遇到的问题：**

| Problem / 问题 | Solution / 解决方法 |
|---------------|-------------------|
| Raft node kept re-electing before peers started | Normal behavior — no majority available until all 3 nodes up; won on Term 42 once all started |
| Term number jumped to 42 | Each failed election increments term; expected when nodes start one by one |

---

### Day 3b — AWS Cloud Deployment / 第三天：AWS 云端部署

Deployed the full stack across 3 AWS EC2 regions.

把整个系统部署到 AWS 三大洲的 EC2 上。

**Problems & Solutions / 遇到的问题：**

| Problem / 问题 | Solution / 解决方法 |
|---------------|-------------------|
| `node.py` only supported `localhost` peers | Updated to parse `IP:PORT` format: `python3 node.py 5001 54.x.x.x:5002 54.x.x.x:5003` |
| `Permission denied` writing `data_5001.json` | `sudo git clone` created root-owned files → `sudo chown -R ec2-user:ec2-user /opt/distributed-kv` |
| `git: command not found` on EC2 | `sudo dnf install -y git` |
| `pip3: command not found` on EC2 | `sudo dnf install -y python3-pip` |
| Chat Server unreachable from outside | Bound to `"localhost"` instead of `"0.0.0.0"` — fixed in `websockets.serve()` |
| SSH terminal freezing | Background node.py logs flooding input; reconnected and used `&` for background processes |
| Wrong SSH key for Oregon | Key named `distributed-system-key-euwest.pem` was actually for Oregon (misnamed at creation time) |
| zsh treating `?` as wildcard in curl | Wrapped URL in quotes: `curl "http://host:port/get?key=hello"` |

---

### Day 4 — Sharding + Full Replication (`node_replicated.py`) / 第四天：分片 + 全量副本

Best of both worlds: consistent hashing sharding (no single write bottleneck) + full replication (every node stores all data, reads from any node).

结合分片和副本的优点：一致性哈希分片（无单点写瓶颈）+ 全量副本（每个节点存所有数据，可从任意节点读）。

**Architecture / 架构：**
- 3 shards, each shard has a primary; all nodes store all data / 3个分片，每个分片有primary，所有节点存全量数据
- Writes go to the shard's primary, which replicates to all peers / 写入发给分片primary，primary同步给所有节点
- Primary assignment: round-robin by shard index; auto-failover to next alive node / Primary按分片索引轮流分配，挂了自动切下一个存活节点
- Background health check thread (every 2s) detects node failures and updates primary mapping / 后台健康检查线程（每2s）检测节点存活，自动更新primary

```
分片 0: primary=5001, 候补=[5002, 5003]
分片 1: primary=5002, 候补=[5001, 5003]  ← 5002挂了 → 5003接管
分片 2: primary=5003, 候补=[5001, 5002]
```

**Problems & Solutions / 遇到的问题：**

| Problem / 问题 | Solution / 解决方法 |
|---------------|-------------------|
| Health check loop held `alive_lock` while making HTTP requests → deadlock with single-threaded HTTP server | Collect ping results outside the lock first, then update state under lock / 先在锁外ping，再加锁更新，避免持锁期间发HTTP请求导致死锁 |

---

### Day 5 — Per-Shard Raft Groups (`node_raft_sharded.py`) / 第五天：每个分片独立 Raft

Combined sharding with per-shard Raft consensus — the architecture used by CockroachDB and TiKV.

将分片与 Raft 共识结合，每个分片独立运行一个 Raft 共识组——这正是 CockroachDB / TiKV 的核心架构。

**Architecture / 架构：**
```
分片 0: Raft Group → 独立选 Leader（可能是5001/5002/5003中任意一个）
分片 1: Raft Group → 独立选 Leader
分片 2: Raft Group → 独立选 Leader

所有节点存全量数据（全量副本），每个分片的写入由该分片的 Raft Leader 负责
```

**Key improvements over `node_replicated.py` / 对比 node_replicated.py 的改进：**

| Feature / 特性 | node_replicated.py | node_raft_sharded.py |
|---------------|-------------------|----------------------|
| Leader election | Simple alive-check / 简单存活检查 | Raft consensus / Raft 共识 |
| Write safety | Lost if primary crashes before sync / primary 宕机可能丢写入 | Committed only after majority ACK / 多数确认才 commit |
| Parallel writes | Yes (different shards) / 是 | Yes (different shard Leaders) / 是 |
| Reads | Any node / 任意节点 | Any node / 任意节点 |

**Key design / 关键设计：**
- `ShardRaft` class holds per-shard Raft state (term, log, commit_index, leader_id, etc.) / 每个分片独立的 Raft 状态类
- All RPCs carry `shard_id` to route to the correct Raft group / 所有 RPC 携带 shard_id 路由到正确的 Raft 组
- `election_timer()` iterates all shards independently / election_timer 遍历所有分片独立计时
- `heartbeat_loop()` sends heartbeats for each shard where this node is Leader / 心跳循环按分片独立发送
- `store_lock` and `shard.lock` never held simultaneously — avoids deadlock / 两把锁不同时持有，避免死锁

**Verification / 验证：**
```bash
# 启动三个节点
python3 node_raft_sharded.py 5001 5002 5003 &
python3 node_raft_sharded.py 5002 5001 5003 &
python3 node_raft_sharded.py 5003 5001 5002 &
sleep 5

# 查看各分片 Leader（三个分片可能有不同 Leader）
curl http://localhost:5001/health

# 写入（转发到正确分片的 Leader + 多数确认）
curl -X POST http://localhost:5001/set -d '{"key":"user:alice","value":"30"}'

# 从任意节点读取（全量副本，任意节点都能读）
curl "http://localhost:5001/get?key=user:alice"
curl "http://localhost:5002/get?key=user:alice"
curl "http://localhost:5003/get?key=user:alice"

# Kill 某个分片的 Leader，验证该分片自动重新选举
pkill -f "node_raft_sharded.py 5002"
sleep 5
curl http://localhost:5001/health   # 分片重新选了新 Leader
curl -X POST http://localhost:5001/set -d '{"key":"chat:msg","value":"after leader change"}'
```

**Problems & Solutions / 遇到的问题：**

| Problem / 问题 | Solution / 解决方法 |
|---------------|-------------------|
| Deadlock risk: shard.lock held during disk write | shard.lock and store_lock never held simultaneously; collect data under shard.lock, apply under store_lock / 两把锁分开持有，先在 shard.lock 内收集数据，再在 store_lock 内写磁盘 |
| Forwarding loop risk: Leader sends to self | Check shard role before forwarding; only forward when role != LEADER / 转发前检查角色，只有非 Leader 才转发 |
| Multiple shards timing out simultaneously | Each ShardRaft has independent random election_timeout; Raft term numbers prevent split votes / 每个分片独立随机超时，任期编号防止裂票 |

---

### Day 5b — Log Snapshot Compaction + Multi-key Transactions / 第五天 b：日志快照压缩 + 多 key 事务

Two new features added to `node_raft_sharded.py`.

在 `node_raft_sharded.py` 基础上增加两个功能。

**Feature 1: Log Snapshot Compaction / 日志快照压缩**

Without snapshots, the Raft log grows forever — restarting a node replays all history.
With snapshots, the log is truncated after each commit batch; a restarting node loads the snapshot and only replays the small tail.

没有快照时，Raft 日志无限增长——节点重启需重放所有历史。加快照后，每次超过阈值就截断旧日志，重启只需从快照 + 少量尾部日志恢复。

- `SNAPSHOT_THRESHOLD = 20` — trigger after 20 log entries (small for demo) / 超过20条触发（演示用小值）
- Each shard saves `snapshot_{PORT}_shard{N}.json` with store state + log metadata / 每个分片保存独立快照文件
- Follower that falls too far behind fetches snapshot from Leader via `POST /install_snapshot` / 落后太多的 Follower 主动向 Leader 拉快照
- `log_offset` field tracks absolute index of `log[0]`; `commit_index` is always absolute / 绝对 index 设计，重启后计算正确

**Feature 2: Multi-key Transactions (2PC) / 多 key 事务（两阶段提交）**

Atomically write multiple keys across different shards — all succeed or all fail.

原子地跨多个分片写入多个 key——要么全成功，要么全失败。

```
客户端 → POST /txn → 协调者节点
  Phase 1: 并发向各分片 Leader 发 /txn_prepare → 锁定 key
  全部 READY → Phase 2: 并发发 /txn_commit → 通过 Raft 写入
  任意 FAIL  → Phase 2: 并发发 /txn_abort  → 释放锁 → 返回 aborted
```

- Keys are locked per-shard; conflicting transactions return `{"status": "locked"}` / key 按分片加锁，冲突事务返回 locked
- Lock timeout: 10s; background `txn_cleanup_loop()` auto-releases stale locks / 10秒超时，后台线程自动释放死锁

**Verification / 验证：**
```bash
# ── 快照压缩 ──
for i in {1..25}; do
  curl -s -X POST http://localhost:5001/set \
    -H "Content-Type: application/json" -d "{\"key\":\"k$i\",\"value\":\"v$i\"}"
done
ls snapshot_*   # 快照文件已生成

pkill -f "node_raft_sharded.py 5002"; sleep 2
python3 node_raft_sharded.py 5002 5001 5003 &
sleep 4
curl "http://localhost:5002/get?key=k1"   # 从快照恢复，能读到

# ── 多 key 事务 ──
curl -X POST http://localhost:5001/txn \
  -H "Content-Type: application/json" \
  -d '{"ops":[{"key":"alice","value":"20"},{"key":"bob","value":"40"}]}'
# → {"status": "ok", "txn_id": "..."}

curl "http://localhost:5001/get?key=alice"   # → 20
curl "http://localhost:5001/get?key=bob"     # → 40
```

**New API Endpoints / 新增 API 接口：**

| Method | Path | Description |
|--------|------|-------------|
| POST | `/txn` | 多 key 原子事务（协调者）/ Multi-key atomic transaction (coordinator) |
| POST | `/txn_prepare` | 锁定 key，暂存意图（分片 Leader）/ Lock keys, stage intent (shard leader) |
| POST | `/txn_commit` | 提交事务，Raft 写入（分片 Leader）/ Commit via Raft (shard leader) |
| POST | `/txn_abort` | 中止事务，释放锁（分片 Leader）/ Abort, release locks (shard leader) |
| POST | `/install_snapshot` | Follower 向 Leader 拉取快照 / Follower fetches snapshot from leader |

**Automated Tests / 自动化测试：**

```bash
python3 test_raft_sharded.py   # 31 个测试用例，全自动，约 30s 跑完
```

覆盖：基础读写、Leader 转发、快照压缩、节点重启恢复、事务提交、锁冲突、锁超时自动释放。

**Problems & Solutions / 遇到的问题：**

| Problem / 问题 | Root Cause / 根本原因 | Solution / 解决方法 |
|---------------|----------------------|-------------------|
| 所有事务测试失败，报 `unreachable` | `HTTPServer` 默认单线程：协调者向自身发 `/txn_prepare` 时，外层请求占着唯一线程，内层请求永远排不上队 → 超时 | 改用 `ThreadingMixIn + HTTPServer`，每个请求在独立线程处理 |
| 写 25 条 key 没有触发快照 | 25 条 key 经哈希分散到 3 个分片，每片只有 ~8 条，低于 `SNAPSHOT_THRESHOLD=20` | 写 60 条才让每片超过阈值；测试设计时要考虑数据分布 |

**Key Insight / 核心收获：**

自动化测试不只是"省事"——它能发现手动 `curl` 永远触发不了的场景。
HTTPServer 单线程死锁这个 bug，手动测试时协调者和 Leader 总在不同节点，永远不会自己调自己，根本发现不了。
是自动化测试（固定用 5001 当协调者，而 5001 也可能是 Leader）第一次真实复现了这个问题。

Automated tests don't just save time — they surface bugs that manual `curl` can never trigger.
The single-threaded HTTPServer deadlock only manifests when the coordinator and the shard leader happen to be the same node. Manual testing never hit this because curl requests naturally go to different nodes. The automated test suite exposed it on the first run.

---

## Known Limitations / 已知局限

- **Simple leader election** (`node.py`) — based on lowest port number, not consensus / 选主基于端口号，非真正共识算法
- **No replication per shard** (`node_sharded.py`) — if a node dies, its keys are unavailable / 分片版无副本，节点挂了该分片不可用
- **Raft without log compaction** (`node_raft.py`) — log grows unbounded; real systems use snapshots / Raft 没有日志压缩，真实系统需要快照
- **No conflict resolution** (`node.py`) — split-brain recovery uses last-write-wins / 脑裂恢复用最后写入覆盖
- **No Raft in replicated shards** (`node_replicated.py`) — primary elected by simple alive-check, not consensus; writes may be lost if primary crashes before replication / 分片primary选举无共识，primary宕机前未同步的写入会丢失 *(fixed in v5)*
- **No log compaction** — log grows unbounded; real systems use snapshots / 日志无压缩，真实系统需要快照（所有版本）
- **No conflict resolution** (`node.py`) — split-brain recovery uses last-write-wins / 脑裂恢复用最后写入覆盖

Next steps: Raft log compaction / snapshots, multi-key transactions, linearizable reads. / 下一步：Raft 日志快照压缩、多 key 事务、线性化读。
