# 面试讲解稿：WAL + Checkpoint 持久化升级

> 配套代码：[`storage.py`](storage.py)、集成点在 [`node_raft_sharded.py`](node_raft_sharded.py)、
> 测试 [`test_wal.py`](test_wal.py)、基准 [`benchmark_storage.py`](benchmark_storage.py)。
> 这份稿子的目标：让你能**白板讲清**「为什么这么做、每个崩溃点为什么安全」。

---

## 0. 一句话电梯陈述

> 「原来每次写都把整个库重新 dump 成 JSON，成本是 O(数据量)，实测 5 万条时单写掉到
> 28 ops/s、p50 涨到 35ms。我加了一层 WAL：每次提交只**追加一条定长帧记录**（O(1)），
> 定期做一次**原子 checkpoint**把全量快照落盘再截断 WAL。崩溃后先读 checkpoint、
> 再 replay 之后的 WAL，用绝对 index 去重保证幂等。默认后端保持不变、WAL 是 opt-in，
> 所以 56 个老测试一个没改。」

---

## 1. 通俗版：WAL 写入流程

把状态机（内存里的 `store` 字典）想成「账本的当前余额」，WAL 想成「流水单」。

一次写入（比如 `set x=1`）在**已经通过 Raft 多数派确认**之后，做三件事，
**顺序很重要**：

1. **先改内存**：`apply_entry()` 把 `store["x"]="1"`（持 `store_lock`）；
2. **再写流水**：`storage.commit(store, [record])` 把这条操作编码成一条 WAL 记录追加到
   `wal_<port>.log`，然后 `flush()`；
3. **判断要不要 checkpoint**：如果 WAL 攒够了 `rotate_records` 条（默认 1000），
   就做一次 checkpoint 并把 WAL 截断。

> **等一下——为什么这里是「先改内存再写 WAL」，而经典 WAL 是「先写日志再改数据」?**
> 关键区别是**这条操作已经 committed 了**。在 Raft 里，「是否要持久化这条写」这个决定，
> 已经由 Raft 的多数派复制 + `commit_index` 拍板了；storage WAL 不负责「决定」，只负责
> 「把已定稿的操作记下来好在崩溃后重放」。所以对崩溃恢复而言，只要 apply 内存和 append
> WAL 都在**同一个 `store_lock` 临界区**里原子完成，谁先谁后都恢复得出同样的结果
> ——因为恢复时我们是拿 WAL 去**重建**内存，而不是拿内存去校验 WAL。经典「先写日志」
> 针对的是「日志是唯一真相、内存可能先于日志落盘导致不一致」的场景；这里内存是易失的、
> 每次崩溃都从 WAL 重建，所以不存在那个风险。（详见 Q1 的完整回答。）

一条 WAL 记录长这样（length-prefixed，可靠识别半写）：

```
MAGIC(4) | 长度(4) | payload(JSON: 版本/shard/绝对index/term/op/key/value) | crc32(4)
```

---

## 2. 通俗版：崩溃恢复流程

进程被 `kill -9`、机器重启……再起来时 `storage.load()`：

1. **读最近一次有效 checkpoint**：拿到「某个时间点的全量 store」+「每个分片当时应用到的
   绝对 index（`applied` 表）」；
2. **从 checkpoint 之后 replay WAL**：顺着 WAL 一条条读，**只应用 `index > applied[分片]`
   的记录**——因为 index ≤ applied 的那些，checkpoint 里已经包含了，重放就重复了；
3. 边 replay 边更新 `applied`，所以即使 WAL 里同一条 index 出现两次，第二次也会被跳过
   → **幂等**；
4. 遇到**尾部半写**（最后一条只写了一半）→ 保留前面所有完整记录，安全停在这；
5. 遇到**中间损坏**（crc 对不上 / MAGIC 错位）→ **抛错**，不假装没事继续。

结果：内存 store 被精确重建到「崩溃前最后一次 flush 的状态」。

---

## 3. 通俗版：checkpoint 的原子发布顺序，以及为什么每个崩溃点都安全

checkpoint 要解决的问题：WAL 不能无限长（恢复要 replay，越长越慢；磁盘也会涨）。
所以定期把「当前全量 store」拍一张快照存下来，然后就能把这张快照**已经覆盖**的那段 WAL 扔掉。

危险在于：**发布 checkpoint 和截断 WAL 这两步之间如果崩溃了会怎样？** 如果顺序错了，
可能「WAL 已经删了，但新 checkpoint 没写成」——数据就没了。

所以严格按这个顺序，**核心不变式：checkpoint 落盘持久之前，绝不动 WAL**：

```
① 写 checkpoint.tmp  →  flush + fsync        （新快照先安全落到磁盘）
② os.replace(tmp, checkpoint.json)           （原子 rename，一瞬间「切换生效」）
   + 对目录 fsync                             （让 rename 本身持久）
③ 现在——也只有现在——才截断 WAL             （旧流水可以扔了）
```

四个崩溃点逐个看，**都不丢不重**：

| 崩溃发生在… | 磁盘上的状态 | 恢复时会怎样 | 结论 |
|---|---|---|---|
| ① 写 tmp 途中 | 只有半个 `.tmp`，`checkpoint.json` 还是旧的 | 用**旧 checkpoint** + **完整旧 WAL** 重建 | ✅ 不丢 |
| ① 和 ② 之间 | `.tmp` 完整但没 rename | 同上，`.tmp` 被忽略/下次覆盖 | ✅ 不丢 |
| ② 完成、③ 之前 | **新 checkpoint 生效**，旧 WAL 还在（含已被快照覆盖的记录） | 读新 checkpoint，replay 旧 WAL，`index ≤ applied` 的记录**被去重跳过** | ✅ 不重复 |
| ③ 截断途中 | 新 checkpoint 在，WAL 被清 | 读新 checkpoint，WAL 空或只剩新记录 | ✅ 一致 |

一句话：**因为「新快照先 fsync 落盘」永远排在「删旧 WAL」前面，任何时刻崩溃，
磁盘上都至少有一份能把数据完整重建出来的东西（旧 checkpoint+旧WAL，或新 checkpoint+残余WAL），
而幂等 replay 保证重叠部分不会被应用两次。**

> 这三段（写入 / 恢复 / checkpoint 顺序）在 `test_wal.py` 里都有对应用例：
> 尾部半写、checksum 损坏、无效 checkpoint、「checkpoint 完成但旧 WAL 仍在」、
> 轮换后恢复、以及真实节点 `SIGKILL` 后恢复。

---

## 4. Raft log 与 storage WAL 的区别（一定要讲清）

面试官最爱追问这个，因为「你不是已经有 Raft log 了吗，为什么还要一个 WAL？」

| | **Raft log**（`shard.log`） | **Storage WAL**（`wal_<port>.log`） |
|---|---|---|
| 解决什么问题 | **多个节点之间**怎么就「发生了哪些写、顺序如何」达成一致 | **单个节点**崩溃后怎么恢复出自己的状态机 |
| 里面有什么 | 可能包含**还没 committed**的条目（等多数派确认） | **只有已经 committed、准备 apply 的操作** |
| 谁可能改它 | 未提交条目可能被更高 term 的 Leader **截断/覆盖** | 只追加已提交操作，写进去就不再变 |
| 站在哪一层 | 状态机**之前**（共识层） | 状态机**之后**（持久化层） |

**为什么 storage WAL 只记录已 committed 的操作？**

因为未提交的 Raft 条目**还可能被推翻**。设想 Leader 收到一条写、还没拿到多数派就挂了，
新 Leader 上台后这条写可能根本不存在。如果我把「未提交」的操作也写进状态机 WAL，
崩溃恢复时就会把一条「集群从未真正承认过」的写恢复进数据库——违反线性一致性。
所以 storage WAL 站在 `commit_index` 之后：**只有 Raft 已经拍板 committed 的操作，
才交给 storage 层去持久化和恢复。** 复制与「未提交条目的持久化」是 Raft log + 快照的职责，
两套日志各司其职、不重叠。

---

## 5. 8–10 个高频追问 + 参考答案

**Q1. 为什么经典 WAL 是「先写日志再改内存」，你这里为什么可以「先改内存再写 WAL」?**
经典 WAL（如数据库 buffer pool）里，内存脏页可能先于日志被刷盘，一旦崩溃就会出现
「数据变了但没日志」的不一致，所以必须 write-ahead。我这里两个前提不同：
(1) 内存 store 完全易失，每次崩溃都**从 WAL 全量重建**，从不把内存当真相去信任；
(2) apply 内存和 append WAL 在**同一个 `store_lock` 临界区**内完成，对外是原子的。
恢复只依赖「checkpoint + WAL」，跟崩溃瞬间内存是什么无关。所以顺序不影响正确性。
真要抬杠「顺序无所谓吗」——是的，只要两者在同一临界区且恢复只信磁盘。
（若追求「WAL 落盘失败就不应答客户端」的更强语义，可以把 append+flush 放在回 ack 之前，
本实现正是这样：`commit` 返回后才算这次写落地。）

**Q2. `flush()` 和 `fsync()` 到底差在哪？你默认用哪个？**
`flush()` 把字节从 Python 的用户态缓冲交给 **OS 页缓存**；此时进程就算被 `kill -9`，
数据也不丢——因为页缓存属于内核，进程死了它还在，OS 会照常刷盘。
`fsync()` 更进一步，把页缓存强制推到**物理磁盘**，这才能扛**掉电 / 内核崩溃**。
我默认每次 commit 只 `flush()`（性能好，且足够扛进程级崩溃，`test_wal.py` 的 SIGKILL
用例证明了这点），`fsync` 做成可配置（`--fsync`）给需要掉电级持久的场景。
**但 checkpoint 一律 fsync**——因为它是恢复的唯一可信基点，且必须在截断 WAL 之前真正落盘。

**Q3. partial write（半写）怎么检测？为什么不怕「最后一条没写完」?**
用 **length-prefixed 定长帧**：`MAGIC(4) | 长度(4) | payload | crc(4)`。replay 时如果在读某一帧
的过程中撞到文件尾（4 字节魔数不足、payload 不足声明长度、crc 不足）——说明这是被打断的
最后一条，**保留它之前所有完整帧，安全停止**。因为 WAL 是 append-only，半写只可能发生在
文件末尾，所以「读到 EOF 时帧不完整」= 尾部半写。这就是不依赖「换行是否完整」的原因。

**Q4. 那「中间」损坏（不是尾部）怎么办？会被当成半写静默吞掉吗？**
不会。两道防线：(1) 每帧头有 **MAGIC**，如果中间某帧错位，下一帧起始 4 字节对不上魔数
→ 判为损坏抛错；(2) **crc32** 覆盖 payload，帧完整但内容被改 → crc 不符 → 抛错。
两者都抛 `StorageCorruptionError`，绝不静默继续。唯一的边界情况是「长度字段被改成指向文件尾
之外」，这跟真实截断无法区分，我保守地当尾部截断处理（保留前面记录），并在 README 已知局限
里如实写明——彻底解决要 per-record 序列号，超出本任务范围。

**Q5. 怎么保证 replay 幂等？为什么需要幂等？**
每条 WAL 记录带**绝对 log index**，恢复时维护每个分片的 `applied` 水位，
**只应用 `index > applied[分片]` 的记录**，应用后推高水位。所以同一条 index 出现两次、
或 checkpoint 已覆盖的记录又在 WAL 里，第二次都会被跳过。
需要幂等是因为「checkpoint 已发布但 WAL 未截断」这个崩溃点：恢复时 checkpoint 和 WAL
会有一段**重叠**，没有去重就会把那段操作应用两遍（对 `set` 也许无害，但对
「先 set 再 delete 再 set」这种序列，重复应用就错了）。Raft 的安全性保证同一个 committed
index 的值永远相同，所以按 index 去重是可靠的。

**Q6. 写放大（write amplification）体现在哪？你的方案把它从哪搬到了哪？**
旧方案：**每次写**都 `json.dump(整个 store)`，写放大 = O(数据量)，且**每写必付**。
新方案：每次写只 append 一条几十字节的记录，写放大≈O(1)；那个 O(N) 的全量落盘被搬到
**checkpoint**，每 `rotate_records` 次写才付一次，**摊薄**了。基准里能直接看到：
WAL 单写 p50 恒为 0.008ms 不随规模变；而它在 5 万条时总吞吐从 8 万掉到 2.8 万 ops/s，
正是那**一次** O(5万) 的 checkpoint 摊到 1000 次写上造成的——这恰好证明「O(N) 被摊薄了」。
`rotate_records` 就是「写放大 ↔ 恢复时长」的旋钮：调大→checkpoint 更稀、稳态写更快，
但恢复要 replay 的 WAL 更长。

**Q7. checkpoint 和 WAL 轮换的崩溃安全顺序，能再说一遍关键点吗？**
一句话：**先让新 checkpoint fsync + 原子 rename 落盘，之后才截断 WAL。** 反过来（先删 WAL
再写 checkpoint）就有窗口：WAL 没了、checkpoint 又没写成 → 丢数据。原子 rename（`os.replace`）
保证「要么看到旧 checkpoint，要么看到完整新 checkpoint，绝不会看到半个」。配合幂等 replay，
「checkpoint 好了但 WAL 还没轮换」也只是多 replay 一段被去重的记录，不出错。

**Q8. 你怎么保证「所有已提交写都走了 WAL」，不会漏掉某条路径？**
我把持久化收敛成**唯一一条 path**：`persist_committed(records)` → `storage.commit()`。
然后审计了所有会「把 committed 条目 apply 到状态机」的地方，全部改成走它：
批量写 `batch_loop`、事务提交 `_do_raft_op`（txn commit 逐条）、follower 的
`append_entries` apply、以及 follower 安装快照（当作一次 checkpoint 发布）。
这样避免了「set 有 WAL 但 delete 没有」「Leader 有但 Follower 没有」「batch 有但事务绕过」
这类不对称漏写。`test_wal.py` 专门覆盖了 delete / 多分片 / batch / 事务 / follower 恢复。

**Q9. 持久化 I/O 和现有的锁怎么配合？会不会引入死锁或长时间卡锁？**
锁序始终是 `store_lock → engine._lock`，storage 引擎**从不**反向去拿 `store_lock` 或
`shard.lock`，所以不会有新的锁环。我特意确认所有 commit 点都在 `store_lock` 内、
但**不在 `shard.lock` 内**——即不会「持着 Raft 分片锁做磁盘 I/O」把选举/心跳卡住。
唯一会稍微多占锁的是 checkpoint（要在 `store_lock` 内做一次 O(N) fsync），
但它不频繁（每 `rotate_records` 次），我在已知局限里如实标注为「简单可验证优先」的取舍。

**Q10. 为什么不直接上 LSM Tree / SSTable / RocksDB？**
过度设计。这个项目的持久化痛点是**单一的**：全量重写的写放大。WAL + checkpoint 用几百行标准库
就精准解决了它，且崩溃安全性可以逐点白板证明、测试可完全覆盖。LSM 的 SSTable、Bloom filter、
多层 compaction 是为「海量数据 + 高写入 + 范围查询」准备的，会引入我这里根本用不到的复杂度和
新的崩溃面。工程上应该「用最简单、最可验证、最贴合现有结构的方案解决真实瓶颈」，
而不是为了显得复杂而堆架构。需要时，当前的版本号化磁盘格式（记录 / checkpoint 都带 version）
也为将来迁移留了口子。

**Q11.（加问）默认后端为什么保持 legacy JSON，而不是直接切 WAL？**
为了**零回归**。WAL 做成 opt-in，默认路径与旧代码字节兼容，56 个既有测试一行没改就全过——
这让「升级」和「启用新特性」解耦：先安全合入抽象层，再按需在需要崩溃恢复性能的部署上开
`--backend=wal`。同时 legacy 后端留作 benchmark 的对照基准，对比数字才有意义。

---

## 6. 可以主动亮出的验证结果（面试时用来收尾）

- **既有测试零改动**：`python3 test_raft_sharded.py` → 56/56 通过（默认 JSON 后端）。
- **新增 WAL 测试**：`python3 test_wal.py` → 15/15，含真实 3 节点 `SIGKILL` 后恢复。
- **基准（存储层）**：单写 p50，JSON 随规模 0.72ms→35ms（5 万条），WAL 恒为 0.008ms；
  5 万条时 WAL 单写吞吐约为 JSON 的 **1000×**。原始数据在 `benchmarks/storage_results_*.json`。
- 所有测试用临时目录，跑完不留残留文件。
