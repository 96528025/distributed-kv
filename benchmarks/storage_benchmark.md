# 持久化后端基准：Legacy JSON 全量重写 vs WAL 追加

脚本：[`../benchmark_storage.py`](../benchmark_storage.py)

```bash
python3 benchmark_storage.py                 # 全部规模（100 / 1k / 10k / 50k）
python3 benchmark_storage.py --quick         # 快速冒烟
python3 benchmark_storage.py --fsync         # 打开 fsync 再测一遍
python3 benchmark_storage.py --scales=100,1000
```

原始结果按 **日期 + fsync 标记**保存，不覆盖既有文件：
`storage_results_<日期>.json` / `storage_results_<日期>.csv`。

---

## 为什么单独测「持久化层」

集群级基准（[`README.md`](README.md)）里，写路径的耗时被 HTTP/1.0、JSON 编解码、
Raft 多数派复制、GIL、单机 CPU 争抢等**共同**主导，持久化只是其中一环，噪声太大，
看不清「换 WAL 到底省了多少」。

本基准把 `StorageEngine` 单拎出来直接压测（无 HTTP、无 Raft、无网络），
干净地回答一个问题：

> **legacy `save_to_disk()` 每次提交整体重写全量 store，单次提交成本是否随数据量线性增长？WAL 追加是否与数据量无关？**

方法：预置 store 已有 N 条数据（一次 baseline 落盘建立稳态起点），然后计时写入
1000 条新数据，测单次 commit 延迟分位与吞吐；再测恢复时间与落盘体积。
每个点重复 3 次取中位数。

---

## 结果（2026-07-23，单机 macOS，Python 3.14.6，fsync=off）

### 1. 单条 commit，跨数据规模 —— 核心对比

| backend | scale | 吞吐 (ops/s) | p50 | p99 | 恢复 | 落盘 |
|---|---:|---:|---:|---:|---:|---:|
| JSON | 100   | 1400 | 0.72ms | 1.17ms | 0.34ms | 30 KB |
| WAL  | 100   | **86,423** | **0.008ms** | 0.018ms | 0.52ms | 32 KB |
| JSON | 1,000 | 769 | 1.29ms | 1.82ms | 0.57ms | 57 KB |
| WAL  | 1,000 | **80,644** | **0.008ms** | 0.019ms | 0.82ms | 62 KB |
| JSON | 10,000 | 132 | 7.54ms | 8.71ms | 2.89ms | 339 KB |
| WAL  | 10,000 | **61,228** | **0.008ms** | 0.017ms | 4.14ms | 360 KB |
| JSON | 50,000 | **28** | **35.3ms** | 44.9ms | 14.5ms | 1628 KB |
| WAL  | 50,000 | **28,595** | **0.008ms** | 0.018ms | 20.8ms | 1728 KB |

**结论（实测，非推断）：**

1. **legacy JSON 的写放大是真实且线性的。** 数据从 100 → 50,000 条（500×），
   JSON 单次写吞吐从 1400 → 28 ops/s（跌 ~50×），p50 从 0.72ms → 35ms（涨 ~49×）。
   这正是「每次提交都 `json.dump(整个 store)`」的 O(N) 成本。
2. **WAL 的单次写与数据量无关。** 同样从 100 → 50,000 条，WAL 的 p50 稳定在 **0.008ms**，
   因为它只 append 一条定长帧记录。50k 规模下 WAL 每次写约比 JSON 快 **~1000×**。
3. **代价一：落盘体积略大。** WAL 是 append 日志，比紧凑的 JSON 稍大（50k：1728KB vs 1628KB），
   但由 checkpoint 轮换封顶（见下）。
4. **代价二：恢复要 replay。** WAL 恢复（checkpoint + replay）比读单个 JSON 文件略慢，
   但绝对值很小（50k：20.8ms vs 14.5ms）。

### 2. WAL 吞吐为何在大规模下也从 86k 降到 28k？

不是写放大——是 **checkpoint 轮换**。本基准 `rotate_records=1000`，而每个点正好写 1000 条，
于是每轮触发**一次** checkpoint，那一次要把整个 store（O(N)）重写一遍。
scale=50k 时这一次 O(50k) 的 checkpoint（≈20ms）摊到 1000 次写上，就把总时间从
「1000×0.008ms=8ms」抬到「~28ms」，吞吐随之下降。

这恰恰说明 WAL 的设计取舍：**把 O(N) 的全量落盘从「每次写」摊薄到「每 rotate_records 次写一次」。**
`rotate_records` 越大，checkpoint 越稀、稳态写越快，但恢复要 replay 的 WAL 越长。
这是一个明确、可调的旋钮（写放大 ↔ 恢复时长）。p50/p99 始终是 0.008/0.018ms，
说明绝大多数写都是纯 append，只有轮换那一下是尖峰。

### 3. Batching 场景（scale=1000）

| backend | batch | 吞吐 (ops/s) | p50 | p99 |
|---|---:|---:|---:|---:|
| JSON | 1  | 766 | 1.30ms | 2.24ms |
| WAL  | 1  | 78,962 | 0.008ms | 0.022ms |
| JSON | 10 | 7,602 | 0.130ms | 0.187ms |
| WAL  | 10 | 149,872 | 0.005ms | 0.008ms |
| JSON | 50 | 35,955 | 0.026ms | 0.036ms |
| WAL  | 50 | 159,656 | 0.005ms | 0.007ms |

batching 对**两个后端都有利**（JSON 把「B 次全量重写」压成「1 次」；WAL 把 B 条帧一次性
写盘），但 WAL 在每个 batch 尺寸上都快一个数量级。

---

## 方差与诚实声明

- 上表是每点 3 次的**中位数**；`storage_results_*.json` 里同时保存了 `throughput_min/max`。
- 单机 + OS 页缓存下，绝对吞吐会随后台负载波动（WAL 单写吞吐在 6 万~9 万 ops/s 间浮动），
  但**相对结论**（JSON 随规模线性劣化、WAL 基本持平）在每次运行里都稳定成立。
- fsync=off：`flush()` 把数据交给 OS 页缓存，足以扛**进程崩溃/SIGKILL**（见测试 14）；
  要扛**掉电**需 `--fsync`，会显著降低吞吐（每次写多一次磁盘同步），可另存一份
  `storage_results_<日期>_fsync.*` 对比。
- 这是**存储层微基准**，不代表端到端集群吞吐（后者仍受 HTTP/Raft/GIL 主导，见集群 README）。
