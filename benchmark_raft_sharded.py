#!/usr/bin/env python3
"""
benchmark_raft_sharded.py —— 分片 Raft KV 存储（v5, node_raft_sharded.py）并发性能基准

测量目标
========
1. 吞吐量        : /set、/get、/txn 各自的 ops/s
2. 延迟分布      : 每种操作的 p50 / p95 / p99 / max（毫秒）
3. 批量写效果    : 串行 /set（concurrency=1）vs 高并发 /set，验证 batch_loop 合并是否真的提吞吐
4. 分片扩展性    : 同一个 3 节点集群内，「所有 key 打到 1 个分片」vs「key 均匀散到 3 个分片」，
                  体现分片带来的并行写能力（同副本因子，干净对比，不掺杂集群规模差异）
5. 并发曲线      : 并发 1 / 10 / 50 / 100 / 200 下的吞吐 + p99，找拐点/瓶颈

两个关键的方法学决定（面试可讲）
================================
(A) 为什么「单分片 vs 3 分片」在同一个 3 节点集群里做？
    node_raft_sharded.py 里 NUM_SHARDS == 集群节点数，分片数不能独立于节点数调整。
    若用「1 节点(1 分片) vs 3 节点(3 分片)」对比，会同时改变副本因子（majority 1 vs 2），
    把「分片并行」和「有没有复制开销」混在一起，不干净。
    所以本脚本固定在同一个 3 节点集群里：
      - 单分片：故意只用哈希落在 shard 0 的 key → 写入全部串行经过 shard 0 的 Leader
      - 三分片：key 均匀散布 → 3 个分片的 Leader（在不同节点）并行提交
    两者副本因子完全相同，唯一差异就是「写入能不能分摊到多个 Raft 组」。

(B) 为什么每个测量点都用「全新的干净集群」？
    源码里 save_to_disk() 每次提交都整体重写全量 store JSON（O(数据量)）。
    这意味着「先跑的测试」store 小、快，「后跑的测试」store 大、慢——测试顺序会污染结果。
    实测差过 6×（同样 concurrency=1，空库 271 ops/s，跑到后面 3000+ key 时只剩 43 ops/s）。
    所以每个测量点前都重启集群、清空状态文件，保证每个数字都是「干净起点」下测出来的，
    彼此可比。所有节点状态文件写到一个临时目录，不污染仓库工作区。

选型理由（为什么纯标准库 + 线程池）
====================================
- HTTP 客户端用标准库 urllib.request：零安装依赖，clone 下来就能跑。
  * 本可用 requests/httpx（连接池）或 aiohttp（协程）。但被测服务端是
    BaseHTTPRequestHandler 且没设 protocol_version → 默认 HTTP/1.0，响应后即断连，
    客户端「连接池复用」这条路本身走不通，requests 的连接池优势在这拿不到。
    用标准库反而更诚实、更可复现。
- 并发用 concurrent.futures.ThreadPoolExecutor：每个线程 = 一个「并发客户端」，
  发阻塞式 HTTP，正好对应真实客户端，也和服务端「一请求一线程」的模型对称。
  并发度 N 就是 N 个线程同时在打请求，面试好讲。
- matplotlib 仅用于出图，可选依赖：没装也能跑，照样产出 CSV/JSON，只跳过 PNG。

用法
====
  python3 benchmark_raft_sharded.py                 # 自动拉起/重启集群，跑全部
  python3 benchmark_raft_sharded.py --quick         # 快速冒烟（请求数调小）
  python3 benchmark_raft_sharded.py --tests curve,batch
  python3 benchmark_raft_sharded.py --no-spawn --target localhost:5001
        # 测已在运行的集群（注意：无法保证干净起点，比较类结果会受测试顺序影响）

结果写到 benchmarks/：results.json / results.csv / *.png
"""

import argparse
import atexit
import csv
import glob
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# matplotlib 是可选的：没装就跳过出图，核心基准照跑
try:
    import matplotlib
    matplotlib.use("Agg")  # 无显示环境也能出图
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    HAS_MPL = True
except Exception:
    HAS_MPL = False

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
NODE_SCRIPT = os.path.join(REPO_DIR, "node_raft_sharded.py")
OUT_DIR = os.path.join(REPO_DIR, "benchmarks")


# ─────────────────────────────────────────────────────────────
# HTTP 客户端：每次请求测一次端到端延迟（含 TCP 建连，因为服务端 HTTP/1.0 会断连）
# ─────────────────────────────────────────────────────────────
def http_post(url, payload, timeout=6.0):
    """POST JSON。返回 (ok, latency_ms, status_code, body_bytes)。永不抛异常。"""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            code = resp.status
    except urllib.error.HTTPError as e:            # 4xx/5xx：服务端明确返回了状态码
        try:
            body = e.read()
        except Exception:
            body = b""
        code = e.code
    except Exception:                              # 连不上 / 超时等
        body = b""
        code = 0
    lat = (time.perf_counter() - t0) * 1000.0
    return (200 <= code < 300), lat, code, body


def http_get(url, timeout=6.0):
    """GET。返回 (ok, latency_ms, status_code, body_bytes)。永不抛异常。"""
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read()
            code = resp.status
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:
            body = b""
        code = e.code
    except Exception:
        body = b""
        code = 0
    lat = (time.perf_counter() - t0) * 1000.0
    return (200 <= code < 300), lat, code, body


# ─────────────────────────────────────────────────────────────
# 与服务端一致的分片哈希（用于分片测试里精确控制 key 落到哪个分片）
# 服务端：int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_SHARDS
# ─────────────────────────────────────────────────────────────
def get_shard(key, num_shards):
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % num_shards


def keys_for_shard(prefix, target_shard, count, num_shards):
    """生成 count 个哈希恰好落在 target_shard 的 key。"""
    out, i = [], 0
    while len(out) < count:
        k = f"{prefix}{i}"
        if get_shard(k, num_shards) == target_shard:
            out.append(k)
        i += 1
    return out


# ─────────────────────────────────────────────────────────────
# 统计工具
# ─────────────────────────────────────────────────────────────
def percentile(sorted_vals, p):
    """线性插值百分位。p ∈ [0,1]。sorted_vals 必须已排序。"""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


class Result:
    """一次负载运行的汇总结果。"""
    def __init__(self, name, concurrency, latencies_ms, ok, err, wall_s):
        self.name = name
        self.concurrency = concurrency
        self.ok = ok
        self.err = err
        self.wall_s = wall_s
        s = sorted(latencies_ms)
        self.count = len(s)
        # 吞吐 = 成功请求数 / 墙钟时间（客户端视角的有效吞吐）
        self.throughput = (ok / wall_s) if wall_s > 0 else 0.0
        self.p50 = percentile(s, 0.50)
        self.p95 = percentile(s, 0.95)
        self.p99 = percentile(s, 0.99)
        self.pmax = s[-1] if s else 0.0
        self.mean = (sum(s) / len(s)) if s else 0.0

    def as_row(self):
        return {
            "test": self.name,
            "concurrency": self.concurrency,
            "ok": self.ok,
            "err": self.err,
            "wall_s": round(self.wall_s, 4),
            "throughput_ops_s": round(self.throughput, 1),
            "mean_ms": round(self.mean, 3),
            "p50_ms": round(self.p50, 3),
            "p95_ms": round(self.p95, 3),
            "p99_ms": round(self.p99, 3),
            "max_ms": round(self.pmax, 3),
        }


# ─────────────────────────────────────────────────────────────
# 负载核心：用固定并发度打 N 个请求，收集每请求延迟
# ─────────────────────────────────────────────────────────────
def run_load(name, op_fn, args_list, concurrency):
    """
    op_fn(arg) -> (ok: bool, latency_ms: float)
    args_list  : 每个元素是一次请求的参数（payload 或 key）
    concurrency: 线程池大小 = 同时在飞的请求数
    吞吐 = 成功数 / 墙钟时间；延迟分位从每请求延迟算。
    """
    latencies, ok, err = [], 0, 0
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(op_fn, a) for a in args_list]
        for f in as_completed(futures):
            success, lat = f.result()
            latencies.append(lat)
            if success:
                ok += 1
            else:
                err += 1
    wall = time.perf_counter() - t0
    return Result(name, concurrency, latencies, ok, err, wall)


# ── 每种操作的 op_fn 工厂 ──────────────────────────────────────
# 说明：写/读都做「客户端侧 Leader 路由」——按 key 的分片，把请求直接发给该分片当前的
# Leader 节点，绕开「随机命中某个 Follower 再转发」这一跳。理由有二：
#   (1) 这正是真实客户端库（TiKV/Cockroach client）的做法，更贴近生产；
#   (2) 去掉「转发命中与否」这个随机变量，让吞吐数字更干净、可比。
# router(key) -> 该 key 所在分片 Leader 的 base URL（如 http://localhost:5002）。
# 若暂时不知道 Leader，则回退到默认 base（服务端仍会自动转发，保证正确性）。
def make_set_op(router):
    def op(kv):
        success, lat, _c, _b = http_post(f"{router(kv['key'])}/set", kv)
        return success, lat
    return op


def make_get_op(router):
    def op(key):
        # 服务端用 path.split("=")[-1] 解析，key 不含 '='，直接拼即可
        success, lat, _c, _b = http_get(f"{router(key)}/get?key={key}")
        return success, lat
    return op


def make_txn_op(base):
    # 事务是跨分片 2PC，任一节点都能当协调者，直接发默认 base 即可。
    def op(ops):
        success, lat, code, body = http_post(f"{base}/txn", {"ops": ops})
        # /txn 即使 aborted 也返回 HTTP 200，需看 body 里的 status
        if success and code == 200:
            try:
                success = json.loads(body).get("status") == "ok"
            except Exception:
                success = False
        return success, lat
    return op


def gen_kv(prefix, n, value_size):
    """生成 n 个 {"key","value"} 写入 payload。"""
    val = "x" * value_size
    return [{"key": f"{prefix}{i}", "value": val} for i in range(n)]


# ─────────────────────────────────────────────────────────────
# 集群管理：拉起 / 关闭 / 清空状态 / 等待选举
# 所有节点状态文件写到 STATE_DIR（临时目录），不污染仓库工作区。
# ─────────────────────────────────────────────────────────────
_spawned = []


def spawn_cluster(ports, state_dir, logdir):
    """在 state_dir 里拉起 len(ports) 节点的集群（cwd=state_dir，状态文件都落这）。"""
    os.makedirs(state_dir, exist_ok=True)
    os.makedirs(logdir, exist_ok=True)
    for p in ports:
        peers = [str(x) for x in ports if x != p]
        logf = open(os.path.join(logdir, f"node_{p}.log"), "w")
        proc = subprocess.Popen(
            [sys.executable, NODE_SCRIPT, str(p), *peers],
            cwd=state_dir, stdout=logf, stderr=subprocess.STDOUT,
        )
        _spawned.append((proc, logf))


def stop_cluster():
    for proc, _ in _spawned:
        try:
            proc.terminate()
        except Exception:
            pass
    for proc, logf in _spawned:
        try:
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        try:
            logf.close()
        except Exception:
            pass
    _spawned.clear()


atexit.register(stop_cluster)


def clear_state(state_dir):
    """清空临时状态目录里的 data/snapshot 文件，保证下一次是干净起点。"""
    for f in glob.glob(os.path.join(state_dir, "data_raft_sharded_*.json")):
        try:
            os.remove(f)
        except Exception:
            pass
    for f in glob.glob(os.path.join(state_dir, "snapshot_*.json")):
        try:
            os.remove(f)
        except Exception:
            pass


def wait_for_ready(ports, num_shards, timeout=30.0):
    """轮询 /health，直到每个分片都选出了 Leader（避免把选举/冷启动算进计时）。"""
    deadline = time.time() + timeout
    base = f"http://localhost:{ports[0]}"
    while time.time() < deadline:
        ok, _lat, code, body = http_get(f"{base}/health", timeout=2.0)
        if ok and code == 200:
            try:
                shards = json.loads(body).get("shards", {})
                have = [shards.get(str(s), {}).get("leader") is not None
                        for s in range(num_shards)]
                if len(have) == num_shards and all(have):
                    return True
            except Exception:
                pass
        time.sleep(0.4)
    print("  ⚠️  等待选举超时，仍继续（结果可能受选举影响）")
    return False


def discover_leaders(host, ports, num_shards):
    """查询 /health，返回 {shard_id: leader_port}（用于客户端侧 Leader 路由）。"""
    leaders = {}
    ok, _lat, code, body = http_get(f"http://{host}:{ports[0]}/health", timeout=2.0)
    if ok and code == 200:
        try:
            shards = json.loads(body).get("shards", {})
            for s in range(num_shards):
                leaders[s] = shards.get(str(s), {}).get("leader")
        except Exception:
            pass
    return leaders


# ─────────────────────────────────────────────────────────────
# prep：每个测量点前的准备。
#   spawn 模式 → 重启一个干净集群 + 小预热（保证「干净起点」，各测量点可比）
#   --no-spawn → 只做小预热（无法清库，比较类结果会受顺序影响，已在报告里标注）
# 每次 prep 后刷新 self.leaders，供客户端侧 Leader 路由使用。
# ─────────────────────────────────────────────────────────────
class Cluster:
    def __init__(self, host, ports, num_shards, base, spawn, state_dir, logdir,
                 value_size, warmup_n):
        self.host = host
        self.ports = ports
        self.num_shards = num_shards
        self.base = base
        self.spawn = spawn
        self.state_dir = state_dir
        self.logdir = logdir
        self.value_size = value_size
        self.warmup_n = warmup_n
        self.leaders = {}

    def router(self, key):
        """按 key 的分片返回该分片 Leader 的 base URL；未知则回退默认 base。"""
        lp = self.leaders.get(get_shard(key, self.num_shards))
        return f"http://{self.host}:{lp}" if lp else self.base

    def leader_spread(self):
        """当前有多少个不同的节点在担任分片 Leader（1 表示全挤在一个节点上）。"""
        vals = [v for v in self.leaders.values() if v]
        return len(set(vals))

    def prep(self):
        """重启（若 spawn）到干净状态，等选举，刷新 Leader 视图，预热。返回 self。"""
        if self.spawn:
            stop_cluster()
            time.sleep(1.0)                       # 让端口从 TIME_WAIT 释放
            clear_state(self.state_dir)
            spawn_cluster(self.ports, self.state_dir, self.logdir)
            time.sleep(1.5)
            wait_for_ready(self.ports, self.num_shards)
        self.leaders = discover_leaders(self.host, self.ports, self.num_shards)
        # 小预热：写一批，热身连接和状态机（不计入结果）
        if self.warmup_n > 0:
            kvs = gen_kv("warmup_", self.warmup_n, self.value_size)
            run_load("warmup", make_set_op(self.router), kvs, concurrency=20)
        return self


# ── median-of-N 辅助：某些「headline」对比（批量/分片）单次波动大，
#    重复 repeats 次、每次都在干净集群上测，取吞吐中位数那一次，稳一些。──
def median_run(cl, name, make_args, concurrency, repeats, op_kind="set"):
    """
    重复 repeats 次：每次 prep（干净集群 + 刷新 Leader）后跑一次负载。
    make_args() 每次返回该次要发的参数列表。返回吞吐中位数的那次 Result，
    以及一个 meta（记录每次吞吐、以及最后一次的 Leader 分布）。
    """
    runs, tputs, spreads = [], [], []
    for _ in range(repeats):
        cl.prep()
        if op_kind == "set":
            op = make_set_op(cl.router)
        else:
            op = make_txn_op(cl.base)
        r = run_load(name, op, make_args(), concurrency)
        runs.append(r)
        tputs.append(round(r.throughput, 1))
        spreads.append(cl.leader_spread())
    order = sorted(range(len(runs)), key=lambda i: runs[i].throughput)
    med = runs[order[len(order) // 2]]
    meta = {"runs_ops_s": tputs, "leader_spread_each": spreads}
    return med, meta


# ─────────────────────────────────────────────────────────────
# 各项测试（每个独立测量点前调用 cl.prep() 拿到干净起点 + 客户端 Leader 路由）
# ─────────────────────────────────────────────────────────────
def test_curve(cl, total, concurrencies, repeats):
    """并发曲线：每个并发度都在干净集群上测 /set，取 repeats 次吞吐中位数（压掉单机尖峰）。"""
    print(f"\n=== 测试：并发曲线（/set 吞吐 & p99 vs 并发度，每点各取{repeats}次中位数）===")
    results = []
    for c in concurrencies:
        r, meta = median_run(
            cl, f"set@c{c}",
            lambda: gen_kv(f"curve_c{c}_", total, cl.value_size),
            c, repeats)
        results.append(r)
        print(f"  并发 {c:>3}: {r.throughput:>8.0f} ops/s | "
              f"p50={r.p50:6.2f}ms p99={r.p99:7.2f}ms max={r.pmax:7.2f}ms | "
              f"各次={meta['runs_ops_s']}")
    return results


def test_per_op(cl, total, concurrency):
    """/set /get /txn 各自的吞吐 + 延迟分布（同一并发度，各自干净起点）。"""
    print(f"\n=== 测试：三种操作吞吐 & 延迟（并发={concurrency}）===")
    results = []

    # /set
    cl.prep()
    r_set = run_load("set", make_set_op(cl.router),
                     gen_kv("perop_set_", total, cl.value_size), concurrency)
    results.append(r_set)

    # /get：同一集群里先写后读（不能在 prefill 和 get 之间重启，否则 key 没了）
    cl.prep()
    prefill = gen_kv("perop_get_", total, cl.value_size)
    run_load("get_prefill", make_set_op(cl.router), prefill, concurrency)
    r_get = run_load("get", make_get_op(cl.router),
                     [kv["key"] for kv in prefill], concurrency)
    results.append(r_get)

    # /txn：每个事务 2 个 key，key 唯一（不会因锁冲突 abort），天然可能跨分片
    cl.prep()
    val = "x" * cl.value_size
    txns = [[{"key": f"perop_txn_{i}_a", "value": val},
             {"key": f"perop_txn_{i}_b", "value": val}] for i in range(total)]
    r_txn = run_load("txn", make_txn_op(cl.base), txns, concurrency)
    results.append(r_txn)

    for r in results:
        note = f" | {r.err} 个 abort/失败" if r.err else ""
        print(f"  {r.name:>4}: {r.throughput:>8.0f} ops/s | "
              f"p50={r.p50:6.2f}ms p95={r.p95:6.2f}ms p99={r.p99:7.2f}ms "
              f"max={r.pmax:7.2f}ms{note}")
    return results


def test_batch(cl, total, concurrent_c, repeats):
    """
    批量写效果：串行(concurrency=1) vs 高并发（两臂各自干净起点，取中位数，公平对比）。
    串行时每个 /set 独占一次 Raft round（batch 里只有 1 条）→ 吞吐 ≈ 1/延迟。
    高并发时 5ms 窗口内多个 /set 被 batch_loop 合并（最多 20 条/round）→ 吞吐应大幅提升。
    """
    print(f"\n=== 测试：批量写效果（串行 c=1 vs 并发 c={concurrent_c}，各取{repeats}次中位数）===")
    serial, m_s = median_run(cl, "set_serial",
                             lambda: gen_kv("batch_serial_", total, cl.value_size),
                             1, repeats)
    conc, m_c = median_run(cl, "set_concurrent",
                           lambda: gen_kv("batch_conc_", total, cl.value_size),
                           concurrent_c, repeats)
    speedup = (conc.throughput / serial.throughput) if serial.throughput > 0 else 0
    print(f"  串行 (c=1)     : {serial.throughput:>8.0f} ops/s | p50={serial.p50:6.2f}ms "
          f"p99={serial.p99:7.2f}ms | 各次={m_s['runs_ops_s']}")
    print(f"  并发 (c={concurrent_c:<3})   : {conc.throughput:>8.0f} ops/s | p50={conc.p50:6.2f}ms "
          f"p99={conc.p99:7.2f}ms | 各次={m_c['runs_ops_s']}")
    print(f"  ⇒ 批量合并带来 ~{speedup:.1f}× 吞吐提升")
    return [serial, conc], speedup, {"serial": m_s, "concurrent": m_c}


def test_shard(cl, total, concurrency, repeats):
    """
    分片扩展性（同一集群，副本因子相同，两臂各自干净起点，取中位数）：
      单分片：所有 key 打到 shard 0 → 写入全部经过一个分片的 Leader
      多分片：key 均匀散到所有分片 → 多个分片的 Leader 并行提交

    重要前提（面试必讲）：每个节点是独立进程、有各自的 store/锁/磁盘文件，
    所以「分片并行提吞吐」只有当 3 个分片 Leader 落在不同节点时才成立。
    Leader 落点由选举随机决定，本脚本会把「Leader 分布在几个节点上」记录下来，
    好解释为什么某次分片没提速（若 3 个 Leader 挤在同一节点，分片就退化成串行）。
    """
    ns = cl.num_shards
    val = "x" * cl.value_size
    print(f"\n=== 测试：分片扩展性（单分片 vs {ns}分片，并发={concurrency}，各取{repeats}次中位数）===")

    dist = [0] * ns
    for i in range(total):
        dist[get_shard(f"shardN_{i}", ns)] += 1

    r_one, m_one = median_run(
        cl, "shard_single",
        lambda: [{"key": k, "value": val}
                 for k in keys_for_shard("shard1_", 0, total, ns)],
        concurrency, repeats)
    r_spread, m_spread = median_run(
        cl, "shard_spread",
        lambda: gen_kv("shardN_", total, cl.value_size),
        concurrency, repeats)

    ratio = (r_spread.throughput / r_one.throughput) if r_one.throughput > 0 else 0
    print(f"  单分片 (全落 shard0)  : {r_one.throughput:>8.0f} ops/s | p99={r_one.p99:7.2f}ms "
          f"| 各次={m_one['runs_ops_s']}")
    print(f"  {ns} 分片 (散布 {dist}) : {r_spread.throughput:>8.0f} ops/s | p99={r_spread.p99:7.2f}ms "
          f"| 各次={m_spread['runs_ops_s']}")
    print(f"  多分片时 Leader 落在几个不同节点上（各次）: {m_spread['leader_spread_each']} "
          f"（=节点数 才可能真并行；越小越退化成串行）")
    print(f"  ⇒ 3 分片 / 单分片 吞吐比 = {ratio:.2f}×")
    print(f"     注：单机上此比值常 <1。原因（见 README）：(a) 打散负载会稀释每个分片的")
    print(f"     batch 队列深度→合并因子下降→Raft round 变多；(b) 所有分片共用同一台机器的")
    print(f"     CPU（GIL）；(c) Leader 常挤在少数节点。分片的『并行写』收益要在多机部署、")
    print(f"     且 Leader 分散到不同机器时才体现。")
    return [r_one, r_spread], ratio, {"single": m_one, "spread": m_spread}


# ─────────────────────────────────────────────────────────────
# 输出：CSV / JSON / 图
# ─────────────────────────────────────────────────────────────
def save_csv(all_results, path):
    rows = [r.as_row() for r in all_results]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  💾 CSV  → {path}")


def save_json(payload, path):
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"  💾 JSON → {path}")


def plot_curve(curve, outdir):
    cs = [r.concurrency for r in curve]
    tput = [r.throughput for r in curve]
    p99 = [r.p99 for r in curve]

    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax1.plot(cs, tput, "o-", color="#22aa77", label="throughput")
    ax1.set_xlabel("concurrency (concurrent clients)")
    ax1.set_ylabel("throughput (ops/s)", color="#22aa77")
    ax1.tick_params(axis="y", labelcolor="#22aa77")
    ax1.set_xscale("log")
    ax1.set_xticks(cs)
    ax1.get_xaxis().set_major_formatter(mticker.ScalarFormatter())

    ax2 = ax1.twinx()
    ax2.plot(cs, p99, "s--", color="#ee5555", label="p99 latency")
    ax2.set_ylabel("p99 latency (ms)", color="#ee5555")
    ax2.tick_params(axis="y", labelcolor="#ee5555")

    plt.title("/set throughput & p99 vs concurrency")
    fig.tight_layout()
    p = os.path.join(outdir, "throughput_vs_concurrency.png")
    fig.savefig(p, dpi=120)
    plt.close(fig)
    print(f"  🖼  {p}")


def plot_latency_dist(per_op, outdir):
    names = [r.name for r in per_op]
    metrics = ["p50", "p95", "p99", "pmax"]
    labels = ["p50", "p95", "p99", "max"]
    x = range(len(names))
    width = 0.2
    fig, ax = plt.subplots(figsize=(8, 5))
    for i, (m, lab) in enumerate(zip(metrics, labels)):
        vals = [getattr(r, m) for r in per_op]
        ax.bar([xi + i * width for xi in x], vals, width, label=lab)
    ax.set_xticks([xi + 1.5 * width for xi in x])
    ax.set_xticklabels(names)
    ax.set_ylabel("latency (ms)")
    ax.set_title("latency distribution per operation (p50/p95/p99/max)")
    ax.legend()
    fig.tight_layout()
    p = os.path.join(outdir, "latency_distribution.png")
    fig.savefig(p, dpi=120)
    plt.close(fig)
    print(f"  🖼  {p}")


def plot_pair(pair, labels, title, fname, outdir):
    tput = [r.throughput for r in pair]
    fig, ax = plt.subplots(figsize=(6, 5))
    bars = ax.bar(labels, tput, color=["#8899aa", "#22aa77"])
    ax.set_ylabel("throughput (ops/s)")
    ax.set_title(title)
    for b, v in zip(bars, tput):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.0f}",
                ha="center", va="bottom")
    fig.tight_layout()
    p = os.path.join(outdir, fname)
    fig.savefig(p, dpi=120)
    plt.close(fig)
    print(f"  🖼  {p}")


# ─────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="分片 Raft KV (v5) 并发性能基准",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--target", default="localhost:5001", help="打请求的目标节点 host:port")
    ap.add_argument("--ports", default="5001,5002,5003", help="集群端口（逗号分隔）")
    ap.add_argument("--no-spawn", action="store_true",
                    help="不自动拉起/重启集群，测已在运行的集群（比较类结果会受测试顺序影响）")
    ap.add_argument("--requests", type=int, default=2000, help="每个测试的请求总数")
    ap.add_argument("--warmup", type=int, default=200, help="每个测量点前的预热请求数")
    ap.add_argument("--value-size", type=int, default=100, help="value 字节数")
    ap.add_argument("--concurrency", default="1,10,50,100,200", help="并发曲线的并发度列表")
    ap.add_argument("--per-op-concurrency", type=int, default=50,
                    help="per-op / batch / shard 测试用的并发度")
    ap.add_argument("--tests", default="curve,perop,batch,shard",
                    help="要跑哪些：curve,perop,batch,shard")
    ap.add_argument("--repeats", type=int, default=3,
                    help="批量/分片对比每臂重复几次取中位数（降低单次波动）")
    ap.add_argument("--quick", action="store_true",
                    help="快速冒烟：requests=400, warmup=50, repeats=1")
    ap.add_argument("--outdir", default=OUT_DIR, help="结果输出目录")
    args = ap.parse_args()

    if args.quick:
        args.requests = 400
        args.warmup = 50
        args.repeats = 1

    ports = [int(p) for p in args.ports.split(",")]
    num_shards = len(ports)
    concurrencies = [int(c) for c in args.concurrency.split(",")]
    tests = [t.strip() for t in args.tests.split(",")]
    host = args.target.split(":")[0]
    base = f"http://{args.target}"
    os.makedirs(args.outdir, exist_ok=True)
    state_dir = os.path.join(args.outdir, "_cluster_state")
    logdir = os.path.join(args.outdir, "node_logs")

    print("=" * 70)
    print("分片 Raft KV (v5) 并发性能基准")
    print(f"  目标={args.target}  集群端口={ports}  分片数={num_shards}")
    print(f"  requests/测试={args.requests}  value={args.value_size}B  "
          f"per-op并发={args.per_op_concurrency}")
    print(f"  批处理参数(源码): BATCH_MAX_SIZE=20, BATCH_TIMEOUT=5ms")
    print(f"  隔离策略: {'每个测量点重启到干净集群' if not args.no_spawn else '不重启(--no-spawn)'}")
    print("=" * 70)

    if args.no_spawn:
        print("\n[集群] --no-spawn：使用已在运行的集群（不清库）")
        wait_for_ready(ports, num_shards, timeout=10)
    else:
        # 第一次拉起交给 cl.prep() 统一处理（它会先 stop 再 clean-spawn）
        print("\n[集群] 将自动拉起/重启（状态文件写到临时目录，不动仓库工作区）")

    cl = Cluster(host, ports, num_shards, base, spawn=not args.no_spawn,
                 state_dir=state_dir, logdir=logdir,
                 value_size=args.value_size, warmup_n=args.warmup)

    all_results = []
    curve = per_op = batch_pair = shard_pair = None
    batch_speedup = shard_speedup = None
    batch_meta = shard_meta = None

    if "curve" in tests:
        curve = test_curve(cl, args.requests, concurrencies, args.repeats)
        all_results += curve
    if "perop" in tests:
        per_op = test_per_op(cl, args.requests, args.per_op_concurrency)
        all_results += per_op
    if "batch" in tests:
        batch_pair, batch_speedup, batch_meta = test_batch(
            cl, args.requests, args.per_op_concurrency, args.repeats)
        all_results += batch_pair
    if "shard" in tests:
        shard_pair, shard_speedup, shard_meta = test_shard(
            cl, args.requests, args.per_op_concurrency, args.repeats)
        all_results += shard_pair

    # ── 保存结果 ──
    print("\n[输出]")
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "target": args.target, "ports": ports, "num_shards": num_shards,
            "requests_per_test": args.requests, "value_size": args.value_size,
            "per_op_concurrency": args.per_op_concurrency,
            "repeats_for_comparisons": args.repeats,
            "batch_max_size": 20, "batch_timeout_ms": 5,
            "isolation": "fresh cluster per measurement" if not args.no_spawn else "no-spawn",
            "routing": "client-side leader routing (per-key → shard leader)",
            "transport": "HTTP/1.0 + JSON (urllib, 每请求新建 TCP 连接)",
            "python": sys.version.split()[0],
        },
        "results": [r.as_row() for r in all_results],
        "batch_speedup": round(batch_speedup, 2) if batch_speedup else None,
        "shard_speedup": round(shard_speedup, 2) if shard_speedup else None,
        "batch_meta": batch_meta,
        "shard_meta": shard_meta,
    }
    save_json(payload, os.path.join(args.outdir, "results.json"))
    if all_results:
        save_csv(all_results, os.path.join(args.outdir, "results.csv"))

    # ── 出图 ──
    if HAS_MPL:
        if curve:
            plot_curve(curve, args.outdir)
        if per_op:
            plot_latency_dist(per_op, args.outdir)
        if batch_pair:
            plot_pair(batch_pair, ["serial c=1", f"concurrent c={args.per_op_concurrency}"],
                      f"batch write effect (~{batch_speedup:.1f}x)",
                      "batch_effect.png", args.outdir)
        if shard_pair:
            plot_pair(shard_pair, ["1 shard (concentrated)", f"{num_shards} shards (spread)"],
                      f"write throughput: concentrated vs spread ({shard_speedup:.2f}x)",
                      "shard_scalability.png", args.outdir)
    else:
        print("  ⚠️  未安装 matplotlib，跳过出图（CSV/JSON 已生成）。")
        print("     出图： python3 -m pip install matplotlib && 重跑，或用 CSV 自行画图")

    # ── 瓶颈说明（如实标注，方便说 gRPC 优化空间）──
    print("\n" + "=" * 70)
    print("瓶颈说明（写进 README / 面试用）")
    print("-" * 70)
    print("本基准测的是『Python + HTTP/1.0 + JSON』这套传输栈的性能，已知瓶颈：")
    print("  1. HTTP/1.0 无 keep-alive：每请求新建 TCP 连接 + 新起服务端线程，")
    print("     建连/线程调度开销占比不小 → 换 gRPC/HTTP2 持久连接可直接省掉。")
    print("  2. JSON 编解码 + Python GIL：CPU 绑定，多核用不满 → protobuf 编解码更快。")
    print("  3. save_to_disk() 每次提交整体重写全量 store JSON：O(数据量) 磁盘写，")
    print("     数据越大越慢 → 换 WAL 追加写 / 增量持久化可去掉随数据量增长的成本。")
    print("  4. 批量窗口 BATCH_TIMEOUT=5ms 是吞吐/延迟旋钮：调大提吞吐、加尾延迟。")
    print("=" * 70)

    if not args.no_spawn:
        print("\n[集群] 关闭 ...")
        stop_cluster()
        # 顺手清掉临时状态目录（图和 CSV/JSON 都在 outdir 根，不受影响）
        try:
            shutil.rmtree(state_dir)
        except Exception:
            pass


if __name__ == "__main__":
    main()
