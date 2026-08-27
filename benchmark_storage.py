"""
benchmark_storage.py — 公平对比 legacy JSON 全量重写 vs WAL 追加 两种持久化后端

**只测持久化层**（这正是本次改动的部分），把 Raft/HTTP/GIL 等噪声排除在外，
从而干净地展示：
  - legacy JSON `save_to_disk()` 每次提交整体重写全量 store → 单次提交成本 O(数据量)；
  - WAL append → 单次提交成本与数据量无关（O(1) 追加）。

测量维度（均写入原始 JSON + CSV，按 backend + 日期区分，不覆盖已有结果）：
  - write throughput（ops/s）
  - p50 / p95 / p99 write latency（单次 commit 端到端）
  - 不同数据规模（store 预置 N 条）下的表现
  - WAL/checkpoint storage growth（落盘文件字节数）
  - restart recovery time（storage.load() 耗时）
  - batching 场景（每次 commit 合并 B 条）

运行：
  python3 benchmark_storage.py                     # 全部规模
  python3 benchmark_storage.py --quick             # 小规模快速冒烟
  python3 benchmark_storage.py --quick --no-save   # 冒烟但不生成结果文件
  python3 benchmark_storage.py --fsync             # 打开 fsync 再测一遍
  python3 benchmark_storage.py --scales 100,1000   # 自定义数据规模

注意：结果是「单机 Python 文件 I/O + OS 页缓存」下的量级参考；方差如实记录。
"""

from __future__ import annotations

import os
import sys
import csv
import json
import time
import shutil
import statistics
import tempfile
from datetime import date

import storage as st


def _pctl(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round(q * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def _summarize_latencies(lat_ms: list[float]) -> dict:
    s = sorted(lat_ms)
    return {
        "p50_ms": round(_pctl(s, 0.50), 4),
        "p95_ms": round(_pctl(s, 0.95), 4),
        "p99_ms": round(_pctl(s, 0.99), 4),
        "max_ms": round(s[-1], 4) if s else 0.0,
        "mean_ms": round(statistics.fmean(s), 4) if s else 0.0,
    }


def _make_engine(backend: str, d: str, port: int, fsync: bool):
    cfg = st.StorageConfig(backend=backend, data_dir=d, port=port,
                           fsync=fsync, rotate_records=1000)
    return st.create_storage_engine(cfg)


def _dir_bytes(d: str, port: int) -> int:
    total = 0
    for name in os.listdir(d):
        if str(port) in name:
            total += os.path.getsize(os.path.join(d, name))
    return total


def bench_one(backend: str, scale: int, n_writes: int, batch: int,
              fsync: bool, reps: int) -> dict:
    """对一个 (backend, scale, batch) 组合跑 reps 次，返回聚合结果。"""
    rep_throughput = []
    rep_lat = []          # 汇总所有 rep 的单次 commit 延迟
    rep_recovery_ms = []
    storage_bytes = 0

    for rep in range(reps):
        d = tempfile.mkdtemp(prefix="bench_")
        port = 40000 + rep
        try:
            eng = _make_engine(backend, d, port, fsync)
            store = eng.load()

            # 预置 scale 条数据（模拟“库里已经有 N 条”）——不计入计时。
            # 用一次 baseline 落盘建立“磁盘上已有 N 条”的起点：
            #   JSON → 一次全量重写；WAL → 一次 checkpoint（即 rotation 后的现实状态）。
            # 这样 seeding 是 O(N) 而非 O(N^2)，且能公平反映稳态下的单次 commit 成本。
            for i in range(scale):
                store[f"seed{i}"] = "x" * 16
            idx = scale
            if backend == "wal":
                eng.checkpoint(store, {0: idx - 1} if scale else None)
            else:
                eng.commit(store, [st.WalRecord(0, 0, 1, "set", "seed0", "x" * 16)]) if scale else None

            # 计时：写 n_writes 条新数据，每 batch 条合并成一次 commit
            latencies = []
            t0 = time.perf_counter()
            done = 0
            while done < n_writes:
                b = min(batch, n_writes - done)
                recs = []
                for j in range(b):
                    k = f"w{done + j}"
                    store[k] = "y" * 16
                    recs.append(st.WalRecord(0, idx, 1, "set", k, "y" * 16))
                    idx += 1
                c0 = time.perf_counter()
                eng.commit(store, recs)
                c1 = time.perf_counter()
                # 单次 commit 的延迟（batch 情况下摊到每条，便于跨 batch 比较）
                per_op = (c1 - c0) * 1000.0 / b
                latencies.extend([per_op] * b)
                done += b
            t1 = time.perf_counter()

            elapsed = t1 - t0
            rep_throughput.append(n_writes / elapsed if elapsed > 0 else 0.0)
            rep_lat.extend(latencies)
            storage_bytes = _dir_bytes(d, port)
            eng.close()

            # 恢复时间：新引擎 load()
            r0 = time.perf_counter()
            eng2 = _make_engine(backend, d, port, fsync)
            recovered = eng2.load()
            r1 = time.perf_counter()
            rep_recovery_ms.append((r1 - r0) * 1000.0)
            assert len(recovered) == scale + n_writes, \
                f"{backend}: recovered {len(recovered)} != {scale + n_writes}"
            eng2.close()
        finally:
            shutil.rmtree(d, ignore_errors=True)

    lat = _summarize_latencies(rep_lat)
    return {
        "backend": backend,
        "scale": scale,
        "n_writes": n_writes,
        "batch": batch,
        "fsync": fsync,
        "reps": reps,
        "throughput_median": round(statistics.median(rep_throughput), 1),
        "throughput_min": round(min(rep_throughput), 1),
        "throughput_max": round(max(rep_throughput), 1),
        **lat,
        "recovery_ms_median": round(statistics.median(rep_recovery_ms), 3),
        "storage_bytes": storage_bytes,
    }


def main():
    args = sys.argv[1:]
    quick = "--quick" in args
    fsync = "--fsync" in args
    save_results = "--no-save" not in args
    scales = None
    for a in args:
        if a.startswith("--scales="):
            scales = [int(x) for x in a.split("=", 1)[1].split(",")]
        elif a == "--scales" and args.index(a) + 1 < len(args):
            scales = [int(x) for x in args[args.index(a) + 1].split(",")]

    if scales is None:
        scales = [100, 1000] if quick else [100, 1000, 10000, 50000]
    n_writes = 200 if quick else 1000
    reps = 2 if quick else 3

    print("═" * 70)
    print(f"Storage backend benchmark: full JSON rewrite vs WAL append")
    print(f"  scales={scales}  writes per point={n_writes}  repetitions={reps}  fsync={fsync}")
    print(f"  Python {sys.version.split()[0]}  platform {sys.platform}")
    print("═" * 70)

    rows = []
    # 1) 单条写（batch=1）跨数据规模：核心对比，暴露 O(n) vs O(1)
    print("\n[1] Single-record commits across data scales (exposes write amplification in the legacy backend)")
    print(f"{'backend':>7} {'scale':>7} {'thpt(ops/s)':>12} {'p50ms':>8} {'p99ms':>8} "
          f"{'recov_ms':>9} {'disk_KB':>8}")
    for scale in scales:
        for backend in ("json", "wal"):
            r = bench_one(backend, scale, n_writes, batch=1, fsync=fsync, reps=reps)
            rows.append(r)
            print(f"{backend:>7} {scale:>7} {r['throughput_median']:>12.1f} "
                  f"{r['p50_ms']:>8.3f} {r['p99_ms']:>8.3f} "
                  f"{r['recovery_ms_median']:>9.2f} {r['storage_bytes']/1024:>8.1f}")

    # 2) batching 场景（固定一个中等规模）
    batch_scale = 1000 if 1000 in scales else scales[-1]
    print(f"\n[2] Batching (scale={batch_scale}, B records merged per commit)")
    print(f"{'backend':>7} {'batch':>6} {'thpt(ops/s)':>12} {'p50ms':>8} {'p99ms':>8}")
    for batch in (1, 10, 50):
        for backend in ("json", "wal"):
            r = bench_one(backend, batch_scale, n_writes, batch=batch, fsync=fsync, reps=reps)
            rows.append(r)
            print(f"{backend:>7} {batch:>6} {r['throughput_median']:>12.1f} "
                  f"{r['p50_ms']:>8.3f} {r['p99_ms']:>8.3f}")

    if not save_results:
        print("\n✅ benchmark smoke completed (--no-save)")
        return

    # ── 保存结果（按 backend 混合，文件名带日期 + fsync 标记）──────────
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "benchmarks")
    os.makedirs(out_dir, exist_ok=True)
    tag = date.today().isoformat() + ("_fsync" if fsync else "")
    json_path = os.path.join(out_dir, f"storage_results_{tag}.json")
    csv_path = os.path.join(out_dir, f"storage_results_{tag}.csv")

    meta = {
        "date": date.today().isoformat(),
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "fsync": fsync,
        "n_writes": n_writes,
        "reps": reps,
        "scales": scales,
        "note": "storage-layer microbenchmark; median of reps; single-machine, OS page cache.",
        "results": rows,
    }
    with open(json_path, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print("\n" + "─" * 70)
    print(f"✅ raw results saved:")
    print(f"   {os.path.relpath(json_path)}")
    print(f"   {os.path.relpath(csv_path)}")
    print("─" * 70)


if __name__ == "__main__":
    main()
