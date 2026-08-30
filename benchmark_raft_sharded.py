#!/usr/bin/env python3
"""
Concurrent benchmark for the sharded Raft KV store
(`node_raft_sharded.py`).

Measurements
============
1. Throughput for `/set`, `/get`, and `/txn` in operations per second.
2. Per-operation p50, p95, p99, and maximum end-to-end latency.
3. Batch-write effect: serial `/set` requests versus concurrent requests.
4. Shard distribution: keys concentrated on one shard versus spread across
   all shards in the same three-node cluster.
5. Throughput and p99 latency at concurrency levels 1, 10, 50, 100, and 200.

In the current server, `/get` is a quorum-validated leader read. It is not a
complete ReadIndex implementation, and the benchmark makes no stronger
linearizability claim.

Methodology
===========
The shard comparison uses the same three-node cluster for both arms. In the
current implementation, `NUM_SHARDS` equals the number of cluster nodes, so a
one-node versus three-node comparison would also change the replication factor.
Keeping the cluster fixed isolates workload distribution while preserving the
same replication factor:

* Concentrated: keys are selected to hash to shard 0.
* Spread: keys are distributed across all shards.

Each managed measurement restarts the nodes after clearing data and snapshot
files. The legacy JSON backend rewrites the complete store on every commit, so
accumulated data would otherwise make later measurements systematically slower.
Node state is written to a temporary benchmark directory rather than the
repository working tree.

Implementation choices
======================
* The client uses `urllib.request`, keeping the benchmark dependency-free. The
  server defaults to HTTP/1.0 and closes each response, so connection pooling
  would not provide persistent-connection reuse.
* `ThreadPoolExecutor` models concurrent blocking HTTP clients; concurrency N
  means up to N requests in flight.
* matplotlib is optional. CSV and JSON are always produced; PNG generation is
  skipped when matplotlib is unavailable.

Usage
=====
  python3 benchmark_raft_sharded.py                 # run the full benchmark
  python3 benchmark_raft_sharded.py --quick         # reduced smoke run
  python3 benchmark_raft_sharded.py --tests curve,batch
  python3 benchmark_raft_sharded.py --no-spawn --target localhost:5001
        # benchmark an existing cluster; state is not reset between measurements

Results are written to `benchmarks/` as JSON, CSV, and optional PNG files.
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

# matplotlib is optional; the benchmark still emits CSV and JSON without it.
try:
    import matplotlib
    matplotlib.use("Agg")  # Render without a display server.
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    HAS_MPL = True
except Exception:
    HAS_MPL = False

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
NODE_SCRIPT = os.path.join(REPO_DIR, "node_raft_sharded.py")
OUT_DIR = os.path.join(REPO_DIR, "benchmarks")


# ─────────────────────────────────────────────────────────────
# HTTP client: measure one end-to-end latency per request, including TCP setup.
# ─────────────────────────────────────────────────────────────
def http_post(url, payload, timeout=6.0):
    """POST JSON and return (ok, latency_ms, status_code, body_bytes)."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            code = resp.status
    except urllib.error.HTTPError as e:            # The server returned 4xx/5xx.
        try:
            body = e.read()
        except Exception:
            body = b""
        code = e.code
    except Exception:                              # Connection failure or timeout.
        body = b""
        code = 0
    lat = (time.perf_counter() - t0) * 1000.0
    return (200 <= code < 300), lat, code, body


def http_get(url, timeout=6.0):
    """GET a URL and return (ok, latency_ms, status_code, body_bytes)."""
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
# Match the server's shard hash so the benchmark can control key placement.
# Server formula: int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_SHARDS
# ─────────────────────────────────────────────────────────────
def get_shard(key, num_shards):
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % num_shards


def keys_for_shard(prefix, target_shard, count, num_shards):
    """Generate `count` keys that hash to `target_shard`."""
    out, i = [], 0
    while len(out) < count:
        k = f"{prefix}{i}"
        if get_shard(k, num_shards) == target_shard:
            out.append(k)
        i += 1
    return out


# ─────────────────────────────────────────────────────────────
# Statistics helpers
# ─────────────────────────────────────────────────────────────
def percentile(sorted_vals, p):
    """Return a linearly interpolated percentile from sorted values."""
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
    """Summary of one load run."""
    def __init__(self, name, concurrency, latencies_ms, ok, err, wall_s):
        self.name = name
        self.concurrency = concurrency
        self.ok = ok
        self.err = err
        self.wall_s = wall_s
        s = sorted(latencies_ms)
        self.count = len(s)
        # Client-observed throughput = successful requests / wall-clock time.
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
# Load driver: issue N requests at fixed concurrency and collect each latency.
# ─────────────────────────────────────────────────────────────
def run_load(name, op_fn, args_list, concurrency):
    """
    op_fn(arg) -> (ok: bool, latency_ms: float)
    args_list  : one request argument (payload or key) per element
    concurrency: worker-pool size, which bounds requests in flight
    Throughput is successful requests divided by wall-clock time. Latency
    percentiles are calculated from individual request observations.
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


# ── Per-operation function factories ───────────────────────────
# Reads and writes use client-side leader routing: each request is sent directly
# to the known leader for its key's shard. This removes follower-forwarding
# variability from the comparison. If the leader is unknown, routing falls back
# to the configured base URL and the server handles forwarding.
def make_set_op(router):
    def op(kv):
        success, lat, _c, _b = http_post(f"{router(kv['key'])}/set", kv)
        return success, lat
    return op


def make_get_op(router):
    def op(key):
        # Benchmark-generated keys never contain '=', matching the server parser.
        success, lat, _c, _b = http_get(f"{router(key)}/get?key={key}")
        return success, lat
    return op


def make_txn_op(base):
    # Any node can coordinate a cross-shard 2PC transaction.
    def op(ops):
        success, lat, code, body = http_post(f"{base}/txn", {"ops": ops})
        # An aborted transaction can return HTTP 200, so inspect its status field.
        if success and code == 200:
            try:
                success = json.loads(body).get("status") == "ok"
            except Exception:
                success = False
        return success, lat
    return op


def gen_kv(prefix, n, value_size):
    """Generate `n` key-value write payloads."""
    val = "x" * value_size
    return [{"key": f"{prefix}{i}", "value": val} for i in range(n)]


# ─────────────────────────────────────────────────────────────
# Cluster lifecycle: start, stop, clear state, and wait for leader election.
# Node state is stored under the temporary STATE_DIR, outside the working tree.
# ─────────────────────────────────────────────────────────────
_spawned = []


def spawn_cluster(ports, state_dir, logdir):
    """Start one node per port with all persistent state under `state_dir`."""
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
    """Remove data and snapshot files to provide a clean starting point."""
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
    """Poll /health until every shard has a leader."""
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
    print("  WARNING: leader election timed out; results may include election overhead")
    return False


def discover_leaders(host, ports, num_shards):
    """Query /health and return {shard_id: leader_port}."""
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
# Prepare each measurement:
#   spawn mode: restart after clearing data/snapshot state, then warm up;
#   --no-spawn: warm up the existing cluster without clearing its state.
# Refresh the leader map after preparation for client-side routing.
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
        """Return the shard leader URL, or the configured fallback URL."""
        lp = self.leaders.get(get_shard(key, self.num_shards))
        return f"http://{self.host}:{lp}" if lp else self.base

    def leader_spread(self):
        """Return the number of distinct nodes currently leading a shard."""
        vals = [v for v in self.leaders.values() if v]
        return len(set(vals))

    def prep(self):
        """Restart managed nodes with cleared data/snapshots, then warm up."""
        if self.spawn:
            stop_cluster()
            time.sleep(1.0)                       # Allow sockets to leave TIME_WAIT.
            clear_state(self.state_dir)
            spawn_cluster(self.ports, self.state_dir, self.logdir)
            time.sleep(1.5)
            wait_for_ready(self.ports, self.num_shards)
        self.leaders = discover_leaders(self.host, self.ports, self.num_shards)
        # Warm up request handling and the state machine; exclude it from results.
        if self.warmup_n > 0:
            kvs = gen_kv("warmup_", self.warmup_n, self.value_size)
            run_load("warmup", make_set_op(self.router), kvs, concurrency=20)
        return self


# ── Median-of-N helper for variable batch and shard comparisons ──
def median_run(cl, name, make_args, concurrency, repeats, op_kind="set"):
    """
    Run the load `repeats` times, preparing the cluster before each run.
    Return the median-throughput Result plus per-run throughput and leader
    distribution metadata.
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
# Measurements. Each independent point calls cl.prep() first.
# ─────────────────────────────────────────────────────────────
def test_curve(cl, total, concurrencies, repeats):
    """Measure /set throughput and latency at each concurrency level."""
    print(f"\n=== Concurrency curve: /set throughput and p99 ({repeats} runs per point) ===")
    results = []
    for c in concurrencies:
        r, meta = median_run(
            cl, f"set@c{c}",
            lambda: gen_kv(f"curve_c{c}_", total, cl.value_size),
            c, repeats)
        results.append(r)
        print(f"  concurrency {c:>3}: {r.throughput:>8.0f} ops/s | "
              f"p50={r.p50:6.2f}ms p99={r.p99:7.2f}ms max={r.pmax:7.2f}ms | "
              f"runs={meta['runs_ops_s']}")
    return results


def test_per_op(cl, total, concurrency):
    """Measure /set, /get, and /txn from independently cleared data state."""
    print(f"\n=== Per-operation throughput and latency (concurrency={concurrency}) ===")
    results = []

    # /set
    cl.prep()
    r_set = run_load("set", make_set_op(cl.router),
                     gen_kv("perop_set_", total, cl.value_size), concurrency)
    results.append(r_set)

    # Prefill and read from the same cluster so the requested keys remain present.
    cl.prep()
    prefill = gen_kv("perop_get_", total, cl.value_size)
    run_load("get_prefill", make_set_op(cl.router), prefill, concurrency)
    r_get = run_load("get", make_get_op(cl.router),
                     [kv["key"] for kv in prefill], concurrency)
    results.append(r_get)

    # Each transaction writes two unique keys and may cross shard boundaries.
    cl.prep()
    val = "x" * cl.value_size
    txns = [[{"key": f"perop_txn_{i}_a", "value": val},
             {"key": f"perop_txn_{i}_b", "value": val}] for i in range(total)]
    r_txn = run_load("txn", make_txn_op(cl.base), txns, concurrency)
    results.append(r_txn)

    for r in results:
        note = f" | {r.err} aborted/failed" if r.err else ""
        print(f"  {r.name:>4}: {r.throughput:>8.0f} ops/s | "
              f"p50={r.p50:6.2f}ms p95={r.p95:6.2f}ms p99={r.p99:7.2f}ms "
              f"max={r.pmax:7.2f}ms{note}")
    return results


def test_batch(cl, total, concurrent_c, repeats):
    """
    Compare serial and concurrent writes from independently cleared data state.
    Serial requests normally occupy one Raft round each. Under concurrency,
    `batch_loop` can merge up to 20 requests arriving within its 5 ms window.
    """
    print(f"\n=== Batch-write effect: serial c=1 vs concurrent c={concurrent_c} "
          f"({repeats} runs per arm) ===")
    serial, m_s = median_run(cl, "set_serial",
                             lambda: gen_kv("batch_serial_", total, cl.value_size),
                             1, repeats)
    conc, m_c = median_run(cl, "set_concurrent",
                           lambda: gen_kv("batch_conc_", total, cl.value_size),
                           concurrent_c, repeats)
    speedup = (conc.throughput / serial.throughput) if serial.throughput > 0 else 0
    print(f"  serial (c=1)       : {serial.throughput:>8.0f} ops/s | p50={serial.p50:6.2f}ms "
          f"p99={serial.p99:7.2f}ms | runs={m_s['runs_ops_s']}")
    print(f"  concurrent (c={concurrent_c:<3}): {conc.throughput:>8.0f} ops/s | p50={conc.p50:6.2f}ms "
          f"p99={conc.p99:7.2f}ms | runs={m_c['runs_ops_s']}")
    print(f"  throughput ratio: {speedup:.1f}x")
    return [serial, conc], speedup, {"serial": m_s, "concurrent": m_c}


def test_shard(cl, total, concurrency, repeats):
    """
    Compare concentrated and spread keys in the same cluster and at the same
    replication factor. Data and snapshot state is cleared before each arm.

    Separate shard leaders can execute on separate node processes, but election
    placement is not controlled. Record how many distinct nodes host leaders so
    results can be interpreted alongside that placement.
    """
    ns = cl.num_shards
    val = "x" * cl.value_size
    print(f"\n=== Shard distribution: one shard vs {ns} shards "
          f"(concurrency={concurrency}, {repeats} runs per arm) ===")

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
    print(f"  one shard (all on shard 0): {r_one.throughput:>8.0f} ops/s | "
          f"p99={r_one.p99:7.2f}ms | runs={m_one['runs_ops_s']}")
    print(f"  {ns} shards (distribution {dist}): {r_spread.throughput:>8.0f} ops/s | "
          f"p99={r_spread.p99:7.2f}ms | runs={m_spread['runs_ops_s']}")
    print(f"  distinct leader nodes per spread run: {m_spread['leader_spread_each']}")
    print(f"  spread/concentrated throughput ratio: {ratio:.2f}x")
    print("  On a single host, spreading requests can reduce each shard's batch depth "
          "while all processes still contend for the same CPU. Interpret this ratio "
          "together with leader placement and host-level resource contention.")
    return [r_one, r_spread], ratio, {"single": m_one, "spread": m_spread}


# ─────────────────────────────────────────────────────────────
# Output: CSV, JSON, and charts
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
        description="Concurrent benchmark for the sharded Raft KV store",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--target", default="localhost:5001", help="target node as host:port")
    ap.add_argument("--ports", default="5001,5002,5003", help="comma-separated cluster ports")
    ap.add_argument("--no-spawn", action="store_true",
                    help="benchmark an existing cluster without resetting its state")
    ap.add_argument("--requests", type=int, default=2000, help="requests per measurement")
    ap.add_argument("--warmup", type=int, default=200, help="warm-up requests before each point")
    ap.add_argument("--value-size", type=int, default=100, help="value size in bytes")
    ap.add_argument("--concurrency", default="1,10,50,100,200", help="concurrency levels for the curve")
    ap.add_argument("--per-op-concurrency", type=int, default=50,
                    help="concurrency for per-operation, batch, and shard measurements")
    ap.add_argument("--tests", default="curve,perop,batch,shard",
                    help="comma-separated measurements: curve,perop,batch,shard")
    ap.add_argument("--repeats", type=int, default=3,
                    help="runs per comparison arm; the median-throughput run is reported")
    ap.add_argument("--quick", action="store_true",
                    help="smoke run: requests=400, warmup=50, repeats=1")
    ap.add_argument("--outdir", default=OUT_DIR, help="output directory")
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
    print("Sharded Raft KV concurrent benchmark")
    print(f"  target={args.target}  cluster_ports={ports}  shards={num_shards}")
    print(f"  requests_per_test={args.requests}  value={args.value_size}B  "
          f"per_op_concurrency={args.per_op_concurrency}")
    print("  batching: BATCH_MAX_SIZE=20, BATCH_TIMEOUT=5ms")
    print(f"  isolation: {'restart with data/snapshots cleared per measurement' if not args.no_spawn else 'existing cluster (--no-spawn)'}")
    print("=" * 70)

    if args.no_spawn:
        print("\n[cluster] using the existing cluster; state will not be cleared")
        wait_for_ready(ports, num_shards, timeout=10)
    else:
        # cl.prep() performs the first clean start and all subsequent restarts.
        print("\n[cluster] managed restarts enabled; state uses a temporary directory")

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

    # ── Save results ──
    print("\n[output]")
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "target": args.target, "ports": ports, "num_shards": num_shards,
            "requests_per_test": args.requests, "value_size": args.value_size,
            "per_op_concurrency": args.per_op_concurrency,
            "repeats_for_comparisons": args.repeats,
            "batch_max_size": 20, "batch_timeout_ms": 5,
            "isolation": "restart with data/snapshots cleared per measurement" if not args.no_spawn else "no-spawn",
            "routing": "client-side leader routing (per-key → shard leader)",
            "transport": "HTTP/1.0 + JSON (urllib; new TCP connection per request)",
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

    # ── Generate charts ──
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
        print("  WARNING: matplotlib is unavailable; PNG generation skipped")
        print("  Install matplotlib and rerun to generate charts; CSV and JSON are complete")

    # ── Measurement limitations ──
    print("\n" + "=" * 70)
    print("Measurement limitations")
    print("-" * 70)
    print("This benchmark measures the complete Python, HTTP/1.0, JSON, consensus, and storage path.")
    print("  1. HTTP/1.0 creates a new TCP connection and server thread per request.")
    print("  2. JSON encoding and the Python GIL add CPU and scheduling overhead.")
    print("  3. The legacy JSON backend rewrites the full store on each commit.")
    print("  4. BATCH_TIMEOUT=5ms trades additional queueing latency for batching opportunities.")
    print("The benchmark does not isolate these costs and should not be used to attribute")
    print("the observed limit to any single subsystem.")
    print("=" * 70)

    if not args.no_spawn:
        print("\n[cluster] stopping managed nodes")
        stop_cluster()
        # Remove temporary node state; result files live at the output root.
        try:
            shutil.rmtree(state_dir)
        except Exception:
            pass


if __name__ == "__main__":
    main()
