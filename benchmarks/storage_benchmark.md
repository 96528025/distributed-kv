# Storage backend benchmark: legacy JSON full rewrite vs WAL append

Script: [`../benchmark_storage.py`](../benchmark_storage.py)

```bash
python3 benchmark_storage.py                    # every scale (100 / 1k / 10k / 50k)
python3 benchmark_storage.py --quick            # quick smoke test
python3 benchmark_storage.py --quick --no-save  # smoke test without writing result files
python3 benchmark_storage.py --fsync            # run again with fsync enabled
python3 benchmark_storage.py --scales=100,1000
```

Raw results are saved under a **date plus fsync marker** and never overwrite an existing
file: `storage_results_<date>.json` and `storage_results_<date>.csv`.

---

## Why benchmark the storage layer on its own

In a cluster-level benchmark the write path is dominated *jointly* by HTTP/1.0, JSON
encoding, Raft majority replication, the GIL and CPU contention on a single machine.
Persistence is only one term in that sum, and the noise makes it impossible to see how
much switching to a WAL actually saves.

This benchmark drives `StorageEngine` directly — no HTTP, no Raft, no network — to answer
one question cleanly:

> **Does the legacy `save_to_disk()`, which rewrites the whole store on every commit, cost
> more per commit as the data grows? And is a WAL append independent of data size?**

Method: preload the store with N existing entries (with one baseline flush to reach a
steady state), then time 1000 new writes, measuring per-commit latency percentiles and
throughput, followed by recovery time and on-disk size. Each point is repeated 3 times and
the median is reported.

---

## Preserved results from the source branch (2026-07-23, single machine, macOS, Python 3.14.6, fsync=off)

The raw JSON/CSV below comes from `df874e6` and is kept as an auditable record of the
migration source. The integrated version since fixed appends after a partial tail and
restart index continuity, and made `rotate_records` count actual WAL records rather than
commit calls. The single-write conclusions are unchanged, but checkpoint timing on the
batching rows may differ from this historical data; rerun the script for current numbers.

### 1. Single-record commits across data scales — the core comparison

| backend | scale | throughput (ops/s) | p50 | p99 | recovery | on disk |
|---|---:|---:|---:|---:|---:|---:|
| JSON | 100   | 1400 | 0.72ms | 1.17ms | 0.34ms | 30 KB |
| WAL  | 100   | **86,423** | **0.008ms** | 0.018ms | 0.52ms | 32 KB |
| JSON | 1,000 | 769 | 1.29ms | 1.82ms | 0.57ms | 57 KB |
| WAL  | 1,000 | **80,644** | **0.008ms** | 0.019ms | 0.82ms | 62 KB |
| JSON | 10,000 | 132 | 7.54ms | 8.71ms | 2.89ms | 339 KB |
| WAL  | 10,000 | **61,228** | **0.008ms** | 0.017ms | 4.14ms | 360 KB |
| JSON | 50,000 | **28** | **35.3ms** | 44.9ms | 14.5ms | 1628 KB |
| WAL  | 50,000 | **28,595** | **0.008ms** | 0.018ms | 20.8ms | 1728 KB |

**Conclusions, measured rather than inferred:**

1. **The legacy JSON write amplification is real and linear.** Going from 100 to 50,000
   entries (500x), JSON single-write throughput falls from 1400 to 28 ops/s (about 50x
   worse) and p50 rises from 0.72ms to 35ms (about 49x worse). That is exactly the O(N)
   cost of calling `json.dump(entire store)` on every commit.
2. **A WAL single write is independent of data size.** Across the same 100 to 50,000 range
   the WAL p50 stays at **0.008ms**, because it only appends one fixed-frame record. At the
   50k scale a WAL write is roughly **1000x** faster than a JSON write.
3. **First cost: slightly more disk.** The WAL is an append log and is a little larger than
   compact JSON (50k: 1728KB vs 1628KB), but checkpoint rotation caps that (see below).
4. **Second cost: recovery has to replay.** WAL recovery (checkpoint plus replay) is
   slightly slower than reading a single JSON file, but the absolute numbers are small
   (50k: 20.8ms vs 14.5ms).

### 2. Why does WAL throughput also fall from 86k to 28k at larger scales?

Not write amplification — **checkpoint rotation**. This benchmark uses
`rotate_records=1000` and each point writes exactly 1000 records, so every point triggers
**one** checkpoint, and that one checkpoint rewrites the whole store (O(N)).

At scale=50k that single O(50k) checkpoint (about 20ms) is amortized over 1000 writes,
lifting total time from "1000 x 0.008ms = 8ms" to roughly 28ms, and throughput drops
accordingly.

This is precisely the WAL design trade-off: **the O(N) full flush moves from "every write"
to "once every rotate_records writes."** A larger `rotate_records` means rarer checkpoints
and faster steady-state writes, at the cost of a longer WAL to replay during recovery. It
is an explicit, tunable knob between write amplification and recovery time. p50/p99 stay at
0.008/0.018ms throughout, which shows almost every write is a pure append and only the
rotation is a spike.

### 3. Batching (scale=1000)

| backend | batch | throughput (ops/s) | p50 | p99 |
|---|---:|---:|---:|---:|
| JSON | 1  | 766 | 1.30ms | 2.24ms |
| WAL  | 1  | 78,962 | 0.008ms | 0.022ms |
| JSON | 10 | 7,602 | 0.130ms | 0.187ms |
| WAL  | 10 | 149,872 | 0.005ms | 0.008ms |
| JSON | 50 | 35,955 | 0.026ms | 0.036ms |
| WAL  | 50 | 159,656 | 0.005ms | 0.007ms |

Batching helps **both backends** — JSON collapses B full rewrites into one, and the WAL
writes B frames in a single flush — but the WAL stays an order of magnitude faster at every
batch size.

---

## Variance, and what these numbers do not show

- The tables are the **median** of 3 runs per point; `storage_results_*.json` also stores
  `throughput_min/max`.
- On a single machine with the OS page cache, absolute throughput fluctuates with background
  load (WAL single-write throughput moves between roughly 60k and 90k ops/s). The
  **relative** conclusions — JSON degrading linearly with scale, WAL staying flat — hold in
  every run.
- fsync=off: `flush()` hands data to the OS page cache, which is enough to survive a
  **process crash or SIGKILL** (see test 14 in `test_wal.py`). Surviving **power loss**
  requires `--fsync`, which reduces throughput noticeably by adding a disk sync per write;
  save a separate `storage_results_<date>_fsync.*` to compare.
- This is a **storage-layer microbenchmark**. It does not represent end-to-end cluster
  throughput, which is still dominated by HTTP, Raft and the GIL — see the project README.
