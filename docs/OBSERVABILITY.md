# Observability Contract

This document defines the first two metrics phases for `node_raft_sharded.py`.

## Milestone boundaries

### Phase A — export and generic request visibility

- Dependency-free, thread-safe Counter, Gauge, and Histogram primitives.
- Prometheus text exposition from `GET /metrics`.
- Bounded `method`, `route`, and `status` labels for request counts.
- HTTP handler latency histogram.
- Unit coverage for format, fixed labels, histogram buckets, and concurrent updates.
- A live-process test that scrapes the real endpoint.

Phase A is complete when a fresh node exposes valid metrics and the metrics path does not
require a sidecar or third-party package.

### Phase B — distributed-system signals

- Election attempts and transitions to Leader.
- Read-quorum success and rejection outcomes.
- Majority-replication round latency and quorum failures.
- Local snapshot creation and follower snapshot installation.
- Client-visible transaction coordinator outcomes.
- Scrape-time term, role, commit index, log-window size, and pending transaction gauges.

Phase B is complete when the original 56-check three-node suite still passes and focused
tests prove the critical instrumentation paths are connected.

## Metric contract

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `distributed_kv_node_info` | gauge | `port`, `backend` | Static identity for one node process |
| `distributed_kv_http_requests_total` | counter | `method`, `route`, `status` | Responses produced by the node |
| `distributed_kv_http_request_duration_seconds` | histogram | `method`, `route` | End-to-end handler time |
| `distributed_kv_raft_elections_total` | counter | `shard` | Election attempts started locally |
| `distributed_kv_raft_leader_transitions_total` | counter | `shard` | Times this process became shard Leader |
| `distributed_kv_read_quorum_checks_total` | counter | `shard`, `outcome` | Read barrier result: `success`, `unavailable`, `higher_term`, `leadership_changed`, or `not_leader` |
| `distributed_kv_raft_replication_round_duration_seconds` | histogram | `shard`, `outcome` | Majority wait time for batch or transaction writes |
| `distributed_kv_snapshot_operations_total` | counter | `shard`, `operation` | Successful local `create` or follower `install` |
| `distributed_kv_transaction_coordinator_results_total` | counter | `outcome` | Client-visible `reported_ok`, `aborted`, `invalid`, or `unavailable` result |
| `distributed_kv_raft_term` | gauge | `shard` | Current in-memory term |
| `distributed_kv_raft_commit_index` | gauge | `shard` | Current in-memory commit index |
| `distributed_kv_raft_log_entries` | gauge | `shard` | Entries retained in the current log window |
| `distributed_kv_raft_role` | gauge | `shard`, `role` | One-hot `follower`, `candidate`, or `leader` state |
| `distributed_kv_pending_transactions` | gauge | `shard` | Prepared in-memory transactions |

`reported_ok` is deliberately not named `committed`: the current coordinator does not
durably verify every phase-2 participant result, so that response is not proof of atomic
commit.

## Cardinality and locking rules

- Never add key, value, request ID, operation ID, or transaction ID as a metric label.
- Unknown HTTP paths collapse to the single route label `unknown`.
- Per-shard labels are bounded by the configured fixed cluster size.
- Metric updates use their own locks and never call back into Raft or storage code.
- A scrape first copies shard state while holding one shard lock at a time, releases those
  locks, and only then updates the metric registry. This avoids a shard-lock/metric-lock
  cycle on the request path.

## First useful queries and alerts

- Read availability: rate of
  `distributed_kv_read_quorum_checks_total{outcome="unavailable"}`.
- Write availability: rate of replication histograms with
  `outcome="quorum_unavailable"`.
- Election churn: increase in `distributed_kv_raft_elections_total` compared with Leader
  transitions over the same window.
- Stuck transaction signal: `distributed_kv_pending_transactions > 0` for longer than the
  configured 10-second lock expiry.
- Replica progress: compare commit indexes for the same shard across the three node scrape
  targets.

These are raw signals, not bundled alerts. Counters reset when the process restarts; a
Prometheus server or another time-series store must retain history and handle resets.
