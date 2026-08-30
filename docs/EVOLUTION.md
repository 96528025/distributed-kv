# Engineering Evolution

The repository keeps one current implementation on the default branch. Earlier prototypes
remain available in Git history; they are summarized here because the failure that ended
each prototype explains the current design.

## Naive leader selection to elections

The first prototype chose the live node with the lowest port as leader. It was simple, but
two network partitions could make different nodes believe they were authoritative. The
current implementation instead maintains an independent election term and vote for every
shard, and persists both values before returning a vote-dependent response.

The important lesson was not merely to add an election timer. Election safety depends on a
durable "one vote per term" rule and on rejecting candidates whose logs are less up to date.
Those two properties now have crash and log-freshness regressions in
[`RAFT_CORRECTNESS.md`](RAFT_CORRECTNESS.md).

## One consensus group to per-shard groups

A single consensus group makes one leader the write bottleneck for the entire key space.
The current design hashes each key to a shard and runs a separate Raft state machine for
each shard. Different shard leaders can therefore execute replication rounds independently.

This is static modulo sharding, not consistent hashing. Every node still stores the full
logical data set, and changing the shard count has no migration protocol. The benchmark
also shows that independent groups do not create useful parallelism when all server
processes share one laptop and batch depth falls as traffic spreads across shards.

## Full JSON rewrites to WAL and checkpoints

The compatibility storage backend rewrites the complete JSON store after every commit, so
write cost grows with the data set. The optional WAL backend appends framed, checksummed
records and periodically publishes an atomic full-state checkpoint.

Checkpoint publication follows this order:

1. Write and fsync a temporary checkpoint.
2. Atomically replace the previous checkpoint and fsync the directory.
3. Only then truncate the WAL.

This makes locally applied state recoverable after a process crash. It does not make the
Raft log durable; that is the highest-priority open protocol task.

## Current direction

The project now evolves through an invariant-first loop:

1. Construct a deterministic failure.
2. Name the violated safety or client-semantic property.
3. Implement the smallest defensible fix.
4. Keep the failure as a regression.
5. State what the fix still does not prove.

The prioritized protocol roadmap is durable Raft log recovery, standard conflict repair
and commit tracking, shard-scoped snapshots with ordered apply, and then a complete read
barrier and transaction recovery.
