# Raft Correctness Log

This document tracks every correctness defect found in `node_raft_sharded.py`, the
failure scenario that demonstrates it, the fix, the invariant the fix establishes,
and the regression test that keeps it established.

It is written as a working log, not as a marketing document. Cases are added when
found and completed when the corresponding PR lands.

---

## The five Raft safety properties

Every case below is classified against the properties in Figure 3 of the extended
Raft paper. Keeping them distinct matters: a fix that establishes one of them does
not establish the others.

| Property | Statement | §  |
|---|---|---|
| **Election Safety** | At most one leader can be elected in a given term. | 5.2 |
| **Leader Append-Only** | A leader never overwrites or deletes entries in its own log; it only appends. | 5.3 |
| **Log Matching** | If two logs contain an entry with the same index and term, the logs are identical in all entries up through that index. | 5.3 |
| **Leader Completeness** | If an entry is committed in a term, it is present in the logs of all leaders of higher-numbered terms. | 5.4 |
| **State Machine Safety** | If a server has applied an entry at a given index, no other server will ever apply a *different* entry for the same index. | 5.4.3 |

Two things that are explicitly **not** safety properties, and are not treated as
bugs in this log:

- **A client timeout is an indeterminate outcome, not an abort.** If a client does
  not receive a success response, the entry may still be replicated and committed
  later. This is normal Raft behaviour. The correct client-facing contract is
  "never report success without a majority", plus request-ID deduplication for safe
  retries — *not* truncating the leader's log, which would violate Leader Append-Only.
- **A follower lagging behind, or holding an uncommitted divergent suffix, is legal.**
  Only the *applied* prefix is subject to State Machine Safety.

---

## Status

| Case | Defect | Property at risk | Lands in |
|---|---|---|---|
| [C1](#c1--election-hard-state-was-not-durable) | `currentTerm` / `votedFor` lost on restart | Election Safety | **PR1 ✅** |
| [C2](#c2--requestvote-ignored-log-freshness) | RequestVote ignored candidate log freshness | Leader Completeness | **PR1 ✅** |
| [C3](#c3--raft-log-is-not-durable) | Raft log lost on restart | Leader Completeness, Log Matching | PR2 |
| [C4](#c4--replication-is-full-log-overwrite) | No `nextIndex`/`matchIndex`; leader ships whole log; follower blind-overwrites | Log Matching | PR3 |
| [C5](#c5--commitindex-advanced-by-ack-counting) | Commit counted from RPC acks, no current-term rule | Leader Completeness | PR3 |
| [C6](#c6--client-timeout-reported-as-definitive-failure) | Timeout returns `500 "failed"` | *client semantics* | PR3 |
| [C7](#c7--snapshots-break-shard-namespace-isolation) | Shard snapshot carries the whole global store | State Machine Safety | PR4 |
| [C8](#c8--apply-is-not-ordered-by-log-index) | Apply runs outside `shard.lock` | State Machine Safety | PR4 |
| [C9](#c9--leader-reads-need-full-readindex-semantics) | Quorum confirmation rejects an isolated old Leader, but there is no apply barrier | *linearizability* | **Partial: stale-Leader rejection fixed; full ReadIndex open** |

---

## C1 — Election hard state was not durable

**Property at risk:** Election Safety (§5.2)

### Problem

`ShardRaft.__init__` initialised `term = 0` and `voted_for = None` as pure in-memory
state. `load_from_disk()` restored only the KV state machine and snapshot metadata.
Nothing on disk recorded who a node had voted for.

Election Safety rests on one rule: *a server casts at most one vote per term.* That
rule is only meaningful if the vote outlives a crash.

### Failure scenario

```
term = 8:  node C receives RequestVote from A  → grants, voted_for = A   (memory only)
           node C crashes
           node C restarts                     → term = 0, voted_for = None
term = 8:  node C receives RequestVote from B  → candidate_term(8) > term(0)
                                               → resets voted_for = None
                                               → grants vote to B
result:    A and B can both collect a majority in term 8 → two leaders in one term
```

### Why the original implementation was unsafe

The step-up branch in `_handle_vote` (`if candidate_term > shard.term: voted_for = None`)
is correct Raft — a node moving to a *new* term has not yet voted in it. The bug is
that after a restart the node believes every term is new, so that branch fires for a
term in which it has already voted.

### Fix

Per-shard `(currentTerm, votedFor)` are persisted to `raft_hardstate_<port>.json`
via `tmp → fsync → os.replace → fsync(dir)`, and the write completes **before** any
RPC response that depends on the state is sent. Persist points:

- granting a vote (`_handle_vote`)
- stepping up to a higher term seen in any RPC or RPC response
- incrementing the term when starting an election — persisted *before* the first
  RequestVote leaves the node

The file holds a full latest-wins snapshot of all shards. State is read under
`shard.lock`; the disk write happens with no shard lock held. Concurrent persists
that complete out of order therefore cannot write back an older term — and writing a
*newer* term is always at least as restrictive, so it can never resurrect a spent vote.

Two latent defects surfaced while wiring this up:

- The `prevLog` conflict branch in `_handle_append_entries` returned from inside the
  lock, which would have skipped persistence entirely — replying with a term the node
  would disown after a crash. It now falls through to a single exit path.
- The heartbeat step-down path advanced `currentTerm` **without clearing `votedFor`**.
  Moving to a new term means the node has not yet voted in it; keeping the old vote
  would let it refuse a legitimate candidate in the new term. Now cleared.

If the hard state file fails to parse, the node **refuses to start** rather than
resetting to term 0. Since writes go through `tmp → fsync → os.replace`, the live file
can never be half-written; a parse failure means real corruption, and booting anyway
would silently reintroduce exactly the double-vote bug this case fixes.

The same fail-closed rule covers shard topology. The file records the `num_shards` it
was written under, and a mismatch on startup aborts. Without this, the `RAFT_NUM_SHARDS`
override introduced alongside these tests would be a back door around I-C1.3:

```
start with 3 shards, vote in shards 1 and 2
restart with 1 shard   → shards 1 and 2 silently filtered out of the restore
restart with 3 shards  → shards 1 and 2 are back at term 0, votedFor = None
                       → the node can vote a second time in a term it already voted in
```

There is no shard membership or migration semantics in this implementation, so refusing
to start is the only correct behaviour. The abort is read-only: it must not rewrite the
hard state on its way out (asserted by T1.10c).

### Invariant

> **I-C1.1** If a node has replied `vote_granted = true` for `(shard, term, candidate)`,
> then `(term, candidate)` is durable before that reply is observable.
>
> **I-C1.2** A node's persisted `currentTerm` never decreases across a restart.
>
> **I-C1.3** For a given `(shard, term)`, a node grants at most one distinct
> `candidate_id`, across any number of crashes.
>
> **I-C1.4** Persisted hard state is only ever restored under the shard topology it
> was written under. A shard-count change fails startup rather than discarding the
> election state of shards that fall outside the new range.
>
**I-C1.1 is tested against SIGKILL, which proves the write reached the OS (flush),
not that it reached the platter (fsync).** The code does call `fsync`; the test does
not prove it. Power-loss durability is asserted by construction, not by test — the
same distinction the storage WAL already documents.

### Regression test

`test_raft_correctness.py` → T1.1, T1.5, T1.6, T1.7, T1.10

### Documented follow-ups (not blocking)

- `persist_hard_state()` atomically rewrites **all** shards on every mutation. That is
  O(N) write amplification per change and O(N²) across a term-churn storm. Invisible at
  N=3 and correct as written, but **PR2's per-shard durable journal must not inherit
  this strategy.** The intended evolution is: PR1 correctness-first whole-file atomic
  persistence → PR2 per-shard append journal.
- `_fsync_dir` is POSIX-oriented (`os.open(dir, O_RDONLY)` + `fsync`). Linux CI and
  macOS development cover the supported surface; no Windows abstraction is planned.

### Note: what C1 does *not* establish

C1 gives Election Safety. It does **not** give Leader Completeness, and on a restarted
node it opens a subtler hole until [C3](#c3--raft-log-is-not-durable) lands:

> After a restart the Raft log is still empty (`log = []`), so the node reports its
> own last-log position as the snapshot boundary. As a **voter** it therefore
> understates how much log it has, and will grant votes to candidates that it could
> have rejected had it remembered its own log. Durable `votedFor` makes this look
> fixed while it is not.

This is the reason PR2 follows PR1 immediately.

---

## C2 — RequestVote ignored log freshness

**Property at risk:** Leader Completeness (§5.4)

### Problem

`start_election` computed and transmitted `last_log_index` / `last_log_term`, but
`_handle_vote` never read them:

```python
vote_granted = (
    candidate_term >= shard.term and
    (shard.voted_for is None or shard.voted_for == candidate_id)
)
```

Any candidate with a high enough term won, regardless of how far behind its log was.

### Failure scenario

```
shard 0 log on A, B: [1..50] committed
node C has been partitioned since index 10, log: [1..10]

C's election timeout fires → term++ → RequestVote(term=high, lastLog=(t,10))
A and B grant (no freshness check) → C becomes leader
C replicates its 10-entry log → A and B overwrite theirs
result: committed entries 11..50 are gone from every replica
```

### Why the original implementation was unsafe

The election restriction (§5.4.1) is the *only* mechanism that keeps committed
entries alive across leader changes. Without it, Leader Completeness fails, and
because this implementation also lets a leader overwrite follower logs wholesale
([C4](#c4--replication-is-full-log-overwrite)), the loss is silent and immediate
rather than eventually repaired.

### Fix

`_handle_vote` compares the candidate's last-log tuple against its own,
lexicographically with **term first**:

```
up_to_date = (cand_last_term >  my_last_term) or
             (cand_last_term == my_last_term and cand_last_index >= my_last_index)
```

`start_election` and `_handle_vote` both derive their last-log tuple from a single
helper (`_last_log_locked`) so the two sides cannot drift apart, and so the empty-log
case falls back to the snapshot boundary identically on both sides.

That fallback is not cosmetic. Once snapshot compaction truncates a shard's log to
empty, `log[-1]` no longer exists, and the naive reading is "this node has no log" —
which makes it accept any candidate at all. An empty log after compaction means the
node's history is *at* the snapshot boundary, not absent. T1.4c pins this down.

### Invariant

> **I-C2.1** A node grants a vote only if the candidate's `(lastLogTerm, lastLogIndex)`
> is greater than or equal to its own under term-major lexicographic order.
>
> **I-C2.2** The comparison is on *last log entry* terms, never on `currentTerm`.
>
I-C2.2 is called out explicitly because conflating `lastLogTerm` with `currentTerm`
is the most common way to get this rule subtly wrong — it makes the check pass
almost always, which looks like it works.

### Regression test

`test_raft_correctness.py` → T1.2, T1.3, T1.4a, T1.4b, T1.4c

---

## C3 — Raft log is not durable

**Property at risk:** Leader Completeness, Log Matching · **Lands in PR2**

### Problem

`shard.log` is in-memory only. After a restart, `log = []` and `commit_index` is reset
to `snapshot_index`, so every entry between the last snapshot and the crash is gone.

### Failure scenario

```
2 followers down → leader appends entry at index 40 (no majority, client told "unknown")
leader is SIGKILLed → restarts → index 40 is gone from its log
```

The entry must survive. Losing it is the defect; a leader **removing it on its own**
would be a different defect (Leader Append-Only).

Additionally, per [C1](#note-what-c1-does-not-establish), a restarted node
under-reports its own last-log position and grants votes it should refuse.

### Fix / Invariant / Regression test

Pending PR2. Design constraints already fixed:

- The journal must support **suffix truncation** from day one (followers truncate on
  conflict in PR3), implemented as an append-only `TRUNCATE(from_index)` record so
  that replay has exactly one crash semantics.
- It reuses the framing / CRC / partial-write detection / atomic-file machinery from
  `feat/wal-checkpoint-persistence`, but stays a **semantically separate subsystem**
  from the committed state-machine WAL:

  | | Raft journal | state-machine WAL |
  |---|---|---|
  | written | *before* acking / voting | *after* commit, before apply |
  | holds | `currentTerm`, `votedFor`, **uncommitted** entries | committed operations only |
  | guarantees | no double vote, no loss of acked entries | applied state survives crash |

---

## C4 — Replication is full-log overwrite

**Property at risk:** Log Matching · **Lands in PR3**

### Problem

The leader sends `entries = list(shard.log)` — its entire log window — to every
follower on every heartbeat, with `prev_log_index` hardcoded to `log_offset - 1`.
There is no `nextIndex[]` or `matchIndex[]`. The follower does
`shard.log = list(entries)`, an unconditional whole-log overwrite. The conflict branch
returns a `conflict_index` that no code on the leader side reads.

The consistency check therefore always probes the snapshot boundary, which is trivially
satisfied for any follower that is caught up — it never actually verifies a match point.

### Fix / Invariant / Regression test

Pending PR3.

---

## C5 — commitIndex advanced by ack counting

**Property at risk:** Leader Completeness · **Lands in PR3**

### Problem

`commit_index` is set to the leader's log tail once a majority of AppendEntries RPCs
returned `success`, with no `matchIndex` bookkeeping and no §5.4.2 current-term rule.

The current-term rule happens to be satisfied today only because a leader never commits
anything except entries it just appended in its own term — which is also why a new
leader has no mechanism at all to advance commit over entries inherited from a previous
term (a liveness gap).

### Fix / Invariant / Regression test

Pending PR3.

---

## C6 — Client timeout reported as definitive failure

**Not a safety property — client semantics** · **Lands in PR3**

### Problem

When the majority wait times out, `/set` returns `500 {"error": "majority not reached"}`.
That reads as "the write did not happen". The correct statement is "the outcome is
unknown": the entry is still in the leader's log and may be committed later.

This is **not** a Raft violation, and must not be "fixed" by truncating the leader's
log — that would violate Leader Append-Only. The fix is on the response contract, plus
request-ID deduplication so a client retry is safe.

### Fix / Invariant / Regression test

Pending PR3. Request-ID deduplication tracked separately as a follow-up.

---

## C7 — Snapshots break shard namespace isolation

**Property at risk:** State Machine Safety · **Lands in PR4**

### Problem

All shards share one global `store` dict. `_handle_install_snapshot` returns
`dict(store)` — the whole global state machine, not the requesting shard's key subset —
and the receiver does `store.update(snap["store"])`. `maybe_snapshot` writes the whole
global store into each shard's snapshot file.

### Failure scenario

```
follower F falls behind on shard 0 → requests InstallSnapshot from shard 0's leader L
L is itself a lagging follower for shard 1
L's snapshot payload carries L's stale view of shard 1's keys
F applies it → F's committed shard-1 state is rolled back
```

Recovery has the same problem in reverse: `load_from_disk` applies each shard's
snapshot file in turn, so the final state depends on file iteration order.

### Note on scope

The defect is **namespace isolation**, not shared storage. Multiple Raft groups may
share a physical storage engine; what they may not do is let one group's snapshot
carry or overwrite another group's state. The target structure is:

```
Shard 0 Raft → shard 0 state ─┐
Shard 1 Raft → shard 1 state ─┼→ shared physical storage engine
Shard 2 Raft → shard 2 state ─┘
```

### Fix / Invariant / Regression test

Pending PR4.

---

## C8 — Apply is not ordered by log index

**Property at risk:** State Machine Safety · **Lands in PR4**

### Problem

`_handle_append_entries` collects `to_apply` while holding `shard.lock`, then applies
it **outside** the lock. Two concurrent AppendEntries handlers for the same shard can
therefore apply entries for the same key out of log-index order, leaving a stale value
in the state machine.

### Fix / Invariant / Regression test

Pending PR4. Requires exposing `last_applied` so the property can be asserted on the
*applied* prefix rather than on raw logs.

---

## C9 — Leader reads need full ReadIndex semantics

**Not a safety property — linearizability of reads** · **Partial**

### Fixed slice: reject an isolated old Leader

Before reading local state, the current Leader now sends an AppendEntries probe in its
current term and requires acknowledgements from a majority. A Leader that has lost its
quorum refuses the read. If a peer reports a higher term, the node persists that term and
steps down before returning the failed barrier.

`test_read_quorum.py` includes a real three-process regression: it pauses the old Leader
with `SIGSTOP`, lets the remaining majority elect a replacement and commit a newer value,
then isolates the replacement majority. Once resumed, the old Leader must return `503`
instead of its stale local value.

This establishes only the demonstrated stale-old-Leader rejection path. It is deliberately
described as a quorum-validated Leader read, not as complete ReadIndex or a proof of
linearizability.

### Remaining gaps

1. A newly elected Leader may not yet have applied entries inherited from previous terms,
   so its local store can lag the committed prefix.
2. The implementation does not append and commit a current-term no-op on election, capture
   a read index, or wait until `last_applied` reaches that index before reading.
3. A follower forwards to `shard.leader_id`, a cache from the last AppendEntries. A stale
   target will now reject a read when it cannot confirm quorum, but routing can still fail
   or retry unnecessarily.
4. Full linearizability also depends on the open log replication, commit, durability, and
   ordered-apply cases C3-C8.

### Remaining work / invariant / regression test

Complete ReadIndex semantics remain open: establish a current-term commit barrier, capture
the committed read index after quorum confirmation, wait for `last_applied >= read_index`,
and add history-based tests that exercise concurrent reads, writes, elections, and faults.
