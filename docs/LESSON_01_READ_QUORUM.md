# Lesson 01 — Why “Read From the Leader” Is Not Enough

## Failure scenario

Assume three nodes: A, B, and C.

1. A is Leader and commits `key = v1`.
2. A becomes isolated or paused.
3. B and C form a majority, elect B in a higher term, and commit `key = v2`.
4. A resumes without first hearing from B or C.
5. A still has `role = leader` in memory and could serve its local `v1`.

Leader-only routing is therefore insufficient: a client can reach an old node that still
believes it is Leader.

## Read-quorum invariant

Before serving a local Leader read, the node must establish that it still has authority
in its current term:

> A node may read local state only after itself plus enough successful same-term peer
> responses form a majority.

In a three-node cluster, the local Leader plus one peer is a majority of two.

## Implementation

`confirm_read_quorum()`:

1. Captures the shard term, log window, commit index, and snapshot boundary under
   `shard.lock`.
2. Sends same-term AppendEntries probes to peers concurrently.
3. Waits up to `READ_QUORUM_TIMEOUT`.
4. Updates its term and steps down if a peer responds with a higher term.
5. Returns `True` only if successful same-term acknowledgements reach a majority.

`Handler.do_GET()` calls this barrier before reading `store`. Without quorum it returns
HTTP 503 with `leadership quorum unavailable`; rejecting the request is safer than
returning a value that may be stale. Follower requests still forward to their currently
known Leader, which then performs the same barrier.

## Tests

`test_read_quorum.py` has three focused logic tests and one live failure-injection test.
The live test commits `v1`, pauses the old Leader, elects a replacement, commits `v2`,
then resumes only the isolated old Leader and verifies it returns HTTP 503 instead of
`v1`. Once communication resumes, the old node must converge to `v2`.

## Why this is not full ReadIndex

This barrier closes the isolated-old-Leader stale-read path, but complete linearizability
also depends on unfinished Raft invariants, including election log freshness, current-term
commit rules, conflict repair, correct commit/apply tracking, and durable term/vote/log
state. The accurate claim is therefore “quorum-validated Leader reads,” not a complete
production Raft ReadIndex implementation.

The implementation also sends the current log window during each probe. That fits this
prototype but costs more network work per read than an optimized ReadIndex design.
