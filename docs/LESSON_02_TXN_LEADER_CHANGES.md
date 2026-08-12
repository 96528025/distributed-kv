# Lesson 02 — Transaction Prepare During a Leader Change

## The routing failure

A coordinator can cache a participant's old `leader_id`. If leadership changes before
phase 1, `/txn_prepare` may reach a Follower and return `not_leader`, or the cached node
may be unreachable. Treating either result as an immediate business failure makes an
otherwise healthy transaction abort.

## Phase-1 routing invariant

Before a participant returns `ready`, the coordinator has not made a global commit
decision. It can locate the current participant Leader and retry prepare, provided that:

1. Every attempt uses the same `txn_id`.
2. A `not_leader` response follows a valid known-node Leader hint.
3. An unreachable cached Leader falls back to other known nodes.
4. Deterministic participant results such as `locked` stop discovery immediately.
5. The node that actually handled prepare is recorded for phase 2.

`send_txn_prepare_with_discovery()` implements this bounded discovery. A participant's
`not_leader` response now includes its current `leader_id`. The coordinator sends the
subsequent commit or abort to the actual participant rather than the stale cached address.

## Why phase 2 is different

After one participant commits, blindly changing targets is not safe. This prototype keeps
prepared intents and coordinator decisions only in memory. Correct phase-2 recovery would
need replicated/durable intents, a durable decision, idempotent phase-2 operations, and a
recovery protocol.

This feature therefore hardens participant routing across a Leader change during prepare;
it does not make cross-shard 2PC durable or atomic under every participant failure.

## Tests

`test_txn_routing.py` verifies:

1. `not_leader` redirects prepare with the same `txn_id`.
2. An unreachable initial Leader falls back to another known node.
3. A lock conflict is returned without trying other replicas.
4. A redirected successful prepare commits on its actual participant.
5. An abort is sent to the participant that returned the deterministic failure.
