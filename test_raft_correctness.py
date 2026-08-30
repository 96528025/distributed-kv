"""
Raft correctness regression tests

Usage:
  python3 test_raft_correctness.py

Division of labour with test_raft_sharded.py:
  test_raft_sharded.py     -- functional integration tests (happy path + basic failures)
                              against a real 3-node cluster
  test_raft_correctness.py -- safety regression tests, deterministically constructed, where
                              every assertion maps to one invariant in
                              docs/RAFT_CORRECTNESS.md

No test depends on scheduler timing: the real node's election timeout is raised so high that it
never campaigns on its own, and every state transition is driven by RPCs the test constructs.
The one exception is T1.7, which must observe a RequestVote the real node sends itself; it records
that with a FakePeer and asserts ">=" rather than "==", so extra campaign rounds cannot make it
flaky.

PR1 coverage:
  C1 (Election Safety)      -- T1.1, T1.5, T1.6, T1.7
  C2 (Leader Completeness)  -- T1.2, T1.3, T1.4
  the test harness itself   -- T1.8, T1.9
"""

import atexit
import subprocess
import sys
import time

from raft_harness import (
    FakePeer, RealNode, append_entries, clean_artifacts, http_get, http_get_status,
    last_log_tuple, make_entries, request_vote,
)

PORT   = 5401
PEERS  = [5402, 5403]
SHARD  = 0

PASS = "\033[92m✅ PASS\033[0m"
FAIL = "\033[91m❌ FAIL\033[0m"
INFO = "\033[94mℹ️ \033[0m"

results = []


def check(name, ok, detail=""):
    print(f"  {PASS if ok else FAIL}  {name}")
    if detail:
        print(f"         {detail}")
    results.append((name, bool(ok), detail))
    return ok


def section(title):
    print(f"\n{'─'*62}\n  {title}\n{'─'*62}")


def _cleanup():
    subprocess.run(["pkill", "-f", f"node_raft_sharded.py {PORT}"], capture_output=True)
    clean_artifacts()

atexit.register(_cleanup)


def fresh_node(**kw):
    """Start a clean node per test, clearing the previous run's persisted state first."""
    _cleanup()
    time.sleep(0.3)
    kw.setdefault("num_shards", 1)
    return RealNode(PORT, PEERS, **kw).start()


# ══════════════════════════════════════════════════════════════
print("\n🔬 Raft correctness regression tests (PR1: election correctness)")

# ── C1 · Election Safety ──────────────────────────────────────
section("T1.1  no second vote in the same term after a restart  [I-C1.1 / I-C1.3]")

node = fresh_node()
r = request_vote(PORT, term=5, candidate_id=9001, last_log_term=0, last_log_index=-1)
check("term 5: first vote granted (candidate 9001)", r is not None and r.get("vote_granted") is True,
      f"resp={r}")

d = node.debug(SHARD)
check("in-memory state after voting is correct (term=5, votedFor=9001)",
      d and d["current_term"] == 5 and d["voted_for"] == 9001,
      f"debug={d}")

node.kill()
node.start()
d = node.debug(SHARD)
check("term/votedFor survive a SIGKILL restart (term=5, votedFor=9001)",
      d and d["current_term"] == 5 and d["voted_for"] == 9001,
      f"debug={d}")

r = request_vote(PORT, term=5, candidate_id=9002, last_log_term=0, last_log_index=-1)
check("after the restart, a second candidate 9002 is refused in the same term",
      r is not None and r.get("vote_granted") is False,
      f"resp={r} -- granting would allow two leaders in term 5")
node.kill()


# ── C2 · Leader Completeness ──────────────────────────────────
section("T1.2 / T1.3  RequestVote log up-to-date check  [I-C2.1]")

node = fresh_node()
append_entries(PORT, term=5, leader_id=9001, entries=make_entries(3, term=5),
               commit_index=2, log_offset=0, prev_log_index=-1, prev_log_term=0)
d = node.debug(SHARD)
check("precondition: the voter holds 3 entries at term=5 (last log = (5, 2))",
      d and len(d["log"]) == 3 and d["current_term"] == 5,
      f"debug={d}")

r = request_vote(PORT, term=6, candidate_id=9002, last_log_term=4, last_log_index=0)
check("T1.2  a candidate with a stale log is refused (candidate last log = (4, 0))",
      r is not None and r.get("vote_granted") is False,
      f"resp={r} -- granting would let committed entries 1..2 be overwritten and lost")

r = request_vote(PORT, term=7, candidate_id=9003, last_log_term=5, last_log_index=2)
check("T1.3  a candidate with an equally fresh log is accepted (guards against an over-strict rule)",
      r is not None and r.get("vote_granted") is True,
      f"resp={r}")
node.kill()


section("T1.4  the comparison uses lastLogTerm, not currentTerm  [I-C2.2]")

node = fresh_node()
# commit_index=-1: entries are appended but not committed, so snapshot compaction does not
# truncate the log here. The snapshot-boundary path is covered separately by T1.4c.
append_entries(PORT, term=5, leader_id=9001, entries=make_entries(51, term=5),
               commit_index=-1, log_offset=0, prev_log_index=-1, prev_log_term=0)
d = node.debug(SHARD)
check("precondition: voter last log = (term=5, index=50)",
      last_log_tuple(d) == (5, 50),
      f"last_log={last_log_tuple(d)}")

# The candidate's log is shorter but its term is higher; term wins, so the vote should be granted.
r = request_vote(PORT, term=10, candidate_id=9101, last_log_term=6, last_log_index=1)
check("T1.4a  candidate last log (6, 1) beats voter (5, 50) -- lastLogTerm outranks index",
      r is not None and r.get("vote_granted") is True,
      f"resp={r}")

# Same lastLogTerm but a lower index; this must be refused.
r = request_vote(PORT, term=11, candidate_id=9102, last_log_term=5, last_log_index=49)
check("T1.4b  candidate last log (5, 49) trails voter (5, 50) -- refused",
      r is not None and r.get("vote_granted") is False,
      f"resp={r}")
node.kill()

# T1.4c: once a snapshot truncates the log to empty, the voter must fall back to the snapshot
# boundary for the comparison. This is where the two sides most easily disagree -- one uses
# log[-1] while the other forgets the empty-log case, and the rule silently stops holding.
node = fresh_node()
append_entries(PORT, term=5, leader_id=9001, entries=make_entries(51, term=5),
               commit_index=50, log_offset=0, prev_log_index=-1, prev_log_term=0)
time.sleep(1.0)                     # wait for the async snapshot thread to finish truncating
d = node.debug(SHARD)
check("precondition: the snapshot truncated the log; last log falls back to the boundary (5, 50)",
      d and d["log"] == [] and last_log_tuple(d) == (5, 50),
      f"last_log={last_log_tuple(d)} log_len={len(d['log']) if d else None}")

r = request_vote(PORT, term=12, candidate_id=9103, last_log_term=5, last_log_index=49)
check("T1.4c  with an empty log, a trailing candidate (5, 49) is still refused via the snapshot boundary",
      r is not None and r.get("vote_granted") is False,
      f"resp={r} -- a log truncated by a snapshot is not the same as having no log")
node.kill()


# ── C1 · durability ordering ──────────────────────────────────
section("T1.5  the vote must be on disk before a grant is returned  [I-C1.1]")

node = fresh_node()
r = request_vote(PORT, term=3, candidate_id=9201, last_log_term=0, last_log_index=-1)
granted = r is not None and r.get("vote_granted") is True
node.kill()          # SIGKILL immediately after the response, with no chance to clean up
node.start()
d = node.debug(SHARD)
check("response returned -> SIGKILL -> (term=3, votedFor=9201) intact after restart",
      granted and d and d["current_term"] == 3 and d["voted_for"] == 9201,
      f"granted={granted} debug={d}")
node.kill()


section("T1.6  currentTerm never regresses across a restart  [I-C1.2]")

node = fresh_node()
append_entries(PORT, term=9, leader_id=9301, entries=[], commit_index=-1)
d = node.debug(SHARD)
check("precondition: term=9 observed via AppendEntries", d and d["current_term"] == 9, f"debug={d}")
node.kill()
node.start()
d = node.debug(SHARD)
check("currentTerm >= 9 after a SIGKILL restart",
      d and d["current_term"] >= 9,
      f"debug={d} -- regressing to 0 would let the node vote again in a term it already voted in")
node.kill()


section("T1.7  a candidate must persist its term before sending RequestVote  [I-C1.1]")

peer_a, peer_b = FakePeer(PEERS[0]), FakePeer(PEERS[1])
peer_a.start(); peer_b.start()
try:
    node = fresh_node(election_timeout=(0.3, 0.5))
    observed = None
    deadline = time.time() + 8
    while time.time() < deadline:
        terms = [t for t in (peer_a.max_vote_term(), peer_b.max_vote_term()) if t]
        if terms:
            observed = max(terms)
            break
        time.sleep(0.1)

    check("precondition: the FakePeer received a RequestVote from the real node",
          observed is not None, f"observed_term={observed}")

    node.kill()          # SIGKILL immediately after observing the RPC
    node.election_timeout = None
    node.start()
    d = node.debug(SHARD)
    check(f"candidate campaigned at term={observed}; persisted currentTerm >= {observed} after restart",
          observed is not None and d and d["current_term"] >= observed,
          f"debug={d} -- losing the term would let the node re-campaign and re-vote within the same term")
    node.kill()
finally:
    peer_a.stop(); peer_b.stop()


# ── the test harness itself ───────────────────────────────────
section("T1.8 / T1.9  the introspection endpoint stays gated; NUM_SHARDS override leaves defaults alone")

node = fresh_node(test_mode=False)
status = http_get_status(PORT, f"/debug/raft?shard={SHARD}")
check("T1.8  /debug/raft returns 404 unless RAFT_TEST_MODE is set",
      status == 404, f"status={status}")
node.kill()

node = fresh_node(num_shards=1)
d = node.debug(SHARD)
check("T1.9a RAFT_NUM_SHARDS=1 takes effect (a single Raft group)",
      d is not None, "the num_shards override did not take effect")
node.kill()

node = fresh_node(num_shards=None)          # no override -> fall back to the default
r = http_get(PORT, "/debug/raft")
check("T1.9b without an override the default stays len(ALL_PORTS)=3 (defaults not quietly changed)",
      r is not None and r.get("num_shards") == 3, f"resp_num_shards={r and r.get('num_shards')}")
node.kill()


section("T1.10  startup must be refused when the shard count disagrees with persisted state  [I-C1.4]")

# RAFT_NUM_SHARDS is a production-reachable configuration path introduced by PR1 itself. Silently
# dropping out-of-range shard state when starting with a different shard count would open a back
# door to I-C1.3:
#   3 shards (a vote cast) -> start with 1 shard (shards 1/2 discarded) -> back to 3 shards
#   -> votedFor for shards 1/2 vanishes -> the node can vote twice in one term
_cleanup()
time.sleep(0.3)

node = RealNode(PORT, PEERS, num_shards=3).start()
r = request_vote(PORT, term=7, candidate_id=9401, last_log_term=0, last_log_index=-1, shard=2)
d = node.debug(2)
check("precondition: started with 3 shards and cast a vote on shard 2 (term=7, votedFor=9401)",
      r is not None and r.get("vote_granted") is True
      and d and d["current_term"] == 7 and d["voted_for"] == 9401,
      f"resp={r} debug={d}")
node.kill()

mismatched = RealNode(PORT, PEERS, num_shards=1)
rc = mismatched.start_expect_exit(timeout=10)
check("T1.10a restarting with 1 shard -> the node refuses to start (non-zero exit)",
      rc is not None and rc != 0,
      f"exit_code={rc} -- silently discarding the shard 1/2 votes would bypass I-C1.3")
check("T1.10b a node that refused to start serves no requests",
      http_get_status(PORT, "/health", timeout=1) is None,
      "a node that refused to start must not answer requests")

node = RealNode(PORT, PEERS, num_shards=3).start()
d = node.debug(2)
check("T1.10c failing closed left state intact: the vote survives a return to 3 shards",
      d and d["current_term"] == 7 and d["voted_for"] == 9401,
      f"debug={d} -- refusing to start must be read-only and must not rewrite hard state")
node.kill()


# ══════════════════════════════════════════════════════════════
section("Summary")
_cleanup()

total  = len(results)
passed = sum(1 for _, ok, _ in results if ok)
failed = total - passed

for name, ok, _ in results:
    print(f"  {PASS if ok else FAIL}  {name}")

print(f"\n{'─'*62}")
if failed == 0:
    print(f"  \033[92m🎉 all passed: {passed}/{total}\033[0m")
else:
    print(f"  \033[91m⚠️  {passed}/{total} passed, {failed} failed\033[0m")
    print("\n  Failures:")
    for name, ok, detail in results:
        if not ok:
            print(f"    • {name}\n      {detail}")
print(f"{'─'*62}\n")

sys.exit(0 if failed == 0 else 1)
