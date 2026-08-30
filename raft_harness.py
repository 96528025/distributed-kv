"""Deterministic driver for Raft correctness tests.

Design goal: do not make correctness depend on scheduler timing.

A localhost cluster cannot model a real network partition, and elections in a live
three-node cluster continuously change terms. Tests such as "would this candidate
receive a vote?" would therefore be flaky. This harness instead:

1. Starts one real node with a very large election timeout
   (``RAFT_ELECTION_TIMEOUT_*``), keeping it quietly in the follower state.
2. Drives that node with RPCs constructed explicitly by the test, acting as a
   candidate or Leader.
3. Uses ``FakePeer`` to emulate a peer and record outbound RPCs when the test needs
   to observe behavior initiated by the real node.

Each assertion consequently has one deterministic input and one deterministic
output. The harness relies on the ``/debug/raft`` introspection endpoint, which is
available only when ``RAFT_TEST_MODE=1``.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE   = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE, "node_raft_sharded.py")


# ── HTTP helpers ────────────────────────────────────────────
def http_get(port, path, timeout=3):
    try:
        with urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def http_get_status(port, path, timeout=3):
    """Return the HTTP status, or ``None`` when the connection fails."""
    try:
        with urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return None


def http_post(port, path, data, timeout=3):
    try:
        req = urllib.request.Request(
            f"http://localhost:{port}{path}",
            data=json.dumps(data).encode(),
            method="POST",
        )
        req.add_header("Content-type", "application/json")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


# ── Raft RPC construction ───────────────────────────────────
def request_vote(port, *, term, candidate_id, last_log_term, last_log_index, shard=0):
    """Send ``RequestVote`` to a real node while acting as a candidate.

    ``last_log_term`` is the term of the candidate's final log entry. It is
    distinct from ``term``, which is the candidate's current election term.
    """
    return http_post(port, "/vote", {
        "shard_id":       shard,
        "term":           term,
        "candidate_id":   candidate_id,
        "last_log_term":  last_log_term,
        "last_log_index": last_log_index,
    })


def append_entries(port, *, term, leader_id, entries, commit_index,
                   log_offset=0, prev_log_index=-1, prev_log_term=0, shard=0):
    """Send ``AppendEntries`` to a real node while acting as the Leader.

    An empty ``entries`` list represents a heartbeat.
    """
    return http_post(port, "/append_entries", {
        "shard_id":       shard,
        "term":           term,
        "leader_id":      leader_id,
        "entries":        entries,
        "commit_index":   commit_index,
        "log_offset":     log_offset,
        "prev_log_index": prev_log_index,
        "prev_log_term":  prev_log_term,
    })


def make_entries(count, term, prefix="k"):
    return [{"term": term, "op": "set", "key": f"{prefix}{i}", "value": str(i)}
            for i in range(count)]


def last_log_tuple(dbg):
    """Derive ``(lastLogTerm, lastLogIndex)`` from ``/debug/raft`` output.

    A non-empty log uses its final entry. A log emptied by compaction falls back
    to the snapshot boundary. Both sides of a vote must follow this same rule;
    see C2 and ``_last_log_locked``.
    """
    if dbg is None:
        return None
    if dbg["log"]:
        return dbg["log"][-1]["term"], dbg["log_offset"] + len(dbg["log"]) - 1
    return dbg["snapshot_term"], dbg["snapshot_index"]


# ── Real node ───────────────────────────────────────────────
class RealNode:
    """A real ``node_raft_sharded.py`` process with deterministic timing.

    ``election_timeout=None`` keeps the node from campaigning so the test drives
    every state transition. Supply a short interval when a test needs to observe
    the node initiate an election.
    """

    def __init__(self, port, peers, *, num_shards=1, election_timeout=None,
                 test_mode=True):
        self.port       = port
        self.peers      = [p for p in peers if p != port]
        self.num_shards = num_shards
        self.test_mode  = test_mode
        self.election_timeout = election_timeout
        self.proc       = None

    def _env(self):
        env = dict(os.environ)
        if self.test_mode:
            env["RAFT_TEST_MODE"] = "1"
        else:
            env.pop("RAFT_TEST_MODE", None)
        if self.num_shards is None:
            env.pop("RAFT_NUM_SHARDS", None)   # Default: NUM_SHARDS = len(ALL_PORTS)
        else:
            env["RAFT_NUM_SHARDS"] = str(self.num_shards)
        lo, hi = self.election_timeout or (99999, 99999)
        env["RAFT_ELECTION_TIMEOUT_MIN"] = str(lo)
        env["RAFT_ELECTION_TIMEOUT_MAX"] = str(hi)
        return env

    def start(self, wait=True):
        self.proc = subprocess.Popen(
            [sys.executable, SCRIPT, str(self.port)] + [str(p) for p in self.peers],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            cwd=BASE, env=self._env(),
        )
        if wait:
            self.wait_ready()
        return self

    def wait_ready(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if http_get_status(self.port, "/health", timeout=1) is not None:
                return True
            time.sleep(0.05)
        raise RuntimeError(f"node {self.port} did not become ready in {timeout}s")

    def start_expect_exit(self, timeout=10):
        """Start a node expected to refuse startup and return its exit code.

        Fail-closed tests require an explicit exit instead of serving with
        incomplete state.
        """
        self.start(wait=False)
        rc = self.proc.wait(timeout=timeout)
        self.proc = None
        return rc

    def kill(self):
        """Send ``SIGKILL`` without giving the process a cleanup opportunity.

        This proves that a write reached the operating system via ``flush``; it
        does not prove power-loss durability via ``fsync``. I-C1.1 in
        ``docs/RAFT_CORRECTNESS.md`` records that distinction.
        """
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=5)
        self._wait_port_free()
        self.proc = None

    def _wait_port_free(self, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with socket.socket() as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind(("127.0.0.1", self.port))
                    return True
                except OSError:
                    time.sleep(0.05)
        return False

    def restart(self, *, election_timeout=None):
        self.kill()
        self.election_timeout = election_timeout
        return self.start()

    def debug(self, shard=0):
        """Return one shard's internal state from ``/debug/raft?shard=N``."""
        r = http_get(self.port, f"/debug/raft?shard={shard}")
        if r is None:
            return None
        return r["shards"][str(shard)]

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.kill()


# ── Fake peer ───────────────────────────────────────────────
class FakePeer:
    """Emulate a Raft peer, recording each RPC and returning scripted replies.

    PR1 only records ``RequestVote`` calls initiated by the real node. PR3 will
    also use this peer to emulate a follower with a divergent log.
    """

    def __init__(self, port, *, vote_granted=False, append_success=True):
        self.port           = port
        self.vote_granted   = vote_granted
        self.append_success = append_success
        self.received       = []          # [(path, body)]
        self._lock          = threading.Lock()
        self._server        = None
        self._thread        = None

    def _record(self, path, body):
        with self._lock:
            self.received.append((path, body))

    def votes_received(self):
        with self._lock:
            return [b for p, b in self.received if p == "/vote"]

    def appends_received(self):
        with self._lock:
            return [b for p, b in self.received if p == "/append_entries"]

    def max_vote_term(self):
        terms = [v.get("term", 0) for v in self.votes_received()]
        return max(terms) if terms else None

    def start(self):
        peer = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                n    = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(n)) if n else {}
                peer._record(self.path, body)
                if self.path == "/vote":
                    # Return the same term so the candidate does not step down.
                    resp = {"term": body.get("term", 0),
                            "vote_granted": peer.vote_granted}
                else:
                    resp = {"term": body.get("term", 0),
                            "success": peer.append_success}
                self._send(resp)

            def do_GET(self):
                self._send({"ok": True})

            def _send(self, obj):
                data = json.dumps(obj).encode()
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", self.port), H)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


# ── Artifact cleanup ────────────────────────────────────────
ARTIFACT_GLOBS = [
    "data_raft_sharded_*.json",
    "snapshot_*.json",
    "raft_hardstate_*.json",
    "raft_hardstate_*.json.tmp",
]


def clean_artifacts(ports=None):
    import glob
    for pattern in ARTIFACT_GLOBS:
        for f in glob.glob(os.path.join(BASE, pattern)):
            if ports is None or any(str(p) in os.path.basename(f) for p in ports):
                try:
                    os.remove(f)
                except OSError:
                    pass
