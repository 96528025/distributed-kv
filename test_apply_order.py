"""Applied-state regressions: every committed entry is applied once, in order, on every node.

Case C11 in docs/RAFT_CORRECTNESS.md. A round whose majority wait timed out left its
entries in the leader's log; the next successful round advanced commit_index over them,
but only that round's own entries reached the leader's store. Followers applied the whole
committed range, so the leader answered 404 for a key both followers held.

The unit tests load the node module in-process over a temporary WAL directory and drive
apply_committed(), maybe_snapshot(), install_snapshot() and the AppendEntries handler
directly. The live tests start three real processes on ports 5701-5703 with
RAFT_NUM_SHARDS=1 and test-mode election timeouts of 4-5 s, so pausing both followers
for the one-second majority wait never starts an election.

Run with: python3 test_apply_order.py
"""

import importlib.util
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import json
from unittest import mock


BASE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE, "node_raft_sharded.py")
LIVE_PORTS = [5701, 5702, 5703]
UNIT_PORTS = ["5711", "5712", "5713"]


def load_node_module(data_dir):
    """Load the node module in-process, without its threads or HTTP server."""
    module_name = f"node_raft_sharded_apply_{time.time_ns()}"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT)
    module = importlib.util.module_from_spec(spec)
    argv = [SCRIPT, *UNIT_PORTS, "--backend=wal", f"--data-dir={data_dir}"]
    with mock.patch.object(sys, "argv", argv):
        spec.loader.exec_module(module)
    module.load_from_disk()   # Recovers state and opens the WAL for appends.
    return module


def entry(term, key, value=None):
    """A log entry: a set when a value is given, otherwise a delete."""
    if value is None:
        return {"term": term, "op": "delete", "key": key}
    return {"term": term, "op": "set", "key": key, "value": value}


def request(port, path, data=None, timeout=2.0):
    url = f"http://127.0.0.1:{port}{path}"
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="GET" if data is None else "POST")
    if body is not None:
        req.add_header("Content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read()
            return response.status, json.loads(raw) if path != "/metrics" else raw.decode()
    except urllib.error.HTTPError as error:
        with error:
            payload = error.read()
        return error.code, json.loads(payload) if payload else {}
    except (OSError, TimeoutError):
        return None, None


def wait_until(predicate, timeout=10.0, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None


class ApplyCommittedUnitTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.mkdtemp(prefix="kv-apply-unit-")
        self.addCleanup(shutil.rmtree, self.data_dir, True)
        self.node = load_node_module(self.data_dir)
        self.addCleanup(self.node.storage.close)
        self.shard = self.node.shards[0]

    def _append_entries(self, body):
        """Run the AppendEntries handler without a socket and return its responses."""
        handler = self.node.Handler.__new__(self.node.Handler)
        responses = []
        handler._respond = lambda code, data: responses.append((code, data))
        handler._handle_append_entries(body)
        return responses

    def test_applies_the_committed_prefix_once_and_in_order(self):
        node, shard = self.node, self.shard
        shard.log = [entry(1, "k", "v1"), entry(1, "k", "v2"),
                     entry(1, "gone", "x"), entry(1, "gone")]
        shard.commit_index = 3

        self.assertEqual(node.apply_committed(shard), 4)
        self.assertEqual(node.store, {"k": "v2"})
        self.assertEqual(shard.last_applied, 3)
        self.assertEqual(node.storage.applied_indices()[0], 3)

        # Nothing new is committed, so a second call must not replay anything.
        node.store["k"] = "sentinel"
        self.assertEqual(node.apply_committed(shard), 0)
        self.assertEqual(node.store["k"], "sentinel")

    def test_a_later_commit_applies_the_entry_a_timed_out_round_left_behind(self):
        node, shard = self.node, self.shard
        shard.log = [entry(1, "a", "1")]      # in the log, majority wait timed out
        self.assertEqual(node.apply_committed(shard), 0)

        shard.log.append(entry(1, "b", "2"))
        shard.commit_index = 1                # the next round commits the log tail
        self.assertEqual(node.apply_committed(shard), 2)
        self.assertEqual(node.store, {"a": "1", "b": "2"})

    def test_a_commit_beyond_the_local_log_waits_for_the_entries(self):
        node, shard = self.node, self.shard
        shard.log = [entry(1, "a", "1")]
        shard.commit_index = 2

        self.assertEqual(node.apply_committed(shard), 1)
        self.assertEqual(shard.last_applied, 0)

        shard.log += [entry(1, "b", "2"), entry(1, "c", "3")]
        self.assertEqual(node.apply_committed(shard), 2)
        self.assertEqual(shard.last_applied, 2)
        self.assertEqual(node.store, {"a": "1", "b": "2", "c": "3"})

    def test_compaction_stops_at_last_applied_not_commit_index(self):
        node, shard = self.node, self.shard
        count = node.SNAPSHOT_THRESHOLD + 5
        shard.log = [entry(1, f"k{i}", str(i)) for i in range(count)]
        shard.commit_index = 9
        node.apply_committed(shard)
        shard.commit_index = count - 1        # committed, not applied yet

        node.maybe_snapshot(shard)
        self.assertEqual((shard.snapshot_index, shard.log_offset), (9, 10))
        self.assertEqual(len(shard.log), count - 10)

        node.apply_committed(shard)
        self.assertEqual(shard.last_applied, count - 1)
        self.assertEqual(node.store, {f"k{i}": str(i) for i in range(count)})

    def test_install_snapshot_never_moves_applied_state_backward(self):
        node, shard = self.node, self.shard
        shard.log = [entry(1, f"k{i}", "new") for i in range(13)]
        shard.commit_index = 12
        node.apply_committed(shard)

        older = {"snapshot_index": 5, "snapshot_term": 1, "log_offset": 6,
                 "store": {"k0": "old"}, "tail_log": []}
        self.assertFalse(node.install_snapshot(shard, older))
        self.assertEqual(node.store["k0"], "new")
        self.assertEqual((shard.last_applied, len(shard.log)), (12, 13))

    def test_install_snapshot_resumes_apply_after_its_boundary(self):
        node, shard = self.node, self.shard
        snap = {"snapshot_index": 7, "snapshot_term": 1, "log_offset": 8,
                "store": {"x": "1"}, "tail_log": [entry(1, "y", "2"), entry(1, "z", "3")]}

        self.assertTrue(node.install_snapshot(shard, snap, leader_commit=9))
        self.assertEqual(node.apply_committed(shard), 2)
        self.assertEqual(shard.last_applied, 9)
        self.assertEqual(node.store, {"x": "1", "y": "2", "z": "3"})
        self.assertEqual(node.storage.applied_indices()[0], 9)

    def _follower_with_unapplied_suffix(self):
        """A follower holding leader entries 0-4 that has applied only 0-1."""
        shard = self.shard
        shard.term = 1
        shard.log = [entry(1, f"k{i}", str(i)) for i in range(5)]
        shard.commit_index = 1
        self.node.apply_committed(shard)
        # The leader has since committed 2-5 and compacted through 4.
        return {"shard_id": 0, "term": 1, "leader_id": 5712,
                "entries": [entry(1, "k5", "5")], "commit_index": 5,
                "log_offset": 5, "prev_log_index": 4, "prev_log_term": 1}

    def test_follower_behind_a_compacted_window_installs_a_snapshot_instead_of_skipping(self):
        node, shard = self.node, self.shard
        body = self._follower_with_unapplied_suffix()
        calls = []

        def leader_snapshot(port, path, data, timeout=0.5):
            calls.append(path)
            return {"snapshot_index": 4, "snapshot_term": 1, "log_offset": 5,
                    "store": {f"k{i}": str(i) for i in range(5)},
                    "tail_log": [entry(1, "k5", "5")]}

        node.send_rpc = leader_snapshot
        responses = self._append_entries(body)

        self.assertEqual(calls, ["/install_snapshot"])
        self.assertEqual(responses, [(200, {"term": 1, "success": True})])
        self.assertEqual(shard.last_applied, 5)
        self.assertEqual(node.store, {f"k{i}": str(i) for i in range(6)})

    def test_a_failed_catch_up_is_not_acknowledged(self):
        node, shard = self.node, self.shard
        body = self._follower_with_unapplied_suffix()
        node.send_rpc = lambda *args, **kwargs: None

        responses = self._append_entries(body)

        self.assertEqual(responses, [(200, {"term": 1, "success": False})])
        self.assertEqual((shard.last_applied, len(shard.log)), (1, 5))

    def test_recovery_resumes_from_the_wal_applied_index(self):
        node, shard = self.node, self.shard
        shard.log = [entry(1, "a", "1"), entry(1, "b", "2"), entry(1, "c", "3")]
        shard.commit_index = 2
        node.apply_committed(shard)
        node.storage.close()

        restarted = load_node_module(self.data_dir)
        self.addCleanup(restarted.storage.close)
        recovered = restarted.shards[0]
        self.assertEqual(restarted.store, {"a": "1", "b": "2", "c": "3"})
        self.assertEqual(
            (recovered.last_applied, recovered.commit_index, recovered.log_offset),
            (2, 2, 3),
        )


class LiveApplyRegressionTest(unittest.TestCase):
    """Three real processes; each test starts its own cluster in a fresh data dir."""

    def setUp(self):
        self.data_dir = tempfile.mkdtemp(prefix="kv-apply-live-")
        self.processes = {}
        self.paused = set()
        self.addCleanup(self._cleanup)
        self._start_cluster()
        self.leader = wait_until(self._leader, timeout=20.0)
        self.assertIsNotNone(self.leader, "cluster did not elect a leader")
        self.followers = [port for port in LIVE_PORTS if port != self.leader]

    # ---- cluster control ----
    def _start_cluster(self):
        env = dict(os.environ, RAFT_TEST_MODE="1", RAFT_NUM_SHARDS="1",
                   RAFT_ELECTION_TIMEOUT_MIN="4", RAFT_ELECTION_TIMEOUT_MAX="5")
        for port in LIVE_PORTS:
            peers = [str(peer) for peer in LIVE_PORTS if peer != port]
            self.processes[port] = subprocess.Popen(
                [sys.executable, SCRIPT, str(port), *peers,
                 "--backend=wal", f"--data-dir={self.data_dir}"],
                cwd=BASE, env=env,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )

    def _stop_cluster(self, sig):
        for port in list(self.paused):
            self._resume(port)
        for process in self.processes.values():
            if process.poll() is None:
                process.send_signal(sig)
        for process in self.processes.values():
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self.processes = {}

    def _cleanup(self):
        self._stop_cluster(signal.SIGTERM)
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def _pause(self, port):
        os.kill(self.processes[port].pid, signal.SIGSTOP)
        self.paused.add(port)

    def _resume(self, port):
        process = self.processes.get(port)
        if port in self.paused and process is not None and process.poll() is None:
            os.kill(process.pid, signal.SIGCONT)
        self.paused.discard(port)

    # ---- observation ----
    def _shard(self, port):
        status, body = request(port, "/health", timeout=0.5)
        return body["shards"]["0"] if status == 200 else None

    def _leader(self):
        views = [self._shard(port) for port in LIVE_PORTS]
        if any(view is None for view in views):
            return None
        leaders = {view["leader"] for view in views}
        if len(leaders) != 1:
            return None
        leader = leaders.pop()
        if leader not in LIVE_PORTS:
            return None
        view = self._shard(leader)
        return leader if view and view["role"] == "leader" else None

    def _raft(self, port):
        status, body = request(port, "/debug/raft?shard=0")
        self.assertEqual(status, 200, body)
        return body["shards"]["0"]

    def _stores(self):
        stores = {}
        for port in LIVE_PORTS:
            status, body = request(port, "/all")
            if status != 200:
                return None
            stores[port] = body["data"]
        return stores

    def _assert_stores_converge(self, expected, timeout=10.0):
        def converged():
            stores = self._stores()
            return stores if stores and all(s == expected for s in stores.values()) else None

        self.assertIsNotNone(wait_until(converged, timeout=timeout),
                             f"stores never converged to {expected}: {self._stores()}")

    def _snapshot_installs(self, port):
        status, text = request(port, "/metrics")
        self.assertEqual(status, 200)
        pattern = r'distributed_kv_snapshot_operations_total\{[^}]*operation="install"[^}]*\} (\d+)'
        return sum(int(value) for value in re.findall(pattern, text))

    # ---- scenario ----
    def _leave_uncommitted_entry(self, key, value):
        """Write while both followers are paused: the one-second majority wait times out
        and the entry stays in the leader's log, uncommitted (outcome unknown, C6)."""
        for port in self.followers:
            self._pause(port)
        status, body = request(self.leader, "/set", {"key": key, "value": value}, timeout=5.0)
        for port in self.followers:
            self._resume(port)
        self.assertEqual(status, 500, body)
        self.assertIn("majority not reached", body["error"])

        raft = self._raft(self.leader)
        keys = [item["key"] for item in raft["log"]]
        self.assertIn(key, keys)
        self.assertLess(raft["commit_index"], raft["log_offset"] + keys.index(key))

    def test_leader_applies_an_entry_committed_by_a_later_round_and_keeps_it_after_restart(self):
        self._leave_uncommitted_entry("a", "1")

        status, body = request(self.leader, "/set", {"key": "b", "value": "2"})
        self.assertEqual((status, body.get("status")), (200, "ok"), body)

        expected = {"a": "1", "b": "2"}
        self._assert_stores_converge(expected)
        status, body = request(self.leader, "/get?key=a")
        self.assertEqual((status, body.get("value")), (200, "1"), body)
        for port in LIVE_PORTS:
            view = self._shard(port)
            self.assertEqual(view["last_applied"], view["commit_index"], (port, view))

        # Every node must rebuild the same store from its own WAL.
        self._stop_cluster(signal.SIGKILL)
        self._start_cluster()
        self.assertIsNotNone(wait_until(self._leader, timeout=20.0),
                             "restarted cluster did not elect a leader")
        self._assert_stores_converge(expected)

    def test_lagging_follower_catches_up_through_a_snapshot_that_holds_the_late_entry(self):
        self._leave_uncommitted_entry("a", "1")
        lagging = self.followers[0]
        self._pause(lagging)

        expected = {"a": "1"}
        for i in range(25):   # more than SNAPSHOT_THRESHOLD (20), so the leader compacts
            status, body = request(self.leader, "/set", {"key": f"k{i}", "value": str(i)})
            self.assertEqual(status, 200, body)
            expected[f"k{i}"] = str(i)
        self.assertIsNotNone(
            wait_until(lambda: self._raft(self.leader)["log_offset"] > 0, timeout=3.0),
            "leader never compacted its log",
        )

        self._resume(lagging)
        self._assert_stores_converge(expected)
        self.assertGreaterEqual(self._snapshot_installs(lagging), 1)

    def test_transaction_commit_applies_an_entry_a_timed_out_round_left_behind(self):
        self._leave_uncommitted_entry("a", "1")

        status, body = request(self.leader, "/txn",
                               {"ops": [{"key": "t", "value": "1"}]}, timeout=10.0)
        self.assertEqual((status, body.get("status")), (200, "ok"), body)
        self._assert_stores_converge({"a": "1", "t": "1"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
