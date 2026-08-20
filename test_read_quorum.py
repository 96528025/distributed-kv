"""Read-quorum logic and live stale-read failure-injection tests.

The live test deliberately pauses processes with SIGSTOP:

1. Write v1 through the current Leader.
2. Pause that Leader so the other two nodes elect a replacement.
3. Commit v2 through the replacement Leader.
4. Pause the replacement majority and resume only the old Leader.
5. Verify the isolated old Leader rejects the read instead of returning v1.

Run with: python3 test_read_quorum.py
"""

import glob
import hashlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
from unittest import mock


PORTS = [5101, 5102, 5103]
BASE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE, "node_raft_sharded.py")
KEY = "read_quorum_key"


def request(port, path, data=None, timeout=2.0):
    url = f"http://localhost:{port}{path}"
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="GET" if data is None else "POST")
    if body is not None:
        req.add_header("Content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        with error:
            payload = error.read()
        return error.code, json.loads(payload) if payload else {}
    except (OSError, TimeoutError):
        return None, None


def wait_until(predicate, timeout=12.0, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None


def load_node_module():
    """Load the node module without starting its HTTP server."""
    module_name = f"node_raft_sharded_test_{time.time_ns()}"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT)
    module = importlib.util.module_from_spec(spec)
    argv = [SCRIPT, "5201", "5202", "5203"]
    with mock.patch.object(sys, "argv", argv):
        spec.loader.exec_module(module)
    return module


class ReadQuorumUnitTest(unittest.TestCase):
    def setUp(self):
        self.node = load_node_module()
        self.shard = self.node.shards[0]
        with self.shard.lock:
            self.shard.role = self.node.LEADER
            self.shard.term = 7
            self.shard.leader_id = self.node.MY_PORT

    def test_read_requires_majority_acknowledgement(self):
        self.node.send_rpc = lambda *args, **kwargs: None
        self.assertFalse(self.node.confirm_read_quorum(self.shard, timeout=0.01))

    def test_one_peer_plus_leader_forms_three_node_majority(self):
        def fake_rpc(port, *args, **kwargs):
            return {"term": 7, "success": port == 5202}

        self.node.send_rpc = fake_rpc
        self.assertTrue(self.node.confirm_read_quorum(self.shard, timeout=0.01))

    def test_higher_term_forces_leader_to_step_down(self):
        def fake_rpc(port, *args, **kwargs):
            if port == 5203:
                return {"term": 8, "success": False}
            return {"term": 7, "success": True}

        self.node.send_rpc = fake_rpc
        self.assertFalse(self.node.confirm_read_quorum(self.shard, timeout=0.01))
        with self.shard.lock:
            self.assertEqual(self.shard.term, 8)
            self.assertEqual(self.shard.role, self.node.FOLLOWER)
            self.assertIsNone(self.shard.leader_id)


class ReadQuorumRegressionTest(unittest.TestCase):
    def setUp(self):
        self.processes = {}
        self.paused = set()
        # addCleanup also runs if a later assertion in setUp fails.
        self.addCleanup(self._cleanup_cluster)
        self._clean_files()

        for port in PORTS:
            peers = [str(peer) for peer in PORTS if peer != port]
            self.processes[port] = subprocess.Popen(
                [sys.executable, SCRIPT, str(port), *peers],
                cwd=BASE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        ready = wait_until(lambda: self._leader_seen_by(PORTS), timeout=15.0)
        self.assertIsNotNone(ready, "cluster did not elect a stable leader")

    def _cleanup_cluster(self):
        for port in list(self.paused):
            self._resume(port)
        for process in self.processes.values():
            if process.poll() is None:
                process.terminate()
        for process in self.processes.values():
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        self._clean_files()

    def _clean_files(self):
        for port in PORTS:
            data_file = os.path.join(BASE, f"data_raft_sharded_{port}.json")
            if os.path.exists(data_file):
                os.remove(data_file)
            pattern = os.path.join(BASE, f"snapshot_{port}_shard*.json")
            for snapshot_file in glob.glob(pattern):
                os.remove(snapshot_file)
            # Raft hard state must go too, or currentTerm carries over into the
            # next run: the nodes restart on these same ports and resume at the
            # term they left off at instead of 0. The suite still passes that
            # way, but it stops being hermetic -- terms climb run over run.
            for hard_state in glob.glob(
                    os.path.join(BASE, f"raft_hardstate_{port}.json*")):
                os.remove(hard_state)

    def _pause(self, port):
        os.kill(self.processes[port].pid, signal.SIGSTOP)
        self.paused.add(port)

    def _resume(self, port):
        process = self.processes[port]
        if port in self.paused and process.poll() is None:
            os.kill(process.pid, signal.SIGCONT)
        self.paused.discard(port)

    def _health(self, port):
        status, body = request(port, "/health", timeout=0.5)
        return body if status == 200 else None

    def _shard_id(self):
        digest = hashlib.md5(KEY.encode()).hexdigest()
        return int(digest, 16) % len(PORTS)

    def _leader_seen_by(self, live_ports, excluded=None):
        shard_id = self._shard_id()
        leaders = []
        for port in live_ports:
            health = self._health(port)
            if not health:
                return None
            leader = health["shards"][str(shard_id)]["leader"]
            if leader is None or leader == excluded:
                return None
            leaders.append(leader)
        if len(set(leaders)) != 1:
            return None
        leader = leaders[0]
        if leader not in live_ports:
            return None
        leader_health = self._health(leader)
        if not leader_health:
            return None
        role = leader_health["shards"][str(shard_id)]["role"]
        return leader if role == "leader" else None

    def test_isolated_old_leader_rejects_stale_read(self):
        old_leader = self._leader_seen_by(PORTS)
        self.assertIsNotNone(old_leader)

        status, body = request(PORTS[0], "/set", {"key": KEY, "value": "v1"})
        self.assertEqual((status, body.get("status")), (200, "ok"))

        self._pause(old_leader)
        remaining = [port for port in PORTS if port != old_leader]
        new_leader = wait_until(
            lambda: self._leader_seen_by(remaining, excluded=old_leader),
            timeout=12.0,
        )
        self.assertIsNotNone(new_leader, "remaining majority did not elect a new leader")

        status, body = request(new_leader, "/set", {"key": KEY, "value": "v2"})
        self.assertEqual((status, body.get("status")), (200, "ok"))

        # Keep the replacement majority unavailable while the old Leader handles the read.
        for port in remaining:
            self._pause(port)
        self._resume(old_leader)

        status, body = request(old_leader, f"/get?key={KEY}", timeout=2.0)
        self.assertEqual(status, 503, body)
        self.assertNotEqual((body or {}).get("value"), "v1")
        self.assertIn(
            (body or {}).get("error"),
            {"leadership quorum unavailable", "leader unreachable"},
        )

        # Restored communication must make the old node converge to committed v2.
        for port in remaining:
            self._resume(port)

        def reads_v2():
            read_status, read_body = request(old_leader, f"/get?key={KEY}")
            return read_body if read_status == 200 and read_body.get("value") == "v2" else None

        self.assertIsNotNone(wait_until(reads_v2, timeout=8.0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
