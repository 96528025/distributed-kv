"""Timer regressions: election and transaction-lock timers use the monotonic clock.

A wall-clock step, such as an NTP correction or a manual clock change, must not
start an election on a follower that just heard from its leader, must not
postpone an election that is due, and must not change when a transaction lock's
lease runs out. The node module is loaded in-process; no node processes start.

Run with: python3 test_timers.py
"""

import importlib.util
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock


BASE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE, "node_raft_sharded.py")
WALL_CLOCK_STEP = 3600.0


def load_node_module(data_dir):
    """Load the node module in-process, without its threads or HTTP server."""
    module_name = f"node_raft_sharded_timers_{time.time_ns()}"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT)
    module = importlib.util.module_from_spec(spec)
    argv = [SCRIPT, "5721", "5722", "5723", f"--data-dir={data_dir}"]
    with mock.patch.object(sys, "argv", argv):
        spec.loader.exec_module(module)
    return module


class TimerTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.mkdtemp(prefix="kv-timers-")
        self.addCleanup(shutil.rmtree, self.data_dir, True)
        self.node = load_node_module(self.data_dir)
        self.shard = self.node.shards[0]
        self.shard.election_timeout = 1.5

    def _step_wall_clock(self, seconds):
        stepped = time.time() + seconds
        return mock.patch.object(self.node.time, "time", return_value=stepped)

    def _receive_heartbeat(self):
        """Record a heartbeat through the AppendEntries handler, so the node's own
        clock stamps it rather than the test choosing one."""
        handler = self.node.Handler.__new__(self.node.Handler)
        handler._respond = lambda code, data: None
        handler._handle_append_entries({
            "shard_id": 0, "term": 0, "leader_id": 5722, "entries": [],
            "commit_index": -1, "log_offset": 0,
            "prev_log_index": -1, "prev_log_term": 0,
        })

    def test_a_wall_clock_step_forward_does_not_start_an_election(self):
        self._receive_heartbeat()
        with self._step_wall_clock(WALL_CLOCK_STEP):
            self.assertFalse(self.node.election_due(self.shard))

    def test_a_wall_clock_step_backward_does_not_postpone_an_election(self):
        self._receive_heartbeat()
        # Ten seconds pass on the monotonic clock while the wall clock steps back.
        ten_seconds_later = time.monotonic() + 10
        with self._step_wall_clock(-WALL_CLOCK_STEP), \
                mock.patch.object(self.node.time, "monotonic",
                                  return_value=ten_seconds_later):
            self.assertTrue(self.node.election_due(self.shard))

    def test_a_leader_never_has_an_election_due(self):
        self.shard.role = self.node.LEADER
        self.shard.last_heartbeat = time.monotonic() - 10
        self.assertFalse(self.node.election_due(self.shard))

    def _prepare_lock(self, lease_left):
        self.shard.key_locks["k"] = "txn-1"
        self.shard.pending_txns["txn-1"] = [{"key": "k", "value": "v"}]
        self.shard.lock_expiry["txn-1"] = time.monotonic() + lease_left

    def test_a_wall_clock_step_forward_does_not_expire_a_transaction_lock(self):
        self._prepare_lock(lease_left=10)
        with self._step_wall_clock(WALL_CLOCK_STEP):
            self.node.expire_txn_locks()
        self.assertEqual(self.shard.key_locks, {"k": "txn-1"})
        self.assertIn("txn-1", self.shard.pending_txns)

    def test_an_expired_transaction_lock_is_released(self):
        self._prepare_lock(lease_left=-1)
        self.node.expire_txn_locks()
        self.assertEqual(self.shard.key_locks, {})
        self.assertEqual(self.shard.pending_txns, {})
        self.assertEqual(self.shard.lock_expiry, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
