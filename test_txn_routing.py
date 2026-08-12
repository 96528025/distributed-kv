"""Focused tests for transaction prepare routing across Leader changes."""

import importlib.util
import os
import sys
import time
import unittest
from unittest import mock


BASE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE, "node_raft_sharded.py")


def load_node_module():
    module_name = f"node_raft_sharded_txn_test_{time.time_ns()}"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.object(sys, "argv", [SCRIPT, "5201", "5202", "5203"]):
        spec.loader.exec_module(module)
    return module


class TransactionPrepareRoutingTest(unittest.TestCase):
    def setUp(self):
        self.node = load_node_module()

    def test_follows_not_leader_hint_with_same_txn_id(self):
        calls = []

        def fake_rpc(port, path, data, timeout=0.5):
            calls.append((port, data["txn_id"]))
            if port == 5201:
                return {"status": "not_leader", "leader": 5203}
            if port == 5203:
                return {"status": "ready"}
            return None

        self.node.send_rpc = fake_rpc
        result, participant = self.node.send_txn_prepare_with_discovery(
            "txn-1", 0, [{"key": "a", "value": "1"}], 5201
        )

        self.assertEqual(result["status"], "ready")
        self.assertEqual(participant, 5203)
        self.assertEqual(calls, [(5201, "txn-1"), (5203, "txn-1")])

    def test_unreachable_initial_leader_tries_other_known_nodes(self):
        calls = []

        def fake_rpc(port, path, data, timeout=0.5):
            calls.append((port, data["txn_id"]))
            return {"status": "ready"} if port == 5202 else None

        self.node.send_rpc = fake_rpc
        result, participant = self.node.send_txn_prepare_with_discovery(
            "txn-2", 0, [{"key": "a", "value": "1"}], 5201
        )

        self.assertEqual(result["status"], "ready")
        self.assertEqual(participant, 5202)
        self.assertEqual(calls, [(5201, "txn-2"), (5202, "txn-2")])

    def test_lock_conflict_is_not_retried_as_routing_failure(self):
        calls = []

        def fake_rpc(port, path, data, timeout=0.5):
            calls.append(port)
            return {"status": "locked", "key": "a"}

        self.node.send_rpc = fake_rpc
        result, participant = self.node.send_txn_prepare_with_discovery(
            "txn-3", 0, [{"key": "a", "value": "1"}], 5201
        )

        self.assertEqual(result["status"], "locked")
        self.assertEqual(participant, 5201)
        self.assertEqual(calls, [5201])

    def _coordinator(self, fake_rpc):
        self.node.get_shard = lambda key: 0
        with self.node.shards[0].lock:
            self.node.shards[0].leader_id = 5201
        self.node.send_rpc = fake_rpc

        response = {}
        handler = self.node.Handler.__new__(self.node.Handler)
        handler._respond = lambda code, data: response.update(code=code, data=data)
        handler._handle_txn({"ops": [{"key": "a", "value": "1"}]})
        return response

    def test_leader_change_commits_to_actual_prepare_participant(self):
        calls = []

        def fake_rpc(port, path, data, timeout=0.5):
            calls.append((port, path, data["txn_id"]))
            if path == "/txn_prepare" and port == 5201:
                return {"status": "not_leader", "leader": 5203}
            if path == "/txn_prepare" and port == 5203:
                return {"status": "ready"}
            if path == "/txn_commit":
                return {"status": "ok"}
            return None

        response = self._coordinator(fake_rpc)

        self.assertEqual((response["code"], response["data"]["status"]), (200, "ok"))
        prepare_calls = [call for call in calls if call[1] == "/txn_prepare"]
        commit_calls = [call for call in calls if call[1] == "/txn_commit"]
        self.assertEqual([call[0] for call in prepare_calls], [5201, 5203])
        self.assertEqual([call[0] for call in commit_calls], [5203])
        self.assertEqual(len({call[2] for call in calls}), 1)

    def test_abort_targets_participant_that_returned_lock_conflict(self):
        calls = []

        def fake_rpc(port, path, data, timeout=0.5):
            calls.append((port, path, data["txn_id"]))
            if path == "/txn_prepare" and port == 5201:
                return {"status": "not_leader", "leader": 5202}
            if path == "/txn_prepare" and port == 5202:
                return {"status": "locked", "key": "a", "locked_by": "other"}
            if path == "/txn_abort":
                return {"status": "ok"}
            return None

        response = self._coordinator(fake_rpc)

        self.assertEqual(response["data"]["status"], "aborted")
        abort_calls = [call for call in calls if call[1] == "/txn_abort"]
        self.assertEqual([call[0] for call in abort_calls], [5202])
        self.assertEqual(len({call[2] for call in calls}), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
