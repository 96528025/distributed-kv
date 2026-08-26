"""Unit and live-endpoint tests for the metrics/observability milestone."""

import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from unittest import mock

import metrics


BASE = os.path.dirname(os.path.abspath(__file__))
NODE_SCRIPT = os.path.join(BASE, "node_raft_sharded.py")


def load_node_module(data_dir=None):
    module_name = f"node_raft_sharded_metrics_test_{time.time_ns()}"
    spec = importlib.util.spec_from_file_location(module_name, NODE_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    argv = [NODE_SCRIPT, "5401", "5402", "5403"]
    if data_dir is not None:
        argv.append(f"--data-dir={data_dir}")
    with mock.patch.object(sys, "argv", argv):
        spec.loader.exec_module(module)
    return module


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(port, path, data=None, timeout=1.0):
    body = None if data is None else json.dumps(data).encode("utf-8")
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=body,
        method="GET" if data is None else "POST",
    )
    if body is not None:
        req.add_header("Content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, response.headers, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        with error:
            payload = error.read().decode("utf-8")
            return error.code, error.headers, payload


class MetricsPrimitiveTest(unittest.TestCase):
    def test_counter_gauge_and_histogram_render_prometheus_text(self):
        registry = metrics.Registry()
        counter = registry.counter("requests_total", "Requests", ("route",))
        gauge = registry.gauge("queue_depth", "Queue depth")
        histogram = registry.histogram(
            "latency_seconds", "Latency", (0.1, 0.5), ("route",)
        )

        counter.inc(route='/say"hello')
        gauge.set(3)
        histogram.observe(0.05, route="/get")
        histogram.observe(0.2, route="/get")

        rendered = registry.render()
        self.assertIn('requests_total{route="/say\\"hello"} 1', rendered)
        self.assertIn("queue_depth 3", rendered)
        self.assertIn(
            'latency_seconds_bucket{route="/get",le="0.1"} 1', rendered
        )
        self.assertIn(
            'latency_seconds_bucket{route="/get",le="0.5"} 2', rendered
        )
        self.assertIn(
            'latency_seconds_bucket{route="/get",le="+Inf"} 2', rendered
        )
        self.assertIn('latency_seconds_count{route="/get"} 2', rendered)

    def test_fixed_labels_reject_missing_extra_and_negative_counter_values(self):
        registry = metrics.Registry()
        counter = registry.counter("events_total", "Events", ("outcome",))

        with self.assertRaises(ValueError):
            counter.inc()
        with self.assertRaises(ValueError):
            counter.inc(outcome="ok", unexpected="value")
        with self.assertRaises(ValueError):
            counter.inc(-1, outcome="ok")

    def test_counter_updates_are_thread_safe(self):
        registry = metrics.Registry()
        counter = registry.counter("work_total", "Work")

        def increment_many():
            for _ in range(1000):
                counter.inc()

        workers = [threading.Thread(target=increment_many) for _ in range(8)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        self.assertIn("work_total 8000", registry.render())


class NodeMetricsLogicTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.data_dir.cleanup)
        self.node = load_node_module(self.data_dir.name)

    def test_scrape_time_gauges_match_current_shard_state(self):
        shard = self.node.shards[1]
        with shard.lock:
            shard.term = 9
            shard.role = self.node.CANDIDATE
            shard.commit_index = 17
            shard.log = [{"term": 9, "op": "set", "key": "secret-key"}]
            shard.pending_txns["high-cardinality-txn-id"] = []

        self.node.refresh_state_metrics()
        rendered = self.node.metrics_registry.render()

        self.assertIn('distributed_kv_raft_term{shard="1"} 9', rendered)
        self.assertIn('distributed_kv_raft_commit_index{shard="1"} 17', rendered)
        self.assertIn(
            'distributed_kv_raft_role{shard="1",role="candidate"} 1', rendered
        )
        self.assertIn(
            'distributed_kv_pending_transactions{shard="1"} 1', rendered
        )
        self.assertNotIn("secret-key", rendered)
        self.assertNotIn("high-cardinality-txn-id", rendered)

    def test_read_quorum_failures_and_higher_term_persistence_are_counted(self):
        shard = self.node.shards[0]
        with shard.lock:
            shard.role = self.node.LEADER
            shard.term = 4
            shard.leader_id = self.node.MY_PORT
        self.node.send_rpc = lambda *args, **kwargs: None

        self.assertFalse(self.node.confirm_read_quorum(shard, timeout=0.01))
        rendered = self.node.metrics_registry.render()
        self.assertIn(
            'distributed_kv_read_quorum_checks_total{shard="0",outcome="unavailable"} 1',
            rendered,
        )

        with shard.lock:
            shard.role = self.node.LEADER
            shard.term = 4
            shard.voted_for = self.node.MY_PORT
            shard.leader_id = self.node.MY_PORT
        self.node.send_rpc = lambda *args, **kwargs: {"term": 5, "success": False}
        self.node.persist_hard_state = mock.Mock()

        self.assertFalse(self.node.confirm_read_quorum(shard, timeout=0.01))
        self.node.persist_hard_state.assert_called_once_with()
        with shard.lock:
            self.assertEqual(shard.term, 5)
            self.assertEqual(shard.role, self.node.FOLLOWER)
            self.assertIsNone(shard.voted_for)
        rendered = self.node.metrics_registry.render()
        self.assertIn(
            'distributed_kv_read_quorum_checks_total{shard="0",outcome="higher_term"} 1',
            rendered,
        )

    def test_election_and_leader_transition_are_counted(self):
        shard = self.node.shards[2]
        self.node.send_rpc = lambda *args, **kwargs: None
        self.node.start_election(shard)
        with shard.lock:
            self.node._become_leader_locked(shard)

        rendered = self.node.metrics_registry.render()
        self.assertIn(
            'distributed_kv_raft_elections_total{shard="2"} 1', rendered
        )
        self.assertIn(
            'distributed_kv_raft_leader_transitions_total{shard="2"} 1', rendered
        )

    def test_successful_replication_round_is_observed(self):
        shard = self.node.shards[0]
        with shard.lock:
            shard.role = self.node.LEADER
            shard.term = 3
            shard.leader_id = self.node.MY_PORT
        self.node.send_rpc = lambda *args, **kwargs: {"term": 3, "success": True}
        self.node.persist_committed = lambda records: None
        self.node.maybe_snapshot = lambda _shard: None

        success, error = self.node.Handler._do_raft_op(
            None, shard, "instrumented-key", "value"
        )

        self.assertTrue(success, error)
        rendered = self.node.metrics_registry.render()
        self.assertIn(
            'distributed_kv_raft_replication_round_duration_seconds_count'
            '{shard="0",outcome="success"} 1',
            rendered,
        )

    def test_successful_snapshot_creation_is_counted(self):
        shard = self.node.shards[1]
        with tempfile.TemporaryDirectory() as data_dir:
            self.node.DATA_DIR = data_dir
            with self.node.store_lock:
                self.node.store["snapshot-key"] = "value"
            with shard.lock:
                shard.term = 2
                shard.log = [
                    {"term": 2, "op": "set", "key": f"k{index}", "value": "v"}
                    for index in range(self.node.SNAPSHOT_THRESHOLD + 1)
                ]
                shard.log_offset = 0
                shard.commit_index = self.node.SNAPSHOT_THRESHOLD

            self.node.maybe_snapshot(shard)

            self.assertTrue(os.path.exists(self.node.snapshot_path(shard.shard_id)))
            rendered = self.node.metrics_registry.render()
            self.assertIn(
                'distributed_kv_snapshot_operations_total'
                '{shard="1",operation="create"} 1',
                rendered,
            )


class MetricsEndpointTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.TemporaryDirectory()
        self.port = free_port()
        self.process = subprocess.Popen(
            [
                sys.executable,
                NODE_SCRIPT,
                str(self.port),
                f"--data-dir={self.data_dir.name}",
            ],
            cwd=BASE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.addCleanup(self._cleanup)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                status, _, _ = request(self.port, "/health", timeout=0.2)
                if status == 200:
                    break
            except (OSError, TimeoutError):
                time.sleep(0.05)
        else:
            self.fail("single-node process did not become reachable")

    def _cleanup(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.data_dir.cleanup()

    def test_metrics_endpoint_and_http_instrumentation(self):
        status, _, _ = request(self.port, "/txn", {"ops": []})
        self.assertEqual(status, 400)

        status, headers, body = request(self.port, "/metrics")
        self.assertEqual(status, 200)
        self.assertIn("text/plain", headers.get("Content-Type"))
        self.assertIn(
            f'distributed_kv_node_info{{port="{self.port}",backend="json"}} 1',
            body,
        )
        self.assertIn(
            'distributed_kv_http_requests_total{method="GET",route="/metrics",status="200"} 1',
            body,
        )
        self.assertIn(
            'distributed_kv_http_requests_total{method="POST",route="/txn",status="400"} 1',
            body,
        )
        self.assertIn(
            'distributed_kv_transaction_coordinator_results_total{outcome="invalid"} 1',
            body,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
