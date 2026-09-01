"""Client-facing HTTP contract tests for the sharded node.

Every case here is a regression for a defect where the node either answered the
wrong question or answered nothing at all:

1. A single-node group campaigned forever. The vote tally was only evaluated
   inside the per-peer RequestVote callbacks, and with no peers those never ran,
   so the node stayed a candidate and its term climbed without bound.
2. Malformed or wrongly typed request bodies raised inside the handler. The
   socket closed with no status line, so a client could not distinguish a server
   defect from a network failure.
3. ``/get`` recovered its key with ``path.split("=")[-1]``, which returned the
   suffix of any key containing "=" and the value of any trailing parameter.

Run with: python3 test_http_contract.py
"""

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request


PORT = 5601
BASE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE, "node_raft_sharded.py")


def request(path, data=None, raw=None, timeout=3.0):
    """Return ``(status, payload)``; ``(None, None)`` if no response arrived.

    ``raw`` bypasses JSON encoding so a deliberately malformed body can be sent.
    """
    url = f"http://127.0.0.1:{PORT}{path}"
    if raw is not None:
        body = raw
    elif data is not None:
        body = json.dumps(data).encode()
    else:
        body = None
    req = urllib.request.Request(
        url, data=body, method="GET" if body is None else "POST")
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


def wait_until(predicate, timeout=20.0, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None


class HttpContractTest(unittest.TestCase):
    """One single-node process, kept in its own data directory."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.process = subprocess.Popen(
            [sys.executable, SCRIPT, str(PORT),
             "--backend=wal", f"--data-dir={cls._tmp.name}"],
            cwd=BASE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Waiting for the Leader role, not merely for the port to answer: before
        # the fix the node served /health as a candidate indefinitely.
        cls.elected = wait_until(cls._is_leader)

    @classmethod
    def tearDownClass(cls):
        if cls.process.poll() is None:
            cls.process.terminate()
        try:
            cls.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.process.kill()
            cls.process.wait(timeout=5)
        cls._tmp.cleanup()

    @staticmethod
    def _is_leader():
        status, payload = request("/health")
        if status != 200:
            return False
        return any(shard["role"] == "leader"
                   for shard in payload["shards"].values())

    # 1) Single-node election
    def test_single_node_group_elects_itself(self):
        self.assertTrue(
            self.elected,
            "a single-node group holds a majority with its own vote and must "
            "become Leader instead of campaigning forever")

    def test_single_node_term_does_not_run_away(self):
        _, payload = request("/health")
        term = payload["shards"]["0"]["term"]
        self.assertLess(
            term, 20,
            f"term {term} indicates repeated failed elections, not a settled Leader")

    # 2) Every request gets a status code
    def test_missing_key_returns_400(self):
        status, payload = request("/set", {"value": "v"})
        self.assertEqual(status, 400, "a missing key must not drop the connection")
        self.assertIn("key", payload["error"])

    def test_non_string_key_returns_400(self):
        status, _ = request("/set", {"key": 123, "value": "v"})
        self.assertEqual(status, 400)

    def test_non_string_value_returns_400(self):
        status, _ = request("/set", {"key": "k", "value": 9})
        self.assertEqual(status, 400)

    def test_malformed_json_body_returns_400(self):
        status, _ = request("/set", raw=b"{not json")
        self.assertEqual(status, 400, "a malformed body must not drop the connection")

    def test_non_object_body_returns_400(self):
        status, _ = request("/set", [1, 2, 3])
        self.assertEqual(status, 400)

    def test_delete_validates_its_key(self):
        status, _ = request("/delete", {})
        self.assertEqual(status, 400)

    # 3) Query parsing
    def _round_trip(self, key, value):
        status, _ = request("/set", {"key": key, "value": value})
        self.assertEqual(status, 200, f"writing {key!r} failed")
        query = urllib.parse.urlencode({"key": key})
        return request(f"/get?{query}")

    def test_key_containing_equals_round_trips(self):
        status, payload = self._round_trip("a=b", "V1")
        self.assertEqual(status, 200)
        self.assertEqual(payload["key"], "a=b")
        self.assertEqual(payload["value"], "V1")

    def test_key_containing_space_round_trips(self):
        status, payload = self._round_trip("my key", "V2")
        self.assertEqual(status, 200)
        self.assertEqual(payload["key"], "my key")
        self.assertEqual(payload["value"], "V2")

    def test_trailing_query_parameter_is_ignored(self):
        request("/set", {"key": "plain", "value": "V3"})
        status, payload = request("/get?key=plain&debug=1")
        self.assertEqual(status, 200)
        self.assertEqual(payload["value"], "V3",
                         "a trailing parameter must not be mistaken for the key")

    def test_get_without_key_returns_400(self):
        status, _ = request("/get")
        self.assertEqual(status, 400)

    def test_get_with_repeated_key_returns_400(self):
        status, _ = request("/get?key=a&key=b")
        self.assertEqual(status, 400, "an ambiguous key must be rejected, not guessed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
