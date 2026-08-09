"""
raft_harness.py — Raft 正确性测试用的确定性驱动工具

设计目标：**不靠调度时序赌运行结果**。

localhost 上没法做真网络分区，真三节点集群里选举随时在跑，term 不停变化，
用它来测"某个候选人能不能拿到票"这类问题会变成 flaky test。所以这里的做法是：

  1. 起 **一个真实节点**，把选举超时调到极大（RAFT_ELECTION_TIMEOUT_*），
     它就永远不会自己发起选举，安静地待在 follower 状态；
  2. 由测试**手工构造 RPC** 去驱动它（扮演候选人 / Leader）；
  3. 需要观察真实节点主动发出的 RPC 时，用 FakePeer 扮演对端并录下收到的一切。

这样每个断言都对应一次确定的输入和一次确定的输出。

依赖 RAFT_TEST_MODE=1 才开放的 /debug/raft 内省端点。
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


# ── HTTP 小工具 ─────────────────────────────────────────────
def http_get(port, path, timeout=3):
    try:
        with urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def http_get_status(port, path, timeout=3):
    """返回 HTTP 状态码；连接失败返回 None。用来断言端点是否开放。"""
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


# ── 构造 Raft RPC ───────────────────────────────────────────
def request_vote(port, *, term, candidate_id, last_log_term, last_log_index, shard=0):
    """以候选人身份向真实节点发 RequestVote。

    注意参数命名：last_log_term 是**候选人最后一条日志的 term**，
    与选举用的 term（candidate 的 currentTerm）是两个不同的东西。
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
    """以 Leader 身份向真实节点发 AppendEntries（也用作心跳：entries=[]）。"""
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
    """从 /debug/raft 的输出算出 (lastLogTerm, lastLogIndex)。

    日志非空时取最后一条；日志被快照截空时退回快照边界 —— 这正是
    投票双方必须用同一套算法的地方（见 C2 的 _last_log_locked）。
    """
    if dbg is None:
        return None
    if dbg["log"]:
        return dbg["log"][-1]["term"], dbg["log_offset"] + len(dbg["log"]) - 1
    return dbg["snapshot_term"], dbg["snapshot_index"]


# ── 真实节点 ────────────────────────────────────────────────
class RealNode:
    """一个真实的 node_raft_sharded.py 进程，带确定性计时配置。

    election_timeout=None（默认）表示"永不自发选举"，节点保持 follower，
    完全由测试驱动。需要观察它主动竞选时，传入一个很小的区间。
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
            env.pop("RAFT_NUM_SHARDS", None)   # 走默认行为：NUM_SHARDS = len(ALL_PORTS)
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
        """启动一个**预期会拒绝启动**的节点，返回退出码。

        用于 fail-closed 测试：节点必须明确退出，而不是带着残缺状态服务。
        """
        self.start(wait=False)
        rc = self.proc.wait(timeout=timeout)
        self.proc = None
        return rc

    def kill(self):
        """SIGKILL —— 不给进程任何清理机会。

        这证明的是"写已经交给了操作系统"（flush），**不是**掉电安全（fsync）。
        两者的区别在 docs/RAFT_CORRECTNESS.md 的 I-C1.1 里写清楚了。
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
        """读 /debug/raft?shard=N，返回该分片的内部状态 dict。"""
        r = http_get(self.port, f"/debug/raft?shard={shard}")
        if r is None:
            return None
        return r["shards"][str(shard)]

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.kill()


# ── 假对端 ──────────────────────────────────────────────────
class FakePeer:
    """扮演一个 Raft 对端：录下收到的每一个 RPC，按脚本回应。

    PR1 只用到「录下真实节点主动发出的 RequestVote」这一个能力。
    PR3 会用它扮演持有分歧日志的 follower。
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
                    # 回同一个 term：不去干扰候选人的 term（不触发它降级）
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


# ── 产物清理 ────────────────────────────────────────────────
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
