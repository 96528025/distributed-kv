"""
storage.py — 单节点磁盘持久化引擎（StorageEngine 抽象）

本模块与 Raft 共识**完全解耦**：
  - Raft log（node_raft_sharded.py 里的 shard.log）负责多节点复制与共识。
  - Storage WAL（本模块）只负责**单节点磁盘持久化与进程崩溃恢复**，
    只记录**已经 committed、准备应用到状态机**的操作。

提供两个后端：
  1. JsonStorageEngine —— 旧的“每次提交全量重写 JSON store”路径（legacy / 对照基准）。
  2. WalStorageEngine  —— append-only WAL + 原子 checkpoint（本次升级新增）。

两个后端实现同一套接口：
  - load()                         启动时恢复 store（+ 内部恢复 per-shard applied index）
  - commit(store, records)         持久化一批**已提交**的状态机操作
  - checkpoint(store, applied=...) 发布一次原子 checkpoint（WAL 会顺带轮换）
  - close()                        关闭文件句柄

磁盘格式均带版本号，便于未来迁移。
"""

from __future__ import annotations

import json
import os
import struct
import zlib
import hashlib
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


# ── 版本号（磁盘格式演进用）─────────────────────────────────
WAL_RECORD_VERSION = 1
CHECKPOINT_VERSION = 1

# WAL 每条记录的定长帧头魔数，用来在 replay 时校验“帧对齐”，
# 从而把**中间损坏**（错位）与**尾部半写**（截断）区分开。
_WAL_MAGIC = b"WrE1"
_HEADER = struct.Struct(">I")   # 4 字节大端无符号长度 / crc

# 单条记录 payload 的合理上限（防御性）：长度头若声明超过它，判定为损坏而非尾部截断。
_MAX_RECORD_BYTES = 64 * 1024 * 1024


# ── 异常 ────────────────────────────────────────────────────
class StorageError(Exception):
    """存储层通用错误。"""


class StorageCorruptionError(StorageError):
    """检测到无法安全忽略的损坏（中间 checksum 错误 / checkpoint 校验失败）。

    这类错误**绝不静默跳过**——调用方应把节点置于明确的“恢复失败”状态。
    """


# ── 数据结构 ────────────────────────────────────────────────
@dataclass(frozen=True)
class WalRecord:
    """一条 WAL 记录 —— 只描述**已提交**、待应用到状态机的操作。

    字段与需求一一对应：
      version  记录格式版本
      shard_id Raft/shard 身份（哪个分片的日志）
      index    绝对 log index（该分片内单调递增，用于幂等去重）
      term     该条目的 Raft term
      op       "set" | "delete"
      key      键
      value    值（delete 时为 None）
    checksum 不在此结构内，而是在磁盘帧里由 crc32 承载。
    """
    shard_id: int
    index: int
    term: int
    op: str
    key: str
    value: Optional[str] = None
    version: int = WAL_RECORD_VERSION

    def to_payload(self) -> bytes:
        # 短键，减少写放大；顺序固定，保证可解释与可比对。
        obj = {
            "v": self.version,
            "s": self.shard_id,
            "i": self.index,
            "t": self.term,
            "o": self.op,
            "k": self.key,
            "d": self.value,
        }
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def from_payload(payload: bytes) -> "WalRecord":
        obj = json.loads(payload)
        return WalRecord(
            shard_id=obj["s"],
            index=obj["i"],
            term=obj["t"],
            op=obj["o"],
            key=obj["k"],
            value=obj.get("d"),
            version=obj.get("v", WAL_RECORD_VERSION),
        )


@dataclass
class StorageConfig:
    """持久化配置 —— 路径全部注入，不硬编码。"""
    backend: str = "json"          # "json" | "wal"
    data_dir: str = "."
    port: int = 0
    fsync: bool = False            # 是否对每次 commit 做 fsync（见 durability policy）
    rotate_records: int = 1000     # WAL 累计多少条记录触发一次 checkpoint + 轮换
    rotate_bytes: int = 8 * 1024 * 1024  # 或 WAL 文件超过该字节数触发轮换


# ── 抽象接口 ────────────────────────────────────────────────
class StorageEngine(ABC):
    """持久化后端统一接口。

    约定（与现有并发模型一致）：
      - commit()/checkpoint() 由调用方在**持有 store_lock 时**调用，
        因此对同一引擎的这些调用天然被 store_lock 串行化；
      - 引擎自身**绝不**去获取 store_lock 或 shard.lock，
        锁序恒为 store_lock → engine._lock，不会引入新的死锁。
    """

    @abstractmethod
    def load(self) -> dict[str, str]:
        """恢复并返回 store 快照（幂等；可安全多次调用）。"""

    @abstractmethod
    def commit(self, store: dict[str, str], records: list[WalRecord]) -> None:
        """持久化一批**已提交**操作。records 已被应用到内存 store。"""

    @abstractmethod
    def checkpoint(self, store: dict[str, str],
                   applied: Optional[dict[int, int]] = None) -> None:
        """发布一次原子 checkpoint（WAL 后端会顺带轮换/截断 WAL）。"""

    @abstractmethod
    def close(self) -> None:
        ...

    # 便捷方法：单条 apply（内部转成一批）
    def apply(self, store: dict[str, str], record: WalRecord) -> None:
        self.commit(store, [record])


# ── 后端 1：Legacy JSON 全量重写 ────────────────────────────
class JsonStorageEngine(StorageEngine):
    """旧路径：每次提交都把整个 store 全量重写到一个 JSON 文件。

    保留它有两个目的：
      1. 兼容旧数据文件、保证默认行为与现有测试零改动；
      2. 作为 benchmark 的对照基准（展示 O(n) 全量重写的写放大）。
    """

    def __init__(self, config: StorageConfig):
        self._config = config
        self._path = os.path.join(config.data_dir, f"data_raft_sharded_{config.port}.json")
        self._lock = threading.Lock()

    def load(self) -> dict[str, str]:
        if not os.path.exists(self._path):
            return {}
        with open(self._path, "r") as f:
            data = json.load(f)
        return dict(data)

    def _rewrite(self, store: dict[str, str]) -> None:
        # 与旧 save_to_disk() 字节兼容：json.dump(store)
        tmp = self._path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(store, f)
            f.flush()
            if self._config.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self._path)

    def commit(self, store: dict[str, str], records: list[WalRecord]) -> None:
        with self._lock:
            self._rewrite(store)

    def checkpoint(self, store: dict[str, str],
                   applied: Optional[dict[int, int]] = None) -> None:
        with self._lock:
            self._rewrite(store)

    def close(self) -> None:
        pass


# ── 后端 2：WAL + checkpoint ────────────────────────────────
class WalStorageEngine(StorageEngine):
    """append-only WAL + 原子 checkpoint。

    磁盘布局（均在 data_dir 下）：
      wal_<port>.log            —— append-only 记录流
      checkpoint_<port>.json    —— 最近一次 checkpoint（store 全量 + per-shard applied index）
      checkpoint_<port>.json.tmp—— checkpoint 写入中转（崩溃只会污染它，忽略即可）

    WAL 帧格式（length-prefixed，可靠识别 partial write）：
      MAGIC(4) | payload_len(4, 大端) | payload(JSON) | crc32(4, 大端, 覆盖 payload)

    关键安全不变式：
      **checkpoint 必须在 fsync+rename 落盘后，才允许截断/轮换 WAL。**
    """

    def __init__(self, config: StorageConfig):
        self._config = config
        d = config.data_dir
        p = config.port
        self._wal_path = os.path.join(d, f"wal_{p}.log")
        self._ckpt_path = os.path.join(d, f"checkpoint_{p}.json")
        self._ckpt_tmp = self._ckpt_path + ".tmp"
        self._lock = threading.Lock()

        self._wal_file = None            # 打开的 append 句柄
        self._records_since_ckpt = 0     # 距上次 checkpoint 追加的记录数
        self._wal_bytes = 0              # 当前 WAL 文件字节数
        # per-shard 已持久化/应用到的最大绝对 index（幂等去重 + checkpoint 元数据）
        self._applied: dict[int, int] = {}

    # ---- 帧编解码 ----
    @staticmethod
    def _encode(record: WalRecord) -> bytes:
        payload = record.to_payload()
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        return _WAL_MAGIC + _HEADER.pack(len(payload)) + payload + _HEADER.pack(crc)

    def _open_append(self) -> None:
        self._wal_file = open(self._wal_path, "ab", buffering=0)

    # ---- load / replay ----
    def load(self) -> dict[str, str]:
        with self._lock:
            store: dict[str, str] = {}
            self._applied = {}

            # 1) 加载最新有效 checkpoint（若存在）
            if os.path.exists(self._ckpt_path):
                store, self._applied = self._read_checkpoint(self._ckpt_path)

            # 2) 从 checkpoint 之后 replay WAL（幂等：只应用 index > applied[shard] 的记录）
            if os.path.exists(self._wal_path):
                self._replay_into(store)

            # 3) 打开 WAL 供后续 append，并记录当前大小
            self._open_append()
            self._wal_bytes = os.path.getsize(self._wal_path) if os.path.exists(self._wal_path) else 0
            self._records_since_ckpt = 0
            return store

    def _read_checkpoint(self, path: str) -> tuple[dict[str, str], dict[int, int]]:
        with open(path, "rb") as f:
            raw = f.read()
        try:
            outer = json.loads(raw)
            checksum = outer["checksum"]
            body = outer["payload"]  # body 是一个 JSON 字符串
        except (ValueError, KeyError) as e:
            raise StorageCorruptionError(f"checkpoint 结构无法解析: {path}: {e}")

        if hashlib.sha256(body.encode("utf-8")).hexdigest() != checksum:
            # final checkpoint 是通过 temp+rename 原子发布的，正常崩溃不会写出半个；
            # 若校验失败，说明是真损坏，绝不静默继续。
            raise StorageCorruptionError(f"checkpoint 校验失败（可能损坏）: {path}")

        obj = json.loads(body)
        if obj.get("version") != CHECKPOINT_VERSION:
            raise StorageCorruptionError(
                f"不支持的 checkpoint 版本 {obj.get('version')}（期望 {CHECKPOINT_VERSION}）")
        store = dict(obj["store"])
        applied = {int(k): int(v) for k, v in obj.get("applied", {}).items()}
        return store, applied

    def _replay_into(self, store: dict[str, str]) -> None:
        """按文件顺序 replay WAL。返回时 store / self._applied 已更新。

        - 尾部半写记录：保留此前所有有效记录并安全停止。
        - 中间 checksum/对齐错误：抛 StorageCorruptionError（不静默继续）。
        - 重复记录（index <= applied[shard]）：跳过（幂等）。
        """
        with open(self._wal_path, "rb") as f:
            data = f.read()

        pos = 0
        n = len(data)
        while pos < n:
            # 读魔数：不足 4 字节 → 尾部半写，安全停止
            if n - pos < 4:
                break
            magic = data[pos:pos + 4]
            if magic != _WAL_MAGIC:
                # 4 字节齐全但对不上魔数 → 帧错位 = 中间损坏，绝不静默跳过
                raise StorageCorruptionError(
                    f"WAL 帧对齐失败 @offset={pos}（可能中间损坏）")
            pos += 4

            # 读长度头
            if n - pos < 4:
                break  # 尾部半写
            (plen,) = _HEADER.unpack(data[pos:pos + 4])
            pos += 4

            if plen > _MAX_RECORD_BYTES:
                # 长度头声明了不合理的巨大值：这不是正常的尾部截断，判为损坏。
                raise StorageCorruptionError(
                    f"WAL 记录长度不合理({plen}) @offset={pos - 4}（中间损坏）")

            # 读 payload
            if n - pos < plen:
                break  # 尾部半写（声明长度超过剩余字节）
            payload = data[pos:pos + plen]
            pos += plen

            # 读 crc
            if n - pos < 4:
                break  # 尾部半写
            (crc_stored,) = _HEADER.unpack(data[pos:pos + 4])
            pos += 4

            if (zlib.crc32(payload) & 0xFFFFFFFF) != crc_stored:
                # 完整一帧但 crc 不符 → 内容损坏（非截断），报错
                raise StorageCorruptionError(
                    f"WAL crc 校验失败 @offset={pos}（中间损坏）")

            try:
                rec = WalRecord.from_payload(payload)
            except ValueError as e:
                raise StorageCorruptionError(f"WAL 记录无法解析 @offset={pos}: {e}")

            self._apply_record_to_store(store, rec)

    def _apply_record_to_store(self, store: dict[str, str], rec: WalRecord) -> None:
        prev = self._applied.get(rec.shard_id, -1)
        if rec.index <= prev:
            return  # 幂等：已应用过，跳过
        if rec.op == "delete":
            store.pop(rec.key, None)
        else:
            store[rec.key] = rec.value
        self._applied[rec.shard_id] = rec.index

    # ---- commit ----
    def commit(self, store: dict[str, str], records: list[WalRecord]) -> None:
        if not records:
            return
        with self._lock:
            buf = bytearray()
            for rec in records:
                prev = self._applied.get(rec.shard_id, -1)
                if rec.index <= prev:
                    continue  # 不重复落盘已持久化的 index（幂等）
                buf += self._encode(rec)
                self._applied[rec.shard_id] = rec.index
            if buf:
                # Durability policy：
                #   flush()  把字节交给 OS 页缓存 —— 足以扛住**进程崩溃 / SIGKILL**；
                #   fsync()  才把数据推到物理磁盘 —— 扛**掉电 / OS 崩溃**（可配置，默认关）。
                self._wal_file.write(bytes(buf))
                self._wal_file.flush()
                if self._config.fsync:
                    os.fsync(self._wal_file.fileno())
                self._wal_bytes += len(buf)
                self._records_since_ckpt += 1

            if self._should_rotate():
                self._checkpoint_locked(store, None)

    def _should_rotate(self) -> bool:
        return (self._records_since_ckpt >= self._config.rotate_records
                or self._wal_bytes >= self._config.rotate_bytes)

    # ---- checkpoint（原子发布 + WAL 轮换）----
    def checkpoint(self, store: dict[str, str],
                   applied: Optional[dict[int, int]] = None) -> None:
        with self._lock:
            self._checkpoint_locked(store, applied)

    def _checkpoint_locked(self, store: dict[str, str],
                           applied_override: Optional[dict[int, int]]) -> None:
        """原子发布 checkpoint，然后轮换 WAL。调用方须持 self._lock。

        崩溃安全顺序（每一步都保证任意时刻崩溃都能恢复且不丢/不重放）：
          1) 把 store 全量 + applied 写到 checkpoint.tmp，flush + fsync；
          2) os.replace(tmp, final) 原子发布 checkpoint；
          3) 只有 checkpoint 落盘后，才截断 WAL（重开为空文件）。
        任何在 1/2 之间的崩溃：final checkpoint 未变，恢复用旧 checkpoint + 全量 WAL；
        任何在 2/3 之间的崩溃：新 checkpoint 已生效，WAL 里的记录会因 index<=applied 被跳过。
        """
        if applied_override:
            for sid, idx in applied_override.items():
                self._applied[sid] = max(self._applied.get(sid, -1), idx)

        body = json.dumps({
            "version": CHECKPOINT_VERSION,
            "store": store,
            "applied": {str(k): v for k, v in self._applied.items()},
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        checksum = hashlib.sha256(body.encode("utf-8")).hexdigest()
        outer = json.dumps({"checksum": checksum, "payload": body}, ensure_ascii=False)

        # 步骤 1：写 temp + flush + fsync
        with open(self._ckpt_tmp, "w") as f:
            f.write(outer)
            f.flush()
            os.fsync(f.fileno())   # checkpoint 一律 fsync：它是恢复的唯一可信基点
        # 步骤 2：原子 rename 发布
        os.replace(self._ckpt_tmp, self._ckpt_path)
        # 可选：把目录项也 fsync，确保 rename 本身持久（POSIX）
        self._fsync_dir(os.path.dirname(self._ckpt_path) or ".")

        # 步骤 3：checkpoint 已安全落盘 → 现在才截断 WAL
        if self._wal_file is not None:
            self._wal_file.close()
        with open(self._wal_path, "wb"):
            pass  # 截断为空
        self._open_append()
        self._wal_bytes = 0
        self._records_since_ckpt = 0

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            # 某些平台/文件系统不支持对目录 fsync —— 退化为不做，不影响正确性主线。
            pass

    def close(self) -> None:
        with self._lock:
            if self._wal_file is not None:
                self._wal_file.flush()
                if self._config.fsync:
                    os.fsync(self._wal_file.fileno())
                self._wal_file.close()
                self._wal_file = None


# ── 工厂 ────────────────────────────────────────────────────
def create_storage_engine(config: StorageConfig) -> StorageEngine:
    if config.backend == "wal":
        return WalStorageEngine(config)
    if config.backend == "json":
        return JsonStorageEngine(config)
    raise StorageError(f"未知 backend: {config.backend!r}（支持 'json' | 'wal'）")
