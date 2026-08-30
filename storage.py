"""Single-node persistence engines behind the ``StorageEngine`` interface.

This module is semantically separate from Raft consensus:

* The Raft log in ``node_raft_sharded.py`` drives multi-node replication.
* This storage WAL provides local durability and process-crash recovery. It
  records only committed operations ready to enter the state machine.

Two backends implement the same interface:

* ``JsonStorageEngine`` rewrites the complete JSON store on every commit. It is
  retained for compatibility and as a benchmark baseline.
* ``WalStorageEngine`` uses an append-only WAL and atomic checkpoints.

``load()`` restores the store and per-shard applied indexes, ``commit()``
persists committed state-machine operations, ``checkpoint()`` atomically
publishes state and rotates the WAL, and ``close()`` releases file handles.
Every on-disk format is versioned to support future migrations.
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


# On-disk format versions
WAL_RECORD_VERSION = 1
CHECKPOINT_VERSION = 1

# Fixed magic distinguishes an interior framing error from a truncated tail.
_WAL_MAGIC = b"WrE1"
_HEADER = struct.Struct(">I")   # 4-byte unsigned big-endian length or CRC

# A larger declared payload indicates corruption rather than tail truncation.
_MAX_RECORD_BYTES = 64 * 1024 * 1024


# Exceptions
class StorageError(Exception):
    """Base error for persistence failures."""


class StorageCorruptionError(StorageError):
    """Corruption that cannot be ignored safely, such as an interior CRC error.

    Callers should fail recovery explicitly rather than continue with partial state.
    """


# Data structures
@dataclass(frozen=True)
class WalRecord:
    """A committed operation waiting to be applied to the state machine.

    ``index`` is an absolute, monotonically increasing per-shard log index used
    for idempotency. The frame carries the CRC separately from this payload.
    """
    shard_id: int
    index: int
    term: int
    op: str
    key: str
    value: Optional[str] = None
    version: int = WAL_RECORD_VERSION

    def to_payload(self) -> bytes:
        # Short, stable keys reduce write amplification and keep records comparable.
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
    """Persistence configuration with injected, rather than hard-coded, paths."""
    backend: str = "json"          # "json" | "wal"
    data_dir: str = "."
    port: int = 0
    fsync: bool = False            # fsync each commit; see the durability policy
    rotate_records: int = 1000     # checkpoint after this many WAL records
    rotate_bytes: int = 8 * 1024 * 1024  # or after the WAL reaches this size


# Storage interface
class StorageEngine(ABC):
    """Common interface for persistence backends.

    Callers invoke ``commit()`` and ``checkpoint()`` while holding
    ``store_lock``. Engines never acquire ``store_lock`` or ``shard.lock``;
    lock order therefore remains ``store_lock -> engine._lock``.
    """

    @abstractmethod
    def load(self) -> dict[str, str]:
        """Recover and return a store snapshot; safe to call repeatedly."""

    @abstractmethod
    def commit(self, store: dict[str, str], records: list[WalRecord]) -> None:
        """Persist committed operations already applied to the in-memory store."""

    @abstractmethod
    def checkpoint(self, store: dict[str, str],
                   applied: Optional[dict[int, int]] = None) -> None:
        """Publish an atomic checkpoint; the WAL backend also rotates its log."""

    @abstractmethod
    def close(self) -> None:
        ...

    # Convenience wrapper for a single-record batch.
    def apply(self, store: dict[str, str], record: WalRecord) -> None:
        self.commit(store, [record])

    def applied_indices(self) -> dict[int, int]:
        """Return recovered per-shard state-machine apply positions.

        The JSON backend has no index metadata. WAL checkpoints and replay expose it so
        the node does not reuse record indexes after a restart. This is storage recovery
        metadata, not a replacement for Raft log/snapshot state.
        """
        return {}


# Backend 1: legacy full JSON rewrite
class JsonStorageEngine(StorageEngine):
    """Rewrite the complete store to one JSON file on every commit.

    This backend preserves compatibility with legacy data and provides an O(n)
    full-rewrite baseline for storage benchmarks.
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
        # Preserve the legacy ``json.dump(store)`` representation.
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


# Backend 2: WAL and checkpoint
class WalStorageEngine(StorageEngine):
    """Append-only WAL with atomic checkpoints.

    Files under ``data_dir``:

    * ``wal_<port>.log``: append-only record stream.
    * ``checkpoint_<port>.json``: full store plus per-shard applied indexes.
    * ``checkpoint_<port>.json.tmp``: unpublished checkpoint staging file.

    Frame format::

        MAGIC(4) | payload_len(4, big-endian) | payload(JSON) | crc32(4)

    The WAL may be truncated only after the checkpoint has been fsynced and
    atomically published.
    """

    def __init__(self, config: StorageConfig):
        self._config = config
        d = config.data_dir
        p = config.port
        self._wal_path = os.path.join(d, f"wal_{p}.log")
        self._ckpt_path = os.path.join(d, f"checkpoint_{p}.json")
        self._ckpt_tmp = self._ckpt_path + ".tmp"
        self._lock = threading.Lock()

        self._wal_file = None            # Open append handle.
        self._records_since_ckpt = 0     # Records appended since the checkpoint.
        self._wal_bytes = 0              # Current WAL size in bytes.
        # Highest persisted/applied absolute index per shard, for idempotency and checkpoints.
        self._applied: dict[int, int] = {}

    # ---- Frame encoding and decoding ----
    @staticmethod
    def _encode(record: WalRecord) -> bytes:
        payload = record.to_payload()
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        return _WAL_MAGIC + _HEADER.pack(len(payload)) + payload + _HEADER.pack(crc)

    def _open_append(self) -> None:
        self._wal_file = open(self._wal_path, "ab", buffering=0)

    # ---- Load and replay ----
    def load(self) -> dict[str, str]:
        with self._lock:
            if self._wal_file is not None:
                self._wal_file.close()
                self._wal_file = None
            store: dict[str, str] = {}
            self._applied = {}

            # 1) Load the latest valid checkpoint, if present.
            if os.path.exists(self._ckpt_path):
                store, self._applied = self._read_checkpoint(self._ckpt_path)

            # 2) Replay records newer than each shard's checkpointed applied index.
            if os.path.exists(self._wal_path):
                self._replay_into(store)

            # 3) Open the WAL for appends and record its current size.
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
            body = outer["payload"]  # The body is an encoded JSON string.
        except (ValueError, KeyError) as e:
            raise StorageCorruptionError(f"cannot parse checkpoint structure: {path}: {e}")

        if hashlib.sha256(body.encode("utf-8")).hexdigest() != checksum:
            # Atomic publication cannot produce a partial final checkpoint.
            # A checksum failure therefore indicates real corruption.
            raise StorageCorruptionError(f"checkpoint checksum failed; file may be corrupt: {path}")

        obj = json.loads(body)
        if obj.get("version") != CHECKPOINT_VERSION:
            raise StorageCorruptionError(
                f"unsupported checkpoint version {obj.get('version')} "
                f"(expected {CHECKPOINT_VERSION})")
        store = dict(obj["store"])
        applied = {int(k): int(v) for k, v in obj.get("applied", {}).items()}
        return store, applied

    def _replay_into(self, store: dict[str, str]) -> None:
        """Replay the WAL in file order and update ``store`` and ``_applied``.

        A partial final record ends replay after the valid prefix. Interior CRC
        or alignment errors raise ``StorageCorruptionError``. Records at or
        below a shard's applied index are skipped idempotently.
        """
        with open(self._wal_path, "rb") as f:
            data = f.read()

        pos = 0
        valid_end = 0
        n = len(data)
        while pos < n:
            # Fewer than four magic bytes means a truncated tail.
            if n - pos < 4:
                break
            magic = data[pos:pos + 4]
            if magic != _WAL_MAGIC:
                # A complete but invalid magic value is an interior framing error.
                raise StorageCorruptionError(
                    f"WAL frame alignment failed at offset={pos}; possible interior corruption")
            pos += 4

            # Read the payload length.
            if n - pos < 4:
                break  # Truncated tail.
            (plen,) = _HEADER.unpack(data[pos:pos + 4])
            pos += 4

            if plen > _MAX_RECORD_BYTES:
                # An implausible length indicates corruption, not normal truncation.
                raise StorageCorruptionError(
                    f"invalid WAL record length {plen} at offset={pos - 4}; "
                    "possible interior corruption")

            # Read the payload.
            if n - pos < plen:
                break  # Declared length exceeds the remaining tail.
            payload = data[pos:pos + plen]
            pos += plen

            # Read the CRC.
            if n - pos < 4:
                break  # Truncated tail.
            (crc_stored,) = _HEADER.unpack(data[pos:pos + 4])
            pos += 4

            if (zlib.crc32(payload) & 0xFFFFFFFF) != crc_stored:
                # A complete frame with a mismatched CRC is corrupt, not truncated.
                raise StorageCorruptionError(
                    f"WAL CRC failed at offset={pos}; possible interior corruption")

            try:
                rec = WalRecord.from_payload(payload)
            except (ValueError, KeyError, TypeError) as e:
                raise StorageCorruptionError(f"cannot parse WAL record at offset={pos}: {e}")

            if rec.version != WAL_RECORD_VERSION:
                raise StorageCorruptionError(
                    f"unsupported WAL record version {rec.version} at offset={pos}")
            if rec.op not in ("set", "delete"):
                raise StorageCorruptionError(
                    f"unsupported WAL operation {rec.op!r} at offset={pos}")

            self._apply_record_to_store(store, rec)
            valid_end = pos

        if valid_end < n:
            # A partial final frame is safe to discard, but it must also be removed before
            # reopening in append mode. Otherwise later valid records would sit behind the
            # broken tail and every subsequent replay would stop before reaching them.
            with open(self._wal_path, "r+b") as f:
                f.truncate(valid_end)

    def _apply_record_to_store(self, store: dict[str, str], rec: WalRecord) -> None:
        prev = self._applied.get(rec.shard_id, -1)
        if rec.index <= prev:
            return  # Already applied; skip idempotently.
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
            appended = []
            pending_applied = dict(self._applied)
            for rec in records:
                prev = pending_applied.get(rec.shard_id, -1)
                if rec.index <= prev:
                    continue  # Do not persist an index twice.
                buf += self._encode(rec)
                appended.append(rec)
                pending_applied[rec.shard_id] = rec.index
            if buf:
                # ``flush()`` reaches the OS page cache and survives process crashes.
                # Optional ``fsync()`` provides stronger power-loss durability.
                self._wal_file.write(bytes(buf))
                self._wal_file.flush()
                if self._config.fsync:
                    os.fsync(self._wal_file.fileno())
                # Advance idempotency metadata only after the entire frame batch has been
                # accepted by the file object. A failed write remains retryable.
                for rec in appended:
                    self._applied[rec.shard_id] = rec.index
                self._wal_bytes += len(buf)
                self._records_since_ckpt += len(appended)

            if self._should_rotate():
                self._checkpoint_locked(store, None)

    def _should_rotate(self) -> bool:
        return (self._records_since_ckpt >= self._config.rotate_records
                or self._wal_bytes >= self._config.rotate_bytes)

    # ---- Atomic checkpoint publication and WAL rotation ----
    def checkpoint(self, store: dict[str, str],
                   applied: Optional[dict[int, int]] = None) -> None:
        with self._lock:
            self._checkpoint_locked(store, applied)

    def _checkpoint_locked(self, store: dict[str, str],
                           applied_override: Optional[dict[int, int]]) -> None:
        """Atomically publish a checkpoint, then rotate the WAL.

        The caller must hold ``self._lock``. The crash-safe order is:

        1. Write the full store and applied indexes to the temporary checkpoint,
           then flush and fsync it.
        2. Publish it with ``os.replace``.
        3. Truncate the WAL only after publication.

        A crash before publication leaves the old checkpoint and full WAL. A
        crash after publication leaves duplicate WAL records, which replay skips
        because their indexes are already applied.
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

        # Step 1: write, flush, and fsync the temporary checkpoint.
        with open(self._ckpt_tmp, "w") as f:
            f.write(outer)
            f.flush()
            os.fsync(f.fileno())   # Checkpoints are always fsynced as a recovery anchor.
        # Step 2: publish with an atomic rename.
        os.replace(self._ckpt_tmp, self._ckpt_path)
        # Persist the directory entry where the platform supports it.
        self._fsync_dir(os.path.dirname(self._ckpt_path) or ".")

        # Step 3: now that the checkpoint is durable, truncate the WAL.
        if self._wal_file is not None:
            self._wal_file.close()
        with open(self._wal_path, "wb"):
            pass  # Truncate to an empty file.
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
            # Some platforms do not support fsync on directories.
            pass

    def close(self) -> None:
        with self._lock:
            if self._wal_file is not None:
                self._wal_file.flush()
                if self._config.fsync:
                    os.fsync(self._wal_file.fileno())
                self._wal_file.close()
                self._wal_file = None

    def applied_indices(self) -> dict[int, int]:
        with self._lock:
            return dict(self._applied)


# Factory
def create_storage_engine(config: StorageConfig) -> StorageEngine:
    os.makedirs(config.data_dir, exist_ok=True)
    if config.backend == "wal":
        return WalStorageEngine(config)
    if config.backend == "json":
        return JsonStorageEngine(config)
    raise StorageError(f"unknown backend: {config.backend!r} (supported: 'json' | 'wal')")
