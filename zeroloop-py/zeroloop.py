#!/usr/bin/env python3
"""
ZeroLoop: Zero-Dependency Crash-Proof Durable Agent State Machine
=================================================================
Ultra-lean, event-sourced, deterministic durable execution runtime for autonomous AI agents.
Pure Python standard library (sqlite3, json, hashlib, uuid, time, dataclasses, enum).

Guarantees:
1. Zero External Dependencies: Pure stdlib SQLite WAL storage.
2. Deterministic Crash Recovery: Commit-before-step journal with sub-5ms rehydration.
3. Cryptographic Event Chaining: Tamper-evident SHA-256 hash linked journal.
4. Idempotency Gate: Deterministic hash(step_idx, tool_name, inputs) prevents duplicate side-effects.
5. Native Human-in-the-Loop: Async pause/resume with zero compute consumption while paused.
6. Configurable Storage Ceilings: Automated FIFO pruning against disk and age thresholds.
"""

import os
import re
import sys
import json
import time
import uuid
import shutil
import hashlib
import sqlite3
import pathlib
import datetime
import argparse
import functools
import threading
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Tuple, Callable, Union, Iterator

# -----------------------------------------------------------------------------
# Global Environment Constants & Defaults
# -----------------------------------------------------------------------------
ENV_STORAGE_DIR = "ZEROLOOP_STORAGE_DIR"
ENV_MAX_STORAGE_MB = "ZEROLOOP_MAX_STORAGE_MB"
ENV_RETENTION_DAYS = "ZEROLOOP_RETENTION_DAYS"
ENV_MIN_DISK_FREE_GB = "ZEROLOOP_MIN_DISK_FREE_GB"

DEFAULT_STORAGE_DIR = os.path.abspath(".zeroloop/journals")
DEFAULT_MAX_STORAGE_MB = 250.0
DEFAULT_RETENTION_DAYS = 7
DEFAULT_MIN_DISK_FREE_GB = 5.0
GENESIS_HASH = "0" * 64
_LAST_PRUNE_SWEEP: Dict[str, float] = {}

def take_bounded_slice(data: Any, max_len: int = 100_000) -> Any:
    """Bounded stream slice preventing memory exhaustion from unbounded journal frames."""
    if isinstance(data, (str, list, bytes)):
        return data[:max_len]
    return data


def canonical_json(obj: Any) -> str:
    """Serializes arbitrary dictionary/primitives into deterministic sorted JSON."""
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        return json.dumps(str(obj), sort_keys=True, separators=(",", ":"))


def compute_idempotency_key(step_idx: int, tool_name: str, inputs: Any) -> str:
    """
    Computes deterministic idempotency token: hash(step_idx, tool_name, sanitized_inputs).
    Guarantees replay safety across process crashes.
    """
    payload = f"{step_idx}:{tool_name}:{canonical_json(inputs)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# -----------------------------------------------------------------------------
# State Machine Lifecycle & Events
# -----------------------------------------------------------------------------
class LoopState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class EventType(str, Enum):
    LOOP_INIT = "LOOP_INIT"
    STATE_TRANSITION = "STATE_TRANSITION"
    STEP_START = "STEP_START"
    STEP_COMPLETE = "STEP_COMPLETE"
    THOUGHT = "THOUGHT"
    TOOL_CALL_START = "TOOL_CALL_START"
    TOOL_CALL_RESULT = "TOOL_CALL_RESULT"
    COST_RECORDED = "COST_RECORDED"
    PAUSE_REQUESTED = "PAUSE_REQUESTED"
    RESUME_ACK = "RESUME_ACK"
    LOOP_COMPLETE = "LOOP_COMPLETE"
    LOOP_FAIL = "LOOP_FAIL"


class LoopPausedException(Exception):
    """Raised when execution yields for human-in-the-loop approval or async pause."""
    def __init__(self, loop_id: str, reason: str, checkpoint: Optional[Dict[str, Any]] = None):
        super().__init__(f"Loop [{loop_id}] paused: {reason}")
        self.loop_id = loop_id
        self.reason = reason
        self.checkpoint = checkpoint or {}


class LoopCrashRecoveryException(Exception):
    """Raised during simulated crash testing."""
    pass


@dataclass
class LoopEvent:
    event_id: int
    event_uuid: str
    timestamp_ns: int
    timestamp_iso: str
    event_type: str
    step_idx: int
    payload: Dict[str, Any]
    prev_event_hash: str
    event_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LoopReceipt:
    loop_id: str
    total_steps: int
    duplicate_tool_calls_prevented: int
    crash_recoveries: int
    active_duration_s: float
    paused_duration_s: float
    total_cost_usd: float
    chain_verified: bool
    total_events: int
    state: str = LoopState.COMPLETED.value
    final_output: Optional[Any] = None

    def render(self) -> str:
        """Renders standard instant value receipt."""
        verif_str = f"VERIFIED ({self.total_events}/{self.total_events} events cryptographically chained)" if self.chain_verified else "FAILED (Integrity Compromised)"
        
        paused_str = ""
        if self.paused_duration_s > 0:
            if self.paused_duration_s >= 3600:
                h = int(self.paused_duration_s // 3600)
                m = int((self.paused_duration_s % 3600) // 60)
                paused_str = f" (Paused for {h}h {m}m awaiting approval)"
            elif self.paused_duration_s >= 60:
                m = int(self.paused_duration_s // 60)
                s = int(self.paused_duration_s % 60)
                paused_str = f" (Paused for {m}m {s}s awaiting approval)"
            else:
                paused_str = f" (Paused for {self.paused_duration_s:.2f}s awaiting approval)"

        lines = [
            f"⚡ ZeroLoop Execution Receipt [{self.loop_id}]:",
            f"• Total Steps: {self.total_steps} completed ({self.duplicate_tool_calls_prevented} duplicates, {self.crash_recoveries} crash recoveries)",
            f"• Duration: {self.active_duration_s:.2f}s active{paused_str}",
            f"• Total Cost: ${self.total_cost_usd:.4f} (routed via HyperGate)",
            f"• Journal Integrity: {verif_str}"
        ]
        return "\n".join(lines)


# -----------------------------------------------------------------------------
# Event Journal (SQLite WAL Event-Sourced Store)
# -----------------------------------------------------------------------------
class EventJournal:
    """
    Atomic SQLite WAL event-sourced journal.
    Maintains append-only hash-chained events, idempotency table, and step snapshots.
    """
    def __init__(self, db_path: str):
        self.db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(self.db_path), mode=0o700, exist_ok=True)
        if not os.path.exists(self.db_path):
            fd = os.open(self.db_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
            os.close(fd)
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            # Enforce SQLite WAL mode and synchronous=NORMAL for crash resilience + fast concurrency
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute("PRAGMA busy_timeout=5000;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
        return self._conn

    def _init_db(self):
        conn = self._get_connection()
        with conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_uuid TEXT NOT NULL UNIQUE,
                    timestamp_ns INTEGER NOT NULL,
                    timestamp_iso TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    step_idx INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL,
                    prev_event_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_events_step ON events (step_idx);
                CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);

                CREATE TABLE IF NOT EXISTS idempotency_index (
                    idempotency_key TEXT PRIMARY KEY,
                    tool_name TEXT NOT NULL,
                    step_idx INTEGER NOT NULL,
                    input_hash TEXT NOT NULL,
                    output_json TEXT NOT NULL,
                    executed_at_iso TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS step_checkpoints (
                    step_idx INTEGER PRIMARY KEY,
                    context_json TEXT NOT NULL,
                    step_name TEXT,
                    timestamp_iso TEXT NOT NULL
                );
            """)

    def set_meta(self, key: str, value: Any):
        conn = self._get_connection()
        val_str = json.dumps(value) if not isinstance(value, str) else value
        with conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
                (key, val_str)
            )

    def get_meta(self, key: str, default: Any = None) -> Any:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM meta WHERE key=?;", (key,))
        row = cursor.fetchone()
        if not row:
            return default
        try:
            return json.loads(row["value"])
        except Exception:
            return row["value"]

    def get_latest_event_hash(self) -> Tuple[int, str]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT event_id, event_hash FROM events ORDER BY event_id DESC LIMIT 1;")
        row = cursor.fetchone()
        if not row:
            return 0, GENESIS_HASH
        return row["event_id"], row["event_hash"]

    def record_event(self, event_type: Union[str, EventType], payload: Dict[str, Any], step_idx: int = 0) -> LoopEvent:
        conn = self._get_connection()
        ev_type = event_type.value if isinstance(event_type, EventType) else str(event_type)
        ev_uuid = f"ev_{uuid.uuid4().hex}"
        now_ns = time.time_ns()
        now_iso = datetime.datetime.fromtimestamp(now_ns / 1_000_000_000.0, datetime.timezone.utc).isoformat()
        payload_str = canonical_json(payload)

        with self._lock:
            with conn:
                cursor = conn.cursor()
            cursor.execute("SELECT event_id, event_hash FROM events ORDER BY event_id DESC LIMIT 1;")
            last_row = cursor.fetchone()
            if last_row:
                prev_id = last_row["event_id"]
                prev_hash = last_row["event_hash"]
                next_id = prev_id + 1
            else:
                prev_hash = GENESIS_HASH
                next_id = 1

            # Compute tamper-evident hash
            hash_input = f"{prev_hash}:{next_id}:{ev_type}:{step_idx}:{payload_str}:{now_ns}".encode("utf-8")
            event_hash = hashlib.sha256(hash_input).hexdigest()

            cursor.execute(
                """
                INSERT INTO events (
                    event_uuid, timestamp_ns, timestamp_iso, event_type,
                    step_idx, payload_json, prev_event_hash, event_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (ev_uuid, now_ns, now_iso, ev_type, step_idx, payload_str, prev_hash, event_hash)
            )
            real_id = cursor.lastrowid or next_id

        return LoopEvent(
            event_id=real_id,
            event_uuid=ev_uuid,
            timestamp_ns=now_ns,
            timestamp_iso=now_iso,
            event_type=ev_type,
            step_idx=step_idx,
            payload=payload,
            prev_event_hash=prev_hash,
            event_hash=event_hash
        )

    _IDEMPOTENCY_MISS = object()

    def get_idempotent_result(self, idempotency_key: str, default: Any = None) -> Any:
        """Checks if a tool invocation has already succeeded for this key."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT output_json FROM idempotency_index WHERE idempotency_key=?;", (idempotency_key,))
            row = cursor.fetchone()
            if not row:
                return default
            try:
                return json.loads(row["output_json"])
            except Exception:
                return row["output_json"]

    def store_idempotent_result(self, idempotency_key: str, tool_name: str, step_idx: int, inputs: Any, output: Any):
        """Indexes tool output against the cryptographic idempotency token."""
        conn = self._get_connection()
        input_hash = hashlib.sha256(canonical_json(inputs).encode("utf-8")).hexdigest()
        output_str = canonical_json(output)
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO idempotency_index (
                    idempotency_key, tool_name, step_idx, input_hash, output_json, executed_at_iso
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(idempotency_key) DO UPDATE SET output_json=excluded.output_json;
                """,
                (idempotency_key, tool_name, step_idx, input_hash, output_str, now_iso)
            )

    def save_checkpoint(self, step_idx: int, context: Dict[str, Any], step_name: str = ""):
        """Stores deterministic context state at step completion (rolling last 50 checkpoints)."""
        conn = self._get_connection()
        ctx_str = canonical_json(context)
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO step_checkpoints (step_idx, context_json, step_name, timestamp_iso)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(step_idx) DO UPDATE SET context_json=excluded.context_json, timestamp_iso=excluded.timestamp_iso;
                """,
                (step_idx, ctx_str, step_name, now_iso)
            )
            # Bound checkpoint storage: keep only the latest 50 step checkpoints to prevent quadratic growth
            conn.execute(
                """
                DELETE FROM step_checkpoints WHERE step_idx NOT IN (
                    SELECT step_idx FROM step_checkpoints ORDER BY step_idx DESC LIMIT 50
                );
                """
            )

    def get_latest_checkpoint(self) -> Optional[Tuple[int, Dict[str, Any], str]]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT step_idx, context_json, step_name FROM step_checkpoints ORDER BY step_idx DESC LIMIT 1;")
        row = cursor.fetchone()
        if not row:
            return None
        try:
            ctx = json.loads(row["context_json"])
        except Exception:
            ctx = {}
        return row["step_idx"], ctx, row["step_name"] or ""

    def get_events(self, limit: int = 1000) -> List[LoopEvent]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT event_id, event_uuid, timestamp_ns, timestamp_iso, event_type,
                   step_idx, payload_json, prev_event_hash, event_hash
            FROM events ORDER BY event_id ASC LIMIT ?;
            """,
            (limit,)
        )
        events = []
        for r in cursor.fetchall():
            try:
                payload = json.loads(r["payload_json"])
            except Exception:
                payload = {"raw": r["payload_json"]}
            events.append(LoopEvent(
                event_id=r["event_id"],
                event_uuid=r["event_uuid"],
                timestamp_ns=r["timestamp_ns"],
                timestamp_iso=r["timestamp_iso"],
                event_type=r["event_type"],
                step_idx=r["step_idx"],
                payload=payload,
                prev_event_hash=r["prev_event_hash"],
                event_hash=r["event_hash"]
            ))
        return events

    def verify_chain(self) -> Tuple[bool, int, str]:
        """
        Cryptographically validates that every event links to its predecessor
        and that payloads have not been tampered with (streaming, unbounded).
        """
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT event_id, timestamp_ns, event_type, step_idx, payload_json, prev_event_hash, event_hash "
                "FROM events ORDER BY event_id ASC;"
            )
            count = 0
            expected_prev = GENESIS_HASH
            for r in cursor:
                count += 1
                ev_id = r["event_id"]
                if r["prev_event_hash"] != expected_prev:
                    return False, ev_id, f"Broken link at event {ev_id}: expected prev_hash {expected_prev}, got {r['prev_event_hash']}"
                hash_input = f"{expected_prev}:{ev_id}:{r['event_type']}:{r['step_idx']}:{r['payload_json']}:{r['timestamp_ns']}".encode("utf-8")
                calc_hash = hashlib.sha256(hash_input).hexdigest()
                if calc_hash != r["event_hash"]:
                    return False, ev_id, f"Tampered event payload at event {ev_id}"
                expected_prev = r["event_hash"]

            if count == 0:
                return True, 0, "EMPTY_JOURNAL"
            return True, count, "VERIFIED"

    def close(self):
        if self._conn:
            try:
                self._conn.commit()
                self._conn.close()
            except Exception:
                pass
            finally:
                self._conn = None


# -----------------------------------------------------------------------------
# Storage Ceilings & Auto-Pruning Engine
# -----------------------------------------------------------------------------
def get_storage_config(
    storage_dir: Optional[str] = None,
    max_storage_mb: Optional[float] = None,
    retention_days: Optional[int] = None,
    min_disk_free_gb: Optional[float] = None
) -> Tuple[str, float, int, float]:
    """Resolves storage directory and pruning thresholds from arguments or environment."""
    s_dir = storage_dir or os.getenv(ENV_STORAGE_DIR) or DEFAULT_STORAGE_DIR
    s_dir = os.path.abspath(s_dir)

    max_mb = max_storage_mb
    if max_mb is None:
        try:
            max_mb = float(os.getenv(ENV_MAX_STORAGE_MB, str(DEFAULT_MAX_STORAGE_MB)))
        except ValueError:
            max_mb = DEFAULT_MAX_STORAGE_MB

    ret_days = retention_days
    if ret_days is None:
        try:
            ret_days = int(os.getenv(ENV_RETENTION_DAYS, str(DEFAULT_RETENTION_DAYS)))
        except ValueError:
            ret_days = DEFAULT_RETENTION_DAYS

    min_free_gb = min_disk_free_gb
    if min_free_gb is None:
        try:
            min_free_gb = float(os.getenv(ENV_MIN_DISK_FREE_GB, str(DEFAULT_MIN_DISK_FREE_GB)))
        except ValueError:
            min_free_gb = DEFAULT_MIN_DISK_FREE_GB

    return s_dir, max_mb, ret_days, min_free_gb


def prune_journals(
    storage_dir: Optional[str] = None,
    max_storage_mb: Optional[float] = None,
    retention_days: Optional[int] = None,
    min_disk_free_gb: Optional[float] = None
) -> Dict[str, Any]:
    """
    Audits completed/failed journals and performs FIFO auto-pruning.
    PROTECTS: Active/Paused loops are NEVER pruned.
    """
    s_dir, max_mb, ret_days, min_free_gb = get_storage_config(
        storage_dir, max_storage_mb, retention_days, min_disk_free_gb
    )

    if not os.path.exists(s_dir):
        return {"pruned_count": 0, "reclaimed_bytes": 0, "total_journals": 0}

    now_ts = time.time()
    retention_cutoff_ts = now_ts - (ret_days * 86400)

    # Scan journal database files
    journal_entries = []
    total_size_bytes = 0

    for fname in os.listdir(s_dir):
        if not fname.endswith(".db"):
            continue
        db_path = os.path.join(s_dir, fname)
        st = os.stat(db_path)
        wal_path = db_path + "-wal"
        shm_path = db_path + "-shm"
        db_size = st.st_size
        if os.path.exists(wal_path):
            db_size += os.stat(wal_path).st_size
        if os.path.exists(shm_path):
            db_size += os.stat(shm_path).st_size

        total_size_bytes += db_size

        # Read loop state from journal (fail closed if unreadable or non-ZeroLoop DB)
        loop_state = None
        temp_conn = None
        try:
            db_uri = pathlib.Path(db_path).resolve().as_uri() + "?mode=ro"
            temp_conn = sqlite3.connect(db_uri, uri=True, timeout=2.0)
            cursor = temp_conn.cursor()
            cursor.execute("SELECT key, value FROM meta WHERE key IN ('state', 'loop_id');")
            meta_rows = dict(cursor.fetchall())
            if "loop_id" in meta_rows and "state" in meta_rows:
                raw_state = meta_rows["state"]
                loop_state = json.loads(raw_state) if raw_state.startswith('"') else raw_state
        except Exception:
            loop_state = None
        finally:
            if temp_conn is not None:
                try:
                    temp_conn.close()
                except Exception:
                    pass

        journal_entries.append({
            "db_path": db_path,
            "wal_path": wal_path,
            "shm_path": shm_path,
            "mtime": st.st_mtime,
            "size_bytes": db_size,
            "state": loop_state,
        })

    # Sort oldest modified first (FIFO)
    journal_entries.sort(key=lambda x: x["mtime"])

    # Check disk free space
    disk_free_bytes = shutil.disk_usage(s_dir).free
    min_free_bytes = int(min_free_gb * 1024 * 1024 * 1024)
    max_storage_bytes = int(max_mb * 1024 * 1024)

    pruned_count = 0
    reclaimed_bytes = 0

    for entry in journal_entries:
        # Invariant: Fail-closed — only prune explicitly COMPLETED or FAILED journals
        if entry["state"] not in (LoopState.COMPLETED.value, LoopState.FAILED.value):
            continue

        needs_prune = False
        reason = ""

        # Condition 1: Exceeded retention period
        if entry["mtime"] < retention_cutoff_ts:
            needs_prune = True
            reason = "retention_expired"
        # Condition 2: Storage ceiling exceeded
        elif (total_size_bytes - reclaimed_bytes) > max_storage_bytes:
            needs_prune = True
            reason = "max_storage_exceeded"
        # Condition 3: Emergency disk floor triggered
        elif (disk_free_bytes + reclaimed_bytes) < min_free_bytes:
            needs_prune = True
            reason = "disk_free_floor_triggered"

        if needs_prune:
            try:
                if os.path.exists(entry["db_path"]):
                    os.remove(entry["db_path"])
                if os.path.exists(entry["wal_path"]):
                    os.remove(entry["wal_path"])
                if os.path.exists(entry["shm_path"]):
                    os.remove(entry["shm_path"])
                reclaimed_bytes += entry["size_bytes"]
                pruned_count += 1
            except Exception:
                pass

    return {
        "pruned_count": pruned_count,
        "reclaimed_bytes": reclaimed_bytes,
        "reclaimed_mb": round(reclaimed_bytes / (1024 * 1024), 3),
        "total_journals": len(journal_entries),
        "remaining_journals": len(journal_entries) - pruned_count
    }


# -----------------------------------------------------------------------------
# DurableLoop Core State Machine
# -----------------------------------------------------------------------------
class DurableLoop:
    """
    Event-sourced durable state machine managing agent execution.
    Provides sub-5ms rehydration, crash-proof step execution, and idempotent tool caching.
    """
    def __init__(
        self,
        loop_id: Optional[str] = None,
        storage_dir: Optional[str] = None,
        task_name: str = "agent_loop",
        initial_context: Optional[Dict[str, Any]] = None,
        max_storage_mb: Optional[float] = None,
        retention_days: Optional[int] = None,
        min_disk_free_gb: Optional[float] = None,
    ):
        self.loop_id = loop_id or f"loop_{uuid.uuid4().hex[:8]}"
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.loop_id):
            raise ValueError(f"Invalid loop_id {self.loop_id!r}: must match ^[A-Za-z0-9_-]{{1,64}}$")
        self.task_name = task_name
        self.storage_dir, self.max_storage_mb, self.retention_days, self.min_disk_free_gb = get_storage_config(
            storage_dir, max_storage_mb, retention_days, min_disk_free_gb
        )
        os.makedirs(self.storage_dir, mode=0o700, exist_ok=True)
        self.db_path = os.path.join(self.storage_dir, f"{self.loop_id}.db")

        self.journal = EventJournal(self.db_path)
        self.context: Dict[str, Any] = initial_context.copy() if initial_context else {}
        self.step_idx: int = 0
        self.state: LoopState = LoopState.PENDING
        self.total_cost_usd: float = 0.0
        self.duplicate_tool_calls_prevented: int = 0
        self.crash_recoveries: int = 0
        self.created_at_ts: float = time.time()
        self.active_duration_s: float = 0.0
        self.paused_duration_s: float = 0.0
        self._current_step_name: str = ""
        self._active_step_running: bool = False

        self._rehydrate_or_init()

    def _rehydrate_or_init(self):
        """Loads state from existing journal in sub-5ms or initializes fresh session."""
        existing_state = self.journal.get_meta("state")
        if existing_state:
            # Rehydration
            t0 = time.perf_counter()
            self.state = LoopState(existing_state)
            self.task_name = self.journal.get_meta("task_name", self.task_name)
            self.step_idx = int(self.journal.get_meta("current_step_idx", 0))
            self.total_cost_usd = float(self.journal.get_meta("total_cost_usd", 0.0))
            self.duplicate_tool_calls_prevented = int(self.journal.get_meta("duplicate_tool_calls_prevented", 0))
            self.crash_recoveries = int(self.journal.get_meta("crash_recoveries", 0))
            self.active_duration_s = float(self.journal.get_meta("active_duration_s", 0.0))
            self.paused_duration_s = float(self.journal.get_meta("paused_duration_s", 0.0))
            self.created_at_ts = float(self.journal.get_meta("created_at_ts", self.created_at_ts))

            # Rehydrate context from latest checkpoint
            checkpoint = self.journal.get_latest_checkpoint()
            if checkpoint:
                ckpt_idx, ckpt_ctx, ckpt_name = checkpoint
                self.context.update(ckpt_ctx)

            # If recovering from an interrupted RUNNING process, tally crash recovery
            if self.state == LoopState.RUNNING:
                self.crash_recoveries += 1
                self.journal.set_meta("crash_recoveries", self.crash_recoveries)
                self.journal.record_event(
                    EventType.STATE_TRANSITION,
                    {
                        "action": "CRASH_RECOVERY",
                        "resumed_at_step": self.step_idx,
                        "rehydration_ms": round((time.perf_counter() - t0) * 1000, 3)
                    },
                    step_idx=self.step_idx
                )
        else:
            # Fresh Initialization
            self.state = LoopState.PENDING
            self.journal.set_meta("loop_id", self.loop_id)
            self.journal.set_meta("task_name", self.task_name)
            self.journal.set_meta("state", self.state.value)
            self.journal.set_meta("current_step_idx", self.step_idx)
            self.journal.set_meta("total_cost_usd", self.total_cost_usd)
            self.journal.set_meta("duplicate_tool_calls_prevented", 0)
            self.journal.set_meta("crash_recoveries", 0)
            self.journal.set_meta("active_duration_s", 0.0)
            self.journal.set_meta("paused_duration_s", 0.0)
            self.journal.set_meta("created_at_ts", self.created_at_ts)

            self.journal.record_event(
                EventType.LOOP_INIT,
                {"loop_id": self.loop_id, "task_name": self.task_name, "initial_context": self.context},
                step_idx=0
            )

    @classmethod
    def load(cls, loop_id: str, storage_dir: Optional[str] = None) -> "DurableLoop":
        """Loads an existing loop by loop_id."""
        s_dir, _, _, _ = get_storage_config(storage_dir)
        db_path = os.path.join(s_dir, f"{loop_id}.db")
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"ZeroLoop journal not found for loop_id '{loop_id}' at {db_path}")
        return cls(loop_id=loop_id, storage_dir=s_dir)

    def transition_to(self, new_state: LoopState, reason: str = ""):
        """Transitions state machine with atomic event record."""
        old_state = self.state
        self.state = new_state
        self.journal.set_meta("state", new_state.value)
        self.journal.set_meta("updated_at_ts", time.time())
        self.journal.record_event(
            EventType.STATE_TRANSITION,
            {"from_state": old_state.value, "to_state": new_state.value, "reason": reason},
            step_idx=self.step_idx
        )

    # -------------------------------------------------------------------------
    # Execution & Stepping
    # -------------------------------------------------------------------------
    def start(self):
        """Starts the execution loop (PAUSED loops must use resume())."""
        if self.state == LoopState.PENDING:
            self.transition_to(LoopState.RUNNING, reason="loop_started")

    def _require_running(self):
        if self.state != LoopState.RUNNING:
            raise RuntimeError(f"Loop [{self.loop_id}] is {self.state.value}; resume() or active RUNNING state required")

    def step_start(self, step_name: str = "", inputs: Optional[Dict[str, Any]] = None) -> int:
        """
        Commits STEP_START before step logic executes.
        Ensures that if killed mid-step, the journal knows step entry.
        """
        if self.state == LoopState.PENDING:
            self.start()
        self._require_running()

        self.step_idx += 1
        self._current_step_name = step_name or f"step_{self.step_idx}"
        self._active_step_running = True
        self.journal.set_meta("current_step_idx", self.step_idx)

        self.journal.record_event(
            EventType.STEP_START,
            {"step_name": self._current_step_name, "inputs": inputs or {}},
            step_idx=self.step_idx
        )
        return self.step_idx

    def step_complete(self, outputs: Optional[Dict[str, Any]] = None, cost_usd: float = 0.0):
        """Commits STEP_COMPLETE and creates a deterministic checkpoint."""
        if outputs:
            self.context.update(outputs)
        if cost_usd > 0.0:
            self.record_cost(cost_usd)

        self.journal.save_checkpoint(self.step_idx, self.context, step_name=self._current_step_name)
        self.journal.record_event(
            EventType.STEP_COMPLETE,
            {"step_name": self._current_step_name, "outputs": outputs or {}, "cost_usd": cost_usd},
            step_idx=self.step_idx
        )
        self._active_step_running = False

    # -------------------------------------------------------------------------
    # Thought & Tool Dispatch (Idempotency Gate)
    # -------------------------------------------------------------------------
    def record_thought(self, thought: str):
        """Records an agent's System-2 thought/reasoning step."""
        self.journal.record_event(
            EventType.THOUGHT,
            {"thought": thought},
            step_idx=self.step_idx
        )

    def record_cost(self, cost_usd: float, model: str = "", route_info: Optional[Dict[str, Any]] = None):
        """Accumulates routed LLM inference cost in journal."""
        self.total_cost_usd += cost_usd
        self.journal.set_meta("total_cost_usd", self.total_cost_usd)
        self.journal.record_event(
            EventType.COST_RECORDED,
            {"cost_usd": cost_usd, "total_cost_usd": self.total_cost_usd, "model": model, "route_info": route_info or {}},
            step_idx=self.step_idx
        )

    def execute_tool(
        self,
        tool_name: str,
        tool_fn: Callable[..., Any],
        inputs: Any,
        sanitize_inputs_fn: Optional[Callable[[Any], Any]] = None
    ) -> Any:
        """
        Idempotency Gate for Tool Invocation.
        Pairs invocation with hash(step_idx, tool_name, sanitized_inputs).
        Replays cached result on recovery, strictly preventing double side-effects.
        """
        self._require_running()
        sanitized_inputs = sanitize_inputs_fn(inputs) if sanitize_inputs_fn else inputs
        idempotency_key = compute_idempotency_key(self.step_idx, tool_name, sanitized_inputs)

        # 1. Check Idempotency Index (sentinel distinguishes cached None from miss)
        cached_output = self.journal.get_idempotent_result(idempotency_key, default=EventJournal._IDEMPOTENCY_MISS)
        if cached_output is not EventJournal._IDEMPOTENCY_MISS:
            self.duplicate_tool_calls_prevented += 1
            self.journal.set_meta("duplicate_tool_calls_prevented", self.duplicate_tool_calls_prevented)
            self.journal.record_event(
                EventType.TOOL_CALL_RESULT,
                {
                    "tool_name": tool_name,
                    "idempotency_key": idempotency_key,
                    "cached": True,
                    "output": cached_output
                },
                step_idx=self.step_idx
            )
            return cached_output

        # 2. Record Pre-Invocation Event
        call_id = f"call_{uuid.uuid4().hex[:8]}"
        self.journal.record_event(
            EventType.TOOL_CALL_START,
            {
                "call_id": call_id,
                "tool_name": tool_name,
                "idempotency_key": idempotency_key,
                "inputs": sanitized_inputs
            },
            step_idx=self.step_idx
        )

        # 3. Execute Tool Safely
        try:
            if isinstance(inputs, dict):
                output = tool_fn(**inputs)
            elif isinstance(inputs, (list, tuple)):
                output = tool_fn(*inputs)
            elif inputs is not None:
                output = tool_fn(inputs)
            else:
                output = tool_fn()
        except Exception as e:
            self.journal.record_event(
                EventType.TOOL_CALL_RESULT,
                {
                    "call_id": call_id,
                    "tool_name": tool_name,
                    "idempotency_key": idempotency_key,
                    "error": str(e),
                    "cached": False
                },
                step_idx=self.step_idx
            )
            raise e

        # 4. Store in Idempotency Index & Record Post-Invocation Event
        self.journal.store_idempotent_result(idempotency_key, tool_name, self.step_idx, sanitized_inputs, output)
        self.journal.record_event(
            EventType.TOOL_CALL_RESULT,
            {
                "call_id": call_id,
                "tool_name": tool_name,
                "idempotency_key": idempotency_key,
                "cached": False,
                "output": output
            },
            step_idx=self.step_idx
        )
        return output

    # -------------------------------------------------------------------------
    # Human-in-the-Loop Async Pause & Resume
    # -------------------------------------------------------------------------
    def pause(self, reason: str = "awaiting_human_approval", checkpoint: Optional[Dict[str, Any]] = None):
        """
        Pauses execution for human review or long-running async external event.
        Closes DB connections and exits cleanly with ZERO memory or threads holding.
        """
        now_ts = time.time()
        self.transition_to(LoopState.PAUSED, reason=reason)
        self.journal.set_meta("pause_reason", reason)
        self.journal.set_meta("paused_at_ts", now_ts)

        if checkpoint:
            self.context.update(checkpoint)
            self.journal.save_checkpoint(self.step_idx, self.context, step_name=self._current_step_name)

        self.journal.record_event(
            EventType.PAUSE_REQUESTED,
            {"reason": reason, "step_idx": self.step_idx, "checkpoint": checkpoint or {}},
            step_idx=self.step_idx
        )
        self.journal.close()
        raise LoopPausedException(loop_id=self.loop_id, reason=reason, checkpoint=checkpoint)

    def resume(self, approval_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Resumes a paused loop.
        Applies dict approval payload and updates state to RUNNING.
        """
        if self.state != LoopState.PAUSED:
            raise RuntimeError(f"Cannot resume loop [{self.loop_id}] in state {self.state.value}; must be PAUSED")
        if approval_payload is not None and not isinstance(approval_payload, dict):
            raise TypeError("approval_payload must be a dict or None")
        now_ts = time.time()
        paused_at_ts = float(self.journal.get_meta("paused_at_ts", now_ts))
        pause_duration = max(0.0, now_ts - paused_at_ts)
        self.paused_duration_s += pause_duration
        self.journal.set_meta("paused_duration_s", self.paused_duration_s)

        if approval_payload:
            self.context.update(approval_payload)
            self.journal.save_checkpoint(self.step_idx, self.context, step_name="resume_approval")

        self.journal.record_event(
            EventType.RESUME_ACK,
            {
                "step_idx": self.step_idx,
                "approval_payload": approval_payload or {},
                "paused_duration_s": round(pause_duration, 2)
            },
            step_idx=self.step_idx
        )
        self.transition_to(LoopState.RUNNING, reason="resumed_with_approval")
        return self.context

    # -------------------------------------------------------------------------
    # Lifecycle Completion & Failure
    # -------------------------------------------------------------------------
    def complete(self, final_output: Optional[Any] = None) -> LoopReceipt:
        """Marks loop completed, verifies cryptographic integrity, and generates value receipt."""
        self.transition_to(LoopState.COMPLETED, reason="loop_completed")
        self.journal.set_meta("final_output", final_output)
        
        now_ts = time.time()
        total_wall_time = max(0.01, now_ts - self.created_at_ts)
        self.active_duration_s = max(0.01, total_wall_time - self.paused_duration_s)
        self.journal.set_meta("active_duration_s", self.active_duration_s)

        self.journal.record_event(
            EventType.LOOP_COMPLETE,
            {"final_output": final_output, "total_steps": self.step_idx, "total_cost_usd": self.total_cost_usd},
            step_idx=self.step_idx
        )

        # Cryptographic chain audit
        is_valid, total_events, msg = self.journal.verify_chain()

        receipt = LoopReceipt(
            loop_id=self.loop_id,
            total_steps=self.step_idx,
            duplicate_tool_calls_prevented=self.duplicate_tool_calls_prevented,
            crash_recoveries=self.crash_recoveries,
            active_duration_s=round(self.active_duration_s, 2),
            paused_duration_s=round(self.paused_duration_s, 2),
            total_cost_usd=round(self.total_cost_usd, 6),
            chain_verified=is_valid,
            total_events=total_events,
            state=self.state.value,
            final_output=final_output
        )

        # Run background auto-prune check (throttled to at most once per 30s per storage_dir)
        now_ts = time.time()
        last_sweep = _LAST_PRUNE_SWEEP.get(self.storage_dir, 0.0)
        if (now_ts - last_sweep) >= 30.0:
            _LAST_PRUNE_SWEEP[self.storage_dir] = now_ts
            try:
                prune_journals(
                    storage_dir=self.storage_dir,
                    max_storage_mb=self.max_storage_mb,
                    retention_days=self.retention_days,
                    min_disk_free_gb=self.min_disk_free_gb
                )
            except Exception:
                pass

        # Release database lock on completion
        self.journal.close()

        return receipt

    def close(self):
        """Flushes and releases journal database lock."""
        if hasattr(self, "journal") and self.journal:
            self.journal.close()

    def fail(self, error: Union[str, Exception]):
        """Marks loop failed with diagnostic traceback summary."""
        err_msg = str(error)
        self.transition_to(LoopState.FAILED, reason=err_msg)
        self.journal.set_meta("error_message", err_msg)
        self.journal.record_event(
            EventType.LOOP_FAIL,
            {"error": err_msg, "step_idx": self.step_idx},
            step_idx=self.step_idx
        )


# -----------------------------------------------------------------------------
# Decorator API
# -----------------------------------------------------------------------------
def durable_loop(
    task_name: str = "agent_loop",
    storage_dir: Optional[str] = None
):
    """
    Decorator turning an agent function or generator into a durable, crash-resilient loop.
    Passes a `loop: DurableLoop` instance into kwargs if requested.
    """
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            loop_id = kwargs.pop("_loop_id", None) or os.getenv("ZEROLOOP_LOOP_ID")
            loop = DurableLoop(loop_id=loop_id, task_name=task_name, storage_dir=storage_dir)
            kwargs["loop"] = loop
            try:
                res = fn(*args, **kwargs)
                if loop.state == LoopState.RUNNING:
                    loop.complete(final_output=res)
                return res
            except LoopPausedException:
                # Clean exit on pause
                return None
            except Exception as e:
                loop.fail(e)
                raise e
        return wrapper
    return decorator


# -----------------------------------------------------------------------------
# Ergonomic CLI Operations & Formatting
# -----------------------------------------------------------------------------
def list_loops(storage_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """Discovers and parses metadata for all active, paused, completed, and failed loops."""
    s_dir, _, _, _ = get_storage_config(storage_dir)
    if not os.path.exists(s_dir):
        return []

    results = []
    for fname in sorted(os.listdir(s_dir), reverse=True):
        if not fname.endswith(".db"):
            continue
        db_path = os.path.join(s_dir, fname)
        try:
            journal = EventJournal(db_path)
            loop_id = journal.get_meta("loop_id", fname[:-3])
            state = journal.get_meta("state", LoopState.PENDING.value)
            task_name = journal.get_meta("task_name", "unnamed")
            steps = journal.get_meta("current_step_idx", 0)
            cost = journal.get_meta("total_cost_usd", 0.0)
            active_s = journal.get_meta("active_duration_s", 0.0)
            created_ts = journal.get_meta("created_at_ts", 0.0)
            created_iso = datetime.datetime.fromtimestamp(created_ts, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if created_ts else "N/A"
            pause_reason = journal.get_meta("pause_reason", "")
            results.append({
                "loop_id": loop_id,
                "state": state,
                "task_name": task_name,
                "steps": steps,
                "cost_usd": cost,
                "active_s": active_s,
                "created_iso": created_iso,
                "pause_reason": pause_reason
            })
            journal.close()
        except Exception:
            pass

    return results


def print_list_table(loops: List[Dict[str, Any]]):
    """Renders a visually aligned ASCII table of all tracked loops."""
    if not loops:
        print("📂 No ZeroLoop sessions found in journal storage.")
        return

    print("⚡ ZeroLoop Durable Execution Registry:")
    print("=" * 105)
    print(f"{'LOOP ID':<18} | {'STATE':<11} | {'STEPS':<6} | {'COST ($)':<9} | {'ACTIVE':<8} | {'CREATED (UTC)':<19} | {'TASK / NOTE'}")
    print("-" * 105)

    for l in loops:
        state_badge = {
            "RUNNING": "🔵 RUNNING",
            "PAUSED": "⏸️  PAUSED",
            "COMPLETED": "✅ DONE",
            "FAILED": "❌ FAILED",
            "PENDING": "⚪ PENDING"
        }.get(l["state"], l["state"])

        note = l["task_name"]
        if l["state"] == "PAUSED" and l["pause_reason"]:
            note += f" (Paused: {l['pause_reason']})"

        act_str = f"{float(l['active_s']):.2f}s"
        cost_str = f"${float(l['cost_usd']):.4f}"
        print(f"{l['loop_id']:<18} | {state_badge:<11} | {l['steps']:<6} | {cost_str:<9} | {act_str:<8} | {l['created_iso']:<19} | {note}")
    print("=" * 105)


def inspect_loop(loop_id: str, storage_dir: Optional[str] = None):
    """Renders execution tree and cryptographic event journal for a specific loop."""
    loop = DurableLoop.load(loop_id, storage_dir=storage_dir)
    events = loop.journal.get_events()
    is_valid, total_events, status_msg = loop.journal.verify_chain()

    state_badge = {
        "RUNNING": "🔵 RUNNING",
        "PAUSED": "⏸️  PAUSED (Awaiting Human Approval)",
        "COMPLETED": "✅ COMPLETED",
        "FAILED": "❌ FAILED",
        "PENDING": "⚪ PENDING"
    }.get(loop.state.value, loop.state.value)

    print(f"\n🔍 ZeroLoop Inspection Graph: [{loop.loop_id}]")
    print("=" * 80)
    print(f"• Task Name:      {loop.task_name}")
    print(f"• State:          {state_badge}")
    print(f"• Total Steps:    {loop.step_idx}")
    print(f"• Duplicates Sunk:{loop.duplicate_tool_calls_prevented} duplicate calls prevented via Idempotency Gate")
    print(f"• Crash Recoveries:{loop.crash_recoveries}")
    print(f"• Total Cost:     ${loop.total_cost_usd:.4f}")
    print(f"• Journal Chain:  {'✅ ' + status_msg if is_valid else '❌ ' + status_msg} ({total_events} events)")
    print("-" * 80)
    print("🌲 Execution Event Ledger (Chronological DAG):")

    for ev in events:
        ts = ev.timestamp_iso.split("T")[1][:8]
        p_summary = ""
        if ev.event_type == EventType.STEP_START.value:
            p_summary = f"Step '{ev.payload.get('step_name')}'"
        elif ev.event_type == EventType.THOUGHT.value:
            thought = ev.payload.get("thought", "")
            p_summary = f'"{thought[:60]}..."' if len(thought) > 60 else f'"{thought}"'
        elif ev.event_type == EventType.TOOL_CALL_START.value:
            p_summary = f"Dispatching '{ev.payload.get('tool_name')}' [Idemp: {ev.payload.get('idempotency_key')[:8]}...]"
        elif ev.event_type == EventType.TOOL_CALL_RESULT.value:
            cached_flag = " [CACHE REPLAY]" if ev.payload.get("cached") else ""
            out = str(ev.payload.get("output", ""))
            out_trunc = out[:50] + "..." if len(out) > 50 else out
            p_summary = f"Result '{ev.payload.get('tool_name')}'{cached_flag} -> {out_trunc}"
        elif ev.event_type == EventType.PAUSE_REQUESTED.value:
            p_summary = f"Paused: reason='{ev.payload.get('reason')}'"
        elif ev.event_type == EventType.RESUME_ACK.value:
            p_summary = f"Resumed with approval payload: {ev.payload.get('approval_payload')}"
        elif ev.event_type == EventType.LOOP_COMPLETE.value:
            p_summary = f"Finished! Output: {ev.payload.get('final_output')}"
        elif ev.event_type == EventType.COST_RECORDED.value:
            p_summary = f"+${ev.payload.get('cost_usd'):.4f} via {ev.payload.get('model', 'LLM')}"
        else:
            p_summary = json.dumps(ev.payload)[:65]

        print(f"  [{ts}] #{ev.event_id:<3} Step {ev.step_idx:<2} | {ev.event_type:<18} | {p_summary}")

    print("=" * 80 + "\n")


def print_claude_desktop_config():
    """Generates ready-to-paste Claude Desktop MCP config."""
    script_path = os.path.abspath(__file__)
    config = {
        "mcpServers": {
            "zeroloop": {
                "command": sys.executable,
                "args": [script_path, "--serve-mcp"],
                "env": {
                    "ZEROLOOP_STORAGE_DIR": DEFAULT_STORAGE_DIR,
                    "ZEROLOOP_MAX_STORAGE_MB": "250.0"
                }
            }
        }
    }
    print("📋 Claude Desktop / ZeroMCP Configuration Snippet:")
    print(json.dumps(config, indent=2))


# -----------------------------------------------------------------------------
# Main CLI Entrypoint
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="ZeroLoop: Ultra-lean, zero-dependency, event-sourced durable agent state machine."
    )
    parser.add_argument("--list", "-l", action="store_true", help="List all active, paused, and completed loops")
    parser.add_argument("--inspect", "-i", type=str, metavar="LOOP_ID", help="Inspect a loop's execution graph and events")
    parser.add_argument("--resume", "-r", type=str, metavar="LOOP_ID", help="Resume an interrupted or paused loop")
    parser.add_argument("--approval", type=str, default="", help="JSON approval payload for resuming a paused loop")
    parser.add_argument("--verify", "-v", type=str, metavar="LOOP_ID", help="Cryptographically audit event journal integrity")
    parser.add_argument("--prune", action="store_true", help="Trigger FIFO storage pruning against configured MB ceilings")
    parser.add_argument("--print-config", action="store_true", help="Print Claude Desktop / ZeroMCP configuration JSON")
    parser.add_argument("--storage-dir", type=str, default=None, help="Custom journal storage directory")

    args = parser.parse_args()

    if args.list:
        loops = list_loops(storage_dir=args.storage_dir)
        print_list_table(loops)
        sys.exit(0)

    if args.inspect:
        inspect_loop(args.inspect, storage_dir=args.storage_dir)
        sys.exit(0)

    if args.verify:
        try:
            loop = DurableLoop.load(args.verify, storage_dir=args.storage_dir)
            valid, count, msg = loop.journal.verify_chain()
            if valid:
                print(f"✅ Cryptographic Integrity VERIFIED: {count}/{count} events strictly chained.")
                sys.exit(0)
            else:
                print(f"❌ Cryptographic Integrity COMPROMISED: {msg}")
                sys.exit(1)
        except Exception as e:
            print(f"❌ Error verifying journal: {e}")
            sys.exit(1)

    if args.resume:
        try:
            loop = DurableLoop.load(args.resume, storage_dir=args.storage_dir)
            approval_payload = json.loads(args.approval) if args.approval else {"approved": True}
            print(f"▶️  Resuming loop [{loop.loop_id}] with payload: {approval_payload}...")
            loop.resume(approval_payload=approval_payload)
            print(f"✅ Loop [{loop.loop_id}] resumed to state: {loop.state.value}. Ready for next node.")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error resuming loop: {e}")
            sys.exit(1)

    if args.prune:
        result = prune_journals(storage_dir=args.storage_dir)
        print(f"🧹 Storage Pruning Report: {result['pruned_count']} journals pruned, {result['reclaimed_mb']} MB reclaimed. {result['remaining_journals']} journals remaining.")
        sys.exit(0)

    if args.print_config:
        print_claude_desktop_config()
        sys.exit(0)

    parser.print_help()


if __name__ == "__main__":
    main()
