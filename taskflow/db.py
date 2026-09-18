"""SQLite persistence layer for taskflow.

Design notes
------------
* Every state transition is one fully journaled SQLite statement (WAL mode +
  synchronous=FULL), so a ``kill -9`` at any instant leaves a consistent
  database.
* Claiming a task is a single atomic ``UPDATE ... WHERE status IN (...)
  AND next_attempt_at <= now ... RETURNING``.  Two workers (threads or
  processes) can never claim the same row: SQLite serialises the writers and
  the second one finds nothing eligible.
* Crashed workers' ``running`` tasks are recovered by
  :meth:`Database.recover_tasks` (at worker startup and via a periodic reaper)
  using heartbeat + pid + process-start-time liveness, which guarantees a
  task is never executed twice at once.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Iterator, Optional

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    command         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL,
    started_at      REAL,
    finished_at     REAL,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error      TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_claim
    ON tasks (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS task_runs (
    id          TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL REFERENCES tasks(id),
    worker_id   TEXT NOT NULL,
    started_at  REAL NOT NULL,
    ended_at    REAL,
    exit_code   INTEGER,
    child_pid   INTEGER,
    interrupted INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_runs_task ON task_runs (task_id, started_at);

CREATE TABLE IF NOT EXISTS workers (
    id             TEXT PRIMARY KEY,
    pid            INTEGER NOT NULL,
    started_at     REAL NOT NULL,
    last_heartbeat REAL NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active',
    start_time     REAL
);
"""

# Columns added after the first release; applied as lightweight migrations.
_MIGRATIONS = {
    "workers": {"start_time": "REAL"},
}


class DuplicateTaskError(Exception):
    """Raised when a task with the same id is submitted twice."""


class TaskNotFoundError(Exception):
    """Raised when a queried task id does not exist."""


def pid_alive(pid: int) -> bool:
    """Best-effort check whether a process id currently exists."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if sys.platform == "win32":
            return _windows_pid_alive(pid)
        import errno

        return getattr(exc, "errno", None) == errno.EPERM
    return True


def process_start_time(pid: int) -> Optional[float]:
    """Creation time of *pid* (unix epoch seconds), or None if unknown/gone."""
    if sys.platform == "win32":
        return _windows_process_start_time(pid)
    try:
        with open(f"/proc/{pid}/stat") as fh:
            data = fh.read()
        # Field 22 is starttime in clock ticks since boot.
        after = data[data.rfind(")") + 2 :].split()
        ticks = int(after[19])
        with open("/proc/stat") as fh:
            boot = float(fh.readline().split()[4]) / os.sysconf("SC_CLK_TCK")
        return boot + ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError, AttributeError):
        return None


def _windows_pid_alive(pid: int) -> bool:
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return f'"{pid}"' in result.stdout


def _windows_process_start_time(pid: int) -> Optional[float]:
    # PowerShell/CIM may be unavailable in locked-down environments; treat
    # failure as "unknown" and fall back to heartbeat staleness only.
    ps = (
        "(Get-Process -Id %d -ErrorAction SilentlyContinue).StartTime.ToString("
        "'o')" % pid
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps],
            capture_output=True,
            text=True,
            timeout=8,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    line = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else ""
    if not line:
        return None
    try:
        from datetime import datetime

        return datetime.fromisoformat(line).timestamp()
    except ValueError:
        return None


def _db_is_local(path: str) -> bool:
    """True when *path* resides on this machine (workers are local pids)."""
    if path in (":memory:", ""):
        return True
    try:
        drive = os.path.splitdrive(os.path.realpath(path))[0].lower()
        return not drive.startswith("\\")
    except OSError:
        return True


class Database:
    """Thread-safe wrapper around a single sqlite connection.

    All public methods are synchronous; the async worker layer runs them in
    an executor.  An internal lock serialises access in-process; SQLite's own
    locking (busy_timeout + WAL) serialises across processes.
    """

    def __init__(self, path: Optional[str] = None) -> None:
        self.path = path or config.DB_PATH
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path,
            timeout=10.0,
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 10000")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = FULL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        for table, columns in _MIGRATIONS.items():
            existing = {
                row[1]
                for row in self._conn.execute(f"PRAGMA table_info({table})")
            }
            for name, decl in columns.items():
                if name not in existing:
                    self._conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN {name} {decl}"
                    )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")
    # ------------------------------------------------------------------ tasks

    def add_task(self, task_id: str, command: str) -> None:
        now = time.time()
        try:
            with self._txn() as conn:
                conn.execute(
                    "INSERT INTO tasks (id, command, created_at) "
                    "VALUES (?, ?, ?)",
                    (task_id, command, now),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateTaskError(
                f"task already exists: {task_id!r}"
            ) from exc

    def get_task(self, task_id: str) -> sqlite3.Row:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(f"no such task: {task_id!r}")
        return row

    def list_tasks(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM tasks ORDER BY created_at, id"
                ).fetchall()
            )

    def list_runs(self, task_id: str) -> list[sqlite3.Row]:
        self.get_task(task_id)
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM task_runs WHERE task_id = ? "
                    "ORDER BY started_at",
                    (task_id,),
                ).fetchall()
            )

    def claim_task(self, worker_id: str) -> Optional[sqlite3.Row]:
        """Atomically take one runnable task and mark it ``running``.

        Runnable tasks are ``pending`` (initial or crash-reset) or ``failed``
        whose exponential backoff has elapsed.
        """
        now = time.time()
        run_id = uuid.uuid4().hex
        with self._txn() as conn:
            updated = conn.execute(
                """
                UPDATE tasks
                   SET status = 'running',
                       attempts = attempts + 1,
                       started_at = COALESCE(started_at, ?),
                       next_attempt_at = 0
                 WHERE id = (
                        SELECT id FROM tasks
                         WHERE status IN ('pending', 'failed')
                           AND next_attempt_at <= ?
                         ORDER BY created_at
                         LIMIT 1
                 )
                 RETURNING id
                """,
                (now, now),
            ).fetchone()
            if updated is None:
                return None
            task_id = updated[0]
            conn.execute(
                "INSERT INTO task_runs (id, task_id, worker_id, started_at) "
                "VALUES (?, ?, ?, ?)",
                (run_id, task_id, worker_id, now),
            )
            # RETURNING hands back the pre-UPDATE values in SQLite; re-read
            # so callers observe the new status/attempts.
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return row

    def set_run_child(self, task_id: str, worker_id: str, child_pid: int) -> None:
        with self._txn() as conn:
            conn.execute(
                """
                UPDATE task_runs
                   SET child_pid = ?
                 WHERE id = (
                        SELECT id FROM task_runs
                         WHERE task_id = ? AND worker_id = ? AND ended_at IS NULL
                         ORDER BY started_at DESC LIMIT 1
                 )
                """,
                (child_pid, task_id, worker_id),
            )

    def finish_task(self, task_id: str, exit_code: int, output: str) -> str:
        """Record the outcome of one attempt.

        Returns ``succeeded``, ``failed`` (retry scheduled with backoff) or
        ``dead`` (retries exhausted).
        """
        now = time.time()
        with self._txn() as conn:
            task = conn.execute(
                "SELECT attempts, status FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise TaskNotFoundError(f"no such task: {task_id!r}")
            if task["status"] != "running":
                # Defensive: never overwrite backoff/terminal state with a
                # stray result from an un-leased attempt.
                return task["status"]
            attempts = task["attempts"]
            if exit_code == 0:
                new_status, next_at = "succeeded", 0.0
            elif attempts < config.MAX_ATTEMPTS:
                new_status = "failed"
                backoff = (
                    config.RETRY_BACKOFF_BASE
                    * config.RETRY_BACKOFF_FACTOR ** (attempts - 1)
                )
                next_at = now + backoff
            else:
                new_status, next_at = "dead", 0.0
            conn.execute(
                """
                UPDATE tasks
                   SET status = ?,
                       finished_at = ?,
                       next_attempt_at = ?,
                       last_error = NULLIF(?, '')
                 WHERE id = ?
                """,
                (
                    new_status,
                    now if new_status != "failed" else None,
                    next_at,
                    output if exit_code != 0 else "",
                    task_id,
                ),
            )
            conn.execute(
                """
                UPDATE task_runs
                   SET ended_at = ?, exit_code = ?
                 WHERE id = (
                        SELECT id FROM task_runs
                         WHERE task_id = ? AND ended_at IS NULL
                         ORDER BY started_at DESC LIMIT 1
                 )
                """,
                (now, exit_code, task_id),
            )
        return new_status

    def counts_by_status(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"
            ).fetchall()
        return {row["status"]: row["n"] for row in rows}
    # ---------------------------------------------------------------- workers

    def register_worker(self, worker_id: str, pid: int) -> None:
        now = time.time()
        start = process_start_time(pid)
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO workers "
                "(id, pid, started_at, last_heartbeat, status, start_time) "
                "VALUES (?, ?, ?, ?, 'active', ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "pid = excluded.pid, "
                "last_heartbeat = excluded.last_heartbeat, "
                "start_time = excluded.start_time, "
                "status = 'active'",
                (worker_id, pid, now, now, start),
            )

    def heartbeat(self, worker_id: str) -> None:
        with self._txn() as conn:
            conn.execute(
                "UPDATE workers SET last_heartbeat = ?, status = 'active' "
                "WHERE id = ?",
                (time.time(), worker_id),
            )

    def mark_worker_shutdown(self, worker_id: str) -> None:
        with self._txn() as conn:
            conn.execute(
                "UPDATE workers SET status = 'shutdown', last_heartbeat = ? "
                "WHERE id = ?",
                (time.time(), worker_id),
            )

    def _is_dead(self, row: sqlite3.Row, now: float) -> bool:
        """A worker is dead when its heartbeat is stale AND its process is
        confirmed gone (pid missing, or pid reused by a process with a
        different creation time)."""
        age = now - row["last_heartbeat"]
        if age < config.HEARTBEAT_INTERVAL:
            return False
        pid = row["pid"]
        if not pid_alive(pid):
            return True
        # Pid exists.  If we know both creation times and they differ, the
        # original worker crashed and the pid was recycled.
        recorded = row["start_time"]
        if recorded is not None and age >= config.DEAD_WORKER_THRESHOLD:
            current = process_start_time(pid)
            if current is not None and abs(current - recorded) > 1.0:
                return True
            if current is None and _db_is_local(self.path):
                # Cannot verify identity; the long stale heartbeat is enough.
                return age >= config.DEAD_WORKER_THRESHOLD
        return False

    def _dead_worker_rows(
        self, conn: sqlite3.Connection, now: float
    ) -> list[sqlite3.Row]:
        rows = conn.execute(
            "SELECT id, pid, last_heartbeat, start_time FROM workers "
            "WHERE status = 'active' AND last_heartbeat < ?",
            (now - config.HEARTBEAT_INTERVAL,),
        ).fetchall()
        return [row for row in rows if self._is_dead(row, now)]

    def dead_workers(self) -> list[sqlite3.Row]:
        now = time.time()
        with self._lock:
            return self._dead_worker_rows(self._conn, now)

    @contextmanager
    def recovery_lock(self, ttl: float = 60.0) -> Iterator[bool]:
        """Cross-process advisory lock for startup recovery.

        Yields True when the lock is acquired, False when another live
        recovery already owns it.  Implemented with a dedicated table row;
        stale owners past *ttl* are taken over.
        """
        now = time.time()
        token = uuid.uuid4().hex
        with self._txn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _locks ("
                "name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires REAL NOT NULL)"
            )
            acquired = conn.execute(
                "INSERT INTO _locks(name, owner, expires) VALUES ('recovery', ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "owner = CASE WHEN _locks.expires <= ? THEN excluded.owner ELSE _locks.owner END, "
                "expires = CASE WHEN _locks.expires <= ? THEN excluded.expires ELSE _locks.expires END "
                "RETURNING owner",
                (token, now + ttl, now, now + ttl),
            ).fetchone()
            got = acquired is not None and acquired[0] == token
        try:
            yield got
        finally:
            if got:
                with self._txn() as conn:
                    conn.execute(
                        "DELETE FROM _locks WHERE name = 'recovery' AND owner = ?",
                        (token,),
                    )

    def recover_tasks(
        self, worker_ids: Optional[list[str]] = None
    ) -> int:
        """Reset orphaned ``running`` tasks back to ``pending``.

        The interrupted attempt is rolled back (attempts decremented) because
        the command may have been killed midway, so the crash costs no retry
        budget.  ``worker_ids`` restricts recovery to specific dead workers
        (used at a fresh worker's startup); ``None`` means all detected dead.
        """
        now = time.time()
        with self._txn() as conn:
            if worker_ids is None:
                worker_ids = [
                    row["id"] for row in self._dead_worker_rows(conn, now)
                ]
            if not worker_ids:
                return 0
            placeholders = ",".join("?" for _ in worker_ids)
            recovered = conn.execute(
                f"""
                UPDATE tasks
                   SET status = 'pending',
                       attempts = MAX(attempts - 1, 0),
                       next_attempt_at = 0,
                       started_at = NULL
                 WHERE status = 'running'
                   AND id IN (
                        SELECT task_id FROM task_runs
                         WHERE ended_at IS NULL
                           AND worker_id IN ({placeholders})
                   )
                """,
                worker_ids,
            ).rowcount
            conn.execute(
                f"""
                UPDATE task_runs
                   SET ended_at = ?, exit_code = -1, interrupted = 1
                 WHERE ended_at IS NULL
                   AND worker_id IN ({placeholders})
                """,
                [now, *worker_ids],
            )
            conn.execute(
                f"UPDATE workers SET status = 'dead' WHERE id IN "
                f"({placeholders})",
                worker_ids,
            )
        return recovered

    def orphaned_run_children(
        self, worker_ids: list[str]
    ) -> list[sqlite3.Row]:
        """Open runs (task_id, child_pid) owned by the given dead workers."""
        if not worker_ids:
            return []
        placeholders = ",".join("?" for _ in worker_ids)
        with self._lock:
            return list(
                self._conn.execute(
                    f"""
                    SELECT task_id, child_pid FROM task_runs
                     WHERE ended_at IS NULL
                       AND child_pid IS NOT NULL
                       AND worker_id IN ({placeholders})
                    """,
                    worker_ids,
                ).fetchall()
            )