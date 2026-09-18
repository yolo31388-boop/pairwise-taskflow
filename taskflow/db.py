"""SQLite 持久化层。

所有状态都落盘在同一张 tasks 表里，worker 之间不共享内存，完全靠
SQLite 的原子 UPDATE 抢占任务，保证同一个任务不会同时被两个 worker
执行（单条 UPDATE ... WHERE 条件的隐式行锁就是互斥手段）。

任务生命周期::

    pending -> running -> succeeded
                       -> failed (等待指数退避后被重新领取)
                       -> dead   (重试次数用尽)

pending/failed 都表示“等待被领取”，failed 只表示“上次执行失败、正在
退避”。默认最多执行 4 次 = 首次 + 3 次重试，重试间隔为
base_delay * (1s, 2s, 4s)。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_DEAD = "dead"

# 首次执行 + 3 次重试
DEFAULT_MAX_ATTEMPTS = 4


class DuplicateTaskError(Exception):
    """提交了已存在的任务 id。"""


def connect(db_path: str) -> sqlite3.Connection:
    """打开一个配置好 WAL / busy_timeout 的 SQLite 连接。"""
    if db_path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=30.0,
        isolation_level=None,  # autocommit，事务显式控制
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class Database:
    """线程安全的任务存储。

    每个进程一个实例，内部用一把锁串行化写入事务；跨进程互斥由
    SQLite WAL + busy_timeout + 原子单条 UPDATE 保证。方法都是阻塞
    的，asyncio 侧通过 run_in_executor 调用。
    """

    def __init__(self, db_path: str, max_attempts: int = DEFAULT_MAX_ATTEMPTS):
        self.conn = connect(db_path)
        self._lock = threading.RLock()
        self.max_attempts = max_attempts
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id            TEXT PRIMARY KEY,
                    command       TEXT NOT NULL,
                    status        TEXT NOT NULL,
                    attempts      INTEGER NOT NULL DEFAULT 0,
                    run_after     REAL NOT NULL,
                    lease_token   TEXT,
                    lease_expires REAL,
                    worker_id     TEXT,
                    result        TEXT,
                    created_at    REAL NOT NULL,
                    updated_at    REAL NOT NULL,
                    finished_at   REAL
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_dispatch "
                "ON tasks(status, run_after)"
            )

    # ---- 提交 / 查询 ---------------------------------------------------

    def add_task(self, task_id: str, command: str, now: Optional[float] = None) -> bool:
        """提交任务；任务 id 已存在时抛 DuplicateTaskError。"""
        now = time.time() if now is None else now
        with self._lock:
            try:
                self.conn.execute(
                    "INSERT INTO tasks (id, command, status, attempts, "
                    "run_after, created_at, updated_at) "
                    "VALUES (?, ?, ?, 0, ?, ?, ?)",
                    (task_id, command, STATUS_PENDING, now, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DuplicateTaskError(
                    "任务 %r 已存在，不允许重复提交" % task_id
                ) from exc
        return True

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_tasks(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM tasks ORDER BY created_at, id"
            ).fetchall()
        return [dict(r) for r in rows]

    def counts(self) -> Dict[str, int]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"
            ).fetchall()
        result = {
            STATUS_PENDING: 0,
            STATUS_RUNNING: 0,
            STATUS_SUCCEEDED: 0,
            STATUS_FAILED: 0,
            STATUS_DEAD: 0,
        }
        for row in rows:
            result[row["status"]] = row["n"]
        return result

    # ---- 调度 ----------------------------------------------------------

    def claim_next(
        self,
        worker_id: str,
        lease_token: str,
        lease_seconds: float,
        now: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """原子地领取一个到期任务，没有则返回 None。

        先查出一个到期任务，再用单条带条件的 UPDATE 抢占；
        rowcount == 1 才算抢到。多个进程同时进来也只有一个 UPDATE
        能命中。
        """
        now = time.time() if now is None else now
        expires = now + lease_seconds
        with self._lock:
            row = self.conn.execute(
                "SELECT id FROM tasks "
                "WHERE status IN (?, ?) AND run_after <= ? "
                "ORDER BY run_after, id LIMIT 1",
                (STATUS_PENDING, STATUS_FAILED, now),
            ).fetchone()
            if row is None:
                return None
            task_id = row["id"]
            cur = self.conn.execute(
                "UPDATE tasks SET status = ?, attempts = attempts + 1, "
                "lease_token = ?, lease_expires = ?, worker_id = ?, "
                "updated_at = ? "
                "WHERE id = ? AND status IN (?, ?) AND run_after <= ?",
                (
                    STATUS_RUNNING,
                    lease_token,
                    expires,
                    worker_id,
                    now,
                    task_id,
                    STATUS_PENDING,
                    STATUS_FAILED,
                    now,
                ),
            )
            if cur.rowcount != 1:  # 被别的 worker 抢先
                return None
            return self.get_task(task_id)

    def heartbeat(
        self,
        task_id: str,
        lease_token: str,
        lease_seconds: float,
        now: Optional[float] = None,
    ) -> bool:
        """续租。只有当前持有者能续，返回是否仍持有租约。"""
        now = time.time() if now is None else now
        with self._lock:
            cur = self.conn.execute(
                "UPDATE tasks SET lease_expires = ?, updated_at = ? "
                "WHERE id = ? AND lease_token = ? AND status = ?",
                (now + lease_seconds, now, task_id, lease_token, STATUS_RUNNING),
            )
        return cur.rowcount == 1

    def mark_succeeded(
        self,
        task_id: str,
        lease_token: str,
        output: str,
        now: Optional[float] = None,
    ) -> bool:
        now = time.time() if now is None else now
        with self._lock:
            cur = self.conn.execute(
                "UPDATE tasks SET status = ?, result = ?, lease_token = NULL, "
                "lease_expires = NULL, updated_at = ?, finished_at = ? "
                "WHERE id = ? AND lease_token = ? AND status = ?",
                (
                    STATUS_SUCCEEDED,
                    output,
                    now,
                    now,
                    task_id,
                    lease_token,
                    STATUS_RUNNING,
                ),
            )
        return cur.rowcount == 1

    def mark_failed(
        self,
        task_id: str,
        lease_token: str,
        error: str,
        base_delay: float = 1.0,
        now: Optional[float] = None,
    ) -> str:
        """标记一次执行失败并返回新状态。

        重试次数用尽 -> dead；否则 -> failed，run_after 按
        base_delay * 2**(attempts-1) 指数退避（1s、2s、4s ...）。
        """
        now = time.time() if now is None else now
        with self._lock:
            row = self.conn.execute(
                "SELECT attempts FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                return STATUS_DEAD
            attempts = row["attempts"]
            finished_at: Optional[float] = None
            if attempts >= self.max_attempts:
                status = STATUS_DEAD
                run_after = now
                finished_at = now
            else:
                status = STATUS_FAILED
                run_after = now + retry_delay(attempts, base_delay)
            self.conn.execute(
                "UPDATE tasks SET status = ?, result = ?, lease_token = NULL, "
                "lease_expires = NULL, run_after = ?, updated_at = ?, "
                "finished_at = ? WHERE id = ? AND lease_token = ? "
                "AND status = ?",
                (
                    status,
                    error,
                    run_after,
                    now,
                    finished_at,
                    task_id,
                    lease_token,
                    STATUS_RUNNING,
                ),
            )
        return status

    def reap_stale(self, now: Optional[float] = None) -> List[str]:
        """把租约过期的 running 任务（持有者死亡/被 kill -9）退回待执行。

        attempts 不回滚——死掉的那次执行已算一次尝试；任务会被重新
        领取再跑一次（at-least-once 语义）。
        """
        now = time.time() if now is None else now
        with self._lock:
            rows = self.conn.execute(
                "SELECT id FROM tasks WHERE status = ? AND lease_expires < ?",
                (STATUS_RUNNING, now),
            ).fetchall()
            ids = [r["id"] for r in rows]
            if ids:
                self.conn.execute(
                    "UPDATE tasks SET status = ?, run_after = ?, "
                    "lease_token = NULL, lease_expires = NULL, worker_id = NULL, "
                    "updated_at = ? "
                    "WHERE status = ? AND lease_expires < ?",
                    (STATUS_PENDING, now, now, STATUS_RUNNING, now),
                )
        return ids

    def close(self) -> None:
        with self._lock:
            self.conn.close()


def retry_delay(attempts_done: int, base_delay: float = 1.0) -> float:
    """失败后下一次执行的退避秒数：1, 2, 4 ...（attempts_done 从 1 起）。"""
    return base_delay * (2 ** max(0, attempts_done - 1))
