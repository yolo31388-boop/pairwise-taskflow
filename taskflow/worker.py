"""The async worker engine: N concurrent slots sharing one SQLite queue."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
import uuid

from . import config
from .db import Database
from .processes import kill_process_tree, spawn_shell

log = logging.getLogger("taskflow")


class TaskRunner:
    """Runs claimed tasks with a fixed-size asyncio worker pool."""

    def __init__(self, db_path: str | None = None, concurrency: int = 4) -> None:
        self.db = Database(db_path)
        self.concurrency = concurrency
        self.worker_id = uuid.uuid4().hex
        self.shutdown_event = asyncio.Event()
        self.active = 0

    # ------------------------------------------------------------------ setup

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        def request_shutdown(*_: object) -> None:
            # Signal handlers may run on any thread on Windows; marshal the
            # event set back onto the loop thread safely.
            if not self.shutdown_event.is_set():
                log.warning(
                    "shutdown signal received: draining in-flight tasks"
                )
            loop.call_soon_threadsafe(self.shutdown_event.set)

        sigs = (
            (signal.SIGBREAK, signal.SIGINT)
            if sys.platform == "win32"
            else (signal.SIGTERM, signal.SIGINT)
        )
        for sig in sigs:
            try:
                signal.signal(sig, request_shutdown)
            except (OSError, ValueError, AttributeError):
                # Secondary best effort where supported.
                try:
                    loop.add_signal_handler(sig, request_shutdown)
                except (NotImplementedError, AttributeError, ValueError):
                    pass

    def startup_recover(self) -> None:
        """Reclaim tasks of a previously crashed worker process.

        Runs before any task is claimed.  Leftover child process trees are
        killed first (the OS job object / pdeathsig normally already did
        this), then their tasks are atomically reset to pending -- a task
        can therefore never execute twice at once.
        """
        self.db.register_worker(self.worker_id, os.getpid())
        # Serialise startup recovery across concurrently launched workers.
        with self.db.recovery_lock() as acquired:
            if not acquired:
                return
            dead = self._wait_for_dead_workers()
            if not dead:
                return
            ids = [row["id"] for row in dead]
            for run in self.db.orphaned_run_children(ids):
                kill_process_tree(run["child_pid"])
            recovered = self.db.recover_tasks(ids)
            if recovered:
                log.warning(
                    "recovered %d task(s) left running by crashed worker(s)",
                    recovered,
                )

    def _wait_for_dead_workers(self, timeout: float = 5.0):
        """Return dead workers, briefly waiting for a just-killed pid to
        disappear (process teardown is not synchronous with the kill)."""
        deadline = time.time() + timeout
        while True:
            dead = self.db.dead_workers()
            if dead:
                return dead
            with self.db._lock:
                rows = self.db._conn.execute(
                    "SELECT id, pid FROM workers "
                    "WHERE status = 'active' AND id != ?",
                    (self.worker_id,),
                ).fetchall()
            if not rows or time.time() >= deadline:
                return []
            time.sleep(0.25)

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)
        await loop.run_in_executor(None, self.startup_recover)
        log.info(
            "worker %s started (pid %d, %d concurrent slots)",
            self.worker_id[:8],
            os.getpid(),
            self.concurrency,
        )
        try:
            pool = [
                asyncio.create_task(self._worker_loop(slot))
                for slot in range(self.concurrency)
            ]
            heartbeat = asyncio.create_task(self._heartbeat_loop())
            reaper = asyncio.create_task(self._reaper_loop())
            await asyncio.gather(*pool)
        finally:
            heartbeat.cancel()
            reaper.cancel()
            for task in (heartbeat, reaper):
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await loop.run_in_executor(None, self.db.mark_worker_shutdown, self.worker_id)
            self.db.close()
        log.info("all in-flight tasks finished, exiting cleanly")
        return 0

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(config.HEARTBEAT_INTERVAL)
            await asyncio.get_running_loop().run_in_executor(
                None, self.db.heartbeat, self.worker_id
            )

    async def _reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(config.REAP_INTERVAL)
            if self.shutdown_event.is_set():
                continue
            await asyncio.get_running_loop().run_in_executor(None, self._reap)

    def _reap(self) -> None:
        dead = self.db.dead_workers()
        if not dead:
            return
        ids = [row["id"] for row in dead]
        for run in self.db.orphaned_run_children(ids):
            kill_process_tree(run["child_pid"])
        recovered = self.db.recover_tasks(ids)
        if recovered:
            log.warning("reaper recovered %d orphaned task(s)", recovered)

    # ------------------------------------------------------------------ slots

    async def _worker_loop(self, slot: int) -> None:
        loop = asyncio.get_running_loop()
        while True:
            if self.shutdown_event.is_set():
                return
            task = await loop.run_in_executor(
                None, self.db.claim_task, self.worker_id
            )
            if task is None:
                if self.shutdown_event.is_set():
                    return
                try:
                    await asyncio.wait_for(
                        self.shutdown_event.wait(),
                        timeout=config.CLAIM_POLL_INTERVAL,
                    )
                    return
                except asyncio.TimeoutError:
                    continue
            self.active += 1
            try:
                await self._execute(task, loop)
            finally:
                self.active -= 1

    async def _execute(self, task, loop) -> None:
        task_id = task["id"]
        attempt = task["attempts"]
        log.info("start  %s (attempt %d)", task_id, attempt)
        env = {**os.environ, "TASKFLOW_TASK_ID": task_id}

        def _popen() -> tuple:
            proc = spawn_shell(task["command"], env)
            self.db.set_run_child(task_id, self.worker_id, proc.pid)
            return proc

        proc = await loop.run_in_executor(None, _popen)
        try:
            rc = await loop.run_in_executor(None, proc.wait)
        except asyncio.CancelledError:
            # Pool tasks are never cancelled on shutdown, but stay defensive.
            kill_process_tree(proc.pid)
            raise
        # stdout/stderr inherit the worker's fds, so nothing to capture.
        output = ""
        new_status = await loop.run_in_executor(
            None, self.db.finish_task, task_id, rc, output
        )
        if new_status == "succeeded":
            log.info("done   %s (exit 0)", task_id)
        elif new_status == "failed":
            backoff = (
                config.RETRY_BACKOFF_BASE
                * config.RETRY_BACKOFF_FACTOR ** (attempt - 1)
            )
            log.warning(
                "fail   %s (exit %d, attempt %d) retry in %ds",
                task_id,
                rc,
                attempt,
                backoff,
            )
        else:
            log.error(
                "dead   %s (exit %d after %d attempts)",
                task_id,
                rc,
                attempt,
            )