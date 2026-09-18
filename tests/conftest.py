"""Shared pytest fixtures: temp DB env, CLI runner, spawned worker helper."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
IS_WINDOWS = sys.platform == "win32"
HELPER = REPO_ROOT / "tests" / "helper_script.py"


class WorkerProc:
    def __init__(
        self,
        db_path: Path,
        log_path: Path,
        workers: int,
        extra_env: dict | None = None,
    ):
        env = {
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""),
            "TASKFLOW_DB": str(db_path),
        }
        if extra_env:
            env.update(extra_env)
        kwargs: dict = {"env": env, "cwd": str(REPO_ROOT)}
        if IS_WINDOWS:
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True
        self.log = open(log_path, "wb")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "taskflow", "worker", "--workers", str(workers)],
            stdout=self.log,
            stderr=subprocess.STDOUT,
            **kwargs,
        )

    @property
    def pid(self) -> int:
        return self.proc.pid

    def log_text(self) -> str:
        self.log.flush()
        return Path(self.log.name).read_text(errors="ignore")

    def hard_kill(self) -> None:
        """Kill with no chance to clean up (kill -9 semantics).

        On Windows we kill only the worker pid.  Task subprocess trees are
        bound to a kill-on-close Job Object inside the worker, so the OS
        removes them atomically when the worker dies.
        """
        if IS_WINDOWS:
            subprocess.run(
                ["taskkill", "/F", "/PID", str(self.proc.pid)],
                capture_output=True,
            )
            try:
                os.kill(self.proc.pid, signal.SIGTERM)
            except OSError:
                pass
        else:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        self.close_log()

    def graceful_stop(self) -> int:
        """SIGTERM / CTRL_BREAK, wait for clean exit; returns exit code."""
        if IS_WINDOWS:
            try:
                os.kill(self.proc.pid, signal.CTRL_BREAK_EVENT)
            except (AttributeError, OSError):
                self.hard_kill()
                pytest.skip("CTRL_BREAK_EVENT delivery not supported")
        else:
            os.kill(self.proc.pid, signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.hard_kill()
            raise AssertionError("worker did not exit after graceful signal")
        self.close_log()
        return code

    def close_log(self) -> None:
        try:
            self.log.close()
        except OSError:
            pass


class Env:
    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.db = tmp_path / "queue.db"
        self.helper_dir = tmp_path / "work"
        self.helper_dir.mkdir()
        self.procs: list[WorkerProc] = []
        self.base_env = {
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""),
            "TASKFLOW_DB": str(self.db),
            "TASKFLOW_HELPER_DIR": str(self.helper_dir),
        }

    def run_cli(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "taskflow", *args],
            env=self.base_env,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )

    def submit(
        self, task_id: str, mode: str, *args: str
    ) -> subprocess.CompletedProcess:
        command = f'"{sys.executable}" "{HELPER}" {mode} {" ".join(args)}'.strip()
        return self.run_cli("submit", task_id, "--", command)

    def start_worker(self, workers: int = 4, bomb_seconds: float | None = 2.0) -> WorkerProc:
        extra = {"TASKFLOW_HELPER_DIR": str(self.helper_dir)}
        if bomb_seconds is not None:
            extra["TASKFLOW_BOMB_SECONDS"] = str(bomb_seconds)
        proc = WorkerProc(
            self.db,
            self.tmp / f"worker-{time.time_ns()}.log",
            workers,
            extra_env=extra,
        )
        self.procs.append(proc)
        deadline = time.time() + 10
        while time.time() < deadline:
            if "started" in proc.log_text():
                return proc
            time.sleep(0.1)
        raise AssertionError("worker failed to start:\n" + proc.log_text())

    def status_counts(self) -> dict[str, int]:
        import sqlite3

        con = sqlite3.connect(self.db)
        try:
            return dict(
                con.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
            )
        finally:
            con.close()

    def wait_drained(self, timeout: float = 120.0) -> dict[str, int]:
        deadline = time.time() + timeout
        counts = {}
        while time.time() < deadline:
            counts = self.status_counts()
            active = (
                counts.get("pending", 0)
                + counts.get("running", 0)
                + counts.get("failed", 0)
            )
            if active == 0 and counts:
                return counts
            time.sleep(0.3)
        raise AssertionError(f"tasks did not drain: {counts}")

    def timestamps(self) -> list[str]:
        path = self.helper_dir / "timestamps.log"
        if not path.exists():
            return []
        return path.read_text().splitlines()

    def cleanup(self) -> None:
        for proc in self.procs:
            if proc.proc.poll() is None:
                proc.hard_kill()


@pytest.fixture
def env(tmp_path: Path):
    e = Env(tmp_path)
    yield e
    e.cleanup()


def wait_for(predicate, timeout: float = 30.0, interval: float = 0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError("condition not met before timeout")