"""pytest 公共辅助：进程内跑 worker、起子进程 worker、等待终态。"""

from __future__ import annotations

import asyncio
import os
import sys
import time

import pytest

from taskflow.cli import main as cli_main
from taskflow.db import Database
from taskflow.worker import Worker


def run_worker(db_path, *, workers=4, lease_seconds=2.0,
               heartbeat_interval=0.3, reap_interval=0.2,
               poll_interval=0.05, backoff_base=0.05,
               until_idle=True, wait_after_idle=0.2, max_seconds=30):
    """进程内同步地跑一个 worker，直到队列空且持续 wait_after_idle 没有新任务。"""

    async def _run():
        db = Database(db_path)
        worker = Worker(
            db,
            workers=workers,
            lease_seconds=lease_seconds,
            heartbeat_interval=heartbeat_interval,
            reap_interval=reap_interval,
            poll_interval=poll_interval,
            backoff_base=backoff_base,
            worker_id="inproc-%d" % os.getpid(),
        )
        task = asyncio.create_task(worker.run())
        deadline = time.monotonic() + max_seconds
        last_busy = time.monotonic()
        idle_reported = False
        while time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            counts = db.counts()
            busy = counts["pending"] + counts["running"] + counts["failed"] > 0
            if busy:
                last_busy = time.monotonic()
                idle_reported = False
            elif until_idle:
                if time.monotonic() - last_busy >= wait_after_idle:
                    idle_reported = True
                    break
        worker.request_stop()
        rc = await task
        db.close()
        return rc, idle_reported

    return asyncio.run(_run())


def wait_terminal(db_path, timeout=60, expected=None):
    """阻塞等待所有任务进入 succeeded/dead（可附带期望数量）。"""
    deadline = time.monotonic() + timeout
    db = Database(db_path)
    try:
        while time.monotonic() < deadline:
            counts = db.counts()
            active = counts["pending"] + counts["running"] + counts["failed"]
            total = sum(counts.values())
            if active == 0 and (expected is None or total == expected):
                return counts
            time.sleep(0.1)
        raise AssertionError("等待任务终态超时: %s" % db.counts())
    finally:
        db.close()


def submit(db_path, task_id, command):
    return cli_main(["submit", "--db", db_path, task_id, command])


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "tasks.db")


@pytest.fixture
def worker_subprocess(tmp_path):
    """返回启动子进程 worker 的工厂，日志写到临时文件便于排错。"""
    import subprocess

    handles = []

    def _start(db_path, *, workers=4, lease_seconds=2.0,
               heartbeat_interval=0.3, reap_interval=0.2,
               poll_interval=0.05, backoff_base=0.05):
        log = open(str(tmp_path / ("worker-%d.log" % len(handles))), "wb")
        env = dict(os.environ)
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        cmd = [
            sys.executable, "-m", "taskflow", "worker",
            "--db", db_path,
            "--workers", str(workers),
            "--lease-seconds", str(lease_seconds),
            "--heartbeat-interval", str(heartbeat_interval),
            "--reap-interval", str(reap_interval),
            "--poll-interval", str(poll_interval),
            "--backoff-base", str(backoff_base),
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        handles.append((proc, log))
        return proc

    yield _start

    for proc, log in handles:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()
        log.close()
