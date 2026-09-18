"""Acceptance scenario 2: kill -9 mid-run, restart, running task resumes."""

from __future__ import annotations

import sqlite3
import time

from tests.conftest import wait_for


def _running_task_ids(db_path):
    con = sqlite3.connect(db_path)
    try:
        return {
            row[0]
            for row in con.execute(
                "SELECT id FROM tasks WHERE status = 'running'"
            ).fetchall()
        }
    finally:
        con.close()


def test_hard_kill_resumes_running_task(env):
    env.submit("crashed", "bomb")
    worker = env.start_worker(workers=1, bomb_seconds=120)

    # Wait until the bomb task has actually started executing.
    wait_for(
        lambda: any("BOMB-START" in line for line in env.timestamps()),
        timeout=20,
    )
    assert _running_task_ids(env.db) == {"crashed"}

    # kill -9: no cleanup whatsoever.
    worker.hard_kill()

    # The DB still says running (durability: nothing lost).
    assert _running_task_ids(env.db) == {"crashed"}

    # Restart: orphaned child is killed and the task is re-run.  The resumed
    # attempt finishes quickly thanks to the per-task shorten marker.
    (env.helper_dir / "shorten-crashed").touch()
    env.start_worker(workers=1)

    con = sqlite3.connect(env.db)
    try:
        deadline = time.time() + 90
        while time.time() < deadline:
            status = con.execute(
                "SELECT status FROM tasks WHERE id = 'crashed'"
            ).fetchone()[0]
            assert status != "dead"
            if status == "succeeded":
                break
            time.sleep(0.3)
        else:
            raise AssertionError("task did not reach succeeded after restart")

        # First attempt is recorded as interrupted; second completed.
        runs = con.execute(
            "SELECT exit_code, interrupted FROM task_runs "
            "WHERE task_id = 'crashed' ORDER BY started_at"
        ).fetchall()
    finally:
        con.close()

    assert runs[0] == (-1, 1)
    assert runs[-1][0] == 0
    assert runs[-1][1] == 0
    assert len(runs) >= 2

    # No concurrent execution ever happened (exclusive marker + logs).
    assert not any("OVERLAP-DETECTED" in line for line in env.timestamps())


def test_pending_tasks_survive_hard_kill(env):
    # One worker slot, two tasks: second stays pending while first is killed.
    env.submit("first", "bomb")
    env.submit("waiting", "quick")
    worker = env.start_worker(workers=1, bomb_seconds=120)
    wait_for(
        lambda: any("BOMB-START" in line for line in env.timestamps()),
        timeout=20,
    )
    worker.hard_kill()
    (env.helper_dir / "shorten-first").touch()

    env.start_worker(workers=1)
    counts = env.wait_drained(timeout=120)
    assert counts.get("succeeded", 0) == 2