"""Acceptance scenario 3: graceful shutdown drains in-flight tasks, and tasks
submitted while/after shutdown run on the next start."""

from __future__ import annotations

import sys
import time

from tests.conftest import wait_for


def test_sigterm_drains_then_exits_zero(env):
    # More long tasks than slots so some are running at signal time.
    for i in range(4):
        env.submit(f"gt-{i}", "bomb")
    worker = env.start_worker(workers=2)

    wait_for(
        lambda: sum("BOMB-START" in line for line in env.timestamps()) == 2,
        timeout=20,
    )

    # SIGTERM while two tasks are in flight.  Give them time to finish.
    t0 = time.time()
    code = worker.graceful_stop()
    elapsed = time.time() - t0
    assert code == 0
    assert elapsed >= 1.5  # really waited for the running bombs rather than killed

    # Worker marked itself shut down.
    import sqlite3

    con = sqlite3.connect(env.db)
    try:
        states = dict(con.execute("SELECT id, status FROM workers"))
        task_states = dict(
            con.execute("SELECT id, status FROM tasks WHERE id LIKE 'gt-%'")
        )
    finally:
        con.close()
    assert any(s == "shutdown" for s in states.values())
    assert task_states["gt-0"] == "succeeded"
    assert task_states["gt-1"] == "succeeded"
    # Tasks not started before shutdown are accepted (submit works
    # independently of workers) and simply wait for the next worker.
    assert task_states["gt-2"] == "pending"

    # Restart: remaining tasks execute.
    env.start_worker(workers=2)
    counts = env.wait_drained(timeout=180)
    assert counts.get("succeeded", 0) == 4
    assert counts.get("pending", 0) == 0


def test_tasks_submitted_after_sigterm_run_on_restart(env):
    env.submit("before", "quick")
    worker = env.start_worker(workers=1)
    env.wait_drained(timeout=30)

    worker.graceful_stop()

    # Immediately submit 10 more tasks (no live worker).
    for i in range(10):
        r = env.submit(f"after-{i}", "quick")
        assert r.returncode == 0, r.stderr

    env.start_worker(workers=3)
    counts = env.wait_drained(timeout=60)
    assert counts.get("succeeded", 0) == 11
    lines = env.timestamps()
    assert sum("QUICK-RUN" in line for line in lines) == 11