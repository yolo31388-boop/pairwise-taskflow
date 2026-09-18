"""Acceptance scenario 1: 100 flaky tasks on 3 workers all reach a final state."""

from __future__ import annotations

from tests.conftest import wait_for


def test_duplicate_submit_via_cli(env):
    r = env.submit("dup", "quick")
    assert r.returncode == 0, r.stderr
    r = env.submit("dup", "quick")
    assert r.returncode == 1
    assert "already exists" in r.stderr

    # Only one row, untouched.
    r = env.run_cli("status", "dup")
    assert r.returncode == 0
    assert "pending" in r.stdout


def test_100_flaky_tasks_drain_with_3_workers(env):
    n = 100
    for i in range(n):
        r = env.submit(f"job-{i:03d}", "flaky")
        assert r.returncode == 0, r.stderr

    env.start_worker(workers=3)
    counts = env.wait_drained(timeout=180)

    assert counts.get("pending", 0) == 0
    assert counts.get("running", 0) == 0
    assert counts.get("failed", 0) == 0
    assert counts.get("succeeded", 0) + counts.get("dead", 0) == n

    # Every task really executed at least once.
    lines = env.timestamps()
    assert sum("FLAKY-START" in line for line in lines) >= n

    # The exclusive-marker guard never fired => no concurrent double runs.
    assert not any("OVERLAP-DETECTED" in line for line in lines)

    # list output reports a sane summary.
    listing = env.run_cli("list").stdout
    assert "100 task(s)" in listing


def test_two_worker_processes_share_queue_safely(env):
    n = 20
    for i in range(n):
        env.submit(f"sh-{i:02d}", "flaky")
    env.start_worker(workers=2)
    env.start_worker(workers=2)
    counts = env.wait_drained(timeout=120)
    assert sum(v for k, v in counts.items() if k in ("succeeded", "dead")) == n
    assert not any("OVERLAP-DETECTED" in line for line in env.timestamps())