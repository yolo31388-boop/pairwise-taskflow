"""Unit tests for the SQLite layer (claim atomicity, retries, recovery)."""

from __future__ import annotations

import os
import time

import pytest

from taskflow.db import (
    Database,
    DuplicateTaskError,
    TaskNotFoundError,
)


@pytest.fixture
def db(tmp_path):
    database = Database(str(tmp_path / "t.db"))
    yield database
    database.close()


def test_duplicate_submit_rejected(db):
    db.add_task("a", "echo hi")
    with pytest.raises(DuplicateTaskError):
        db.add_task("a", "echo other")
    # Original command is untouched.
    assert db.get_task("a")["command"] == "echo hi"
    assert db.get_task("a")["attempts"] == 0
    assert db.get_task("a")["status"] == "pending"


def test_missing_task_lookup(db):
    with pytest.raises(TaskNotFoundError):
        db.get_task("nope")


def test_claim_is_atomic_and_exclusive(db):
    for i in range(5):
        db.add_task(f"t{i}", "true")
    claimed = set()
    for worker in ("w1", "w2", "w3"):
        for _ in range(5):
            row = db.claim_task(worker)
            if row is not None:
                claimed.add(row["id"])
                assert row["status"] == "running"
                assert row["attempts"] == 1
    assert claimed == {f"t{i}" for i in range(5)}
    assert db.claim_task("w1") is None


def test_succeeded_path(db):
    db.add_task("ok", "true")
    task = db.claim_task("w1")
    assert task["id"] == "ok"
    assert db.finish_task("ok", 0, "") == "succeeded"
    row = db.get_task("ok")
    assert row["status"] == "succeeded"
    assert row["finished_at"]


def test_retry_then_dead(db, monkeypatch):
    from taskflow import config

    monkeypatch.setattr(config, "MAX_ATTEMPTS", 4)
    monkeypatch.setattr(config, "RETRY_BACKOFF_BASE", 0)
    db.add_task("bad", "false")
    statuses = []
    for expected_attempt in range(1, 5):
        task = db.claim_task("w1")
        assert task["attempts"] == expected_attempt
        statuses.append(db.finish_task("bad", 1, "boom"))
        if expected_attempt < 4:
            # Waiting on backoff (0s here so claimable, but status failed).
            assert statuses[-1] == "failed"
            task = db.get_task("bad")
            assert task["next_attempt_at"] <= time.time()
    assert statuses == ["failed", "failed", "failed", "dead"]
    assert db.get_task("bad")["status"] == "dead"


def test_exponential_backoff_values(db, monkeypatch):
    from taskflow import config

    monkeypatch.setattr(config, "MAX_ATTEMPTS", 4)
    monkeypatch.setattr(config, "RETRY_BACKOFF_BASE", 1)
    monkeypatch.setattr(config, "RETRY_BACKOFF_FACTOR", 2)
    db.add_task("bad", "false")
    schedules = []
    for attempt in range(1, 4):
        before = time.time()
        claimed = db.claim_task("w1")
        assert claimed["attempts"] == attempt
        db.finish_task("bad", 1, "x")
        after = time.time()
        target = db.get_task("bad")["next_attempt_at"]
        schedules.append((target - before, target - after))
        if attempt < 3:
            time.sleep(2 ** attempt + 0.05)  # wait out the 1s/2s backoff
    # Delays are 1s, 2s, 4s; bounds allow scheduling/measurement slack.
    wants = (1, 2, 4)
    for (lo, hi), want in zip(schedules, wants):
        assert want - 0.1 <= lo
        assert hi <= want + 0.1


def test_crashed_worker_tasks_are_recovered(db):
    # A worker that does not exist (dead pid, stale heartbeat).
    dead_pid = _find_dead_pid()
    db.register_worker("ghost", dead_pid)
    db._conn.execute(
        "UPDATE workers SET last_heartbeat = ? WHERE id = 'ghost'",
        (time.time() - 100,),
    )
    db.add_task("z", "sleep 5")
    task = db.claim_task("ghost")
    assert task["status"] == "running"

    dead = db.dead_workers()
    assert [row["id"] for row in dead] == ["ghost"]
    recovered = db.recover_tasks()
    assert recovered == 1
    row = db.get_task("z")
    assert row["status"] == "pending"
    assert row["attempts"] == 0

    again = db.claim_task("live")
    assert again["id"] == "z"
    assert again["attempts"] == 1


def test_recover_specific_worker_bypasses_scan(db):
    # Startup path names the dead worker explicitly.
    db.register_worker("old", os.getpid())
    db.add_task("z", "sleep 5")
    db.claim_task("old")
    assert db.recover_tasks(["old"]) == 1
    assert db.get_task("z")["status"] == "pending"


def test_live_worker_tasks_are_not_reaped(db):
    db.register_worker("me", os.getpid())
    db.heartbeat("me")
    db.add_task("live", "true")
    db.claim_task("me")
    assert db.dead_workers() == []
    assert db.recover_tasks() == 0
    assert db.get_task("live")["status"] == "running"


def _find_dead_pid() -> int:
    from taskflow.db import pid_alive

    for pid in range(900000, 901000):
        if not pid_alive(pid):
            return pid
    return 999999
