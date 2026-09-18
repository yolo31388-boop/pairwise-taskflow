"""持久化 / 重复提交 / 指数退避 / dead 的单元测试。"""

from __future__ import annotations

import time

import pytest

from taskflow.db import (
    Database,
    DuplicateTaskError,
    STATUS_DEAD,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    retry_delay,
)


def test_duplicate_submit_rejected(db_path):
    db = Database(db_path)
    assert db.add_task("t1", "echo hello") is True
    with pytest.raises(DuplicateTaskError):
        db.add_task("t1", "echo other")
    # 原任务不被覆盖/重置
    task = db.get_task("t1")
    assert task["command"] == "echo hello"
    assert task["status"] == STATUS_PENDING
    assert task["attempts"] == 0
    db.close()


def test_claim_is_mutex_across_connections(db_path):
    db1 = Database(db_path)
    db2 = Database(db_path)
    db1.add_task("t1", "echo x")

    a = db1.claim_next("w1", "token-a", 10.0)
    assert a is not None and a["id"] == "t1"
    # 第二个连接不能重复领取
    assert db2.claim_next("w2", "token-b", 10.0) is None
    db1.close()
    db2.close()


def test_fencing_token_blocks_stale_owner(db_path):
    db = Database(db_path)
    t0 = 1000.0
    db.add_task("t1", "echo x", now=t0)
    db.claim_next("w1", "old-token", 10.0, now=t0)
    # 租约过期、被回收并由新持有者接管后，旧 token 再写结果必须失败
    assert db.reap_stale(now=t0 + 11) == ["t1"]
    assert db.claim_next("w2", "new-token", 10.0, now=t0 + 11) is not None
    assert db.mark_succeeded("t1", "old-token", "stale", now=t0 + 12) is False
    assert db.get_task("t1")["status"] == STATUS_RUNNING
    # 新持有者可以正常写
    assert db.mark_succeeded("t1", "new-token", "fresh", now=t0 + 12) is True
    assert db.get_task("t1")["status"] == STATUS_SUCCEEDED
    db.close()


def test_backoff_sequence_and_dead(db_path):
    db = Database(db_path)
    db.add_task("t1", "false")
    base = 0.01
    statuses = []
    for expected_attempt in range(1, 5):  # 4 次执行：首次 + 3 次重试
        task = db.claim_next("w1", "tok-%d" % expected_attempt, 10.0)
        assert task["attempts"] == expected_attempt
        before = time.monotonic()
        status = db.mark_failed("t1", task["lease_token"], "boom", base_delay=base)
        statuses.append(status)
        if expected_attempt < 4:
            assert status == STATUS_FAILED
            row = db.get_task("t1")
            assert row["run_after"] >= before + retry_delay(expected_attempt, base)
            # 未到退避时间不能领取
            assert db.claim_next("w1", "early", 10.0) is None
            time.sleep(retry_delay(expected_attempt, base) + 0.02)
    assert statuses == [STATUS_FAILED, STATUS_FAILED, STATUS_FAILED, STATUS_DEAD]
    assert db.get_task("t1")["status"] == STATUS_DEAD
    # dead 任务不会再被领取
    assert db.claim_next("w1", "tok-x", 10.0) is None
    db.close()


def test_success_flow(db_path):
    db = Database(db_path)
    db.add_task("t1", "echo ok")
    task = db.claim_next("w1", "tok", 10.0)
    assert task["status"] == STATUS_RUNNING and task["attempts"] == 1
    assert db.mark_succeeded("t1", "tok", "ok\n") is True
    final = db.get_task("t1")
    assert final["status"] == STATUS_SUCCEEDED
    assert final["finished_at"] is not None
    assert db.claim_next("w1", "tok2", 10.0) is None
    db.close()


def test_retry_delay_formula():
    assert retry_delay(1) == 1
    assert retry_delay(2) == 2
    assert retry_delay(3) == 4
    assert retry_delay(2, 0.5) == 1.0
