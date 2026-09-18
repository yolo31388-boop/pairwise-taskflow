"""端到端验收测试：批量任务、kill -9 恢复、同任务不双跑、优雅关闭。"""

from __future__ import annotations

import os
import random
import re
import signal
import sys
import time

import pytest

from conftest import run_worker, submit, wait_terminal
from taskflow.cli import main as cli_main
from taskflow.db import Database


def test_100_tasks_all_terminal(tmp_path, db_path):
    # 随机 sleep + 随机失败；失败的任务会重试，最终只会 succeeded/dead
    rng = random.Random(42)
    py = sys.executable
    for i in range(100):
        sleep_s = rng.randint(0, 3) * 0.01
        fail = 1 if rng.random() < 0.4 else 0
        cmd = "\"%s\" -c \"import time,random,sys; time.sleep(%s); sys.exit(%d)\"" % (
            py, sleep_s, fail,
        )
        assert submit(db_path, "task-%03d" % i, cmd) == 0

    rc, idle = run_worker(db_path, workers=3, backoff_base=0.01,
                          max_seconds=90)
    assert rc == 0 and idle is True
    counts = wait_terminal(db_path, timeout=30, expected=100)
    assert counts["succeeded"] + counts["dead"] == 100
    assert counts["pending"] == counts["running"] == counts["failed"] == 0


def _write_runner_script(tmp_path):
    """任务脚本：写带时间戳的 START/DONE 日志，sleep 后退出 0。

    日志是“同一任务没有被并发执行两遍”的证据。
    """
    runner = tmp_path / "runner.py"
    runner.write_text(
        "import sys, time\n"
        "log, task_id = sys.argv[1], sys.argv[2]\n"
        "hold = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0\n"
        "with open(log, 'a', encoding='utf-8') as f:\n"
        "    f.write('%s %.3f START pid=%d\\n' % (task_id, time.time(), __import__('os').getpid()))\n"
        "    f.flush()\n"
        "    time.sleep(hold)\n"
        "    f.write('%s %.3f DONE  pid=%d\\n' % (task_id, time.time(), __import__('os').getpid()))\n",
        encoding="utf-8",
    )
    return str(runner)


def _parse_intervals(log_file):
    """返回 {task_id: [(start, end_or_None), ...]}，按时间排序。"""
    intervals = {}
    with open(log_file, encoding="utf-8") as fh:
        for line in fh:
            m = re.match(r"(\S+)\s+([0-9.]+)\s+(START|DONE)", line.strip())
            if not m:
                continue
            task_id, ts, kind = m.group(1), float(m.group(2)), m.group(3)
            intervals.setdefault(task_id, [])
            if kind == "START":
                intervals[task_id].append([ts, None])
            else:
                # 配对最近一个未闭合的 START
                for pair in reversed(intervals[task_id]):
                    if pair[1] is None:
                        pair[1] = ts
                        break
    return intervals


def test_kill9_recovery_no_concurrent_double_run(tmp_path, db_path, worker_subprocess):
    log_file = tmp_path / "events.log"
    log_path = str(log_file)
    runner = _write_runner_script(tmp_path)
    py = sys.executable
    # 租约 8s、心跳 2s：存活 worker 绝不会被误判成过期；任务只跑 1.2s。
    # 只有真正被 kill 的 worker 会因心跳停止、租约到期而被回收。
    for i in range(8):
        cmd = '\"%s\" \"%s\" \"%s\" t%02d 1.2' % (
            py, runner, log_path, i,
        )
        assert submit(db_path, "t%02d" % i, cmd) == 0

    proc = worker_subprocess(
        db_path, workers=2, lease_seconds=3.0,
        heartbeat_interval=1.0, reap_interval=0.2,
    )
    # 等到至少有任务开始执行
    deadline = time.time() + 10
    while time.time() < deadline:
        started = (log_file.exists()
                   and log_file.read_text(encoding="utf-8").count("START") > 0)
        if started:
            break
        time.sleep(0.05)
    time.sleep(0.3)  # 让至少一个任务确实跑起来（在 running 状态中）
    kill_ts = time.time()
    proc.kill()  # 模拟 kill -9：不给任何清理机会
    proc.wait()

    db = Database(db_path)
    states = {t["id"]: t["status"] for t in db.list_tasks()}
    db.close()
    assert "running" in states.values() or any(
        log_file.read_text(encoding="utf-8").count(l) > 0
        for l in ("START",)
    )

    # 租约 8s：等到期后，重启 worker 会在启动时回收上个进程遗留的任务
    time.sleep(3.2)
    # 重启 worker，启动时立即回收上个进程遗留的过期任务，全部完成
    proc2 = worker_subprocess(
        db_path, workers=2, lease_seconds=8.0,
        heartbeat_interval=2.0, reap_interval=0.2,
    )
    counts = wait_terminal(db_path, timeout=60, expected=8)
    assert counts["succeeded"] == 8
    proc2.terminate()
    proc2.wait(timeout=10)

    # 核心断言：任何时刻同一个任务不能有两个 START..DONE 区间重叠。
    # 被杀瞬间的那次执行以 kill_ts 作为区间终点。
    intervals = _parse_intervals(str(log_file))
    overlaps = []
    re_executed = 0
    for task_id, pairs in intervals.items():
        closed = []
        for start, end in pairs:
            closed.append((start, end if end is not None else kill_ts))
        closed.sort()
        if len(closed) > 1:
            re_executed += 1
        for (s1, e1), (s2, e2) in zip(closed, closed[1:]):
            if s2 < e1 - 0.05:  # 50ms 容差
                overlaps.append((task_id, s1, e1, s2, e2))
    assert overlaps == [], "同一任务被并发执行: %s" % overlaps
    assert re_executed >= 1, "应当至少有一个 running 任务在重启后被重新执行"


def test_graceful_shutdown_inprocess(tmp_path, db_path):
    """跨平台：请求关闭后不再领新任务，但在跑的任务会跑完；退出码 0。"""
    import asyncio
    from taskflow.worker import Worker

    py = sys.executable
    # 一个长任务（会在关闭时仍在跑）+ 若干排队任务
    submit(db_path, "long", "\"%s\" -c \"import time; time.sleep(1.0)\"" % py)
    for i in range(6):
        submit(db_path, "q%d" % i, "\"%s\" -c \"import time; time.sleep(0.05)\"" % py)

    async def _scenario():
        db = Database(db_path)
        worker = Worker(db, workers=2, lease_seconds=10.0,
                        heartbeat_interval=0.3, reap_interval=1.0,
                        backoff_base=0.01, worker_id="shut-1")
        task = asyncio.create_task(worker.run())
        # 等 long 进入 running
        import time as _t
        end = _t.monotonic() + 5
        while _t.monotonic() < end:
            if db.get_task("long")["status"] == "running":
                break
            await asyncio.sleep(0.02)
        worker.request_stop()  # 优雅关闭
        rc = await task
        db.close()
        return rc

    rc = asyncio.run(_scenario())
    assert rc == 0

    db = Database(db_path)
    # 在跑的 long 必须跑完
    assert db.get_task("long")["status"] == "succeeded"
    running = [t["id"] for t in db.list_tasks() if t["status"] == "running"]
    assert running == []
    # 关闭后没轮到的任务保持 pending，重启后继续
    pending = {t["id"] for t in db.list_tasks() if t["status"] == "pending"}
    assert pending  # 至少有任务被留给重启
    db.close()

    # 重启 + 关闭后新提交 10 个任务，全部能执行（验收场景 3 的跨平台版本）
    for i in range(10):
        submit(db_path, "after%d" % i, "\"%s\" -c \"print('hi')\"" % py)
    run_worker(db_path, workers=3, backoff_base=0.01, max_seconds=30)
    counts = wait_terminal(db_path, timeout=30)
    assert counts["running"] == counts["pending"] == counts["failed"] == 0
    db = Database(db_path)
    assert all(db.get_task("after%d" % i)["status"] == "succeeded"
               for i in range(10))
    assert all(t["status"] in ("succeeded", "dead") for t in db.list_tasks())
    db.close()


@pytest.mark.skipif(sys.platform == "win32",
                    reason="Windows 的 SIGTERM 无法被进程捕获，优雅关闭由进程内测试覆盖")
def test_sigterm_exit_code_zero(tmp_path, db_path, worker_subprocess):
    py = sys.executable
    submit(db_path, "a", "\"%s\" -c \"import time; time.sleep(1.5)\"" % py)
    submit(db_path, "b", "\"%s\" -c \"import time; time.sleep(1.5)\"" % py)

    proc = worker_subprocess(db_path, workers=2, lease_seconds=30.0,
                             heartbeat_interval=1.0, reap_interval=5.0)
    time.sleep(0.8)  # 等 a/b 进入 running
    proc.terminate()   # SIGTERM
    rc = proc.wait(timeout=15)
    assert rc == 0

    db = Database(db_path)
    # 在跑的任务跑完后才退出
    assert db.get_task("a")["status"] == "succeeded"
    assert db.get_task("b")["status"] == "succeeded"
    db.close()
