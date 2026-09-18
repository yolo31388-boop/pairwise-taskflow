"""taskflow 命令行入口。

子命令::

    taskflow submit  <任务id> <命令>   提交任务（重复 id 报错，不覆盖）
    taskflow worker  [--workers N]    启动 worker 消费任务
    taskflow status [任务id]          查看任务状态/全部状态
    taskflow list                     列出所有任务

数据库路径优先级：--db 参数 > TASKFLOW_DB 环境变量 > ./taskflow.db
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from . import __version__, db as dbmod
from .db import Database
from .worker import Worker, install_signal_handlers

DEFAULT_DB = "taskflow.db"


def resolve_db(args: argparse.Namespace) -> str:
    return getattr(args, "db", None) or os.environ.get("TASKFLOW_DB") or DEFAULT_DB


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def cmd_submit(args: argparse.Namespace) -> int:
    db = Database(resolve_db(args))
    try:
        db.add_task(args.task_id, args.command)
    except dbmod.DuplicateTaskError as exc:
        print("提交失败：%s" % exc, file=sys.stderr)
        return 2
    finally:
        db.close()
    print("已提交任务 %s" % args.task_id)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    db = Database(resolve_db(args))
    try:
        if args.task_id:
            task = db.get_task(args.task_id)
            if task is None:
                print("没有这个任务：%s" % args.task_id, file=sys.stderr)
                return 1
            _print_task(task)
            return 0
        counts = db.counts()
    finally:
        db.close()
    total = sum(counts.values())
    print("任务总数: %d" % total)
    for name in ("pending", "running", "succeeded", "failed", "dead"):
        print("  %-9s %d" % (name, counts[name]))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    db = Database(resolve_db(args))
    try:
        tasks = db.list_tasks()
    finally:
        db.close()
    if not tasks:
        print("（暂无任务）")
        return 0
    for task in tasks:
        print("%-24s %-9s attempts=%d worker=%s" % (
            task["id"],
            task["status"],
            task["attempts"],
            task["worker_id"] or "-",
        ))
    return 0


def _print_task(task) -> None:
    print("id:       %s" % task["id"])
    print("command:  %s" % task["command"])
    print("status:   %s" % task["status"])
    print("attempts: %d" % task["attempts"])
    print("worker:   %s" % (task["worker_id"] or "-"))
    if task["result"]:
        print("result:   %s" % task["result"].strip()[:500])


def cmd_worker(args: argparse.Namespace) -> int:
    setup_logging(args.verbose)
    db = Database(resolve_db(args))
    worker = Worker(
        db,
        workers=args.workers,
        lease_seconds=args.lease_seconds,
        heartbeat_interval=args.heartbeat_interval,
        poll_interval=args.poll_interval,
        reap_interval=args.reap_interval,
        backoff_base=args.backoff_base,
    )
    install_signal_handlers(worker)

    async def _main() -> int:
        return await worker.run()

    try:
        return asyncio.run(_main())
    except KeyboardInterrupt:
        # 极端情况下信号处理器没生效时的兜底
        worker.request_stop()
        return 0
    finally:
        db.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taskflow",
        description="基于 SQLite + asyncio 的轻量并发任务队列（仅标准库）",
    )
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_db(p: argparse.ArgumentParser) -> None:
        p.add_argument("--db", help="SQLite 路径（默认 taskflow.db，也可用 TASKFLOW_DB）")

    p_submit = sub.add_parser("submit", help="提交一个任务")
    p_submit.add_argument("task_id", help="任务 id（重复提交会报错）")
    p_submit.add_argument("command", help="要执行的 shell 命令")
    add_db(p_submit)
    p_submit.set_defaults(func=cmd_submit)

    p_worker = sub.add_parser("worker", help="启动 worker 持续消费任务")
    p_worker.add_argument("--workers", type=int, default=4, help="并发槽位数，默认 4")
    p_worker.add_argument("--lease-seconds", dest="lease_seconds", type=float,
                          default=float(os.environ.get("TASKFLOW_LEASE_SECONDS", "10")),
                          help="任务租约秒数，默认 10")
    p_worker.add_argument("--heartbeat-interval", dest="heartbeat_interval",
                          type=float,
                          default=float(os.environ.get("TASKFLOW_HEARTBEAT_INTERVAL", "3")),
                          help="心跳续租间隔秒数，默认 3")
    p_worker.add_argument("--poll-interval", dest="poll_interval", type=float,
                          default=float(os.environ.get("TASKFLOW_POLL_INTERVAL", "0.2")),
                          help="空队列轮询间隔秒数，默认 0.2")
    p_worker.add_argument("--reap-interval", dest="reap_interval", type=float,
                          default=float(os.environ.get("TASKFLOW_REAP_INTERVAL", "2")),
                          help="过期任务扫描间隔秒数，默认 2")
    p_worker.add_argument("--backoff-base", dest="backoff_base", type=float,
                          default=float(os.environ.get("TASKFLOW_BACKOFF_BASE", "1")),
                          help="重试退避基数秒数，默认 1（1/2/4 秒）")
    p_worker.add_argument("-v", "--verbose", action="store_true", help="调试日志")
    add_db(p_worker)
    p_worker.set_defaults(func=cmd_worker)

    p_status = sub.add_parser("status", help="查看状态计数或单个任务详情")
    p_status.add_argument("task_id", nargs="?", help="不填则显示各状态计数")
    add_db(p_status)
    p_status.set_defaults(func=cmd_status)

    p_list = sub.add_parser("list", help="列出全部任务")
    add_db(p_list)
    p_list.set_defaults(func=cmd_list)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
