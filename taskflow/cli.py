"""Command line interface for taskflow.

Usage::

    taskflow submit <task_id> <command...>
    taskflow worker [--workers N] [--db PATH]
    taskflow status <task_id>
    taskflow list
    taskflow runs <task_id>

The database location defaults to ./taskflow.db and can be overridden with
--db on every subcommand or the TASKFLOW_DB environment variable.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime

from . import config
from .db import Database, DuplicateTaskError, TaskNotFoundError
from .worker import TaskRunner


def _fmt_ts(value: float | None) -> str:
    if not value:
        return "-"
    return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taskflow",
        description="A small concurrent shell-command task queue on SQLite.",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite database path (default: ./taskflow.db or $TASKFLOW_DB)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_submit = sub.add_parser("submit", help="submit a task")
    p_submit.add_argument("task_id", help="unique task id")
    p_submit.add_argument(
        "cmd",
        nargs=argparse.REMAINDER,
        help="shell command (use -- before it if it starts with '-')",
    )

    p_worker = sub.add_parser("worker", help="run a worker process")
    p_worker.add_argument(
        "--workers",
        type=int,
        default=config.DEFAULT_WORKERS,
        help=f"number of concurrent slots (default {config.DEFAULT_WORKERS})",
    )

    p_status = sub.add_parser("status", help="show one task")
    p_status.add_argument("task_id")

    sub.add_parser("list", help="list all tasks")

    p_runs = sub.add_parser("runs", help="show attempt history of a task")
    p_runs.add_argument("task_id")

    return parser


def resolve_db(args: argparse.Namespace) -> str:
    if args.db:
        return args.db
    return config.DB_PATH


# ----------------------------------------------------------------- subcommands


def cmd_submit(args: argparse.Namespace) -> int:
    parts = list(args.cmd)
    if parts and parts[0] == "--":
        parts = parts[1:]
    if not parts:
        print("taskflow submit: a shell command is required", file=sys.stderr)
        return 2
    command = " ".join(parts)
    db = Database(resolve_db(args))
    try:
        db.add_task(args.task_id, command)
    except DuplicateTaskError as exc:
        print(f"taskflow: {exc}", file=sys.stderr)
        return 1
    finally:
        db.close()
    print(f"submitted {args.task_id}: {command}")
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    if args.workers < 1:
        print("taskflow worker: --workers must be >= 1", file=sys.stderr)
        return 2
    runner = TaskRunner(resolve_db(args), concurrency=args.workers)
    try:
        return asyncio.run(runner.run())
    except KeyboardInterrupt:
        # SIGINT is handled inside the loop; this only covers the tiny
        # window before the loop starts.
        return 0


def cmd_status(args: argparse.Namespace) -> int:
    db = Database(resolve_db(args))
    try:
        task = db.get_task(args.task_id)
    except TaskNotFoundError as exc:
        print(f"taskflow: {exc}", file=sys.stderr)
        return 1
    finally:
        db.close()
    print(f"id:       {task['id']}")
    print(f"status:   {task['status']}")
    print(f"command:  {task['command']}")
    print(f"attempts: {task['attempts']}")
    print(f"created:  {_fmt_ts(task['created_at'])}")
    print(f"started:  {_fmt_ts(task['started_at'])}")
    print(f"finished: {_fmt_ts(task['finished_at'])}")
    if task["last_error"]:
        print(f"error:    {task['last_error']}")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    db = Database(resolve_db(args))
    try:
        tasks = db.list_tasks()
        counts = db.counts_by_status()
    finally:
        db.close()
    if not tasks:
        print("(no tasks)")
        return 0
    summary = ", ".join(
        f"{name}={counts.get(name, 0)}"
        for name in ("pending", "running", "failed", "succeeded", "dead")
        if counts.get(name, 0)
    )
    print(f"{len(tasks)} task(s): {summary}")
    print(f"{'ID':<24}{'STATUS':<11}{'ATTEMPTS':<10}COMMAND")
    for task in tasks:
        print(
            f"{task['id']:<24}{task['status']:<11}"
            f"{task['attempts']:<10}{task['command']}"
        )
    return 0


def cmd_runs(args: argparse.Namespace) -> int:
    db = Database(resolve_db(args))
    try:
        runs = db.list_runs(args.task_id)
    except TaskNotFoundError as exc:
        print(f"taskflow: {exc}", file=sys.stderr)
        return 1
    finally:
        db.close()
    if not runs:
        print("(no attempts recorded)")
        return 0
    for run in runs:
        flag = " INTERRUPTED" if run["interrupted"] else ""
        print(
            f"{_fmt_ts(run['started_at'])} -> {_fmt_ts(run['ended_at'])} "
            f"exit={run['exit_code']} worker={run['worker_id'][:8]}{flag}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "submit": cmd_submit,
        "worker": cmd_worker,
        "status": cmd_status,
        "list": cmd_list,
        "runs": cmd_runs,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    sys.exit(main())