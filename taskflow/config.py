"""Tunable constants for taskflow.

Overrides are read from environment variables (useful in tests).
"""

from __future__ import annotations

import os
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


DEFAULT_DB_PATH = str(Path.cwd() / "taskflow.db")
DB_PATH = os.environ.get("TASKFLOW_DB", DEFAULT_DB_PATH)

DEFAULT_WORKERS = 4

# A task is attempted up to MAX_ATTEMPTS times in total (initial run plus
# MAX_ATTEMPTS - 1 retries); user-facing wording is "retry up to 3 times".
MAX_ATTEMPTS = _env_int("TASKFLOW_MAX_ATTEMPTS", 4)

# Exponential backoff between attempts: 1s, 2s, 4s after each failed attempt.
RETRY_BACKOFF_BASE = _env_int("TASKFLOW_RETRY_BACKOFF_BASE", 1)
RETRY_BACKOFF_FACTOR = _env_int("TASKFLOW_RETRY_BACKOFF_FACTOR", 2)

# Worker liveness / lease bookkeeping.
HEARTBEAT_INTERVAL = _env_int("TASKFLOW_HEARTBEAT_INTERVAL", 5)
LEASE_TIMEOUT = _env_int("TASKFLOW_LEASE_TIMEOUT", 30)
# Heartbeat must be at least this stale before a (pid-dead) worker is treated
# as crashed. The staleness gate protects against pid-reuse false negatives.
DEAD_WORKER_THRESHOLD = LEASE_TIMEOUT * 2
REAP_INTERVAL = _env_int("TASKFLOW_REAP_INTERVAL", 7)

# Idle polling interval for the worker claim loop.
CLAIM_POLL_INTERVAL = float(os.environ.get("TASKFLOW_CLAIM_POLL", "0.2"))