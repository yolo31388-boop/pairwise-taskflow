"""Worker-side command script used by tests.

Modes (selected by argv[1]):

  flaky       holds an exclusive per-task marker, sleeps briefly, then exits 1
              with ~25% probability (or always when fail-marker exists).
  bomb        holds an exclusive per-task marker and sleeps 60s; a hard-killed
              bomb leaves a stale marker, proving later restart works.
  quick       print a timestamped line, exit 0.
  alwaysfail  exit 1 (for retry/backoff semantics).

Environment:
  TASKFLOW_TASK_ID     task id (set by the worker)
  TASKFLOW_HELPER_DIR  directory holding logs, markers and locks
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
import time

BASE = os.environ["TASKFLOW_HELPER_DIR"]
TASK_ID = os.environ.get("TASKFLOW_TASK_ID", "unknown")
LOG = os.path.join(BASE, "timestamps.log")


def log(message: str) -> None:
    with open(LOG, "a") as fh:
        fh.write(f"{time.time():.4f} pid={os.getpid()} task={TASK_ID} {message}\n")


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, AttributeError):
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
            capture_output=True, text=True, timeout=5,
        )
        return f'"{pid}"' in r.stdout
    return True


def hold_lock():
    """Exclusive per-task marker directory held for the process lifetime.

    A concurrent second invocation of the same task finds a live owner and
    exits 2 (logged as OVERLAP-DETECTED).  A marker whose owner pid is dead
    is a stale marker left by a hard-killed run and is taken over.
    """
    path = os.path.join(BASE, f"lock-{TASK_ID}")
    try:
        os.mkdir(path)
    except FileExistsError:
        pid_file = os.path.join(path, "pid")
        try:
            with open(pid_file) as fh:
                owner = int(fh.read().strip())
        except (OSError, ValueError):
            owner = -1
        if _pid_alive(owner):
            log("OVERLAP-DETECTED")
            sys.stderr.write(f"overlap: task {TASK_ID} already running\n")
            sys.exit(2)
        # Stale marker from a hard-killed run; take it over.
        try:
            os.remove(pid_file)
        except OSError:
            pass
        try:
            os.rmdir(path)
        except OSError:
            pass
        try:
            os.mkdir(path)
        except FileExistsError:
            log("OVERLAP-DETECTED")
            sys.exit(2)
    with open(os.path.join(path, "pid"), "w") as fh:
        fh.write(str(os.getpid()))

    class Guard:
        def drop(self) -> None:
            for name in ("pid",):
                try:
                    os.remove(os.path.join(path, name))
                except OSError:
                    pass
            try:
                os.rmdir(path)
            except OSError:
                pass

    return Guard()


def mode_flaky() -> int:
    guard = hold_lock()
    random.seed()
    log("FLAKY-START")
    time.sleep(random.random() * 0.25)
    forced = os.path.exists(os.path.join(BASE, "fail-marker"))
    fail = forced or random.random() < 0.25
    if fail:
        log("FLAKY-FAIL")
        guard.drop()
        return 1
    log("FLAKY-DONE")
    guard.drop()
    return 0


def mode_bomb() -> int:
    guard = hold_lock()
    default = float(os.environ.get("TASKFLOW_BOMB_SECONDS", "2"))
    # A per-task "shorten" marker lets the resumed attempt finish quickly.
    shorten = os.path.join(BASE, f"shorten-{TASK_ID}")
    duration = 0.5 if os.path.exists(shorten) else default
    log(f"BOMB-START duration={duration}")
    try:
        deadline = time.time() + duration
        while time.time() < deadline:
            time.sleep(0.1)
    finally:
        guard.drop()
    log("BOMB-DONE")
    return 0


def mode_quick() -> int:
    log("QUICK-RUN")
    return 0


def mode_alwaysfail() -> int:
    log("ALWAYSFAIL")
    return 1


MODES = {
    "flaky": mode_flaky,
    "bomb": mode_bomb,
    "quick": mode_quick,
    "alwaysfail": mode_alwaysfail,
}


def main() -> int:
    return MODES[sys.argv[1]]()


if __name__ == "__main__":
    sys.exit(main())