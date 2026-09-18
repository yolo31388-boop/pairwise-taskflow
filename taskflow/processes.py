"""Cross-platform helpers for launching and killing task subprocesses.

On Windows every task's shell is assigned to a Job Object configured with
``JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE``.  All handles live inside the worker
process, so if the worker is killed hard (taskkill /F, kill -9 equivalent)
the OS immediately terminates the shell *and every descendant* -- this is
what prevents a task executing twice after crash recovery.
"""

from __future__ import annotations

import ctypes
import os
import signal
import subprocess
import sys
from ctypes import wintypes


def _linux_pdeathsig() -> None:
    """Forked-child hook on Linux: die if the parent worker dies.

    Survives the following execve (process-level attribute), closing the tiny
    window between fork and heartbeat-based recovery.
    """
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(1, signal.SIGKILL, 0, 0, 0)  # PR_SET_PDEATHSIG = 1
    except Exception:  # pragma: no cover - best effort only
        pass


# --------------------------------------------------------------------- Windows

if sys.platform == "win32":
    _kernel32 = ctypes.windll.kernel32

    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    JobObjectExtendedLimitInformation = 9

    class _IOCounters(ctypes.Structure):
        _fields_ = [(f"f{i}", ctypes.c_ulonglong) for i in range(6)]

    class _BasicLimit(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_void_p),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _ExtLimit(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimit),
            ("IoInfo", _IOCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    def _create_kill_on_close_job() -> int:
        job = _kernel32.CreateJobObjectW(None, None)
        if not job:
            raise ctypes.WinError()
        info = _ExtLimit()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        ok = _kernel32.SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
        if not ok:
            raise ctypes.WinError()
        return int(job)

    _JOB_HANDLE: int | None = None

    def _job_handle() -> int:
        global _JOB_HANDLE
        if _JOB_HANDLE is None:
            _JOB_HANDLE = _create_kill_on_close_job()
            # The handle is intentionally never closed while we live: the OS
            # closes it when the worker process dies, killing every task
            # subtree assigned to the job.
        return _JOB_HANDLE

    def _assign_to_job(proc: subprocess.Popen) -> None:
        try:
            _kernel32.AssignProcessToJobObject(_job_handle(), int(proc._handle))
        except Exception:
            pass  # best effort; heartbeat/reaper recovery remains the backstop

else:
    def _assign_to_job(proc: subprocess.Popen) -> None:  # type: ignore[misc]
        return None


def spawn_shell(command: str, env: dict[str, str]):
    """Start *command* through the system shell, output inheriting our fds."""
    kwargs: dict = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
        if sys.platform.startswith("linux"):
            kwargs["preexec_fn"] = _linux_pdeathsig
    proc = subprocess.Popen(command, shell=True, env=env, **kwargs)
    _assign_to_job(proc)
    return proc


def kill_process_tree(pid: int) -> None:
    """Best-effort hard kill of a process and all of its descendants."""
    if not pid or pid <= 0:
        return
    if sys.platform == "win32":
        _kill_tree_windows(pid)
    else:
        _kill_tree_posix(pid)


def _posix_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _kill_tree_posix(pid: int) -> None:
    try:
        pgid = os.getpgid(pid)
    except ProcessLookupError:
        return
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except PermissionError:
        pass
    import time

    deadline = time.time() + 2
    while time.time() < deadline:
        try:
            waited, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if waited == pid or not _posix_pid_alive(pid):
            return
        time.sleep(0.05)


def _kill_tree_windows(pid: int) -> None:
    # Preferred: kill the whole tree via taskkill.  When that is blocked
    # (restricted environments), kill the single pid; descendants spawned by
    # our own spawn_shell are already bound by the kill-on-close job object.
    try:
        result = subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            capture_output=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        result = None
    if result is not None and result.returncode == 0:
        return
    try:
        subprocess.run(
            ["taskkill", "/F", "/PID", str(pid)],
            capture_output=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass