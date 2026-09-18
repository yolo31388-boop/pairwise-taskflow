"""asyncio worker：多槽位并发执行 shell 命令。

互斥与恢复
----------
每个任务被领取时带一个随机 lease_token 和过期时间 lease_expires：

* 执行期间有独立心跳协程周期性续租；
* 完成/失败时只有 token 匹配的持有者能写结果（fencing），即使租约
  因卡顿被别的 worker 接管，陈旧持有者的结果也不会覆盖新执行；
* worker 被 kill -9 后心跳停止，租约到期，任务被 reaper 退回 pending，
  由存活的 worker 或重启后的 worker 重新执行。

优雅关闭
--------
收到 SIGINT/SIGTERM 后停止领取新任务，正在跑的子进程等它跑完，
结果照常落库，然后以退出码 0 结束。
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import signal
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from . import db as dbmod

log = logging.getLogger("taskflow")


class Worker:
    def __init__(
        self,
        db: "dbmod.Database",
        workers: int = 4,
        lease_seconds: float = 10.0,
        heartbeat_interval: float = 3.0,
        poll_interval: float = 0.2,
        reap_interval: float = 2.0,
        backoff_base: float = 1.0,
        worker_id: Optional[str] = None,
    ) -> None:
        self.db = db
        self.workers = max(1, int(workers))
        self.lease_seconds = float(lease_seconds)
        self.heartbeat_interval = float(heartbeat_interval)
        self.poll_interval = float(poll_interval)
        self.reap_interval = float(reap_interval)
        self.backoff_base = float(backoff_base)
        self.worker_id = worker_id or "%s-%d" % (platform.node(), os.getpid())
        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, self.workers),
            thread_name_prefix="taskflow-db",
        )

    # ---- 对外控制 ------------------------------------------------------

    def request_stop(self) -> None:
        """请求优雅关闭（信号处理器 / 测试都可调用）。"""
        if not self._stop.is_set():
            self._stop.set()
            log.info("[%s] 收到关闭信号，不再领取新任务，等待在跑的任务完成…",
                     self.worker_id)

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    # ---- DB helper -----------------------------------------------------

    async def _db(self, fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, lambda: fn(*args, **kwargs)
        )

    # ---- 主循环 --------------------------------------------------------

    async def run(self) -> int:
        log.info(
            "[%s] worker 启动：%d 个并发槽位，租约 %.1fs，心跳 %.1fs",
            self.worker_id,
            self.workers,
            self.lease_seconds,
            self.heartbeat_interval,
        )
        # 启动即回收一次上一个被强杀进程遗留的过期任务
        reaped = await self._db(self.db.reap_stale)
        if reaped:
            log.info("[%s] 启动回收 %d 个过期任务：%s",
                     self.worker_id, len(reaped), ", ".join(reaped))

        slots = [asyncio.create_task(self._slot_loop(i))
                 for i in range(self.workers)]
        reaper = asyncio.create_task(self._reap_loop())
        try:
            await asyncio.gather(*slots)
        finally:
            reaper.cancel()
            try:
                await reaper
            except BaseException:
                pass
            self._executor.shutdown(wait=True)
            log.info("[%s] 所有在跑任务已结束，worker 退出", self.worker_id)
        return 0

    async def _reap_loop(self) -> None:
        while not self._stop.is_set():
            try:
                ids = await self._db(self.db.reap_stale)
                if ids:
                    log.warning(
                        "[%s] 回收 %d 个租约过期任务（原持有者可能已崩溃）：%s",
                        self.worker_id,
                        len(ids),
                        ", ".join(ids),
                    )
            except Exception:  # 回收失败不应杀死 worker
                log.exception("reaper 出错")
            # 可被立即唤醒，避免关闭时还要等一个 reap_interval
            stopped = await self._interruptible_sleep(self.reap_interval)
            if stopped:
                return

    async def _interruptible_sleep(self, seconds: float) -> bool:
        """睡一会；期间收到关闭信号则提前返回 True。"""
        loop = asyncio.get_running_loop()
        end = loop.time() + seconds
        while not self._stop.is_set():
            step = min(0.1, end - loop.time())
            if step <= 0:
                return False
            await asyncio.sleep(step)
        return True

    async def _slot_loop(self, slot: int) -> None:
        while not self._stop.is_set():
            try:
                task = await self._db(
                    self.db.claim_next,
                    self.worker_id,
                    uuid.uuid4().hex,
                    self.lease_seconds,
                )
                if task is None:
                    await asyncio.sleep(self.poll_interval)
                    continue
                await self._execute(slot, task)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("[%s] 槽位%d 出现异常，稍后继续",
                              self.worker_id, slot)
                await asyncio.sleep(self.poll_interval)

    # ---- 单个任务 ------------------------------------------------------

    async def _execute(self, slot: int, task: Dict[str, Any]) -> None:
        task_id = task["id"]
        command = task["command"]
        token = task["lease_token"]
        attempt = task["attempts"]
        log.info(
            "[%s] 槽位%d 领取任务 %s（第 %d 次尝试）: %s",
            self.worker_id, slot, task_id, attempt, command,
        )
        start = time.monotonic()
        proc = None
        lost_lease = False

        async def _heartbeat() -> None:
            nonlocal lost_lease
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                ok = await self._db(
                    self.db.heartbeat, task_id, token, self.lease_seconds
                )
                if not ok:
                    lost_lease = True
                    log.warning(
                        "[%s] 任务 %s 的租约已被接管，尝试终止本地子进程",
                        self.worker_id, task_id,
                    )
                    if proc is not None and proc.returncode is None:
                        try:
                            proc.kill()
                        except ProcessLookupError:
                            pass
                    return

        hb = asyncio.create_task(_heartbeat())
        try:
            popen_kwargs: Dict[str, Any] = {}
            if platform.system() == "Windows":
                # 让任务子进程脱离当前控制台进程组：worker 收到
                # Ctrl+C / Ctrl+Break 优雅关闭时，信号不会广播杀掉
                # 正在运行的子进程，保证“在跑的任务等它跑完”。
                popen_kwargs["creationflags"] = getattr(
                    subprocess, "CREATE_NEW_PROCESS_GROUP", 0
                )
            try:
                proc = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    **popen_kwargs,
                )
            except OSError as exc:
                hb.cancel()
                await self._record_failure(task_id, token, "无法启动命令: %s" % exc)
                return

            stdout, _ = await proc.communicate()
            hb.cancel()
            elapsed = time.monotonic() - start
            output = self._decode(stdout)
            if proc.returncode == 0 and not lost_lease:
                ok = await self._db(self.db.mark_succeeded, task_id, token, output)
                log.info("[%s] 任务 %s 成功，用时 %.2fs（结果%s）",
                         self.worker_id, task_id, elapsed,
                         "已落库" if ok else "被丢弃：租约已易主")
            elif lost_lease:
                log.warning(
                    "[%s] 任务 %s 完成但租约已易主（returncode=%s），结果不覆盖",
                    self.worker_id, task_id, proc.returncode,
                )
            else:
                error = "退出码 %d；输出: %s" % (proc.returncode, output[-2000:])
                new_status = await self._db(
                    self.db.mark_failed, task_id, token, error, self.backoff_base
                )
                if new_status == dbmod.STATUS_DEAD:
                    log.error("[%s] 任务 %s 第 %d 次仍失败，进入 dead",
                              self.worker_id, task_id, attempt)
                else:
                    log.warning(
                        "[%s] 任务 %s 第 %d 次失败，%.0fs 后重试",
                        self.worker_id, task_id, attempt,
                        dbmod.retry_delay(attempt, self.backoff_base),
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 执行器自身异常不能杀死整个 worker
            hb.cancel()
            if proc is not None and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            await self._db(
                self.db.mark_failed,
                task_id,
                token,
                "执行器异常: %r" % exc,
                self.backoff_base,
            )
            log.exception("[%s] 任务 %s 执行异常", self.worker_id, task_id)
        finally:
            if not hb.done():
                hb.cancel()

    async def _record_failure(self, task_id: str, token: str, error: str) -> None:
        status = await self._db(
            self.db.mark_failed, task_id, token, error, self.backoff_base
        )
        log.warning("[%s] 任务 %s 启动失败 -> %s: %s",
                    self.worker_id, task_id, status, error)

    @staticmethod
    def _decode(data: bytes) -> str:
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("utf-8", errors="replace")


def install_signal_handlers(worker: Worker) -> None:
    """在主线程注册关闭信号 -> request_stop。

    POSIX：SIGTERM、SIGINT；Windows：SIGINT（Ctrl+C，仅真实控制台）
    和 SIGBREAK（Ctrl+Break）。Windows 的 SIGTERM 无法被进程捕获。
    用 signal.signal 注册的处理器在事件循环的主线程中通过回调唤醒，
    不依赖仅 Unix 支持的 loop.add_signal_handler。
    """
    is_windows = platform.system() == "Windows"
    if is_windows:
        names = ("SIGINT", "SIGBREAK")
    else:
        names = ("SIGTERM", "SIGINT")
    for name in names:
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, lambda *_: worker.request_stop())
        except (ValueError, OSError, RuntimeError):
            # 非主线程等无法注册的场景：POSIX 再尝试事件循环级注册
            if not is_windows:
                loop = asyncio.get_event_loop()
                loop.add_signal_handler(sig, worker.request_stop)
