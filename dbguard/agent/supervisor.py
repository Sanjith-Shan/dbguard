"""mysqld process supervision.

The agent is the container's main process (under tini) and runs mysqld as its child.
`docker-entrypoint.sh mysqld` execs (through gosu) into mysqld, so the child pid is the
mysqld pid once the entrypoint is done. The child gets MYSQLD_PARENT_PID set to the
agent's pid, which is how mysqld recognises a monitoring process: RESTART and the
post-CLONE restart then exit with code 16 instead of failing with ER 3707.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time

import structlog

log = structlog.get_logger("dbguard.agent.supervisor")

RESTART_EXIT_CODE = 16


class NullSupervisor:
    """--no-supervise: mysqld is someone else's child. Nothing to kill or restart."""

    supervised = False
    pid: int | None = None
    generation = 0
    restarts = 0

    @property
    def alive(self) -> bool | None:
        return None

    async def start(self) -> None:
        return None

    def kill(self, sig: int = signal.SIGKILL) -> int | None:
        return None

    async def wait_exit(self, timeout: float) -> bool:
        return False

    async def wait_generation(self, gen: int, timeout: float) -> bool:
        return False

    async def stop(self, timeout: float = 120.0) -> None:
        return None


class MysqldSupervisor:
    supervised = True

    def __init__(self, cmd: list[str], *, min_backoff: float = 0.5, max_backoff: float = 5.0,
                 on_restart=None):
        self.cmd = cmd
        self.min_backoff = min_backoff
        self.max_backoff = max_backoff
        self.proc: asyncio.subprocess.Process | None = None
        self.generation = 0          # bumps every time a new mysqld is spawned
        self.restarts = 0
        self._stopping = False
        self._immediate = False      # next restart without backoff (kill fence, clone)
        self._task: asyncio.Task | None = None
        self._changed = asyncio.Condition()
        self._on_restart = on_restart

    @property
    def pid(self) -> int | None:
        return self.proc.pid if self.proc and self.proc.returncode is None else None

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="mysqld-supervisor")

    async def _notify(self) -> None:
        async with self._changed:
            self._changed.notify_all()

    async def _run(self) -> None:
        backoff = self.min_backoff
        env = dict(os.environ)
        env["MYSQLD_PARENT_PID"] = str(os.getpid())
        while not self._stopping:
            started = time.monotonic()
            try:
                self.proc = await asyncio.create_subprocess_exec(*self.cmd, env=env)
            except OSError as e:
                log.error("mysqld_spawn_failed", cmd=self.cmd, error=str(e))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)
                continue
            self.generation += 1
            log.info("mysqld_started", pid=self.proc.pid, generation=self.generation,
                     cmd=self.cmd)
            await self._notify()
            if self._on_restart and self.generation > 1:
                try:
                    self._on_restart()
                except Exception:
                    log.exception("on_restart_failed")
            rc = await self.proc.wait()
            ran = time.monotonic() - started
            log.warning("mysqld_exited", pid=self.proc.pid, returncode=rc, ran_s=round(ran, 2),
                        stopping=self._stopping)
            await self._notify()
            if self._stopping:
                break
            self.restarts += 1
            if rc == RESTART_EXIT_CODE or self._immediate:
                self._immediate = False
                backoff = self.min_backoff
                continue
            if ran > 30:
                backoff = self.min_backoff
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.max_backoff)

    def kill(self, sig: int = signal.SIGKILL) -> int | None:
        pid = self.pid
        if pid is None:
            return None
        if sig == signal.SIGKILL:
            self._immediate = True
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return None
        log.warning("mysqld_signalled", pid=pid, signal=signal.Signals(sig).name)
        return pid

    async def wait_exit(self, timeout: float) -> bool:
        proc = self.proc
        if proc is None or proc.returncode is not None:
            return True
        try:
            await asyncio.wait_for(asyncio.shield(proc.wait()), timeout)
            return True
        except TimeoutError:
            return False

    async def wait_generation(self, gen: int, timeout: float) -> bool:
        """Wait until a mysqld newer than generation `gen` is running."""
        async def cond() -> None:
            async with self._changed:
                await self._changed.wait_for(lambda: self.generation > gen and self.alive)
        try:
            await asyncio.wait_for(cond(), timeout)
            return True
        except TimeoutError:
            return False

    async def stop(self, timeout: float = 120.0) -> None:
        self._stopping = True
        proc = self.proc
        if proc is not None and proc.returncode is None:
            try:
                proc.send_signal(signal.SIGCONT)  # a stopped process cannot act on SIGTERM
                proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
            log.info("mysqld_stopping", pid=proc.pid)
            if not await self.wait_exit(timeout):
                log.error("mysqld_stop_timeout_sigkill", pid=proc.pid)
                proc.kill()
                await proc.wait()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
