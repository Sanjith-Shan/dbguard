"""MysqldSupervisor against a stand-in child process (a sleeping python)."""

from __future__ import annotations

import asyncio
import signal
import sys

from dbguard.agent.supervisor import RESTART_EXIT_CODE, MysqldSupervisor

SLEEPER = [sys.executable, "-c", "import time; time.sleep(600)"]


async def test_restart_after_sigkill_and_stop_with_sigterm():
    restarts = []
    sup = MysqldSupervisor(SLEEPER, min_backoff=0.05, on_restart=lambda: restarts.append(1))
    await sup.start()
    assert await sup.wait_generation(0, 5)
    first = sup.pid
    assert sup.alive and first
    gen = sup.generation
    assert sup.kill(signal.SIGKILL) == first
    assert await sup.wait_generation(gen, 5)
    assert sup.pid != first and sup.alive and restarts == [1]
    await sup.stop(timeout=5)
    assert not sup.alive
    assert sup.proc.returncode == -signal.SIGTERM


async def test_stop_resumes_a_stopped_child():
    sup = MysqldSupervisor(SLEEPER, min_backoff=0.05)
    await sup.start()
    assert await sup.wait_generation(0, 5)
    sup.kill(signal.SIGSTOP)
    await sup.stop(timeout=5)
    assert not sup.alive


async def test_exit_code_16_restarts_without_backoff():
    code = f"import sys; sys.exit({RESTART_EXIT_CODE})"
    sup = MysqldSupervisor([sys.executable, "-c", code], min_backoff=5.0)
    await sup.start()
    await asyncio.sleep(1.5)
    # With a 5 s backoff only one start would fit in 1.5 s. Code 16 skips it.
    assert sup.generation >= 3
    await sup.stop(timeout=5)
