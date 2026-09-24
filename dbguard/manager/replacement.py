"""Replacing a lost replica with a new host.

When a set has been DEGRADED for longer than ``rebuild_after_s``, the manager asks a
Provisioner for a new node, waits for its agent, clones it from a healthy replica (never
from the primary, to keep the primary's I/O for clients) and repoints it.

In this lab the new host is the set's spare container, started with docker compose. In a
real fleet ``Provisioner.provision`` is where the manager would call the host allocator
(ask the inventory service for a machine in the right failure domain, image it with the
agent, and return its name once the agent answers). Nothing else in the manager changes.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import time
from typing import TYPE_CHECKING, Protocol

import structlog

from dbguard.manager.model import Observation, State

if TYPE_CHECKING:
    from dbguard.manager.controller import SetController

log = structlog.get_logger("dbguard.manager.replacement")

AGENT_WAIT_S = 180.0


class Provisioner(Protocol):
    async def provision(self, rs: str, node: str) -> None:
        """Make ``node`` exist and start its agent. Raise on failure."""


class NullProvisioner:
    """Does nothing, for tests and for fleets where the spare is always running."""

    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    async def provision(self, rs: str, node: str) -> None:
        self.calls.append((rs, node))


class ComposeProvisioner:
    """``docker compose -f <file> --profile spare up -d <node>``.

    The manager container mounts the compose file at /deploy/docker-compose.yml and the
    docker socket. Override with DBGUARD_COMPOSE_FILE and DBGUARD_COMPOSE_PROJECT.
    """

    def __init__(self, compose_file: str | None = None, project: str | None = None,
                 timeout_s: float = 120.0):
        self.compose_file = compose_file or os.environ.get("DBGUARD_COMPOSE_FILE",
                                                           "/deploy/docker-compose.yml")
        self.project = project or os.environ.get("DBGUARD_COMPOSE_PROJECT")
        self.timeout_s = timeout_s

    def command(self, node: str) -> list[str]:
        cmd = ["docker", "compose", "-f", self.compose_file]
        if self.project:
            cmd += ["-p", self.project]
        return cmd + ["--profile", "spare", "up", "-d", "--no-recreate", node]

    async def provision(self, rs: str, node: str) -> None:
        cmd = self.command(node)
        log.warning("provision", rs=rs, node=node, cmd=shlex.join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), self.timeout_s)
        except TimeoutError:
            proc.kill()
            raise RuntimeError(f"{shlex.join(cmd)} timed out after {self.timeout_s:.0f} s")
        if proc.returncode != 0:
            raise RuntimeError(f"{shlex.join(cmd)} exited {proc.returncode}: "
                               f"{out.decode(errors='replace')[-500:]}")


async def maybe_replace(ctl: SetController, ob: Observation, healthy: list[str]) -> None:
    spare = ctl.scfg.spare
    if not spare or spare in ctl.members or ctl.provisioner is None:
        return
    if ctl.st.state != State.DEGRADED or ctl.unhealthy_since is None:
        return      # never while FAILING_OVER, SUSPECT, HALTED or already REBUILDING
    if ctl.st.in_cooldown():
        return      # a failover just happened, give the old primary time to come back
    if ob.ts - ctl.unhealthy_since < ctl.cfg.rebuild_after_s:
        return
    if not healthy:
        return  # no donor; the spare would have nothing to clone from
    ctl.start_maintenance(replace(ctl, spare, healthy[0]), [spare])


async def replace(ctl: SetController, spare: str, donor: str):
    from dbguard.manager.rejoin import rebuild_and_join

    t0 = time.monotonic()
    ctl.st.to(State.REBUILDING, note=f"provisioning {spare} to replace a lost replica")
    try:
        await ctl.provisioner.provision(ctl.rs, spare)
    except Exception as e:  # noqa: BLE001
        ctl.event(type="replace", old_primary=None, new_primary=ctl.primary,
                  note=f"provisioning {spare} failed: {e}")
        ctl.unhealthy_since = time.time()   # wait another rebuild_after_s before retrying
        return None
    end = time.monotonic() + AGENT_WAIT_S
    while time.monotonic() < end:
        nv = await ctl.agents.status(spare)
        if nv.usable:
            break
        await asyncio.sleep(1.0)
    else:
        ctl.event(type="replace", new_primary=ctl.primary,
                  note=f"{spare} agent did not come up within {AGENT_WAIT_S:.0f} s")
        ctl.unhealthy_since = time.time()
        return None
    ev = await rebuild_and_join(ctl, spare, donor, phantom_count=0, phantom=None,
                                why="replacement for a lost replica", event_type="replace")
    if ev is not None:
        ctl.members.append(spare)
        log.warning("replaced", rs=ctl.rs, node=spare, total_s=round(time.monotonic() - t0, 3))
    return ev
