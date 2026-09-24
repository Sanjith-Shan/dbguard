"""The manager process, one SetController per replica set sharing an event log and metrics.

Sets are independent. Each runs its own control loop task, so a failover in ``rs1`` never
waits on ``rs2``, and the harness checks that injecting into one set changes nothing in the
other. There is one manager, and if it dies nothing fails over (docs/DESIGN.md section 12).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog

from dbguard.events import EventLog
from dbguard.manager.client import Addressing, AgentClient
from dbguard.manager.controller import SetController
from dbguard.manager.metrics import Metrics
from dbguard.manager.probe import MysqlProber, Prober
from dbguard.manager.replacement import Provisioner

log = structlog.get_logger("dbguard.manager")


class Manager:
    """Builds the shared clients and one controller per configured set."""

    def __init__(self, cfg, *, addressing: Addressing | None = None,
                 state_dir: str | None = None, mode: str | None = None,
                 rejoin: str | None = None, prober: Prober | None = None,
                 agents: AgentClient | None = None, provisioner: Provisioner | None = None,
                 events: EventLog | None = None, quiesce_interval_s: float | None = None):
        self.cfg = cfg
        self.mode = mode or cfg.mode
        self.addr = addressing or Addressing(agent_port=cfg.agent_port,
                                             mysql_port=cfg.mysql.port)
        self.agents = agents or AgentClient(self.addr)
        self.prober = prober if prober is not None else MysqlProber(
            self.addr, cfg.mysql.user, cfg.mysql.password, cfg.probe_timeout_s)
        path = Path(state_dir) / "events.jsonl" if state_dir else None
        self.events = events or EventLog(path)
        self.metrics = Metrics()
        self.sets: dict[str, SetController] = {
            rs: SetController(rs, cfg, scfg, self.agents, self.prober, self.events,
                              metrics=self.metrics, provisioner=provisioner, mode=self.mode,
                              rejoin=rejoin, quiesce_interval_s=quiesce_interval_s)
            for rs, scfg in cfg.sets.items()
        }
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Start every set's control loop."""
        for rs, ctl in self.sets.items():
            self._tasks.append(asyncio.create_task(ctl.run(), name=f"set-{rs}"))
        log.info("manager started", mode=self.mode, sets=list(self.sets))

    async def stop(self) -> None:
        """Stop polling everything. Never touches the fleet."""
        for ctl in self.sets.values():
            await ctl.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await self.agents.close()
        await self.prober.close()
        log.info("manager stopped")
