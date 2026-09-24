"""The manager's own view of a set: agent /status of every node and a write probe of the
believed primary. Every call is bounded so a hung node never stalls the loop."""

from __future__ import annotations

import asyncio
import ssl
import time
from collections.abc import Sequence
from typing import Protocol

import aiomysql
import structlog

from dbguard.manager.client import Addressing, AgentClient
from dbguard.manager.model import NodeView, Observation, ProbeResult

log = structlog.get_logger("dbguard.manager.probe")

ER_READ_ONLY = 1290   # --super-read-only / --read-only
ER_CANT_CONNECT = 2003
ER_LOST = 2013


class Prober(Protocol):
    async def probe(self, rs: str, node: str) -> ProbeResult: ...
    async def close(self) -> None: ...


def _tls() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class MysqlProber:
    """SELECT 1 plus an upsert into dbguard.manager_probe, within probe_timeout_s.

    The write is the point. A primary partitioned from its replicas answers SELECT 1
    happily, but with semi-sync AFTER_SYNC and an hour timeout its commits wait for an ack
    that never comes, so the upsert times out and the probe fails (Experiment 3).

    One connection per node is kept while probes succeed. Any error or timeout closes
    it, because a cancelled aiomysql call leaves the protocol mid-packet. A probe that
    timed out on a blocked commit leaves that server thread waiting for its ack, one per
    failed probe, until the fence kills client threads.
    """

    def __init__(self, addressing: Addressing, user: str, password: str, timeout_s: float,
                 use_tls: bool = True):
        self.addr = addressing
        self.user, self.password = user, password
        self.timeout_s = timeout_s
        self.use_tls = use_tls
        self._conns: dict[str, aiomysql.Connection] = {}

    async def _conn(self, node: str) -> aiomysql.Connection:
        c = self._conns.get(node)
        if c is not None and not c.closed:
            return c
        host, port = self.addr.mysql_addr(node)
        c = await aiomysql.connect(host=host, port=port, user=self.user,
                                   password=self.password, autocommit=True,
                                   connect_timeout=self.timeout_s,
                                   ssl=_tls() if self.use_tls else None)
        self._conns[node] = c
        return c

    def _drop(self, node: str) -> None:
        c = self._conns.pop(node, None)
        if c is not None:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    async def probe(self, rs: str, node: str) -> ProbeResult:
        t0 = time.monotonic()
        ts = time.time()
        state = {"select_ok": False}

        async def go():
            c = await self._conn(node)
            async with c.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchall()
                state["select_ok"] = True
                await cur.execute(
                    "INSERT INTO dbguard.manager_probe (rs, ts) VALUES (%s, NOW(6)) "
                    "ON DUPLICATE KEY UPDATE ts=NOW(6)", (rs,))

        try:
            await asyncio.wait_for(go(), self.timeout_s)
            return ProbeResult(ok=True, ts=ts, duration_s=time.monotonic() - t0, select_ok=True)
        except TimeoutError:
            self._drop(node)
            kind, err = "timeout", f"no answer in {self.timeout_s:.1f} s"
            if state["select_ok"]:
                err = f"SELECT 1 ok but heartbeat write blocked for {self.timeout_s:.1f} s"
        except Exception as e:  # noqa: BLE001
            self._drop(node)
            code = e.args[0] if e.args and isinstance(e.args[0], int) else None
            kind = {ER_READ_ONLY: "readonly", ER_CANT_CONNECT: "connect"}.get(code, "error")
            if isinstance(e, OSError):
                kind = "connect"
            err = f"{type(e).__name__}: {e}"[:300]
        return ProbeResult(ok=False, ts=ts, duration_s=time.monotonic() - t0,
                           select_ok=state["select_ok"], kind=kind, error=err)

    async def close(self) -> None:
        for n in list(self._conns):
            self._drop(n)


async def observe(rs: str, primary: str | None, nodes: Sequence[str], agents: AgentClient,
                  prober: Prober | None, probe_primary: bool = True) -> Observation:
    """One bounded poll: every agent's /status and the primary probe, concurrently."""
    ts = time.time()
    tasks = [agents.status(n) for n in nodes]
    do_probe = probe_primary and primary is not None and prober is not None
    if do_probe:
        tasks.append(prober.probe(rs, primary))
    res = await asyncio.gather(*tasks, return_exceptions=True)
    views: dict[str, NodeView] = {}
    for n, r in zip(nodes, res[: len(nodes)]):
        views[n] = r if isinstance(r, NodeView) else NodeView.unreachable(n, repr(r))
    probe = None
    if do_probe:
        r = res[-1]
        probe = r if isinstance(r, ProbeResult) else ProbeResult(
            ok=False, ts=ts, kind="error", error=repr(r))
    return Observation(ts=ts, primary=primary, probe=probe, nodes=views)
