"""In-process fake fleet, agents speaking the real HTTP contract over a simulated replica set.

tests/test_manager_sim.py runs the real manager against it, so whole failovers, rejoins and
switchovers are tested without MySQL or Docker. Each FakeNode models mysqld plus agent (a
gtid_executed, a relay log, a source, semi-sync flags, a fence flag), and ``FakeFleet.step``
moves time forward. With semi-sync the primary commits only when a replica can take the
transaction, and counts it acknowledged only once a replica holds it, so ``acked`` is exactly
what a lossless failover must keep. Faults are flags, ``kill``, ``hung`` (mysqld stopped),
``frozen`` (whole container), ``partition(a, b)`` and ``manager_cut``.
"""

from __future__ import annotations

import asyncio
import itertools
import time
import uuid as uuidlib
from dataclasses import dataclass, field

from aiohttp import web

from dbguard.gtid import GtidSet
from dbguard.manager.model import ProbeResult


@dataclass
class FakeNode:
    """One simulated mysqld and its agent."""

    name: str
    rs: str
    uuid: str = field(default_factory=lambda: str(uuidlib.uuid4()))
    executed: GtidSet = field(default_factory=GtidSet)
    retrieved: GtidSet = field(default_factory=GtidSet)
    super_read_only: bool = True
    fenced: bool = False
    source: str | None = None
    semisync_source: bool = False
    semisync_replica: bool = False
    alive: bool = True          # container
    hung: bool = False          # mysqld SIGSTOPped
    frozen: bool = False        # whole container SIGSTOPped
    mysqld_up: bool = True
    apply_per_step: int = 1000  # relay-log transactions applied per step
    receive: bool = True        # IO thread takes new transactions (to stage lag/divergence)
    heartbeat_ts: float | None = None
    next_gno: int = 1
    sql_error: str | None = None
    io_stopped: bool = False    # STOP REPLICA IO_THREAD
    status_delay: float = 0.0   # a slow /status (loaded agent)
    repoint_delay: float = 0.0  # a slow /repoint (START REPLICA taking seconds)
    fence_delay: float = 0.0    # a slow /fence
    fail_next: dict = field(default_factory=dict)  # path -> times to answer 500 lost link
    partial: GtidSet = field(default_factory=GtidSet)  # in the relay log, never appliable
    calls: list[str] = field(default_factory=list)

    @property
    def responsive(self) -> bool:
        """Container up, mysqld up and not stopped."""
        return self.alive and self.mysqld_up and not self.hung and not self.frozen

    @property
    def writable(self) -> bool:
        """Accepts writes."""
        return self.responsive and not self.super_read_only and not self.fenced


class FakeFleet:
    """Every simulated node, the network cuts between them and the acknowledged writes."""

    def __init__(self, sets: dict[str, list[str]], semisync: bool = True,
                 spares: dict[str, str] | None = None):
        self.semisync = semisync
        self.nodes: dict[str, FakeNode] = {}
        self.sets = sets
        for rs, names in sets.items():
            for n in names:
                self.nodes[n] = FakeNode(n, rs)
        for rs, n in (spares or {}).items():
            self.nodes[n] = FakeNode(n, rs, alive=False)
        self.cut: set[frozenset] = set()          # node pairs that cannot talk
        self.manager_cut: set[str] = set()        # manager cannot reach these mysqlds
        self.writing = False
        self.acked: dict[str, GtidSet] = {rs: GtidSet() for rs in sets}
        self.stalled: dict[str, bool] = {}
        self._runners: list[web.AppRunner] = []
        self.urls: dict[str, str] = {}
        self._task: asyncio.Task | None = None
        self.closing = False
        self.log: list[tuple[float, str, str]] = []

    # ------------------------------------------------------------- topology helpers
    def setup_replication(self, rs: str, primary: str | None = None) -> None:
        """Make ``primary`` (default the first node) writable and the rest its replicas."""
        names = self.sets[rs]
        primary = primary or names[0]
        for n in names:
            node = self.nodes[n]
            if n == primary:
                node.super_read_only = False
                node.semisync_source = self.semisync
            else:
                node.source = primary
                node.semisync_replica = self.semisync

    def partition(self, a: str, b: str) -> None:
        """Cut the link between two nodes."""
        self.cut.add(frozenset((a, b)))

    def heal(self) -> None:
        """Remove every cut, including the manager's."""
        self.cut.clear()
        self.manager_cut.clear()

    def can_talk(self, a: str, b: str) -> bool:
        """True unless the pair is cut."""
        return frozenset((a, b)) not in self.cut

    def kill(self, n: str) -> None:
        """The container is gone, agent and mysqld with it."""
        node = self.nodes[n]
        node.alive = False
        node.mysqld_up = False

    def start(self, n: str) -> None:
        """Container restart: mysqld boots read-only, replication config survives."""
        node = self.nodes[n]
        node.alive = True
        node.mysqld_up = True
        node.hung = node.frozen = False
        node.super_read_only = True
        node.semisync_source = False
        node.retrieved = GtidSet()     # relay_log_recovery discards the relay log

    def primary_of(self, rs: str) -> list[str]:
        """Every writable node of ``rs``, one when all is well."""
        return [n for n in self.sets[rs] if self.nodes[n].writable]

    # ------------------------------------------------------------- simulation
    def io_ok(self, node: FakeNode) -> bool:
        """The node's IO thread can receive from its source now."""
        if not node.responsive or node.source is None or node.io_stopped:
            return False
        src = self.nodes.get(node.source)
        return bool(src and src.responsive and self.can_talk(node.name, src.name))

    def step(self) -> None:
        """Advance time one step. Commit, replicate into relay logs, apply."""
        now = time.time()
        for rs in self.sets:
            for n in self.sets[rs] + [x for x in self.nodes if self.nodes[x].rs == rs
                                      and x not in self.sets[rs]]:
                p = self.nodes[n]
                if not p.writable:
                    continue
                # commit one transaction if the workload is on
                repl = [r for r in self.nodes.values()
                        if r.source == n and self.io_ok(r) and r.semisync_replica
                        and r.receive]
                if self.writing:
                    if p.semisync_source and not repl:
                        self.stalled[n] = True
                    else:
                        self.stalled[n] = False
                        g = GtidSet.of(p.uuid, p.next_gno)
                        p.next_gno += 1
                        p.executed = p.executed | g
                        # AFTER_SYNC: acknowledged only once a replica has it in its relay
                        # log. Replicas copy at the step below, so ack after that.
                        if not p.semisync_source:
                            self.acked[rs] = self.acked[rs] | g
                if not (p.semisync_source and not repl):
                    p.heartbeat_ts = now
        # replication
        for r in self.nodes.values():
            if not self.io_ok(r) or not r.receive:
                continue
            src = self.nodes[r.source]
            new = src.executed - r.executed - r.retrieved
            if new:
                r.retrieved = r.retrieved | new
                if src.semisync_source and r.semisync_replica:
                    self.acked[r.rs] = self.acked[r.rs] | new
            r.heartbeat_ts = src.heartbeat_ts
        for r in self.nodes.values():
            if not r.responsive or r.sql_error:
                continue
            todo = r.retrieved - r.executed - r.partial
            if todo:
                take = []
                for (u, t), ivs in todo.items():
                    for s, e in ivs:
                        take.append((u, t, s, e))
                budget = r.apply_per_step
                add = {}
                for u, t, s, e in take:
                    if budget <= 0:
                        break
                    e2 = min(e, s + budget - 1)
                    add.setdefault((u, t), []).append((s, e2))
                    budget -= e2 - s + 1
                r.executed = r.executed | GtidSet(add)

    async def _run(self, interval: float) -> None:
        while True:
            self.step()
            await asyncio.sleep(interval)

    # ------------------------------------------------------------- agent HTTP
    def status_body(self, node: FakeNode) -> dict:
        """The agent's /status body for ``node``."""
        now = time.time()
        src = self.nodes.get(node.source) if node.source else None
        io = None
        if node.source and node.responsive:
            io = "No" if node.io_stopped else ("Yes" if self.io_ok(node) else "Connecting")
        sql = None
        if node.source and node.responsive:
            sql = "No" if node.sql_error else "Yes"
        clients = sum(1 for r in self.nodes.values()
                      if r.source == node.name and self.io_ok(r) and r.semisync_replica)
        return {
            "node": node.name, "rs": node.rs, "ts": now,
            "mysqld_alive": node.alive and node.mysqld_up,
            "mysqld_responsive": node.responsive,
            "mysqld_pid": 1 if node.mysqld_up else None,
            "fenced": node.fenced,
            "super_read_only": node.super_read_only if node.responsive else None,
            "read_only": node.super_read_only if node.responsive else None,
            "gtid_executed": str(node.executed) if node.responsive else None,
            "replica": {
                "configured": node.source is not None and node.responsive,
                "source_host": node.source if node.responsive else None,
                "io_running": io, "sql_running": sql,
                "seconds_behind_source": 0 if io == "Yes" else None,
                "retrieved_gtid_set": str(node.retrieved) if node.responsive else None,
                "executed_gtid_set": str(node.executed) if node.responsive else None,
                "last_io_error": None if io != "Connecting" else "cannot connect",
                "last_sql_error": node.sql_error,
            },
            "semisync": {"source_enabled": node.semisync_source,
                         "replica_enabled": node.semisync_replica,
                         "source_status": node.semisync_source and clients > 0,
                         "replica_status": node.semisync_replica and io == "Yes",
                         "source_clients": clients, "avg_wait_time_us": 150,
                         "no_tx": 0, "yes_tx": 0},
            "source_reachable": (src is not None and src.alive and not src.frozen and
                                 self.can_talk(node.name, src.name)) if node.source else None,
            "heartbeat": {"ts": node.heartbeat_ts,
                          "age_s": (now - node.heartbeat_ts) if node.heartbeat_ts else None,
                          "writer": None},
            "agent_uptime_s": 1.0, "agent_version": "fake",
        }

    def _app(self, node: FakeNode) -> web.Application:
        fleet = self

        @web.middleware
        async def faults(request, handler):
            while node.frozen and not fleet.closing:
                await asyncio.sleep(0.05)
            if not node.alive:
                request.transport.close()
                raise web.HTTPServiceUnavailable()
            node.calls.append(request.path)
            if node.fail_next.get(request.path, 0) > 0:
                node.fail_next[request.path] -= 1
                return web.json_response({"error": "(2013, 'Lost connection to MySQL server "
                                          "during query')", "sql_code": 2013,
                                          "timeout": False}, status=500)
            return await handler(request)

        async def status(request):
            if node.status_delay:
                await asyncio.sleep(node.status_delay)
            return web.json_response(fleet.status_body(node))

        async def fence(request):
            if node.fence_delay:
                await asyncio.sleep(node.fence_delay)
            node.fenced = True
            fleet.log.append((time.time(), node.name, "fenced"))
            method = "sql"
            if node.hung or not node.mysqld_up:
                await asyncio.sleep(0.05)
                method = "kill"
                node.hung = False
                node.mysqld_up = True
            node.super_read_only = True
            return web.json_response({"fenced": True, "method": method,
                                      "gtid_executed": str(node.executed),
                                      "duration_ms": 1, "killed_threads": 0})

        async def unfence(request):
            node.fenced = False
            return web.json_response({"fenced": False})

        async def promote(request):
            if not node.responsive:
                return web.json_response({"error": "mysqld down"}, status=503)
            node.source = None
            node.retrieved = GtidSet()
            node.partial = GtidSet()
            node.io_stopped = False
            node.semisync_replica = False
            node.semisync_source = fleet.semisync
            node.super_read_only = False
            node.fenced = False
            fleet.log.append((time.time(), node.name, "promoted"))
            return web.json_response({"gtid_executed": str(node.executed), "duration_ms": 1})

        async def repoint(request):
            body = await request.json()
            if node.repoint_delay:
                await asyncio.sleep(node.repoint_delay)
            if not node.responsive:
                return web.json_response({"error": "mysqld down"}, status=503)
            node.super_read_only = True
            node.semisync_source = False
            node.semisync_replica = fleet.semisync
            node.source = body["source"]
            fleet.log.append((time.time(), node.name, "repointed"))
            node.retrieved = GtidSet()     # CHANGE REPLICATION SOURCE purges relay logs
            node.partial = GtidSet()
            node.io_stopped = False
            node.sql_error = None
            return web.json_response({"ok": True, "duration_ms": 1})

        async def rebuild(request):
            body = await request.json()
            donor = fleet.nodes[body["donor"]]
            phantom = (node.executed - donor.executed).count()
            await asyncio.sleep(0.05)
            node.executed = donor.executed
            node.retrieved = GtidSet()
            node.source = None
            node.super_read_only = True
            node.sql_error = None
            return web.json_response({"ok": True, "phantom_gtids": phantom, "duration_ms": 50,
                                      "bytes": 1_000_000, "gtid_executed": str(node.executed)})

        async def health(request):
            return web.json_response({"ok": True})

        async def primary(request):
            if node.writable:
                return web.json_response({"role": "primary"})
            return web.json_response({"role": "replica"}, status=503)

        app = web.Application(middlewares=[faults])
        app.add_routes([
            web.get("/status", status), web.get("/primary", primary),
            web.get("/health", health),
            web.post("/fence", fence), web.post("/unfence", unfence),
            web.post("/promote", promote), web.post("/repoint", repoint),
            web.post("/rebuild", rebuild),
        ])
        return app

    async def start_agents(self, step_interval: float = 0.02) -> dict[str, str]:
        """Serve one fake agent per node on a free port and start the clock. Returns the URLs."""
        for n, node in self.nodes.items():
            runner = web.AppRunner(self._app(node), access_log=None)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            port = site._server.sockets[0].getsockname()[1]
            self.urls[n] = f"http://127.0.0.1:{port}"
            self._runners.append(runner)
        self._task = asyncio.create_task(self._run(step_interval))
        return self.urls

    async def close(self) -> None:
        """Stop the clock and every fake agent."""
        self.closing = True
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        for r in self._runners:
            await r.cleanup()

    def overrides(self) -> dict[str, tuple[str, int, str]]:
        """Addressing overrides pointing the manager at the fake agents."""
        return {n: ("127.0.0.1", 0, u) for n, u in self.urls.items()}


class FakeProber:
    """The manager's SELECT 1 + heartbeat write, against the simulation."""

    def __init__(self, fleet: FakeFleet, timeout_s: float):
        self.fleet = fleet
        self.timeout_s = timeout_s
        self.count = itertools.count()
        self.stopped_io: list[str] = []

    async def probe(self, rs: str, node: str) -> ProbeResult:
        """Probe the simulated primary as MysqlProber would."""
        ts = time.time()
        n = self.fleet.nodes[node]
        if node in self.fleet.manager_cut or n.hung or n.frozen:
            await asyncio.sleep(self.timeout_s)
            return ProbeResult(ok=False, ts=ts, duration_s=self.timeout_s, kind="timeout",
                               error="no answer")
        if not n.alive or not n.mysqld_up:
            return ProbeResult(ok=False, ts=ts, duration_s=0.001, kind="connect",
                               error="connection refused")
        if n.super_read_only or n.fenced:
            return ProbeResult(ok=False, ts=ts, duration_s=0.001, select_ok=True,
                               kind="readonly", error="1290 super-read-only")
        repl = [r for r in self.fleet.nodes.values()
                if r.source == node and self.fleet.io_ok(r) and r.semisync_replica]
        if n.semisync_source and not repl:
            await asyncio.sleep(self.timeout_s)
            return ProbeResult(ok=False, ts=ts, duration_s=self.timeout_s, select_ok=True,
                               kind="timeout", error="SELECT 1 ok but heartbeat write blocked")
        return ProbeResult(ok=True, ts=ts, duration_s=0.001, select_ok=True)

    async def stop_io(self, node: str, timeout: float = 3.0) -> str | None:
        """Stop the node's IO thread."""
        n = self.fleet.nodes[node]
        if not n.responsive:
            return "not responsive"
        self.stopped_io.append(node)
        n.io_stopped = True
        return None

    async def gtid_waiter(self, node: str, timeout: float = 3.0):
        """A waiter on the simulated node, None when it is not responsive."""
        n = self.fleet.nodes[node]
        if not n.responsive:
            return None
        return FakeGtidWaiter(n)

    async def replica_state(self, node: str, timeout: float = 3.0) -> dict | None:
        """The SHOW REPLICA STATUS fields failover reads."""
        n = self.fleet.nodes[node]
        if not n.responsive:
            return None
        if n.source is None:
            return {}
        done = (n.retrieved - n.executed).is_subset(n.partial)
        return {"Replica_SQL_Running_State":
                "Replica has read all relay log; waiting for more updates" if done
                else "Waiting for dependent transaction to commit",
                "Retrieved_Gtid_Set": str(n.retrieved), "Executed_Gtid_Set": str(n.executed)}

    async def close(self) -> None:
        """Nothing to release."""


class FakeGtidWaiter:
    """WAIT_FOR_EXECUTED_GTID_SET against a simulated node."""

    def __init__(self, node: FakeNode):
        self.node = node

    async def wait(self, gtid_set: str, timeout_s: float) -> tuple[bool, str | None]:
        """Poll the node until it executed ``gtid_set`` or the timeout passed."""
        target = GtidSet.parse(gtid_set)
        end = time.monotonic() + timeout_s
        while True:
            if target.is_subset(self.node.executed):
                return True, str(self.node.executed)
            if time.monotonic() >= end:
                return False, str(self.node.executed)
            await asyncio.sleep(0.005)

    async def close(self) -> None:
        """Nothing to release."""
