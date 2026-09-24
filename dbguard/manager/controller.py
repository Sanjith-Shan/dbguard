"""One replica set: the poll loop, the verdict, and the actions. One asyncio task per set.

The loop observes (bounded), evaluates the detector, and then either fails over or
reconciles (rejoin, stragglers, degraded, replacement). Slow work (clone rebuilds,
provisioning a spare) runs as a background maintenance task so detection never pauses
for it. Actions that change roles hold ``self.lock`` so an API switchover and an
automatic failover can never interleave.
"""

from __future__ import annotations

import asyncio
import collections
import time
from typing import TYPE_CHECKING

import structlog

from dbguard.events import Event, EventLog
from dbguard.gtid import GtidSet
from dbguard.manager.client import AgentClient
from dbguard.manager.detector import DetectParams, Verdict, evaluate
from dbguard.manager.model import NodeView, Observation, State
from dbguard.manager.client import STATUS_TIMEOUT_S
from dbguard.manager.probe import Prober, StatusCache, observe
from dbguard.manager.state import SetState

if TYPE_CHECKING:
    from dbguard.manager.config import FleetConfig, SetConfig
    from dbguard.manager.metrics import Metrics
    from dbguard.manager.replacement import Provisioner

log = structlog.get_logger("dbguard.manager")

ACTION_BACKOFF_S = 10.0     # do not repeat a rejoin action on the same node sooner
DEGRADED_GRACE_S = 2.0      # a replica may be briefly not-Yes right after a repoint


class SetController:
    def __init__(self, rs: str, cfg: FleetConfig, scfg: SetConfig, agents: AgentClient,
                 prober: Prober | None, events: EventLog, metrics: Metrics | None = None,
                 provisioner: Provisioner | None = None, mode: str | None = None,
                 rejoin: str | None = None):
        self.rs = rs
        self.cfg = cfg
        self.scfg = scfg
        self.mode = mode or cfg.mode
        self.rejoin_mode = rejoin or cfg.rejoin
        self.agents = agents
        self.prober = prober
        self.events = events
        self.metrics = metrics
        self.provisioner = provisioner
        self.members: list[str] = list(scfg.nodes)
        self.st = SetState(rs, self.mode, events, on_change=self._on_state)
        self.lock = asyncio.Lock()
        self.history: collections.deque[Observation] = collections.deque()
        self.last_obs: Observation | None = None
        self.verdict: Verdict | None = None
        self.backoff: dict[str, float] = {}
        self.manual_noted: set[str] = set()
        self.split_noted: set[str] = set()
        self.cold_start_seen = 0
        self.busy: set[str] = set()             # nodes a maintenance task is working on
        self.maint: asyncio.Task | None = None
        self.unhealthy_since: float | None = None
        self.stall_since: float | None = None
        self.replicas_only_logged = False
        self.cooldown_logged = False
        self.last_discover_note: str | None = None
        self._stop = asyncio.Event()
        self.cache = StatusCache(agents, cfg.poll_interval_s, STATUS_TIMEOUT_S)
        self.node_tasks: dict[str, asyncio.Task] = {}   # rejoin repoints and fences
        if metrics:
            metrics.state(rs, self.st.state)

    # ---------------------------------------------------------------- properties
    @property
    def primary(self) -> str | None:
        return self.st.primary

    @property
    def configured_replicas(self) -> int:
        return len(self.scfg.nodes) - 1

    @property
    def params(self) -> DetectParams:
        return DetectParams(mode=self.mode, detect_window_s=self.cfg.detect_window_s,
                            probe_failures=self.cfg.probe_failures,
                            configured_replicas=self.configured_replicas)

    def all_nodes(self) -> list[str]:
        out = list(self.members)
        if self.scfg.spare and self.scfg.spare not in out:
            out.append(self.scfg.spare)
        return out

    def view(self, node: str) -> NodeView | None:
        return self.last_obs.nodes.get(node) if self.last_obs else None

    def _on_state(self, rs: str, old: State, new: State) -> None:
        if self.metrics:
            self.metrics.state(rs, new)

    def set_primary(self, node: str | None) -> None:
        self.cache.invalidate()
        self.st.primary = node
        self.history.clear()
        self.verdict = None

    def event(self, **kw) -> Event:
        self.cache.invalidate()
        kw.setdefault("mode", self.mode)
        return self.events.append(Event(rs=self.rs, **kw))

    # ---------------------------------------------------------------- loop
    async def run(self) -> None:
        interval = self.cfg.poll_interval_s
        while not self._stop.is_set():
            t0 = time.monotonic()
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("tick failed", rs=self.rs, error=str(e))
            dt = time.monotonic() - t0
            try:
                await asyncio.wait_for(self._stop.wait(), max(0.0, interval - dt))
            except TimeoutError:
                pass

    async def stop(self) -> None:
        """Stop polling. Takes no action on the fleet; a running rebuild is abandoned
        (the agent finishes or fails it on its own)."""
        self._stop.set()
        if self.maint and not self.maint.done():
            self.maint.cancel()
        for t in self.node_tasks.values():
            t.cancel()
        await self.cache.close()

    async def poll(self) -> Observation:
        ob = await observe(self.rs, self.primary, self.all_nodes(), self.agents, self.prober,
                           cache=self.cache)
        self.last_obs = ob
        self.history.append(ob)
        keep = max(self.cfg.detect_window_s * 3, 10.0)
        while self.history and ob.ts - self.history[0].ts > keep:
            self.history.popleft()
        if self.metrics:
            self.metrics.observe(self.rs, ob)
        return ob

    async def tick(self) -> None:
        ob = await self.poll()
        if self.st.halted:
            return
        if self.lock.locked():
            return      # a switchover or rejoin from the API is running
        async with self.lock:
            if self.primary is None:
                from dbguard.manager.bootstrap import discover
                await discover(self, ob)
                return
            if await self._cold_start_check(ob):
                return
            v = evaluate(list(self.history), self.members, self.params)
            self.verdict = v
            self._check_stall(ob, ob.nodes.get(self.primary))
            if v.dead:
                if self.st.in_cooldown():
                    if not self.cooldown_logged:
                        self.cooldown_logged = True
                        self.st.to(State.SUSPECT, note=(
                            f"primary {self.primary} looks dead but the set failed over "
                            f"less than {self.cfg.cooldown_s:.0f} s ago, no automatic action"))
                    return
                from dbguard.manager.failover import failover
                await failover(self, v)
                return
            self.cooldown_logged = False
            if v.kind == "SUSPECT":
                self.st.to(State.SUSPECT, note=self._suspect_note(v),
                           detect=_detect(v))
                return
            if v.kind == "REPLICAS_ONLY":
                if not self.replicas_only_logged:
                    self.replicas_only_logged = True
                    log.warning("replicas cannot see primary but manager can", rs=self.rs,
                                primary=self.primary, votes=v.replica_votes)
            else:
                self.replicas_only_logged = False
            await self.reconcile(ob)

    async def _cold_start_check(self, ob: Observation) -> bool:
        """Re-checked every tick, not only at discovery: right after a whole-set restart the
        source may still be booting or a replica may show a transient IO error, and a one
        shot check left both sets SUSPECT for 180 s on the real fleet (docs/BUGS.md). Acts
        after two consecutive polls agree."""
        from dbguard.manager.bootstrap import cold_start, cold_start_choice
        node, halt = cold_start_choice(ob.nodes, self.members, self.primary)
        if halt:
            self.st.halt(halt)
            return True
        if node is None:
            self.cold_start_seen = 0
            return False
        self.cold_start_seen += 1
        if self.cold_start_seen < 2:
            return False
        self.cold_start_seen = 0
        await cold_start(self, node, ob.nodes)
        return True

    def whole_set_restart(self, ob: Observation | None = None) -> bool:
        ob = ob or self.last_obs
        if ob is None or self.primary is None:
            return False
        pv = ob.nodes.get(self.primary)
        return (pv is not None and pv.usable and pv.super_read_only is True and not pv.fenced
                and not any(ob.nodes[m].writable for m in self.members if m in ob.nodes))

    def _suspect_note(self, v: Verdict) -> str:
        return (f"manager probe of {self.primary} failed {v.probe_streak} times "
                f"({v.probe_error}), {v.replica_votes} of {v.replica_total} replicas "
                f"report losing it, no action")

    # ---------------------------------------------------------------- reconcile
    async def reconcile(self, ob: Observation) -> None:
        from dbguard.manager.rejoin import rejoin_actions
        from dbguard.manager.replacement import maybe_replace

        self._adopt_spare(ob)

        await rejoin_actions(self, ob)

        healthy = [n for n in self.members
                   if n != self.primary and ob.nodes.get(n) and
                   ob.nodes[n].replicating_from(self.primary)]
        now = ob.ts
        maint_running = self.maint is not None and not self.maint.done()
        pv = ob.nodes.get(self.primary)
        primary_ok = pv is not None and pv.writable
        if primary_ok and healthy and len(healthy) >= self.configured_replicas:
            self.unhealthy_since = None
            if not maint_running:
                self.st.to(State.HEALTHY, note=f"primary {self.primary}, replicas "
                           f"{', '.join(healthy)} replicating")
        else:
            if self.unhealthy_since is None:
                self.unhealthy_since = now
            if maint_running:
                self.st.to(State.REBUILDING, note=f"rebuilding {', '.join(sorted(self.busy))}")
            elif now - self.unhealthy_since >= DEGRADED_GRACE_S or \
                    self.st.state == State.FAILING_OVER:
                missing = [n for n in self.members if n != self.primary and n not in healthy]
                if not primary_ok:
                    missing.insert(0, f"primary {self.primary} not writable")
                self.st.to(State.DEGRADED, note=(
                    f"{len(healthy)} of {self.configured_replicas} replicas healthy, "
                    f"missing {', '.join(missing) or 'none'}"))
            if self.st.state == State.DEGRADED:
                await maybe_replace(self, ob, healthy)

    def _adopt_spare(self, ob: Observation) -> None:
        """A spare that replicates from the primary was made a member by an earlier
        replacement. Membership lives in memory, so after a manager restart it is found
        again here, otherwise it would be reported as a spare and never repointed."""
        sp = self.scfg.spare
        if sp and sp not in self.members:
            nv = ob.nodes.get(sp)
            if nv is not None and nv.usable and nv.replica.configured and \
                    nv.replica.source_host in self.members:
                self.members.append(sp)
                log.info("spare is a member", rs=self.rs, node=sp,
                         source=nv.replica.source_host)

    def _check_stall(self, ob: Observation, pv: NodeView | None) -> None:
        stalled = False
        why = ""
        if pv is not None and pv.usable and self.mode == "dbguard":
            ss = pv.semisync
            if ss.source_enabled and ss.source_clients == 0:
                stalled = True
                why = "primary is waiting for a replica ack, no semi-sync replica connected"
        if ob.probe is not None and ob.probe.write_stalled:
            stalled = True
            why = why or "manager heartbeat write blocked while SELECT 1 answers"
        if stalled and self.stall_since is None:
            self.stall_since = ob.ts
            self.event(type="stall", old_primary=self.primary, new_primary=self.primary,
                       note=f"writes stalled on {self.primary}: {why}")
        elif not stalled and self.stall_since is not None:
            dur = ob.ts - self.stall_since
            self.stall_since = None
            self.event(type="stall", old_primary=self.primary, new_primary=self.primary,
                       note=f"writes resumed on {self.primary} after {dur:.1f} s")

    # ---------------------------------------------------------------- helpers
    async def fresh_views(self, nodes: list[str] | None = None) -> dict[str, NodeView]:
        nodes = nodes if nodes is not None else self.members
        res = await asyncio.gather(*(self.agents.status(n) for n in nodes))
        return {v.name: v for v in res}

    def start_maintenance(self, coro, nodes: list[str]) -> bool:
        if self.maint is not None and not self.maint.done():
            coro.close()
            return False
        self.busy = set(nodes)

        async def wrapped():
            try:
                await coro
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("maintenance failed", rs=self.rs, nodes=nodes, error=str(e))
            finally:
                self.busy = set()

        self.maint = asyncio.create_task(wrapped(), name=f"maint-{self.rs}")
        return True

    def node_busy(self, node: str) -> bool:
        t = self.node_tasks.get(node)
        return node in self.busy or (t is not None and not t.done())

    def spawn_node_task(self, node: str, coro) -> bool:
        """Run a short per-node action (rejoin repoint, second-writer fence) in the
        background so the poll tick never waits on an agent (REVIEW #10)."""
        if self.node_busy(node):
            coro.close()
            return False

        async def wrapped():
            try:
                await coro
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.exception("node task failed", rs=self.rs, node=node, error=str(e))

        self.node_tasks[node] = asyncio.create_task(wrapped(), name=f"node-{node}")
        return True

    def healthy_replicas(self, views: dict[str, NodeView], exclude=()) -> list[str]:
        return [n for n in self.members
                if n != self.primary and n not in exclude and n in views
                and views[n].replicating_from(self.primary)]

    def status(self) -> dict:
        nodes = {}
        for n in self.all_nodes():
            nv = self.view(n)
            if nv is None:
                continue
            if n == self.scfg.spare and n not in self.members:
                role = "spare"
            elif not nv.usable:
                role = "down"
            elif n == self.primary and nv.writable:
                role = "primary"
            elif nv.fenced and not nv.replica.configured:
                role = "fenced"
            else:
                role = "replica" if nv.super_read_only is not False else "primary"
            nodes[n] = {
                "role": role,
                "reachable": nv.reachable,
                "gtid_executed": str(nv.gtid_executed) if nv.usable else None,
                "lag_s": (float(nv.replica.seconds_behind_source)
                          if nv.replica.seconds_behind_source is not None else None),
                "heartbeat_age_s": nv.heartbeat_age_s,
                "semisync": nv.semisync_word,
                "io_running": nv.replica.io_running,
                "sql_running": nv.replica.sql_running,
            }
        last = self.events.last(self.rs)
        return {
            "rs": self.rs,
            "state": self.st.state.value,
            "halt_reason": self.st.halt_reason,
            "primary": self.primary,
            "since": self.st.since,
            "nodes": nodes,
            "last_event": last.row() if last else None,
            "failovers_total": self.st.failovers_total,
            "cooldown_until": self.st.cooldown_until,
        }


def _detect(v: Verdict):
    from dbguard.events import Detect
    return Detect(manager_probe_failed=v.probe_failed, replica_votes=v.replica_votes,
                  replica_total=v.replica_total, duration_s=round(v.detect_s, 3))


def primary_gtid(views: dict[str, NodeView], primary: str | None) -> GtidSet | None:
    pv = views.get(primary) if primary else None
    return pv.gtid_executed if pv is not None and pv.usable else None
