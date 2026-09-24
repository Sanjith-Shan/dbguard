"""Automatic failover, the steps of docs/DESIGN.md section 6, each timed into the Event.

dbguard mode runs 1 fence, 2 stop IO threads, 3 choose, 4 catch up, 5 repoint, 6 promote,
7 record. The fence runs concurrently with steps 2 to 5 because none of them can make a write
acknowledged, and promote waits for it, so the old primary is fenced or given up on before
anything becomes writable. Repoint precedes promote so the winner's semi-sync replicas are
already attached when it becomes writable. Promoting first stalled every commit until the
first repoint finished. Naive mode skips the fence and catch-up, chooses by executed set alone
and promotes before repointing, so the measurements can show why the other steps exist.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import structlog

from dbguard.events import (
    CatchupStep,
    ChooseStep,
    Detect,
    FenceStep,
    PromoteStep,
    RepointStep,
    Steps,
)
from dbguard.gtid import GtidSet
from dbguard.manager.client import STATUS_TIMEOUT_S, AgentError
from dbguard.manager.detector import Verdict
from dbguard.manager.model import State
from dbguard.manager.selection import choose

if TYPE_CHECKING:
    from dbguard.manager.controller import SetController

log = structlog.get_logger("dbguard.manager.failover")

# The agent bounds a whole role change (/promote, /repoint) to 30 s. The manager waits a
# little longer so it never declares a promote failed while the agent is still finishing
# it (docs/REVIEW_2026-09-23.md #17, INTERFACES.md).
ROLE_CHANGE_TIMEOUT_S = 35.0
SQL_STATE_DONE = "Replica has read all relay log"


async def fence(ctl: SetController, node: str) -> tuple[FenceStep, dict | None]:
    """Ask ``node``'s agent to fence within the fence deadline.

    An unreachable agent is recorded, not retried. Its node is dead or cut off, and with its
    replicas' IO threads stopped and then repointed away it cannot get a write acknowledged."""
    t0 = time.monotonic()
    try:
        resp = await ctl.agents.post(node, "/fence", timeout=ctl.cfg.fence_deadline_s)
        outcome = resp.get("method") if resp.get("method") in ("sql", "kill") else "sql"
    except AgentError as e:
        log.warning("fence: agent unreachable, relying on semi-sync", rs=ctl.rs, node=node,
                    error=str(e))
        resp, outcome = None, "unreachable"
    step = FenceStep(duration_s=round(time.monotonic() - t0, 3), outcome=outcome)
    if ctl.metrics:
        ctl.metrics.step_seconds["fence"].labels(rs=ctl.rs).observe(step.duration_s)
    return step, resp


async def wait_caught_up(ctl: "SetController", node: str, deadline_s: float,
                         target=None) -> tuple[bool, str]:
    """Poll until the node applied its whole relay log (and holds ``target`` if given).

    Caught up means Retrieved_Gtid_Set is a subset of what is applied. When the source died
    mid-send the relay log can end in a partial transaction whose GTID is in the retrieved
    set but can never be applied, so with the IO thread stopped a SQL thread in state
    "Replica has read all relay log" also counts as caught up, and the unapplied GTIDs are
    reported (docs/REVIEW_2026-09-23.md #9).
    """
    end = time.monotonic() + deadline_s
    why = "no status"
    while True:
        nv = await ctl.agents.status(node, timeout=STATUS_TIMEOUT_S)
        if nv.usable:
            applied = nv.gtid_executed | nv.replica.executed
            if nv.replica.last_sql_error:
                return False, f"SQL thread error on {node}: {nv.replica.last_sql_error}"
            if target is not None and not target.is_subset(applied | nv.replica.retrieved):
                miss = (target - applied).count()
                why = f"{node} lacks {miss} of the old primary's transactions"
            elif nv.relay_applied:
                return True, "ok"
            else:
                pending = nv.replica.retrieved - applied
                why = f"{node} still has {pending.count()} relay-log transactions unapplied"
                if nv.replica.io_running != "Yes" and ctl.prober is not None and \
                        hasattr(ctl.prober, "replica_state"):
                    rs = await ctl.prober.replica_state(node)
                    state = (rs or {}).get("Replica_SQL_Running_State") or ""
                    if state.startswith(SQL_STATE_DONE) and (
                            target is None or target.is_subset(applied)):
                        return True, (f"SQL thread read all relay log, {pending.count()} "
                                      f"retrieved but unapplied GTIDs are a partial "
                                      f"transaction ({pending})")
        else:
            why = f"{node} not responsive ({nv.error or 'mysqld down'})"
        if time.monotonic() >= end:
            return False, why
        await asyncio.sleep(min(0.1, max(0.02, ctl.cfg.poll_interval_s / 5)))


async def stop_io_threads(ctl: "SetController", nodes: list[str]) -> dict[str, str | None]:
    """STOP REPLICA IO_THREAD on every candidate before comparing their sets.

    A fence by kill restarts the old primary at once, read-only but still serving its
    binlog, including transactions crash recovery committed that were never acknowledged.
    Replicas with SOURCE_CONNECT_RETRY=1 would reconnect and fetch them while the manager
    compares, so the snapshot the subset check passed on would not hold. Orchestrator stops
    the IO threads for the same reason. /repoint and /promote restart or reset them.
    """
    if ctl.prober is None or not hasattr(ctl.prober, "stop_io"):
        return {}
    res = await asyncio.gather(*(ctl.prober.stop_io(n) for n in nodes),
                               return_exceptions=True)
    return {n: (None if r is None else str(r)) for n, r in zip(nodes, res)}


async def repoint_all(ctl: SetController, nodes: list[str], source: str) -> RepointStep:
    """/repoint every node to ``source`` in parallel. The step lists the nodes that succeeded."""
    t0 = time.monotonic()
    res = await asyncio.gather(
        *(ctl.agents.post(n, "/repoint", {"source": source}, timeout=ROLE_CHANGE_TIMEOUT_S)
          for n in nodes), return_exceptions=True)
    ok = [n for n, r in zip(nodes, res) if not isinstance(r, BaseException)]
    for n, r in zip(nodes, res):
        if isinstance(r, BaseException):
            log.warning("repoint failed", rs=ctl.rs, node=n, error=str(r))
    step = RepointStep(duration_s=round(time.monotonic() - t0, 3), nodes=ok)
    if ctl.metrics:
        ctl.metrics.step_seconds["repoint"].labels(rs=ctl.rs).observe(step.duration_s)
    return step


async def failover(ctl: SetController, v: Verdict) -> None:
    """Fail the set over from its dead primary, following docs/DESIGN.md section 6.

    Ends with a promoted winner and the set DEGRADED until the old primary rejoins, or with
    the set HALTED and a failover event whose ``new_primary`` is null."""
    run = _Failover(ctl, v)
    run.start()
    tc = time.monotonic()  # the choose step's duration has always included steps 1 and 2
    io_stop = await run.fence_and_stop_io()                       # steps 1 and 2
    winner = await run.choose_winner(tc, io_stop)                 # step 3
    if winner is None or not await run.catch_up(winner):          # step 4
        return
    if run.mode == "dbguard":
        await run.repoint(winner)                                 # step 5
    await run.await_fence_before_promote(winner)
    promoted, watermark = await run.promote(winner)               # step 6
    if not promoted:
        return
    if run.mode != "dbguard":
        await run.repoint(winner)                                 # naive: step 5 after 6
    run.record(winner, watermark)                                 # step 7


class _Failover:
    """One failover run, the state its steps share and the event they fill in."""

    def __init__(self, ctl: SetController, v: Verdict):
        self.ctl, self.v = ctl, v
        self.rs, self.mode, self.old = ctl.rs, ctl.mode, ctl.primary
        self.t0 = time.monotonic()
        self.steps = Steps()
        self.notes: list[str] = []
        self.m = ctl.metrics
        self.others = [n for n in ctl.members if n != self.old]
        self.views: dict = {}
        self.fence_task: asyncio.Task | None = None
        self.fence_resp: dict | None = None
        self.detect = Detect(manager_probe_failed=v.probe_failed, replica_votes=v.replica_votes,
                             replica_total=v.replica_total, duration_s=round(v.detect_s, 3))

    def start(self) -> None:
        """Enter FAILING_OVER and log what detection saw."""
        v = self.v
        self.ctl.st.to(State.FAILING_OVER, emit=False)
        log.warning("failover start", rs=self.rs, old_primary=self.old, trigger=v.trigger,
                    mode=self.mode, probe_streak=v.probe_streak, votes=v.replica_votes,
                    reasons=v.reasons)
        if self.m:
            self.m.step_seconds["detect"].labels(rs=self.rs).observe(v.detect_s)

    def finish(self, new_primary, outcome, watermark=None, halt_reason=None):
        """Record the failover event and its metrics, and HALT the set if asked to."""
        total = round(time.monotonic() - self.t0, 3)
        if halt_reason:
            self.notes.append(f"halted: {halt_reason}")
        ev = self.ctl.event(type="failover", old_primary=self.old, new_primary=new_primary,
                            trigger=self.v.trigger, detect=self.detect, steps=self.steps,
                            total_s=total, watermark_gtid=watermark,
                            note="; ".join(self.notes) or None)
        if self.m:
            self.m.failovers.labels(rs=self.rs, outcome=outcome,
                                    trigger=self.v.trigger or "dead").inc()
            if outcome == "ok":
                self.m.step_seconds["failover_total"].labels(rs=self.rs).observe(total)
        if halt_reason:
            self.ctl.st.halt(halt_reason)
        log.warning("failover end", rs=self.rs, outcome=outcome, new_primary=new_primary,
                    total_s=total)
        return ev

    async def fence_and_stop_io(self) -> dict[str, str | None]:
        """Steps 1 and 2. Start the fence in the background, then stop the candidates' IO threads.

        The fence runs concurrently with steps 2 to 5 because none of them can make a write
        acknowledged, and promotion waits for it, so fence-before-promote still holds. Against
        a vanished container the fence costs its whole deadline, which used to sit in front of
        every other step. If an IO thread could not be stopped, that replica might still take
        acked writes from a live old primary, so the fence is awaited before choosing."""
        if self.mode != "dbguard":
            self.steps.fence = FenceStep(duration_s=0.0, outcome="skipped")
            return {}
        self.fence_task = asyncio.create_task(fence(self.ctl, self.old))
        pre = self.ctl.last_obs.nodes if self.ctl.last_obs else {}
        io_stop = await stop_io_threads(
            self.ctl, [n for n in self.others if n in pre and pre[n].usable and
                       pre[n].replica.configured])
        failed_stop = {n: e for n, e in io_stop.items() if e}
        if failed_stop:
            self.notes.append("STOP REPLICA IO_THREAD failed on " + "; ".join(
                f"{n} ({e})" for n, e in failed_stop.items()) + ", waited for the fence")
            await asyncio.wait({self.fence_task})
        return io_stop

    async def await_fence(self) -> None:
        """Collect the fence's outcome, waiting for it to finish or give up."""
        if self.fence_task is not None and self.steps.fence is None:
            self.steps.fence, self.fence_resp = await self.fence_task
            if self.steps.fence.outcome == "unreachable":
                self.notes.append(f"{self.old} agent unreachable, not fenced, "
                                  f"relying on semi-sync")

    async def choose_winner(self, tc: float, io_stop: dict[str, str | None]) -> str | None:
        """Step 3. Choose the candidate holding the most transactions, or HALT on divergence."""
        self.views = await self.ctl.fresh_views(self.others)
        choice = choose([self.views[n] for n in self.others], mode=self.mode,
                        old_primary=self.old)
        self.steps.choose = ChooseStep(
            duration_s=round(time.monotonic() - tc, 3), candidates=choice.candidates,
            winner=choice.winner, subset_ok=choice.subset_ok,
            io_stopped=sorted(n for n, e in io_stop.items() if e is None))
        if choice.excluded:
            self.notes.append("not candidates: " + "; ".join(f"{n} {w}" for n, w in
                                                              choice.excluded.items()))
        if choice.halt_reason:
            await self.await_fence()
            self.finish(None, "halted", halt_reason=choice.halt_reason)
            return None
        assert choice.winner is not None
        return choice.winner

    async def catch_up(self, winner: str) -> bool:
        """Step 4. Wait for the winner to apply its relay log. False means the set HALTed."""
        if self.mode != "dbguard":
            self.steps.catchup = CatchupStep(duration_s=0.0, ok=True)
            return True
        tk = time.monotonic()
        ok, why = await wait_caught_up(self.ctl, winner, self.ctl.cfg.catchup_deadline_s)
        self.steps.catchup = CatchupStep(duration_s=round(time.monotonic() - tk, 3), ok=ok)
        if ok and why != "ok":
            self.notes.append(why)
        if not ok:
            await self.await_fence()
            self.finish(None, "halted", halt_reason=f"catch-up deadline "
                        f"{self.ctl.cfg.catchup_deadline_s:.0f} s passed, {why}")
        return ok

    async def repoint(self, winner: str) -> None:
        """Step 5. Point every other reachable member at the winner.

        In dbguard mode this runs before promotion. The winner is caught up and every other
        candidate's set is a subset of its own, so they can attach while it is read-only, and
        its semi-sync replicas are connected when it becomes writable. Promoting first left
        it unable to acknowledge a commit until the first repoint finished (docs/BUGS.md)."""
        targets = [n for n in self.others if n != winner and self.views[n].usable]
        self.steps.repoint = await repoint_all(self.ctl, targets, winner)
        failed = sorted(set(targets) - set(self.steps.repoint.nodes))
        if failed:
            self.notes.append(f"repoint failed on {', '.join(failed)}, will retry")

    async def await_fence_before_promote(self, winner: str) -> None:
        """Nothing becomes writable until the fence has finished or given up.

        The fenced primary's final gtid_executed also says how many unacknowledged
        transactions it holds that the winner lacks, which rejoin will discard."""
        await self.await_fence()
        if self.fence_resp and self.fence_resp.get("gtid_executed") is not None:
            extra = GtidSet.parse(self.fence_resp["gtid_executed"]) - self.views[winner].have
            if extra:
                self.notes.append(f"old primary holds {extra.count()} unacknowledged "
                                  f"transactions the winner lacks, they will be discarded "
                                  f"at rejoin")

    async def promote(self, winner: str) -> tuple[bool, str | None]:
        """Step 6. Promote the winner through its agent. Returns (ok, watermark GTID set)."""
        tp = time.monotonic()
        try:
            resp = await self.ctl.agents.post(winner, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
        except AgentError as e:
            self.steps.promote = PromoteStep(duration_s=round(time.monotonic() - tp, 3))
            self.finish(None, "halted", halt_reason=f"promote of {winner} failed: {e}")
            return False, None
        self.steps.promote = PromoteStep(duration_s=round(time.monotonic() - tp, 3))
        if self.m:
            self.m.step_seconds["promote"].labels(rs=self.rs).observe(
                self.steps.promote.duration_s)
        watermark = resp.get("gtid_executed") if isinstance(resp, dict) else None
        self.ctl.set_primary(winner)
        return True, watermark

    def record(self, winner: str, watermark: str | None) -> None:
        """Step 7. Count the failover, start the cooldown, write the event, go DEGRADED."""
        st = self.ctl.st
        st.failovers_total += 1
        st.cooldown_until = time.time() + self.ctl.cfg.cooldown_s
        self.ctl.unhealthy_since = time.time()
        self.finish(winner, "ok", watermark=watermark)
        st.to(State.DEGRADED, note=f"failed over from {self.old} to {winner}, {self.old} is "
              f"not a replica yet")
