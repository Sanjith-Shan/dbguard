"""Automatic failover, the six steps of the spec, each timed into the Event.

dbguard mode: fence, choose (subset check), catch up, promote, repoint, record.
naive mode:   no fence, choose by largest Executed_Gtid_Set, no subset check, no catch-up
              wait, promote, repoint, record. It exists so the numbers can show why the
              other steps are there.
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
from dbguard.manager.client import AgentError
from dbguard.manager.detector import Verdict
from dbguard.manager.model import State
from dbguard.manager.selection import choose

if TYPE_CHECKING:
    from dbguard.manager.controller import SetController

log = structlog.get_logger("dbguard.manager.failover")

ROLE_CHANGE_TIMEOUT_S = 30.0


async def fence(ctl: SetController, node: str) -> tuple[FenceStep, dict | None]:
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


async def wait_caught_up(ctl: SetController, node: str, deadline_s: float,
                         target=None) -> tuple[bool, str]:
    """Poll until the node applied its whole relay log (and holds ``target`` if given)."""
    end = time.monotonic() + deadline_s
    why = "no status"
    while True:
        nv = await ctl.agents.status(node, timeout=1.0)
        if nv.usable:
            applied = nv.gtid_executed | nv.replica.executed
            if nv.replica.last_sql_error:
                return False, f"SQL thread error on {node}: {nv.replica.last_sql_error}"
            if nv.relay_applied and (target is None or target.is_subset(applied)):
                return True, "ok"
            lag = (nv.replica.retrieved - applied).count()
            miss = (target - applied).count() if target is not None else 0
            why = f"{node} still has {lag} relay-log transactions unapplied" + (
                f" and lacks {miss} of the old primary's" if miss else "")
        else:
            why = f"{node} not responsive ({nv.error or 'mysqld down'})"
        if time.monotonic() >= end:
            return False, why
        await asyncio.sleep(min(0.1, max(0.02, ctl.cfg.poll_interval_s / 5)))


async def repoint_all(ctl: SetController, nodes: list[str], source: str) -> RepointStep:
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
    rs, mode, old = ctl.rs, ctl.mode, ctl.primary
    t0 = time.monotonic()
    ctl.st.to(State.FAILING_OVER, emit=False)
    log.warning("failover start", rs=rs, old_primary=old, trigger=v.trigger, mode=mode,
                probe_streak=v.probe_streak, votes=v.replica_votes, reasons=v.reasons)
    detect = Detect(manager_probe_failed=v.probe_failed, replica_votes=v.replica_votes,
                    replica_total=v.replica_total, duration_s=round(v.detect_s, 3))
    steps = Steps()
    notes: list[str] = []
    m = ctl.metrics
    if m:
        m.step_seconds["detect"].labels(rs=rs).observe(v.detect_s)

    def finish(new_primary, outcome, watermark=None, halt_reason=None):
        total = round(time.monotonic() - t0, 3)
        if halt_reason:
            notes.append(f"halted: {halt_reason}")
        ev = ctl.event(type="failover", old_primary=old, new_primary=new_primary,
                       trigger=v.trigger, detect=detect, steps=steps, total_s=total,
                       watermark_gtid=watermark, note="; ".join(notes) or None)
        if m:
            m.failovers.labels(rs=rs, outcome=outcome, trigger=v.trigger or "dead").inc()
            if outcome == "ok":
                m.step_seconds["failover_total"].labels(rs=rs).observe(total)
        if halt_reason:
            ctl.st.halt(halt_reason)
        log.warning("failover end", rs=rs, outcome=outcome, new_primary=new_primary,
                    total_s=total)
        return ev

    # 1. fence ---------------------------------------------------------------------
    fence_resp = None
    if mode == "dbguard":
        steps.fence, fence_resp = await fence(ctl, old)
        if steps.fence.outcome == "unreachable":
            notes.append(f"{old} agent unreachable, not fenced, relying on semi-sync")
    else:
        steps.fence = FenceStep(duration_s=0.0, outcome="skipped")

    # 2. choose --------------------------------------------------------------------
    tc = time.monotonic()
    others = [n for n in ctl.members if n != old]
    views = await ctl.fresh_views(others)
    choice = choose([views[n] for n in others], mode=mode, old_primary=old)
    steps.choose = ChooseStep(duration_s=round(time.monotonic() - tc, 3),
                              candidates=choice.candidates, winner=choice.winner,
                              subset_ok=choice.subset_ok)
    if choice.excluded:
        notes.append("not candidates: " + "; ".join(f"{n} {w}" for n, w in
                                                     choice.excluded.items()))
    if fence_resp and fence_resp.get("gtid_executed") and choice.winner:
        from dbguard.gtid import GtidSet
        extra = GtidSet.parse(fence_resp["gtid_executed"]) - views[choice.winner].have
        if extra:
            notes.append(f"old primary holds {extra.count()} unacknowledged transactions "
                         f"the winner lacks, they will be discarded at rejoin")
    if choice.halt_reason:
        finish(None, "halted", halt_reason=choice.halt_reason)
        return
    winner = choice.winner
    assert winner is not None

    # 3. catch up ------------------------------------------------------------------
    if mode == "dbguard":
        tk = time.monotonic()
        ok, why = await wait_caught_up(ctl, winner, ctl.cfg.catchup_deadline_s)
        steps.catchup = CatchupStep(duration_s=round(time.monotonic() - tk, 3), ok=ok)
        if not ok:
            finish(None, "halted", halt_reason=f"catch-up deadline "
                   f"{ctl.cfg.catchup_deadline_s:.0f} s passed, {why}")
            return
    else:
        steps.catchup = CatchupStep(duration_s=0.0, ok=True)

    # 4. promote -------------------------------------------------------------------
    tp = time.monotonic()
    try:
        resp = await ctl.agents.post(winner, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
    except AgentError as e:
        steps.promote = PromoteStep(duration_s=round(time.monotonic() - tp, 3))
        finish(None, "halted", halt_reason=f"promote of {winner} failed: {e}")
        return
    steps.promote = PromoteStep(duration_s=round(time.monotonic() - tp, 3))
    if m:
        m.step_seconds["promote"].labels(rs=rs).observe(steps.promote.duration_s)
    watermark = resp.get("gtid_executed") if isinstance(resp, dict) else None
    ctl.set_primary(winner)

    # 5. repoint -------------------------------------------------------------------
    targets = [n for n in others if n != winner and views[n].usable]
    steps.repoint = await repoint_all(ctl, targets, winner)
    failed = sorted(set(targets) - set(steps.repoint.nodes))
    if failed:
        notes.append(f"repoint failed on {', '.join(failed)}, will retry")

    # 6. record --------------------------------------------------------------------
    ctl.st.failovers_total += 1
    ctl.st.cooldown_until = time.time() + ctl.cfg.cooldown_s
    ctl.unhealthy_since = time.time()
    finish(winner, "ok", watermark=watermark)
    ctl.st.to(State.DEGRADED, note=f"failed over from {old} to {winner}, {old} is not a "
              f"replica yet")
