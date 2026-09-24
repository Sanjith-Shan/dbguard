"""Planned switchover, the graceful path.

Quiesce the old primary (POST /fence: super_read_only=1 and client threads killed, the
same operation as a fence), wait until the candidate has applied every transaction the
old primary committed, promote it, repoint everyone else including the old primary. No
acknowledged write can be lost because the old primary stops accepting writes before the
candidate is compared with it. If the candidate does not catch up in time the old
primary is promoted back and nothing changed.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import structlog

from dbguard.events import CatchupStep, ChooseStep, PromoteStep, Rejoin, RepointStep, Steps
from dbguard.gtid import GtidSet
from dbguard.manager.client import AgentError
from dbguard.manager.failover import ROLE_CHANGE_TIMEOUT_S, fence, repoint_all, wait_caught_up
from dbguard.manager.model import State
from dbguard.manager.selection import candidate_problem, choose

if TYPE_CHECKING:
    from dbguard.events import Event
    from dbguard.manager.controller import SetController

log = structlog.get_logger("dbguard.manager.switchover")


class SwitchoverError(Exception):
    pass


async def switchover(ctl: SetController, to: str | None = None) -> Event:
    async with ctl.lock:
        return await _switchover(ctl, to)


async def _switchover(ctl: SetController, to: str | None) -> Event:
    if ctl.st.halted:
        raise SwitchoverError(f"{ctl.rs} is HALTED ({ctl.st.halt_reason}), resume it first")
    old = ctl.primary
    if old is None:
        raise SwitchoverError(f"{ctl.rs} has no primary")
    if to == old:
        raise SwitchoverError(f"{to} is already the primary")
    t0 = time.monotonic()
    views = await ctl.fresh_views()
    pv = views.get(old)
    if pv is None or not pv.writable:
        raise SwitchoverError(f"primary {old} is not writable, this is a failover, not a "
                              f"switchover")
    others = [n for n in ctl.members if n != old]
    tc = time.monotonic()
    if to is None:
        ch = choose([views[n] for n in others], mode="dbguard", old_primary=old)
        if ch.winner is None:
            raise SwitchoverError(ch.halt_reason or "no candidate")
        to = ch.winner
        cands = ch.candidates
    else:
        if to not in others:
            raise SwitchoverError(f"{to} is not a member of {ctl.rs}")
        why = candidate_problem(views[to], "dbguard", old)
        if why:
            raise SwitchoverError(f"{to} cannot be promoted: {why}")
        # No errant-transaction check here: the two snapshots were read at different
        # instants while writes flow, so the replica can look ahead of the primary. The
        # check runs after the quiesce, against the old primary's final set.
        cands = [to]
    steps = Steps(choose=ChooseStep(duration_s=round(time.monotonic() - tc, 3),
                                    candidates=cands, winner=to, subset_ok=True))
    ctl.st.to(State.FAILING_OVER, emit=False)
    notes: list[str] = []

    # --- before the stall ---------------------------------------------------------------
    # Move every other replica under the candidate while the old primary still takes
    # writes. The candidate is an ordinary replica with log_replica_updates, so they keep
    # receiving everything through it, and when it is promoted its semi-sync replicas are
    # already attached. The old primary's own repoint (a START REPLICA that took up to 2.4 s
    # on the real fleet) then happens after the stall instead of inside it.
    movers = [n for n in ctl.members if n not in (old, to) and views[n].usable
              and views[n].replica.configured]
    attached: list[str] = []
    if movers:
        steps.prepare = await repoint_all(ctl, movers, to)
        attached = await wait_attached(ctl, steps.prepare.nodes, to, deadline_s=5.0)
    # Pre-open a connection to the candidate and let it get close to the primary first, so
    # the catch-up inside the stall is only the last few transactions.
    waiter = None
    if ctl.prober is not None and hasattr(ctl.prober, "gtid_waiter"):
        waiter = await ctl.prober.gtid_waiter(to)
    try:
        if waiter is not None:
            try:
                await waiter.wait(str(pv.gtid_executed), min(ctl.cfg.catchup_deadline_s, 10.0))
            except Exception:  # noqa: BLE001
                waiter = None
        return await _stall(ctl, old, to, views, steps, notes, attached, waiter, t0)
    finally:
        if waiter is not None:
            await waiter.close()


async def wait_attached(ctl: SetController, nodes: list[str], source: str,
                        deadline_s: float) -> list[str]:
    """Wait (polling every 50 ms) until these replicas stream from ``source``."""
    end = time.monotonic() + deadline_s
    while True:
        views = await ctl.fresh_views(nodes)
        ok = [n for n in nodes if views[n].usable and views[n].replica.source_host == source
              and views[n].replica.io_running == "Yes"]
        if len(ok) == len(nodes) or time.monotonic() >= end:
            return ok
        await asyncio.sleep(0.05)


async def _stall(ctl: SetController, old: str, to: str, views, steps: Steps, notes: list[str],
                 attached: list[str], waiter, t0: float) -> Event:
    # --- the stall: quiesce, catch up, promote -------------------------------------------
    ts = time.monotonic()
    steps.fence, resp = await fence(ctl, old)
    if steps.fence.outcome == "unreachable":
        ctl.st.to(State.HEALTHY, emit=False)
        raise SwitchoverError(f"could not quiesce {old}, nothing changed")
    # An empty string is a real answer (a primary with no transactions yet), not a missing
    # one. Treating "" as unknown skipped the errant check entirely.
    final = GtidSet.parse(resp["gtid_executed"]) \
        if resp and resp.get("gtid_executed") is not None else None

    tk = time.monotonic()
    ok, why = False, "not checked"
    if waiter is not None and final is not None:
        try:
            ok, got = await waiter.wait(str(final), ctl.cfg.catchup_deadline_s)
            why = "ok" if ok else f"{to} did not apply {old}'s final set in time"
            if ok and got is not None:
                errant = GtidSet.parse(got) - final
                if errant:
                    ok, why = False, (f"{to} holds {errant.count()} transactions {old} never "
                                      f"had ({errant})")
        except Exception as e:  # noqa: BLE001
            waiter, why = None, f"waiter failed: {e}"
    if waiter is None or final is None:
        ok, why = await wait_caught_up(ctl, to, ctl.cfg.catchup_deadline_s, target=final)
        if ok and final is not None:
            tv = await ctl.agents.status(to)
            errant = tv.have - final if tv.usable else None
            if errant:
                ok, why = False, (f"{to} holds {errant.count()} transactions {old} never "
                                  f"had ({errant})")
    steps.catchup = CatchupStep(duration_s=round(time.monotonic() - tk, 3), ok=ok)
    if not ok:
        note = f"{to} not promotable after quiesce ({why}), promoted {old} back"
        try:
            await ctl.agents.post(old, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
        except AgentError as e:
            ctl.st.halt(f"switchover aborted and {old} could not be made writable again: {e}")
            raise SwitchoverError(ctl.st.halt_reason)
        ctl.st.to(State.HEALTHY, emit=False)
        ctl.event(type="switchover", old_primary=old, new_primary=old, trigger="planned",
                  steps=steps, total_s=round(time.monotonic() - t0, 3),
                  stall_s=round(time.monotonic() - ts, 3), note=note)
        raise SwitchoverError(note)

    old_repointed = False
    if not attached:
        # No other replica streams from the candidate (a two-node set, or the move failed):
        # the old primary is its only possible semi-sync replica, so repoint it inside the
        # stall, before the promote, or the first commit would wait for it anyway.
        steps.repoint = await repoint_all(ctl, [old], to)
        old_repointed = old in steps.repoint.nodes
        notes.append("no replica attached to the candidate before the quiesce, repointed "
                     f"{old} inside the stall")

    tp = time.monotonic()
    try:
        presp = await ctl.agents.post(to, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
    except AgentError as e:
        ctl.st.halt(f"switchover: promote of {to} failed after {old} was quiesced: {e}")
        raise SwitchoverError(ctl.st.halt_reason)
    steps.promote = PromoteStep(duration_s=round(time.monotonic() - tp, 3))
    stall = round(time.monotonic() - ts, 3)
    ctl.set_primary(to)

    # --- after the stall ------------------------------------------------------------------
    if not old_repointed:
        steps.repoint = RepointStep(duration_s=0.0, nodes=[])
        notes.append(f"{old} is repointed to {to} in the background after the stall")
        ctl.spawn_node_task(old, _repoint_after(ctl, old, to))
    ev = ctl.event(type="switchover", old_primary=old, new_primary=to, trigger="planned",
                   steps=steps, total_s=round(time.monotonic() - t0, 3), stall_s=stall,
                   watermark_gtid=presp.get("gtid_executed") if isinstance(presp, dict)
                   else None, note="; ".join(notes) or None)
    ctl.unhealthy_since = time.time()
    ctl.st.to(State.HEALTHY, emit=False)
    log.info("switchover done", rs=ctl.rs, old=old, new=to, stall_s=stall,
             total_s=ev.total_s)
    return ev


async def _repoint_after(ctl: SetController, old: str, to: str) -> None:
    t0 = time.monotonic()
    step = await repoint_all(ctl, [old], to)
    ok = old in step.nodes
    ctl.event(type="rejoin", old_primary=old, new_primary=to,
              rejoin=Rejoin(branch="repoint" if ok else "none", node=old,
                            duration_s=round(time.monotonic() - t0, 3)),
              note=(f"former primary {old} repointed to {to} after the switchover" if ok else
                    f"repoint of former primary {old} after the switchover failed, the "
                    f"rejoin pass will retry"))
