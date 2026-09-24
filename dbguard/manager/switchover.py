"""Planned switchover, the graceful path.

Quiesce the old primary (POST /fence: super_read_only=1 and client threads killed, the
same operation as a fence), wait until the candidate has applied every transaction the
old primary committed, promote it, repoint everyone else including the old primary. No
acknowledged write can be lost because the old primary stops accepting writes before the
candidate is compared with it. If the candidate does not catch up in time the old
primary is promoted back and nothing changed.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import structlog

from dbguard.events import CatchupStep, ChooseStep, PromoteStep, Steps
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
    steps.fence, resp = await fence(ctl, old)
    if steps.fence.outcome == "unreachable":
        ctl.st.to(State.HEALTHY, emit=False)
        raise SwitchoverError(f"could not quiesce {old}, nothing changed")
    # An empty string is a real answer (a primary with no transactions yet), not a missing
    # one. Treating "" as unknown skipped the errant check entirely.
    final = GtidSet.parse(resp["gtid_executed"]) \
        if resp and resp.get("gtid_executed") is not None else None

    tk = time.monotonic()
    ok, why = await wait_caught_up(ctl, to, ctl.cfg.catchup_deadline_s, target=final)
    steps.catchup = CatchupStep(duration_s=round(time.monotonic() - tk, 3), ok=ok)
    if ok and final is not None:
        tv = await ctl.agents.status(to)
        errant = tv.have - final if tv.usable else None
        if errant:
            ok, why = False, (f"{to} holds {errant.count()} transactions {old} never had "
                              f"({errant})")
    if not ok:
        note = f"{to} not promotable after quiesce ({why}), promoted {old} back"
        try:
            await ctl.agents.post(old, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
        except AgentError as e:
            ctl.st.halt(f"switchover aborted and {old} could not be made writable again: {e}")
            raise SwitchoverError(ctl.st.halt_reason)
        ctl.st.to(State.HEALTHY, emit=False)
        ev = ctl.event(type="switchover", old_primary=old, new_primary=old, trigger="planned",
                       steps=steps, total_s=round(time.monotonic() - t0, 3), note=note)
        raise SwitchoverError(note)

    # Repoint everyone to the candidate BEFORE promoting it. The candidate is read-only and
    # holds everything the old primary had, so replicas can attach to it now, and when it is
    # promoted its semi-sync replicas are already connected. Promoting first left the new
    # primary waiting up to 3 s for its first ack on the real fleet (docs/BUGS.md).
    targets = [n for n in ctl.members if n != to and views[n].usable]
    steps.repoint = await repoint_all(ctl, targets, to)

    tp = time.monotonic()
    try:
        presp = await ctl.agents.post(to, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
    except AgentError as e:
        ctl.st.halt(f"switchover: promote of {to} failed after {old} was quiesced and the "
                    f"others were repointed to it: {e}")
        raise SwitchoverError(ctl.st.halt_reason)
    steps.promote = PromoteStep(duration_s=round(time.monotonic() - tp, 3))
    ctl.set_primary(to)
    ev = ctl.event(type="switchover", old_primary=old, new_primary=to, trigger="planned",
                   steps=steps, total_s=round(time.monotonic() - t0, 3),
                   watermark_gtid=presp.get("gtid_executed") if isinstance(presp, dict)
                   else None)
    ctl.unhealthy_since = time.time()
    ctl.st.to(State.HEALTHY, emit=False)
    log.info("switchover done", rs=ctl.rs, old=old, new=to, total_s=ev.total_s)
    return ev
