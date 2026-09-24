"""Finding the primary when the manager starts, before any failover can happen.

Exactly one writable member is the primary, and more than one HALTs the set. With none,
members that are all read-only, unreplicated and identical are a new set, bootstrapped by
promoting ``nodes[0]``. Otherwise, if every node booted read-only (a whole-set restart, since
my.cnf boots read-only) and one member holds every transaction any other holds, it is promoted
in place as a ``cold_start``. That rule exists because rs2 once failed over from a primary that
was merely rebooting. Replicas agreeing on a source that is not answering yet make it the
believed primary, and detection decides from there.
"""

from __future__ import annotations

import collections
import time
from typing import TYPE_CHECKING

import structlog

from dbguard.gtid import GtidSet
from dbguard.manager.client import AgentError
from dbguard.manager.failover import ROLE_CHANGE_TIMEOUT_S, repoint_all
from dbguard.manager.model import NodeView, Observation

if TYPE_CHECKING:
    from dbguard.manager.controller import SetController

log = structlog.get_logger("dbguard.manager.bootstrap")


async def discover(ctl: SetController, ob: Observation) -> None:
    """Called each tick while the set has no primary. Settles on one, or waits."""
    views = {n: ob.nodes[n] for n in ctl.members if n in ob.nodes}
    writable = [n for n, v in views.items() if v.writable]
    if len(writable) == 1:
        ctl.set_primary(writable[0])
        log.info("primary discovered", rs=ctl.rs, primary=writable[0])
        return
    if len(writable) > 1:
        ctl.st.halt(f"more than one writable node: {', '.join(writable)}; decide which "
                    f"writes to keep, fence the others, then resume")
        return
    usable = [n for n, v in views.items() if v.usable]
    if len(usable) == len(ctl.members):
        vs = [views[n] for n in ctl.members]
        fresh = all(v.super_read_only is not False and not v.replica.configured for v in vs)
        same = len({v.gtid_executed for v in vs}) == 1
        if fresh and same:
            await bootstrap(ctl)
            return
    sources = collections.Counter(
        v.replica.source_host for v in views.values()
        if v.usable and v.replica.configured and v.replica.source_host in ctl.members)
    src = None
    if sources:
        s0, n0 = sources.most_common(1)[0]
        if n0 * 2 > len(ctl.members) - 1:
            src = s0
    # With a source the replicas agree on, only that source can be the cold-start node
    # until it answers (it may simply be booting last). Without one, every node must answer.
    node, halt = cold_start_choice(views, ctl.members, src)
    if halt:
        ctl.st.halt(halt)
        return
    if node:
        await cold_start(ctl, node, views)
        return
    if src is not None:
        # Not (yet) a cold start, typically the source is not answering. Believe it; the
        # tick re-checks for a cold start every poll and detection handles a dead one.
        ctl.set_primary(src)
        log.warning("no writable node, believing replicas' source", rs=ctl.rs, primary=src)
        return
    note = (f"no primary found yet: {len(usable)} of {len(ctl.members)} nodes answer, "
            f"none writable")
    if note != ctl.last_discover_note:
        ctl.last_discover_note = note
        log.warning("discover", rs=ctl.rs, note=note)


def cold_start_choice(views: dict[str, NodeView], members: list[str],
                      believed: str | None) -> tuple[str | None, str | None]:
    """Is this a whole-set restart, and whom to promote in place?

    Returns (node, None) to promote ``node``, (None, reason) to HALT, or (None, None) when
    this is not a cold start. A cold start is: no member is writable, and the believed
    primary (when there is one) answers, is not fenced, and is read-only. That is what every
    node looks like after the whole set rebooted, because my.cnf boots read-only. There is
    no primary to lose, so replica votes (a heartbeat row minutes old, IO threads still
    Connecting to a source that is itself booting) do not matter and are not consulted.

    The node promoted is the believed primary if its gtid_executed holds everything any
    reachable member holds. Otherwise the reachable, unfenced, read-only member with the
    largest executed set that holds everything the others hold. If no member holds
    everything, the sets diverged and a human decides.
    """
    reach = {m: views[m] for m in members if m in views and views[m].usable}
    if any(v.writable for v in reach.values()):
        return None, None
    if believed is not None:
        bv = reach.get(believed)
        if bv is None or bv.fenced or bv.super_read_only is not True:
            return None, None       # a primary that is down or fenced is a failover case
    elif not any(v.replica.configured for v in reach.values()) or len(reach) < len(members):
        # A brand new set (bootstrap), or replicas that do not agree on a source and not
        # every node answering yet: wait, the missing node may hold the newest data.
        return None, None
    everything = GtidSet()
    for v in reach.values():
        everything = everything | v.have
    cands = [m for m, v in reach.items() if not v.fenced and v.super_read_only is True]
    if believed in cands and everything.is_subset(reach[believed].gtid_executed):
        return believed, None
    full = [m for m in cands if everything.is_subset(reach[m].gtid_executed)]
    if full:
        return max(full, key=lambda m: (reach[m].gtid_executed.count(),
                                        -members.index(m))), None
    detail = "; ".join(f"{m} lacks {(everything - reach[m].gtid_executed).count()}"
                       for m in cands) or "no unfenced read-only member"
    return None, (f"cold start: every node is read-only but no node holds every "
                  f"transaction the others hold ({detail}); decide which data to keep")


async def cold_start(ctl: SetController, src: str, views: dict[str, NodeView]) -> None:
    """Promote ``src`` in place after a whole-set restart and record a cold_start event."""
    t0 = time.monotonic()
    log.warning("cold start, promoting in place", rs=ctl.rs, primary=src)
    # Replicas that do not already follow ``src`` (it was not their source before the
    # restart) are repointed first, so it has its semi-sync replicas when it becomes
    # writable. ``src`` holds everything they hold, so this is safe.
    movers = [m for m in ctl.members if m != src and m in views and views[m].usable
              and views[m].replica.source_host != src]
    if movers:
        await repoint_all(ctl, movers, src)
    try:
        resp = await ctl.agents.post(src, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
    except AgentError as e:
        ctl.set_primary(src)
        log.warning("cold start promote failed, letting detection decide", rs=ctl.rs,
                    primary=src, error=str(e))
        return
    reps = [n for n, v in views.items() if n != src and v.usable]
    old = ctl.primary
    ctl.set_primary(src)
    ctl.event(type="cold_start", old_primary=old or src, new_primary=src,
              total_s=round(time.monotonic() - t0, 3),
              watermark_gtid=resp.get("gtid_executed") if isinstance(resp, dict) else None,
              note=f"no node was writable (whole-set restart), {src} holds every transaction "
                   f"of {', '.join(reps) or 'no other reachable node'}, promoted it in place")
    # HEALTHY or DEGRADED is decided by the next reconcile, on fresh views


async def bootstrap(ctl: SetController) -> None:
    """Promote ``nodes[0]`` of a brand new set and repoint the rest to it."""
    first, rest = ctl.scfg.nodes[0], [n for n in ctl.members if n != ctl.scfg.nodes[0]]
    t0 = time.monotonic()
    log.warning("bootstrap", rs=ctl.rs, primary=first, replicas=rest)
    try:
        resp = await ctl.agents.post(first, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
    except AgentError as e:
        ctl.st.halt(f"bootstrap: promote of {first} failed: {e}")
        return
    ctl.set_primary(first)
    step = await repoint_all(ctl, rest, first)
    ctl.event(type="bootstrap", new_primary=first, total_s=round(time.monotonic() - t0, 3),
              watermark_gtid=resp.get("gtid_executed") if isinstance(resp, dict) else None,
              note=f"new set, promoted {first}, repointed {', '.join(step.nodes)}")
    # HEALTHY or DEGRADED is decided by the next reconcile, on fresh views
