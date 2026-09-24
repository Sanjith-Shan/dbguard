"""Finding the primary at startup, and bootstrapping a brand new set.

Startup rules, in order:
1. exactly one member is writable (super_read_only=0, not fenced): it is the primary;
2. more than one is writable: HALTED, a human decides which writes to keep;
3. none writable, every member reachable, read-only, not replicating, with empty or
   identical gtid_executed: a new set, so bootstrap it (promote nodes[0], repoint the
   rest) per docs/INTERFACES.md;
4. none writable but replicas agree on a source S. If S is reachable, not fenced, holds
   every transaction any replica holds, and every reachable replica replicates from S
   without an IO error, this is a cold start (the whole set rebooted, my.cnf makes every
   node boot read-only): promote S in place and record a ``cold_start`` event. Failing
   over here would be a false failover, S is fine, only read-only. Otherwise believe S is
   the primary and let detection decide (a manager restarted during an outage);
5. otherwise wait, the fleet may still be starting.
"""

from __future__ import annotations

import collections
import time
from typing import TYPE_CHECKING

import structlog

from dbguard.manager.client import AgentError
from dbguard.manager.failover import ROLE_CHANGE_TIMEOUT_S
from dbguard.manager.model import NodeView, Observation

if TYPE_CHECKING:
    from dbguard.manager.controller import SetController

log = structlog.get_logger("dbguard.manager.bootstrap")


async def discover(ctl: SetController, ob: Observation) -> None:
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
    if sources:
        src, n = sources.most_common(1)[0]
        if n * 2 > len(ctl.members) - 1:
            why = cold_start_problem(views, src)
            if why is None:
                await cold_start(ctl, src, views)
                return
            ctl.set_primary(src)
            log.warning("no writable node, believing replicas' source", rs=ctl.rs, primary=src,
                        not_cold_start=why)
            return
    note = (f"no primary found yet: {len(usable)} of {len(ctl.members)} nodes answer, "
            f"none writable")
    if note != ctl.last_discover_note:
        ctl.last_discover_note = note
        log.warning("discover", rs=ctl.rs, note=note)


def cold_start_problem(views: dict[str, NodeView], src: str) -> str | None:
    """Why ``src`` may not be promoted in place at cold start, or None if it may."""
    sv = views.get(src)
    if sv is None or not sv.usable:
        return f"{src} not responsive"
    if sv.fenced:
        return f"{src} is fenced, someone took it out on purpose"
    if sv.super_read_only is not True:
        return f"{src} super_read_only is {sv.super_read_only}"
    if sv.replica.configured:
        return f"{src} is itself replicating from {sv.replica.source_host}"
    for n, v in views.items():
        if n == src or not v.usable:
            continue
        r = v.replica
        if not r.configured or r.source_host != src:
            return f"{n} does not replicate from {src}"
        if r.io_running != "Yes" and r.last_io_error:
            return f"{n} IO error: {r.last_io_error}"
        extra = v.have - sv.gtid_executed
        if extra:
            return f"{n} holds {extra.count()} transactions {src} lacks"
    return None


async def cold_start(ctl: SetController, src: str, views: dict[str, NodeView]) -> None:
    t0 = time.monotonic()
    log.warning("cold start, promoting in place", rs=ctl.rs, primary=src)
    try:
        resp = await ctl.agents.post(src, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
    except AgentError as e:
        ctl.set_primary(src)
        log.warning("cold start promote failed, letting detection decide", rs=ctl.rs,
                    primary=src, error=str(e))
        return
    ctl.set_primary(src)
    reps = [n for n, v in views.items() if n != src and v.usable]
    ctl.event(type="cold_start", old_primary=src, new_primary=src,
              total_s=round(time.monotonic() - t0, 3),
              watermark_gtid=resp.get("gtid_executed") if isinstance(resp, dict) else None,
              note=f"every node booted read-only, {', '.join(reps) or 'no replica'} "
                   f"replicate from {src} and hold nothing it lacks, promoted it in place")
    # HEALTHY or DEGRADED is decided by the next reconcile, on fresh views


async def bootstrap(ctl: SetController) -> None:
    first, rest = ctl.scfg.nodes[0], [n for n in ctl.members if n != ctl.scfg.nodes[0]]
    t0 = time.monotonic()
    log.warning("bootstrap", rs=ctl.rs, primary=first, replicas=rest)
    try:
        resp = await ctl.agents.post(first, "/promote", timeout=ROLE_CHANGE_TIMEOUT_S)
    except AgentError as e:
        ctl.st.halt(f"bootstrap: promote of {first} failed: {e}")
        return
    ctl.set_primary(first)
    from dbguard.manager.failover import repoint_all
    step = await repoint_all(ctl, rest, first)
    ctl.event(type="bootstrap", new_primary=first, total_s=round(time.monotonic() - t0, 3),
              watermark_gtid=resp.get("gtid_executed") if isinstance(resp, dict) else None,
              note=f"new set, promoted {first}, repointed {', '.join(step.nodes)}")
    # HEALTHY or DEGRADED is decided by the next reconcile, on fresh views
