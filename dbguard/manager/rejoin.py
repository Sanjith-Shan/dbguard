"""Bringing nodes back under the current primary.

Covers the former primary coming back and any straggler replicating from the wrong
source (or not at all). The rule is the last paragraph of the 2014 GTID post:

- the node's gtid_executed is a subset of the primary's: it has nothing the primary
  lacks, so repoint it with SOURCE_AUTO_POSITION=1;
- otherwise it holds transactions the primary never received. With AFTER_SYNC those
  were never acknowledged to a client and must not survive: rebuild it with the clone
  plugin from a healthy replica (never from the primary, to keep primary I/O clean),
  then repoint. The count of discarded GTIDs is recorded.

``rejoin: manual`` records that a node wants to rejoin and does nothing else, except
fencing a node that is writable while it is not the primary, which is always done.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import structlog

from dbguard.events import Clone, Event, Rejoin
from dbguard.manager.client import AgentError
from dbguard.manager.failover import ROLE_CHANGE_TIMEOUT_S
from dbguard.manager.model import NodeView, Observation

if TYPE_CHECKING:
    from dbguard.manager.controller import SetController

log = structlog.get_logger("dbguard.manager.rejoin")

REBUILD_TIMEOUT_S = 1800.0
RESTART_WAIT_S = 180.0


def needs_rejoin(nv: NodeView, primary: str) -> str | None:
    """Why this node is not a proper replica of ``primary``, or None."""
    r = nv.replica
    if not r.configured:
        return "not replicating"
    if r.source_host != primary:
        return f"replicating from {r.source_host}, not {primary}"
    if r.io_running == "No":
        return f"IO thread stopped ({r.last_io_error or 'no error'})"
    if r.sql_running == "No":
        return f"SQL thread stopped ({r.last_sql_error or 'no error'})"
    return None


async def rejoin_actions(ctl: SetController, ob: Observation) -> None:
    primary = ctl.primary
    pv = ob.nodes.get(primary) if primary else None
    if pv is None or not pv.usable:
        return
    now = time.time()
    for n in ctl.members:
        if n == primary or ctl.node_busy(n):
            continue
        nv = ob.nodes.get(n)
        if nv is None or not nv.usable:
            continue
        if nv.writable:
            # A second writer. Fence first, think later (woken primary, stale manager).
            ctl.spawn_node_task(n, fence_second_writer(ctl, n, primary))
            continue
        why = needs_rejoin(nv, primary)
        if why is None:
            ctl.manual_noted.discard(n)
            continue
        if ctl.backoff.get(n, 0) > now:
            continue
        ctl.backoff[n] = now + 10.0
        if ctl.rejoin_mode == "manual":
            if n not in ctl.manual_noted:
                ctl.manual_noted.add(n)
                ctl.event(type="rejoin", old_primary=n, new_primary=primary,
                          rejoin=Rejoin(branch="manual", node=n,
                                        phantom_gtids=(nv.gtid_executed - pv.gtid_executed)
                                        .count()),
                          note=f"{n} {why}, rejoin is manual, waiting for an operator")
            continue
        ctl.spawn_node_task(n, rejoin_node(ctl, n, nv, pv, why))


async def fence_second_writer(ctl: "SetController", n: str, primary: str) -> None:
    log.error("second writable node, fencing", rs=ctl.rs, node=n, primary=primary)
    try:
        await ctl.agents.post(n, "/fence", timeout=ctl.cfg.fence_deadline_s)
        note = f"{n} was writable while {primary} is primary, fenced it"
    except AgentError as e:
        note = f"{n} is writable while {primary} is primary and fencing failed: {e}"
    if ctl.mode == "naive":
        note += " (naive mode borrows this safety net from dbguard mode)"
    ctl.event(type="rejoin", old_primary=n, new_primary=primary, note=note,
              rejoin=Rejoin(branch="none", node=n))


async def rejoin_node(ctl: SetController, n: str, nv: NodeView, pv: NodeView,
                      why: str = "operator request", wait: bool = False) -> Event | None:
    primary = ctl.primary
    phantom = nv.gtid_executed - pv.gtid_executed
    if phantom.is_empty:
        t0 = time.monotonic()
        try:
            await ctl.agents.post(n, "/repoint", {"source": primary},
                                  timeout=ROLE_CHANGE_TIMEOUT_S)
        except AgentError as e:
            log.warning("rejoin repoint failed", rs=ctl.rs, node=n, error=str(e))
            return None
        return ctl.event(type="rejoin", old_primary=n, new_primary=primary,
                         rejoin=Rejoin(branch="repoint", phantom_gtids=0, node=n,
                                       duration_s=round(time.monotonic() - t0, 3)),
                         note=f"{n} {why}, gtid_executed is a subset of {primary}'s, repointed")
    views = ctl.last_obs.nodes if ctl.last_obs else {}
    donors = ctl.healthy_replicas(views, exclude=(n,))
    if not donors:
        log.warning("rebuild needed but no healthy replica to clone from", rs=ctl.rs, node=n,
                    phantom=phantom.count())
        ctl.backoff[n] = time.time() + 10.0
        return None
    donor = donors[0]
    coro = rebuild_and_join(ctl, n, donor, phantom_count=phantom.count(), phantom=str(phantom),
                            why=why, event_type="rejoin")
    if wait:
        return await coro
    ctl.start_maintenance(coro, [n])
    return None


async def rebuild_and_join(ctl: SetController, node: str, donor: str, *, phantom_count: int,
                           phantom: str | None, why: str, event_type: str) -> Event | None:
    """Clone ``node`` from ``donor``, wait for mysqld, repoint to the current primary."""
    t0 = time.monotonic()
    log.warning("rebuild", rs=ctl.rs, node=node, donor=donor, phantom=phantom_count)
    try:
        resp = await ctl.agents.post(node, "/rebuild", {"donor": donor},
                                     timeout=REBUILD_TIMEOUT_S)
    except AgentError as e:
        ctl.event(type="rebuild", old_primary=node, new_primary=ctl.primary,
                  note=f"clone of {node} from {donor} failed: {e}")
        return None
    clone_s = time.monotonic() - t0
    end = time.monotonic() + RESTART_WAIT_S
    while time.monotonic() < end:
        nv = await ctl.agents.status(node)
        if nv.usable:
            break
        await asyncio.sleep(0.5)
    source = ctl.primary
    try:
        await ctl.agents.post(node, "/repoint", {"source": source},
                              timeout=ROLE_CHANGE_TIMEOUT_S)
    except AgentError as e:
        log.warning("repoint after rebuild failed", rs=ctl.rs, node=node, error=str(e))
    nbytes = int(resp.get("bytes") or 0) if isinstance(resp, dict) else 0
    ph = int(resp.get("phantom_gtids") or phantom_count) if isinstance(resp, dict) else \
        phantom_count
    dur = round(time.monotonic() - t0, 3)
    if ctl.metrics:
        ctl.metrics.phantom.labels(rs=ctl.rs).inc(ph)
        ctl.metrics.rebuilt.labels(rs=ctl.rs, reason=event_type).inc()
    note = f"{node} {why}; cloned from {donor}, repointed to {source}"
    if phantom_count:
        note += f"; discarded {phantom_count} phantom transactions ({phantom})"
    return ctl.event(
        type=event_type, old_primary=node, new_primary=source,
        rejoin=Rejoin(branch="rebuild", phantom_gtids=ph, duration_s=dur, node=node),
        clone=Clone(bytes=nbytes, duration_s=round(clone_s, 3), donor=donor,
                    mb_per_s=round(nbytes / 1e6 / clone_s, 2) if clone_s > 0 and nbytes else None),
        note=note)
