"""Rejoin, the step after a failover, bringing the old primary and any straggler back.

The choice is exact and made by GTID_SUBSET on fresh reads. A node is first fenced if it
still looks like a primary and must be quiescent (no semi-sync waiters, two equal reads),
because killed waiters on a woken primary commit seconds later as phantoms. A subset of the
primary's set is repointed with SOURCE_AUTO_POSITION=1 and watched for errant GTIDs, anything
else is rebuilt by clone and its phantom count recorded. The donor is always a healthy
replica, never the primary, because a clone reads the whole dataset and blocks DDL on the
donor, and a clone from a primary stalled on semi-sync never finished. ``rejoin: manual``
only records the need, but a second writer is fenced in every mode except naive.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import structlog

from dbguard.events import Clone, Event, Rejoin
from dbguard.gtid import GtidSet
from dbguard.manager.client import AgentError
from dbguard.manager.failover import ROLE_CHANGE_TIMEOUT_S
from dbguard.manager.model import NodeView, Observation

if TYPE_CHECKING:
    from dbguard.manager.controller import SetController

log = structlog.get_logger("dbguard.manager.rejoin")

RETRY_BACKOFF_S = 10.0       # do not repeat a rejoin action on the same node sooner
REBUILD_TIMEOUT_S = 1800.0   # a clone takes as long as the data does
RESTART_WAIT_S = 180.0       # mysqld restarts after a clone before it can be repointed


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
    """After a poll, fence second writers and start a rejoin for every member that needs one.

    Each node is retried at most every 10 s, and a node with a task already running is skipped."""
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
            if ctl.mode == "naive":
                # A naive tool has no fence. Record the split brain once and leave both
                # writable, so the harness measures what a naive baseline really does.
                if n not in ctl.split_noted:
                    ctl.split_noted.add(n)
                    ctl.event(type="split_brain", old_primary=n, new_primary=primary,
                              note=f"{n} and {primary} are both writable; naive mode does "
                                   f"not fence")
                continue
            # A second writer. Fence first, think later (woken primary, stale manager).
            ctl.spawn_node_task(n, fence_second_writer(ctl, n, primary))
            continue
        ctl.split_noted.discard(n)
        why = needs_rejoin(nv, primary)
        if why is None:
            ctl.manual_noted.discard(n)
            continue
        if ctl.backoff.get(n, 0) > now:
            continue
        ctl.backoff[n] = now + RETRY_BACKOFF_S
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
    """Fence a node that is writable while another is primary, and record it."""
    log.error("second writable node, fencing", rs=ctl.rs, node=n, primary=primary)
    try:
        await ctl.agents.post(n, "/fence", timeout=ctl.cfg.fence_deadline_s)
        note = f"{n} was writable while {primary} is primary, fenced it"
    except AgentError as e:
        note = f"{n} is writable while {primary} is primary and fencing failed: {e}"
    ctl.event(type="rejoin", old_primary=n, new_primary=primary, note=note,
              rejoin=Rejoin(branch="none", node=n))


QUIESCE_DEADLINE_S = 20.0    # longer than this and the node is rebuilt, not trusted
QUIESCE_INTERVAL_S = 1.0     # default: two equal gtid_executed reads this far apart
VERIFY_READS = 10            # after a repoint, watch for errant GTIDs this many intervals


async def quiesce(ctl: SetController, n: str) -> tuple[NodeView | None, str]:
    """Wait until ``n``'s gtid_executed can be trusted for the subset check.

    A woken primary still has sessions that were binlogged and waiting for a semi-sync ack
    when it froze. When the fence kills them they commit, seconds after the node became
    reachable, as unacknowledged phantoms. A gtid_executed read before that is too small, the
    subset check passes, and the node is repointed with GTIDs the primary never had: silent
    divergence (hang-container, 2 of 2 runs, docs/BUGS.md). So: fresh /status only, no
    semi-sync waiters, no committing threads, and two equal reads ctl.quiesce_interval_s apart
    (QUIESCE_INTERVAL_S, 1 s, in production).
    Returns the last view and "ok", or None and why it never settled.
    """
    end = time.monotonic() + QUIESCE_DEADLINE_S
    prev: NodeView | None = None
    why = "no status"
    while time.monotonic() < end:
        nv = await ctl.agents.status(n)
        if not nv.usable:
            why, prev = f"{n} not responsive", None
        else:
            waiting = int((nv.raw or {}).get("semisync", {}).get("wait_sessions") or 0)
            committing = None
            if ctl.prober is not None and hasattr(ctl.prober, "committing_threads"):
                committing = await ctl.prober.committing_threads(n)
            if waiting or committing:
                why, prev = (f"{n} has {waiting} semi-sync waiters and "
                             f"{committing or 0} committing threads"), None
            elif prev is not None and prev.gtid_executed == nv.gtid_executed:
                return nv, "ok"
            else:
                why, prev = f"{n} gtid_executed still changing", nv
        await asyncio.sleep(ctl.quiesce_interval_s)
    return None, why


async def rejoin_node(ctl: SetController, n: str, nv: NodeView | None = None,
                      pv: NodeView | None = None, why: str = "operator request",
                      wait: bool = False, inline_verify: bool = True) -> Event | None:
    """Repoint ``n`` if it holds nothing the primary lacks, else rebuild it from a replica.

    Never decides from a cached view. The node is fenced first when it looks like a former
    primary (semi-sync source side on, or waiters), then must be quiescent, then the subset
    check runs on fresh reads of both sets. ``nv`` and ``pv`` are ignored beyond logging;
    they stay in the signature for callers. A rebuild runs in the background unless ``wait``
    is set. After a repoint a watcher checks for errant GTIDs for VERIFY_READS intervals
    (10 s in production)."""
    primary = ctl.primary
    fresh = await ctl.agents.status(n)
    if fresh.usable and (fresh.semisync.source_enabled or
                         (fresh.raw or {}).get("semisync", {}).get("wait_sessions")):
        try:
            await ctl.agents.post(n, "/fence", timeout=ctl.cfg.fence_deadline_s)
        except AgentError as e:
            log.warning("rejoin: fence before the subset check failed", rs=ctl.rs, node=n,
                        error=str(e))
    stable, qwhy = await quiesce(ctl, n)
    if stable is None:
        log.warning("rejoin: node never quiesced, rebuilding instead", rs=ctl.rs, node=n,
                    why=qwhy)
        return await _rebuild(ctl, n, GtidSet(), f"{why}; not quiescent ({qwhy})", wait,
                              branch="rebuild")
    pnow = await ctl.agents.status(primary)          # read the primary AFTER the node
    if not pnow.usable:
        log.warning("rejoin: primary not responsive, retry later", rs=ctl.rs, node=n)
        ctl.backoff[n] = time.time() + RETRY_BACKOFF_S
        return None
    phantom = stable.gtid_executed - pnow.gtid_executed
    if phantom.is_empty:
        t0 = time.monotonic()
        try:
            await ctl.agents.post(n, "/repoint", {"source": primary},
                                  timeout=ROLE_CHANGE_TIMEOUT_S)
        except AgentError as e:
            log.warning("rejoin repoint failed", rs=ctl.rs, node=n, error=str(e))
            return None
        ev = ctl.event(type="rejoin", old_primary=n, new_primary=primary,
                       rejoin=Rejoin(branch="repoint", phantom_gtids=0, node=n,
                                     duration_s=round(time.monotonic() - t0, 3)),
                       note=f"{n} {why}, quiescent gtid_executed is a subset of {primary}'s, "
                            f"repointed")
        verify = verify_after_repoint(ctl, n, primary)
        if inline_verify:
            # Called from the node's own background task (rejoin_actions): watching inline
            # keeps the node marked busy for the whole watch, so no second rejoin starts.
            await verify
        elif not ctl.spawn_node_task(n, verify):
            log.warning("errant watch not started, node busy", rs=ctl.rs, node=n)
        return ev
    return await _rebuild(ctl, n, phantom, why, wait, branch="rebuild")


async def _rebuild(ctl: SetController, n: str, phantom: GtidSet, why: str, wait: bool,
                   branch: str) -> Event | None:
    """Clone ``n`` from the first healthy replica, in the background unless ``wait``."""
    views = await ctl.fresh_views([m for m in ctl.members if m != n])
    donors = ctl.healthy_replicas(views, exclude=(n,))
    if not donors:
        log.warning("rebuild needed but no healthy replica to clone from", rs=ctl.rs, node=n,
                    phantom=phantom.count())
        ctl.backoff[n] = time.time() + RETRY_BACKOFF_S
        return None
    coro = rebuild_and_join(ctl, n, donors[0], phantom_count=phantom.count(),
                            phantom=str(phantom) if phantom else None, why=why,
                            event_type="rejoin", branch=branch)
    if wait:
        return await coro
    ctl.start_maintenance(coro, [n])
    return None


async def verify_after_repoint(ctl: SetController, n: str, primary: str) -> None:
    """Belt and braces: after a repoint, the node must never hold a GTID the primary lacks.
    Read the node first, then the primary (which only grows), so a transaction the node got
    by replication is always in the primary's later read. Errant twice in a row means stop
    trusting it: rebuild, branch rebuild_after_errant."""
    end = time.monotonic() + VERIFY_READS * ctl.quiesce_interval_s
    seen = 0
    while time.monotonic() < end:
        await asyncio.sleep(ctl.quiesce_interval_s)
        if ctl.primary != primary:
            return
        nv = await ctl.agents.status(n)
        pv = await ctl.agents.status(primary)
        if not (nv.usable and pv.usable):
            continue
        errant = nv.gtid_executed - pv.gtid_executed
        seen = seen + 1 if errant else 0
        if seen >= 2:
            log.error("errant GTIDs after repoint, rebuilding", rs=ctl.rs, node=n,
                      errant=str(errant))
            await start_rebuild_when_free(
                ctl, n, errant, f"held {errant.count()} GTIDs {primary} lacks after its repoint",
                branch="rebuild_after_errant")
            return


async def start_rebuild_when_free(ctl: SetController, n: str, phantom: GtidSet, why: str,
                                  branch: str) -> None:
    """Start the rebuild through start_maintenance, like every other clone, so the node is
    marked busy and rejoin_actions never starts a second rejoin or rebuild of it. Waits for
    the maintenance slot and for a donor, retrying until the manager stops."""
    while True:
        await _wait_maintenance_slot(ctl)
        views = await ctl.fresh_views([m for m in ctl.members if m != n])
        donors = ctl.healthy_replicas(views, exclude=(n,))
        if donors:
            coro = rebuild_and_join(ctl, n, donors[0], phantom_count=phantom.count(),
                                    phantom=str(phantom), why=why, event_type="rejoin",
                                    branch=branch)
            if ctl.start_maintenance(coro, [n]):
                return
        else:
            log.warning("rebuild needed but no healthy replica to clone from", rs=ctl.rs,
                        node=n, phantom=phantom.count())
        await asyncio.sleep(ctl.quiesce_interval_s)


async def _wait_maintenance_slot(ctl: SetController) -> None:
    """Wait until no other maintenance task (clone, replacement) is running."""
    while ctl.maint is not None and not ctl.maint.done():
        await asyncio.sleep(0.5)


async def rebuild_and_join(ctl: SetController, node: str, donor: str, *, phantom_count: int,
                           phantom: str | None, why: str, event_type: str,
                           branch: str = "rebuild") -> Event | None:
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
        rejoin=Rejoin(branch=branch, phantom_gtids=ph, duration_s=dur, node=node),
        clone=Clone(bytes=nbytes, duration_s=round(clone_s, 3), donor=donor,
                    mb_per_s=round(nbytes / 1e6 / clone_s, 2) if clone_s > 0 and nbytes else None),
        note=note)
