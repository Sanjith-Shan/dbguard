"""Finding the primary at startup, and bootstrapping a brand new set.

Startup rules, in order:
1. exactly one member is writable (super_read_only=0, not fenced): it is the primary;
2. more than one is writable: HALTED, a human decides which writes to keep;
3. none writable, every member reachable, read-only, not replicating, with empty or
   identical gtid_executed: a new set, so bootstrap it (promote nodes[0], repoint the
   rest) per docs/INTERFACES.md;
4. none writable but replicas agree on a source: believe that source is the primary and
   let detection decide (a manager restarted during an outage lands here);
5. otherwise wait, the fleet may still be starting.
"""

from __future__ import annotations

import collections
import time
from typing import TYPE_CHECKING

import structlog

from dbguard.manager.client import AgentError
from dbguard.manager.failover import ROLE_CHANGE_TIMEOUT_S
from dbguard.manager.model import Observation, State

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
            ctl.set_primary(src)
            log.warning("no writable node, believing replicas' source", rs=ctl.rs, primary=src)
            return
    note = (f"no primary found yet: {len(usable)} of {len(ctl.members)} nodes answer, "
            f"none writable")
    if note != ctl.last_discover_note:
        ctl.last_discover_note = note
        log.warning("discover", rs=ctl.rs, note=note)


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
    ctl.st.to(State.HEALTHY, emit=False)
