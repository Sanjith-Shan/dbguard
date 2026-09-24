"""`dbgctl doctor <set>`: the set's state in sentences, and what a human should do."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from dbguard.manager.detector import evaluate
from dbguard.manager.model import State
from dbguard.manager.selection import choose

if TYPE_CHECKING:
    from dbguard.manager.controller import SetController

VERDICT_WORD = {"OK": "HEALTHY", "SUSPECT": "SUSPECT", "DEAD": "FAILED",
                "REPLICAS_ONLY": "HEALTHY", "NO_PRIMARY": "NO PRIMARY"}


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")


def next_step_for(reason: str, rs: str) -> str:
    r = reason.lower()
    if "diverged" in r:
        return (f"Next: compare the GTID gaps below, decide whose transactions to keep, "
                f"rebuild the other replica from the one you keep (POST /rebuild on its "
                f"agent), then run dbgctl resume {rs}.")
    if "catch-up" in r:
        return ("Next: run SHOW REPLICA STATUS on the chosen replica, look for a stuck or "
                "slow SQL thread, fix it, then dbgctl resume " + rs + " and let the manager "
                "retry, or dbgctl failover " + rs + " --to <node>.")
    if "more than one writable" in r:
        return ("Next: find which node clients wrote to last (compare gtid_executed), fence "
                "the other with POST /fence on its agent, then dbgctl resume " + rs + ".")
    if "promote" in r:
        return ("Next: read the agent log of the node named above, promote by hand with "
                "POST /promote once the cause is fixed, then dbgctl resume " + rs + ".")
    if "no promotable" in r:
        return ("Next: bring a replica back (start its container or mysqld), then dbgctl "
                "resume " + rs + ".")
    return f"Next: read the events (dbgctl events {rs}), fix the cause, then dbgctl resume {rs}."


def doctor(ctl: SetController, now: float | None = None) -> dict:
    now = now or time.time()
    rs = ctl.rs
    ob = ctl.last_obs
    lines: list[str] = []
    gaps: dict[str, str] = {}
    if ob is None:
        return {"lines": [f"{rs}: no observation yet"], "verdict": "UNKNOWN", "gaps": {}}
    p = ctl.primary
    pv = ob.nodes.get(p) if p else None
    v = evaluate(list(ctl.history), ctl.members, ctl.params, now=ob.ts) if p else None
    verdict = VERDICT_WORD.get(v.kind, v.kind) if v else ctl.st.state.value
    if ctl.st.state == State.HALTED:
        verdict = "HALTED"

    # 1. the verdict sentence ------------------------------------------------------
    if p is None:
        reach = sum(1 for n in ctl.members if (nv := ob.nodes.get(n)) and nv.usable)
        lines.append(f"{rs}: no primary found yet, {reach} of {len(ctl.members)} nodes "
                     f"reachable, state {ctl.st.state.value}.")
    else:
        parts = []
        if v.probe_failed:
            parts.append(f"primary {p} unreachable from manager for {v.probe_down_s:.1f} s "
                         f"({v.probe_error})")
        else:
            lat = f" in {ob.probe.duration_s * 1000:.0f} ms" if ob.probe else ""
            parts.append(f"primary {p} answers the manager's write probe{lat}")
        if v.replica_votes:
            hb = [nv.heartbeat_age_s for n, why in v.reasons.items()
                  if (nv := ob.nodes.get(n)) and nv.heartbeat_age_s is not None
                  and any("heartbeat" in w for w in why)]
            if hb and len(hb) == v.replica_votes:
                what = f"heartbeat stale for {max(hb):.1f} s"
            else:
                what = "losing it (" + "; ".join(
                    f"{n} {', '.join(w)}" for n, w in v.reasons.items()) + ")"
            parts.append(f"{v.replica_votes} of {v.replica_total} replicas report {what}")
        else:
            parts.append(f"0 of {v.replica_total} replicas report a problem with it")
        sentence = f"{rs}: " + ", ".join(parts) + f", verdict {verdict}"
        if v.kind in ("DEAD", "SUSPECT") and v.quorum or v.kind == "DEAD":
            others = [ob.nodes[n] for n in ctl.members if n != p and n in ob.nodes]
            ch = choose(others, mode=ctl.mode, old_primary=p)
            if ch.halt_reason:
                act = f"would HALT ({ch.halt_reason})"
            else:
                act = ("would fence and promote " if ctl.mode == "dbguard"
                       else "would promote ") + ch.winner
                if ch.ahead_by:
                    loser, n = max(ch.ahead_by.items(), key=lambda kv: kv[1])
                    act += (f" (retrieved set is {_plural(n, 'transaction')} ahead of {loser})"
                            if n else f" (level with {loser}, chosen by node order)")
            if v.kind == "SUSPECT":
                left = max(0.0, ctl.cfg.detect_window_s - min(v.probe_down_s, v.quorum_s))
                act = f"if this holds {left:.1f} s more it " + act
            if ctl.st.in_cooldown(now):
                act += (f", but the set is in cooldown for "
                        f"{ctl.st.cooldown_until - now:.0f} s more so it will not act")
            sentence += ", " + act
        elif v.kind == "SUSPECT":
            sentence += (", the manager is probably the partitioned one, taking no action")
        elif v.kind == "REPLICAS_ONLY":
            sentence += (", replicas cannot see the primary but the manager can, taking no "
                         "action unless writes stall")
        lines.append(sentence + ".")

    # 2. halted --------------------------------------------------------------------
    if ctl.st.state == State.HALTED:
        lines.append(f"{rs} is HALTED since {now - ctl.st.since:.0f} s ago: "
                     f"{ctl.st.halt_reason}.")
        lines.append(next_step_for(ctl.st.halt_reason or "", rs))

    # 3. semi-sync -----------------------------------------------------------------
    if pv is not None and pv.usable:
        ss = pv.semisync
        if ctl.mode == "naive" or not ss.source_enabled:
            lines.append(f"Semi-sync is off on {p}, commits are acknowledged before any "
                         f"replica has them, a crash can lose acknowledged writes.")
        elif ss.source_clients == 0:
            since = f" since {now - ctl.stall_since:.1f} s" if ctl.stall_since else ""
            lines.append(f"Primary is waiting for a replica ack, no replica connected, "
                         f"writes are stalled{since}. Bring a replica back; this stall is "
                         f"the price of never acknowledging an unprotected write.")
        else:
            lines.append(f"Semi-sync is on, {_plural(ss.source_clients, 'replica')} "
                         f"connected, average ack wait {ss.avg_wait_time_us} us, "
                         f"{ss.no_tx} commits ever acknowledged without a replica.")
    elif p is not None:
        lines.append(f"Primary {p} cannot be read through its agent "
                     f"({pv.error if pv else 'no status'}).")

    # 4. every other node and its GTID gap ------------------------------------------
    pg = pv.gtid_executed if pv is not None and pv.usable else None
    for n in ctl.all_nodes():
        if n == p:
            continue
        nv = ob.nodes.get(n)
        spare = n == ctl.scfg.spare and n not in ctl.members
        if nv is None or not nv.usable:
            why = "agent unreachable" if nv is None or not nv.reachable else "mysqld down"
            lines.append(f"{n} is {'the spare, ' if spare else ''}down ({why}).")
            continue
        r = nv.replica
        if spare:
            lines.append(f"{n} is the spare, up and not part of the set.")
            continue
        if r.configured:
            desc = (f"{n} replicates from {r.source_host}, IO {r.io_running}, SQL "
                    f"{r.sql_running}")
            if r.seconds_behind_source is not None:
                desc += f", {r.seconds_behind_source} s behind"
        else:
            desc = f"{n} is not replicating"
        if nv.heartbeat_age_s is not None:
            desc += f", heartbeat {nv.heartbeat_age_s:.1f} s old"
        if r.last_io_error:
            desc += f", IO error: {r.last_io_error}"
        if r.last_sql_error:
            desc += f", SQL error: {r.last_sql_error}"
        if pg is not None:
            missing = pg - nv.gtid_executed
            extra = nv.gtid_executed - pg
            gaps[n] = str(missing)
            desc += (f", missing {_plural(missing.count(), 'transaction')} of the primary"
                     if missing else ", has every transaction of the primary")
            if extra:
                desc += (f", and holds {_plural(extra.count(), 'transaction')} the primary "
                         f"never had ({extra}), so it must be rebuilt, not repointed")
        lines.append(desc + ".")

    # 5. cooldown and last event -----------------------------------------------------
    if ctl.st.in_cooldown(now):
        lines.append(f"Cooldown: no automatic failover for {ctl.st.cooldown_until - now:.0f} s "
                     f"more, a planned switchover is still allowed.")
    last = ctl.events.last(rs)
    if last is not None:
        lines.append(f"Last event {now - last.ts:.0f} s ago: {last.type}"
                     + (f" {last.old_primary} -> {last.new_primary}"
                        if last.type in ("failover", "switchover") else "")
                     + (f", {last.note}" if last.note else "") + ".")
    return {"lines": lines, "verdict": verdict, "gaps": gaps}
