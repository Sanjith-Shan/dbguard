"""Holistic failure detection. A pure function of recent observations.

The design follows orchestrator's split between DeadMaster and UnreachableMaster: the
manager's own failed probe is only half the evidence, the other half is what the
replicas see. A primary is DEAD only when both hold, continuously, for detect_window_s:

1. The manager's probe (``SELECT 1`` and a heartbeat WRITE) failed ``probe_failures``
   consecutive times. It must be a write. A primary cut off from its replicas still
   answers ``SELECT 1``, but with semi-sync its commits block waiting for an ack, so only
   a write notices (Experiment 3).
2. Strictly more than half of the witnesses, and at least one, vote that they lost the
   primary. A witness is a replica the manager can reach that is configured to replicate
   from the primary. A vote means its IO thread is not running, its agent cannot TCP
   connect to the primary, or its heartbeat row is older than detect_window_s. Zero
   witnesses means nobody can confirm, and the verdict stays SUSPECT.

(1) without (2) is SUSPECT: the manager is probably the partitioned one (Experiment 4),
and the correct action is none. (2) without (1) is logged and left alone. Naive mode
decides from (1) alone, which is exactly what makes it false-fail over.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from dbguard.manager.model import NodeView, Observation


@dataclass(frozen=True)
class DetectParams:
    mode: str = "dbguard"
    detect_window_s: float = 5.0
    probe_failures: int = 3
    configured_replicas: int = 2   # reporting only, the majority is over witnesses


@dataclass(frozen=True)
class Verdict:
    kind: str                     # OK | SUSPECT | DEAD | REPLICAS_ONLY | NO_PRIMARY
    primary: str | None = None
    probe_failed: bool = False
    probe_streak: int = 0
    probe_down_s: float = 0.0     # how long the manager's probe has been failing
    probe_error: str | None = None
    write_stalled: bool = False   # SELECT 1 answers, the heartbeat write times out
    replica_votes: int = 0
    replica_total: int = 0        # witnesses: reachable replicas of this primary
    quorum: bool = False
    quorum_s: float = 0.0         # how long the replica majority has held
    reasons: dict[str, list[str]] = field(default_factory=dict)
    trigger: str | None = None    # dead | hung | partition
    detect_s: float = 0.0         # first evidence to now

    @property
    def dead(self) -> bool:
        return self.kind == "DEAD"


def is_witness(nv: NodeView | None, primary: str) -> bool:
    """A replica that can testify about ``primary``: reachable, mysqld answering, and
    configured to replicate from it."""
    return (nv is not None and nv.usable and nv.replica.configured
            and nv.replica.source_host == primary)


def replica_vote(nv: NodeView, primary: str, window_s: float) -> list[str]:
    """Reasons this replica believes the primary is gone. Empty list means no vote.

    An unreachable replica, one whose mysqld is down, or one that does not replicate
    from this primary does not vote: it knows nothing about the primary.
    """
    if not nv.usable:
        return []
    r = nv.replica
    if not (r.configured and r.source_host == primary):
        # Not a replica of this primary (a fenced old primary, a node replicating from
        # elsewhere): its IO thread and its heartbeat row say nothing about the primary.
        return []
    out = []
    if r.io_running != "Yes":
        out.append(f"IO thread {r.io_running or 'unknown'}")
    if nv.source_reachable is False:
        out.append("cannot reach primary")
    if nv.heartbeat_age_s is not None and nv.heartbeat_age_s > window_s:
        out.append(f"heartbeat stale for {nv.heartbeat_age_s:.1f} s")
    return out


def classify_trigger(pv: NodeView | None) -> str:
    """Best effort label for the event. The agent answering tells us most."""
    if pv is None or not pv.reachable:
        return "dead"
    if not pv.mysqld_alive:
        return "dead"
    if not pv.mysqld_responsive:
        return "hung"
    return "partition"


def evaluate(history: Sequence[Observation], members: Sequence[str], p: DetectParams,
             now: float | None = None) -> Verdict:
    if not history:
        return Verdict(kind="OK")
    last = history[-1]
    primary = last.primary
    if primary is None:
        return Verdict(kind="NO_PRIMARY")
    now = last.ts if now is None else now
    replicas = [m for m in members if m != primary]

    # (1) the manager's probe streak
    streak = 0
    down_since = None
    for ob in reversed(history):
        if ob.primary != primary or ob.probe is None:
            break
        if ob.probe.ok:
            break
        streak += 1
        down_since = ob.ts
    probe_failed = streak > 0
    probe_down_s = (now - down_since) if down_since is not None else 0.0

    # (2) the replicas' view. Witnesses are the replicas the manager can reach right now
    # (agent and mysqld answer) that are configured to replicate from this primary. The
    # majority is over witnesses, not over configured replicas, so a DEGRADED set whose
    # other replica already died (Experiment 6, kill-two) can still fail over.
    # Trade-off: with one live replica, that replica is a single witness, and one
    # replica's broken IO thread plus a manager partition is enough to fail over. Zero
    # witnesses means nobody can confirm, so the verdict stays SUSPECT.
    def votes_at(ob: Observation) -> tuple[int, int, dict[str, list[str]]]:
        rs: dict[str, list[str]] = {}
        witnesses = 0
        for m in replicas:
            nv = ob.nodes.get(m)
            if not is_witness(nv, primary):
                continue
            witnesses += 1
            why = replica_vote(nv, primary, p.detect_window_s)
            if why:
                rs[m] = why
        return len(rs), witnesses, rs

    def has_quorum(votes: int, witnesses: int) -> bool:
        return witnesses > 0 and votes >= 1 and votes * 2 > witnesses

    votes, witnesses, reasons = votes_at(last)
    quorum = has_quorum(votes, witnesses)
    quorum_since = None
    if quorum:
        for ob in reversed(history):
            if ob.primary != primary:
                break
            n, w, _ = votes_at(ob)
            if not has_quorum(n, w):
                break
            quorum_since = ob.ts
    quorum_s = (now - quorum_since) if quorum_since is not None else 0.0

    pv = last.nodes.get(primary)
    common = dict(
        primary=primary, probe_failed=probe_failed, probe_streak=streak,
        probe_down_s=probe_down_s,
        probe_error=last.probe.error if last.probe and not last.probe.ok else None,
        write_stalled=bool(last.probe and last.probe.write_stalled),
        replica_votes=votes, replica_total=witnesses, quorum=quorum,
        quorum_s=quorum_s, reasons=reasons,
    )
    enough_probes = streak >= p.probe_failures

    if p.mode == "naive":
        if enough_probes and probe_down_s >= p.detect_window_s:
            return Verdict(kind="DEAD", trigger=classify_trigger(pv), detect_s=probe_down_s,
                           **common)
        return Verdict(kind="SUSPECT" if probe_failed else "OK", **common)

    if enough_probes and quorum:
        both_s = min(probe_down_s, quorum_s)
        if both_s >= p.detect_window_s:
            return Verdict(kind="DEAD", trigger=classify_trigger(pv),
                           detect_s=max(probe_down_s, quorum_s), **common)
    if probe_failed:
        return Verdict(kind="SUSPECT", **common)
    if quorum:
        return Verdict(kind="REPLICAS_ONLY", **common)
    return Verdict(kind="OK", **common)
