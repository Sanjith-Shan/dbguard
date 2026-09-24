"""Candidate selection for promotion. Pure, hypothesis-tested (tests/test_selection.py)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from dbguard.gtid import GtidSet
from dbguard.manager.model import NodeView


@dataclass(frozen=True)
class Choice:
    winner: str | None
    candidates: list[str]
    excluded: dict[str, str] = field(default_factory=dict)   # node -> why not a candidate
    subset_ok: bool = True
    diverged: str | None = None                              # set when subset check fails
    ahead_by: dict[str, int] = field(default_factory=dict)   # loser -> winner's lead in trx

    @property
    def halt_reason(self) -> str | None:
        if self.diverged:
            return f"replicas diverged: {self.diverged}"
        if self.winner is None:
            return "no promotable replica: " + (
                "; ".join(f"{n} {why}" for n, why in self.excluded.items()) or "none configured"
            )
        return None


def candidate_problem(nv: NodeView, mode: str, old_primary: str | None) -> str | None:
    """Why this node may not be promoted, or None if it may."""
    if not nv.reachable:
        return "agent unreachable"
    if not nv.mysqld_responsive:
        return "mysqld not responsive"
    r = nv.replica
    if not r.configured:
        # Its retrieved set is empty because it never replicated, not because it is
        # behind. An empty set is a subset of everything, so letting it in would make
        # the subset check pass vacuously (docs/BUGS.md, Manager).
        return "replication not configured"
    if r.last_sql_error:
        return f"SQL thread error: {r.last_sql_error}"
    if r.sql_running == "No" and mode == "dbguard":
        # stopped by hand: the catch-up wait could never finish
        return "SQL thread stopped"
    if mode == "dbguard" and not nv.semisync.replica_enabled:
        return "semi-sync replica was not enabled"
    return None


def choose(nodes: Sequence[NodeView], mode: str = "dbguard",
           old_primary: str | None = None) -> Choice:
    """Pick the promotion target.

    dbguard: the candidate holding the most transactions (executed plus retrieved, see
    NodeView.have) wins, ties broken by executed count and then by node order. Every
    other candidate's set must be a subset of the winner's, else the replicas diverged and
    nobody is promoted.

    naive: largest Executed_Gtid_Set, no subset check.
    """
    excluded: dict[str, str] = {}
    cands: list[tuple[int, NodeView]] = []
    for i, nv in enumerate(nodes):
        if nv.name == old_primary:
            continue
        why = candidate_problem(nv, mode, old_primary)
        if why:
            excluded[nv.name] = why
        else:
            cands.append((i, nv))
    names = [nv.name for _, nv in cands]
    if not cands:
        return Choice(winner=None, candidates=[], excluded=excluded)

    if mode == "naive":
        def key(t):
            return (-t[1].gtid_executed.count(), t[0])
        w = min(cands, key=key)[1]
        return Choice(winner=w.name, candidates=names, excluded=excluded, subset_ok=True,
                      ahead_by={nv.name: (w.gtid_executed - nv.gtid_executed).count()
                                for _, nv in cands if nv is not w})

    def dkey(t):
        i, nv = t
        return (-nv.have.count(), -nv.gtid_executed.count(), i)

    w = min(cands, key=dkey)[1]
    whave: GtidSet = w.have
    ahead = {}
    bad = []
    for _, nv in cands:
        if nv is w:
            continue
        extra = nv.have - whave
        if not extra.is_empty:
            back = whave - nv.have
            msg = f"{nv.name} has {extra.count()} transactions {w.name} lacks ({extra})"
            if back:
                msg += f" and {w.name} has {back.count()} {nv.name} lacks ({back})"
            bad.append(msg)
        ahead[nv.name] = (whave - nv.have).count()
    if bad:
        return Choice(winner=None, candidates=names, excluded=excluded, subset_ok=False,
                      diverged="; ".join(bad), ahead_by=ahead)
    return Choice(winner=w.name, candidates=names, excluded=excluded, subset_ok=True,
                  ahead_by=ahead)
