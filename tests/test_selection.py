"""Candidate selection: hypothesis over random fleets of 2 to 4 candidates."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from dbguard.gtid import GtidSet
from dbguard.manager.model import NodeView, ReplicaView, SemisyncView
from dbguard.manager.selection import candidate_problem, choose

P = "11111111-1111-1111-1111-111111111111"   # the old primary's uuid
E = [f"22222222-2222-2222-2222-22222222222{i}" for i in range(4)]  # errant uuids


def node(name, executed: GtidSet, retrieved: GtidSet = GtidSet(), *, configured=True,
         reachable=True, responsive=True, sql_error=None, semisync=True, sql="Yes"):
    if not reachable:
        return NodeView.unreachable(name, "down")
    return NodeView(
        name=name, ts=0.0, reachable=True, mysqld_alive=responsive,
        mysqld_responsive=responsive, super_read_only=True, gtid_executed=executed,
        replica=ReplicaView(configured=configured, source_host="old" if configured else None,
                            io_running="Connecting", sql_running=sql, retrieved=retrieved,
                            executed=executed, last_sql_error=sql_error),
        semisync=SemisyncView(replica_enabled=semisync))


def prefix(n: int) -> GtidSet:
    return GtidSet.of(P, (1, n)) if n > 0 else GtidSet()


@st.composite
def fleets(draw, diverge=True):
    """Replicas of one primary: each holds a prefix of its binlog, split between applied
    and relay log, optionally plus errant transactions of its own."""
    k = draw(st.integers(2, 4))
    nodes = []
    for i in range(k):
        have = draw(st.integers(0, 50))
        applied = draw(st.integers(0, have))
        # relay log holds (some suffix of) applied+1..have; after a repoint it may be empty
        # for what was applied before
        rstart = draw(st.integers(1, applied + 1))
        retrieved = GtidSet.of(P, (rstart, have)) if have >= rstart else GtidSet()
        executed = prefix(applied)
        if diverge and draw(st.booleans()) and draw(st.booleans()):
            executed = executed | GtidSet.of(E[i], (1, draw(st.integers(1, 3))))
        kind = draw(st.sampled_from(["ok", "ok", "ok", "unconfigured", "down", "sqlerr",
                                     "nosemisync"]))
        nodes.append(node(
            f"n{i}", executed, retrieved,
            configured=kind != "unconfigured", reachable=kind != "down",
            sql_error="dup key" if kind == "sqlerr" else None,
            semisync=kind != "nosemisync"))
    return nodes


@settings(max_examples=400)
@given(fleets())
def test_winner_is_max_and_superset_or_halt(nodes):
    ch = choose(nodes, mode="dbguard")
    cands = [n for n in nodes if candidate_problem(n, "dbguard", None) is None]
    assert sorted(ch.candidates) == sorted(n.name for n in cands)
    if ch.winner is None:
        assert ch.halt_reason
        if cands:
            # halting is only allowed when some pair really diverged
            assert not ch.subset_ok
            assert any(not a.have.is_subset(b.have) and not b.have.is_subset(a.have)
                       for a in cands for b in cands if a is not b)
        return
    w = next(n for n in nodes if n.name == ch.winner)
    assert w in cands, "never promote a non-candidate"
    assert all(w.have.count() >= c.have.count() for c in cands)
    assert all(c.have.is_subset(w.have) for c in cands), "every loser is a subset"
    assert ch.subset_ok


@settings(max_examples=300)
@given(fleets(diverge=False))
def test_no_divergence_never_halts_when_a_candidate_exists(nodes):
    ch = choose(nodes, mode="dbguard")
    cands = [n for n in nodes if candidate_problem(n, "dbguard", None) is None]
    if cands:
        assert ch.winner is not None
        best = max(c.have.count() for c in cands)
        assert next(n for n in nodes if n.name == ch.winner).have.count() == best


@given(fleets())
def test_naive_picks_largest_executed_without_subset_check(nodes):
    ch = choose(nodes, mode="naive")
    cands = [n for n in nodes if candidate_problem(n, "naive", None) is None]
    if not cands:
        assert ch.winner is None
        return
    w = next(n for n in nodes if n.name == ch.winner)
    assert w.gtid_executed.count() == max(c.gtid_executed.count() for c in cands)
    assert ch.subset_ok


def test_tie_breaks_by_executed_then_node_order():
    a = node("a", prefix(5), GtidSet.of(P, (6, 10)))   # have 10, executed 5
    b = node("b", prefix(10), GtidSet())                # have 10, executed 10
    c = node("c", prefix(10), GtidSet())
    assert choose([a, b, c]).winner == "b"
    assert choose([c, b, a]).winner == "c"


def test_repointed_replica_with_small_retrieved_set_is_not_behind():
    """Retrieved_Gtid_Set is reset by CHANGE REPLICATION SOURCE. Comparing retrieved sets
    alone would pick 'a' (3 retrieved) over 'b' (1 retrieved) although b holds more."""
    a = node("a", prefix(50), GtidSet.of(P, (48, 50)))
    b = node("b", prefix(51), GtidSet.of(P, (51, 51)))
    ch = choose([a, b])
    assert ch.winner == "b" and ch.ahead_by == {"a": 1}


def test_empty_unconfigured_node_is_not_a_candidate():
    """An empty set is a subset of everything: a never-configured node must not slip in
    and must not make the subset check pass vacuously."""
    fresh = node("fresh", GtidSet(), GtidSet(), configured=False)
    lag = node("lag", prefix(3), GtidSet())
    ch = choose([fresh, lag])
    assert ch.winner == "lag" and "fresh" in ch.excluded
    ch = choose([fresh])
    assert ch.winner is None and "replication not configured" in ch.halt_reason


def test_divergence_reports_detail():
    a = node("a", prefix(5) | GtidSet.of(E[0], 1))
    b = node("b", prefix(6))
    ch = choose([a, b])
    assert ch.winner is None and not ch.subset_ok
    assert ch.halt_reason.startswith("replicas diverged: b has 1 transactions a lacks")
    assert "and a has 1 b lacks" in ch.halt_reason


def test_old_primary_is_never_a_candidate():
    a = node("old", prefix(100))
    b = node("b", prefix(1))
    assert choose([a, b], old_primary="old").winner == "b"


@given(st.integers(0, 30))
def test_all_empty_or_equal(x):
    ch = choose([node("a", prefix(x)), node("b", prefix(x))])
    assert ch.winner == "a" and ch.subset_ok
