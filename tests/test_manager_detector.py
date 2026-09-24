"""Detector verdicts on synthetic observation histories."""

from __future__ import annotations

from dbguard.manager.detector import DetectParams, evaluate
from dbguard.manager.model import NodeView, Observation, ProbeResult, ReplicaView

MEMBERS = ["p", "r1", "r2"]
W = 5.0
P = DetectParams(mode="dbguard", detect_window_s=W, probe_failures=3, configured_replicas=2)
NAIVE = DetectParams(mode="naive", detect_window_s=W, probe_failures=3, configured_replicas=2)


def replica(name, io="Yes", hb=0.3, reach=True, src="p", up=True):
    if not up:
        return NodeView.unreachable(name, "down")
    return NodeView(name=name, ts=0, reachable=True, mysqld_alive=True,
                    mysqld_responsive=True, super_read_only=True,
                    replica=ReplicaView(configured=True, source_host=src, io_running=io,
                                        sql_running="Yes"),
                    source_reachable=reach, heartbeat_age_s=hb)


def primary(alive=True, responsive=True, reachable=True):
    if not reachable:
        return NodeView.unreachable("p", "timeout")
    return NodeView(name="p", ts=0, reachable=True, mysqld_alive=alive,
                    mysqld_responsive=responsive, super_read_only=False)


def ok_probe(t):
    return ProbeResult(ok=True, ts=t, select_ok=True)


def bad_probe(t, stalled=False):
    return ProbeResult(ok=False, ts=t, select_ok=stalled, kind="timeout", error="x")


def history(seconds, probe, r1, r2, p=None, dt=0.5, start=0.0):
    """Observations every dt for ``seconds``; probe/r1/r2 are functions of t."""
    out = []
    t = start
    while t <= start + seconds + 1e-9:
        out.append(Observation(ts=t, primary="p", probe=probe(t),
                               nodes={"p": (p or primary)(), "r1": r1(t), "r2": r2(t)}))
        t += dt
    return out


def test_healthy():
    h = history(10, ok_probe, lambda t: replica("r1"), lambda t: replica("r2"))
    assert evaluate(h, MEMBERS, P).kind == "OK"


def test_dead_primary_needs_the_full_window():
    # killed at t=0: probes fail, IO threads drop at t=2 (replica_net_timeout)
    def r(n):
        return lambda t: replica(n, io="Connecting" if t >= 2 else "Yes", hb=t,
                                   reach=t < 2)
    h = history(6.5, bad_probe, r("r1"), r("r2"), p=lambda: primary(reachable=False))
    v = evaluate(h, MEMBERS, P)
    assert v.kind == "SUSPECT" and v.quorum      # quorum only 4.5 s old
    h = history(7.0, bad_probe, r("r1"), r("r2"), p=lambda: primary(reachable=False))
    v = evaluate(h, MEMBERS, P)
    assert v.dead and v.trigger == "dead"
    assert v.replica_votes == 2 and v.probe_failed and v.detect_s >= W


def test_hung_primary_heartbeat_route():
    """SIGSTOP: IO threads may still say Yes, but the heartbeat stops advancing."""
    h = history(12, bad_probe, lambda t: replica("r1", hb=t), lambda t: replica("r2", hb=t),
                p=lambda: primary(responsive=False))
    v = evaluate(h, MEMBERS, P)
    assert v.dead and v.trigger == "hung"
    assert all(any("heartbeat" in w for w in why) for why in v.reasons.values())


def test_manager_partitioned_is_suspect_forever():
    h = history(60, bad_probe, lambda t: replica("r1"), lambda t: replica("r2"))
    v = evaluate(h, MEMBERS, P)
    assert v.kind == "SUSPECT" and v.replica_votes == 0


def test_naive_fires_on_probe_alone():
    h = history(6, bad_probe, lambda t: replica("r1"), lambda t: replica("r2"))
    assert evaluate(h, MEMBERS, NAIVE).dead
    h = history(4, bad_probe, lambda t: replica("r1"), lambda t: replica("r2"))
    assert evaluate(h, MEMBERS, NAIVE).kind == "SUSPECT"


def test_replicas_partitioned_but_writes_fine_stays_healthy():
    h = history(20, ok_probe, lambda t: replica("r1", io="Connecting", reach=False),
                lambda t: replica("r2", io="Connecting", reach=False))
    v = evaluate(h, MEMBERS, P)
    assert v.kind == "REPLICAS_ONLY" and not v.dead


def test_replicas_partitioned_with_stalled_write_fails_over():
    """Experiment 3: SELECT 1 still answers, the semi-sync write blocks."""
    h = history(8, lambda t: bad_probe(t, stalled=True),
                lambda t: replica("r1", io="Connecting", reach=False),
                lambda t: replica("r2", io="Connecting", reach=False))
    v = evaluate(h, MEMBERS, P)
    assert v.dead and v.write_stalled and v.trigger == "partition"


def test_one_of_two_replicas_is_not_a_majority():
    h = history(20, bad_probe, lambda t: replica("r1", io="Connecting", reach=False),
                lambda t: replica("r2"))
    v = evaluate(h, MEMBERS, P)
    assert v.kind == "SUSPECT" and v.replica_votes == 1


def test_degraded_set_with_one_live_replica_can_fail_over():
    """Experiment 6 and kill-two: r2 died first, so r1 is the only witness. Its vote is a
    majority of one. This is the trade-off: a single live replica is a single witness."""
    h = history(20, bad_probe, lambda t: replica("r1", io="Connecting"),
                lambda t: replica("r2", up=False), p=lambda: primary(reachable=False))
    v = evaluate(h, MEMBERS, P)
    assert v.dead and v.replica_votes == 1 and v.replica_total == 1


def test_one_live_replica_that_sees_the_primary_blocks_failover():
    h = history(20, bad_probe, lambda t: replica("r1"), lambda t: replica("r2", up=False))
    v = evaluate(h, MEMBERS, P)
    assert v.kind == "SUSPECT" and v.replica_total == 1 and v.replica_votes == 0


def test_zero_witnesses_stays_suspect():
    h = history(30, bad_probe, lambda t: replica("r1", up=False),
                lambda t: replica("r2", up=False), p=lambda: primary(reachable=False))
    v = evaluate(h, MEMBERS, P)
    assert v.kind == "SUSPECT" and v.replica_total == 0 and not v.quorum


def test_half_is_not_a_majority():
    members = ["p", "r1", "r2", "r3", "r4"]
    obs = []
    for i in range(30):
        t = i * 0.5
        obs.append(Observation(ts=t, primary="p", probe=bad_probe(t), nodes={
            "p": primary(reachable=False), "r1": replica("r1", io="Connecting"),
            "r2": replica("r2", io="Connecting"), "r3": replica("r3"),
            "r4": replica("r4")}))
    v = evaluate(obs, members, DetectParams(detect_window_s=W, configured_replicas=4))
    assert v.kind == "SUSPECT" and v.replica_votes == 2 and v.replica_total == 4


def test_replica_of_another_source_is_not_a_witness():
    h = history(20, bad_probe, lambda t: replica("r1", hb=0.2),
                lambda t: replica("r2", io="Connecting", hb=99, src="r1"))
    v = evaluate(h, MEMBERS, P)
    assert v.kind == "SUSPECT" and v.replica_total == 1


def test_flapping_probe_resets_the_window():
    def probe(t):
        return ok_probe(t) if int(t * 2) % 8 == 0 else bad_probe(t)
    h = history(30, probe, lambda t: replica("r1", io="Connecting"),
                lambda t: replica("r2", io="Connecting"))
    assert not evaluate(h, MEMBERS, P).dead


def test_single_probe_failure_is_not_enough():
    def probe(t):
        return bad_probe(t) if t >= 29.9 else ok_probe(t)
    h = history(30, probe, lambda t: replica("r1", io="Connecting"),
                lambda t: replica("r2", io="Connecting"))
    v = evaluate(h, MEMBERS, P)
    assert v.kind == "SUSPECT" and v.probe_streak == 1


def test_three_replica_majority():
    members = ["p", "r1", "r2", "r3"]
    p3 = DetectParams(detect_window_s=W, probe_failures=3, configured_replicas=3)
    obs = []
    for i in range(20):
        t = i * 0.5
        obs.append(Observation(ts=t, primary="p", probe=bad_probe(t), nodes={
            "p": primary(reachable=False), "r1": replica("r1", io="Connecting"),
            "r2": replica("r2", io="Connecting"), "r3": replica("r3")}))
    assert evaluate(obs, members, p3).dead
