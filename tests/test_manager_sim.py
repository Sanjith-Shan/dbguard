"""End-to-end manager scenarios against the in-process FakeFleet (real HTTP, fake mysqld)."""

from __future__ import annotations

import asyncio
import time

import pytest

from dbguard.events import Event
from dbguard.gtid import GtidSet
from dbguard.manager.api import make_app
from dbguard.manager.client import Addressing
from dbguard.manager.config import FleetConfig
from dbguard.manager.fake import FakeFleet, FakeProber
from dbguard.manager.manager import Manager
from dbguard.manager.model import State

A = ["mysql-a1", "mysql-a2", "mysql-a3"]
B = ["mysql-b1", "mysql-b2", "mysql-b3"]

CONTRACT_KEYS = {"ts", "rs", "type", "mode", "old_primary", "new_primary", "trigger", "detect",
                 "steps", "total_s", "watermark_gtid", "rejoin", "note"}


class FakeProvisioner:
    def __init__(self, fleet):
        self.fleet = fleet
        self.calls = []

    async def provision(self, rs, node):
        self.calls.append(node)
        self.fleet.start(node)


class Sim:
    def __init__(self, fleet, mgr, cfg):
        self.fleet, self.mgr, self.cfg = fleet, mgr, cfg

    def ctl(self, rs="rs1"):
        return self.mgr.sets[rs]

    def events(self, rs="rs1", type=None):
        return [e for e in self.mgr.events.query(rs=rs) if type is None or e.type == type]

    async def until(self, cond, timeout=5.0, what="condition"):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if cond():
                return
            await asyncio.sleep(0.02)
        raise AssertionError(f"timed out waiting for {what}; state="
                             f"{ {rs: (c.st.state.value, c.primary, c.st.halt_reason) for rs, c in self.mgr.sets.items()} }"
                             f" events={[(e.type, e.note) for e in self.mgr.events.query()]}")

    async def healthy(self, rs="rs1", primary=None):
        await self.until(lambda: self.ctl(rs).st.state == State.HEALTHY and
                         (primary is None or self.ctl(rs).primary == primary),
                         what=f"{rs} HEALTHY")


@pytest.fixture
async def sim_factory(tmp_path):
    made = []

    async def make(mode="dbguard", sets=None, spares=None, replicate=True, rejoin=None,
                   provision=False, start=True, **kw):
        sets = sets or {"rs1": A, "rs2": B}
        fleet = FakeFleet(sets, semisync=(mode == "dbguard"), spares=spares)
        if replicate:
            for rs in sets:
                fleet.setup_replication(rs)
        await fleet.start_agents()
        conf = dict(mode=mode, detect_window_s=0.4, probe_timeout_s=0.1, probe_failures=3,
                    fence_deadline_s=0.5, catchup_deadline_s=2, rebuild_after_s=0.6,
                    cooldown_s=1.0, poll_interval_s=0.05,
                    sets={rs: {"nodes": ns, "spare": (spares or {}).get(rs)}
                          for rs, ns in sets.items()})
        conf.update(kw)
        cfg = FleetConfig.model_validate(conf)
        mgr = Manager(cfg, addressing=Addressing(overrides=fleet.overrides()),
                      prober=FakeProber(fleet, cfg.probe_timeout_s),
                      provisioner=FakeProvisioner(fleet) if provision else None,
                      state_dir=str(tmp_path), rejoin=rejoin)
        if start:
            await mgr.start()
        s = Sim(fleet, mgr, cfg)
        made.append(s)
        return s

    yield make
    for s in made:
        await s.mgr.stop()
        await s.fleet.close()


def assert_contract(ev: Event):
    row = ev.row()
    assert CONTRACT_KEYS <= set(row)
    Event.model_validate(row)
    if row["type"] == "failover" and row["new_primary"]:
        st = row["steps"]
        for k in ("fence", "choose", "catchup", "promote", "repoint"):
            assert st[k] is not None and "duration_s" in st[k], k
        assert st["choose"]["winner"] == row["new_primary"]
        assert row["detect"]["replica_total"] == 2


def lossless(sim, rs="rs1"):
    ctl = sim.ctl(rs)
    new = sim.fleet.nodes[ctl.primary]
    lost = sim.fleet.acked[rs] - new.executed
    assert lost.is_empty, f"lost acknowledged writes {lost}"


# ------------------------------------------------------------------------------ scenarios


async def test_discover_and_bootstrap(sim_factory):
    s = await sim_factory(replicate=False)
    await s.healthy("rs1", "mysql-a1")
    await s.healthy("rs2", "mysql-b1")
    assert [e.type for e in s.events("rs1", "bootstrap")] == ["bootstrap"]
    assert s.fleet.nodes["mysql-a2"].source == "mysql-a1"


async def test_kill_primary_full_failover_and_repoint_rejoin(sim_factory):
    s = await sim_factory()
    await s.healthy("rs1", "mysql-a1")
    await s.healthy("rs2", "mysql-b1")
    s.fleet.writing = True
    await asyncio.sleep(0.3)
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover event")
    ev = s.events("rs1", "failover")[0]
    assert_contract(ev)
    assert ev.old_primary == "mysql-a1"
    assert ev.new_primary in ("mysql-a2", "mysql-a3")
    assert ev.trigger == "dead"
    assert ev.steps.fence.outcome == "unreachable"
    assert ev.steps.choose.subset_ok
    assert ev.steps.catchup.ok
    assert ev.detect.manager_probe_failed and ev.detect.replica_votes == 2
    assert ev.watermark_gtid
    new = ev.new_primary
    assert s.fleet.primary_of("rs1") == [new]
    other = ({"mysql-a2", "mysql-a3"} - {new}).pop()
    assert s.fleet.nodes[other].source == new
    await asyncio.sleep(0.2)
    lossless(s)
    # rs2 untouched
    assert s.ctl("rs2").primary == "mysql-b1"
    assert not s.events("rs2", "failover")
    # old primary comes back with nothing extra -> repoint branch
    s.fleet.start("mysql-a1")
    await s.until(lambda: s.events("rs1", "rejoin"), what="rejoin")
    rj = s.events("rs1", "rejoin")[0]
    assert rj.rejoin.branch == "repoint" and rj.rejoin.phantom_gtids == 0
    assert s.fleet.nodes["mysql-a1"].source == new
    await s.healthy("rs1", new)
    lossless(s)


async def test_rejoin_rebuild_discards_phantoms(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    await asyncio.sleep(0.2)
    a1 = s.fleet.nodes["mysql-a1"]
    # committed to a1's binlog, never reached a replica, so never acknowledged
    for r in ("mysql-a2", "mysql-a3"):
        s.fleet.nodes[r].receive = False
    s.fleet.writing = False
    a1.executed = a1.executed | GtidSet.of(a1.uuid, (a1.next_gno, a1.next_gno + 2))
    a1.next_gno += 3
    s.fleet.kill("mysql-a1")
    for r in ("mysql-a2", "mysql-a3"):
        s.fleet.nodes[r].receive = True
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    new = s.events("rs1", "failover")[0].new_primary
    s.fleet.start("mysql-a1")
    await s.until(lambda: any(e.rejoin and e.rejoin.branch == "rebuild"
                              for e in s.events("rs1", "rejoin")), what="rebuild")
    rj = [e for e in s.events("rs1", "rejoin") if e.rejoin.branch == "rebuild"][0]
    assert rj.rejoin.phantom_gtids == 3
    assert rj.clone.donor not in (new, "mysql-a1"), "clone donor must be a replica"
    await s.healthy("rs1", new)
    assert a1.executed.is_subset(s.fleet.nodes[new].executed)


async def test_rejoin_manual_does_nothing(sim_factory):
    s = await sim_factory(sets={"rs1": A}, rejoin="manual")
    await s.healthy("rs1", "mysql-a1")
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    s.fleet.start("mysql-a1")
    await s.until(lambda: s.events("rs1", "rejoin"), what="manual rejoin event")
    assert s.events("rs1", "rejoin")[0].rejoin.branch == "manual"
    await asyncio.sleep(0.3)
    assert "/repoint" not in s.fleet.nodes["mysql-a1"].calls
    assert s.ctl().st.state == State.DEGRADED


async def test_manager_partitioned_is_suspect_only(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    s.fleet.manager_cut.add("mysql-a1")
    await s.until(lambda: s.ctl().st.state == State.SUSPECT, what="SUSPECT")
    await asyncio.sleep(1.2)   # three detect windows
    assert not s.events("rs1", "failover")
    assert s.ctl().primary == "mysql-a1"
    s.fleet.heal()
    await s.healthy("rs1", "mysql-a1")


async def test_naive_fails_over_on_probe_alone(sim_factory):
    s = await sim_factory(mode="naive", sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.manager_cut.add("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="naive false failover")
    ev = s.events("rs1", "failover")[0]
    assert ev.mode == "naive" and ev.steps.fence.outcome == "skipped"
    assert ev.steps.catchup.duration_s == 0.0
    # naive did not fence: the old primary is still writable, a second writer, which the
    # rejoin pass then fences
    await s.until(lambda: s.fleet.nodes["mysql-a1"].fenced, what="second writer fenced")


async def test_replicas_partitioned_stalls_then_fails_over(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    await asyncio.sleep(0.2)
    s.fleet.partition("mysql-a1", "mysql-a2")
    s.fleet.partition("mysql-a1", "mysql-a3")
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    ev = s.events("rs1", "failover")[0]
    assert ev.trigger == "partition"
    assert ev.steps.fence.outcome == "sql"
    assert s.fleet.nodes["mysql-a1"].fenced
    assert any(e.type == "stall" for e in s.events("rs1"))
    lossless(s)


async def test_hung_primary_is_killed_by_fence(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    await asyncio.sleep(0.2)
    s.fleet.nodes["mysql-a1"].hung = True
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    ev = s.events("rs1", "failover")[0]
    assert ev.trigger == "hung"
    assert ev.steps.fence.outcome == "kill"
    lossless(s)
    # the killed mysqld comes back read-only and fenced; it rejoins as a replica
    await s.until(lambda: s.fleet.nodes["mysql-a1"].source == ev.new_primary, what="rejoin")


async def test_frozen_container_fails_over_unfenced(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.nodes["mysql-a1"].frozen = True
    await s.until(lambda: s.events("rs1", "failover"), timeout=8, what="failover")
    ev = s.events("rs1", "failover")[0]
    assert ev.steps.fence.outcome == "unreachable"
    assert ev.new_primary in ("mysql-a2", "mysql-a3")


async def test_diverged_replicas_halt(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    for n in ("mysql-a2", "mysql-a3"):
        node = s.fleet.nodes[n]
        node.executed = node.executed | GtidSet.of(node.uuid, 1)   # errant, each its own
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.ctl().st.state == State.HALTED, what="HALTED")
    assert s.ctl().st.halt_reason.startswith("replicas diverged")
    ev = s.events("rs1", "failover")[0]
    assert ev.new_primary is None and not ev.steps.choose.subset_ok
    assert s.fleet.primary_of("rs1") == []
    assert s.events("rs1", "halt")
    # doctor explains it and what to do
    d = __import__("dbguard.manager.doctor", fromlist=["doctor"]).doctor(s.ctl())
    assert d["verdict"] == "HALTED"
    assert any("Next:" in line for line in d["lines"])


async def test_catchup_waits_for_relay_log(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    for n in ("mysql-a2", "mysql-a3"):
        s.fleet.nodes[n].apply_per_step = 0         # SQL threads stall, relay log grows
    s.fleet.writing = True
    await asyncio.sleep(0.3)
    s.fleet.writing = False
    s.fleet.kill("mysql-a1")

    async def resume_apply():
        await asyncio.sleep(1.0)
        for n in ("mysql-a2", "mysql-a3"):
            s.fleet.nodes[n].apply_per_step = 1
    t = asyncio.create_task(resume_apply())
    await s.until(lambda: s.events("rs1", "failover"), timeout=8, what="failover")
    await t
    ev = s.events("rs1", "failover")[0]
    assert ev.steps.catchup.ok and ev.steps.catchup.duration_s > 0.3
    lossless(s)


async def test_catchup_deadline_halts(sim_factory):
    s = await sim_factory(sets={"rs1": A}, catchup_deadline_s=0.5)
    await s.healthy("rs1", "mysql-a1")
    for n in ("mysql-a2", "mysql-a3"):
        s.fleet.nodes[n].apply_per_step = 0
    s.fleet.writing = True
    await asyncio.sleep(0.2)
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.ctl().st.state == State.HALTED, what="HALTED")
    assert "catch-up" in s.ctl().st.halt_reason
    assert s.fleet.primary_of("rs1") == []


async def test_cooldown_blocks_second_automatic_failover(sim_factory):
    s = await sim_factory(sets={"rs1": A}, cooldown_s=3.0)
    await s.healthy("rs1", "mysql-a1")
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover 1")
    new = s.ctl().primary
    s.fleet.start("mysql-a1")
    await s.healthy("rs1", new)
    s.fleet.kill(new)
    await s.until(lambda: s.ctl().st.state == State.SUSPECT, what="SUSPECT in cooldown")
    assert len(s.events("rs1", "failover")) == 1
    await s.until(lambda: len(s.events("rs1", "failover")) == 2, timeout=6,
                  what="failover after cooldown")


async def test_planned_switchover_via_api(sim_factory):
    # Generous deadline: under a loaded full-suite run the simulation task can be starved
    # long enough to miss a 2 s catch-up deadline, which aborts the switchover by design.
    s = await sim_factory(sets={"rs1": A}, catchup_deadline_s=15)
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    a1 = s.fleet.nodes["mysql-a1"]
    start_gno = a1.next_gno
    await s.until(lambda: a1.next_gno > start_gno + 3, what="some writes")
    from aiohttp.test_utils import TestClient, TestServer
    async with TestClient(TestServer(make_app(s.mgr))) as c:
        r = await c.post("/v1/sets/rs1/failover", json={"to": "mysql-a3"})
        body = await r.json()
        assert r.status == 200, body
        ev = body["event"]
        assert ev["type"] == "switchover" and ev["new_primary"] == "mysql-a3"
        assert ev["trigger"] == "planned" and ev["steps"]["catchup"]["ok"]
        assert s.fleet.primary_of("rs1") == ["mysql-a3"]
        assert s.fleet.nodes["mysql-a1"].source == "mysql-a3"
        r = await c.get("/v1/sets/rs1/primary")
        assert (await r.json())["primary"] == "mysql-a3"
        # every write acknowledged before or after the switch is on the new primary
        a3 = s.fleet.nodes["mysql-a3"]
        await s.until(lambda: s.fleet.acked["rs1"].is_subset(a3.executed),
                      what="acked writes on mysql-a3")
        lossless(s)
        # switching to a node that is not a member fails cleanly
        r = await c.post("/v1/sets/rs1/failover", json={"to": "mysql-b1"})
        assert r.status == 409


async def test_api_routes(sim_factory):
    s = await sim_factory()
    await s.healthy("rs1", "mysql-a1")
    await s.healthy("rs2", "mysql-b1")
    from aiohttp.test_utils import TestClient, TestServer
    async with TestClient(TestServer(make_app(s.mgr))) as c:
        st = await (await c.get("/v1/status")).json()
        assert st["mode"] == "dbguard" and set(st["sets"]) == {"rs1", "rs2"}
        rs1 = st["sets"]["rs1"]
        for k in ("rs", "state", "halt_reason", "primary", "since", "nodes", "last_event",
                  "failovers_total", "cooldown_until"):
            assert k in rs1
        assert rs1["nodes"]["mysql-a1"]["role"] == "primary"
        assert rs1["nodes"]["mysql-a2"]["role"] == "replica"
        assert rs1["nodes"]["mysql-a2"]["semisync"] == "replica"
        d = await (await c.get("/v1/sets/rs1/doctor")).json()
        assert d["verdict"] == "HEALTHY" and d["lines"] and set(d["gaps"]) >= {"mysql-a2"}
        r = await c.post("/v1/sets/rs1/halt")
        assert (await r.json())["state"] == "HALTED"
        r = await c.post("/v1/sets/rs1/resume")
        assert (await r.json())["state"] == "HEALTHY"
        evs = (await (await c.get("/v1/events?rs=rs1")).json())["events"]
        assert [e["type"] for e in evs][-2:] == ["halt", "resume"]
        since = evs[-1]["ts"]
        assert (await (await c.get(f"/v1/events?since={since}")).json())["events"] == []
        m = await (await c.get("/metrics")).text()
        for name in ("dbguard_set_state", "dbguard_failovers_total",
                     "dbguard_heartbeat_age_seconds", "dbguard_semisync_avg_wait_us"):
            assert name in m
        assert (await c.get("/health")).status == 200
        assert (await c.get("/v1/sets/nope")).status == 404


async def test_halted_set_takes_no_action(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.ctl().st.halt("operator")
    s.fleet.kill("mysql-a1")
    await asyncio.sleep(1.0)
    assert not s.events("rs1", "failover")


async def test_replica_loss_degraded_then_replaced_by_spare(sim_factory):
    s = await sim_factory(sets={"rs1": A}, spares={"rs1": "mysql-a4"}, provision=True)
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    s.fleet.kill("mysql-a3")
    await s.until(lambda: s.events("rs1", "replace"), timeout=6, what="replace")
    assert s.events("rs1", "degraded")
    ev = s.events("rs1", "replace")[0]
    assert ev.clone.bytes == 1_000_000 and ev.clone.donor == "mysql-a2"
    assert "mysql-a4" in s.ctl().members
    await s.healthy("rs1", "mysql-a1")
    assert s.fleet.nodes["mysql-a4"].source == "mysql-a1"


async def test_straggler_repointed(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.nodes["mysql-a3"].source = "mysql-a2"      # Experiment 4 style confusion
    await s.until(lambda: s.fleet.nodes["mysql-a3"].source == "mysql-a1", what="repoint")
    assert any(e.rejoin and e.rejoin.branch == "repoint" for e in s.events("rs1", "rejoin"))


async def test_agent_status_timeout_never_stalls_loop(sim_factory):
    """A frozen replica agent costs its 1 s timeout per poll, and the other set keeps
    polling at its own pace."""
    s = await sim_factory()
    await s.healthy("rs1", "mysql-a1")
    s.fleet.nodes["mysql-b2"].frozen = True
    t0 = s.ctl("rs1").last_obs.ts
    await asyncio.sleep(0.5)
    assert s.ctl("rs1").last_obs.ts - t0 > 0.3
    await s.until(lambda: s.ctl("rs2").st.state == State.DEGRADED, timeout=6,
                  what="rs2 DEGRADED")
    assert s.ctl("rs1").st.state == State.HEALTHY


async def test_doctor_sentence_for_a_dead_primary(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    await asyncio.sleep(0.2)
    s.fleet.nodes["mysql-a2"].receive = False     # a3 will be ahead
    await asyncio.sleep(0.2)
    s.ctl().st.halt("operator wants to look first")
    s.fleet.kill("mysql-a1")
    await asyncio.sleep(1.0)
    from dbguard.manager.doctor import doctor
    d = doctor(s.ctl())
    first = d["lines"][0]
    assert first.startswith("rs1: primary mysql-a1 unreachable from manager for ")
    assert "2 of 2 replicas report" in first
    assert "would fence and promote mysql-a3 (retrieved set is" in first
    assert "ahead of mysql-a2)" in first
    assert d["verdict"] == "HALTED"
    assert any("HALTED" in line and "operator wants to look first" in line
               for line in d["lines"])


async def test_degraded_set_fails_over_to_its_last_replica(sim_factory):
    """kill-two: a replica dies, then the primary. The survivor is the only witness."""
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    s.fleet.kill("mysql-a3")
    await s.until(lambda: s.ctl().st.state == State.DEGRADED, what="DEGRADED")
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    ev = s.events("rs1", "failover")[0]
    assert ev.new_primary == "mysql-a2"
    assert ev.detect.replica_votes == 1 and ev.detect.replica_total == 1
    lossless(s)


async def test_cold_start_promotes_in_place(sim_factory):
    """Whole set rebooted: every node read-only, replicas still point at mysql-a1, the
    heartbeat row is hours old. Promote mysql-a1 in place, never fail over."""
    fleet_sets = {"rs1": A}
    s = await sim_factory(sets=fleet_sets, replicate=False, start=False)
    s.fleet.setup_replication("rs1")
    a1 = s.fleet.nodes["mysql-a1"]
    a1.super_read_only = True
    a1.semisync_source = False
    for n in A:
        s.fleet.nodes[n].heartbeat_ts = time.time() - 32545
    await s.mgr.start()
    await s.healthy("rs1", "mysql-a1")
    assert [e.type for e in s.events("rs1", "cold_start")] == ["cold_start"]
    assert not s.events("rs1", "failover")
    assert s.fleet.primary_of("rs1") == ["mysql-a1"]


async def test_cold_start_refused_when_a_replica_is_ahead(sim_factory):
    s = await sim_factory(sets={"rs1": A}, replicate=False, start=False)
    s.fleet.setup_replication("rs1")
    a1, a2 = s.fleet.nodes["mysql-a1"], s.fleet.nodes["mysql-a2"]
    a1.super_read_only = True
    a2.executed = a2.executed | GtidSet.of(a1.uuid, (1, 5))    # a2 holds more than a1
    await s.mgr.start()
    await asyncio.sleep(0.5)
    assert not s.events("rs1", "cold_start")
    assert "/promote" not in a1.calls


async def test_spare_already_replicating_is_adopted_after_restart(sim_factory):
    s = await sim_factory(sets={"rs1": A}, spares={"rs1": "mysql-a4"}, start=False)
    a4 = s.fleet.nodes["mysql-a4"]
    s.fleet.start("mysql-a4")
    a4.source, a4.semisync_replica = "mysql-a1", True
    await s.mgr.start()
    await s.healthy("rs1", "mysql-a1")
    await s.until(lambda: "mysql-a4" in s.ctl().members, what="adopted")
    assert s.ctl().status()["nodes"]["mysql-a4"]["role"] == "replica"


async def test_switchover_refuses_candidate_with_errant_transactions(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    a3 = s.fleet.nodes["mysql-a3"]
    a3.executed = a3.executed | GtidSet.of(a3.uuid, 1)
    from dbguard.manager.switchover import SwitchoverError, switchover
    with pytest.raises(SwitchoverError, match="never had"):
        await switchover(s.ctl(), "mysql-a3")
    assert s.fleet.primary_of("rs1") == ["mysql-a1"]     # rolled back, still writable
    assert s.ctl().primary == "mysql-a1"
