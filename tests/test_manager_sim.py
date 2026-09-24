"""End-to-end manager scenarios against the in-process FakeFleet (real HTTP, fake mysqld)."""

from __future__ import annotations

import asyncio
import time

import pytest

from dbguard.events import Event
from dbguard.gtid import GtidSet
from dbguard.manager.api import make_app
from dbguard.manager.client import Addressing
from dbguard.config import FleetConfig
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
    rjs = s.events("rs1", "rejoin")
    assert [e.rejoin.node for e in rjs] == ["mysql-a1"], "only the old primary rejoins"
    rj = rjs[0]
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
    # naive has no fence: the old primary stays writable, a split brain that is recorded
    # and left alone for the harness to measure
    await s.until(lambda: s.events("rs1", "split_brain"), what="split_brain event")
    await asyncio.sleep(0.3)
    assert not s.fleet.nodes["mysql-a1"].fenced
    assert "/fence" not in s.fleet.nodes["mysql-a1"].calls
    assert len(s.events("rs1", "split_brain")) == 1
    assert s.fleet.primary_of("rs1") == ["mysql-a1", ev.new_primary]


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
        await s.until(lambda: s.fleet.nodes["mysql-a1"].source == "mysql-a3",
                      what="old primary repointed in the background")
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


async def test_cold_start_promotes_the_superset_when_the_old_primary_is_behind(sim_factory):
    s = await sim_factory(sets={"rs1": A}, replicate=False, start=False)
    s.fleet.setup_replication("rs1")
    a1, a2 = s.fleet.nodes["mysql-a1"], s.fleet.nodes["mysql-a2"]
    a1.super_read_only = True
    a2.executed = a2.executed | GtidSet.of(a1.uuid, (1, 5))    # a2 holds more than a1
    await s.mgr.start()
    await s.until(lambda: s.events("rs1", "cold_start"), what="cold_start")
    assert s.events("rs1", "cold_start")[0].new_primary == "mysql-a2"
    assert "/promote" not in a1.calls
    await s.healthy("rs1", "mysql-a2")

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


# ------------------------------------------------------- review 2026-09-23 regressions


async def test_slow_status_replicas_still_witness(sim_factory):
    """REVIEW #3: a /status taking 1.5 s (loaded agent, TCP probe behind a DROP) must not
    make replicas vanish as witnesses and candidates."""
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    for n in ("mysql-a2", "mysql-a3"):
        s.fleet.nodes[n].status_delay = 1.5
    await asyncio.sleep(1.8)
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), timeout=10, what="failover")
    ev = s.events("rs1", "failover")[0]
    assert ev.new_primary in ("mysql-a2", "mysql-a3")
    assert ev.detect.replica_total == 2


async def test_no_replacement_during_cooldown(sim_factory):
    """REVIEW #2: after a failover the set is DEGRADED, but a spare must not be provisioned
    while the old primary may still come back (cooldown)."""
    s = await sim_factory(sets={"rs1": A}, spares={"rs1": "mysql-a4"}, provision=True,
                          rebuild_after_s=0.2, cooldown_s=2.0)
    await s.healthy("rs1", "mysql-a1")
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    t_fo = s.events("rs1", "failover")[0].ts
    await s.until(lambda: s.events("rs1", "replace"), timeout=6, what="replace after cooldown")
    assert s.events("rs1", "replace")[0].ts - t_fo >= 1.9


async def test_io_threads_stopped_before_choosing(sim_factory):
    """REVIEW #4: candidates' IO threads are stopped before their sets are compared."""
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    ev = s.events("rs1", "failover")[0]
    assert sorted(s.mgr.prober.stopped_io) == ["mysql-a2", "mysql-a3"]
    assert ev.steps.choose.io_stopped == ["mysql-a2", "mysql-a3"]
    loser = ({"mysql-a2", "mysql-a3"} - {ev.new_primary}).pop()
    assert not s.fleet.nodes[loser].io_stopped, "repoint restarts the IO thread"


async def test_naive_does_not_stop_io_threads(sim_factory):
    s = await sim_factory(mode="naive", sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    assert s.mgr.prober.stopped_io == []


async def test_role_change_retried_once_on_lost_link(sim_factory):
    """REVIEW #8: the fence killed the agent's pooled link, /repoint answers 500 2013 once."""
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    loser_links = {"/repoint": 1}
    for n in ("mysql-a2", "mysql-a3"):
        s.fleet.nodes[n].fail_next = dict(loser_links)
        s.fleet.nodes[n].fail_next["/promote"] = 1
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    ev = s.events("rs1", "failover")[0]
    assert ev.new_primary and ev.steps.repoint.nodes, ev.note
    assert ev.note is None or "repoint failed" not in ev.note


async def test_catchup_accepts_partial_trailing_transaction(sim_factory):
    """REVIEW #9: the source died mid-send, the relay log ends in a GTID that can never be
    applied. With the IO thread stopped and the SQL thread idle, that is caught up."""
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    await asyncio.sleep(0.2)
    s.fleet.writing = False
    await asyncio.sleep(0.2)
    a1 = s.fleet.nodes["mysql-a1"]
    torn = GtidSet.of(a1.uuid, a1.next_gno)
    for n in ("mysql-a2", "mysql-a3"):
        r = s.fleet.nodes[n]
        r.receive = False
        r.retrieved = r.retrieved | torn
        r.partial = torn
    s.fleet.kill("mysql-a1")
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    ev = s.events("rs1", "failover")[0]
    assert ev.new_primary and ev.steps.catchup.ok
    assert ev.steps.catchup.duration_s < 1.5
    assert "partial transaction" in ev.note


async def test_slow_rejoin_repoint_does_not_pause_detection(sim_factory):
    """REVIEW #10: a rejoin repoint that takes seconds runs in the background."""
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    a3 = s.fleet.nodes["mysql-a3"]
    a3.repoint_delay = 2.0
    a3.source = "mysql-a2"                       # straggler, needs a repoint
    await s.until(lambda: "/repoint" in a3.calls, what="repoint started")
    t0 = s.ctl().last_obs.ts
    await asyncio.sleep(0.8)
    assert s.ctl().last_obs.ts - t0 > 0.5, "the poll loop kept running"
    await s.until(lambda: a3.source == "mysql-a1", what="repoint done")


async def test_wake_guard_gets_unknown_while_discovering(sim_factory):
    """REVIEW #16: before discovery the answer is 503 unknown, never null."""
    s = await sim_factory(sets={"rs1": A}, start=False)
    from aiohttp.test_utils import TestClient, TestServer
    async with TestClient(TestServer(make_app(s.mgr))) as c:
        r = await c.get("/v1/sets/rs1/primary")
        assert r.status == 503 and (await r.json())["primary"] == "unknown"
        await s.mgr.start()
        await s.healthy("rs1", "mysql-a1")
        r = await c.get("/v1/sets/rs1/primary")
        assert r.status == 200 and (await r.json())["primary"] == "mysql-a1"


def test_role_change_budget_exceeds_agent_budget():
    """REVIEW #17: the agent bounds a whole role change to 30 s, the manager waits longer."""
    from dbguard.manager.failover import ROLE_CHANGE_TIMEOUT_S
    assert ROLE_CHANGE_TIMEOUT_S > 30.0


async def test_slow_fence_still_precedes_promote(sim_factory):
    """The fence runs concurrently with choose/catch-up/repoint, but nothing is promoted
    before it has finished."""
    s = await sim_factory(sets={"rs1": A}, fence_deadline_s=2.0)
    await s.healthy("rs1", "mysql-a1")
    s.fleet.nodes["mysql-a1"].fence_delay = 0.8
    s.fleet.partition("mysql-a1", "mysql-a2")
    s.fleet.partition("mysql-a1", "mysql-a3")
    s.fleet.writing = True
    await s.until(lambda: s.events("rs1", "failover"), what="failover")
    ev = s.events("rs1", "failover")[0]
    order = [what for _, _, what in s.fleet.log]
    assert order.index("fenced") < order.index("promoted")
    assert ev.steps.fence.outcome == "sql" and ev.steps.fence.duration_s >= 0.8
    lossless(s)


async def test_discovering_until_a_primary_is_found(sim_factory):
    """Fresh fleet, mysqld still initialising: every node down. The set must say
    DISCOVERING with primary null, never HEALTHY, until discovery finds a primary."""
    s = await sim_factory(sets={"rs1": A}, start=False)
    for n in A:
        s.fleet.nodes[n].mysqld_up = False
    await s.mgr.start()
    await asyncio.sleep(0.6)
    st = s.ctl().status()
    assert st["state"] == "DISCOVERING" and st["primary"] is None
    assert all(v["role"] == "down" for v in st["nodes"].values())
    from dbguard.manager.doctor import doctor
    d = doctor(s.ctl())
    assert d["lines"][0] == "rs1: no primary found yet, 0 of 3 nodes reachable, state DISCOVERING."
    assert d["verdict"] == "DISCOVERING"
    assert not s.events("rs1", "healthy")
    for n in A:
        s.fleet.nodes[n].mysqld_up = True
    await s.healthy("rs1", "mysql-a1")
    assert s.ctl().status()["primary"] == "mysql-a1"


async def test_healthy_requires_streaming_replicas(sim_factory):
    """Primary found but no replica streams: DEGRADED, not HEALTHY. (Naive mode, so the
    primary keeps accepting writes without replicas and the probe stays green.)"""
    s = await sim_factory(mode="naive", sets={"rs1": A}, start=False)
    for n in ("mysql-a2", "mysql-a3"):
        s.fleet.nodes[n].frozen = True
    await s.mgr.start()
    await s.until(lambda: s.ctl().primary == "mysql-a1", what="discovered")
    await s.until(lambda: s.ctl().st.state == State.DEGRADED, what="DEGRADED")
    assert not s.events("rs1", "healthy")


async def test_switchover_order_keeps_the_stall_short(sim_factory):
    """Replicas move under the candidate BEFORE the quiesce, the stall is quiesce + catch-up +
    promote only, and the old primary is repointed after the stall."""
    s = await sim_factory(sets={"rs1": A}, catchup_deadline_s=15)
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    a1 = s.fleet.nodes["mysql-a1"]
    n0 = a1.next_gno
    await s.until(lambda: a1.next_gno > n0 + 3, what="writes")
    s.fleet.nodes["mysql-a1"].repoint_delay = 0.5      # a slow START REPLICA on the old primary
    s.fleet.log.clear()
    from dbguard.manager.switchover import switchover
    ev = await switchover(s.ctl(), "mysql-a3")
    log = [(n, what) for _, n, what in s.fleet.log]
    assert log.index(("mysql-a2", "repointed")) < log.index(("mysql-a1", "fenced"))
    assert log.index(("mysql-a1", "fenced")) < log.index(("mysql-a3", "promoted"))
    assert ("mysql-a1", "repointed") not in log[: log.index(("mysql-a3", "promoted"))]
    assert ev.steps.prepare.nodes == ["mysql-a2"]
    assert ev.stall_s is not None and ev.stall_s < 0.5, ev.stall_s
    assert ev.stall_s <= ev.total_s
    for k in ("fence", "catchup", "promote"):
        assert getattr(ev.steps, k).duration_s <= ev.stall_s
    await s.until(lambda: a1.source == "mysql-a3", what="old primary repointed after")
    await s.until(lambda: any(e.rejoin and e.rejoin.node == "mysql-a1"
                              for e in s.events("rs1", "rejoin")), what="rejoin event")
    await s.until(lambda: s.fleet.acked["rs1"].is_subset(s.fleet.nodes["mysql-a3"].executed),
                  what="lossless")


async def test_switchover_two_node_set_repoints_old_primary_inside_the_stall(sim_factory):
    s = await sim_factory(sets={"rs1": ["mysql-a1", "mysql-a2"]})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.log.clear()
    from dbguard.manager.switchover import switchover
    ev = await switchover(s.ctl(), "mysql-a2")
    log = [(n, what) for _, n, what in s.fleet.log]
    assert log.index(("mysql-a1", "repointed")) < log.index(("mysql-a2", "promoted"))
    assert ev.steps.prepare is None and ev.steps.repoint.nodes == ["mysql-a1"]
    assert "inside the stall" in ev.note



def _whole_set_restart(fleet, minutes_old=5.0):
    """What every node looks like after Docker restarted the whole fleet on its volumes."""
    for rs in fleet.sets:
        fleet.setup_replication(rs)
        p = fleet.nodes[fleet.sets[rs][0]]
        p.super_read_only = True
        p.semisync_source = False
        for n in fleet.sets[rs]:
            fleet.nodes[n].heartbeat_ts = time.time() - minutes_old * 60


async def test_cold_start_when_the_old_primary_boots_last(sim_factory):
    """The real incident: after a Docker Desktop crash every node booted read-only, the
    heartbeat row was minutes old, the manager's probe got 1290 and both replicas 'reported
    losing it'. The source was not answering yet at discovery, so the one-shot cold start
    rule never fired and both sets sat SUSPECT for 180 s."""
    # detect window long enough that the source boots within it, as on the real fleet
    s = await sim_factory(replicate=False, start=False, detect_window_s=3.0)
    _whole_set_restart(s.fleet)
    for p in ("mysql-a1", "mysql-b1"):
        s.fleet.nodes[p].mysqld_up = False          # the source is still booting
    await s.mgr.start()
    await asyncio.sleep(0.5)
    assert s.ctl("rs1").primary == "mysql-a1"       # believed from the replicas
    for p in ("mysql-a1", "mysql-b1"):
        s.fleet.nodes[p].mysqld_up = True
    t0 = time.time()
    await s.until(lambda: s.events("rs1", "cold_start") and s.events("rs2", "cold_start"),
                  timeout=3, what="cold_start on both sets")
    assert time.time() - t0 < 1.5
    for rs, p in (("rs1", "mysql-a1"), ("rs2", "mysql-b1")):
        await s.healthy(rs, p)
        assert not s.events(rs, "failover")
        assert s.fleet.primary_of(rs) == [p]


async def test_cold_start_ignores_io_connecting_and_stale_heartbeat_votes(sim_factory):
    s = await sim_factory(sets={"rs1": A}, replicate=False, start=False)
    _whole_set_restart(s.fleet)
    s.fleet.partition("mysql-a1", "mysql-a2")       # IO threads stuck Connecting
    s.fleet.partition("mysql-a1", "mysql-a3")
    await s.mgr.start()
    await s.until(lambda: s.events("rs1", "cold_start"), timeout=3, what="cold_start")
    assert s.events("rs1", "cold_start")[0].new_primary == "mysql-a1"
    assert not s.events("rs1", "failover")


async def test_cold_start_halts_when_no_node_holds_everything(sim_factory):
    s = await sim_factory(sets={"rs1": A}, replicate=False, start=False)
    _whole_set_restart(s.fleet)
    for n in ("mysql-a2", "mysql-a3"):
        node = s.fleet.nodes[n]
        node.executed = node.executed | GtidSet.of(node.uuid, 1)
    await s.mgr.start()
    await s.until(lambda: s.ctl().st.state == State.HALTED, timeout=3, what="HALTED")
    assert s.ctl().st.halt_reason.startswith("cold start: every node is read-only")
    assert s.fleet.primary_of("rs1") == []


async def test_doctor_calls_a_read_only_set_a_whole_set_restart(sim_factory):
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.ctl().st.halt("look first")                    # keep the manager from acting
    a1 = s.fleet.nodes["mysql-a1"]
    a1.super_read_only = True
    await asyncio.sleep(0.4)
    from dbguard.manager.doctor import doctor
    first = doctor(s.ctl())["lines"][0]
    assert "no node is writable, this looks like a whole-set restart" in first


async def test_crash_restarted_primary_without_fence_is_promoted_in_place(sim_factory):
    """mysqld crashed and the agent restarted it read-only, not fenced: nobody is writable,
    so it is a one-node cold start, not a failover."""
    s = await sim_factory(sets={"rs1": A})
    await s.healthy("rs1", "mysql-a1")
    s.fleet.writing = True
    await asyncio.sleep(0.2)
    s.fleet.start("mysql-a1")                        # boots read-only, config kept
    await s.until(lambda: s.events("rs1", "cold_start"), timeout=3, what="cold_start")
    assert not s.events("rs1", "failover")
    await s.healthy("rs1", "mysql-a1")
    lossless(s)


async def test_cold_start_when_replication_is_not_running_yet(sim_factory):
    """The rs2 case after the reboot: rs1 got its cold start, rs2 stayed read-only for 4
    minutes because its replicas were not replicating from mysql-b1 yet (IO thread not
    started on one, Connecting on the other, and at first not answering at all). No node is
    writable and every set is contained in mysql-b1's, so it is a cold start, on a later
    tick if not the first."""
    s = await sim_factory(sets={"rs2": B}, replicate=False, start=False, detect_window_s=3.0)
    _whole_set_restart(s.fleet)
    b2, b3 = s.fleet.nodes["mysql-b2"], s.fleet.nodes["mysql-b3"]
    b2.io_stopped = True                              # IO thread not started
    s.fleet.partition("mysql-b1", "mysql-b3")         # IO thread Connecting
    b2.mysqld_up = b3.mysqld_up = False               # replicas still booting at first
    await s.mgr.start()
    await asyncio.sleep(0.4)
    assert not s.events("rs2", "cold_start")
    b2.mysqld_up = b3.mysqld_up = True
    await s.until(lambda: s.events("rs2", "cold_start"), timeout=2, what="cold_start rs2")
    ev = s.events("rs2", "cold_start")[0]
    assert ev.new_primary == "mysql-b1"
    assert s.fleet.primary_of("rs2") == ["mysql-b1"]
    assert not s.events("rs2", "failover")
