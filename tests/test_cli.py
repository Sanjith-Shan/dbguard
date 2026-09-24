"""dbgctl against an aiohttp server that serves contract-shaped JSON (docs/INTERFACES.md)."""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time

import pytest
from aiohttp import web
from click.testing import CliRunner

from dbguard.cli.dbgctl import cli, event_line, parse_since

NOW = time.time()

EVENT = {
    "ts": NOW - 30, "rs": "rs1", "type": "failover", "mode": "dbguard",
    "old_primary": "mysql-a1", "new_primary": "mysql-a3", "trigger": "dead",
    "detect": {"manager_probe_failed": True, "replica_votes": 2, "replica_total": 2, "duration_s": 5.1},
    "steps": {"fence": {"duration_s": 3.0, "outcome": "unreachable"},
              "choose": {"duration_s": 0.02, "candidates": ["mysql-a2", "mysql-a3"],
                         "winner": "mysql-a3", "subset_ok": True},
              "catchup": {"duration_s": 0.1, "ok": True}, "promote": {"duration_s": 0.05},
              "repoint": {"duration_s": 0.1, "nodes": ["mysql-a2"]}},
    "total_s": 8.4, "watermark_gtid": "u:1-100", "rejoin": None, "note": None,
}
REJOIN = dict(EVENT, ts=NOW - 10, type="rejoin", trigger="dead",
              rejoin={"branch": "rebuild", "phantom_gtids": 2, "duration_s": 14.2})


def node(role, lag=0.0, ss="replica"):
    return {"role": role, "reachable": role != "down", "gtid_executed": "u:1-100", "lag_s": lag,
            "heartbeat_age_s": 0.4, "semisync": ss, "io_running": "Yes", "sql_running": "Yes"}


SET_RS1 = {"rs": "rs1", "state": "HEALTHY", "halt_reason": None, "primary": "mysql-a3",
           "since": NOW - 5,
           "nodes": {"mysql-a1": node("replica", 0.2), "mysql-a2": node("replica", 1.5),
                     "mysql-a3": node("primary", None, "source")},
           "last_event": REJOIN, "failovers_total": 1, "cooldown_until": None}
SET_RS2 = {"rs": "rs2", "state": "HALTED", "halt_reason": "replicas diverged",
           "primary": "mysql-b1", "since": NOW - 100,
           "nodes": {"mysql-b1": node("primary", None, "source"), "mysql-b2": node("down"),
                     "mysql-b3": node("replica")},
           "last_event": None, "failovers_total": 0, "cooldown_until": None}


class FakeManager:
    def __init__(self):
        self.calls: list[tuple[str, str, object]] = []
        self.events = [EVENT, REJOIN]

    def app(self) -> web.Application:
        app = web.Application()
        r = app.router
        r.add_get("/v1/status", self.status)
        r.add_get("/v1/sets/{rs}", self.get_set)
        r.add_get("/v1/sets/{rs}/doctor", self.doctor)
        r.add_post("/v1/sets/{rs}/failover", self.failover)
        r.add_post("/v1/sets/{rs}/halt", self.halt)
        r.add_post("/v1/sets/{rs}/resume", self.resume)
        r.add_post("/v1/sets/{rs}/rejoin", self.rejoin)
        r.add_get("/v1/events", self.get_events)
        return app

    async def status(self, req):
        return web.json_response({"mode": "dbguard", "sets": {"rs1": SET_RS1, "rs2": SET_RS2}})

    async def get_set(self, req):
        rs = req.match_info["rs"]
        if rs not in ("rs1", "rs2"):
            return web.json_response({"error": "unknown set"}, status=404)
        return web.json_response(SET_RS1 if rs == "rs1" else SET_RS2)

    async def doctor(self, req):
        if req.match_info["rs"] not in ("rs1", "rs2"):
            return web.json_response({"error": "unknown set"}, status=404)
        return web.json_response({
            "lines": ["rs1: primary mysql-a3 healthy, 2 of 2 replicas streaming.",
                      "mysql-a2 is 3 transactions behind."],
            "verdict": "HEALTHY", "gaps": {"mysql-a1": "", "mysql-a2": "u:98-100"}})

    async def failover(self, req):
        body = await req.json()
        self.calls.append(("failover", req.match_info["rs"], body))
        ev = dict(EVENT, type="switchover", trigger="planned", new_primary=body.get("to") or "mysql-a2")
        return web.json_response({"event": ev})

    async def halt(self, req):
        self.calls.append(("halt", req.match_info["rs"], None))
        return web.json_response({"state": "HALTED"})

    async def resume(self, req):
        self.calls.append(("resume", req.match_info["rs"], None))
        return web.json_response({"state": "HEALTHY"})

    async def rejoin(self, req):
        body = await req.json()
        self.calls.append(("rejoin", req.match_info["rs"], body))
        return web.json_response({"event": REJOIN})

    async def get_events(self, req):
        rs = req.query.get("rs")
        since = float(req.query["since"]) if "since" in req.query else None
        evs = [e for e in self.events if (not rs or e["rs"] == rs) and (since is None or e["ts"] >= since)]
        return web.json_response({"events": evs})


@pytest.fixture(scope="module")
def server():
    fake = FakeManager()
    loop = asyncio.new_event_loop()
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    runner = web.AppRunner(fake.app())
    started = threading.Event()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner.setup())
        loop.run_until_complete(web.TCPSite(runner, "127.0.0.1", port).start())
        started.set()
        loop.run_forever()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    started.wait(5)
    yield fake, f"http://127.0.0.1:{port}"
    loop.call_soon_threadsafe(loop.stop)
    t.join(5)


def run(url, *args):
    return CliRunner().invoke(cli, ["--manager", url, *args], catch_exceptions=False)


def test_status_table(server):
    _, url = server
    r = run(url, "status")
    assert r.exit_code == 0, r.output
    out = r.output
    assert "mode: dbguard" in out
    assert "rs1" in out and "HEALTHY" in out and "mysql-a3" in out
    assert "mysql-a2(lag 1.5s, ss replica)" in out
    assert "mysql-b2(down)" in out
    assert "HALTED [replicas diverged]" in out
    assert "rejoin=rebuild phantom=2" in out


def test_status_json(server):
    _, url = server
    r = run(url, "status", "--json")
    assert json.loads(r.output)["sets"]["rs2"]["state"] == "HALTED"


def test_doctor(server):
    _, url = server
    r = run(url, "doctor", "rs1")
    assert r.exit_code == 0
    assert "2 of 2 replicas streaming" in r.output
    assert "mysql-a2: u:98-100" in r.output
    assert "mysql-a1: none" in r.output
    assert "verdict: HEALTHY" in r.output


def test_failover_halt_resume_rejoin(server):
    fake, url = server
    fake.calls.clear()
    r = run(url, "failover", "rs1", "--to", "mysql-a1")
    assert r.exit_code == 0 and "switchover" in r.output and "mysql-a1" in r.output
    assert run(url, "halt", "rs1").output.strip() == "rs1: state HALTED"
    assert run(url, "resume", "rs1").output.strip() == "rs1: state HEALTHY"
    assert "rejoin=rebuild" in run(url, "rejoin", "rs1", "mysql-a1").output
    assert fake.calls == [("failover", "rs1", {"to": "mysql-a1"}), ("halt", "rs1", None),
                          ("resume", "rs1", None), ("rejoin", "rs1", {"node": "mysql-a1"})]


def test_failover_default_target(server):
    fake, url = server
    fake.calls.clear()
    run(url, "failover", "rs2")
    assert fake.calls == [("failover", "rs2", {"to": None})]


def test_events_filters(server):
    _, url = server
    r = run(url, "events", "--rs", "rs1", "--since", "20s", "--json")
    lines = [json.loads(x) for x in r.output.splitlines()]
    assert [e["type"] for e in lines] == ["rejoin"]
    r = run(url, "events")
    assert "failover mysql-a1 -> mysql-a3 trigger=dead fence=unreachable total=8.40s" in r.output


def test_unreachable_manager_exit_code():
    r = CliRunner().invoke(cli, ["--manager", "http://127.0.0.1:9", "status"])
    assert r.exit_code == 2
    assert "dbgctl:" in r.output


def test_unknown_set_is_error(server):
    _, url = server
    r = CliRunner().invoke(cli, ["--manager", url, "doctor", "rs9"])
    assert r.exit_code == 1


def test_helpers():
    assert event_line(None) == "-"
    assert abs(parse_since("10m") - (time.time() - 600)) < 2
    assert parse_since("123.5") == 123.5
