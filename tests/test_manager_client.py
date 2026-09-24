"""Addressing, host maps, the compose provisioner command, event log round trip."""

from __future__ import annotations

from dbguard.events import Event, EventLog, Rejoin
from dbguard.manager.client import Addressing, published_ports
from dbguard.manager.replacement import ComposeProvisioner


def test_published_ports_pattern():
    assert published_ports("mysql-a1") == (13311, 18011)
    assert published_ports("mysql-a4") == (13314, 18014)
    assert published_ports("mysql-b3") == (13323, 18023)
    assert published_ports("haproxy") is None


def test_addressing_default_host_ports_and_map():
    a = Addressing()
    assert a.mysql_addr("mysql-a1") == ("mysql-a1", 3306)
    assert a.agent_url("mysql-a1") == "http://mysql-a1:8080"
    h = Addressing(host_ports=True)
    assert h.mysql_addr("mysql-b2") == ("127.0.0.1", 13322)
    assert h.agent_url("mysql-b2") == "http://127.0.0.1:18022"
    m = Addressing.from_env(8080, 3306, host_ports=True,
                            env={"DBGUARD_HOST_MAP": "mysql-a1=10.0.0.5:3307:9000"})
    assert m.mysql_addr("mysql-a1") == ("10.0.0.5", 3307)
    assert m.agent_url("mysql-a1") == "http://10.0.0.5:9000"
    assert m.agent_url("mysql-a2") == "http://127.0.0.1:18012"


def test_compose_provisioner_command():
    p = ComposeProvisioner(compose_file="/deploy/docker-compose.yml")
    assert p.command("mysql-a4") == ["docker", "compose", "-f", "/deploy/docker-compose.yml",
                                     "--profile", "spare", "up", "-d", "--no-recreate",
                                     "mysql-a4"]


def test_event_log_roundtrip_and_query(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    log.append(Event(rs="rs1", type="suspect", ts=1.0))
    log.append(Event(rs="rs2", type="healthy", ts=2.0))
    log.append(Event(rs="rs1", type="rejoin", ts=3.0,
                     rejoin=Rejoin(branch="rebuild", phantom_gtids=4, node="mysql-a1")))
    with open(tmp_path / "events.jsonl", "a") as f:
        f.write('{"torn": ')                       # a crash mid-write
    again = EventLog(tmp_path / "events.jsonl")
    assert [e.type for e in again.query(rs="rs1")] == ["suspect", "rejoin"]
    assert [e.ts for e in again.query(since=1.5)] == [2.0, 3.0]
    assert again.last("rs1").rejoin.phantom_gtids == 4
    again.append(Event(rs="rs1", type="healthy", ts=4.0))
    assert [e.ts for e in EventLog(tmp_path / "events.jsonl").query()] == [1.0, 2.0, 3.0, 4.0]
