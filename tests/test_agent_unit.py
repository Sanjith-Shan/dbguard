"""Unit tests for dbguard-agent with a fake MySQL layer, a fake supervisor and a fake
manager. No mysqld and no Docker needed."""

from __future__ import annotations

import asyncio
import os
import signal
import time
from contextlib import asynccontextmanager
from typing import Any

import pytest
from aiohttp.test_utils import TestClient, TestServer

from dbguard.agent.core import Agent, AgentError
from dbguard.agent.db import DbError
from dbguard.agent.gtidcount import _interval_count, _parse, subtract_count
from dbguard.agent.http import make_app
from dbguard.agent.settings import AgentSettings

UUID_A = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
UUID_B = "aaaaaaaa-bbbb-cccc-dddd-000000000001"


# --------------------------------------------------------------------------- fakes

class FakeSession:
    def __init__(self, db: FakeDB):
        self.db = db
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True

    async def _run(self, sql: str, args: Any, timeout: float | None) -> Any:
        if self._closed:
            raise DbError("closed", code=2013)
        if self.db.down:
            raise DbError("Can't connect", code=2003)
        if self.db.delay:
            await asyncio.sleep(self.db.delay)
        for frag in self.db.hang:
            if frag in sql:
                await asyncio.sleep(timeout if timeout is not None else 1.0)
                self._closed = True
                raise DbError(f"timeout after {timeout}s", timeout=True)
        self.db.log.append(sql)
        for hook in self.db.hooks:
            hook(sql, args)
        return self.db.respond(sql, args)

    async def query(self, sql, args=None, *, timeout=None, log_sql=False):
        return await self._run(sql, args, timeout)

    async def execute(self, sql, args=None, *, timeout=None, log_sql=False):
        r = await self._run(sql, args, timeout)
        return r if isinstance(r, int) else 0


class FakeDB:
    def __init__(self) -> None:
        self.log: list[str] = []
        self.hooks: list = []
        self.hang: set[str] = set()
        self.down = False
        self.delay = 0.0
        self.sro = 1
        self.ro = 1
        self.gtid = f"{UUID_A}:1-10"
        self.src_enabled = 0
        self.rep_enabled = 0
        self.wait_sessions = 0
        self.semisync_clients = 2
        self.replica: dict[str, Any] | None = None
        self.heartbeat: tuple[float, str] | None = None
        self.clients = [{"id": 101, "user": "app"}, {"id": 102, "user": "chaos"}]

    # SQL "engine"
    def respond(self, sql: str, args: Any) -> Any:
        s = sql.strip()
        if s.startswith("SELECT @@GLOBAL.super_read_only AS sro, @@GLOBAL.read_only"):
            return [{"sro": self.sro, "ro": self.ro, "g": self.gtid}]
        if s.startswith("SELECT @@GLOBAL.super_read_only"):
            return [{"sro": self.sro}]
        if s.startswith("SELECT @@GLOBAL.gtid_executed"):
            return [{"g": self.gtid}]
        if s == "SELECT 1 AS one":
            return [{"one": 1}]
        if s == "SET GLOBAL super_read_only=1":
            self.sro, self.ro = 1, 1
        elif s == "SET GLOBAL super_read_only=0":
            self.sro = 0
        elif s == "SET GLOBAL read_only=0":
            self.sro, self.ro = 0, 0
        elif s.startswith("SET GLOBAL rpl_semi_sync_source_enabled="):
            self.src_enabled = int(s[-1])
        elif s.startswith("SET GLOBAL rpl_semi_sync_replica_enabled="):
            self.rep_enabled = int(s[-1])
        elif s.startswith("SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync"):
            return [{"Variable_name": "rpl_semi_sync_replica_enabled",
                     "Value": "ON" if self.rep_enabled else "OFF"},
                    {"Variable_name": "rpl_semi_sync_source_enabled",
                     "Value": "ON" if self.src_enabled else "OFF"}]
        elif s.startswith("SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync"):
            rows = [{"Variable_name": "Rpl_semi_sync_source_status",
                     "Value": "ON" if self.src_enabled else "OFF"},
                    {"Variable_name": "Rpl_semi_sync_replica_status", "Value": "OFF"},
                    {"Variable_name": "Rpl_semi_sync_source_clients",
                     "Value": str(self.semisync_clients)},
                    {"Variable_name": "Rpl_semi_sync_source_tx_avg_wait_time", "Value": "310"},
                    {"Variable_name": "Rpl_semi_sync_source_no_tx", "Value": "0"},
                    {"Variable_name": "Rpl_semi_sync_source_yes_tx", "Value": "42"},
                    {"Variable_name": "Rpl_semi_sync_source_wait_sessions",
                     "Value": str(self.wait_sessions)}]
            pattern = s.split("'")[1]
            if "%" not in pattern:
                rows = [r for r in rows if r["Variable_name"] == pattern]
            return rows
        elif s == "SHOW REPLICA STATUS":
            return [self.replica] if self.replica else []
        elif "FROM dbguard.heartbeat" in s:
            return [{"ts": self.heartbeat[0], "writer": self.heartbeat[1]}] \
                if self.heartbeat else []
        elif "performance_schema.threads" in s:
            return list(self.clients)
        elif s == "RESET REPLICA ALL":
            self.replica = None
        return 0

    # Database protocol
    async def connect(self, timeout=None) -> FakeSession:
        if self.down:
            raise DbError("Can't connect", code=2003)
        if "CONNECT" in self.hang:
            await asyncio.sleep(timeout or 1.0)
            raise DbError("connect timeout", timeout=True)
        return FakeSession(self)

    @asynccontextmanager
    async def session(self, timeout=None):
        s = await self.connect(timeout)
        try:
            yield s
        finally:
            s.close()

    async def close(self) -> None:
        pass

    def executed(self, *, skip_reads: bool = True) -> list[str]:
        if not skip_reads:
            return list(self.log)
        return [s for s in self.log if not s.startswith(("SELECT", "SHOW"))]


class FakeSupervisor:
    supervised = True

    def __init__(self) -> None:
        self.signals: list[int] = []
        self._alive = True
        self.generation = 1

    @property
    def pid(self):
        return 4242 if self._alive else None

    @property
    def alive(self):
        return self._alive

    async def start(self):
        pass

    def kill(self, sig=signal.SIGKILL):
        if not self._alive:
            return None
        self.signals.append(sig)
        if sig == signal.SIGKILL:
            self._alive = False
        return 4242

    async def wait_exit(self, timeout):
        return not self._alive

    async def wait_generation(self, gen, timeout):
        if self.held_s is not None and not self.released:
            return False
        self._alive = True
        self.generation += 1
        return True

    held_s: float | None = None
    released = False

    def hold(self, seconds):
        self.held_s, self.released = seconds, False

    def release_hold(self):
        if self.held_s is None:
            return False
        self.released = True
        return True

    @property
    def held(self):
        return self.held_s is not None and not self.released

    @property
    def hold_remaining_s(self):
        return float(self.held_s) if self.held else 0.0

    async def stop(self, timeout=120.0):
        pass


class FakeManager:
    def __init__(self, primary: str | None = None, reachable: bool = True):
        self.primary = primary
        self.ok = reachable
        self.urls: list[str] = []

    async def __call__(self, url: str, timeout: float) -> dict[str, Any]:
        self.urls.append(url)
        if not self.ok:
            raise ConnectionRefusedError("manager down")
        return {"primary": self.primary, "state": "HEALTHY"}


@pytest.fixture
def settings(tmp_path) -> AgentSettings:
    return AgentSettings(node="mysql-a1", rs="rs1", semisync=True,
                         manager_url="http://dbguard:9090", state_dir=str(tmp_path),
                         fence_deadline_s=0.2, sql_timeout_s=0.1)


def make_agent(settings, db=None, sup=None, manager=None) -> Agent:
    async def reachable(h, p):
        return True
    return Agent(settings, db or FakeDB(), sup or FakeSupervisor(),
                 manager_get=manager or FakeManager("mysql-a1"), reachable=reachable)


# --------------------------------------------------------------------------- fence

async def test_fence_sets_flag_before_set_super_read_only(settings):
    db = FakeDB()
    seen: list[bool] = []

    def hook(sql, args):
        if sql == "SET GLOBAL super_read_only=1":
            seen.append(os.path.exists(settings.fence_file))
    db.hooks.append(hook)
    agent = make_agent(settings, db)
    code, body = await agent.fence()
    assert code == 200
    assert seen == [True], "fence flag must be on disk before SET super_read_only"
    assert body["fenced"] is True and body["method"] == "sql"
    assert body["killed_threads"] == 2
    assert body["gtid_executed"] == f"{UUID_A}:1-10"
    ex = db.executed()
    assert ex[0] == "SET GLOBAL super_read_only=1"
    assert ex[1:] == ["KILL 101", "KILL 102"]
    assert set(body) == {"fenced", "method", "gtid_executed", "duration_ms", "killed_threads"}


async def test_fence_thread_query_excludes_repl_and_self(settings):
    db = FakeDB()
    captured = {}
    db.hooks.append(lambda sql, args: captured.update(sql=sql, args=args)
                    if "performance_schema.threads" in sql else None)
    await make_agent(settings, db).fence()
    assert "CONNECTION_ID()" in captured["sql"]
    assert "TYPE='FOREGROUND'" in captured["sql"]
    assert "repl" in captured["args"][0] and "event_scheduler" in captured["args"][0]


async def test_fence_kill_path_on_deadline(settings):
    db = FakeDB()
    db.sro = 0
    db.hang.add("SET GLOBAL super_read_only=1")
    sup = FakeSupervisor()
    agent = make_agent(settings, db, sup)
    code, body = await agent.fence()
    assert code == 200
    assert body["method"] == "kill"
    assert sup.signals == [signal.SIGKILL]
    assert body["gtid_executed"] is None
    assert body["duration_ms"] >= settings.fence_deadline_s * 1000 * 0.9
    assert os.path.exists(settings.fence_file)


async def test_fence_kill_path_when_connect_hangs(settings):
    db = FakeDB()
    db.hang.add("CONNECT")
    sup = FakeSupervisor()
    code, body = await make_agent(settings, db, sup).fence()
    assert code == 200 and body["method"] == "kill" and sup.signals == [signal.SIGKILL]


async def test_fence_unsupervised_cannot_kill(settings):
    from dbguard.agent.supervisor import NullSupervisor
    db = FakeDB()
    db.hang.add("SET GLOBAL super_read_only=1")
    code, body = await make_agent(settings, db, NullSupervisor()).fence()
    assert code == 500 and body["method"] == "failed" and body["fenced"] is True


async def test_fence_is_idempotent(settings):
    db = FakeDB()
    agent = make_agent(settings, db)
    for _ in range(3):
        code, body = await agent.fence()
        assert code == 200 and body["fenced"] is True
    assert agent.fenced


async def test_unfence_clears_flag_only(settings):
    db = FakeDB()
    agent = make_agent(settings, db)
    await agent.fence()
    db.log.clear()
    assert await agent.unfence() == {"fenced": False}
    assert not os.path.exists(settings.fence_file)
    assert db.executed() == []  # read_only untouched


# --------------------------------------------------------------------------- roles

async def test_promote_sql_sequence(settings):
    db = FakeDB()
    db.replica = {"Source_Host": "mysql-a2"}
    agent = make_agent(settings, db)
    await agent.fence()
    db.log.clear()
    body = await agent.promote()
    assert db.executed() == [
        "STOP REPLICA",
        "RESET REPLICA ALL",
        "SET GLOBAL rpl_semi_sync_replica_enabled=0",
        "SET GLOBAL rpl_semi_sync_source_enabled=1",
        "SET GLOBAL super_read_only=0",
        "SET GLOBAL read_only=0",
    ]
    assert set(body) == {"gtid_executed", "duration_ms"}
    assert not agent.fenced and not os.path.exists(settings.fence_file)
    assert db.sro == 0 and db.src_enabled == 1


async def test_promote_naive_mode_keeps_semisync_off(settings):
    settings.semisync = False
    db = FakeDB()
    await make_agent(settings, db).promote()
    assert "SET GLOBAL rpl_semi_sync_source_enabled=0" in db.executed()


async def test_repoint_sql_sequence(settings):
    db = FakeDB()
    db.sro = 0
    captured = {}
    db.hooks.append(lambda sql, args: captured.update(args=args)
                    if sql.startswith("CHANGE REPLICATION SOURCE") else None)
    body = await make_agent(settings, db).repoint("mysql-a3")
    ex = db.executed()
    assert ex[:4] == [
        "STOP REPLICA",
        "SET GLOBAL super_read_only=1",
        "SET GLOBAL rpl_semi_sync_source_enabled=0",
        "SET GLOBAL rpl_semi_sync_replica_enabled=1",
    ]
    assert ex[4].startswith("CHANGE REPLICATION SOURCE TO SOURCE_HOST=%(host)s")
    for opt in ("SOURCE_AUTO_POSITION=1", "SOURCE_HEARTBEAT_PERIOD=0.5",
                "SOURCE_CONNECT_RETRY=1", "SOURCE_RETRY_COUNT=86400"):
        assert opt in ex[4]
    assert ex[5] == "START REPLICA"
    assert ex.index("SET GLOBAL super_read_only=1") < ex.index("START REPLICA")
    assert captured["args"] == {"host": "mysql-a3", "port": 3306, "user": "repl",
                                "password": "repl"}
    assert body["ok"] is True and "duration_ms" in body


async def test_repoint_rejects_self(settings):
    with pytest.raises(AgentError):
        await make_agent(settings).repoint("mysql-a1")


async def test_configure_keeps_source_semisync_while_sessions_wait(settings):
    db = FakeDB()
    db.sro, db.src_enabled, db.wait_sessions = 1, 1, 3
    body = await make_agent(settings, db).configure(None)
    assert "SET GLOBAL rpl_semi_sync_source_enabled=0" not in db.executed()
    assert body["source_enabled"] is True


async def test_configure_restarts_io_thread_for_replica_side(settings):
    db = FakeDB()
    db.sro, db.rep_enabled = 1, 0
    db.replica = {"Replica_IO_Running": "Yes"}
    await make_agent(settings, db).configure(True)
    assert db.executed() == ["SET GLOBAL rpl_semi_sync_replica_enabled=1",
                             "STOP REPLICA IO_THREAD", "START REPLICA IO_THREAD"]


async def test_rebuild_counts_phantoms_before_clone(settings):
    db = FakeDB()
    db.gtid = f"{UUID_A}:1-12,{UUID_B}:1-3"
    donor = FakeDB()
    donor.gtid = f"{UUID_A}:1-10,{UUID_B}:1-5"
    agent = Agent(settings, db, FakeSupervisor(), manager_get=FakeManager("mysql-a2"),
                  donor_db=lambda host: donor)

    during: list = []

    def after_clone(sql, args):
        if sql.startswith("CLONE INSTANCE"):
            during.append((agent.rebuilding, db.sro))
            db.gtid = donor.gtid
            db.sro = 1  # the restarted mysqld boots read-only
            raise DbError("Lost connection", code=2013)
    db.hooks.append(after_clone)
    body = await agent.rebuild("mysql-a2")
    assert body["ok"] is True and body["phantom_gtids"] == 2
    ex = db.executed()
    assert ex.index("SET GLOBAL clone_valid_donor_list=%s") < \
        next(i for i, s in enumerate(ex) if s.startswith("CLONE INSTANCE"))
    assert ex[-3:] == ["STOP REPLICA", "RESET REPLICA ALL", "SET GLOBAL super_read_only=1"]
    # CLONE needs a writable recipient, and /primary must stay 503 meanwhile.
    assert during == [(True, 0)]
    assert not agent.rebuilding


def test_subtract_count():
    assert subtract_count(f"{UUID_A}:1-12,{UUID_B}:1-3", f"{UUID_A}:1-10,{UUID_B}:1-5") == 2
    assert subtract_count(f"{UUID_A}:1-5", f"{UUID_A}:1-5") == 0
    assert subtract_count("", f"{UUID_A}:1-5") == 0
    a, b = f"{UUID_A}:1-10:15-20", f"{UUID_A}:3-4:16"
    local = sum(_interval_count(v, _parse(b).get(k, [])) for k, v in _parse(a).items())
    assert local == subtract_count(a, b) == 13


# --------------------------------------------------------------------------- wake guard

@pytest.mark.parametrize("manager,writable,fence_file,decision,fences", [
    (FakeManager("mysql-a2"), True, False, "fence", True),        # someone else is primary
    (FakeManager("mysql-a2"), False, False, "ok", False),        # other primary, I'm read-only
    (FakeManager("mysql-a1"), True, False, "ok", False),         # I am the primary
    (FakeManager(None, reachable=False), True, True, "fence_file", True),
    (FakeManager(None, reachable=False), True, False, "leave", False),
])
async def test_wake_guard(settings, manager, writable, fence_file, decision, fences):
    db = FakeDB()
    db.sro = 0 if writable else 1
    if fence_file:
        open(settings.fence_file, "w").close()  # noqa: ASYNC230
    agent = make_agent(settings, db, manager=manager)
    assert await agent.wake_guard("startup") == decision
    assert ("SET GLOBAL super_read_only=1" in db.executed()) is fences
    assert manager.urls == ["http://dbguard:9090/v1/sets/rs1/primary"]


async def test_wake_gap_triggers_guard_before_primary_answers(settings):
    db = FakeDB()
    db.sro = 0
    manager = FakeManager("mysql-a2")
    agent = make_agent(settings, db, manager=manager)
    agent.startup_done.set()
    agent.last_tick -= 10  # the container was frozen for 10 s
    code, body = await agent.primary_check()
    assert (code, body) == (503, {"role": "fenced"})
    assert manager.urls


# --------------------------------------------------------------------------- HTTP

@pytest.fixture
async def client_factory():
    clients: list[TestClient] = []

    async def make(agent: Agent) -> TestClient:
        c = TestClient(TestServer(make_app(agent)))
        await c.start_server()
        clients.append(c)
        return c
    yield make
    for c in clients:
        await c.close()


@pytest.mark.parametrize("sro,fenced,hung,down,code,role", [
    (0, False, False, False, 200, "primary"),
    (1, False, False, False, 503, "replica"),
    (0, True, False, False, 503, "fenced"),
    (1, True, False, False, 503, "fenced"),
    (0, False, True, False, 503, "unknown"),
    (0, False, False, True, 503, "unknown"),
])
async def test_primary_truth_table(settings, client_factory, sro, fenced, hung, down, code,
                                   role):
    db = FakeDB()
    db.sro = sro
    agent = make_agent(settings, db)
    agent.startup_done.set()
    if fenced:
        await agent.fence()
        db.sro = sro
    if hung:
        db.hang.add("super_read_only")
    db.down = down
    c = await client_factory(agent)
    r = await c.get("/primary")
    assert r.status == code
    assert await r.json() == {"role": role}


async def test_primary_unknown_before_startup_guard(settings, client_factory):
    db = FakeDB()
    db.sro = 0
    c = await client_factory(make_agent(settings, db))
    r = await c.get("/primary")
    assert r.status == 503 and (await r.json())["role"] == "unknown"


STATUS_KEYS = {"node", "rs", "ts", "mysqld_alive", "mysqld_responsive", "mysqld_pid", "fenced",
               "super_read_only", "read_only", "gtid_executed", "replica", "semisync",
               "source_reachable", "heartbeat", "agent_uptime_s", "agent_version",
               "self_fence"}
REPLICA_KEYS = {"configured", "source_host", "io_running", "sql_running",
                "seconds_behind_source", "retrieved_gtid_set", "executed_gtid_set",
                "last_io_error", "last_sql_error"}
SEMISYNC_KEYS = {"source_enabled", "replica_enabled", "source_status", "replica_status",
                 "source_clients", "avg_wait_time_us", "no_tx", "yes_tx"}
HEARTBEAT_KEYS = {"ts", "age_s", "writer"}


def _check_shape(d: dict) -> None:
    assert STATUS_KEYS <= set(d)
    assert REPLICA_KEYS <= set(d["replica"])
    assert SEMISYNC_KEYS <= set(d["semisync"])
    assert HEARTBEAT_KEYS <= set(d["heartbeat"])


async def test_status_shape_replica(settings, client_factory):
    db = FakeDB()
    db.replica = {"Source_Host": "mysql-a2", "Replica_IO_Running": "Yes",
                  "Replica_SQL_Running": "Yes", "Seconds_Behind_Source": 0,
                  "Retrieved_Gtid_Set": f"{UUID_A}:1-10", "Executed_Gtid_Set": f"{UUID_A}:1-10",
                  "Last_IO_Error": "", "Last_SQL_Error": ""}
    db.heartbeat = (time.time() - 0.4, "mysql-a2")
    c = await client_factory(make_agent(settings, db))
    d = await (await c.get("/status")).json()
    _check_shape(d)
    assert d["mysqld_responsive"] is True and d["mysqld_alive"] is True
    assert d["mysqld_pid"] == 4242
    assert d["super_read_only"] is True and d["gtid_executed"] == f"{UUID_A}:1-10"
    assert d["replica"]["configured"] is True and d["replica"]["io_running"] == "Yes"
    assert d["replica"]["last_io_error"] is None
    assert d["source_reachable"] is True
    assert d["self_fence"] == {"enabled": True, "manager_unreachable_s": 0.0,
                               "semisync_clients": None}
    assert d["semisync"]["yes_tx"] == 42 and d["semisync"]["avg_wait_time_us"] == 310
    assert d["heartbeat"]["writer"] == "mysql-a2" and 0.3 < d["heartbeat"]["age_s"] < 5


async def test_status_hung_mysqld_is_null_not_blocking(settings, client_factory):
    db = FakeDB()
    db.hang.add("SELECT @@GLOBAL.super_read_only")
    c = await client_factory(make_agent(settings, db))
    d = await (await c.get("/status")).json()
    _check_shape(d)
    assert d["mysqld_responsive"] is False
    assert d["super_read_only"] is None and d["gtid_executed"] is None
    assert d["semisync"]["source_enabled"] is None


async def test_http_routes(settings, client_factory):
    db = FakeDB()
    sup = FakeSupervisor()
    agent = make_agent(settings, db, sup)
    c = await client_factory(agent)
    assert (await c.get("/health")).status == 200
    r = await c.post("/fence")
    assert r.status == 200 and (await r.json())["method"] == "sql"
    r = await c.post("/unfence")
    assert await r.json() == {"fenced": False}
    r = await c.post("/promote")
    assert r.status == 200 and "gtid_executed" in await r.json()
    r = await c.post("/repoint", json={"source": "mysql-a2"})
    assert await r.json() == {"ok": True, "duration_ms": (await r.json())["duration_ms"]}
    r = await c.post("/repoint", json={})
    assert r.status == 400
    r = await c.post("/configure", json={"semisync": False})
    assert r.status == 200 and (await r.json())["semisync"] is False
    r = await c.post("/configure", json={"semisync": "yes"})
    assert r.status == 400
    r = await c.get("/metrics")
    text = await r.text()
    assert "dbguard_agent_fences_total" in text and "dbguard_agent_promotions_total" in text
    r = await c.post("/hang-mysqld", json={"seconds": 0.01})
    assert r.status == 200 and sup.signals[-1] == signal.SIGSTOP
    r = await c.post("/kill-mysqld")
    assert r.status == 200 and sup.signals[-1] == signal.SIGKILL
    r = await c.post("/kill-mysqld")
    assert r.status == 409


# --------------------------------------------------------------------------- self-fence lease

def _lease_agent(settings, clients: int, manager_up: bool, **kw):
    db = FakeDB()
    db.sro = 0
    db.semisync_clients = clients
    settings.self_fence_after_s = kw.get("after", 10.0)
    settings.semisync = kw.get("semisync", True)
    return make_agent(settings, db, manager=FakeManager("mysql-a1", reachable=manager_up)), db


async def test_self_fence_when_manager_and_all_semisync_replicas_are_gone(settings):
    agent, db = _lease_agent(settings, clients=0, manager_up=False)
    assert await agent.self_fence_tick() == "counting"
    assert not agent.fenced
    agent._mgr_unreachable_since -= 11  # 11 s of continuous manager silence
    assert agent.self_fence_state()["manager_unreachable_s"] >= 10
    assert await agent.self_fence_tick() == "self_fence"
    assert agent.fenced and "SET GLOBAL super_read_only=1" in db.executed()
    assert agent.m.self_fences._value.get() == 1
    assert agent.self_fence_state()["manager_unreachable_s"] == 0.0


async def test_self_fence_not_triggered_in_experiment4_manager_partition_clients_stay_2(settings):
    agent, db = _lease_agent(settings, clients=2, manager_up=False)
    for _ in range(3):
        assert await agent.self_fence_tick() == "ok"
    agent._mgr_unreachable_since = time.monotonic() - 60
    assert await agent.self_fence_tick() == "ok"
    assert not agent.fenced and "SET GLOBAL super_read_only=1" not in db.executed()
    assert agent.self_fence_state() == {"enabled": True, "manager_unreachable_s": 0.0,
                                        "semisync_clients": 2}


async def test_self_fence_not_triggered_when_manager_reachable_and_no_replicas(settings):
    agent, _db = _lease_agent(settings, clients=0, manager_up=True)
    agent._mgr_unreachable_since = time.monotonic() - 60
    assert await agent.self_fence_tick() == "ok"
    assert not agent.fenced
    assert agent.self_fence_state()["manager_unreachable_s"] == 0.0


@pytest.mark.parametrize("semisync,after", [(False, 10.0), (True, 0.0)])
async def test_self_fence_disabled_in_naive_mode_or_after_zero(settings, semisync, after):
    agent, _db = _lease_agent(settings, clients=0, manager_up=False, semisync=semisync,
                             after=after)
    agent._mgr_unreachable_since = time.monotonic() - 60
    assert await agent.self_fence_tick() == "disabled"
    assert not agent.fenced and agent.self_fence_state()["enabled"] is False


async def test_self_fence_ignores_replicas(settings):
    agent, db = _lease_agent(settings, clients=0, manager_up=False)
    db.sro = 1
    assert await agent.self_fence_tick() == "not_primary"
    assert not agent.fenced


async def test_primary_is_503_while_rebuilding(settings):
    db = FakeDB()
    db.sro = 0
    agent = make_agent(settings, db)
    agent.startup_done.set()
    agent.rebuilding = True
    assert await agent.primary_check() == (503, {"role": "unknown"})


async def test_status_answers_within_one_deadline_when_mysqld_is_slow(settings):
    """Review #3. Each query is under its own 1 s timeout, but together they would take
    1.5 s, and the source TCP probe another 0.5 s. /status must still answer by its single
    deadline and degrade the fields it did not get to to null."""
    settings.sql_timeout_s = 1.0
    settings.status_deadline_s = 1.0
    db = FakeDB()
    db.delay = 0.3
    db.replica = {"Source_Host": "mysql-a2", "Replica_IO_Running": "Yes"}

    async def slow_probe(host, port):
        await asyncio.sleep(0.5)
        return False
    agent = Agent(settings, db, FakeSupervisor(), manager_get=FakeManager("mysql-a1"),
                  reachable=slow_probe)
    t0 = time.monotonic()
    d = await agent.status()
    elapsed = time.monotonic() - t0
    assert elapsed < 1.05, elapsed
    _check_shape(d)
    assert d["mysqld_responsive"] is True and d["gtid_executed"] == f"{UUID_A}:1-10"
    assert d["heartbeat"]["ts"] is None  # never reached
    assert "deadline" in d["error"]


# --------------------------------------------------------------------------- restart hold

async def test_kill_fence_holds_mysqld_restart(settings):
    """Review #4. After a kill fence the restarted mysqld would serve its unacked binlog
    tail to replicas not yet repointed, so the agent holds the restart."""
    settings.restart_hold_s = 20.0
    db = FakeDB()
    db.hang.add("SET GLOBAL super_read_only=1")
    sup = FakeSupervisor()
    agent = make_agent(settings, db, sup)
    code, body = await agent.fence()
    assert body["method"] == "kill" and sup.held_s == 20.0 and sup.held
    d = await agent.status()
    assert d["mysqld_restart_hold_s"] == 20.0


@pytest.mark.parametrize("op", ["promote", "repoint", "rebuild"])
async def test_role_change_ends_restart_hold(settings, op):
    db = FakeDB()
    sup = FakeSupervisor()
    agent = Agent(settings, db, sup, manager_get=FakeManager("mysql-a1"),
                  donor_db=lambda host: FakeDB())
    sup.hold(20.0)
    sup._alive = False
    if op == "promote":
        await agent.promote()
    elif op == "repoint":
        await agent.repoint("mysql-a2")
    else:
        await agent.rebuild("mysql-a2")
    assert sup.released and not sup.held and sup.alive


async def test_sql_fence_does_not_hold(settings):
    sup = FakeSupervisor()
    code, body = await make_agent(settings, FakeDB(), sup).fence()
    assert body["method"] == "sql" and sup.held_s is None


# --------------------------------------------------------------------------- guard toggles

async def test_configure_toggles_guards_without_touching_mysqld(settings, client_factory):
    """Review #12. The orchestrator baseline turns both DBGuard guards off at runtime."""
    db = FakeDB()
    db.sro = 0
    db.semisync_clients = 0
    manager = FakeManager("mysql-zz", reachable=False)
    open(settings.fence_file, "w").close()  # noqa: ASYNC230  would make the guard fence
    agent = make_agent(settings, db, manager=manager)
    c = await client_factory(agent)
    r = await c.post("/configure", json={"self_fence": False, "wake_guard": False})
    assert r.status == 200
    assert await r.json() == {"ok": True, "semisync": True, "self_fence": False,
                              "wake_guard": False}
    assert db.log == []  # toggles only, no SQL
    d = await (await c.get("/status")).json()
    assert d["self_fence"]["enabled"] is False and d["wake_guard"] == {"enabled": False}
    assert await agent.wake_guard("wake") == "disabled"
    agent._mgr_unreachable_since = time.monotonic() - 60
    assert await agent.self_fence_tick() == "disabled"
    assert "SET GLOBAL super_read_only=1" not in db.executed()
    r = await c.post("/configure", json={"wake_guard": True, "self_fence": True})
    assert (await r.json())["wake_guard"] is True
    d = await (await c.get("/status")).json()
    assert d["self_fence"]["enabled"] is True and d["wake_guard"] == {"enabled": True}
    assert await agent.wake_guard("wake") == "fence_file"
    r = await c.post("/configure", json={"wake_guard": "no"})
    assert r.status == 400


def test_wake_guard_env_default_and_off():
    assert AgentSettings.from_env({}).wake_guard is True
    assert AgentSettings.from_env({"DBGUARD_WAKE_GUARD": "0"}).wake_guard is False
    assert AgentSettings.from_env({"DBGUARD_RESTART_HOLD_S": "5"}).restart_hold_s == 5.0


# --------------------------------------------------------------------------- heartbeat gate

async def _run_heartbeat(agent, seconds):
    agent.s.heartbeat_interval_s = 0.01
    task = asyncio.create_task(agent._heartbeat())
    await asyncio.sleep(seconds)
    agent._stopping = True
    await asyncio.wait_for(task, 2)


async def test_heartbeat_writes_on_a_healthy_primary(settings):
    db = FakeDB()
    db.sro = 0
    agent = make_agent(settings, db)
    agent.startup_done.set()
    await _run_heartbeat(agent, 0.1)
    assert any(s.startswith("INSERT INTO dbguard.heartbeat") for s in db.log)


async def test_heartbeat_waits_for_wake_guard_after_a_freeze(settings):
    """Review #15. A thawed primary must not write a heartbeat (an unacked GTID) before
    the wake guard has run, and here the guard fences it."""
    db = FakeDB()
    db.sro = 0
    agent = make_agent(settings, db, manager=FakeManager("mysql-a2"))
    agent.startup_done.set()
    agent.last_tick -= 10  # frozen for 10 s
    await _run_heartbeat(agent, 0.2)
    assert "SET GLOBAL super_read_only=1" in db.log
    assert not any(s.startswith("INSERT INTO dbguard.heartbeat") for s in db.log)
    assert agent.fenced
