"""Optional shared-token auth on the agent API (dbguard/auth.py, docs/INTERFACES.md)."""

from __future__ import annotations

import textwrap

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from dbguard.agent.http import make_app
from dbguard.agent.settings import AgentSettings
from dbguard.auth import bearer_headers, check_bearer, is_open, token_from_env
from dbguard.config import load_config
from dbguard.manager.client import Addressing, AgentClient
from tests.test_agent_unit import FakeDB, FakeSupervisor, make_agent

TOKEN = "s3cret-token"
GOOD = {"Authorization": f"Bearer {TOKEN}"}


def test_token_from_env_blank_is_off():
    assert token_from_env({}) is None
    assert token_from_env({"DBGUARD_AGENT_TOKEN": ""}) is None
    assert token_from_env({"DBGUARD_AGENT_TOKEN": "  "}) is None
    assert token_from_env({"DBGUARD_AGENT_TOKEN": " abc "}) == "abc"


def test_check_bearer_and_open_routes():
    assert check_bearer(f"Bearer {TOKEN}", TOKEN)
    assert check_bearer(f"bearer {TOKEN}", TOKEN)
    assert not check_bearer(None, TOKEN)
    assert not check_bearer("", TOKEN)
    assert not check_bearer("Bearer wrong", TOKEN)
    assert not check_bearer(f"Basic {TOKEN}", TOKEN)
    assert not check_bearer(TOKEN, TOKEN)
    assert is_open("GET", "/primary") and is_open("HEAD", "/health") and is_open("get", "/metrics")
    assert not is_open("GET", "/status") and not is_open("POST", "/primary")
    assert bearer_headers(None) == {} and bearer_headers(TOKEN) == GOOD


def test_settings_read_token_and_hide_it():
    s = AgentSettings.from_env({"DBGUARD_NODE": "mysql-a1", "DBGUARD_AGENT_TOKEN": TOKEN})
    assert s.agent_token == TOKEN and TOKEN not in repr(s)
    assert AgentSettings.from_env({"DBGUARD_NODE": "mysql-a1"}).agent_token is None


@pytest.fixture
async def agent_client(tmp_path):
    clients: list[TestClient] = []

    async def make(token: str | None):
        settings = AgentSettings(node="mysql-a1", rs="rs1", semisync=True,
                                 manager_url="http://dbguard:9090", state_dir=str(tmp_path),
                                 fence_deadline_s=0.2, sql_timeout_s=0.1, agent_token=token)
        agent = make_agent(settings, FakeDB(), FakeSupervisor())
        c = TestClient(TestServer(make_app(agent)))
        await c.start_server()
        clients.append(c)
        return agent, c
    yield make
    for c in clients:
        await c.close()


async def test_token_unset_behaviour_unchanged(agent_client):
    agent, c = await agent_client(None)
    assert (await c.get("/status")).status == 200
    assert (await c.post("/fence")).status == 200
    assert (await c.post("/unfence", headers={"Authorization": "Bearer junk"})).status == 200
    assert (await c.get("/health")).status == 200
    assert agent.m.unauthorized._value.get() == 0


async def test_token_set_correct_header(agent_client):
    _, c = await agent_client(TOKEN)
    assert (await c.get("/status", headers=GOOD)).status == 200
    r = await c.post("/fence", headers=GOOD)
    assert r.status == 200 and (await r.json())["fenced"] is True
    assert (await c.post("/unfence", headers=GOOD)).status == 200
    r = await c.post("/repoint", json={}, headers=GOOD)
    assert r.status == 400            # past auth, into the handler


@pytest.mark.parametrize("headers", [
    {}, {"Authorization": "Bearer wrong"}, {"Authorization": f"Basic {TOKEN}"},
    {"Authorization": f"Bearer {TOKEN}x"}, {"Authorization": "Bearer "},
])
async def test_token_set_missing_or_wrong_is_401(agent_client, headers):
    agent, c = await agent_client(TOKEN)
    for method, path in [("GET", "/status"), ("POST", "/fence"), ("POST", "/promote"),
                         ("POST", "/kill-mysqld"), ("POST", "/configure")]:
        r = await c.request(method, path, headers=headers, json={})
        assert r.status == 401, path
        assert await r.json() == {"error": "unauthorized"}
    assert agent.m.unauthorized._value.get() == 5
    assert not agent.fenced
    r = await c.get("/metrics")
    assert "dbguard_agent_unauthorized_total 5.0" in await r.text()


async def test_token_set_open_routes_need_no_header(agent_client):
    agent, c = await agent_client(TOKEN)
    assert (await c.get("/health")).status == 200
    assert (await c.get("/metrics")).status == 200
    r = await c.get("/primary")
    assert r.status in (200, 503) and "role" in await r.json()
    r = await c.get("/primary", headers={"Authorization": "Bearer wrong"})
    assert r.status in (200, 503)
    assert agent.m.unauthorized._value.get() == 0


# --------------------------------------------------------------------------- manager side

async def _recorder():
    seen: list[str | None] = []

    async def any_route(request: web.Request) -> web.Response:
        seen.append(request.headers.get("Authorization"))
        return web.json_response({"ok": True})
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", any_route)
    server = TestServer(app)
    await server.start_server()
    return server, seen


@pytest.mark.parametrize("token,expect", [(TOKEN, f"Bearer {TOKEN}"), (None, None)])
async def test_manager_client_sends_header(token, expect):
    server, seen = await _recorder()
    addr = Addressing(overrides={"mysql-a1": ("127.0.0.1", 3306,
                                              str(server.make_url("")).rstrip("/"))})
    client = AgentClient(addr, token=token)
    try:
        await client.post("mysql-a1", "/fence", timeout=2)
        await client.request("GET", "mysql-a1", "/status", timeout=2)
        await client.post("mysql-a1", "/promote", timeout=2)
    finally:
        await client.close()
        await server.close()
    assert seen == [expect] * 3


def _fleet(tmp_path, extra: str = "") -> str:
    p = tmp_path / "fleet.yaml"
    p.write_text(textwrap.dedent("""
        sets:
          rs1: {nodes: [mysql-a1, mysql-a2, mysql-a3]}
    """) + extra)
    return str(p)


def test_config_agent_token_file_and_env(tmp_path):
    assert load_config(_fleet(tmp_path), env={}).agent_token is None
    cfg = load_config(_fleet(tmp_path, "agent_token: from-file\n"), env={})
    assert cfg.agent_token == "from-file" and "from-file" not in repr(cfg)
    # empty env (compose's ${DBGUARD_AGENT_TOKEN:-}) keeps the file; a value wins
    assert load_config(_fleet(tmp_path, "agent_token: from-file\n"),
                       env={"DBGUARD_AGENT_TOKEN": ""}).agent_token == "from-file"
    assert load_config(_fleet(tmp_path, "agent_token: from-file\n"),
                       env={"DBGUARD_AGENT_TOKEN": "from-env"}).agent_token == "from-env"


def test_manager_builds_client_with_token(tmp_path, monkeypatch):
    from dbguard.manager.manager import Manager
    monkeypatch.delenv("DBGUARD_AGENT_TOKEN", raising=False)
    cfg = load_config(_fleet(tmp_path, "agent_token: from-file\n"), env={})
    assert Manager(cfg).agents.headers == {"Authorization": "Bearer from-file"}
    cfg = load_config(_fleet(tmp_path), env={})
    assert Manager(cfg).agents.headers == {}
    monkeypatch.setenv("DBGUARD_AGENT_TOKEN", "env-only")
    assert Manager(cfg).agents.headers == {"Authorization": "Bearer env-only"}
