"""aiohttp routes for the agent API (docs/INTERFACES.md, Agent HTTP API)."""

from __future__ import annotations

import json
from typing import Any

import structlog
from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST

from dbguard.agent.core import Agent, AgentError
from dbguard.agent.db import DbError

log = structlog.get_logger("dbguard.agent.http")

AGENT_KEY: web.AppKey[Agent] = web.AppKey("agent", Agent)


async def _body(request: web.Request) -> dict[str, Any]:
    if not request.can_read_body:
        return {}
    try:
        data = await request.json()
    except (json.JSONDecodeError, ValueError) as e:
        raise web.HTTPBadRequest(text=json.dumps({"error": f"bad json: {e}"}),
                                 content_type="application/json") from e
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text=json.dumps({"error": "body must be an object"}),
                                 content_type="application/json")
    return data


@web.middleware
async def errors(request: web.Request, handler):
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except AgentError as e:
        log.warning("request_failed", path=request.path, error=str(e), status=e.status)
        return web.json_response({"error": str(e), **e.extra}, status=e.status)
    except DbError as e:
        log.warning("request_failed", path=request.path, error=str(e), sql_code=e.code)
        return web.json_response({"error": str(e), "sql_code": e.code,
                                  "timeout": e.timeout}, status=503 if e.timeout else 500)


async def primary(request: web.Request) -> web.Response:
    code, body = await request.app[AGENT_KEY].primary_check()
    return web.json_response(body, status=code)


async def status(request: web.Request) -> web.Response:
    return web.json_response(await request.app[AGENT_KEY].status())


async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def fence(request: web.Request) -> web.Response:
    code, body = await request.app[AGENT_KEY].fence(reason="api")
    return web.json_response(body, status=code)


async def unfence(request: web.Request) -> web.Response:
    return web.json_response(await request.app[AGENT_KEY].unfence())


async def promote(request: web.Request) -> web.Response:
    return web.json_response(await request.app[AGENT_KEY].promote())


async def repoint(request: web.Request) -> web.Response:
    body = await _body(request)
    return web.json_response(await request.app[AGENT_KEY].repoint(str(body.get("source") or "")))


async def rebuild(request: web.Request) -> web.Response:
    body = await _body(request)
    return web.json_response(await request.app[AGENT_KEY].rebuild(str(body.get("donor") or "")))


async def configure(request: web.Request) -> web.Response:
    body = await _body(request)
    semisync = body.get("semisync")
    if semisync is not None and not isinstance(semisync, bool):
        raise AgentError("semisync must be a boolean", status=400)
    return web.json_response(await request.app[AGENT_KEY].configure(semisync))


async def kill_mysqld(request: web.Request) -> web.Response:
    return web.json_response(request.app[AGENT_KEY].kill_mysqld())


async def hang_mysqld(request: web.Request) -> web.Response:
    body = await _body(request)
    try:
        seconds = float(body.get("seconds", 10))
    except (TypeError, ValueError) as e:
        raise AgentError("seconds must be a number", status=400) from e
    return web.json_response(request.app[AGENT_KEY].hang_mysqld(seconds))


async def metrics(request: web.Request) -> web.Response:
    agent = request.app[AGENT_KEY]
    agent.m.heartbeat_stalled.set(agent.heartbeat_stalled_s())
    alive = agent.mysqld_alive()
    if alive is not None:
        agent.m.mysqld_alive.set(1 if alive else 0)
    return web.Response(body=agent.m.render(),
                        headers={"Content-Type": CONTENT_TYPE_LATEST})


def make_app(agent: Agent) -> web.Application:
    app = web.Application(middlewares=[errors])
    app[AGENT_KEY] = agent
    app.router.add_get("/primary", primary)
    app.router.add_get("/status", status)
    app.router.add_get("/health", health)
    app.router.add_post("/fence", fence)
    app.router.add_post("/unfence", unfence)
    app.router.add_post("/promote", promote)
    app.router.add_post("/repoint", repoint)
    app.router.add_post("/rebuild", rebuild)
    app.router.add_post("/configure", configure)
    app.router.add_post("/kill-mysqld", kill_mysqld)
    app.router.add_post("/hang-mysqld", hang_mysqld)
    app.router.add_get("/metrics", metrics)
    return app
