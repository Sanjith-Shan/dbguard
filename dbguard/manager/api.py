"""Manager HTTP API (:9090), the routes of docs/INTERFACES.md.

Handlers only read a controller's state or call into it. The one with a safety role is
``GET /v1/sets/{rs}/primary``, which agents ask before trusting that they are primary. It
answers null during a failover, so a woken old primary fences itself, and 503 ``unknown``
while discovering, so an agent leaves its state alone instead of reading it as "someone else".
"""

from __future__ import annotations

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST

from dbguard.manager.doctor import doctor
from dbguard.manager.model import State

MGR: web.AppKey = web.AppKey("mgr")


def _ctl(request: web.Request):
    """The set named in the path, or 404."""
    mgr = request.app[MGR]
    rs = request.match_info["rs"]
    ctl = mgr.sets.get(rs)
    if ctl is None:
        raise web.HTTPNotFound(text=f'{{"error":"unknown set {rs}"}}',
                               content_type="application/json")
    return ctl


async def _body(request: web.Request) -> dict:
    """The JSON body as a dict, {} when absent, 400 when not JSON."""
    if not request.can_read_body:
        return {}
    try:
        b = await request.json()
    except Exception:  # noqa: BLE001
        raise web.HTTPBadRequest(text='{"error":"body must be JSON"}',
                                 content_type="application/json")
    return b if isinstance(b, dict) else {}


async def status(request):
    """GET /v1/status, every set."""
    mgr = request.app[MGR]
    return web.json_response({"mode": mgr.mode,
                              "sets": {rs: c.status() for rs, c in mgr.sets.items()}})


async def set_status(request):
    """GET /v1/sets/{rs}."""
    return web.json_response(_ctl(request).status())


async def set_primary(request):
    """GET /v1/sets/{rs}/primary, asked by agents' wake guard and lease."""
    ctl = _ctl(request)
    # Mid-failover the old primary is still ctl.primary until the promote step. A woken
    # node asking now must not be told it is the primary, so answer null.
    if ctl.primary is None:
        # Discovering (just started) or no primary known. "unknown" means the agent must
        # leave its state alone, never read it as "someone else is primary" (REVIEW #16).
        return web.json_response({"primary": "unknown", "state": ctl.st.state.value},
                                 status=503)
    primary = None if ctl.st.state == State.FAILING_OVER else ctl.primary
    return web.json_response({"primary": primary, "state": ctl.st.state.value})


async def set_doctor(request):
    """GET /v1/sets/{rs}/doctor."""
    return web.json_response(doctor(_ctl(request)))


async def set_failover(request):
    """POST /v1/sets/{rs}/failover, a planned switchover. 409 when refused."""
    from dbguard.manager.switchover import SwitchoverError, switchover
    ctl = _ctl(request)
    body = await _body(request)
    try:
        ev = await switchover(ctl, body.get("to"))
    except SwitchoverError as e:
        return web.json_response({"error": str(e), "state": ctl.st.state.value}, status=409)
    return web.json_response({"event": ev.row()})


async def set_halt(request):
    """POST /v1/sets/{rs}/halt."""
    ctl = _ctl(request)
    body = await _body(request)
    ctl.st.halt(body.get("reason") or "halted by operator")
    return web.json_response({"state": ctl.st.state.value, "halt_reason": ctl.st.halt_reason})


async def set_resume(request):
    """POST /v1/sets/{rs}/resume. Detection history and backoffs start over."""
    ctl = _ctl(request)
    ctl.st.resume()
    ctl.history.clear()
    ctl.backoff.clear()
    return web.json_response({"state": ctl.st.state.value})


async def set_rejoin(request):
    """POST /v1/sets/{rs}/rejoin, rejoin one node now. A rebuild runs in the background."""
    from dbguard.manager.rejoin import needs_rejoin, rejoin_node
    ctl = _ctl(request)
    body = await _body(request)
    node = body.get("node")
    if node not in ctl.all_nodes():
        return web.json_response({"error": f"{node} is not in {ctl.rs}"}, status=400)
    if node == ctl.primary:
        return web.json_response({"error": f"{node} is the primary"}, status=400)
    async with ctl.lock:
        views = await ctl.fresh_views([node] + ([ctl.primary] if ctl.primary else []))
        nv, pv = views[node], views.get(ctl.primary)
        if not nv.usable or pv is None or not pv.usable:
            return web.json_response({"error": "node or primary not responsive"}, status=409)
        if node not in ctl.members:
            ctl.members.append(node)
        ev = await rejoin_node(ctl, node, nv, pv, why=needs_rejoin(nv, ctl.primary) or
                               "operator request", wait=False)
    return web.json_response({"event": ev.row() if ev else None,
                              "state": ctl.st.state.value,
                              "note": None if ev else "rebuild started in the background"})


async def events(request):
    """GET /v1/events, filtered by ``rs`` and ``since``."""
    mgr = request.app[MGR]
    rs = request.query.get("rs") or None
    since = request.query.get("since")
    try:
        since_f = float(since) if since else None
    except ValueError:
        raise web.HTTPBadRequest(text='{"error":"since must be a unix time"}',
                                 content_type="application/json")
    evs = mgr.events.query(rs=rs, since=since_f)
    return web.json_response({"events": [e.row() for e in evs]})


async def metrics(request):
    """GET /metrics."""
    mgr = request.app[MGR]
    for rs, c in mgr.sets.items():
        mgr.metrics.state(rs, c.st.state)
    return web.Response(body=mgr.metrics.render(),
                        headers={"Content-Type": CONTENT_TYPE_LATEST})


async def health(request):
    """GET /health."""
    return web.json_response({"ok": True})


def make_app(mgr) -> web.Application:
    """The aiohttp application serving ``mgr``."""
    app = web.Application()
    app[MGR] = mgr
    app.add_routes([
        web.get("/v1/status", status),
        web.get("/v1/sets/{rs}", set_status),
        web.get("/v1/sets/{rs}/primary", set_primary),
        web.get("/v1/sets/{rs}/doctor", set_doctor),
        web.post("/v1/sets/{rs}/failover", set_failover),
        web.post("/v1/sets/{rs}/halt", set_halt),
        web.post("/v1/sets/{rs}/resume", set_resume),
        web.post("/v1/sets/{rs}/rejoin", set_rejoin),
        web.get("/v1/events", events),
        web.get("/metrics", metrics),
        web.get("/health", health),
    ])
    return app
