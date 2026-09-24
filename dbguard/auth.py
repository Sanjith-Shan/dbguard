"""Optional shared-token authentication for the agent HTTP API (docs/INTERFACES.md).

``DBGUARD_AGENT_TOKEN`` set on an agent makes every route except the three unauthenticated
reads (``GET /health``, ``GET /metrics``, ``GET /primary``, which HAProxy's check and the
compose healthcheck use) require ``Authorization: Bearer <token>``. Unset or empty, nothing
changes. The manager sends the same header on every call. This is a shared secret over
plaintext HTTP: it stops a stray caller on the network from fencing or promoting a node, not
an attacker who can read the traffic (docs/DESIGN.md, Operations).
"""

from __future__ import annotations

import hmac
import os
from collections.abc import Mapping

ENV = "DBGUARD_AGENT_TOKEN"

# (method, path) pairs served without a token. HEAD rides along with GET in aiohttp.
OPEN_ROUTES = frozenset((m, p) for m in ("GET", "HEAD")
                        for p in ("/health", "/metrics", "/primary"))


def token_from_env(env: Mapping[str, str] | None = None) -> str | None:
    """The token from ``DBGUARD_AGENT_TOKEN``, None when unset or blank (auth off)."""
    e = os.environ if env is None else env
    return (e.get(ENV) or "").strip() or None


def is_open(method: str, path: str) -> bool:
    """True when ``method path`` is served without a token."""
    return (method.upper(), path) in OPEN_ROUTES


def check_bearer(header: str | None, token: str) -> bool:
    """True when ``header`` is ``Bearer <token>``, compared in constant time."""
    scheme, _, given = (header or "").partition(" ")
    ok = hmac.compare_digest(given.strip().encode(), token.encode())
    return ok and scheme.lower() == "bearer"


def bearer_headers(token: str | None) -> dict[str, str]:
    """``{"Authorization": "Bearer <token>"}``, or {} when there is no token."""
    return {"Authorization": f"Bearer {token}"} if token else {}
