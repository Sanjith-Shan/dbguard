"""Dependency-free JSON-over-HTTP client for the Manager and Agent APIs (docs/INTERFACES.md).

stdlib urllib only, so dbgctl works on a host that has nothing but Python.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_MANAGER_URL = "http://127.0.0.1:19090"


class ApiError(RuntimeError):
    def __init__(self, status: int | None, message: str, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


def request(method: str, url: str, body: Any = None, timeout: float = 5.0) -> tuple[int, Any]:
    """Returns (status, parsed JSON or text). HTTP error statuses are returned, not raised.
    Connection failures raise ApiError(status=None)."""
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    elif method in ("POST", "PUT"):
        data = b""
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status, raw = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        reason = getattr(e, "reason", e)
        raise ApiError(None, f"{method} {url}: {reason}") from e
    text = raw.decode(errors="replace")
    try:
        return status, json.loads(text) if text else None
    except json.JSONDecodeError:
        return status, text


class ManagerClient:
    def __init__(self, base_url: str | None = None, timeout: float = 5.0):
        self.base = (base_url or os.environ.get("DBGUARD_MANAGER_URL") or DEFAULT_MANAGER_URL).rstrip("/")
        self.timeout = timeout

    def _call(self, method: str, path: str, body: Any = None, ok=(200,)) -> Any:
        status, payload = request(method, self.base + path, body, self.timeout)
        if status not in ok:
            msg = payload.get("error") if isinstance(payload, dict) else payload
            raise ApiError(status, f"{method} {path} -> HTTP {status}: {msg}", payload)
        return payload

    def status(self) -> dict:
        return self._call("GET", "/v1/status")

    def set(self, rs: str) -> dict:
        return self._call("GET", f"/v1/sets/{rs}")

    def primary(self, rs: str) -> dict:
        return self._call("GET", f"/v1/sets/{rs}/primary")

    def doctor(self, rs: str) -> dict:
        return self._call("GET", f"/v1/sets/{rs}/doctor")

    def failover(self, rs: str, to: str | None = None, timeout: float = 60.0) -> dict:
        status, payload = request("POST", f"{self.base}/v1/sets/{rs}/failover", {"to": to}, timeout)
        if status != 200:
            msg = payload.get("error") if isinstance(payload, dict) else payload
            raise ApiError(status, f"failover {rs} -> HTTP {status}: {msg}", payload)
        return payload

    def halt(self, rs: str) -> dict:
        return self._call("POST", f"/v1/sets/{rs}/halt", {})

    def resume(self, rs: str) -> dict:
        return self._call("POST", f"/v1/sets/{rs}/resume", {})

    def rejoin(self, rs: str, node: str) -> dict:
        return self._call("POST", f"/v1/sets/{rs}/rejoin", {"node": node})

    def events(self, rs: str | None = None, since: float | None = None) -> list[dict]:
        q = {}
        if rs:
            q["rs"] = rs
        if since is not None:
            q["since"] = f"{since:.6f}"
        path = "/v1/events" + ("?" + urllib.parse.urlencode(q) if q else "")
        payload = self._call("GET", path)
        return payload.get("events", []) if isinstance(payload, dict) else []


class AgentClient:
    def __init__(self, base_url: str, timeout: float = 3.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path: str) -> tuple[int, Any]:
        return request("GET", self.base + path, None, self.timeout)

    def post(self, path: str, body: Any = None, timeout: float | None = None) -> tuple[int, Any]:
        return request("POST", self.base + path, body if body is not None else {},
                       timeout or self.timeout)

    def status(self) -> dict | None:
        try:
            code, body = self.get("/status")
        except ApiError:
            return None
        return body if code == 200 and isinstance(body, dict) else None

    def primary_code(self) -> int | None:
        try:
            return self.get("/primary")[0]
        except ApiError:
            return None
