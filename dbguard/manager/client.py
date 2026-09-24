"""How the manager reaches nodes: addressing and the agent HTTP client."""

from __future__ import annotations

import asyncio
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from dbguard.manager.model import NodeView


@dataclass
class Addressing:
    """Where each node's agent and mysqld are, from the manager's point of view.

    Inside the compose network every node is reachable by name (``mysql-a1:3306``,
    ``http://mysql-a1:8080``). When the manager runs on the host, the names do not
    resolve, so a host map points each node at its published ports:

    - ``--host-ports`` uses the published pattern of docs/INTERFACES.md: ``mysql-a1`` is
      127.0.0.1:13311 (mysqld) and 127.0.0.1:18011 (agent), ``mysql-b3`` is 13323 and 18023.
    - ``DBGUARD_HOST_MAP="mysql-a1=127.0.0.1:13311:18011,mysql-a2=..."`` overrides nodes
      one by one, and wins over ``--host-ports``.

    Names sent to agents (the repoint source, the clone donor) are always the node names,
    because agents resolve them inside the network.
    """

    agent_port: int = 8080
    mysql_port: int = 3306
    overrides: dict[str, tuple[str, int, str]] = field(default_factory=dict)
    host_ports: bool = False

    def mysql_addr(self, node: str) -> tuple[str, int]:
        if node in self.overrides:
            h, p, _ = self.overrides[node]
            return h, p
        if self.host_ports and (pp := published_ports(node)):
            return "127.0.0.1", pp[0]
        return node, self.mysql_port

    def agent_url(self, node: str) -> str:
        if node in self.overrides:
            return self.overrides[node][2]
        if self.host_ports and (pp := published_ports(node)):
            return f"http://127.0.0.1:{pp[1]}"
        return f"http://{node}:{self.agent_port}"

    @staticmethod
    def parse_map(text: str | None) -> dict[str, tuple[str, int, str]]:
        out = {}
        for item in (text or "").split(","):
            item = item.strip()
            if not item:
                continue
            node, _, rest = item.partition("=")
            host, mport, aport = rest.split(":")
            out[node.strip()] = (host, int(mport), f"http://{host}:{int(aport)}")
        return out

    @classmethod
    def from_env(cls, agent_port: int, mysql_port: int, host_ports: bool = False,
                 env: dict | None = None) -> Addressing:
        e = os.environ if env is None else env
        return cls(agent_port=agent_port, mysql_port=mysql_port,
                   overrides=cls.parse_map(e.get("DBGUARD_HOST_MAP")),
                   host_ports=host_ports or e.get("DBGUARD_HOST_PORTS") == "1")


_NODE_RE = re.compile(r"^mysql-([a-z])(\d)$")


def published_ports(node: str) -> tuple[int, int] | None:
    m = _NODE_RE.match(node)
    if not m:
        return None
    s = ord(m.group(1)) - ord("a") + 1
    i = int(m.group(2))
    return 13300 + 10 * s + i, 18000 + 10 * s + i


# The agent's /status runs several queries and a TCP probe of its source. Under load or
# behind an iptables DROP it can take well over 1 s, and a replica whose /status times out
# is neither a witness nor a candidate (docs/REVIEW_2026-09-23.md #3).
STATUS_TIMEOUT_S = 3.0

# An agent answering 500 with one of these lost its pooled MySQL link, typically because a
# fence killed it. Role changes are idempotent, so the manager retries them once.
LOST_LINK_CODES = (2006, 2013)


class AgentError(Exception):
    def __init__(self, node: str, path: str, msg: str, status: int | None = None,
                 body: Any = None, timeout: bool = False):
        super().__init__(f"{node} {path}: {msg}")
        self.node, self.path, self.status, self.body, self.timeout = node, path, status, body, timeout


class AgentClient:
    """Every call is bounded. A hung agent costs at most its timeout, never the loop."""

    def __init__(self, addressing: Addressing, status_timeout_s: float = STATUS_TIMEOUT_S):
        self.addr = addressing
        self.status_timeout_s = status_timeout_s
        self._session: aiohttp.ClientSession | None = None

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=0, force_close=False,
                                               enable_cleanup_closed=True),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def status(self, node: str, timeout: float | None = None) -> NodeView:
        t = timeout or self.status_timeout_s
        try:
            body = await self.request("GET", node, "/status", timeout=t)
            return NodeView.from_status(node, body, ts=time.time())
        except AgentError as e:
            return NodeView.unreachable(node, str(e.args[0]), ts=time.time())

    async def request(self, method: str, node: str, path: str, body: Any = None,
                      timeout: float = 5.0) -> Any:
        url = self.addr.agent_url(node) + path
        s = await self.session()
        try:
            async def go():
                async with s.request(method, url, json=body,
                                     timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    try:
                        data = await r.json(content_type=None)
                    except Exception:  # noqa: BLE001
                        data = {"raw": (await r.text())[:500]}
                    if r.status >= 400:
                        raise AgentError(node, path, f"HTTP {r.status} {data}", status=r.status,
                                         body=data)
                    return data
            return await asyncio.wait_for(go(), timeout + 0.25)
        except AgentError:
            raise
        except TimeoutError as e:
            raise AgentError(node, path, f"timeout after {timeout:.1f} s", timeout=True) from e
        except (aiohttp.ClientError, OSError, ValueError) as e:
            raise AgentError(node, path, f"{type(e).__name__}: {e}") from e

    async def post(self, node: str, path: str, body: Any = None, timeout: float = 10.0,
                   retry_lost_link: bool | None = None) -> Any:
        """POST to the agent. /promote, /repoint and /configure are retried once when the
        agent reports a lost MySQL link (2006, 2013): a fence on that node kills pooled
        connections, and every step of a role change is idempotent."""
        if retry_lost_link is None:
            retry_lost_link = path in ("/promote", "/repoint", "/configure")
        try:
            return await self.request("POST", node, path, body=body or {}, timeout=timeout)
        except AgentError as e:
            code = e.body.get("sql_code") if isinstance(e.body, dict) else None
            if not (retry_lost_link and e.status == 500 and code in LOST_LINK_CODES):
                raise
        return await self.request("POST", node, path, body=body or {}, timeout=timeout)
