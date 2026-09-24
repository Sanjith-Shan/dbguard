"""aiomysql helpers shared by the agent and the manager.

Auth decision (docs/INTERFACES.md, accounts section, and docs/BUGS.md): every account is
``caching_sha2_password``. Over plain TCP the first login after a mysqld start needs either
TLS or an RSA exchange (which needs the ``cryptography`` package on the client). mysqld 8.4
auto-generates self-signed certificates at first start, so every DBGuard connection uses
TLS with verification off (``insecure_tls()``). Replication uses ``SOURCE_SSL=1`` for the
same reason. Pass ``tls=False`` only against a server known to have the account cached.
"""

from __future__ import annotations

import asyncio
import ssl
import time
from collections.abc import Awaitable, Iterable
from typing import Any, TypeVar

import aiomysql
import structlog

log = structlog.get_logger("dbguard.mysqlx")

T = TypeVar("T")


def insecure_tls() -> ssl.SSLContext:
    """TLS context for the fleet's self-signed server certs (encryption, no verification)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def with_timeout(coro: Awaitable[T], seconds: float) -> T:
    """Await ``coro`` with a deadline. Raises ``asyncio.TimeoutError`` (== TimeoutError)."""
    return await asyncio.wait_for(coro, timeout=seconds)


async def connect(host: str, port: int, user: str, password: str, timeout: float = 2.0,
                  db: str | None = None, *, tls: bool = True,
                  autocommit: bool = True) -> aiomysql.Connection:
    """Open one connection. The whole handshake (TCP, TLS, auth) is bounded by ``timeout``."""
    kwargs: dict[str, Any] = dict(host=host, port=port, user=user, password=password,
                                  connect_timeout=timeout, autocommit=autocommit,
                                  cursorclass=aiomysql.DictCursor)
    if db is not None:
        kwargs["db"] = db
    if tls:
        kwargs["ssl"] = insecure_tls()
    return await with_timeout(aiomysql.connect(**kwargs), timeout)


async def query(conn: aiomysql.Connection, sql: str, args: Any = None) -> list[dict[str, Any]]:
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(sql, args)
        rows = await cur.fetchall()
    return [dict(r) for r in rows] if rows else []


async def execute(conn: aiomysql.Connection, sql: str, args: Any = None) -> int:
    t0 = time.monotonic()
    async with conn.cursor() as cur:
        n = await cur.execute(sql, args)
    log.debug("sql", sql=sql, duration_ms=round((time.monotonic() - t0) * 1000, 2))
    return n


def _none_if_empty(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def _gtid_text(v: Any) -> str | None:
    """SHOW output wraps GTID sets over lines ("uuid:1-5,\\nuuid2:1-3"). Normalise."""
    if v is None:
        return None
    return "".join(str(v).split())


def parse_replica_status(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Map one SHOW REPLICA STATUS row onto the agent /status ``replica`` object."""
    if not row:
        return None
    sbs = row.get("Seconds_Behind_Source")
    return {
        "configured": True,
        "source_host": _none_if_empty(row.get("Source_Host")),
        "io_running": _none_if_empty(row.get("Replica_IO_Running")),
        "sql_running": _none_if_empty(row.get("Replica_SQL_Running")),
        "seconds_behind_source": int(sbs) if sbs is not None else None,
        "retrieved_gtid_set": _gtid_text(row.get("Retrieved_Gtid_Set")),
        "executed_gtid_set": _gtid_text(row.get("Executed_Gtid_Set")),
        "last_io_error": _none_if_empty(row.get("Last_IO_Error")),
        "last_sql_error": _none_if_empty(row.get("Last_SQL_Error")),
    }


UNCONFIGURED_REPLICA: dict[str, Any] = {
    "configured": False, "source_host": None, "io_running": None, "sql_running": None,
    "seconds_behind_source": None, "retrieved_gtid_set": None, "executed_gtid_set": None,
    "last_io_error": None, "last_sql_error": None,
}


async def replica_status(conn: aiomysql.Connection) -> dict[str, Any] | None:
    """SHOW REPLICA STATUS parsed to the /status ``replica`` shape, None if not a replica."""
    rows = await query(conn, "SHOW REPLICA STATUS")
    return parse_replica_status(rows[0] if rows else None)


def _in_list(names: Iterable[str]) -> tuple[str, list[str]]:
    names = list(names)
    return ", ".join(["%s"] * len(names)), names


async def global_vars(conn: aiomysql.Connection, names: Iterable[str]) -> dict[str, str]:
    """Values of the named global variables (missing names are absent from the result)."""
    ph, args = _in_list(names)
    if not args:
        return {}
    rows = await query(conn, "SELECT VARIABLE_NAME AS n, VARIABLE_VALUE AS v FROM "
                             f"performance_schema.global_variables WHERE VARIABLE_NAME IN ({ph})",
                       args)
    return {r["n"]: r["v"] for r in rows}


async def global_status(conn: aiomysql.Connection, names: Iterable[str]) -> dict[str, str]:
    """Values of the named global status counters (missing names are absent)."""
    ph, args = _in_list(names)
    if not args:
        return {}
    rows = await query(conn, "SELECT VARIABLE_NAME AS n, VARIABLE_VALUE AS v FROM "
                             f"performance_schema.global_status WHERE VARIABLE_NAME IN ({ph})",
                       args)
    return {r["n"]: r["v"] for r in rows}


async def gtid_executed(conn: aiomysql.Connection) -> str:
    rows = await query(conn, "SELECT @@GLOBAL.gtid_executed AS g")
    return _gtid_text(rows[0]["g"]) or ""
