"""Synchronous PyMySQL client for the harness and the schema-change tool.

``bin/bootstrap``, ``bin/workload``, ``bin/checker`` and ``dbgctl osc`` connect to the fleet
through these functions. They sit outside the failover path, which runs in the agent
(``dbguard/agent/db.py``) and the manager (``dbguard/manager/probe.py``) on their own aiomysql
connections. Same auth decision as everywhere, TLS with verification off, see
``dbguard/mysqlx.py``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pymysql
import pymysql.cursors

from dbguard.mysqlx import gtid_text, insecure_tls, parse_replica_status


def connect(host: str, port: int, user: str, password: str, timeout: float = 2.0,
            db: str | None = None, *, tls: bool = True, autocommit: bool = True,
            read_timeout: float | None = None,
            write_timeout: float | None = None) -> pymysql.connections.Connection:
    """Open one DictCursor connection, over TLS unless ``tls=False`` (tests only)."""
    return pymysql.connect(
        host=host, port=port, user=user, password=password, database=db,
        connect_timeout=timeout, read_timeout=read_timeout, write_timeout=write_timeout,
        autocommit=autocommit, cursorclass=pymysql.cursors.DictCursor,
        ssl=insecure_tls() if tls else None,
    )


def query(conn: pymysql.connections.Connection, sql: str,
          args: Any = None) -> list[dict[str, Any]]:
    """Run ``sql`` and return every row as a dict."""
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return list(cur.fetchall())


def execute(conn: pymysql.connections.Connection, sql: str, args: Any = None) -> int:
    """Run ``sql`` and return the affected row count."""
    with conn.cursor() as cur:
        return cur.execute(sql, args)


def replica_status(conn: pymysql.connections.Connection) -> dict[str, Any] | None:
    """SHOW REPLICA STATUS in the /status ``replica`` shape, None when not a replica."""
    rows = query(conn, "SHOW REPLICA STATUS")
    return parse_replica_status(rows[0] if rows else None)


def _kv(conn, table: str, names: Iterable[str]) -> dict[str, str]:
    """Named rows of a performance_schema name/value table. Missing names are absent."""
    names = list(names)
    if not names:
        return {}
    ph = ", ".join(["%s"] * len(names))
    rows = query(conn, f"SELECT VARIABLE_NAME AS n, VARIABLE_VALUE AS v FROM "
                       f"performance_schema.{table} WHERE VARIABLE_NAME IN ({ph})", names)
    return {r["n"]: r["v"] for r in rows}


def global_vars(conn, names: Iterable[str]) -> dict[str, str]:
    """Values of the named global variables."""
    return _kv(conn, "global_variables", names)


def global_status(conn, names: Iterable[str]) -> dict[str, str]:
    """Values of the named global status counters."""
    return _kv(conn, "global_status", names)


def gtid_executed(conn) -> str:
    """The server's ``@@GLOBAL.gtid_executed`` with line breaks removed."""
    return gtid_text(query(conn, "SELECT @@GLOBAL.gtid_executed AS g")[0]["g"]) or ""
