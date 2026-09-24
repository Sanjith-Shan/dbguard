"""Synchronous PyMySQL twin of dbguard.mysqlx for the bin/ scripts (bootstrap, workload,
checker). Same auth decision: TLS with verification off, see dbguard/mysqlx.py."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pymysql
import pymysql.cursors

from dbguard.mysqlx import (
    UNCONFIGURED_REPLICA,  # noqa: F401  (re-exported)
    _gtid_text,
    insecure_tls,
    parse_replica_status,
)


def connect(host: str, port: int, user: str, password: str, timeout: float = 2.0,
            db: str | None = None, *, tls: bool = True, autocommit: bool = True,
            read_timeout: float | None = None,
            write_timeout: float | None = None) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=host, port=port, user=user, password=password, database=db,
        connect_timeout=timeout, read_timeout=read_timeout, write_timeout=write_timeout,
        autocommit=autocommit, cursorclass=pymysql.cursors.DictCursor,
        ssl=insecure_tls() if tls else None,
    )


def query(conn: pymysql.connections.Connection, sql: str,
          args: Any = None) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return list(cur.fetchall())


def execute(conn: pymysql.connections.Connection, sql: str, args: Any = None) -> int:
    with conn.cursor() as cur:
        return cur.execute(sql, args)


def replica_status(conn: pymysql.connections.Connection) -> dict[str, Any] | None:
    rows = query(conn, "SHOW REPLICA STATUS")
    return parse_replica_status(rows[0] if rows else None)


def _kv(conn, table: str, names: Iterable[str]) -> dict[str, str]:
    names = list(names)
    if not names:
        return {}
    ph = ", ".join(["%s"] * len(names))
    rows = query(conn, f"SELECT VARIABLE_NAME AS n, VARIABLE_VALUE AS v FROM "
                       f"performance_schema.{table} WHERE VARIABLE_NAME IN ({ph})", names)
    return {r["n"]: r["v"] for r in rows}


def global_vars(conn, names: Iterable[str]) -> dict[str, str]:
    return _kv(conn, "global_variables", names)


def global_status(conn, names: Iterable[str]) -> dict[str, str]:
    return _kv(conn, "global_status", names)


def gtid_executed(conn) -> str:
    return _gtid_text(query(conn, "SELECT @@GLOBAL.gtid_executed AS g")[0]["g"]) or ""
