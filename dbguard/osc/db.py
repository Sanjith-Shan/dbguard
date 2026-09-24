"""The real Executor, a thin wrapper over a PyMySQL connection from dbguard.mysqlx_sync
(TLS without certificate check, the fleet's auth decision in docs/INTERFACES.md)."""

from __future__ import annotations

from typing import Any

import pymysql

from dbguard import mysqlx_sync


def error_code(e: BaseException) -> int | None:
    if isinstance(e, pymysql.err.MySQLError) and e.args and isinstance(e.args[0], int):
        return e.args[0]
    return None


class PyMySQLExecutor:
    def __init__(self, conn: pymysql.connections.Connection):
        self.conn = conn

    def query(self, sql: str, args: Any = None) -> list[dict[str, Any]]:
        return mysqlx_sync.query(self.conn, sql, args)

    def execute(self, sql: str, args: Any = None) -> int:
        return mysqlx_sync.execute(self.conn, sql, args)

    def thread_id(self) -> int | None:
        try:
            return self.conn.thread_id()
        except Exception:  # noqa: BLE001
            return None

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass


def connector(host: str, port: int, user: str, password: str, *, tls: bool = True,
              timeout: float = 5.0):
    """Returns a zero-argument function that opens a fresh executor. The runner uses a second
    connection for cleanup after Ctrl-C, when the first may be stuck mid-statement."""
    def open_() -> PyMySQLExecutor:
        return PyMySQLExecutor(mysqlx_sync.connect(host, port, user, password, timeout=timeout,
                                                   tls=tls, autocommit=True))
    return open_
