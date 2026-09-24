"""The agent's SQL layer, bounded so a hung mysqld can never hold the agent.

Everything the agent sends to mysqld goes through a ``Session``, so tests can inject a fake.
Every call carries a timeout, and a statement that times out leaves the aiomysql connection
mid-protocol, so the session is closed on any error and never reused. Accounts use
caching_sha2_password, so the agent always connects with TLS and verification off, which is
why this module has its own ``insecure_tls()`` (docs/INTERFACES.md, docs/BUGS.md).
"""

from __future__ import annotations

import asyncio
import contextlib
import ssl
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Protocol

import aiomysql
import pymysql
import structlog

log = structlog.get_logger("dbguard.agent.sql")

ACCESS_DENIED = 1045


class DbError(Exception):
    """mysqld refused, errored, or did not answer in time."""

    def __init__(self, msg: str, *, code: int | None = None, timeout: bool = False):
        super().__init__(msg)
        self.code = code
        self.timeout = timeout


class Session(Protocol):
    """One connection. Any error closes it for good."""

    async def query(self, sql: str, args: Any = None, *, timeout: float | None = None,
                    log_sql: bool = False) -> list[dict[str, Any]]:
        """Rows as dicts."""

    async def execute(self, sql: str, args: Any = None, *, timeout: float | None = None,
                      log_sql: bool = False) -> int:
        """Affected row count."""

    def close(self) -> None:
        """Close the connection."""

    @property
    def closed(self) -> bool:
        """True once closed or broken."""


class Database(Protocol):
    """A connection factory with a small idle pool."""

    async def connect(self, timeout: float | None = None) -> Session:
        """A new, unpooled session."""

    def session(self, timeout: float | None = None) -> Any:
        """Async context manager lending a pooled session."""

    async def close(self) -> None:
        """Close every pooled session."""


def insecure_tls() -> ssl.SSLContext:
    """TLS for mysqld's self-signed certificate, encrypted and unverified."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _redact(sql: str, args: Any) -> str:
    """The statement for the log, with password arguments masked."""
    if args is None:
        return sql
    if isinstance(args, dict):
        shown = {k: ("***" if "password" in k.lower() else v) for k, v in args.items()}
        return f"{sql} -- args={shown}"
    return f"{sql} -- args={list(args)}"


class MySQLSession:
    """An aiomysql connection behind the Session protocol."""

    def __init__(self, conn: aiomysql.Connection, default_timeout: float):
        self._conn = conn
        self._default_timeout = default_timeout
        self._closed = False

    @property
    def closed(self) -> bool:
        """True once closed or broken."""
        return self._closed or self._conn.closed

    def close(self) -> None:
        """Close the connection, ignoring errors."""
        if not self._closed:
            self._closed = True
            with contextlib.suppress(Exception):
                self._conn.close()

    async def _run(self, sql: str, args: Any, timeout: float | None, log_sql: bool,
                   fetch: bool) -> Any:
        """Run one statement within ``timeout``, turning every failure into DbError."""
        if self.closed:
            raise DbError("session closed")
        t = self._default_timeout if timeout is None else timeout
        t0 = time.monotonic()
        err: str | None = None

        async def go() -> Any:
            async with self._conn.cursor(aiomysql.DictCursor) as cur:
                n = await cur.execute(sql, args)
                if fetch:
                    return list(await cur.fetchall())
                return n

        try:
            return await asyncio.wait_for(go(), timeout=t)
        except TimeoutError as e:
            err = f"timeout after {t}s"
            self.close()
            raise DbError(err, timeout=True) from e
        except pymysql.err.MySQLError as e:
            code = e.args[0] if e.args and isinstance(e.args[0], int) else None
            err = str(e)
            if code is None or code == 0 or code >= 2000:  # client side error, link is gone
                self.close()
            raise DbError(err, code=code) from e
        except (OSError, RuntimeError, AttributeError) as e:
            err = repr(e)
            self.close()
            raise DbError(err) from e
        finally:
            if log_sql:
                dur = round((time.monotonic() - t0) * 1000, 2)
                if err is None:
                    log.info("sql", sql=_redact(sql, args), duration_ms=dur)
                else:
                    log.warning("sql", sql=_redact(sql, args), duration_ms=dur, error=err)

    async def query(self, sql: str, args: Any = None, *, timeout: float | None = None,
                    log_sql: bool = False) -> list[dict[str, Any]]:
        """Rows as dicts."""
        return await self._run(sql, args, timeout, log_sql, True)

    async def execute(self, sql: str, args: Any = None, *, timeout: float | None = None,
                      log_sql: bool = False) -> int:
        """Affected row count."""
        return await self._run(sql, args, timeout, log_sql, False)


class MySQL:
    """Connection factory with a tiny idle pool.

    Tries the primary credentials first and falls back to root on access denied (the
    dbguard user does not exist until the init scripts ran).
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 fallback: tuple[str, str] | None = None, default_timeout: float = 1.0,
                 max_idle: int = 4):
        self.host = host
        self.port = port
        self.creds = [(user, password)]
        if fallback and fallback[1] is not None and fallback != (user, password):
            self.creds.append(fallback)
        self.default_timeout = default_timeout
        self.max_idle = max_idle
        self._idle: list[MySQLSession] = []
        self._tls = insecure_tls()

    async def _open(self, user: str, password: str, timeout: float) -> aiomysql.Connection:
        """One TLS connection attempt with these credentials."""
        return await asyncio.wait_for(
            aiomysql.connect(host=self.host, port=self.port, user=user, password=password,
                             autocommit=True, connect_timeout=timeout, ssl=self._tls,
                             program_name="dbguard-agent"),
            timeout=timeout)

    async def connect(self, timeout: float | None = None) -> MySQLSession:
        """A new session, trying each credential until one is not denied."""
        t = self.default_timeout if timeout is None else timeout
        last: Exception | None = None
        for user, password in self.creds:
            try:
                conn = await self._open(user, password, t)
                return MySQLSession(conn, self.default_timeout)
            except TimeoutError as e:
                raise DbError(f"connect timeout after {t}s", timeout=True) from e
            except pymysql.err.OperationalError as e:
                code = e.args[0] if e.args else None
                last = e
                if code == ACCESS_DENIED:
                    continue
                raise DbError(str(e), code=code) from e
            except (OSError, RuntimeError) as e:
                raise DbError(repr(e)) from e
        raise DbError(str(last), code=ACCESS_DENIED)

    @asynccontextmanager
    async def session(self, timeout: float | None = None) -> AsyncIterator[MySQLSession]:
        """Lend a pooled session, returning it only if the body finished without error."""
        s: MySQLSession | None = None
        while self._idle:
            cand = self._idle.pop()
            if not cand.closed:
                s = cand
                break
        if s is None:
            s = await self.connect(timeout)
        ok = False
        try:
            yield s
            ok = True
        finally:
            if ok and not s.closed and len(self._idle) < self.max_idle:
                self._idle.append(s)
            else:
                s.close()

    def drop_idle(self) -> None:
        """Close every idle pooled session."""
        for s in self._idle:
            s.close()
        self._idle.clear()

    async def close(self) -> None:
        """Close the pool."""
        self.drop_idle()
