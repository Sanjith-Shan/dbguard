"""The agent's logic: role changes, fence, status, heartbeat, wake guard.

HTTP lives in http.py. This module takes its SQL layer, its mysqld supervisor and its
manager client as constructor arguments so the unit tests can replace all three.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import socket
import tempfile
import time
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from dbguard.agent.db import Database, DbError, MySQL, Session
from dbguard.agent.gtidcount import subtract_count
from dbguard.agent.metrics import AgentMetrics
from dbguard.agent.settings import AgentSettings

log = structlog.get_logger("dbguard.agent")

try:
    from importlib.metadata import version as _pkg_version

    AGENT_VERSION = _pkg_version("dbguard")
except Exception:  # noqa: BLE001
    AGENT_VERSION = "0.1.0"

ManagerGet = Callable[[str, float], Awaitable[dict[str, Any]]]

CLIENT_THREADS_SQL = (
    "SELECT PROCESSLIST_ID AS id, PROCESSLIST_USER AS user FROM performance_schema.threads "
    "WHERE TYPE='FOREGROUND' AND NAME='thread/sql/one_connection' "
    "AND PROCESSLIST_ID IS NOT NULL AND PROCESSLIST_ID <> CONNECTION_ID() "
    "AND (PROCESSLIST_USER IS NULL OR PROCESSLIST_USER NOT IN %s)"
)
# repl carries binlog dump threads. The others are system accounts that own threads.
PROTECTED_USERS = ("event_scheduler", "system user", "mysql.session", "mysql.sys")

HEARTBEAT_SQL = (
    "INSERT INTO dbguard.heartbeat (rs, ts, writer) VALUES (%s, NOW(6), %s) "
    "ON DUPLICATE KEY UPDATE ts=NOW(6), writer=VALUES(writer)"
)

RESTART_CLIENT_CODES = {0, 2006, 2013, 2055}   # link lost while mysqld restarts
CLONE_NOT_SUPERVISED = 3707


class AgentError(Exception):
    def __init__(self, msg: str, status: int = 500, **extra: Any):
        super().__init__(msg)
        self.status = status
        self.extra = extra


def _b(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip().upper() in ("1", "ON", "YES", "TRUE")
    return bool(int(v))


def _i(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _gtid(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bytes):
        v = v.decode()
    return v.replace("\n", "")


def _s(v: Any) -> str | None:
    if v is None or v == "":
        return None
    return v.decode() if isinstance(v, bytes) else str(v)


async def default_manager_get(url: str, timeout: float) -> dict[str, Any]:
    import aiohttp

    async with (aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as s,
                s.get(url) as r):
        if r.status != 200:
            raise AgentError(f"manager answered {r.status}", status=502)
        return await r.json()


async def tcp_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        _, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
        w.close()
        return True
    except (OSError, TimeoutError, socket.gaierror):
        return False


class Agent:
    def __init__(self, settings: AgentSettings, db: Database, supervisor: Any, *,
                 manager_get: ManagerGet | None = None,
                 donor_db: Callable[[str], Database] | None = None,
                 metrics: AgentMetrics | None = None,
                 reachable: Callable[[str, int], Awaitable[bool]] | None = None):
        self.s = settings
        self.db = db
        self.sup = supervisor
        self.manager_get = manager_get or default_manager_get
        self.donor_db = donor_db or self._default_donor_db
        self.reachable = reachable or (lambda h, p: tcp_reachable(h, p, 0.5))
        self.m = metrics or AgentMetrics()
        self.semisync = settings.semisync
        self.fenced = os.path.exists(settings.fence_file)
        self.started_mono = time.monotonic()
        self.last_tick = time.monotonic()
        self.startup_done = asyncio.Event()
        self._fence_lock = asyncio.Lock()
        self._role_lock = asyncio.Lock()
        self._guard_task: asyncio.Task | None = None
        self._tasks: list[asyncio.Task] = []
        self._stopping = False
        self._hb_inflight_since: float | None = None
        self._hb_session: Session | None = None
        self._role = "unknown"
        self.rebuilding = False
        self._mgr_unreachable_since: float | None = None
        self._sf_clients: int | None = None
        self.m.fenced.set(1 if self.fenced else 0)
        self.m.set_role("fenced" if self.fenced else "unknown")

    # ------------------------------------------------------------------ helpers

    def _default_donor_db(self, host: str) -> Database:
        return MySQL(host, self.s.source_port, self.s.mysql_user, self.s.mysql_password,
                     fallback=("root", self.s.root_password) if self.s.root_password else None,
                     default_timeout=self.s.sql_timeout_s)

    async def _sql(self, fn: Callable[[Session], Awaitable[Any]],
                   timeout: float | None = None) -> Any:
        """Run fn on a pooled session, retrying once if the pooled link was stale."""
        for attempt in (0, 1):
            try:
                async with self.db.session(timeout if timeout is not None
                                           else self.s.sql_timeout_s) as sess:
                    return await fn(sess)
            except DbError as e:
                if attempt == 0 and not e.timeout and e.code in RESTART_CLIENT_CODES:
                    continue
                raise

    async def _role_sql(self, sess: Session, sql: str, args: Any = None,
                        timeout: float | None = None) -> int:
        return await sess.execute(sql, args, timeout=timeout or self.s.role_change_timeout_s,
                                  log_sql=True)

    def _write_fence_file(self) -> None:
        os.makedirs(self.s.state_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".fenced.", dir=self.s.state_dir)
        try:
            with os.fdopen(fd, "w") as f:
                f.write(f"{time.time()}\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.s.fence_file)
            dfd = os.open(self.s.state_dir, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _set_fenced(self, value: bool) -> None:
        if value:
            self._write_fence_file()
        else:
            try:
                os.unlink(self.s.fence_file)
            except FileNotFoundError:
                pass
        self.fenced = value
        self.m.fenced.set(1 if value else 0)
        if value:
            self._set_role("fenced")
        log.info("fence_flag", fenced=value, file=self.s.fence_file)

    def _set_role(self, role: str) -> None:
        if role != self._role:
            log.info("role", role=role, previous=self._role)
        self._role = role
        self.m.set_role(role)

    def mysqld_alive(self) -> bool | None:
        return self.sup.alive

    # ------------------------------------------------------------------ /primary

    async def primary_check(self) -> tuple[int, dict[str, str]]:
        await self.ensure_awake()
        if self.fenced:
            return 503, {"role": "fenced"}
        if not self.startup_done.is_set() or self.rebuilding:
            return 503, {"role": "unknown"}
        try:
            rows = await self._sql(lambda s: s.query(
                "SELECT @@GLOBAL.super_read_only AS sro", timeout=self.s.sql_timeout_s))
            sro = _b(rows[0]["sro"])
        except DbError:
            self._set_role("unknown")
            return 503, {"role": "unknown"}
        if self.fenced:  # a fence may have landed while we waited on mysqld
            return 503, {"role": "fenced"}
        if sro is False:
            self._set_role("primary")
            return 200, {"role": "primary"}
        self._set_role("replica")
        return 503, {"role": "replica"}

    # ------------------------------------------------------------------ /status

    async def status(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "node": self.s.node, "rs": self.s.rs, "ts": time.time(),
            "mysqld_alive": None, "mysqld_responsive": False, "mysqld_pid": self.sup.pid,
            "fenced": self.fenced, "super_read_only": None, "read_only": None,
            "gtid_executed": None,
            "replica": {"configured": False, "source_host": None, "io_running": None,
                        "sql_running": None, "seconds_behind_source": None,
                        "retrieved_gtid_set": None, "executed_gtid_set": None,
                        "last_io_error": None, "last_sql_error": None},
            "semisync": {"source_enabled": None, "replica_enabled": None,
                         "source_status": None, "replica_status": None,
                         "source_clients": None, "avg_wait_time_us": None,
                         "no_tx": None, "yes_tx": None, "wait_sessions": None,
                         "mode": self.semisync},
            "source_reachable": None,
            "heartbeat": {"ts": None, "age_s": None, "writer": None,
                          "stalled_s": self.heartbeat_stalled_s()},
            "agent_uptime_s": round(time.monotonic() - self.started_mono, 3),
            "agent_version": AGENT_VERSION,
            "self_fence": self.self_fence_state(),
        }

        async def collect(sess: Session) -> None:
            t = self.s.sql_timeout_s
            r = (await sess.query("SELECT @@GLOBAL.super_read_only AS sro, "
                                  "@@GLOBAL.read_only AS ro, @@GLOBAL.gtid_executed AS g",
                                  timeout=t))[0]
            out["mysqld_responsive"] = True
            out["super_read_only"] = _b(r["sro"])
            out["read_only"] = _b(r["ro"])
            out["gtid_executed"] = _gtid(r["g"])
            ss = out["semisync"]
            for row in await sess.query("SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync_%%_enabled'",
                                        timeout=t):
                name, val = row["Variable_name"], row["Value"]
                if name == "rpl_semi_sync_source_enabled":
                    ss["source_enabled"] = _b(val)
                elif name == "rpl_semi_sync_replica_enabled":
                    ss["replica_enabled"] = _b(val)
            smap = {"Rpl_semi_sync_source_status": ("source_status", _b),
                    "Rpl_semi_sync_replica_status": ("replica_status", _b),
                    "Rpl_semi_sync_source_clients": ("source_clients", _i),
                    "Rpl_semi_sync_source_tx_avg_wait_time": ("avg_wait_time_us", _i),
                    "Rpl_semi_sync_source_no_tx": ("no_tx", _i),
                    "Rpl_semi_sync_source_yes_tx": ("yes_tx", _i),
                    "Rpl_semi_sync_source_wait_sessions": ("wait_sessions", _i)}
            for row in await sess.query("SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_%%'", timeout=t):
                if row["Variable_name"] in smap:
                    key, conv = smap[row["Variable_name"]]
                    ss[key] = conv(row["Value"])
            rs = await sess.query("SHOW REPLICA STATUS", timeout=t)
            if rs:
                row = rs[0]
                out["replica"] = {
                    "configured": True,
                    "source_host": _s(row.get("Source_Host")),
                    "io_running": _s(row.get("Replica_IO_Running")),
                    "sql_running": _s(row.get("Replica_SQL_Running")),
                    "seconds_behind_source": _i(row.get("Seconds_Behind_Source")),
                    "retrieved_gtid_set": _gtid(row.get("Retrieved_Gtid_Set")) or "",
                    "executed_gtid_set": _gtid(row.get("Executed_Gtid_Set")) or "",
                    "last_io_error": _s(row.get("Last_IO_Error")),
                    "last_sql_error": _s(row.get("Last_SQL_Error")),
                }
            try:
                hb = await sess.query("SELECT UNIX_TIMESTAMP(ts) AS ts, writer FROM "
                                      "dbguard.heartbeat WHERE rs=%s", (self.s.rs,), timeout=t)
            except DbError as e:
                if e.timeout or e.code is None or e.code in RESTART_CLIENT_CODES:
                    raise
                hb = []  # schema not created yet
            if hb and hb[0]["ts"] is not None:
                ts = float(hb[0]["ts"])
                out["heartbeat"]["ts"] = ts
                out["heartbeat"]["age_s"] = round(max(0.0, time.time() - ts), 3)
                out["heartbeat"]["writer"] = _s(hb[0]["writer"])

        try:
            await self._sql(collect)
        except DbError as e:
            out["mysqld_responsive"] = False
            out["error"] = str(e)
        src = out["replica"]["source_host"]
        if src:
            out["source_reachable"] = await self.reachable(src, self.s.source_port)
        alive = self.sup.alive
        out["mysqld_alive"] = alive if alive is not None else out["mysqld_responsive"]
        self.m.mysqld_alive.set(1 if out["mysqld_alive"] else 0)
        if out["heartbeat"]["age_s"] is not None:
            self.m.heartbeat_age.set(out["heartbeat"]["age_s"])
        if self.fenced:
            self._set_role("fenced")
        elif out["super_read_only"] is False:
            self._set_role("primary")
        elif out["super_read_only"] is True:
            self._set_role("replica")
        else:
            self._set_role("unknown")
        out["role"] = self._role
        return out

    # ------------------------------------------------------------------ /fence

    async def fence(self, reason: str = "api") -> tuple[int, dict[str, Any]]:
        async with self._fence_lock:
            t0 = time.monotonic()
            self._set_fenced(True)  # FIRST: /primary answers 503 from here on
            deadline = t0 + self.s.fence_deadline_s
            # The agent's own pooled and heartbeat links are closed, not killed.
            self.db_drop_idle()
            if self._hb_session is not None:
                self._hb_session.close()
            method = "sql"
            killed = 0
            gtid: str | None = None
            error: str | None = None
            sess: Session | None = None
            try:
                sess = await self.db.connect(timeout=max(0.05, deadline - time.monotonic()))
                await sess.execute("SET GLOBAL super_read_only=1",
                                   timeout=max(0.05, deadline - time.monotonic()), log_sql=True)
            except DbError as e:
                error = str(e)
                method = "kill"
                if sess is not None:
                    sess.close()
                sess = None
            if method == "sql":
                assert sess is not None
                try:
                    killed = await self._kill_clients(sess)
                    rows = await sess.query("SELECT @@GLOBAL.gtid_executed AS g",
                                            timeout=self.s.sql_timeout_s)
                    gtid = _gtid(rows[0]["g"])
                except DbError as e:
                    log.warning("fence_post_set_error", error=str(e))
                finally:
                    sess.close()
            else:
                pid = self.sup.kill(signal.SIGKILL)
                if pid is None:
                    # Nothing to kill: either mysqld is already gone, or we do not own it.
                    if self.sup.supervised and not self.sup.alive:
                        log.warning("fence_mysqld_already_down", error=error)
                    else:
                        dur = round((time.monotonic() - t0) * 1000, 2)
                        self.m.fences.labels(method="failed").inc()
                        log.error("fence_failed", reason=reason, error=error, duration_ms=dur)
                        return 500, {"fenced": True, "method": "failed", "gtid_executed": None,
                                     "duration_ms": dur, "killed_threads": 0, "error": error}
                else:
                    await self.sup.wait_exit(1.0)
                self.db_drop_idle()
            dur = round((time.monotonic() - t0) * 1000, 2)
            self.m.fences.labels(method=method).inc()
            log.info("fence", reason=reason, method=method, duration_ms=dur,
                     killed_threads=killed, gtid_executed=gtid, error=error)
            return 200, {"fenced": True, "method": method, "gtid_executed": gtid,
                         "duration_ms": dur, "killed_threads": killed}

    async def _kill_clients(self, sess: Session) -> int:
        rows = await sess.query(CLIENT_THREADS_SQL,
                                ((self.s.repl_user,) + PROTECTED_USERS,),
                                timeout=self.s.sql_timeout_s, log_sql=True)
        killed = 0
        for row in rows:
            try:
                await sess.execute(f"KILL {int(row['id'])}", timeout=self.s.sql_timeout_s,
                                   log_sql=True)
                killed += 1
            except DbError as e:
                if e.timeout:
                    raise
                # ER_NO_SUCH_THREAD: it ended on its own between the SELECT and the KILL.
        return killed

    def db_drop_idle(self) -> None:
        drop = getattr(self.db, "drop_idle", None)
        if drop:
            drop()

    async def unfence(self) -> dict[str, Any]:
        async with self._fence_lock:
            self._set_fenced(False)
        return {"fenced": False}

    # ------------------------------------------------------------------ role changes

    async def promote(self) -> dict[str, Any]:
        async with self._role_lock:
            t0 = time.monotonic()
            async with self.db.session(self.s.sql_timeout_s) as s:
                await self._role_sql(s, "STOP REPLICA")
                await self._role_sql(s, "RESET REPLICA ALL")
                # Semi-sync source goes on before the node becomes writable, so no write
                # is ever acknowledged without a replica ack.
                await self._role_sql(s, "SET GLOBAL rpl_semi_sync_replica_enabled=0")
                await self._role_sql(s, "SET GLOBAL rpl_semi_sync_source_enabled="
                                     + ("1" if self.semisync else "0"))
                await self._role_sql(s, "SET GLOBAL super_read_only=0")
                await self._role_sql(s, "SET GLOBAL read_only=0")
                rows = await s.query("SELECT @@GLOBAL.gtid_executed AS g",
                                     timeout=self.s.role_change_timeout_s)
            async with self._fence_lock:
                self._set_fenced(False)
            self._set_role("primary")
            self.m.promotions.inc()
            dur = round((time.monotonic() - t0) * 1000, 2)
            gtid = _gtid(rows[0]["g"])
            log.info("promote", duration_ms=dur, gtid_executed=gtid, semisync=self.semisync)
            return {"gtid_executed": gtid, "duration_ms": dur}

    async def repoint(self, source: str) -> dict[str, Any]:
        if not source or source == self.s.node:
            raise AgentError(f"bad source {source!r}", status=400)
        async with self._role_lock:
            t0 = time.monotonic()
            async with self.db.session(self.s.sql_timeout_s) as s:
                await self._role_sql(s, "STOP REPLICA")
                await self._role_sql(s, "SET GLOBAL super_read_only=1")
                await self._role_sql(s, "SET GLOBAL rpl_semi_sync_source_enabled=0")
                await self._role_sql(s, "SET GLOBAL rpl_semi_sync_replica_enabled="
                                     + ("1" if self.semisync else "0"))
                await self._role_sql(
                    s,
                    "CHANGE REPLICATION SOURCE TO SOURCE_HOST=%(host)s, SOURCE_PORT=%(port)s, "
                    "SOURCE_USER=%(user)s, SOURCE_PASSWORD=%(password)s, "
                    "SOURCE_AUTO_POSITION=1, SOURCE_HEARTBEAT_PERIOD=0.5, "
                    "SOURCE_CONNECT_RETRY=1, SOURCE_RETRY_COUNT=86400, SOURCE_SSL=1",
                    {"host": source, "port": self.s.source_port, "user": self.s.repl_user,
                     "password": self.s.repl_password})
                await self._role_sql(s, "START REPLICA")
            self._set_role("fenced" if self.fenced else "replica")
            self.m.repoints.inc()
            dur = round((time.monotonic() - t0) * 1000, 2)
            log.info("repoint", source=source, duration_ms=dur, semisync=self.semisync)
            return {"ok": True, "duration_ms": dur}

    async def configure(self, semisync: bool | None = None, reason: str = "api"
                        ) -> dict[str, Any]:
        if semisync is not None:
            self.semisync = bool(semisync)
        async with self._role_lock:
            async with self.db.session(self.s.sql_timeout_s) as s:
                r = (await s.query("SELECT @@GLOBAL.super_read_only AS sro",
                                   timeout=self.s.sql_timeout_s))[0]
                writable = _b(r["sro"]) is False and not self.fenced
                cur = {row["Variable_name"]: _b(row["Value"]) for row in await s.query(
                    "SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync_%%_enabled'",
                    timeout=self.s.sql_timeout_s)}
                want_src = self.semisync and writable
                want_rep = self.semisync and not writable
                if cur.get("rpl_semi_sync_source_enabled") and not want_src:
                    waiting = {row["Variable_name"]: _i(row["Value"]) for row in await s.query(
                        "SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_wait_sessions'",
                        timeout=self.s.sql_timeout_s)}
                    if waiting.get("Rpl_semi_sync_source_wait_sessions"):
                        # Switching the source side off would release sessions that are
                        # waiting for an ack, and acknowledge writes no replica has.
                        log.warning("configure_keep_source_semisync", reason=reason,
                                    wait_sessions=waiting)
                        want_src = True
                if cur.get("rpl_semi_sync_source_enabled") != want_src:
                    await self._role_sql(s, "SET GLOBAL rpl_semi_sync_source_enabled="
                                         + ("1" if want_src else "0"))
                if cur.get("rpl_semi_sync_replica_enabled") != want_rep:
                    await self._role_sql(s, "SET GLOBAL rpl_semi_sync_replica_enabled="
                                         + ("1" if want_rep else "0"))
                    rs = await s.query("SHOW REPLICA STATUS", timeout=self.s.sql_timeout_s)
                    if rs and _s(rs[0].get("Replica_IO_Running")) in ("Yes", "Connecting"):
                        # The replica side only takes effect when the IO thread starts.
                        await self._role_sql(s, "STOP REPLICA IO_THREAD")
                        await self._role_sql(s, "START REPLICA IO_THREAD")
            role = "primary" if writable else ("fenced" if self.fenced else "replica")
            log.info("configure", reason=reason, semisync=self.semisync, role=role,
                     source_enabled=want_src, replica_enabled=want_rep)
            return {"ok": True, "semisync": self.semisync, "role": role,
                    "source_enabled": want_src, "replica_enabled": want_rep}

    async def rebuild(self, donor: str) -> dict[str, Any]:
        if not donor or donor == self.s.node:
            raise AgentError(f"bad donor {donor!r}", status=400)
        async with self._role_lock:
            t0 = time.monotonic()
            own = (await self._sql(lambda s: s.query("SELECT @@GLOBAL.gtid_executed AS g")))[0]
            own_gtid = _gtid(own["g"])
            ddb = self.donor_db(donor)
            try:
                async with ddb.session(self.s.sql_timeout_s * 3) as ds:
                    drow = (await ds.query("SELECT @@GLOBAL.gtid_executed AS g",
                                           timeout=self.s.sql_timeout_s * 3))[0]
            except DbError as e:
                self.m.rebuilds.labels(outcome="donor_unreachable").inc()
                raise AgentError(f"donor {donor} unreachable: {e}", status=502) from e
            finally:
                await ddb.close()
            donor_gtid = _gtid(drow["g"])
            phantom = subtract_count(own_gtid, donor_gtid)
            log.info("rebuild_start", donor=donor, own_gtid=own_gtid, donor_gtid=donor_gtid,
                     phantom_gtids=phantom)
            gen = self.sup.generation
            s = await self.db.connect(self.s.sql_timeout_s)
            restarting = False
            # CLONE refuses to run on a super_read_only recipient (ER 1290 on 8.4), so the
            # node is briefly writable. While `rebuilding` is set /primary answers 503, and
            # neither the heartbeat nor the guards treat the node as a primary. Anything
            # written in that window is wiped by the clone itself.
            self.rebuilding = True
            try:
                await self._role_sql(s, "STOP REPLICA")
                await self._role_sql(s, "SET GLOBAL clone_valid_donor_list=%s",
                                     (f"{donor}:{self.s.source_port}",))
                await self._role_sql(s, "SET GLOBAL super_read_only=0")
                try:
                    await self._role_sql(
                        s, "CLONE INSTANCE FROM %s@%s:%s IDENTIFIED BY %s REQUIRE SSL",
                        (self.s.mysql_user, donor, self.s.source_port, self.s.mysql_password),
                        timeout=self.s.clone_timeout_s)
                    restarting = True
                except DbError as e:
                    if e.code == CLONE_NOT_SUPERVISED or e.code in RESTART_CLIENT_CODES:
                        restarting = True
                        log.info("rebuild_clone_done_restart_pending", code=e.code, error=str(e))
                    else:
                        self.m.rebuilds.labels(outcome="clone_failed").inc()
                        with contextlib.suppress(DbError):
                            await self._role_sql(s, "SET GLOBAL super_read_only=1")
                        self.rebuilding = False
                        raise AgentError(f"clone failed: {e}", status=500) from e
            except BaseException:
                self.rebuilding = False
                raise
            finally:
                s.close()
            self.db_drop_idle()
            try:
                return await self._rebuild_finish(donor, gen, t0, phantom, restarting)
            finally:
                self.rebuilding = False

    async def _rebuild_finish(self, donor: str, gen: int, t0: float, phantom: int,
                              restarting: bool) -> dict[str, Any]:
        if self.sup.supervised:
            if not await self.sup.wait_exit(60.0):
                # mysqld did not stop on its own after the clone. Make it.
                self.sup.kill(signal.SIGTERM)
                await self.sup.wait_exit(120.0)
            if not await self.sup.wait_generation(gen, self.s.restart_wait_s):
                self.m.rebuilds.labels(outcome="restart_timeout").inc()
                raise AgentError("mysqld did not come back after clone", status=500)
        if not await self.wait_responsive(self.s.restart_wait_s):
            self.m.rebuilds.labels(outcome="restart_timeout").inc()
            raise AgentError("mysqld not responsive after clone", status=500)

        async def post(sess: Session) -> dict[str, Any]:
            await self._role_sql(sess, "STOP REPLICA")
            await self._role_sql(sess, "RESET REPLICA ALL")
            await self._role_sql(sess, "SET GLOBAL super_read_only=1")
            g = (await sess.query("SELECT @@GLOBAL.gtid_executed AS g"))[0]["g"]
            b = None
            try:
                rows = await sess.query(
                    "SELECT SUM(DATA) AS b FROM performance_schema.clone_progress")
                b = _i(rows[0]["b"]) if rows else None
            except DbError:
                pass
            return {"g": _gtid(g), "b": b}

        res = await self._sql(post, timeout=5.0)
        dur = round((time.monotonic() - t0) * 1000, 2)
        self.m.rebuilds.labels(outcome="ok").inc()
        self.m.phantom_gtids.inc(phantom)
        log.info("rebuild", donor=donor, phantom_gtids=phantom, duration_ms=dur,
                 bytes=res["b"], gtid_executed=res["g"], restarted=restarting)
        return {"ok": True, "phantom_gtids": phantom, "duration_ms": dur,
                "bytes": res["b"] or 0, "gtid_executed": res["g"]}

    # ------------------------------------------------------------------ test hooks

    def kill_mysqld(self) -> dict[str, Any]:
        pid = self.sup.kill(signal.SIGKILL)
        if pid is None:
            raise AgentError("no supervised mysqld to kill", status=409)
        self.db_drop_idle()
        return {"ok": True, "pid": pid}

    def hang_mysqld(self, seconds: float) -> dict[str, Any]:
        pid = self.sup.kill(signal.SIGSTOP)
        if pid is None:
            raise AgentError("no supervised mysqld to hang", status=409)

        async def resume() -> None:
            await asyncio.sleep(seconds)
            if self.sup.kill(signal.SIGCONT) is not None:
                log.info("mysqld_resumed", pid=pid)

        self._tasks.append(asyncio.create_task(resume()))
        return {"ok": True, "pid": pid, "seconds": seconds}

    # ------------------------------------------------------------------ liveness

    async def responsive(self) -> bool:
        try:
            await self._sql(lambda s: s.query("SELECT 1 AS one"))
            return True
        except DbError:
            return False

    async def wait_responsive(self, timeout: float) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end and not self._stopping:
            if await self.responsive():
                return True
            await asyncio.sleep(0.5)
        return False

    # ------------------------------------------------------------------ wake guard

    async def wake_guard(self, reason: str) -> str:
        """Returns the decision: fence | ok | fence_file | leave | fence_failed."""
        url = f"{self.s.manager_url.rstrip('/')}/v1/sets/{self.s.rs}/primary" \
            if self.s.manager_url else None
        answer: dict[str, Any] | None = None
        err: str | None = None
        if url:
            try:
                answer = await self.manager_get(url, self.s.manager_timeout_s)
            except Exception as e:  # noqa: BLE001  any failure means unreachable
                err = f"{type(e).__name__}: {e}"
        else:
            err = "DBGUARD_MANAGER_URL not set"
        writable: bool | None = None
        try:
            rows = await self._sql(lambda s: s.query("SELECT @@GLOBAL.super_read_only AS sro"))
            writable = _b(rows[0]["sro"]) is False and not self.rebuilding
        except DbError as e:
            err = (err + "; " if err else "") + f"mysqld: {e}"
        fence_file = os.path.exists(self.s.fence_file)
        if answer is not None:
            primary = answer.get("primary")
            if primary != self.s.node and writable is not False:
                # writable None means mysqld did not answer; fencing is the safe choice.
                decision = "fence"
            else:
                decision = "ok"
        else:
            primary = None
            decision = "fence_file" if fence_file else "leave"
        log.info("wake_guard", reason=reason, decision=decision, manager_primary=primary,
                 manager_state=(answer or {}).get("state"), writable=writable,
                 fence_file=fence_file, error=err)
        self.m.wake_guard.labels(reason=reason, decision=decision).inc()
        if decision in ("fence", "fence_file"):
            code, _ = await self.fence(reason=f"wake_guard:{reason}")
            if code != 200:
                return "fence_failed"
        return decision

    def _trigger_guard(self, reason: str) -> asyncio.Task:
        if self._guard_task is None or self._guard_task.done():
            self._guard_task = asyncio.create_task(self.wake_guard(reason), name="wake-guard")
        return self._guard_task

    async def ensure_awake(self) -> None:
        """Called by /primary so a frozen-then-thawed agent never answers stale."""
        now = time.monotonic()
        if now - self.last_tick > self.s.wake_gap_s:
            log.warning("wake_gap", gap_s=round(now - self.last_tick, 3), where="request")
            self.last_tick = now
            self._trigger_guard("wake")
        task = self._guard_task
        if task is not None and not task.done():
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.shield(task), 2.5)

    async def _ticker(self) -> None:
        while not self._stopping:
            await asyncio.sleep(0.5)
            now = time.monotonic()
            gap = now - self.last_tick
            self.last_tick = now
            if gap > self.s.wake_gap_s:
                log.warning("wake_gap", gap_s=round(gap, 3), where="ticker")
                self._trigger_guard("wake")

    # ------------------------------------------------------------------ heartbeat

    def heartbeat_stalled_s(self) -> float:
        since = self._hb_inflight_since
        return round(time.monotonic() - since, 3) if since is not None else 0.0

    async def _heartbeat(self) -> None:
        await self.startup_done.wait()
        while not self._stopping:
            await asyncio.sleep(self.s.heartbeat_interval_s)
            self.m.heartbeat_stalled.set(0)
            if self.fenced or self.rebuilding:
                continue
            try:
                if self._hb_session is None or self._hb_session.closed:
                    self._hb_session = await self.db.connect(self.s.sql_timeout_s)
                sess = self._hb_session
                rows = await sess.query("SELECT @@GLOBAL.super_read_only AS sro",
                                        timeout=self.s.sql_timeout_s)
                if _b(rows[0]["sro"]) is not False or self.fenced or self.rebuilding:
                    continue
                self._hb_inflight_since = time.monotonic()
                # A semi-sync stall blocks this for as long as the source timeout. That
                # is by design, /status reports it as heartbeat.stalled_s.
                await sess.execute(HEARTBEAT_SQL, (self.s.rs, self.s.node), timeout=3700.0)
                self.m.heartbeat_writes.inc()
            except DbError as e:
                self.m.heartbeat_errors.inc()
                if self._hb_session is not None:
                    self._hb_session.close()
                self._hb_session = None
                log.debug("heartbeat_error", error=str(e))
            finally:
                self._hb_inflight_since = None

    # ------------------------------------------------------------------ self-fence lease

    def self_fence_enabled(self) -> bool:
        return bool(self.semisync and self.s.self_fence_after_s > 0 and self.s.manager_url)

    def manager_unreachable_s(self) -> float:
        since = self._mgr_unreachable_since
        return round(time.monotonic() - since, 3) if since is not None else 0.0

    def self_fence_state(self) -> dict[str, Any]:
        return {"enabled": self.self_fence_enabled(),
                "manager_unreachable_s": self.manager_unreachable_s(),
                "semisync_clients": self._sf_clients}

    def _sf_reset(self) -> None:
        self._mgr_unreachable_since = None

    async def self_fence_tick(self) -> str:
        """One lease check. A primary cut off from the manager AND from every semi-sync
        replica fences itself after self_fence_after_s, before the one hour semi-sync
        timeout can fall back to async and acknowledge writes that exist nowhere else.

        Returns disabled | not_primary | unknown | ok | counting | self_fence."""
        if not self.self_fence_enabled():
            self._sf_reset()
            return "disabled"
        if self.fenced or self.rebuilding:
            self._sf_reset()
            return "not_primary"
        try:
            async def read(sess: Session) -> tuple[bool | None, int | None]:
                r = await sess.query("SELECT @@GLOBAL.super_read_only AS sro")
                rows = await sess.query(
                    "SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_clients'")
                return _b(r[0]["sro"]), (_i(rows[0]["Value"]) if rows else None)
            sro, clients = await self._sql(read)
        except DbError:
            sro, clients = None, None
        self._sf_clients = clients
        if sro is True:
            self._sf_reset()
            return "not_primary"
        url = f"{self.s.manager_url.rstrip('/')}/v1/sets/{self.s.rs}/primary"
        err: str | None = None
        try:
            await self.manager_get(url, self.s.manager_timeout_s)
            reachable = True
        except Exception as e:  # noqa: BLE001  any failure means unreachable
            reachable = False
            err = f"{type(e).__name__}: {e}"
        if reachable:
            self._sf_reset()
            return "ok"
        if self._mgr_unreachable_since is None:
            self._mgr_unreachable_since = time.monotonic()
        if sro is None or clients is None:
            return "unknown"  # mysqld did not answer; keep counting, decide on facts only
        if clients > 0:
            # Semi-sync replicas still ack, so the manager alone being gone (Experiment 4,
            # manager partitioned from the primary) is no reason to stop taking writes.
            self._sf_reset()
            return "ok"
        unreachable_s = self.manager_unreachable_s()
        if unreachable_s < self.s.self_fence_after_s:
            return "counting"
        log.warning("self_fence", manager_unreachable_s=unreachable_s,
                    semisync_clients=clients, manager_error=err,
                    after_s=self.s.self_fence_after_s)
        self.m.self_fences.inc()
        await self.fence(reason="self_fence")
        self._sf_reset()
        return "self_fence"

    async def _self_fence_loop(self) -> None:
        await self.startup_done.wait()
        while not self._stopping:
            await asyncio.sleep(self.s.self_fence_interval_s)
            try:
                await self.self_fence_tick()
            except Exception:
                log.exception("self_fence_tick_failed")

    # ------------------------------------------------------------------ lifecycle

    async def _startup(self) -> None:
        log.info("agent_startup", node=self.s.node, rs=self.s.rs, semisync=self.semisync,
                 fenced=self.fenced, supervised=self.sup.supervised)
        while not self._stopping and not await self.wait_responsive(5.0):
            log.info("waiting_for_mysqld")
        if self._stopping:
            return
        await self.wake_guard("startup")
        await self._reassert("startup")
        self.startup_done.set()

    async def _reassert(self, reason: str) -> None:
        try:
            await self.configure(None, reason=reason)
        except (DbError, AgentError) as e:
            log.warning("configure_failed", reason=reason, error=str(e))

    def on_mysqld_restart(self) -> None:
        """Supervisor callback: a new mysqld was spawned (not the first one)."""
        self.m.mysqld_restarts.inc()
        self.db_drop_idle()

        async def after() -> None:
            if await self.wait_responsive(self.s.restart_wait_s):
                await self._reassert("mysqld_restart")

        self._tasks.append(asyncio.create_task(after(), name="after-restart"))

    async def start(self) -> None:
        await self.sup.start()
        self._tasks += [asyncio.create_task(self._startup(), name="startup"),
                        asyncio.create_task(self._ticker(), name="ticker"),
                        asyncio.create_task(self._heartbeat(), name="heartbeat"),
                        asyncio.create_task(self._self_fence_loop(), name="self-fence")]

    async def stop(self) -> None:
        self._stopping = True
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        if self._hb_session is not None:
            self._hb_session.close()
        await self.sup.stop()
        await self.db.close()
