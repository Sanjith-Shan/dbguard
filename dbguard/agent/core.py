"""The agent's logic, the only code that changes its node's role (HTTP lives in http.py).

The fence flag file is written before any SQL, because the SQL may never return, and it
survives restarts. When the SET cannot finish, mysqld is killed and its restart held for 20 s,
so the dead primary cannot serve its unacked binlog tail. The self-fence lease fences a primary
that has lost both the manager and every semi-sync replica before the one-hour semi-sync
timeout can fall back to async. ``/primary`` answers HAProxy from memory (the flag plus a
100 ms sample of super_read_only), so a hung mysqld fails it by staleness, never by a stuck
query. The wake guard asks the manager who is primary after a freeze. SQL layer, supervisor
and manager client are constructor arguments so the unit tests replace all three.
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
    """A request failed. ``status`` is the HTTP answer and ``extra`` joins its body."""

    def __init__(self, msg: str, status: int = 500, **extra: Any):
        super().__init__(msg)
        self.status = status
        self.extra = extra


def _b(v: Any) -> bool | None:
    """A MySQL boolean (1, ON, YES) as bool, None for NULL."""
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip().upper() in ("1", "ON", "YES", "TRUE")
    return bool(int(v))


def _i(v: Any) -> int | None:
    """An int, or None for NULL, '' and junk."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _gtid(v: Any) -> str | None:
    """A GTID set with the server's line breaks removed."""
    if v is None:
        return None
    if isinstance(v, bytes):
        v = v.decode()
    return v.replace("\n", "")


def _s(v: Any) -> str | None:
    """A string, None for NULL and ''."""
    if v is None or v == "":
        return None
    return v.decode() if isinstance(v, bytes) else str(v)


class ManagerUnknown(Exception):
    """The manager answered but does not know the primary yet (503, e.g. before discovery)."""


async def default_manager_get(url: str, timeout: float) -> dict[str, Any]:
    """GET the manager's /v1/sets/<rs>/primary. A 503 raises ManagerUnknown."""
    import aiohttp

    async with (aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as s,
                s.get(url) as r):
        if r.status == 503:
            raise ManagerUnknown(f"manager answered 503: {(await r.text())[:200]}")
        if r.status != 200:
            raise AgentError(f"manager answered {r.status}", status=502)
        return await r.json()


async def tcp_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    """True if a TCP connection to host:port opens within ``timeout``."""
    try:
        _, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
        w.close()
        return True
    except (OSError, TimeoutError, socket.gaierror):
        return False


class Agent:
    """One node's agent. Owns the fence flag, the role changes and the guards."""

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
        self._step: str | None = None
        self._primary_sample: tuple[float, bool | None, bool | None] | None = None
        self._sample_session: Session | None = None
        self._sample_lock = asyncio.Lock()
        self.self_fence_on = True
        self.wake_guard_on = settings.wake_guard
        self._mgr_unreachable_since: float | None = None
        self._sf_clients: int | None = None
        self.m.fenced.set(1 if self.fenced else 0)
        self.m.set_role("fenced" if self.fenced else "unknown")

    # ------------------------------------------------------------------ helpers

    def _default_donor_db(self, host: str) -> Database:
        """A connection factory for the clone donor's mysqld."""
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
        """Run one logged role-change statement, remembering it as the current step."""
        self._step = sql
        return await sess.execute(sql, args, timeout=timeout or self.s.role_change_timeout_s,
                                  log_sql=True)

    def _write_fence_file(self) -> None:
        """Persist the flag durably, temp file, fsync, rename, fsync the directory."""
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
        """Set or clear the fence flag, on disk first when setting."""
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
        """Record the role for logs and the role gauge."""
        if role != self._role:
            log.info("role", role=role, previous=self._role)
        self._role = role
        self.m.set_role(role)

    def mysqld_alive(self) -> bool | None:
        """Whether the supervised mysqld process runs, None when not supervised."""
        return self.sup.alive

    # ------------------------------------------------------------------ /primary

    async def primary_check(self) -> tuple[int, dict[str, str]]:
        """HAProxy's check. Answers from memory only, never awaits SQL: the fence flag,
        then the refresher's sample of @@super_read_only (fresh within
        DBGUARD_PRIMARY_STALE_S). A hung mysqld fails it by staleness, a fence by the flag."""
        t0 = time.perf_counter()
        try:
            return self._primary_answer()
        finally:
            self.m.primary_check.observe(time.perf_counter() - t0)

    def _primary_answer(self) -> tuple[int, dict[str, str]]:
        """The /primary decision, fence flag first, then startup, wake guard and sample age."""
        if self.fenced:
            return 503, {"role": "fenced"}
        if not self.startup_done.is_set() or self.rebuilding:
            return 503, {"role": "unknown"}
        now = time.monotonic()
        if now - self.last_tick > self.s.wake_gap_s:
            # Woken from a freeze. Start the guard and say "unknown" until it has run.
            log.warning("wake_gap", gap_s=round(now - self.last_tick, 3), where="primary")
            self.last_tick = now
            self._trigger_guard("wake")
        if self._guard_task is not None and not self._guard_task.done():
            return 503, {"role": "unknown"}
        sample = self._primary_sample
        if sample is None or now - sample[0] > self.s.primary_stale_s:
            self._set_role("unknown")
            return 503, {"role": "unknown"}
        if sample[1] is False:
            self._set_role("primary")
            return 200, {"role": "primary"}
        self._set_role("replica")
        return 503, {"role": "replica"}

    # ------------------------------------------------------------------ /primary sampler

    def invalidate_primary_sample(self) -> None:
        """Forget the sample so /primary answers unknown until a new one."""
        self._primary_sample = None

    async def refresh_primary_sample(self) -> bool:
        """One sample on the refresher's own connection. Callers that just changed
        super_read_only call this so HAProxy's next check sees the change."""
        async with self._sample_lock:
            try:
                if self._sample_session is None or self._sample_session.closed:
                    self._sample_session = await self.db.connect(self.s.primary_sample_timeout_s)
                rows = await self._sample_session.query(
                    "SELECT @@GLOBAL.super_read_only AS sro, @@GLOBAL.read_only AS ro",
                    timeout=self.s.primary_sample_timeout_s)
                self._primary_sample = (time.monotonic(), _b(rows[0]["sro"]),
                                        _b(rows[0]["ro"]))
                return True
            except DbError:
                if self._sample_session is not None:
                    self._sample_session.close()
                self._sample_session = None
                return False

    async def _primary_refresher(self) -> None:
        """Sample super_read_only every ``primary_sample_interval_s`` for /primary."""
        while not self._stopping:
            await self.refresh_primary_sample()
            await asyncio.sleep(self.s.primary_sample_interval_s)

    # ------------------------------------------------------------------ /status

    async def status(self) -> dict[str, Any]:
        """GET /status, every key of docs/INTERFACES.md, null when not collected in time.

        One deadline covers the whole handler, and the source TCP probe runs beside the SQL."""
        out: dict[str, Any] = {
            "node": self.s.node, "rs": self.s.rs, "ts": time.time(),
            "mysqld_alive": None, "mysqld_responsive": False, "mysqld_pid": self.sup.pid,
            "mysqld_restart_hold_s": self.sup.hold_remaining_s,
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
            "wake_guard": {"enabled": self.wake_guard_on},
        }

        t0 = time.monotonic()
        deadline = t0 + self.s.status_deadline_s
        probe: list[asyncio.Task] = []

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
                src_host = out["replica"]["source_host"]
                if src_host:
                    # Runs beside the remaining SQL so a DROPped source costs 0.5 s once.
                    probe.append(asyncio.create_task(
                        self.reachable(src_host, self.s.source_port)))
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

        # One deadline for the whole handler (the manager's timeout is larger), so a slow
        # or hung mysqld degrades fields to null instead of making /status late.
        sql_budget = max(0.05, deadline - time.monotonic() - 0.1)
        try:
            await asyncio.wait_for(self._sql(collect), sql_budget)
        except DbError as e:
            out["mysqld_responsive"] = False
            out["error"] = str(e)
        except TimeoutError:
            out["error"] = f"status deadline {self.s.status_deadline_s}s reached"
            if out["gtid_executed"] is None:
                out["mysqld_responsive"] = False
        if probe:
            try:
                out["source_reachable"] = await asyncio.wait_for(
                    probe[0], max(0.01, deadline - time.monotonic()))
            except TimeoutError:
                out["source_reachable"] = None
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
        """POST /fence, the fence sequence of docs/DESIGN.md section 4, in its order.

        The flag goes first because the SQL may never return. A frozen mysqld executes
        nothing, and a primary cut off from its replicas has sessions waiting for an ack
        that the SET queues behind, so a partitioned primary is fenced by the kill path."""
        async with self._fence_lock:
            t0 = time.monotonic()
            # 1. The flag file. /primary answers 503 from here on, whatever the SQL does.
            self._set_fenced(True)
            # 2. Close our own links, then super_read_only=1 within the fence deadline.
            sess, error = await self._fence_set_read_only(t0 + self.s.fence_deadline_s)
            killed, gtid = 0, None
            if sess is not None:
                # 3. The SET returned. Kill client threads, read the final gtid_executed.
                method = "sql"
                killed, gtid = await self._fence_after_set(sess)
            else:
                # 4. It did not. SIGKILL mysqld and hold its restart.
                method = "kill"
                failed = await self._fence_kill_mysqld(reason, error, t0)
                if failed is not None:
                    return failed
            dur = round((time.monotonic() - t0) * 1000, 2)
            self.m.fences.labels(method=method).inc()
            log.info("fence", reason=reason, method=method, duration_ms=dur,
                     killed_threads=killed, gtid_executed=gtid, error=error)
            return 200, {"fenced": True, "method": method, "gtid_executed": gtid,
                         "duration_ms": dur, "killed_threads": killed}

    async def _fence_set_read_only(self, deadline: float) -> tuple[Session | None, str | None]:
        """Fence step 2. Returns the session that ran the SET, or None and the error."""
        # The agent's own pooled and heartbeat links are closed, not killed.
        self.db_drop_idle()
        if self._hb_session is not None:
            self._hb_session.close()
        sess: Session | None = None
        try:
            sess = await self.db.connect(timeout=max(0.05, deadline - time.monotonic()))
            await sess.execute("SET GLOBAL super_read_only=1",
                               timeout=max(0.05, deadline - time.monotonic()), log_sql=True)
            return sess, None
        except DbError as e:
            if sess is not None:
                sess.close()
            return None, str(e)

    async def _fence_after_set(self, sess: Session) -> tuple[int, str | None]:
        """Fence step 3. Returns (killed client threads, final gtid_executed), closing ``sess``."""
        killed, gtid = 0, None
        await self.refresh_primary_sample()
        try:
            killed = await self._kill_clients(sess)
            rows = await sess.query("SELECT @@GLOBAL.gtid_executed AS g",
                                    timeout=self.s.sql_timeout_s)
            gtid = _gtid(rows[0]["g"])
        except DbError as e:
            log.warning("fence_post_set_error", error=str(e))
        finally:
            sess.close()
        return killed, gtid

    async def _fence_kill_mysqld(self, reason: str, error: str | None,
                                 t0: float) -> tuple[int, dict[str, Any]] | None:
        """Fence step 4. SIGKILL mysqld with a restart hold. Returns the 500 answer when
        there is nothing to kill because the agent does not own mysqld, else None."""
        # Hold the restart so the dead primary does not come back and serve its
        # unacked binlog tail to replicas that are not repointed yet (review #4).
        self.sup.hold(self.s.restart_hold_s)
        self.invalidate_primary_sample()
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
        return None

    async def _kill_clients(self, sess: Session) -> int:
        """KILL every client thread except replication and system accounts."""
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
        """Close the pooled idle connections, which a fence or restart has made stale."""
        drop = getattr(self.db, "drop_idle", None)
        if drop:
            drop()

    async def unfence(self) -> dict[str, Any]:
        """POST /unfence. Clears the flag only, read_only is unchanged."""
        async with self._fence_lock:
            self._set_fenced(False)
        return {"fenced": False}

    # ------------------------------------------------------------------ role changes

    async def _end_restart_hold(self, why: str) -> None:
        """A role change needs mysqld. End a kill-fence hold and wait for it to be up."""
        if not self.sup.held:
            return
        self._step = "wait mysqld restart after hold"
        gen = self.sup.generation
        self.sup.release_hold()
        log.info("mysqld_restart_hold_released", by=why)
        if not await self.sup.wait_generation(gen, self.s.restart_wait_s):
            raise AgentError("mysqld did not restart after the hold", status=503)
        if not await self.wait_responsive(self.s.restart_wait_s):
            raise AgentError("mysqld not responsive after the hold", status=503)

    async def _deadlined(self, op: str, fn: Callable[[], Awaitable[Any]]) -> Any:
        """One overall deadline for a whole role-change request (the manager waits 35 s).
        On expiry the node stays in whatever state it reached and the caller gets 504."""
        self._step = "wait role lock"
        t0 = time.monotonic()
        try:
            async with asyncio.timeout(self.s.role_change_deadline_s):
                return await fn()
        except TimeoutError as e:
            dur = round((time.monotonic() - t0) * 1000, 2)
            log.error("role_change_deadline", op=op, step=self._step, duration_ms=dur,
                      deadline_s=self.s.role_change_deadline_s, fenced=self.fenced)
            raise AgentError("deadline", status=504, step=self._step) from e

    async def promote(self) -> dict[str, Any]:
        """POST /promote under the role-change deadline."""
        return await self._deadlined("promote", self._promote)

    async def _promote(self) -> dict[str, Any]:
        """Replica to primary. Semi-sync source on before writable, and writable last."""
        async with self._role_lock:
            t0 = time.monotonic()
            await self._end_restart_hold("promote")
            async with self.db.session(self.s.sql_timeout_s) as s:
                await self._role_sql(s, "STOP REPLICA")
                await self._role_sql(s, "RESET REPLICA ALL")
                # Semi-sync source goes on before the node becomes writable, so no write
                # is ever acknowledged without a replica ack.
                await self._role_sql(s, "SET GLOBAL rpl_semi_sync_replica_enabled=0")
                await self._role_sql(s, "SET GLOBAL rpl_semi_sync_source_enabled="
                                     + ("1" if self.semisync else "0"))
                # Writable is the LAST statement, in one SET. A promote cut short by the
                # deadline leaves the node read-only, or writable with semi-sync source
                # already on, and the fence flag (cleared below) still set.
                await self._role_sql(s, "SET GLOBAL super_read_only=0, read_only=0")
                self._step = "clear fence flag"
                async with self._fence_lock:
                    self._set_fenced(False)
                self._step = "refresh /primary sample"
                await self.refresh_primary_sample()
                self._step = "SELECT @@GLOBAL.gtid_executed"
                rows = await s.query("SELECT @@GLOBAL.gtid_executed AS g",
                                     timeout=self.s.role_change_timeout_s)
            self._set_role("primary")
            self.m.promotions.inc()
            dur = round((time.monotonic() - t0) * 1000, 2)
            gtid = _gtid(rows[0]["g"])
            log.info("promote", duration_ms=dur, gtid_executed=gtid, semisync=self.semisync)
            return {"gtid_executed": gtid, "duration_ms": dur}

    async def repoint(self, source: str) -> dict[str, Any]:
        """POST /repoint under the role-change deadline."""
        if not source or source == self.s.node:
            raise AgentError(f"bad source {source!r}", status=400)
        return await self._deadlined("repoint", lambda: self._repoint(source))

    async def _repoint(self, source: str) -> dict[str, Any]:
        """Become a read-only semi-sync replica of ``source`` with auto-positioning."""
        async with self._role_lock:
            t0 = time.monotonic()
            await self._end_restart_hold("repoint")
            async with self.db.session(self.s.sql_timeout_s) as s:
                await self._role_sql(s, "STOP REPLICA")
                await self._role_sql(s, "SET GLOBAL super_read_only=1")
                self._step = "refresh /primary sample"
                await self.refresh_primary_sample()
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

    def guard_toggles(self) -> dict[str, bool]:
        """The runtime switches of the self-fence lease and the wake guard."""
        return {"self_fence": self.self_fence_on, "wake_guard": self.wake_guard_on}

    async def configure(self, semisync: bool | None = None, reason: str = "api", *,
                        self_fence: bool | None = None, wake_guard: bool | None = None
                        ) -> dict[str, Any]:
        """Re-assert semi-sync for the current role, and flip the runtime guard toggles.
        A body with only toggles does not touch mysqld (review #12, the orchestrator
        baseline turns both guards off without recreating containers)."""
        if self_fence is not None:
            self.self_fence_on = bool(self_fence)
            self._sf_reset()
        if wake_guard is not None:
            self.wake_guard_on = bool(wake_guard)
        if self_fence is not None or wake_guard is not None:
            log.info("configure_guards", reason=reason, **self.guard_toggles())
            if semisync is None:
                return {"ok": True, "semisync": self.semisync, **self.guard_toggles()}
        if semisync is not None:
            self.semisync = bool(semisync)
        return await self._deadlined("configure", lambda: self._configure(reason))

    async def _configure(self, reason: str) -> dict[str, Any]:
        """Set the semi-sync sides for the current role, never releasing waiting sessions."""
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
                    "source_enabled": want_src, "replica_enabled": want_rep,
                    **self.guard_toggles()}

    async def rebuild(self, donor: str) -> dict[str, Any]:
        """POST /rebuild, replace this node's data with a clone of ``donor``.

        Counts the phantoms first (own gtid_executed minus the donor's), then clones.
        The node ends read-only and unreplicated, and the manager repoints it."""
        if not donor or donor == self.s.node:
            raise AgentError(f"bad donor {donor!r}", status=400)
        async with self._role_lock:
            t0 = time.monotonic()
            await self._end_restart_hold("rebuild")
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
        """Wait for mysqld to restart after the clone, then leave it read-only, unreplicated."""
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
        """POST /kill-mysqld test hook. SIGKILL, and the supervisor restarts at once."""
        pid = self.sup.kill(signal.SIGKILL)
        if pid is None:
            raise AgentError("no supervised mysqld to kill", status=409)
        self.db_drop_idle()
        return {"ok": True, "pid": pid}

    def hang_mysqld(self, seconds: float) -> dict[str, Any]:
        """POST /hang-mysqld test hook. SIGSTOP now, SIGCONT after ``seconds``."""
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
        """True if mysqld answers SELECT 1."""
        try:
            await self._sql(lambda s: s.query("SELECT 1 AS one"))
            return True
        except DbError:
            return False

    async def wait_responsive(self, timeout: float) -> bool:
        """Poll every 0.5 s until mysqld answers or ``timeout`` passes."""
        end = time.monotonic() + timeout
        while time.monotonic() < end and not self._stopping:
            if await self.responsive():
                return True
            await asyncio.sleep(0.5)
        return False

    # ------------------------------------------------------------------ wake guard

    async def wake_guard(self, reason: str) -> str:
        """Returns the decision: fence | ok | fence_file | leave | fence_failed | disabled."""
        if not self.wake_guard_on:
            log.info("wake_guard", reason=reason, decision="disabled")
            self.m.wake_guard.labels(reason=reason, decision="disabled").inc()
            return "disabled"
        url = f"{self.s.manager_url.rstrip('/')}/v1/sets/{self.s.rs}/primary" \
            if self.s.manager_url else None
        answer: dict[str, Any] | None = None
        err: str | None = None
        unknown = False
        if url:
            try:
                answer = await self.manager_get(url, self.s.manager_timeout_s)
            except ManagerUnknown as e:
                unknown, err = True, str(e)
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
        if answer is not None and answer.get("primary") == "unknown":
            unknown = True
        if unknown:
            # The manager is up but has no opinion yet (still discovering). Fencing a
            # healthy primary on that would be wrong, so leave state alone (review #16).
            primary = (answer or {}).get("primary")
            decision = "leave"
        elif answer is not None:
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
                 manager_unknown=unknown,
                 manager_state=(answer or {}).get("state"), writable=writable,
                 fence_file=fence_file, error=err)
        self.m.wake_guard.labels(reason=reason, decision=decision).inc()
        if decision in ("fence", "fence_file"):
            code, _ = await self.fence(reason=f"wake_guard:{reason}")
            if code != 200:
                return "fence_failed"
        return decision

    def _trigger_guard(self, reason: str) -> asyncio.Task:
        """Start the wake guard unless it is already running."""
        if self._guard_task is None or self._guard_task.done():
            self._guard_task = asyncio.create_task(self.wake_guard(reason), name="wake-guard")
        return self._guard_task

    def guard_pending(self) -> bool:
        """A monotonic gap nobody has handled yet, or a wake guard still running."""
        if time.monotonic() - self.last_tick > self.s.wake_gap_s:
            return True
        return self._guard_task is not None and not self._guard_task.done()

    async def ensure_awake(self) -> None:
        """Run the wake guard first if a freeze went unnoticed, and wait for it (2.5 s at most).

        The heartbeat writer calls it so a woken primary never writes before the guard ran."""
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
        """Tick every 0.5 s. A gap over ``wake_gap_s`` means the container was frozen."""
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
        """How long the in-flight heartbeat write has waited, 0.0 when none is."""
        since = self._hb_inflight_since
        return round(time.monotonic() - since, 3) if since is not None else 0.0

    async def _heartbeat(self) -> None:
        """Write the heartbeat row every 500 ms while this node is an unfenced primary."""
        await self.startup_done.wait()
        while not self._stopping:
            await asyncio.sleep(self.s.heartbeat_interval_s)
            self.m.heartbeat_stalled.set(0)
            # After a freeze the wake guard runs before any heartbeat write, or the woken
            # node would binlog an unacked heartbeat GTID (review #15).
            await self.ensure_awake()
            if self.fenced or self.rebuilding or self.guard_pending():
                continue
            try:
                if self._hb_session is None or self._hb_session.closed:
                    self._hb_session = await self.db.connect(self.s.sql_timeout_s)
                sess = self._hb_session
                rows = await sess.query("SELECT @@GLOBAL.super_read_only AS sro",
                                        timeout=self.s.sql_timeout_s)
                if (_b(rows[0]["sro"]) is not False or self.fenced or self.rebuilding
                        or self.guard_pending()):
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
        """The lease runs only with semi-sync on, a manager URL and a non-zero timeout."""
        return bool(self.self_fence_on and self.semisync and self.s.self_fence_after_s > 0
                    and self.s.manager_url)

    def manager_unreachable_s(self) -> float:
        """How long the manager has been continuously unreachable."""
        since = self._mgr_unreachable_since
        return round(time.monotonic() - since, 3) if since is not None else 0.0

    def self_fence_state(self) -> dict[str, Any]:
        """The ``self_fence`` object of /status."""
        return {"enabled": self.self_fence_enabled(),
                "manager_unreachable_s": self.manager_unreachable_s(),
                "semisync_clients": self._sf_clients}

    def _sf_reset(self) -> None:
        """Restart the lease's count."""
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
        except ManagerUnknown:
            reachable = True  # it answered, it is just still discovering
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
        """Run one lease check every ``self_fence_interval_s``."""
        await self.startup_done.wait()
        while not self._stopping:
            await asyncio.sleep(self.s.self_fence_interval_s)
            try:
                await self.self_fence_tick()
            except Exception:
                log.exception("self_fence_tick_failed")

    # ------------------------------------------------------------------ lifecycle

    async def _startup(self) -> None:
        """Wait for mysqld, run the startup guard, re-assert semi-sync, open /primary."""
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
        """Re-apply semi-sync for the current role, logging a failure."""
        try:
            await self.configure(None, reason=reason)
        except (DbError, AgentError) as e:
            log.warning("configure_failed", reason=reason, error=str(e))

    def on_mysqld_restart(self) -> None:
        """Supervisor callback: a new mysqld was spawned (not the first one)."""
        self.m.mysqld_restarts.inc()
        self.db_drop_idle()
        self.invalidate_primary_sample()

        async def after() -> None:
            if await self.wait_responsive(self.s.restart_wait_s):
                await self._reassert("mysqld_restart")

        self._tasks.append(asyncio.create_task(after(), name="after-restart"))

    async def start(self) -> None:
        """Start mysqld and every background loop."""
        await self.sup.start()
        self._tasks += [asyncio.create_task(self._startup(), name="startup"),
                        asyncio.create_task(self._ticker(), name="ticker"),
                        asyncio.create_task(self._heartbeat(), name="heartbeat"),
                        asyncio.create_task(self._self_fence_loop(), name="self-fence"),
                        asyncio.create_task(self._primary_refresher(), name="primary-sampler")]

    async def stop(self) -> None:
        """Stop the loops, then mysqld (SIGTERM, waited for)."""
        self._stopping = True
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        if self._hb_session is not None:
            self._hb_session.close()
        if self._sample_session is not None:
            self._sample_session.close()
        await self.sup.stop()
        await self.db.close()
