"""The chaos workload (docs/INTERFACES.md "Workload and checker").

K client threads each INSERT (client_id, seq, ts, run_id, payload) into chaos.writes with
autocommit, one row at a time, through HAProxy. A row goes into the ack log only after the
INSERT returned OK. On any error the client logs an error row for that exact seq, drops
the connection, backs off 50 ms, reconnects and continues with seq + 1.

Ack log lines (JSONL, flushed per line):
  {"client":c,"seq":s,"t_ok":<unix>,"latency_ms":f,"t_start":<unix>,"conn":n}
  {"client":c,"seq":s,"error":"...","t_err":<unix>,"t_start":<unix>,"conn":n}
t_start is the wall clock when the INSERT was sent. conn counts this client's connections
(1 for the first, +1 per reconnect). A failover is over at the first ok whose request
STARTED after the first error, because an ok that lands after an error may belong to a
write that committed on the old primary before the fault.
A seq whose INSERT was still in flight when the workload stopped is logged as an error row
with error "in-flight at shutdown", so the checker treats it as may-or-may-not-exist.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

from dbguard import mysqlx_sync

INSERT_SQL = ("INSERT INTO chaos.writes (client_id, seq, ts, run_id, payload) "
              "VALUES (%s, %s, NOW(6), %s, %s)")
BACKOFF_S = 0.05
PAYLOAD_BYTES = 256


def percentile(xs: list[float], q: float) -> float | None:
    """Nearest-rank percentile, q in [0, 100]."""
    if not xs:
        return None
    s = sorted(xs)
    return s[max(1, math.ceil(q / 100 * len(s))) - 1]


class AckLog:
    """Thread-safe JSONL writer. Each line is written and flushed while holding the lock."""

    def __init__(self, fh: TextIO | None):
        self._fh = fh
        self._lock = threading.Lock()

    def write(self, row: dict[str, Any]) -> None:
        if self._fh is None:
            return
        line = json.dumps(row, separators=(",", ":")) + "\n"
        with self._lock:
            self._fh.write(line)
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.flush()
                try:
                    os.fsync(self._fh.fileno())
                except (OSError, ValueError):
                    pass
                self._fh.close()
                self._fh = None


def read_ack_log(path: str | Path) -> tuple[list[dict], list[dict]]:
    """Parse an ack log into (ok rows, error rows). Torn or blank lines are skipped."""
    oks: list[dict] = []
    errs: list[dict] = []
    p = Path(path)
    if not p.exists():
        return oks, errs
    with p.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(r, dict) or "client" not in r or "seq" not in r:
                continue
            if "t_ok" in r:
                oks.append(r)
            elif "error" in r or "t_err" in r:
                errs.append(r)
    return oks, errs


@dataclass
class ClientStats:
    acked: int = 0
    errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    ok_ts: list[float] = field(default_factory=list)
    ok_start_ts: list[float] = field(default_factory=list)
    err_ts: list[float] = field(default_factory=list)
    in_flight_seq: int | None = None


@dataclass
class WorkloadOptions:
    host: str = "127.0.0.1"
    port: int = 13306
    clients: int = 8
    duration: float = 60.0
    run_id: str = ""
    user: str = "chaos"
    password: str = "chaos"
    connect_timeout: float = 2.0
    query_timeout: float | None = None
    tls: bool = True


class Client(threading.Thread):
    def __init__(self, cid: int, opts: WorkloadOptions, ack: AckLog, stop: threading.Event):
        super().__init__(name=f"client-{cid}", daemon=True)
        self.cid = cid
        self.opts = opts
        self.ack = ack
        self.stop_ev = stop
        self.stats = ClientStats()
        self.conn = None
        self.last_error: str | None = None
        self.conn_no = 0

    def _connect(self):
        """Connect with the whole handshake bounded by connect_timeout.

        PyMySQL applies connect_timeout to the TCP connect only and then reads the server
        greeting with read_timeout, which is None here so that a semi-sync stall can block a
        commit for as long as it lasts. During a failover HAProxy accepts the TCP connection
        while it has no server UP and may never send a greeting, so the client hung forever
        and wrote nothing after the kill. The handshake now uses connect_timeout as its read
        timeout, and the query timeout is restored once the session is up."""
        o = self.opts
        conn = mysqlx_sync.connect(o.host, o.port, o.user, o.password, timeout=o.connect_timeout,
                                   tls=o.tls, read_timeout=o.connect_timeout,
                                   write_timeout=o.connect_timeout)
        conn._read_timeout = o.query_timeout
        conn._write_timeout = o.query_timeout
        return conn

    def _drop(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    def run(self) -> None:
        seq = 0
        st = self.stats
        while not self.stop_ev.is_set():
            if self.conn is None:
                try:
                    self.conn = self._connect()
                    self.conn_no += 1
                except Exception as e:  # connection refused, reset, auth... keep trying
                    self.last_error = f"connect: {e}"
                    self.stop_ev.wait(BACKOFF_S)
                    continue
            seq += 1
            payload = os.urandom(PAYLOAD_BYTES)
            st.in_flight_seq = seq
            t_start = time.time()
            t0 = time.perf_counter()
            try:
                with self.conn.cursor() as cur:
                    cur.execute(INSERT_SQL, (self.cid, seq, self.opts.run_id, payload))
            except Exception as e:
                now = time.time()
                st.errors += 1
                st.err_ts.append(now)
                self.last_error = str(e)
                self.ack.write({"client": self.cid, "seq": seq, "error": str(e)[:300],
                                "t_err": now, "t_start": round(t_start, 6),
                                "conn": self.conn_no})
                st.in_flight_seq = None  # only after the row is in the log (see run())
                self._drop()
                self.stop_ev.wait(BACKOFF_S)
                continue
            lat = (time.perf_counter() - t0) * 1000.0
            now = time.time()
            st.acked += 1
            st.latencies_ms.append(lat)
            st.ok_ts.append(now)
            st.ok_start_ts.append(t_start)
            self.ack.write({"client": self.cid, "seq": seq, "t_ok": now,
                            "latency_ms": round(lat, 3), "t_start": round(t_start, 6),
                            "conn": self.conn_no})
            st.in_flight_seq = None  # only after the ack is in the log (see run())
        self._drop()


def summarize(clients: list[Client], opts: WorkloadOptions, t_start: float, t_end: float,
              in_flight: int) -> dict[str, Any]:
    lats: list[float] = []
    oks: list[float] = []
    ok_pairs: list[tuple[float, float]] = []   # (t_start, t_ok)
    errs: list[float] = []
    for c in clients:
        lats.extend(c.stats.latencies_ms)
        oks.extend(c.stats.ok_ts)
        ok_pairs.extend(zip(c.stats.ok_start_ts, c.stats.ok_ts))
        errs.extend(c.stats.err_ts)
    oks.sort()
    errs.sort()
    first_err = errs[0] if errs else None
    first_ok_after = None
    if first_err is not None:
        # only a write that was sent after the first error proves the new primary serves
        first_ok_after = min((t for ts, t in ok_pairs if ts >= first_err), default=None)
    elapsed = max(t_end - t_start, 1e-9)
    p50 = percentile(lats, 50)
    p99 = percentile(lats, 99)
    return {
        "run_id": opts.run_id,
        "host": opts.host, "port": opts.port, "clients": opts.clients,
        "duration_s": round(elapsed, 3),
        "acked": len(oks), "errors": len(errs), "in_flight_at_stop": in_flight,
        "first_error_ts": first_err, "first_ok_after_error_ts": first_ok_after,
        "latency_p50_ms": round(p50, 3) if p50 is not None else None,
        "latency_p99_ms": round(p99, 3) if p99 is not None else None,
        "latency_max_ms": round(max(lats), 3) if lats else None,
        "writes_per_s": round(len(oks) / elapsed, 1),
        "last_errors": sorted({c.last_error for c in clients if c.last_error})[:5],
    }


def run(opts: WorkloadOptions, ack_log: str | Path | None = None,
        stop: threading.Event | None = None, install_signals: bool = False) -> dict[str, Any]:
    if not opts.run_id:
        opts.run_id = f"wl-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    stop = stop or threading.Event()
    fh = None
    if ack_log:
        Path(ack_log).parent.mkdir(parents=True, exist_ok=True)
        fh = open(ack_log, "a", buffering=1)
    ack = AckLog(fh)

    if install_signals:
        def _on_signal(signum, _frame):
            stop.set()
        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)

    clients = [Client(i, opts, ack, stop) for i in range(1, opts.clients + 1)]
    t_start = time.time()
    for c in clients:
        c.start()
    stop.wait(opts.duration)
    stop.set()
    t_end = time.time()
    for c in clients:
        c.join(timeout=2.0)
    # A client still blocked in an INSERT (a stalled primary) has an unknown outcome.
    in_flight = 0
    for c in clients:
        seq = c.stats.in_flight_seq
        if seq is not None:
            in_flight += 1
            ack.write({"client": c.cid, "seq": seq, "error": "in-flight at shutdown",
                       "t_err": t_end})
    ack.close()
    return summarize(clients, opts, t_start, t_end, in_flight)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="workload", description=__doc__.split("\n")[0])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=13306)
    p.add_argument("--clients", type=int, default=8)
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--run-id", default="")
    p.add_argument("--ack-log", default=None, help="JSONL ack log (appended)")
    p.add_argument("--summary-file", default=None)
    p.add_argument("--user", default="chaos")
    p.add_argument("--password", default="chaos")
    p.add_argument("--connect-timeout", type=float, default=2.0)
    p.add_argument("--query-timeout", type=float, default=None,
                   help="read/write timeout per INSERT (default none: a stall blocks)")
    p.add_argument("--no-tls", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    a = build_parser().parse_args(argv)
    opts = WorkloadOptions(host=a.host, port=a.port, clients=a.clients, duration=a.duration,
                           run_id=a.run_id, user=a.user, password=a.password,
                           connect_timeout=a.connect_timeout, query_timeout=a.query_timeout,
                           tls=not a.no_tls)
    summary = run(opts, a.ack_log, install_signals=True)
    text = json.dumps(summary)
    if a.summary_file:
        Path(a.summary_file).parent.mkdir(parents=True, exist_ok=True)
        Path(a.summary_file).write_text(text + "\n")
    print(text, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
