"""`dbgctl osc`: online schema change (docs/OSC.md). Unlike the rest of dbgctl it talks to
MySQL directly, to the primary given by --host/--port. The manager is only used, when
--manager and --rs are given, to read replica lag for throttling."""

from __future__ import annotations

import json
import signal
import sys
from datetime import datetime

import click


def conn_options(f):
    """Add the connection options every osc subcommand takes."""
    for opt in reversed([
        click.option("--host", default="127.0.0.1", show_default=True,
                     help="the PRIMARY (dbgctl status shows which node it is)"),
        click.option("--port", default=3306, show_default=True, type=int),
        click.option("--user", default="dbguard", show_default=True),
        click.option("--password", default="dbguard", show_default=True,
                     envvar="DBGUARD_OSC_PASSWORD"),
        click.option("--tls/--no-tls", default=True, show_default=True,
                     help="TLS without certificate check, as every DBGuard client"),
    ]):
        f = opt(f)
    return f


def connector(*a, **kw):
    """Lazy: PyMySQL is only needed by `dbgctl osc`, the rest of dbgctl is stdlib only."""
    from dbguard.osc.db import connector as c
    return c(*a, **kw)


def _err(msg: str) -> None:
    click.echo(msg, err=True)


def _interrupts() -> None:
    """SIGINT and SIGTERM both raise KeyboardInterrupt, so the runner cleans up. SIGINT is
    set explicitly because a job started with `&` from a non-interactive shell inherits
    SIGINT as ignored, and Python then never installs its own handler (found when a
    backgrounded bench ignored `kill -INT`)."""
    def h(signum, frame):  # noqa: ARG001
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, h)


@click.group()
def osc() -> None:
    """Online schema change: shadow table, triggers, chunked copy, checksum, atomic rename."""


@osc.command()
@conn_options
@click.option("--db", required=True)
@click.option("--table", required=True)
@click.option("--alter", "alter_text", required=True,
              help='clauses only, e.g. "ADD COLUMN note VARCHAR(32) NULL"')
@click.option("--chunk-size", default=1000, show_default=True, help="starting rows per chunk")
@click.option("--chunk-time", default=0.1, show_default=True,
              help="target seconds per chunk, the size adapts to it")
@click.option("--fixed-chunk", is_flag=True, help="do not adapt the chunk size")
@click.option("--max-lag-s", default=2.0, show_default=True)
@click.option("--max-load-threads", default=20, show_default=True,
              help="pause while Threads_running is above this")
@click.option("--manager", "manager_url", default=None,
              help="manager URL, with --rs, to pause on replica lag (else lag is not checked)")
@click.option("--rs", default=None, help="replica set name for the lag check")
@click.option("--dry-run", is_flag=True,
              help="preflight, apply the ALTER to an empty shadow, drop it, change nothing")
@click.option("--allow-instant/--no-instant", default=True, show_default=True,
              help="use ALGORITHM=INSTANT when the ALTER qualifies")
@click.option("--drop-old", is_flag=True, help="drop the original table after the swap")
@click.option("--allow-type-change", is_flag=True,
              help="allow the ALTER to change types of existing columns")
@click.option("--no-progress-table", is_flag=True, help="do not write dbguard.osc_progress")
@click.option("--json", "as_json", is_flag=True, help="print the result as JSON on stdout")
def run(host, port, user, password, tls, db, table, alter_text, chunk_size, chunk_time,
        fixed_chunk, max_lag_s, max_load_threads, manager_url, rs, dry_run, allow_instant,
        drop_old, allow_type_change, no_progress_table, as_json) -> None:
    """Run one online schema change on --host (the primary)."""
    from dbguard.osc.runner import Options, OscError, Runner, manager_lag_fn

    if bool(manager_url) != bool(rs):
        raise click.UsageError("--manager and --rs go together")
    opts = Options(db=db, table=table, alter=alter_text, chunk_size=chunk_size,
                   target_chunk_s=chunk_time, adaptive=not fixed_chunk, max_lag_s=max_lag_s,
                   max_load_threads=max_load_threads, dry_run=dry_run,
                   allow_instant=allow_instant, drop_old=drop_old,
                   allow_type_change=allow_type_change, track_progress=not no_progress_table)
    lag_fn = manager_lag_fn(manager_url, rs) if manager_url else None
    if lag_fn is None:
        _err("osc: no --manager/--rs, replica lag is not checked")
    try:
        runner = Runner(opts, connector(host, port, user, password, tls=tls), lag_fn=lag_fn,
                        log=lambda m: _err(f"[{datetime.now():%H:%M:%S}] {m}"))
    except ValueError as e:
        raise click.UsageError(str(e)) from e
    _interrupts()
    code = 0
    try:
        runner.run()
    except KeyboardInterrupt:
        code = 130
    except OscError:
        code = 1
    except Exception as e:  # noqa: BLE001
        _err(f"osc: {type(e).__name__}: {e}")
        code = 1
    res = runner.result
    if as_json:
        click.echo(json.dumps(res, default=str))
    elif res.get("ok"):
        if res.get("dry_run"):
            click.echo(f"dry run ok, would use {res.get('method')}")
        elif res.get("method") == "instant":
            click.echo(f"done: ALGORITHM=INSTANT in {res.get('instant_s', 0) * 1000:.1f} ms")
        elif res.get("method") == "copy":
            click.echo(f"done: copied {res.get('rows_copied')} rows in {res.get('chunks')} "
                       f"chunks, ~{res.get('copy_mb_s')} MB/s, checksum "
                       f"{res.get('checksum_chunks')} chunks match, swap "
                       f"{(res.get('swap_s') or 0) * 1000:.1f} ms, total {res.get('total_s')} s")
    sys.exit(code)


@osc.command()
@conn_options
@click.option("--db", default=None, help="only this schema")
@click.option("--json", "as_json", is_flag=True)
def status(host, port, user, password, tls, db, as_json) -> None:
    """Progress of running and recent changes, from dbguard.osc_progress."""
    from dbguard.osc.runner import read_progress

    ex = connector(host, port, user, password, tls=tls)()
    try:
        rows = read_progress(ex, db)
    finally:
        ex.close()
    if as_json:
        click.echo(json.dumps(rows, default=str))
        return
    if not rows:
        click.echo("no online schema change recorded")
        return
    for r in rows:
        age = float(r["age_s"] or 0)
        stale = (r["phase"] not in ("done", "failed") and age > 10)
        est = max(int(r["rows_est"] or 0), 1)
        pct = min(100.0, 100.0 * int(r["rows_copied"] or 0) / est)
        stale_note = (" (STALE, no update for %.0fs, process gone? run osc cleanup)" % age
                      if stale else "")
        click.echo(f"{r['db']}.{r['tbl']}: {r['phase']}{stale_note}"
                   f" method={r['method'] or '-'} copied {r['rows_copied']}/~{r['rows_est']} "
                   f"(~{pct:.0f}%) chunks={r['chunks']} chunk_size={r['chunk_size']} "
                   f"throttled={float(r['throttled_s'] or 0):.1f}s started={r['started']} "
                   f"owner={r['owner']}" + (f" note={r['note']}" if r["note"] else ""))


@osc.command()
@conn_options
@click.option("--db", required=True)
@click.option("--table", required=True)
@click.option("--old", "drop_old", is_flag=True, help="also drop __osc_old_<table>")
def cleanup(host, port, user, password, tls, db, table, drop_old) -> None:
    """Drop leftover osc triggers and the shadow table of a failed or interrupted run."""
    from dbguard.osc.runner import cleanup as do_cleanup

    ex = connector(host, port, user, password, tls=tls)()
    try:
        do_cleanup(ex, db, table, drop_old=drop_old, log=click.echo)
    finally:
        ex.close()


@osc.command()
@conn_options
@click.option("--rows", default=500_000, show_default=True)
@click.option("--threads", default=8, show_default=True, help="foreground writer threads")
@click.option("--phase-s", default=15.0, show_default=True,
              help="seconds of foreground load before and after each change")
@click.option("--modes", default="osc,osc-throttled,inplace,instant,osc-instant",
              show_default=True,
              help="comma list of osc, osc-throttled, inplace, instant, osc-instant")
@click.option("--repeat", default=1, show_default=True, help="run every mode this many times")
@click.option("--throttle-threads", default=4, show_default=True,
              help="--max-load-threads for the osc-throttled mode")
@click.option("--alter", "alter_text",
              default="ADD COLUMN note VARCHAR(32) NULL, ADD INDEX idx_ts (ts)",
              show_default=True, help="alter for the osc and inplace modes")
@click.option("--instant-alter", default="ADD COLUMN note VARCHAR(32) NULL", show_default=True)
@click.option("--out", default=None, help="write the JSON results here")
def bench(host, port, user, password, tls, rows, threads, phase_s, modes, repeat,
          throttle_threads, alter_text, instant_alter, out) -> None:
    """Foreground QPS and p99 before, during and after each way of running an ALTER.
    Loads its own table in schema osc_bench. For a throwaway server only."""
    from dbguard.osc.bench import run_bench

    _interrupts()
    res = run_bench(connector(host, port, user, password, tls=tls), rows=rows,
                    threads=threads, phase_s=phase_s,
                    modes=[m.strip() for m in modes.split(",") if m.strip()],
                    alter=alter_text, instant_alter=instant_alter, log=_err, repeat=repeat,
                    throttle_threads=throttle_threads)
    text = json.dumps(res, indent=2, default=str)
    if out:
        with open(out, "w") as f:
            f.write(text + "\n")
    click.echo(text)
