"""dbgctl, the operator CLI. Talks only to the Manager HTTP API (docs/INTERFACES.md)."""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime

import click

from dbguard.cli.client import ApiError, ManagerClient
from dbguard.cli.osc import osc


def _fmt_ts(ts: float | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


def _fmt_s(v) -> str:
    return "-" if v is None else f"{float(v):.2f}s"


def event_line(ev: dict | None) -> str:
    """One-line human summary of an Event row."""
    if not ev:
        return "-"
    parts = [_fmt_ts(ev.get("ts")), ev.get("rs") or "?", ev.get("type") or "?"]
    old, new = ev.get("old_primary"), ev.get("new_primary")
    if old or new:
        parts.append(f"{old or '-'} -> {new or '-'}")
    if ev.get("trigger"):
        parts.append(f"trigger={ev['trigger']}")
    fence = ((ev.get("steps") or {}).get("fence") or {}).get("outcome")
    if fence:
        parts.append(f"fence={fence}")
    if ev.get("total_s") is not None:
        parts.append(f"total={_fmt_s(ev['total_s'])}")
    rj = ev.get("rejoin")
    if rj:
        parts.append(f"rejoin={rj.get('branch')} phantom={rj.get('phantom_gtids')} "
                     f"in {_fmt_s(rj.get('duration_s'))}")
    if ev.get("note"):
        parts.append(f"({ev['note']})")
    return " ".join(str(p) for p in parts)


def status_rows(status: dict) -> list[list[str]]:
    rows = []
    for rs, st in sorted((status.get("sets") or {}).items()):
        replicas = []
        for name, n in sorted((st.get("nodes") or {}).items()):
            if name == st.get("primary"):
                continue
            role = n.get("role") or "?"
            if role == "replica":
                lag = n.get("lag_s")
                lag_s = "?" if lag is None else f"{lag:.1f}s"
                replicas.append(f"{name}(lag {lag_s}, ss {n.get('semisync') or '?'})")
            else:
                replicas.append(f"{name}({role})")
        state = st.get("state") or "?"
        if st.get("halt_reason"):
            state += f" [{st['halt_reason']}]"
        rows.append([rs, state, st.get("primary") or "-", ", ".join(replicas) or "-",
                     event_line(st.get("last_event"))])
    return rows


def render_table(header: list[str], rows: list[list[str]]) -> str:
    widths = [max(len(str(r[i])) for r in [header] + rows) for i in range(len(header))]
    out = ["  ".join(str(h).ljust(w) for h, w in zip(header, widths))]
    out.append("  ".join("-" * w for w in widths))
    for r in rows:
        out.append("  ".join(str(c).ljust(w) for c, w in zip(r, widths)).rstrip())
    return "\n".join(out)


def _die(e: ApiError) -> None:
    click.echo(f"dbgctl: {e}", err=True)
    sys.exit(2 if e.status is None else 1)


@click.group()
@click.option("--manager", envvar="DBGUARD_MANAGER_URL", default="http://127.0.0.1:19090",
              show_default=True, help="Manager base URL (env DBGUARD_MANAGER_URL)")
@click.option("--timeout", default=5.0, show_default=True, help="HTTP timeout in seconds")
@click.pass_context
def cli(ctx: click.Context, manager: str, timeout: float) -> None:
    """Operate a DBGuard fleet through the manager API."""
    ctx.obj = ManagerClient(manager, timeout=timeout)


@cli.command()
@click.option("--json", "as_json", is_flag=True, help="machine-readable output")
@click.pass_obj
def status(mc: ManagerClient, as_json: bool) -> None:
    """Every set with state, primary, replicas (lag, semi-sync) and the last event."""
    try:
        st = mc.status()
    except ApiError as e:
        _die(e)
    if as_json:
        click.echo(json.dumps(st, indent=2, sort_keys=True))
        return
    click.echo(f"mode: {st.get('mode', '?')}")
    click.echo(render_table(["SET", "STATE", "PRIMARY", "REPLICAS", "LAST EVENT"], status_rows(st)))


@cli.command()
@click.argument("rs")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def doctor(mc: ManagerClient, rs: str, as_json: bool) -> None:
    """Explain a set's state in sentences, with the GTID gaps per node."""
    try:
        d = mc.doctor(rs)
    except ApiError as e:
        _die(e)
    if as_json:
        click.echo(json.dumps(d, indent=2, sort_keys=True))
        return
    for line in d.get("lines") or []:
        click.echo(line)
    if d.get("verdict"):
        click.echo(f"verdict: {d['verdict']}")
    gaps = d.get("gaps") or {}
    if gaps:
        click.echo("GTID gaps versus the primary (GTID_SUBTRACT(primary, node)):")
        for node, gap in sorted(gaps.items()):
            click.echo(f"  {node}: {gap or 'none'}")


@cli.command()
@click.argument("rs")
@click.option("--to", "to", default=None, help="target replica (default: manager chooses)")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def failover(mc: ManagerClient, rs: str, to: str | None, as_json: bool) -> None:
    """Planned switchover of RS (graceful, lossless)."""
    t0 = time.time()
    try:
        r = mc.failover(rs, to)
    except ApiError as e:
        _die(e)
    if as_json:
        click.echo(json.dumps(r, indent=2, sort_keys=True))
        return
    click.echo(f"switchover done in {time.time() - t0:.2f}s: {event_line(r.get('event'))}")


def _simple(name: str, doc: str):
    @click.argument("rs")
    @click.pass_obj
    def cmd(mc: ManagerClient, rs: str) -> None:
        try:
            r = getattr(mc, name)(rs)
        except ApiError as e:
            _die(e)
        click.echo(f"{rs}: state {r.get('state') if isinstance(r, dict) else r}")
    cmd.__doc__ = doc
    return cli.command(name=name)(cmd)


_simple("halt", "Stop all automatic action on RS (state HALTED).")
_simple("resume", "Resume automation on a HALTED RS.")


@cli.command()
@click.argument("rs")
@click.argument("node")
@click.pass_obj
def rejoin(mc: ManagerClient, rs: str, node: str) -> None:
    """Trigger rejoin of NODE into RS (repoint if subset, else rebuild by clone)."""
    try:
        r = mc.rejoin(rs, node)
    except ApiError as e:
        _die(e)
    ev = r.get("event") if isinstance(r, dict) else None
    click.echo(event_line(ev) if ev else json.dumps(r))


@cli.command()
@click.option("--rs", default=None)
@click.option("--since", default=None, help="unix ts, or relative like 10m / 2h / 30s")
@click.option("--follow", "-f", is_flag=True, help="keep polling for new events")
@click.option("--json", "as_json", is_flag=True, help="one JSON object per line")
@click.option("--interval", default=1.0, show_default=True)
@click.pass_obj
def events(mc: ManagerClient, rs: str | None, since: str | None, follow: bool, as_json: bool,
           interval: float) -> None:
    """Show the event log (newest last)."""
    since_ts = parse_since(since)
    last = since_ts
    seen: set[str] = set()
    while True:
        try:
            evs = mc.events(rs, last)
        except ApiError as e:
            if not follow:
                _die(e)
            click.echo(f"dbgctl: {e} (retrying)", err=True)
            evs = []
        for ev in evs:
            key = json.dumps(ev, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            click.echo(json.dumps(ev) if as_json else event_line(ev))
            if ev.get("ts") is not None:
                last = max(last or 0, float(ev["ts"]))
        if not follow:
            return
        sys.stdout.flush()
        time.sleep(interval)


def parse_since(s: str | None) -> float | None:
    if s is None:
        return None
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if s[-1:] in units and s[:-1].replace(".", "").isdigit():
        return time.time() - float(s[:-1]) * units[s[-1]]
    return float(s)


cli.add_command(osc)


def main() -> None:
    cli(prog_name="dbgctl")


if __name__ == "__main__":
    main()
