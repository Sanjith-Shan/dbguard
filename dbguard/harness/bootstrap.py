"""bin/bootstrap: turn a freshly started, all-read-only fleet into replica sets with direct SQL.

For each set: nodes[0] becomes primary (writable, semi-sync source on), every other node
replicates from it with the INTERFACES.md options (semi-sync replica on). Idempotent: a
replica already replicating from the right source with both threads running is left alone,
and a primary that is already writable is only re-asserted.

Until the manager exists this is how the fleet is formed. It talks to the published host
ports (1331X/1332X) as root, and tells the replicas to use the compose service names.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

from dbguard import mysqlx_sync as m
from dbguard.config import FleetConfig, load_config

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "..", "deploy", "fleet.yaml")


def host_port(node: str) -> int:
    """mysql-a1 -> 13311, mysql-b4 -> 13324 (docs/INTERFACES.md host ports)."""
    letter, idx = node.rsplit("-", 1)[1][0], int(node.rsplit("-", 1)[1][1:])
    return 13300 + (10 if letter == "a" else 20) + idx


def log(msg: str, **kw: Any) -> None:
    print(json.dumps({"event": "bootstrap", "msg": msg, **kw}), file=sys.stderr, flush=True)


def wait_ready(nodes: list[str], user: str, password: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    pending = list(nodes)
    while pending:
        still = []
        for n in pending:
            try:
                c = m.connect("127.0.0.1", host_port(n), user, password, timeout=2)
                m.query(c, "SELECT 1")
                c.close()
            except Exception as e:
                still.append(n)
                last = str(e)
        pending = still
        if pending:
            if time.monotonic() > deadline:
                raise SystemExit(f"mysqld not ready after {timeout:.0f}s: {pending} ({last})")
            time.sleep(1)
    log("all mysqld ready", nodes=nodes)


def semisync_available(conn) -> bool:
    return "rpl_semi_sync_source_wait_point" in m.global_vars(
        conn, ["rpl_semi_sync_source_wait_point"])


def setup_primary(node: str, conn, semisync: bool) -> None:
    rs = m.replica_status(conn)
    if rs is not None:
        m.execute(conn, "STOP REPLICA")
        m.execute(conn, "RESET REPLICA ALL")
    m.execute(conn, "SET GLOBAL rpl_semi_sync_replica_enabled=0") if semisync_available(conn) \
        else None
    if semisync and semisync_available(conn):
        m.execute(conn, "SET GLOBAL rpl_semi_sync_source_enabled=1")
    elif semisync_available(conn):
        m.execute(conn, "SET GLOBAL rpl_semi_sync_source_enabled=0")
    m.execute(conn, "SET GLOBAL super_read_only=0")
    m.execute(conn, "SET GLOBAL read_only=0")
    log("primary ready", node=node, semisync=semisync)


def setup_replica(node: str, conn, source: str, cfg: FleetConfig, semisync: bool) -> None:
    has_semi = semisync_available(conn)
    m.execute(conn, "SET GLOBAL super_read_only=1")  # also sets read_only=1
    if has_semi:
        m.execute(conn, "SET GLOBAL rpl_semi_sync_source_enabled=0")
    rs = m.replica_status(conn)
    ok = (rs is not None and rs["source_host"] == source and rs["io_running"] == "Yes"
          and rs["sql_running"] == "Yes")
    want_semi = 1 if (semisync and has_semi) else 0
    cur_semi = m.global_vars(conn, ["rpl_semi_sync_replica_enabled"]).get(
        "rpl_semi_sync_replica_enabled")
    if ok and cur_semi == ("ON" if want_semi else "OFF"):
        log("replica already replicating", node=node, source=source)
        return
    if rs is not None:
        m.execute(conn, "STOP REPLICA")
    if has_semi:
        # takes effect for the IO thread at its next start
        m.execute(conn, f"SET GLOBAL rpl_semi_sync_replica_enabled={want_semi}")
    m.execute(conn, (
        "CHANGE REPLICATION SOURCE TO SOURCE_HOST=%s, SOURCE_PORT=%s, SOURCE_USER=%s, "
        "SOURCE_PASSWORD=%s, SOURCE_AUTO_POSITION=1, SOURCE_HEARTBEAT_PERIOD=0.5, "
        "SOURCE_CONNECT_RETRY=1, SOURCE_RETRY_COUNT=86400, SOURCE_SSL=1"),
        (source, cfg.mysql.port, cfg.mysql.repl_user, cfg.mysql.repl_password))
    m.execute(conn, "START REPLICA")
    log("replica started", node=node, source=source, semisync=bool(want_semi))


def topology(cfg: FleetConfig, user: str, password: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for rs, sc in cfg.sets.items():
        nodes: dict[str, Any] = {}
        for n in sc.nodes:
            try:
                c = m.connect("127.0.0.1", host_port(n), user, password, timeout=2)
            except Exception as e:
                nodes[n] = {"error": str(e)}
                continue
            v = m.global_vars(c, ["super_read_only", "read_only", "rpl_semi_sync_source_enabled",
                                  "rpl_semi_sync_replica_enabled"])
            s = m.global_status(c, ["Rpl_semi_sync_source_status", "Rpl_semi_sync_source_clients",
                                    "Rpl_semi_sync_replica_status"])
            r = m.replica_status(c)
            nodes[n] = {
                "super_read_only": v.get("super_read_only"),
                "semisync_source": v.get("rpl_semi_sync_source_enabled"),
                "semisync_replica": v.get("rpl_semi_sync_replica_enabled"),
                "source_status": s.get("Rpl_semi_sync_source_status"),
                "source_clients": s.get("Rpl_semi_sync_source_clients"),
                "replica_status": s.get("Rpl_semi_sync_replica_status"),
                "replica": None if r is None else {k: r[k] for k in (
                    "source_host", "io_running", "sql_running", "last_io_error")},
                "gtid_executed": m.gtid_executed(c),
            }
            c.close()
        out[rs] = nodes
    return out


def wait_semisync_clients(cfg: FleetConfig, user: str, password: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    for sc in cfg.sets.values():
        c = m.connect("127.0.0.1", host_port(sc.nodes[0]), user, password, timeout=2)
        want = str(len(sc.nodes) - 1)
        while True:
            got = m.global_status(c, ["Rpl_semi_sync_source_clients"]).get(
                "Rpl_semi_sync_source_clients")
            if got == want or time.monotonic() > deadline:
                break
            time.sleep(0.2)
        c.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bootstrap", description=__doc__.split("\n")[0])
    p.add_argument("--config", default=os.path.normpath(DEFAULT_CONFIG))
    p.add_argument("--naive", action="store_true",
                   help="skip semi-sync (also implied by DBGUARD_SEMISYNC=0)")
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="root")
    p.add_argument("--wait", type=float, default=300.0, help="seconds to wait for mysqld")
    p.add_argument("--sets", default="", help="comma separated subset of sets (default all)")
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    semisync = not a.naive and os.environ.get("DBGUARD_SEMISYNC", "1") != "0"
    sets = {k: v for k, v in cfg.sets.items() if not a.sets or k in a.sets.split(",")}
    all_nodes = [n for sc in sets.values() for n in sc.nodes]
    wait_ready(all_nodes, a.user, a.password, a.wait)

    for rs, sc in sets.items():
        primary = sc.nodes[0]
        conns = {n: m.connect("127.0.0.1", host_port(n), a.user, a.password, timeout=3)
                 for n in sc.nodes}
        # Refuse to bootstrap a set whose nodes already disagree (not a fresh fleet).
        from dbguard.gtid import GtidSet
        g = {n: GtidSet.parse(m.gtid_executed(c)) for n, c in conns.items()}
        for n in sc.nodes[1:]:
            if not g[n].is_subset(g[primary]):
                raise SystemExit(f"{rs}: {n} has GTIDs the intended primary {primary} lacks: "
                                 f"{g[n].subtract(g[primary])}. Not a fresh fleet, refusing.")
        setup_primary(primary, conns[primary], semisync)
        for n in sc.nodes[1:]:
            setup_replica(n, conns[n], primary, cfg, semisync)
        for c in conns.values():
            c.close()
    if semisync:
        wait_semisync_clients(FleetConfig(**{**cfg.model_dump(), "sets": sets}), a.user,
                              a.password, 30)
    else:
        time.sleep(1)
    print(json.dumps({"semisync": semisync, "topology": topology(
        FleetConfig(**{**cfg.model_dump(), "sets": sets}), a.user, a.password)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
