"""The checker (docs/INTERFACES.md "Workload and checker").

Connects directly to every node of one replica set and asserts the four properties:

- lossless       every (client, seq) in the ack log exists on the primary
- no phantom     every row of the run on the primary was acknowledged, or its exact seq got
                 an error (an in-flight write may land or not, that is neither)
- single writer  exactly one node has super_read_only=0 AND read_only=0, and a direct
                 INSERT (as root, inside a transaction that is rolled back) fails on every
                 other reachable node
- convergence    within --converge-timeout every reachable replica's gtid_executed equals
                 the primary's (compared as GTID sets)

The logic works on the small ``Node`` protocol so tests can drive it with fakes.
Prints one JSON verdict. Exit 0 if all four pass, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from dbguard.gtid import GtidSet
from dbguard.harness.workload import read_ack_log

PROBE_SQL = ("INSERT INTO chaos.writes (client_id, seq, ts, run_id, payload) "
             "VALUES (-1, -1, NOW(6), %s, NULL)")
LIST_LIMIT = 50


class Node(Protocol):
    name: str

    def read_only(self) -> tuple[bool, bool]:
        """(super_read_only, read_only)."""

    def probe_insert(self) -> str | None:
        """Try a write inside a transaction and roll it back. None if the INSERT was
        accepted, else the error text."""

    def gtid_executed(self) -> str: ...

    def rows(self, run_id: str) -> set[tuple[int, int]]: ...


class MySQLNode:
    """A live node reached over its published host port."""

    def __init__(self, name: str, host: str, port: int, user: str, password: str,
                 timeout: float = 3.0):
        from dbguard import mysqlx_sync
        self.name = name
        self._m = mysqlx_sync
        self.conn = mysqlx_sync.connect(host, port, user, password, timeout=timeout,
                                        read_timeout=30, write_timeout=30)

    def read_only(self) -> tuple[bool, bool]:
        r = self._m.query(self.conn, "SELECT @@GLOBAL.super_read_only AS s, "
                                     "@@GLOBAL.read_only AS r")[0]
        return bool(int(r["s"])), bool(int(r["r"]))

    def probe_insert(self) -> str | None:
        try:
            self._m.execute(self.conn, "BEGIN")
            self._m.execute(self.conn, PROBE_SQL, ("checker-probe",))
        except Exception as e:
            try:
                self._m.execute(self.conn, "ROLLBACK")
            except Exception:
                pass
            return str(e)
        self._m.execute(self.conn, "ROLLBACK")
        return None

    def gtid_executed(self) -> str:
        return self._m.gtid_executed(self.conn)

    def rows(self, run_id: str) -> set[tuple[int, int]]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT client_id, seq FROM chaos.writes WHERE run_id=%s", (run_id,))
            return {(int(r["client_id"]), int(r["seq"])) for r in cur.fetchall()}

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


@dataclass
class AckSets:
    acked: set[tuple[int, int]]
    errored: set[tuple[int, int]]


def load_acks(paths: Iterable[str]) -> AckSets:
    acked: set[tuple[int, int]] = set()
    errored: set[tuple[int, int]] = set()
    for p in paths:
        oks, errs = read_ack_log(p)
        acked.update((int(r["client"]), int(r["seq"])) for r in oks)
        errored.update((int(r["client"]), int(r["seq"])) for r in errs)
    return AckSets(acked, errored)


def lossless_and_phantom(acks: AckSets, rows: set[tuple[int, int]]) -> dict[str, Any]:
    lost = sorted(acks.acked - rows)
    phantom = sorted(rows - acks.acked - acks.errored)
    maybe = acks.errored - acks.acked
    return {
        "lossless": {"ok": not lost, "count": len(lost), "lost": [list(x) for x in lost[:LIST_LIMIT]]},
        "no_phantom": {"ok": not phantom, "count": len(phantom),
                       "phantom": [list(x) for x in phantom[:LIST_LIMIT]],
                       "errored_landed": len(maybe & rows),
                       "errored_absent": len(maybe - rows)},
    }


def single_writer(nodes: list[Node], unreachable: dict[str, str]) -> dict[str, Any]:
    writable: list[str] = []
    probes: dict[str, str] = {}
    violations = 0
    for n in nodes:
        sro, ro = n.read_only()
        if not sro and not ro:
            writable.append(n.name)
            continue
        err = n.probe_insert()
        if err is None:
            probes[n.name] = "accepted"
            violations += 1
        else:
            probes[n.name] = "rejected"
    if len(writable) > 1:
        violations += len(writable) - 1
    ok = len(writable) == 1 and violations == 0
    return {"ok": ok, "violations": violations if writable else max(violations, 1),
            "writable": writable, "insert_probe": probes, "unreachable": sorted(unreachable)}


def convergence(primary: Node, replicas: list[Node], timeout: float,
                clock: Callable[[], float] = time.monotonic,
                sleep: Callable[[float], None] = time.sleep,
                poll: float = 0.5) -> dict[str, Any]:
    t0 = clock()
    while True:
        target = GtidSet.parse(primary.gtid_executed())
        gtids = {r.name: GtidSet.parse(r.gtid_executed()) for r in replicas}
        behind = {name: str(target.subtract(g)) for name, g in gtids.items() if g != target}
        elapsed = clock() - t0
        if not behind:
            return {"ok": True, "converge_s": round(elapsed, 3), "primary_gtid": str(target),
                    "diverged": {}}
        if elapsed >= timeout:
            extra = {name: str(g.subtract(target)) for name, g in gtids.items()
                     if not g.is_subset(target)}
            return {"ok": False, "converge_s": None, "primary_gtid": str(target),
                    "missing_on_replica": behind, "extra_on_replica": extra,
                    "diverged": {name: str(g) for name, g in gtids.items() if g != target}}
        sleep(poll)


def check(run_id: str, acks: AckSets, nodes: list[Node], unreachable: dict[str, str],
          converge_timeout: float = 60.0, **conv_kw: Any) -> dict[str, Any]:
    verdict: dict[str, Any] = {"run_id": run_id, "ts": time.time(),
                               "nodes": [n.name for n in nodes] + sorted(unreachable),
                               "acked": len(acks.acked), "errored": len(acks.errored)}
    sw = single_writer(nodes, unreachable)
    props: dict[str, Any] = {"single_writer": sw}
    primary_name = sw["writable"][0] if len(sw["writable"]) == 1 else None
    verdict["primary"] = primary_name
    if primary_name is None:
        props["lossless"] = {"ok": False, "count": None, "lost": [], "reason": "no single primary"}
        props["no_phantom"] = {"ok": False, "count": None, "phantom": [],
                               "reason": "no single primary"}
        props["convergence"] = {"ok": False, "converge_s": None, "reason": "no single primary"}
    else:
        primary = next(n for n in nodes if n.name == primary_name)
        replicas = [n for n in nodes if n.name != primary_name]
        props["convergence"] = convergence(primary, replicas, converge_timeout, **conv_kw)
        rows = primary.rows(run_id)
        verdict["rows"] = len(rows)
        props.update(lossless_and_phantom(acks, rows))
    verdict["properties"] = props
    lost = props["lossless"]
    ph = props["no_phantom"]
    verdict.update({
        "lost_acked_writes": lost["count"],
        "lost_list": lost["lost"][:20],
        "phantom_writes": ph["count"],
        "phantom_list": ph["phantom"][:20],
        "single_writer_violations": sw["violations"],
        "converged": props["convergence"]["ok"],
        "converge_s": props["convergence"].get("converge_s"),
    })
    verdict["pass"] = all(p["ok"] for p in props.values())
    return verdict


def parse_nodes(spec: str) -> list[tuple[str, str, int]]:
    """"mysql-a1:13311,mysql-a2:13312" -> [(name, host, port)]. The name is the label, the
    connection goes to 127.0.0.1:port unless given as name@host:port."""
    out = []
    for item in [s.strip() for s in spec.split(",") if s.strip()]:
        name, _, port = item.rpartition(":")
        host = "127.0.0.1"
        if "@" in name:
            name, _, host = name.partition("@")
        out.append((name, host, int(port)))
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="checker", description=__doc__.split("\n")[0])
    p.add_argument("--run-id", required=True)
    p.add_argument("--ack-log", action="append", required=True,
                   help="ack log path, repeatable (or comma separated)")
    p.add_argument("--nodes", default="mysql-a1:13311,mysql-a2:13312,mysql-a3:13313")
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="root")
    p.add_argument("--converge-timeout", type=float, default=60.0)
    p.add_argument("--connect-timeout", type=float, default=3.0)
    a = p.parse_args(argv)

    paths = [x for arg in a.ack_log for x in arg.split(",") if x]
    acks = load_acks(paths)
    nodes: list[MySQLNode] = []
    unreachable: dict[str, str] = {}
    for name, host, port in parse_nodes(a.nodes):
        try:
            nodes.append(MySQLNode(name, host, port, a.user, a.password, a.connect_timeout))
        except Exception as e:
            unreachable[name] = str(e)
    try:
        verdict = check(a.run_id, acks, nodes, unreachable, a.converge_timeout)
    except Exception as e:
        verdict = {"run_id": a.run_id, "pass": False, "error": f"{type(e).__name__}: {e}"}
    finally:
        for n in nodes:
            n.close()
    verdict["unreachable"] = unreachable
    print(json.dumps(verdict), flush=True)
    return 0 if verdict.get("pass") else 1


if __name__ == "__main__":
    sys.exit(main())
