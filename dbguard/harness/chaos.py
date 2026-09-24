"""The chaos harness: inject one fault per run into a live fleet and record one result row.

Usage (bin/chaos):
  bin/chaos --scenario kill --runs 2 --mode dbguard
  bin/chaos --all --mode naive

Row schema: docs/INTERFACES.md "Chaos result row". Extra keys added by the harness are
listed there too (errors, resume_s, semisync, netem_ms, primary_at_inject, ...).

Timing. Every timestamp is the host's wall clock (time.time()), the same clock the workload
uses for its ack log, so inject_ts, t_ok and t_err are directly comparable. failover_s,
stall_s and the commit latencies are computed here from the ack log itself (the workload's
summary is kept alongside for cross-checking).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dbguard.cli.client import AgentClient, ApiError, ManagerClient, request
from dbguard.harness import docker

REPO = docker.REPO
RESULTS = REPO / "results"
TMP = RESULTS / "tmp"
FLEET_YAML = REPO / "deploy" / "fleet.yaml"

SCENARIOS = ["kill", "hang-container", "hang-process", "partition-replicas", "partition-manager",
             "cost", "replica-loss", "switchover", "disk-full", "kill-two"]
# --all order: cheap and non-destructive first, the dataset-growing one late
ALL_ORDER = ["switchover", "kill", "hang-process", "hang-container", "partition-replicas",
             "partition-manager", "kill-two", "cost", "replica-loss", "disk-full"]
MODES = ["dbguard", "naive", "orchestrator"]
MANAGER_CONTAINER = "dbguard"
ORCH_CONTAINER = "orchestrator"
ORCH_URL = os.environ.get("DBGUARD_ORCH_URL", "http://127.0.0.1:13000")
HOOKS_FILE = "/var/lib/orchestrator/hooks/events.jsonl"
STATE_EVENT_TYPES = {"failover", "switchover", "rejoin", "rebuild", "replace", "halt", "resume",
                     "suspect", "degraded", "healthy", "stall"}


def log(msg: str) -> None:
    docker.log(msg)


# ---------------------------------------------------------------- topology

def set_index(rs: str) -> int:
    m = re.fullmatch(r"rs(\d+)", rs)
    if not m:
        raise ValueError(f"bad set name {rs}")
    return int(m.group(1))


def set_letter(rs: str) -> str:
    return "abcdefgh"[set_index(rs) - 1]


def set_nodes(rs: str) -> list[str]:
    return [f"mysql-{set_letter(rs)}{i}" for i in (1, 2, 3)]


def spare_node(rs: str) -> str:
    return f"mysql-{set_letter(rs)}4"


def _node_parts(node: str) -> tuple[int, int]:
    m = re.fullmatch(r"mysql-([a-h])(\d)", node)
    if not m:
        raise ValueError(f"bad node name {node}")
    return "abcdefgh".index(m.group(1)) + 1, int(m.group(2))


def mysql_port(node: str) -> int:
    s, i = _node_parts(node)
    return 13300 + 10 * s + i


def agent_port(node: str) -> int:
    s, i = _node_parts(node)
    return 18000 + 10 * s + i


def haproxy_port(rs: str) -> int:
    return 13305 + set_index(rs)


def agent(node: str) -> AgentClient:
    return AgentClient(f"http://127.0.0.1:{agent_port(node)}", timeout=2.0)


# ---------------------------------------------------------------- GTID helpers (harness-local)

def parse_gtid(s: str | None) -> dict[str, list[tuple[int, int]]]:
    """'uuid:1-5:7,uuid2:3' -> {uuid: merged intervals}. Tolerates whitespace and newlines."""
    out: dict[str, list[tuple[int, int]]] = {}
    if not s:
        return out
    for part in re.sub(r"\s+", "", s).split(","):
        if not part:
            continue
        fields = part.split(":")
        u = fields[0].lower()
        ivs = out.setdefault(u, [])
        for f in fields[1:]:
            if not f or not f[0].isdigit():   # tagged GTIDs (8.3+) keep the tag in the key
                u = f"{fields[0].lower()}:{f}"
                ivs = out.setdefault(u, [])
                continue
            a, _, b = f.partition("-")
            ivs.append((int(a), int(b or a)))
    for u, ivs in out.items():
        ivs.sort()
        merged: list[tuple[int, int]] = []
        for a, b in ivs:
            if merged and a <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], b))
            else:
                merged.append((a, b))
        out[u] = merged
    return {u: v for u, v in out.items() if v}


def gtid_count(s: str | None) -> int:
    return sum(b - a + 1 for ivs in parse_gtid(s).values() for a, b in ivs)


def gtid_equal(a: str | None, b: str | None) -> bool:
    return parse_gtid(a) == parse_gtid(b)


# ---------------------------------------------------------------- ack log analysis

@dataclass
class AckStats:
    acked: int = 0
    errors: int = 0
    errors_after_inject: int = 0
    first_error_ts: float | None = None
    first_ok_after_ts: float | None = None
    failover_s: float | None = None
    stall_s: float | None = None
    stall_start: float | None = None
    stall_end: float | None = None
    commit_p50_ms: float | None = None
    commit_p99_ms: float | None = None
    writes_per_s: float | None = None
    first_ts: float | None = None
    last_ts: float | None = None


def read_ack_log(path: Path) -> tuple[list[dict], list[dict]]:
    oks, errs = [], []
    if not path.exists():
        return oks, errs
    for line in path.read_text().splitlines():
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "t_ok" in r:
            oks.append(r)
        elif "t_err" in r or "error" in r:
            errs.append(r)
    return oks, errs


def nearest_rank(xs: list[float], q: float) -> float | None:
    import math
    xs = sorted(xs)
    if not xs:
        return None
    return xs[max(1, math.ceil(q / 100 * len(xs))) - 1]


def analyze_acks(oks: list[dict], errs: list[dict], inject_ts: float | None,
                 window_end: float | None = None, stall_threshold_s: float = 1.0) -> AckStats:
    """failover_s: inject -> first ok after the first error at/after inject. If there was no
    error (a stall without errors, e.g. semi-sync blocking commits), inject -> end of the
    largest ack gap that starts after inject - 1 s, when that gap exceeds stall_threshold_s.
    stall_s: the largest gap between consecutive acknowledged writes (all clients merged) in
    [inject - 1 s, window_end]."""
    st = AckStats(acked=len(oks), errors=len(errs))
    t_ok = sorted(float(r["t_ok"]) for r in oks)
    if t_ok:
        st.first_ts, st.last_ts = t_ok[0], t_ok[-1]
        lat = [float(r["latency_ms"]) for r in oks if r.get("latency_ms") is not None]
        st.commit_p50_ms = nearest_rank(lat, 50)
        st.commit_p99_ms = nearest_rank(lat, 99)
        span = t_ok[-1] - t_ok[0]
        st.writes_per_s = len(t_ok) / span if span > 0 else None
    if inject_ts is None:
        return st
    t_err = sorted(float(r["t_err"]) for r in errs if r.get("t_err") is not None)
    after = [t for t in t_err if t >= inject_ts - 0.05]
    st.errors_after_inject = len(after)
    if after:
        st.first_error_ts = after[0]
        nxt = [t for t in t_ok if t > after[0]]
        st.first_ok_after_ts = nxt[0] if nxt else None
    # largest ack gap around/after the injection
    lo = inject_ts - 1.0
    hi = window_end if window_end is not None else float("inf")
    before = [t for t in t_ok if t < lo]
    pts = ([before[-1]] if before else []) + [t for t in t_ok if lo <= t <= hi]
    best = (0.0, None, None)
    for a, b in zip(pts, pts[1:]):
        if b - a > best[0] and b >= lo:
            best = (b - a, a, b)
    if best[1] is not None:
        st.stall_s, st.stall_start, st.stall_end = best
    if st.first_ok_after_ts is not None:
        st.failover_s = st.first_ok_after_ts - inject_ts
        # client-visible stall for an error-driven failover: last ok before the first error
        # to the first ok after it
        prev = [t for t in t_ok if t <= st.first_error_ts]
        if prev:
            st.stall_s = max(st.stall_s or 0.0, st.first_ok_after_ts - prev[-1])
    elif st.stall_end is not None and st.stall_s and st.stall_s > stall_threshold_s \
            and st.stall_end > inject_ts:
        st.failover_s = st.stall_end - inject_ts
    return st


# ---------------------------------------------------------------- config knobs

def load_knobs() -> dict:
    knobs = {"detect_window_s": None, "probe_timeout_s": None, "probe_failures": None,
             "rebuild_after_s": None, "cooldown_s": None, "fence_deadline_s": None,
             "catchup_deadline_s": None, "rejoin": None, "mode": None}
    try:
        import yaml
        y = yaml.safe_load(FLEET_YAML.read_text()) or {}
        for k in knobs:
            if k in y:
                knobs[k] = y[k]
    except Exception as e:  # noqa: BLE001
        log(f"cannot read {FLEET_YAML}: {e}")
    return knobs


# ---------------------------------------------------------------- SQL (direct via host ports)

def insecure_tls():
    """TLS without certificate checks (docs/INTERFACES.md auth decision): caching_sha2 over
    plain TCP needs the cryptography package for the RSA exchange."""
    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def sql(node_or_port: str | int, query: str, args: tuple = (), *, user: str = "root",
        password: str = "root", timeout: float = 5.0, many: bool = True,
        db: str | None = None) -> list[tuple]:
    import pymysql
    port = node_or_port if isinstance(node_or_port, int) else mysql_port(node_or_port)
    conn = pymysql.connect(host="127.0.0.1", port=port, user=user, password=password,
                           connect_timeout=timeout, read_timeout=timeout, write_timeout=timeout,
                           autocommit=True, database=db, ssl=insecure_tls())
    try:
        with conn.cursor() as cur:
            cur.execute(query, args or None)
            return list(cur.fetchall()) if many else [cur.fetchone()]
    finally:
        conn.close()


# ---------------------------------------------------------------- subprocess tools

class Workload:
    """bin/workload in a subprocess. The ack log is the source of truth."""

    def __init__(self, run_dir: Path, run_id: str, port: int, clients: int, duration: float,
                 tag: str = "wl"):
        self.ack_log = run_dir / f"{run_id}.{tag}.ack.jsonl" if tag != "wl" else \
            TMP / f"{run_id}.ack.jsonl"
        self.out = run_dir / f"{tag}.stdout"
        self.err = run_dir / f"{tag}.stderr"
        self.args = [sys.executable, str(REPO / "bin" / "workload"), "--host", "127.0.0.1",
                     "--port", str(port), "--clients", str(clients), "--duration",
                     f"{duration:g}", "--run-id", run_id, "--ack-log", str(self.ack_log)]
        self.duration = duration
        self.proc: subprocess.Popen | None = None
        self.t_start: float | None = None

    def start(self) -> None:
        self.ack_log.parent.mkdir(parents=True, exist_ok=True)
        if self.ack_log.exists():
            self.ack_log.unlink()
        log("$ " + " ".join(self.args))
        self.t_start = time.time()
        self.proc = subprocess.Popen(self.args, stdout=open(self.out, "w"),
                                     stderr=open(self.err, "w"), cwd=REPO)

    def acks_so_far(self) -> int:
        try:
            return sum(1 for line in self.ack_log.open() if '"t_ok"' in line)
        except FileNotFoundError:
            return 0

    def last_ok(self) -> float | None:
        oks, _ = read_ack_log(self.ack_log)
        return max((float(r["t_ok"]) for r in oks), default=None)

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def wait(self, grace: float = 60.0) -> dict | None:
        if self.proc is None:
            return None
        remaining = (self.t_start or time.time()) + self.duration + grace - time.time()
        try:
            self.proc.wait(timeout=max(1.0, remaining))
        except subprocess.TimeoutExpired:
            log("workload overran its duration, sending SIGINT")
            self.stop()
        return self.summary()

    def stop(self) -> dict | None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        return self.summary()

    def summary(self) -> dict | None:
        return last_json(self.out.read_text() if self.out.exists() else "")


def last_json(text: str) -> dict | None:
    text = text.strip()
    if not text:
        return None
    try:
        v = json.loads(text)
        return v if isinstance(v, dict) else None
    except json.JSONDecodeError:
        pass
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return None


def run_checker(run_id: str, ack_log: Path, nodes: list[str], run_dir: Path,
                extra: list[str] = ()) -> dict:
    args = [sys.executable, str(REPO / "bin" / "checker"), "--run-id", run_id, "--ack-log",
            str(ack_log), "--nodes", ",".join(f"{n}:{mysql_port(n)}" for n in nodes), *extra]
    log("$ " + " ".join(args))
    cp = subprocess.run(args, capture_output=True, text=True, timeout=300, cwd=REPO)
    (run_dir / "checker.stdout").write_text(cp.stdout)
    (run_dir / "checker.stderr").write_text(cp.stderr)
    v = last_json(cp.stdout)
    if v is None:
        raise RuntimeError(f"checker produced no JSON verdict (rc={cp.returncode}): "
                           f"{cp.stderr.strip()[-500:]}")
    return v


def checker_fields(v: dict) -> dict:
    """Map the checker verdict onto the result-row fields, tolerating naming variants."""
    def pick(*keys, default=None):
        for k in keys:
            cur: Any = v
            for part in k.split("."):
                cur = cur.get(part) if isinstance(cur, dict) else None
            if cur is not None:
                return cur
        return default

    def count(x):
        if isinstance(x, list):
            return len(x)
        return int(x or 0)

    lost_list = pick("lost_list", "lost", "lossless.lost", default=[])
    lost_n = pick("lost_acked_writes", "lost_count", "lossless.count")
    phantom = pick("phantom_writes", "phantom_count", "phantom", "no_phantom.count")
    swv = pick("single_writer_violations", "single_writer.violations", "writers_violations")
    conv = pick("converged", "convergence.ok", "convergence.converged")
    conv_s = pick("converge_s", "convergence.converge_s", "convergence.seconds")
    return {
        "acked_writes_checker": pick("acked", "acked_writes"),
        "lost_acked_writes": count(lost_n if lost_n is not None else lost_list),
        "lost_list": (lost_list if isinstance(lost_list, list) else [])[:20],
        "phantom_writes": count(phantom),
        "single_writer_violations": count(swv),
        "converged": bool(conv) if conv is not None else False,
        "converge_s": conv_s,
    }


# ---------------------------------------------------------------- the fleet under test

class Fleet:
    def __init__(self, rs: str, mode: str, manager_url: str | None = None):
        self.rs = rs
        self.mode = mode
        self.mc = ManagerClient(manager_url, timeout=3.0)

    # -- views
    @property
    def nodes(self) -> list[str]:
        return set_nodes(self.rs)

    def statuses(self, nodes: list[str] | None = None) -> dict[str, dict | None]:
        nodes = nodes or self.nodes + [spare_node(self.rs)]
        out: dict[str, dict | None] = {}
        threads = []

        def one(n):
            out[n] = agent(n).status()
        for n in nodes:
            t = threading.Thread(target=one, args=(n,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        return out

    def writable_nodes(self) -> list[str]:
        return [n for n, s in self.statuses().items()
                if s and s.get("super_read_only") is False and s.get("mysqld_responsive")]

    def agent_primaries(self) -> list[str]:
        return [n for n in self.nodes + [spare_node(self.rs)] if agent(n).primary_code() == 200]

    def manager_set(self) -> dict | None:
        try:
            return self.mc.set(self.rs)
        except ApiError:
            return None

    def primary(self) -> str | None:
        if self.mode != "orchestrator":
            st = self.manager_set()
            if st and st.get("primary"):
                return st["primary"]
        w = self.agent_primaries()
        return w[0] if len(w) == 1 else None

    def members(self) -> list[str]:
        """Nodes the manager counts in the set (may include the spare after a replace)."""
        st = self.manager_set() if self.mode != "orchestrator" else None
        if st and st.get("nodes"):
            return [n for n, v in st["nodes"].items() if v.get("role") != "spare"]
        return self.nodes

    def events(self, since: float, rs: str | None = None) -> list[dict]:
        if self.mode == "orchestrator":
            return orch_events(since)
        try:
            return [e for e in self.mc.events(rs or self.rs, since - 0.001)
                    if float(e.get("ts") or 0) >= since]
        except ApiError:
            return []

    # -- waits
    def wait_new_primary(self, old: str, timeout: float = 90.0) -> tuple[str | None, float | None]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.mode == "orchestrator":
                w = self.agent_primaries()
                if len(w) == 1 and w[0] != old:
                    return w[0], time.time()
            else:
                st = self.manager_set()
                if st and st.get("primary") and st["primary"] != old and \
                        st.get("state") not in ("FAILING_OVER", "SUSPECT", "HALTED"):
                    return st["primary"], time.time()
                if st and st.get("state") == "HALTED":
                    log(f"{self.rs} HALTED: {st.get('halt_reason')}")
                    return None, None
            time.sleep(0.25)
        return None, None

    def wait_event(self, since: float, types: set[str], timeout: float,
                   pred: Callable[[dict], bool] = lambda e: True) -> dict | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for e in self.events(since):
                if e.get("type") in types and pred(e):
                    return e
            time.sleep(0.5)
        return None

    def wait_state(self, states: set[str], timeout: float) -> dict | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            st = self.manager_set()
            if st and st.get("state") in states:
                return st
            time.sleep(0.5)
        return None

    # -- heal
    def healthy_now(self) -> tuple[bool, str]:
        """(ok, why). HEALTHY with a primary and 2 streaming replicas, one agent answering 200,
        every member's gtid_executed equal, and the cooldown expired."""
        prims = self.agent_primaries()
        if len(prims) != 1:
            return False, f"agents answering /primary 200: {prims}"
        p = prims[0]
        if self.mode != "orchestrator":
            st = self.manager_set()
            if not st:
                return False, "manager unreachable"
            if st.get("state") != "HEALTHY":
                return False, f"state {st.get('state')} {st.get('halt_reason') or ''}"
            if st.get("primary") != p:
                return False, f"manager primary {st.get('primary')} != agent primary {p}"
            reps = [n for n, v in (st.get("nodes") or {}).items()
                    if v.get("role") == "replica" and v.get("reachable")]
            if len(reps) < 2:
                return False, f"replicas {reps}"
            cd = st.get("cooldown_until")
            if cd and float(cd) > time.time():
                return False, f"cooldown for {float(cd) - time.time():.0f}s"
            members = [p] + reps
        else:
            members = self.nodes
        ss = self.statuses(members)
        pg = (ss.get(p) or {}).get("gtid_executed")
        for n in members:
            s = ss.get(n)
            if not s:
                return False, f"{n} agent unreachable"
            if n == p:
                continue
            r = s.get("replica") or {}
            if r.get("source_host") != p or r.get("io_running") != "Yes" or r.get("sql_running") != "Yes":
                return False, f"{n} not replicating from {p}: {r.get('source_host')} io={r.get('io_running')} sql={r.get('sql_running')}"
            if not gtid_equal(s.get("gtid_executed"), pg):
                return False, f"{n} gtid differs from {p}"
        return True, "ok"

    def clear_faults(self, extra_containers: list[str] = ()) -> None:
        for c in self.nodes + [spare_node(self.rs)] + list(extra_containers):
            st = docker.container_status(c)
            if st is None:
                continue
            if st == "paused":
                docker.thaw(c)
                _frozen.discard(c)
            if st in ("running", "paused"):
                docker.clear_net(c)

    def heal(self, timeout: float = 120.0, extra_fault_containers: list[str] = (),
             clear: bool = True) -> dict:
        """Bring every container back, clear faults, wait for a clean set. Hard reset on failure."""
        t0 = time.time()
        if clear:
            self.clear_faults(extra_fault_containers)
        svcs = self.nodes + ["haproxy"] + ([MANAGER_CONTAINER] if self.mode != "orchestrator" else [])
        down = [s for s in svcs if not docker.is_running(s)]
        if down:
            docker.up(*down)
        why = ""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.mode == "orchestrator":
                self.harness_repair()
            else:
                st = self.manager_set()
                if st and st.get("state") == "HALTED":
                    why = f"HALTED: {st.get('halt_reason')}"
                    break
            ok, why = self.healthy_now()
            if ok:
                log(f"heal: {self.rs} clean in {time.time() - t0:.1f}s, primary {self.primary()}")
                return {"hard_reset": False, "heal_s": time.time() - t0}
            time.sleep(1.0)
        log(f"heal: {self.rs} not clean after {time.time() - t0:.0f}s ({why}), hard reset")
        self.hard_reset()
        return {"hard_reset": True, "heal_s": time.time() - t0, "heal_reason": why}

    def hard_reset(self, timeout: float = 240.0) -> None:
        nodes = self.nodes + [spare_node(self.rs)]
        vols = [v for n in nodes for v in docker.volumes_of(n)]
        docker.rm(*nodes)
        docker.rm_volume(*vols)
        if self.mode != "orchestrator":
            # the manager holds per-set state (HALTED, cooldown); restart it so it bootstraps
            docker.stop(MANAGER_CONTAINER)
        docker.up(*self.nodes)
        if self.mode != "orchestrator":
            docker.up(MANAGER_CONTAINER)
        else:
            bootstrap = REPO / "bin" / "bootstrap"
            deadline = time.time() + 120
            while time.time() < deadline and not all(agent(n).status() for n in self.nodes):
                time.sleep(2)
            subprocess.run([sys.executable, str(bootstrap), "--sets", self.rs], cwd=REPO, check=False)
        deadline = time.time() + timeout
        why = ""
        while time.time() < deadline:
            if self.mode == "orchestrator":
                self.harness_repair()
            ok, why = self.healthy_now()
            if ok:
                log(f"hard reset of {self.rs} done")
                if self.mode == "orchestrator":
                    orch_discover(self.primary() or self.nodes[0])
                return
            time.sleep(2)
        raise RuntimeError(f"hard reset of {self.rs} did not converge: {why}")

    # -- orchestrator mode: nobody rejoins a dead primary, the harness does it via the agents
    def harness_repair(self) -> list[dict]:
        acts = []
        ss = self.statuses(self.nodes)
        writable = [n for n, s in ss.items() if s and s.get("super_read_only") is False]
        if len(writable) > 1:
            log(f"repair: more than one writable node {writable}, fencing all but the orchestrator master")
            return acts
        if not writable:
            alive = {n: s for n, s in ss.items() if s and s.get("mysqld_responsive")}
            if not alive:
                return acts
            p = max(alive, key=lambda n: gtid_count(alive[n].get("gtid_executed")))
            code, body = agent(p).post("/promote", timeout=30)
            acts.append({"promote": p, "code": code})
            return acts
        p = writable[0]
        for n, s in ss.items():
            if n == p or not s or not s.get("mysqld_responsive"):
                continue
            r = s.get("replica") or {}
            if r.get("source_host") == p and r.get("io_running") in ("Yes", "Connecting") \
                    and r.get("sql_running") == "Yes":
                continue
            ng = s.get("gtid_executed") or ""
            try:
                subset = bool(sql(p, "SELECT GTID_SUBSET(%s, @@GLOBAL.gtid_executed)", (ng,))[0][0])
            except Exception as e:  # noqa: BLE001
                log(f"repair: subset check failed {e}")
                continue
            t0 = time.time()
            if subset:
                code, body = agent(n).post("/repoint", {"source": p}, timeout=30)
                acts.append({"node": n, "branch": "repoint", "phantom_gtids": 0,
                             "duration_s": time.time() - t0, "code": code})
            else:
                phantom = gtid_count(sql(p, "SELECT GTID_SUBTRACT(%s, @@GLOBAL.gtid_executed)", (ng,))[0][0])
                code, body = agent(n).post("/rebuild", {"donor": p}, timeout=600)
                agent(n).post("/repoint", {"source": p}, timeout=30)
                acts.append({"node": n, "branch": "rebuild", "phantom_gtids": phantom,
                             "duration_s": time.time() - t0, "code": code, "bytes":
                             (body or {}).get("bytes") if isinstance(body, dict) else None})
            log(f"repair: {acts[-1]}")
        return acts


_frozen: set[str] = set()


def frozen(c: str) -> bool:
    return c in _frozen


def freeze(c: str) -> None:
    _frozen.add(c)
    docker.freeze(c)


def thaw(c: str) -> None:
    docker.thaw(c)
    _frozen.discard(c)


# ---------------------------------------------------------------- orchestrator helpers

def orch_get(path: str, timeout: float = 5.0) -> Any:
    try:
        code, body = request("GET", ORCH_URL + path, timeout=timeout)
    except ApiError:
        return None
    return body if code == 200 else None


def orch_discover(*hosts: str) -> None:
    for h in hosts:
        orch_get(f"/api/discover/{h}/3306")


def orch_events(since: float) -> list[dict]:
    """Hook lines written by deploy/orchestrator hooks, shaped like Event rows."""
    cp = docker.exec_(ORCH_CONTAINER, ["cat", HOOKS_FILE], check=False, mutate=False, user=None)
    out = []
    for line in cp.stdout.splitlines():
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if float(e.get("ts") or 0) >= since:
            out.append(e)
    return out


def orch_setup(rs_list: list[str]) -> None:
    docker.up(ORCH_CONTAINER, profiles=("orchestrator",))
    docker.stop(MANAGER_CONTAINER)
    deadline = time.time() + 120
    while time.time() < deadline and orch_get("/api/health") is None:
        time.sleep(2)
    for rs in rs_list:
        f = Fleet(rs, "orchestrator")
        p = f.primary() or set_nodes(rs)[0]
        orch_discover(p, *[n for n in set_nodes(rs) if n != p])
    deadline = time.time() + 90
    while time.time() < deadline:
        ok = True
        for rs in rs_list:
            inst = orch_get(f"/api/cluster/alias/{rs}") or []
            if len([i for i in inst if i.get("IsLastCheckValid")]) < 3:
                ok = False
        if ok:
            log("orchestrator sees every set")
            return
        time.sleep(2)
    log("WARNING orchestrator did not report 3 valid instances per set within 90 s")


def orch_teardown() -> None:
    docker.stop(ORCH_CONTAINER, profiles=("orchestrator",))
    docker.up(MANAGER_CONTAINER)


# ---------------------------------------------------------------- run context

@dataclass
class Opts:
    scenario: str
    runs: int = 1
    mode: str = "dbguard"
    rs: str = "rs1"
    clients: int = 8
    workload_seconds: float = 40.0
    inject_after: float = 10.0
    out: Path | None = None
    keep_going: bool = False
    hang_seconds: float = 90.0
    partition_hold_s: float = 30.0
    cost_seconds: float = 60.0
    semisync: str | None = None       # cost: "on" | "off"
    netem: float | None = None        # cost: ms
    cost_all: bool = False
    data_mb: int = 200
    rs2_workload: bool = False
    stall_hold_s: float = 10.0
    heal_timeout: float = 120.0
    skip_heal_before: bool = False


@dataclass
class Run:
    opts: Opts
    fleet: Fleet
    run_id: str
    dir: Path
    commands: list[str] = field(default_factory=list)
    row: dict = field(default_factory=dict)
    t_start: float = field(default_factory=time.time)


def base_row(run: Run, env: dict) -> dict:
    o = run.opts
    return {
        "run_id": run.run_id, "scenario": o.scenario, "mode": o.mode, "rs": o.rs,
        "ts": datetime.now(UTC).isoformat(timespec="seconds"),
        "host": env["host"], "mysql_version": env["mysql_version"],
        "docker_version": env["docker_version"],
        "detect_window_s": env["knobs"].get("detect_window_s"),
        "probe_timeout_s": env["knobs"].get("probe_timeout_s"),
        "config": env["knobs"],
        "clients": o.clients, "workload_s": o.workload_seconds, "inject_ts": None,
        "failover_s": None, "first_error_ts": None, "first_ok_after_ts": None,
        "acked_writes": 0, "lost_acked_writes": 0, "lost_list": [], "phantom_writes": 0,
        "single_writer_violations": 0, "converged": False, "converge_s": None,
        "false_failover": False, "stall_s": None, "writes_on_woken_primary": None,
        "rejoin": None, "event": None, "rs2_state_changes": 0,
        "commit_p50_ms": None, "commit_p99_ms": None, "writes_per_s": None, "clone": None,
        "errors": 0, "primary_at_inject": None, "new_primary": None, "notes": "",
    }


def note(run: Run, msg: str) -> None:
    log(f"note: {msg}")
    run.row["notes"] = (run.row.get("notes") + "; " if run.row.get("notes") else "") + msg


def apply_acks(run: Run, wl: Workload, inject_ts: float | None, window_end: float | None = None) -> AckStats:
    oks, errs = read_ack_log(wl.ack_log)
    st = analyze_acks(oks, errs, inject_ts, window_end)
    r = run.row
    r.update({"acked_writes": st.acked, "errors": st.errors, "first_error_ts": st.first_error_ts,
              "first_ok_after_ts": st.first_ok_after_ts, "failover_s": st.failover_s,
              "stall_s": st.stall_s, "commit_p50_ms": st.commit_p50_ms,
              "commit_p99_ms": st.commit_p99_ms, "writes_per_s": st.writes_per_s})
    r["workload_summary"] = wl.summary()
    return st


def check(run: Run, wl: Workload, nodes: list[str] | None = None) -> None:
    nodes = nodes or [n for n in run.fleet.members() if agent(n).status()]
    try:
        v = run_checker(run.run_id, wl.ack_log, nodes, run.dir)
    except Exception as e:  # noqa: BLE001
        note(run, f"checker failed: {e}")
        return
    run.row.update(checker_fields(v))
    run.row["checker_nodes"] = nodes


def match_failover(evs: list[dict], old: str | None) -> dict | None:
    for e in evs:
        if e.get("type") in ("failover", "switchover") and (old is None or e.get("old_primary") in (old, None)):
            return e
    return None


def start_workload(run: Run, duration: float) -> Workload:
    wl = Workload(run.dir, run.run_id, haproxy_port(run.opts.rs), run.opts.clients, duration)
    wl.start()
    return wl


def wait_until_writing(wl: Workload, seconds: float) -> None:
    """Sleep `seconds` after start, but fail fast if the workload is not acknowledging writes."""
    t_end = (wl.t_start or time.time()) + seconds
    while time.time() < t_end:
        if not wl.alive():
            raise RuntimeError(f"workload exited early: {wl.err.read_text()[-800:] if wl.err.exists() else ''}")
        time.sleep(0.2)
    if wl.acks_so_far() == 0:
        raise RuntimeError("workload acknowledged no writes before injection")


def rejoin_after_restart(run: Run, old: str, t_restart: float, timeout: float = 240.0) -> None:
    f = run.fleet
    if f.mode == "orchestrator":
        deadline = time.time() + timeout
        while time.time() < deadline:
            s = agent(old).status()
            if s and s.get("mysqld_responsive"):
                break
            time.sleep(1)
        acts = [a for a in f.harness_repair() if a.get("node") == old]
        if acts:
            a = acts[0]
            run.row["rejoin"] = {"branch": "harness-" + a["branch"], "phantom_gtids": a["phantom_gtids"],
                                 "duration_s": time.time() - t_restart}
            note(run, "orchestrator does not rejoin a dead primary, the harness did it through the agents")
        return
    ev = f.wait_event(t_restart - 1, {"rejoin", "rebuild"}, timeout,
                      lambda e: rejoin_matches(e, old))
    others = [e for e in f.events(t_restart - 1) if e.get("type") == "rejoin"
              and (e.get("rejoin") or {}).get("branch") == "none"]
    if others:
        run.row["second_writer_events"] = others
    if ev is None:
        note(run, f"no rejoin event for {old} within {timeout:.0f}s")
        return
    rj = dict(ev.get("rejoin") or {})
    rj.setdefault("branch", ev.get("type"))
    rj.setdefault("phantom_gtids", 0)
    if rj.get("duration_s") is None:
        rj["duration_s"] = float(ev["ts"]) - t_restart
    rj["since_restart_s"] = float(ev["ts"]) - t_restart
    run.row["rejoin"] = rj
    run.row["rejoin_event"] = ev


def rejoin_matches(e: dict, node: str) -> bool:
    """A finished rejoin of `node`: branch repoint, rebuild or manual (branch none is the
    manager fencing a second writer, not a rejoin)."""
    rj = e.get("rejoin") or {}
    who = rj.get("node") or e.get("old_primary")
    return who == node and rj.get("branch") in ("repoint", "rebuild", "manual")


def woken_writes(run: Run, woken: str, new_primary: str, t_wake: float, watch_s: float = 15.0) -> None:
    """After SIGCONT: poll the woken node for `watch_s` and record the worst case seen."""
    worst = 0
    worst_post = 0
    http200 = False
    sro0 = False
    samples = 0
    deadline = time.time() + watch_s
    while time.time() < deadline:
        if agent(woken).primary_code() == 200:
            http200 = True
        try:
            v = sql(woken, "SELECT @@GLOBAL.super_read_only", timeout=2)[0][0]
            if int(v) == 0:
                sro0 = True
            rows_w = set(sql(woken, "SELECT client_id, seq, UNIX_TIMESTAMP(ts) FROM chaos.writes "
                                    "WHERE run_id=%s", (run.run_id,), timeout=5))
            rows_p = {(c, s) for c, s in sql(new_primary, "SELECT client_id, seq FROM chaos.writes "
                                                          "WHERE run_id=%s", (run.run_id,), timeout=5)}
            missing = [(c, s, t) for c, s, t in rows_w if (c, s) not in rows_p]
            worst = max(worst, len(missing))
            worst_post = max(worst_post, sum(1 for _, _, t in missing if float(t) >= t_wake - 0.5))
            samples += 1
        except Exception:  # noqa: BLE001  (mysqld may be restarting after a kill fence)
            pass
        time.sleep(0.5)
    run.row["writes_on_woken_primary"] = worst
    run.row["woken"] = {"node": woken, "samples": samples, "missing_on_new_primary": worst,
                        "written_after_wake": worst_post, "primary_200_seen": http200,
                        "super_read_only_0_seen": sro0}
    st = agent(woken).status() or {}
    try:
        run.row["woken"]["super_read_only_now"] = int(sql(woken, "SELECT @@GLOBAL.super_read_only")[0][0])
    except Exception:  # noqa: BLE001
        run.row["woken"]["super_read_only_now"] = None
    run.row["woken"]["primary_code_now"] = agent(woken).primary_code()
    run.row["woken"]["fenced_now"] = st.get("fenced")


# ---------------------------------------------------------------- scenarios

def _failover_common(run: Run, inject: Callable[[str], None], *, extra_s: float = 0.0,
                     after_failover: Callable[[str, str | None], None] | None = None,
                     restart_old: bool = True) -> None:
    o, f = run.opts, run.fleet
    old = f.primary()
    if not old:
        raise RuntimeError("no primary before injection")
    run.row["primary_at_inject"] = old
    wl = start_workload(run, o.workload_seconds + extra_s)
    wait_until_writing(wl, o.inject_after)
    inject_ts = time.time()
    run.row["inject_ts"] = inject_ts
    inject(old)
    new, t_new = f.wait_new_primary(old, 90.0)
    run.row["new_primary"] = new
    if new is None:
        note(run, "no new primary within 90 s")
    else:
        run.row["manager_new_primary_s"] = t_new - inject_ts
    if after_failover:
        after_failover(old, new)
    wl.wait()
    apply_acks(run, wl, inject_ts)
    ev = match_failover(f.events(inject_ts - 1), old)
    run.row["event"] = ev
    if ev is None and new:
        note(run, "primary changed but no failover event found")
    check(run, wl, [n for n in f.members() if n != old and agent(n).status()])
    if restart_old:
        t_restart = time.time()
        if not docker.is_running(old):
            docker.up(old)
        rejoin_after_restart(run, old, t_restart)


def sc_kill(run: Run) -> None:
    _failover_common(run, lambda p: docker.kill(p, "KILL"))


def sc_kill_two(run: Run) -> None:
    f = run.fleet
    victims: list[str] = []

    def inject(p: str) -> None:
        reps = [n for n in f.members() if n != p]
        ss = f.statuses(reps)
        big = max(reps, key=lambda n: gtid_count(((ss.get(n) or {}).get("replica") or {}).get("retrieved_gtid_set")))
        victims[:] = [p, big]
        run.row["killed"] = victims
        ts = [threading.Thread(target=docker.kill, args=(v, "KILL")) for v in victims]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

    def after(old: str, new: str | None) -> None:
        pass

    _failover_common(run, inject, after_failover=after, restart_old=False)
    t_restart = time.time()
    down = [v for v in victims if not docker.is_running(v)]
    if down:
        docker.up(*down)
    rejoin_after_restart(run, victims[0], t_restart)
    note(run, "semi-sync with wait_for_replica_count=1 does not cover two simultaneous losses; "
              "lost_acked_writes here is the published boundary of the guarantee")


def sc_hang_container(run: Run) -> None:
    o = run.opts

    def after(old: str, new: str | None) -> None:
        wake_at = run.row["inject_ts"] + o.hang_seconds
        while time.time() < wake_at:
            time.sleep(0.5)
        thaw(old)
        t_wake = time.time()
        run.row["wake_ts"] = t_wake
        if new:
            woken_writes(run, old, new, t_wake)

    def inject(p: str) -> None:
        freeze(p)
        note(run, "hang mechanism: docker pause (cgroup freezer, every process in the container)")
        # the manager must see a silent agent, not just a silent mysqld
        t_end = time.time() + 3
        answered = False
        while time.time() < t_end:
            if agent(p).status() is not None:
                answered = True
            time.sleep(0.5)
        run.row["agent_answered_while_frozen"] = answered
        cp = docker.exec_(p, ["true"], check=False, timeout=10)
        run.row["docker_exec_while_frozen_rc"] = cp.returncode

    _failover_common(run, inject, extra_s=o.hang_seconds + 20, after_failover=after,
                     restart_old=False)
    rejoin_after_restart(run, run.row["primary_at_inject"], run.row.get("wake_ts") or time.time())


def sc_hang_process(run: Run) -> None:
    o = run.opts
    pid: list[int] = []

    def inject(p: str) -> None:
        st = agent(p).status() or {}
        mp = st.get("mysqld_pid") or docker.pid_of_mysqld(p)
        if not mp:
            raise RuntimeError(f"cannot find mysqld pid in {p}")
        pid.append(int(mp))
        run.row["mysqld_pid"] = int(mp)
        docker.signal_pid(p, int(mp), "STOP")
        note(run, f"hang mechanism: docker exec kill -STOP {mp} (mysqld only, agent alive)")

    def after(old: str, new: str | None) -> None:
        wake_at = run.row["inject_ts"] + o.hang_seconds
        while time.time() < wake_at:
            time.sleep(0.5)
        docker.signal_pid(old, pid[0], "CONT")
        t_wake = time.time()
        run.row["wake_ts"] = t_wake
        now_pid = (agent(old).status() or {}).get("mysqld_pid")
        run.row["mysqld_restarted"] = now_pid is not None and now_pid != pid[0]
        if new:
            woken_writes(run, old, new, t_wake)

    _failover_common(run, inject, extra_s=o.hang_seconds + 20, after_failover=after,
                     restart_old=False)
    ev = run.row.get("event") or {}
    run.row["fence_method"] = ((ev.get("steps") or {}).get("fence") or {}).get("outcome")
    rejoin_after_restart(run, run.row["primary_at_inject"], run.row.get("wake_ts") or time.time())


def sc_partition_replicas(run: Run) -> None:
    f = run.fleet
    parts: dict[str, list[str]] = {}

    def inject(p: str) -> None:
        reps = [n for n in f.members() if n != p]
        parts[p] = reps
        for r in reps:
            docker.iptables_drop(p, r, "both")

    def after(old: str, new: str | None) -> None:
        # heal only once the old primary is fenced (or 60 s passed)
        deadline = time.time() + 60
        while time.time() < deadline:
            s = agent(old).status() or {}
            if s.get("fenced") or s.get("super_read_only") is True:
                break
            time.sleep(0.5)
        run.row["old_primary_fenced"] = bool((agent(old).status() or {}).get("fenced"))
        docker.iptables_flush(old)
        run.row["heal_ts"] = time.time()

    _failover_common(run, inject, after_failover=after, restart_old=False)
    rejoin_after_restart(run, run.row["primary_at_inject"], run.row.get("heal_ts") or time.time())


def sc_partition_manager(run: Run) -> None:
    o, f = run.opts, run.fleet
    watcher = ORCH_CONTAINER if o.mode == "orchestrator" else MANAGER_CONTAINER
    p = f.primary()
    if not p:
        raise RuntimeError("no primary")
    run.row["primary_at_inject"] = p
    wl = start_workload(run, o.inject_after + o.partition_hold_s + 15)
    wait_until_writing(wl, o.inject_after)
    inject_ts = time.time()
    run.row["inject_ts"] = inject_ts
    docker.iptables_drop(watcher, p, "both")
    end = inject_ts + o.partition_hold_s
    seen_primaries = set()
    while time.time() < end:
        seen_primaries.update(f.agent_primaries())
        time.sleep(1.0)
    docker.iptables_flush(watcher)
    time.sleep(10)  # grace: a failover decided at the end of the window still counts
    seen_primaries.update(f.agent_primaries())
    wl.wait()
    apply_acks(run, wl, inject_ts)
    evs = [e for e in f.events(inject_ts - 1) if e.get("type") in ("failover", "switchover")]
    now_p = f.primary()
    run.row["false_failover"] = bool(evs) or (now_p is not None and now_p != p) or seen_primaries - {p} != set()
    run.row["event"] = evs[0] if evs else None
    run.row["new_primary"] = now_p
    run.row["states_seen"] = sorted({e.get("type") for e in f.events(inject_ts - 1)})
    run.row["failover_s"] = None if not run.row["false_failover"] else run.row["failover_s"]
    check(run, wl)
    if run.row["errors"]:
        note(run, f"workload saw {run.row['errors']} errors during a manager-only partition")


def _set_semisync(f: Fleet, on: bool) -> dict:
    res = {}
    for n in f.members():
        try:
            code, body = agent(n).post("/configure", {"semisync": on}, timeout=10)
            res[n] = code
        except ApiError as e:
            res[n] = str(e)
    return res


def sc_cost(run: Run) -> None:
    """One variant per run row. Variants are expanded by the caller."""
    o, f = run.opts, run.fleet
    ss_on = (o.semisync or "on") == "on"
    netem = float(o.netem or 0)
    run.row.update({"semisync": ss_on, "netem_ms": netem, "workload_s": o.cost_seconds})
    p = f.primary()
    run.row["primary_at_inject"] = p
    reps = [n for n in f.members() if n != p]
    try:
        if not ss_on:
            run.row["configure"] = _set_semisync(f, False)
        if netem:
            for r in reps:
                docker.netem_delay(r, netem)
        time.sleep(2)
        st = agent(p).status() or {}
        run.row["semisync_effective"] = (st.get("semisync") or {}).get("source_status")
        wl = start_workload(run, o.cost_seconds)
        wl.wait()
        apply_acks(run, wl, None)
        # steady state: drop the first 2 s of connection warm-up from the rate
        oks, _ = read_ack_log(wl.ack_log)
        ts = sorted(float(r["t_ok"]) for r in oks)
        if ts:
            warm = [t for t in ts if t >= ts[0] + 2]
            if len(warm) > 1:
                run.row["writes_per_s"] = len(warm) / (warm[-1] - warm[0])
    finally:
        if netem:
            for r in reps:
                docker.netem_clear(r)
        if not ss_on and o.mode != "naive":
            _set_semisync(f, True)
    run.row["errors"] = run.row.get("errors", 0)
    check(run, wl)


def grow_dataset(f: Fleet, mb: int) -> float:
    p = f.primary()
    have = sql(p, "SELECT COALESCE(SUM(LENGTH(data)),0) FROM chaos.blob")[0][0]
    need = int(mb * 1_000_000) - int(have)
    if need <= 0:
        return int(have) / 1e6
    rows = need // 4096 + 1
    log(f"growing chaos.blob by {need / 1e6:.0f} MB ({rows} rows) on {p}")
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=mysql_port(p), user="root", password="root",
                           autocommit=True, read_timeout=120, write_timeout=120, ssl=insecure_tls())
    try:
        with conn.cursor() as cur:
            batch = 200
            payload = os.urandom(4096)
            for i in range(0, rows, batch):
                n = min(batch, rows - i)
                cur.execute("INSERT INTO chaos.blob (data) VALUES " + ",".join(["(%s)"] * n),
                            [payload] * n)
    finally:
        conn.close()
    return (int(have) + rows * 4096) / 1e6


def _find_bytes(obj: Any) -> int | None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("bytes", "clone_bytes") and isinstance(v, (int, float)):
                return int(v)
            b = _find_bytes(v)
            if b is not None:
                return b
    return None


def sc_replica_loss(run: Run) -> None:
    o, f = run.opts, run.fleet
    run.row["dataset_mb"] = grow_dataset(f, o.data_mb)
    p = f.primary()
    run.row["primary_at_inject"] = p
    r1, r2 = [n for n in f.members() if n != p][:2]
    run.row["killed"] = [r1, r2]
    wl = start_workload(run, o.inject_after + 70 + o.stall_hold_s)
    wait_until_writing(wl, o.inject_after)
    inject_ts = time.time()
    run.row["inject_ts"] = inject_ts
    # phase 1: one replica down -> DEGRADED, writes continue
    docker.kill(r1, "KILL")
    st = f.wait_state({"DEGRADED"}, 30)
    run.row["degraded_seen"] = st is not None
    time.sleep(5)
    oks, _ = read_ack_log(wl.ack_log)
    run.row["writes_while_degraded"] = sum(1 for r in oks if float(r["t_ok"]) > inject_ts + 3)
    # phase 2: survivor down -> writes stall (semi-sync timeout is an hour)
    t_kill2 = time.time()
    run.row["kill2_ts"] = t_kill2
    docker.kill(r2, "KILL")
    stall_ev = f.wait_event(t_kill2 - 1, {"stall"}, 30)
    run.row["stall_event"] = stall_ev is not None
    time.sleep(max(0.0, o.stall_hold_s - (time.time() - t_kill2)))
    # phase 3: restore one replica -> writes resume
    t_restore = time.time()
    run.row["restore_ts"] = t_restore
    docker.up(r1)
    deadline = time.time() + 90
    resumed = None
    while time.time() < deadline:
        oks, _ = read_ack_log(wl.ack_log)
        after = [float(r["t_ok"]) for r in oks if float(r["t_ok"]) > t_restore]
        if after:
            resumed = min(after)
            break
        time.sleep(0.5)
    run.row["resume_s"] = (resumed - t_restore) if resumed else None
    oks, _ = read_ack_log(wl.ack_log)
    before = [float(r["t_ok"]) for r in oks if float(r["t_ok"]) <= t_restore]
    last_before = max(before) if before else None
    run.row["stall_s"] = (resumed - last_before) if (resumed and last_before) else None
    wl.stop()
    stall_s, resume_s = run.row["stall_s"], run.row["resume_s"]
    apply_acks(run, wl, inject_ts)
    run.row["stall_s"], run.row["resume_s"] = stall_s, resume_s
    run.row["failover_s"] = None
    check(run, wl, [n for n in [p, r1] if agent(n).status()])
    # phase 4: stay DEGRADED past rebuild_after_s, the manager provisions and clones the spare
    rebuild_after = float(load_knobs().get("rebuild_after_s") or 60)
    ev = f.wait_event(t_restore - 1, {"replace"}, rebuild_after + 300,
                      lambda e: bool(e.get("clone")) or "failed" in (e.get("note") or ""))
    run.row["event"] = ev
    if ev:
        c = ev.get("clone") or {}
        b = c.get("bytes") or _find_bytes(ev)
        dur = c.get("duration_s") or (ev.get("rejoin") or {}).get("duration_s") or ev.get("total_s")
        run.row["clone"] = {"bytes": b, "duration_s": dur, "donor": c.get("donor"),
                            "mb_per_s": c.get("mb_per_s") or ((b / 1e6 / dur) if (b and dur) else None)}
    else:
        note(run, "no replace event")
    # restore: bring the original replica back, then drop the spare
    docker.up(r2)
    t_back = time.time()
    rejoin_after_restart(run, r2, t_back, timeout=120)
    spare = spare_node(o.rs)
    if docker.container_status(spare):
        vols = docker.volumes_of(spare)
        docker.rm(spare)
        docker.rm_volume(*vols)


def sc_switchover(run: Run) -> None:
    o, f = run.opts, run.fleet
    old = f.primary()
    if not old:
        raise RuntimeError("no primary")
    run.row["primary_at_inject"] = old
    wl = start_workload(run, o.workload_seconds)
    wait_until_writing(wl, o.inject_after)
    inject_ts = time.time()
    run.row["inject_ts"] = inject_ts
    if o.mode == "orchestrator":
        r = orch_get(f"/api/graceful-master-takeover-auto/{o.rs}", timeout=60)
        run.row["event"] = {"orchestrator": r}
    else:
        try:
            r = f.mc.failover(o.rs, None, timeout=60)
            run.row["event"] = r.get("event") if isinstance(r, dict) else None
        except ApiError as e:
            note(run, f"switchover API failed: {e}")
    run.row["switchover_api_s"] = time.time() - inject_ts
    new, _ = f.wait_new_primary(old, 30)
    run.row["new_primary"] = new
    wl.wait()
    st = apply_acks(run, wl, inject_ts, window_end=inject_ts + run.row["switchover_api_s"] + 5)
    run.row["errors"] = st.errors_after_inject
    run.row["failover_s"] = None  # not a failure; the stall is the metric
    check(run, wl)


def sc_disk_full(run: Run) -> None:
    # TODO(disk-full): needs a small per-node tmpfs for the binlog dir in
    # deploy/docker-compose.yml (fleet-owned) and log_bin pointing into it in my.cnf.
    # Filling the datadir volume would fill the Docker VM disk that every container shares.
    raise NotImplementedError("disk-full needs a per-node binlog tmpfs in the fleet compose file; "
                              "see docs/INTERFACES.md 'disk-full'")


SCENARIO_FN: dict[str, Callable[[Run], None]] = {
    "kill": sc_kill, "hang-container": sc_hang_container, "hang-process": sc_hang_process,
    "partition-replicas": sc_partition_replicas, "partition-manager": sc_partition_manager,
    "cost": sc_cost, "replica-loss": sc_replica_loss, "switchover": sc_switchover,
    "disk-full": sc_disk_full, "kill-two": sc_kill_two,
}


# ---------------------------------------------------------------- driver

def environment(rs: str) -> dict:
    f = Fleet(rs, "dbguard")
    p = f.primary() or set_nodes(rs)[0]
    return {"host": docker.host_description(), "docker_version": docker.docker_version(),
            "mysql_version": docker.mysql_version(port=mysql_port(p)), "knobs": load_knobs()}


def verify_mode(mode: str) -> None:
    if mode == "orchestrator":
        return
    try:
        st = ManagerClient(timeout=3).status()
    except ApiError as e:
        raise SystemExit(f"manager unreachable: {e}")
    if st.get("mode") != mode:
        raise SystemExit(f"the fleet runs in mode {st.get('mode')!r}, not {mode!r}. Restart it in "
                         f"{mode} mode first (fleet.yaml mode and DBGUARD_SEMISYNC on every node).")


def rs2_changes(f: Fleet, since: float, other: str) -> int:
    if f.mode == "orchestrator":
        return 0 if len(Fleet(other, "orchestrator").agent_primaries()) == 1 else 1
    try:
        return sum(1 for e in f.mc.events(other, since) if e.get("type") in STATE_EVENT_TYPES
                   and float(e.get("ts") or 0) >= since)
    except ApiError:
        return -1


def run_once(opts: Opts, env: dict, i: int) -> dict:
    run_id = f"{opts.scenario}-{opts.mode}-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{i}-{uuid.uuid4().hex[:4]}"
    d = TMP / run_id
    d.mkdir(parents=True, exist_ok=True)
    f = Fleet(opts.rs, opts.mode)
    run = Run(opts=opts, fleet=f, run_id=run_id, dir=d)
    docker.set_command_log(run.commands)
    run.row = base_row(run, env)
    other = "rs2" if opts.rs != "rs2" else "rs1"
    other_p = Fleet(other, opts.mode).primary()
    rs2_wl = None
    t0 = time.time()
    log(f"=== run {i + 1}/{opts.runs} {run_id}")
    try:
        if opts.rs2_workload:
            rs2_wl = Workload(d, run_id, haproxy_port(other), 2, 10_000, tag="rs2")
            rs2_wl.start()
        SCENARIO_FN[opts.scenario](run)
    finally:
        if rs2_wl:
            rs2_wl.stop()
            oks, errs = read_ack_log(rs2_wl.ack_log)
            run.row["rs2_workload"] = {"acked": len(oks), "errors": len(errs)}
        run.row["rs2_state_changes"] = rs2_changes(f, t0, other)
        if Fleet(other, opts.mode).primary() not in (other_p, None) and run.row["rs2_state_changes"] == 0:
            run.row["rs2_state_changes"] = 1
        extra = [MANAGER_CONTAINER] if opts.mode != "orchestrator" and opts.scenario == "partition-manager" else []
        if opts.mode == "orchestrator":
            extra.append(ORCH_CONTAINER)
        for c in list(_frozen):
            thaw(c)
        run.row["heal"] = f.heal(opts.heal_timeout, extra_fault_containers=extra)
        run.row["duration_s"] = time.time() - t0
        run.row["commands"] = run.commands
        docker.set_command_log(None)
        (d / "row.json").write_text(json.dumps(run.row, indent=2, default=str))
    return run.row


def cleanup_run_files(run_id: str) -> None:
    """Keep only the JSON row (disk is scarce). DBGUARD_KEEP_ACKS=1 keeps everything."""
    if os.environ.get("DBGUARD_KEEP_ACKS") == "1":
        return
    for p in [TMP / f"{run_id}.ack.jsonl", *(TMP / run_id).glob("*.ack.jsonl"),
              *(TMP / run_id).glob("*.stdout"), *(TMP / run_id).glob("*.stderr")]:
        try:
            if p.name.startswith("checker") and p.stat().st_size < 200_000:
                continue
            p.unlink()
        except FileNotFoundError:
            pass


def expand_cost(opts: Opts) -> list[Opts]:
    from dataclasses import replace
    if not opts.cost_all:
        return [opts]
    ss = ["on", "off"] if opts.mode == "dbguard" else ["off"]
    return [replace(opts, semisync=s, netem=n) for s in ss for n in (0, 2, 20)]


def run_scenario(opts: Opts) -> int:
    out = opts.out or RESULTS / f"{opts.scenario}_{opts.mode}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    TMP.mkdir(parents=True, exist_ok=True)
    env = environment(opts.rs)
    failures = 0
    variants = expand_cost(opts) if opts.scenario == "cost" else [opts]
    f = Fleet(opts.rs, opts.mode)
    if not opts.skip_heal_before:
        f.heal(opts.heal_timeout)
    for v in variants:
        for i in range(v.runs):
            try:
                row = run_once(v, env, i)
            except Exception as e:  # noqa: BLE001
                failures += 1
                err = {"scenario": v.scenario, "mode": v.mode, "ts": time.time(), "error": repr(e),
                       "traceback": traceback.format_exc()}
                with open(out.with_suffix(".errors.jsonl"), "a") as fh:
                    fh.write(json.dumps(err) + "\n")
                log(f"run failed: {e!r}")
                traceback.print_exc()
                if not opts.keep_going:
                    return 1
                continue
            with open(out, "a") as fh:
                fh.write(json.dumps(row, default=str) + "\n")
            cleanup_run_files(row["run_id"])
            log("row: " + json.dumps({k: row.get(k) for k in (
                "run_id", "failover_s", "stall_s", "acked_writes", "lost_acked_writes",
                "phantom_writes", "single_writer_violations", "converged", "false_failover",
                "writes_on_woken_primary", "rejoin", "errors", "commit_p50_ms", "commit_p99_ms",
                "writes_per_s", "clone", "rs2_state_changes", "notes")}, default=str))
    return 1 if failures else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="chaos", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--scenario", choices=SCENARIOS)
    g.add_argument("--all", action="store_true", help="every scenario (and every cost variant)")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--mode", choices=MODES, default="dbguard")
    ap.add_argument("--rs", default="rs1")
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--workload-seconds", type=float, default=40.0)
    ap.add_argument("--inject-after", type=float, default=10.0)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--keep-going", action="store_true")
    ap.add_argument("--hang-seconds", type=float, default=90.0)
    ap.add_argument("--partition-hold", type=float, default=30.0)
    ap.add_argument("--cost-seconds", type=float, default=60.0)
    ap.add_argument("--semisync", choices=["on", "off"], default=None, help="cost: semi-sync variant")
    ap.add_argument("--netem", type=float, default=None, help="cost: delay in ms on the replicas")
    ap.add_argument("--cost-all", action="store_true", help="cost: every semisync x netem variant")
    ap.add_argument("--data-mb", type=int, default=200, help="replica-loss: dataset size for clone")
    ap.add_argument("--rs2-workload", action="store_true", help="2-client workload on rs2 too")
    ap.add_argument("--stall-hold", type=float, default=10.0)
    ap.add_argument("--heal-timeout", type=float, default=120.0)
    ap.add_argument("--no-heal-before", action="store_true")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    verify_mode(a.mode)
    scenarios = ALL_ORDER if a.all else [a.scenario]
    common = dict(runs=a.runs, mode=a.mode, rs=a.rs, clients=a.clients,
                  workload_seconds=a.workload_seconds, inject_after=a.inject_after,
                  keep_going=a.keep_going or a.all, hang_seconds=a.hang_seconds,
                  partition_hold_s=a.partition_hold, cost_seconds=a.cost_seconds,
                  semisync=a.semisync, netem=a.netem, cost_all=a.cost_all or a.all,
                  data_mb=a.data_mb, rs2_workload=a.rs2_workload, stall_hold_s=a.stall_hold,
                  heal_timeout=a.heal_timeout, skip_heal_before=a.no_heal_before)
    if a.mode == "orchestrator":
        orch_setup([a.rs])
    rc = 0
    try:
        for sc in scenarios:
            if a.mode == "orchestrator" and sc in ("cost", "replica-loss"):
                log(f"skip {sc} in orchestrator mode (no manager, nothing to compare)")
                continue
            o = Opts(scenario=sc, out=a.out if not a.all else None, **common)
            rc |= run_scenario(o)
    finally:
        for c in list(_frozen):
            thaw(c)
        if a.mode == "orchestrator":
            orch_teardown()
    return rc
