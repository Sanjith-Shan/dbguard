"""Thin subprocess helpers around `docker` and `docker compose` for the chaos harness.

Every mutation logs the exact command line to stderr (and to the optional per-run
command log) so a run can be replayed by hand.

Network faults (iptables, tc netem) are injected from a helper container that joins the
target container's network namespace (`docker run --net container:<c> --cap-add NET_ADMIN`).
That is the same kernel state as running iptables inside the target, but it does not depend
on the target image shipping iptables or tc, and it works for the manager and the
Orchestrator images too.

Freezing a container: `docker kill -s STOP <c>` delivers SIGSTOP to PID 1 only (tini), so
the agent and mysqld keep running. `freeze()` therefore uses `docker pause` (cgroup freezer),
which stops every process in the container.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
COMPOSE_FILE = Path(os.environ.get("DBGUARD_COMPOSE_FILE", REPO / "deploy" / "docker-compose.yml"))
NETTOOLS_IMAGE = "dbguard-nettools:1"
NETTOOLS_DOCKERFILE = "FROM alpine:3.20\nRUN apk add --no-cache iptables iproute2 iproute2-tc\n"

_cmd_log: list[str] | None = None


def set_command_log(buf: list[str] | None) -> None:
    """Collect every mutating command into `buf` (the chaos runner stores it per run)."""
    global _cmd_log
    _cmd_log = buf


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S", time.localtime()) + f".{int(time.time() * 1000) % 1000:03d}"
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)


def _run(args: list[str], *, check: bool = True, mutate: bool = True, timeout: float = 120,
         input: str | None = None) -> subprocess.CompletedProcess:
    line = shlex.join(args)
    if mutate:
        log(f"$ {line}")
        if _cmd_log is not None:
            _cmd_log.append(f"{time.time():.3f} {line}")
    cp = subprocess.run(args, capture_output=True, text=True, timeout=timeout, input=input)
    if check and cp.returncode != 0:
        raise DockerError(f"{line} -> rc={cp.returncode}: {cp.stderr.strip() or cp.stdout.strip()}")
    return cp


class DockerError(RuntimeError):
    pass


def compose_args(*extra: str, profiles: tuple[str, ...] = ()) -> list[str]:
    args = ["docker", "compose", "-f", str(COMPOSE_FILE)]
    for p in profiles:
        args += ["--profile", p]
    return args + list(extra)


# ---------------------------------------------------------------- signals and lifecycle

def kill(container: str, signal: str = "KILL") -> None:
    _run(["docker", "kill", "-s", signal, container])


def freeze(container: str) -> None:
    """Freeze every process in the container with the cgroup freezer (`docker pause`).

    `docker kill -s STOP` would reach PID 1 (tini) only and leave the agent and mysqld
    running. The freezer stops every process at once, which is what SIGSTOP on all of them
    would look like from outside: TCP connects still complete in the kernel, nothing answers.
    """
    _run(["docker", "pause", container])


def thaw(container: str) -> None:
    _run(["docker", "unpause", container], check=False)


pause = freeze
unpause = thaw


def exec_(container: str, cmd: list[str] | str, *, check: bool = True, user: str | None = "0",
          timeout: float = 120, mutate: bool = True) -> subprocess.CompletedProcess:
    if isinstance(cmd, str):
        cmd = ["sh", "-c", cmd]
    args = ["docker", "exec"] + (["-u", user] if user else []) + [container] + cmd
    return _run(args, check=check, timeout=timeout, mutate=mutate)


exec = exec_


def env_of(container: str) -> dict[str, str]:
    cp = _run(["docker", "inspect", "-f", "{{json .Config.Env}}", container], check=False,
              mutate=False)
    if cp.returncode != 0:
        return {}
    return dict(e.split("=", 1) for e in json.loads(cp.stdout or "[]") if "=" in e)


def pin_compose_env(nodes: list[str], manager: str = "dbguard") -> dict[str, str]:
    """Make later `docker compose up` calls reproduce the running fleet's mode. Compose
    interpolates DBGUARD_SEMISYNC and DBGUARD_MODE from the caller's environment, so a node
    restarted from a shell without them would come back with the defaults (semi-sync on)."""
    pinned = {}
    for n in nodes:
        v = env_of(n).get("DBGUARD_SEMISYNC")
        if v is not None:
            pinned["DBGUARD_SEMISYNC"] = v
            break
    menv = env_of(manager)
    if menv.get("DBGUARD_MODE"):
        pinned["DBGUARD_MODE"] = menv["DBGUARD_MODE"]
    for k, v in pinned.items():
        if os.environ.get(k) not in (None, v):
            log(f"WARNING {k}={os.environ[k]} in the environment but the fleet runs {k}={v}, using {v}")
        os.environ[k] = v
    return pinned


def up(*services: str, profiles: tuple[str, ...] = (), wait: bool = False, deps: bool = False) -> None:
    extra = ["up", "-d"] + ([] if deps else ["--no-deps"]) + (["--wait"] if wait else []) + list(services)
    _run(compose_args(*extra, profiles=profiles), timeout=600)


def stop(*services: str, profiles: tuple[str, ...] = ()) -> None:
    _run(compose_args("stop", *services, profiles=profiles), timeout=180)


def start(*services: str, profiles: tuple[str, ...] = ()) -> None:
    _run(compose_args("start", *services, profiles=profiles), timeout=180)


def rm(*services: str, profiles: tuple[str, ...] = ()) -> None:
    _run(compose_args("rm", "-sfv", *services, profiles=profiles), timeout=180)


def volumes_of(container: str) -> list[str]:
    cp = _run(["docker", "inspect", "-f", "{{json .Mounts}}", container], check=False, mutate=False)
    if cp.returncode != 0:
        return []
    return [m["Name"] for m in json.loads(cp.stdout or "[]") if m.get("Type") == "volume"]


def rm_volume(*names: str) -> None:
    for n in names:
        _run(["docker", "volume", "rm", "-f", n], check=False)


def is_running(container: str) -> bool:
    cp = _run(["docker", "inspect", "-f", "{{.State.Running}} {{.State.Paused}}", container],
              check=False, mutate=False)
    return cp.returncode == 0 and cp.stdout.strip().startswith("true")


def container_status(container: str) -> str | None:
    cp = _run(["docker", "inspect", "-f", "{{.State.Status}}", container], check=False, mutate=False)
    return cp.stdout.strip() if cp.returncode == 0 else None


def container_ip(container: str) -> str:
    cp = _run(["docker", "inspect", "-f",
               "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", container], mutate=False)
    ips = cp.stdout.split()
    if not ips:
        raise DockerError(f"{container} has no IP (not running?)")
    return ips[0]


def pid_of_mysqld(container: str) -> int | None:
    """PID of mysqld inside the container's PID namespace."""
    cp = exec_(container, ["pgrep", "-x", "mysqld"], check=False, mutate=False)
    if cp.returncode == 0 and cp.stdout.strip():
        return int(cp.stdout.split()[0])
    # images without procps: scan /proc
    cp = exec_(container, "for p in /proc/[0-9]*; do [ \"$(cat $p/comm 2>/dev/null)\" = mysqld ] "
               "&& echo ${p#/proc/}; done", check=False, mutate=False)
    pids = cp.stdout.split()
    return int(pids[0]) if pids else None


def signal_pid(container: str, pid: int, sig: str) -> None:
    exec_(container, ["kill", f"-{sig}", str(pid)], check=False)


# ---------------------------------------------------------------- network faults

def _ensure_nettools() -> None:
    cp = _run(["docker", "image", "inspect", NETTOOLS_IMAGE], check=False, mutate=False)
    if cp.returncode != 0:
        _run(["docker", "build", "-q", "-t", NETTOOLS_IMAGE, "-"], input=NETTOOLS_DOCKERFILE,
             timeout=600)


def netns(container: str, script: str, *, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell script with NET_ADMIN inside `container`'s network namespace."""
    _ensure_nettools()
    return _run(["docker", "run", "--rm", "--net", f"container:{container}", "--cap-add",
                 "NET_ADMIN", NETTOOLS_IMAGE, "sh", "-c", script], check=check)


def iptables_drop(container: str, dst: str, direction: str = "both") -> None:
    """Drop traffic between `container` and `dst` (IP or container name)."""
    ip = dst if dst.replace(".", "").isdigit() else container_ip(dst)
    rules = []
    if direction in ("out", "both"):
        rules.append(f"iptables -A OUTPUT -d {ip} -j DROP")
    if direction in ("in", "both"):
        rules.append(f"iptables -A INPUT -s {ip} -j DROP")
    netns(container, " && ".join(rules))


def iptables_flush(container: str) -> None:
    netns(container, "iptables -F INPUT; iptables -F OUTPUT", check=False)


def netem_delay(container: str, ms: float, dev: str = "eth0") -> None:
    netns(container, f"tc qdisc replace dev {dev} root netem delay {ms}ms")


def netem_clear(container: str, dev: str = "eth0") -> None:
    netns(container, f"tc qdisc del dev {dev} root 2>/dev/null; true", check=False)


def clear_net(container: str, dev: str = "eth0") -> None:
    """Flush iptables INPUT/OUTPUT and remove any root qdisc, in one helper run."""
    netns(container, f"iptables -F INPUT; iptables -F OUTPUT; tc qdisc del dev {dev} root "
                     "2>/dev/null; true", check=False)


def fault_state(container: str) -> str:
    """iptables rules and qdisc, for diagnostics."""
    cp = netns(container, "iptables -S; tc qdisc show dev eth0", check=False)
    return cp.stdout


# ---------------------------------------------------------------- versions

def docker_version() -> str:
    cp = _run(["docker", "version", "--format", "{{.Server.Platform.Name}}|{{.Server.Version}}"],
              check=False, mutate=False)
    plat, _, ver = cp.stdout.strip().partition("|")
    return f"{plat} (engine {ver})" if plat else ver


def host_description() -> str:
    def sh(*a: str) -> str:
        try:
            return subprocess.run(list(a), capture_output=True, text=True, timeout=5).stdout.strip()
        except Exception:
            return ""
    cpu = sh("sysctl", "-n", "machdep.cpu.brand_string") or sh("uname", "-m")
    ncpu = sh("sysctl", "-n", "hw.ncpu") or str(os.cpu_count())
    mem = sh("sysctl", "-n", "hw.memsize")
    mem_s = f"{int(mem) / 2**30:.0f}GB" if mem.isdigit() else "?"
    info = _run(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"], check=False, mutate=False)
    vm = ""
    if info.returncode == 0 and len(info.stdout.split()) == 2:
        vcpu, vmem = info.stdout.split()
        vm = f", VM {vcpu} cpu {int(vmem) / 2**30:.1f}GB"
    return f"{cpu}, {docker_version()}, {ncpu} cpu {mem_s}{vm}"


def mysql_version(host: str = "127.0.0.1", port: int = 13311, user: str = "root",
                  password: str = "root") -> str | None:
    try:
        import ssl

        import pymysql
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        conn = pymysql.connect(host=host, port=port, user=user, password=password,
                               connect_timeout=3, read_timeout=3, ssl=ctx)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                return cur.fetchone()[0]
        finally:
            conn.close()
    except Exception:
        return None
