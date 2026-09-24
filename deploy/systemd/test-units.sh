#!/usr/bin/env bash
# Install and exercise dbguard.service and dbguard-agent.service under a real systemd
# (Ubuntu 24.04 built locally, in a privileged container), without a VM.
#
# What it proves: the units parse, start, run as the right users with their hardening,
# restart on failure, and stop their whole process tree. What it does not: mysqld is a
# stand-in (`sleep`) because installing MySQL 8.4 in the test container costs ~1 GB, so
# the agent answers /health but reports mysqld unresponsive. The manager points at the
# lab fleet.yaml whose hosts do not resolve here, so it runs with every node unreachable.
#
# Usage: deploy/systemd/test-units.sh            (prints a report, removes the container)
#        KEEP=1 deploy/systemd/test-units.sh     (leave the container running)
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE=dbguard-systemd-test:24.04
# A native image. The public systemd-ubuntu images either stop at 22.04 (Python 3.10, too
# old for the package) or have no arm64 build, and systemd under amd64 emulation on Apple
# Silicon cannot start any service (even /bin/true exits 255, journald and dbus fail).
docker build -q -t "$IMAGE" - >/dev/null <<'DOCKERFILE'
FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      systemd systemd-sysv dbus python3-venv python3-pip procps \
 && rm -rf /var/lib/apt/lists/*
STOPSIGNAL SIGRTMIN+3
CMD ["/sbin/init"]
DOCKERFILE
NAME=dbguard-systemd-test

cleanup() {
  [ "${KEEP:-0}" = 1 ] && return
  docker rm -fv "$NAME" >/dev/null 2>&1 || true
  [ "${KEEP_IMAGE:-0}" = 1 ] || docker rmi "$IMAGE" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker rm -fv "$NAME" >/dev/null 2>&1 || true

docker run -d --name "$NAME" --privileged --cgroupns=host \
  -v /sys/fs/cgroup:/sys/fs/cgroup:rw -v "$REPO":/src:ro "$IMAGE" >/dev/null

ex() { docker exec "$NAME" bash -c "$*"; }

echo "== waiting for systemd"
for _ in $(seq 1 60); do
  s=$(docker exec "$NAME" systemctl is-system-running 2>/dev/null || true)
  case "$s" in running|degraded) break ;; esac
  sleep 1
done
echo "system state: $s"

echo "== installing the dbguard package"
ex 'mkdir -p /tmp/src && cd /src && tar --exclude=.venv --exclude=.git --exclude=results --exclude="*.egg-info" -cf - . | tar -xf - -C /tmp/src'
ex 'python3 -m venv /opt/dbguard/venv && /opt/dbguard/venv/bin/pip install -q /tmp/src >/dev/null && ls /opt/dbguard/venv/bin/ | grep -E "^dbg"'

echo "== configuration"
ex 'useradd --system --no-create-home --shell /usr/sbin/nologin dbguard'
ex 'mkdir -p /etc/dbguard && cp /src/deploy/fleet.yaml /etc/dbguard/fleet.yaml'
ex 'cp /src/deploy/systemd/dbguard.env.example /etc/dbguard/dbguard.env'
ex 'sed -e "s#^DBGUARD_MYSQLD_CMD=.*#DBGUARD_MYSQLD_CMD=\"/bin/sleep infinity\"#" -e "s#^DBGUARD_MANAGER_URL=.*#DBGUARD_MANAGER_URL=http://127.0.0.1:9090#" /src/deploy/systemd/agent.env.example > /etc/dbguard/agent.env'
ex 'cp /src/deploy/systemd/dbguard.service /src/deploy/systemd/dbguard-agent.service /etc/systemd/system/'
ex 'systemd-analyze verify /etc/systemd/system/dbguard.service /etc/systemd/system/dbguard-agent.service && echo "systemd-analyze verify: ok"'
ex 'systemctl daemon-reload && systemctl enable --now dbguard.service dbguard-agent.service 2>&1'
sleep 6

health() {
  ex "/opt/dbguard/venv/bin/python -c \"import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:$1/health', timeout=3); print('GET :$1/health', r.status)\"" 2>/dev/null || echo "GET :$1/health failed"
}

echo "== status"
ex 'systemctl status --no-pager -n 5 dbguard.service dbguard-agent.service' || true
health 9090
health 8080
echo "== users and hardening"
ex 'for u in dbguard dbguard-agent; do pid=$(systemctl show -p MainPID --value $u); echo "$u MainPID=$pid user=$(ps -o user= -p $pid) NoNewPrivs=$(grep NoNewPrivs /proc/$pid/status | tr -s "\t" " ")"; done'
ex 'systemctl show dbguard-agent -p KillMode -p TimeoutStopUSec -p Restart -p RestartUSec -p ProtectSystem -p PrivateTmp'
ex 'echo "agent children:"; ps --ppid $(systemctl show -p MainPID --value dbguard-agent) -o pid,user,cmd'

echo "== Restart=on-failure: SIGKILL the agent"
ex 'kill -9 $(systemctl show -p MainPID --value dbguard-agent)'
sleep 5
ex 'systemctl show dbguard-agent -p ActiveState -p SubState -p NRestarts'
health 8080

echo "== stop takes the whole tree down (KillMode=mixed)"
ex 'systemctl stop dbguard-agent; systemctl show dbguard-agent -p ActiveState; echo "sleep stand-ins left: $(pgrep -c -x sleep || true)"'
ex 'systemctl stop dbguard; systemctl show dbguard -p ActiveState -p Result'
echo "== done"
