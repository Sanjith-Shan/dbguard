# systemd units

`dbguard.service` runs the manager, one per fleet. `dbguard-agent.service` runs the agent,
one per MySQL host, and the agent starts `mysqld` as its child. That is why the agent unit
replaces `mysql.service` (`Conflicts=`) and runs as root. It has to SIGKILL and restart a
mysqld that will not accept a fence in SQL. mysqld itself drops to the `mysql` user.

Both units use `Type=simple`, `Restart=on-failure`, `RestartSec=2`, `KillMode=mixed` (SIGTERM
to the daemon alone, SIGKILL to whatever is left after `TimeoutStopSec=30`), and
`NoNewPrivileges`, `PrivateTmp`, `ProtectSystem`, `ProtectHome`. The manager runs as the
`dbguard` user with `ProtectSystem=strict` and only `/var/lib/dbguard` writable. The agent
gets `ProtectSystem=full`, which leaves `/var/lib/mysql` and `/run/mysqld` writable.

## Install

On the manager host

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin dbguard
sudo python3.12 -m venv /opt/dbguard/venv
sudo /opt/dbguard/venv/bin/pip install /path/to/dbguard
sudo mkdir -p /etc/dbguard
sudo cp deploy/fleet.yaml /etc/dbguard/fleet.yaml          # edit hosts and credentials
sudo cp deploy/systemd/dbguard.env.example /etc/dbguard/dbguard.env
sudo cp deploy/systemd/dbguard.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now dbguard
```

On every MySQL host (MySQL 8.4 installed, `mysql.service` disabled)

```sh
sudo systemctl disable --now mysql
sudo python3.12 -m venv /opt/dbguard/venv
sudo /opt/dbguard/venv/bin/pip install /path/to/dbguard
sudo mkdir -p /etc/dbguard
sudo cp deploy/systemd/agent.env.example /etc/dbguard/agent.env   # node, set, server id, manager URL
sudo cp deploy/systemd/dbguard-agent.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now dbguard-agent
```

`/etc/dbguard/agent.env` sets `DBGUARD_MYSQLD_CMD=/usr/sbin/mysqld --user=mysql` so the agent
starts the distribution's mysqld directly. `dbguard.env` sets `DBGUARD_PROVISION=null`, because
outside the Docker lab there is no `docker compose` to provision a spare, and the manager logs
the replacement request instead.

## Tested here, without a VM

`deploy/systemd/test-units.sh` builds a small Ubuntu 24.04 image with systemd (269 MB), boots
it as PID 1 in a privileged container, installs the package into `/opt/dbguard/venv`, installs
both units, and checks that they start, run as the right users with `NoNewPrivs` set, answer
`/health`, restart after a SIGKILL of the agent, and take the whole process tree down on
stop. It removes the container and the image when it finishes.

What it does not test. mysqld is a stand-in (`/bin/sleep infinity`), because MySQL 8.4 in the
test image would cost about 1 GB, so the agent answers `/health` and reports mysqld
unresponsive. The manager reads the lab `fleet.yaml`, whose hosts do not resolve inside the
test container, so it runs with every node unreachable. The public systemd images were no use
here. `eniocarboni/docker-ubuntu-systemd` stops at 22.04 (Python 3.10, the package needs
3.12), and `jrei/systemd-ubuntu:24.04` has no arm64 build. Under amd64 emulation on Apple
Silicon systemd could not start any service at all, not even `/bin/true` (exit 255, journald
and dbus failed too).

Output of the last run

```
== waiting for systemd
system state: running
== installing the dbguard package
dbgctl
dbguard
dbguard-agent
== configuration
systemd-analyze verify: ok
== status
● dbguard.service - DBGuard MySQL replica-set manager
     Loaded: loaded (/etc/systemd/system/dbguard.service; enabled; preset: enabled)
     Active: active (running) since Thu 2026-09-24 06:22:22 UTC; 6s ago
       Docs: https://github.com/Sanjith-Shan/dbguard/blob/main/docs/RUNBOOK.md
   Main PID: 217 (dbguard)
      Tasks: 9 (limit: 9520)
     Memory: 34.4M (peak: 36.7M)
        CPU: 346ms
     CGroup: /docker/10a71aa860ec958819caae888dc324f59fc1160221d53b1de4442f8c4bcccd99/system.slice/dbguard.service
             └─217 /opt/dbguard/venv/bin/python3 /opt/dbguard/venv/bin/dbguard --config /etc/dbguard/fleet.yaml --state-dir /var/lib/dbguard --listen 0.0.0.0:9090

Sep 24 06:22:22 10a71aa860ec systemd[1]: Started dbguard.service - DBGuard MySQL replica-set manager.
Sep 24 06:22:22 10a71aa860ec dbguard[217]: {"mode": "dbguard", "sets": ["rs1", "rs2"], "event": "manager started", "level": "info", "timestamp": "2026-09-24T06:22:22.297409Z"}
Sep 24 06:22:22 10a71aa860ec dbguard[217]: {"listen": "0.0.0.0:9090", "mode": "dbguard", "state_dir": "/var/lib/dbguard", "host_ports": false, "host_map": false, "event": "listening", "level": "info", "timestamp": "2026-09-24T06:22:22.297470Z"}
Sep 24 06:22:22 10a71aa860ec dbguard[217]: {"rs": "rs1", "note": "no primary found yet: 0 of 3 nodes answer, none writable", "event": "discover", "level": "warning", "timestamp": "2026-09-24T06:22:22.311565Z"}
Sep 24 06:22:22 10a71aa860ec dbguard[217]: {"rs": "rs2", "note": "no primary found yet: 0 of 3 nodes answer, none writable", "event": "discover", "level": "warning", "timestamp": "2026-09-24T06:22:22.311704Z"}

● dbguard-agent.service - DBGuard agent (mysqld supervisor, fence, role changes)
     Loaded: loaded (/etc/systemd/system/dbguard-agent.service; enabled; preset: enabled)
     Active: active (running) since Thu 2026-09-24 06:22:22 UTC; 6s ago
       Docs: https://github.com/Sanjith-Shan/dbguard/blob/main/docs/DESIGN.md
   Main PID: 218 (dbguard-agent)
      Tasks: 2 (limit: 9520)
     Memory: 26.9M (peak: 27.1M)
        CPU: 210ms
     CGroup: /docker/10a71aa860ec958819caae888dc324f59fc1160221d53b1de4442f8c4bcccd99/system.slice/dbguard-agent.service
             ├─218 /opt/dbguard/venv/bin/python3 /opt/dbguard/venv/bin/dbguard-agent
             └─219 /bin/sleep infinity

Sep 24 06:22:22 10a71aa860ec systemd[1]: Started dbguard-agent.service - DBGuard agent (mysqld supervisor, fence, role changes).
Sep 24 06:22:22 10a71aa860ec dbguard-agent[218]: {"port": 8080, "supervise": true, "cmd": ["/bin/sleep", "infinity"], "event": "agent_listening", "level": "info", "timestamp": "2026-09-24T06:22:22.246478Z"}
Sep 24 06:22:22 10a71aa860ec dbguard-agent[218]: {"node": "mysql-a1", "rs": "rs1", "semisync": true, "fenced": false, "supervised": true, "event": "agent_startup", "level": "info", "timestamp": "2026-09-24T06:22:22.247861Z"}
Sep 24 06:22:22 10a71aa860ec dbguard-agent[218]: {"pid": 219, "generation": 1, "cmd": ["/bin/sleep", "infinity"], "event": "mysqld_started", "level": "info", "timestamp": "2026-09-24T06:22:22.248247Z"}
Sep 24 06:22:27 10a71aa860ec dbguard-agent[218]: {"event": "waiting_for_mysqld", "level": "info", "timestamp": "2026-09-24T06:22:27.285861Z"}
GET :9090/health 200
GET :8080/health 200
== users and hardening
dbguard MainPID=217 user=dbguard NoNewPrivs=NoNewPrivs: 1
dbguard-agent MainPID=218 user=root NoNewPrivs=NoNewPrivs: 1
Restart=on-failure
RestartUSec=2s
TimeoutStopUSec=30s
PrivateTmp=yes
ProtectSystem=full
KillMode=mixed
agent children:
    PID USER     CMD
    219 root     /bin/sleep infinity
== Restart=on-failure: SIGKILL the agent
NRestarts=1
ActiveState=active
SubState=running
GET :8080/health 200
== stop takes the whole tree down (KillMode=mixed)
ActiveState=inactive
sleep stand-ins left: 0
Result=success
ActiveState=inactive
== done
```
