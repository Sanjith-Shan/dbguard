# DBGuard internal interfaces (the contract every component builds against)

This file is the agreement between the four parts built in parallel: the fleet
(compose, images, HAProxy), the agent, the manager, and the harness (chaos, workload,
checker, dbgctl). Change it only by editing this file first. Everything here is
authoritative over any module docstring.

## Repo layout

```
dbguard/                  Python 3.12 package (installed as `dbguard`)
  gtid.py                 GtidSet, pure Python, hypothesis-tested
  config.py               pydantic models, loads fleet.yaml
  mysqlx.py               aiomysql helpers shared by agent and manager
  events.py               JSON event rows (schema below)
  agent/                  dbguard-agent (mysqld supervisor + HTTP :8080)
  manager/                dbguard (manager daemon, HTTP :9090)
  cli/                    dbgctl
bin/                      workload, checker, chaos, report, demo (python scripts, executable)
deploy/
  Dockerfile              FROM mysql:8.4 + python3.12 + this package  (image: dbguard-node)
  Dockerfile.manager      FROM python:3.12-slim + this package        (image: dbguard-manager)
  docker-compose.yml      the fleet
  fleet.yaml              manager config (mounted into the manager container)
  mysql/my.cnf            base config, semi-sync via env DBGUARD_SEMISYNC=1|0
  mysql/init/*.sql        users, dbguard schema, chaos schema
  haproxy/haproxy.cfg
  orchestrator/           orchestrator.conf.json and hook scripts
  systemd/                dbguard.service, dbguard-agent.service
tests/                    pytest, hypothesis
results/                  chaos JSON rows, one file per scenario/mode: results/<scenario>_<mode>.jsonl
docs/                     DESIGN, CAPACITY, RUNBOOK, BUGS, INTERFACES (this)
```

## Names and addresses

- Replica sets: `rs1` = mysql-a1, mysql-a2, mysql-a3 (spare mysql-a4). `rs2` = mysql-b1,
  mysql-b2, mysql-b3 (spare mysql-b4). Initial primary is the `*1` node.
- Every node container runs **one** `dbguard-agent` process as PID 1 (under tini) which
  starts `mysqld` as its child. The agent listens on `:8080`. mysqld on `:3306`.
- Compose network `dbguard` (bridge). Containers are reachable by service name.
- HAProxy: `haproxy:3306` -> rs1 primary, `haproxy:3307` -> rs2 primary, stats on `:8404`.
  Health check is `GET /primary` on each node's agent. `on-marked-down shutdown-sessions`.
- Manager container `dbguard`, HTTP `:9090`. Orchestrator container `orchestrator`, `:3000`
  (compose profile `orchestrator`).
- All host ports published: 13306 (rs1 via haproxy), 13307 (rs2), 18404 (haproxy stats),
  19090 (manager), 13000 (orchestrator), 1330X direct to each mysqld (13311 a1, 13312 a2,
  13313 a3, 13314 a4, 13321 b1, 13322 b2, 13323 b3, 13324 b4), 1808X agents in the same
  pattern (18011 a1 ... 18024 b4).

## MySQL accounts and schema (deploy/mysql/init)

- `root` / `root` (only for init and the harness). `dbguard` / `dbguard` with
  `ALL PRIVILEGES` plus `BACKUP_ADMIN, CLONE_ADMIN, REPLICATION SLAVE, REPLICATION CLIENT,
  SYSTEM_VARIABLES_ADMIN, CONNECTION_ADMIN`, host `%`. `repl` / `repl` with
  `REPLICATION SLAVE`. `chaos` / `chaos` on `chaos.*`.
- Every account uses `caching_sha2_password` (8.4 default). Connections inside the
  compose network are plain TCP, `ssl=False` explicitly in aiomysql (8.4 clients default to
  requiring the server public key for caching_sha2 over insecure links, so pass
  `server_public_key` or use `get_server_public_key=True`. aiomysql: use
  `auth_plugin`-free connect and set `ssl=None`; if that fails with "public key", switch the
  accounts to `caching_sha2_password` with TLS from the server's auto-generated certs,
  `ssl=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=CERT_NONE`).
  The fleet agent decides once and records it in BUGS.md if it bit.
- Schema `dbguard`:
  - `heartbeat(rs VARCHAR(16) PRIMARY KEY, ts TIMESTAMP(6), writer VARCHAR(64))` written by
    the primary's agent every 500 ms with `INSERT ... ON DUPLICATE KEY UPDATE`.
  - `manager_probe(rs VARCHAR(16) PRIMARY KEY, ts TIMESTAMP(6))` written by the manager's probe.
- Schema `chaos`: `writes(client_id INT, seq BIGINT, ts TIMESTAMP(6), run_id VARCHAR(64),
  payload VARBINARY(512), PRIMARY KEY(client_id, seq, run_id))`, plus
  `blob(id BIGINT AUTO_INCREMENT PRIMARY KEY, data VARBINARY(4096))` used to grow the
  dataset for clone throughput.

## my.cnf (every node, semi-sync toggled by env)

```
server_id           from env DBGUARD_SERVER_ID (a1=11 a2=12 a3=13 a4=14 b1=21 ... b4=24)
gtid_mode=ON  enforce_gtid_consistency=ON  log_bin  log_replica_updates=ON  binlog_format=ROW
super_read_only=1  read_only=1              (every node boots read-only; only promotion clears it)
plugin-load-add=semisync_source.so;semisync_replica.so;mysql_clone.so
rpl_semi_sync_source_wait_point=AFTER_SYNC
rpl_semi_sync_source_wait_for_replica_count=1
rpl_semi_sync_source_timeout=3600000
rpl_semi_sync_source_enabled / rpl_semi_sync_replica_enabled are set by the agent at role change
replica_net_timeout=2      (IO thread notices a dead source in ~2 s, not 60 s)
sync_binlog=1  innodb_flush_log_at_trx_commit=1
report_host=<container name>
```
Replication is always configured with `SOURCE_AUTO_POSITION=1, SOURCE_HEARTBEAT_PERIOD=0.5,
SOURCE_CONNECT_RETRY=1, SOURCE_RETRY_COUNT=86400, SOURCE_SSL=0` (or SSL=1 if the auth decision
above needs it), user `repl`.

Naive mode (`DBGUARD_SEMISYNC=0`): semi-sync plugins loaded but never enabled. Everything
else identical.

## Agent HTTP API (`:8080`, JSON)

| Method | Path | Response |
|---|---|---|
| GET | `/primary` | 200 `{"role":"primary"}` iff `@@super_read_only=0` AND fence flag clear AND mysqld answers within 1 s. Otherwise 503 `{"role":"replica"|"fenced"|"unknown"}`. |
| GET | `/status` | see below |
| GET | `/health` | 200 if the agent process is alive (used by compose healthcheck) |
| POST | `/fence` | `{"fenced":true,"method":"sql"|"kill","gtid_executed":"...","duration_ms":n,"killed_threads":n}` |
| POST | `/unfence` | `{"fenced":false}` (clears the flag only; does not change read_only) |
| POST | `/promote` | `{"gtid_executed":"...","duration_ms":n}` runs STOP REPLICA; RESET REPLICA ALL; SET GLOBAL super_read_only=0, read_only=0; semi-sync source on, replica off; clears fence |
| POST | `/repoint` body `{"source":"mysql-a3"}` | `{"ok":true,"duration_ms":n}` STOP REPLICA; sets super_read_only=1; semi-sync replica on, source off; CHANGE REPLICATION SOURCE TO ...; START REPLICA |
| POST | `/rebuild` body `{"donor":"mysql-a2"}` | `{"ok":true,"phantom_gtids":n,"duration_ms":n,"bytes":n}` SET GLOBAL clone_valid_donor_list; CLONE INSTANCE FROM ...; waits for mysqld to restart (the agent restarts it); leaves node read-only and unreplicated; caller repoints |
| POST | `/configure` body `{"semisync":true}` | re-asserts semi-sync variables for the current role |
| POST | `/kill-mysqld` | test hook, SIGKILL mysqld (agent restarts it) |
| POST | `/hang-mysqld` body `{"seconds":n}` | test hook, SIGSTOP mysqld then SIGCONT after n s |
| GET | `/metrics` | Prometheus text |

`/status` body (all keys always present, null when unknown):
```
{"node":"mysql-a1","rs":"rs1","ts":<unix float>,
 "mysqld_alive":bool,"mysqld_responsive":bool,"mysqld_pid":int|null,
 "fenced":bool,"super_read_only":bool|null,"read_only":bool|null,
 "gtid_executed":"..."|null,
 "replica":{"configured":bool,"source_host":str|null,"io_running":"Yes"|"No"|"Connecting"|null,
            "sql_running":"Yes"|"No"|null,"seconds_behind_source":int|null,
            "retrieved_gtid_set":str|null,"executed_gtid_set":str|null,
            "last_io_error":str|null,"last_sql_error":str|null},
 "semisync":{"source_enabled":bool,"replica_enabled":bool,"source_status":bool,
             "replica_status":bool,"source_clients":int,"avg_wait_time_us":int,
             "no_tx":int,"yes_tx":int},
 "source_reachable":bool|null,       (TCP connect to source_host:3306 within 500 ms)
 "heartbeat":{"ts":<unix float>|null,"age_s":float|null,"writer":str|null},
 "agent_uptime_s":float,"agent_version":str}
```

Fence semantics: set the flag (persisted at `/var/lib/dbguard/fenced`) FIRST so `/primary`
fails immediately, then `SET GLOBAL super_read_only=1` with a 2 s deadline, then kill every
non-system, non-replication thread. If the SET does not return in 2 s the agent SIGKILLs
mysqld and reports `method:"kill"`. The agent restarts mysqld afterwards (it boots read-only
by my.cnf). The flag is cleared only by `/promote` or `/unfence`.

Startup and wake guard (woken-primary window): on agent start, and whenever the agent's
monotonic loop observes a gap > 3 s (the container was frozen), the agent asks the manager
`GET /v1/sets/<rs>/primary`. If the answer is not this node and this node has
`super_read_only=0`, it fences itself before doing anything else. If the manager is
unreachable, it fences if the fence file exists, else leaves state alone and logs. Env
`DBGUARD_MANAGER_URL=http://dbguard:9090`.

Env for the agent container: `DBGUARD_NODE`, `DBGUARD_RS`, `DBGUARD_SERVER_ID`,
`DBGUARD_SEMISYNC`, `DBGUARD_MANAGER_URL`, `MYSQL_ROOT_PASSWORD`.

## Manager HTTP API (`:9090`, JSON)

| Method | Path | Response |
|---|---|---|
| GET | `/v1/status` | `{"mode":"dbguard"|"naive","sets":{"rs1":<SetStatus>,...}}` |
| GET | `/v1/sets/{rs}` | `<SetStatus>` |
| GET | `/v1/sets/{rs}/primary` | `{"primary":"mysql-a1"|null,"state":"HEALTHY"}` (used by agents' guard) |
| GET | `/v1/sets/{rs}/doctor` | `{"lines":[...sentences...],"verdict":str,"gaps":{"mysql-a2":"<gtid subtract vs primary>",...}}` |
| POST | `/v1/sets/{rs}/failover` body `{"to":"mysql-a3"|null}` | planned switchover; `{"event":<Event>}` |
| POST | `/v1/sets/{rs}/halt` / `/resume` | `{"state":...}` |
| POST | `/v1/sets/{rs}/rejoin` body `{"node":...}` | manual rejoin trigger |
| GET | `/v1/events?rs=&since=` | `{"events":[...]}` newest last |
| GET | `/metrics` | Prometheus text |
| GET | `/health` | 200 |

`SetStatus`:
```
{"rs":"rs1","state":"HEALTHY|SUSPECT|FAILING_OVER|DEGRADED|REBUILDING|HALTED",
 "halt_reason":str|null,"primary":"mysql-a1"|null,"since":<unix>,
 "nodes":{"mysql-a1":{"role":"primary|replica|down|fenced|spare","reachable":bool,
          "gtid_executed":..,"lag_s":float|null,"heartbeat_age_s":float|null,
          "semisync":"source|replica|off","io_running":..,"sql_running":..}},
 "last_event":<Event>|null,"failovers_total":n,"cooldown_until":<unix>|null}
```

## Event row (dbguard/events.py), one JSON object per line in `/var/lib/dbguard/events.jsonl`
and in the API

```
{"ts":<unix>,"rs":"rs1","type":"failover|switchover|rejoin|rebuild|replace|halt|resume|suspect|degraded|healthy|stall",
 "mode":"dbguard|naive","old_primary":str|null,"new_primary":str|null,
 "trigger":"dead|hung|partition|planned|manual",
 "detect":{"manager_probe_failed":bool,"replica_votes":n,"replica_total":n,"duration_s":f},
 "steps":{"fence":{"duration_s":f,"outcome":"sql|kill|unreachable|skipped"},
          "choose":{"duration_s":f,"candidates":[...],"winner":str,"subset_ok":bool},
          "catchup":{"duration_s":f,"ok":bool},
          "promote":{"duration_s":f},
          "repoint":{"duration_s":f,"nodes":[...]}},
 "total_s":f,"watermark_gtid":str|null,
 "rejoin":{"branch":"repoint|rebuild|manual|none","phantom_gtids":n,"duration_s":f}|null,
 "note":str|null}
```

## Chaos result row (results/<scenario>_<mode>.jsonl), one per run

```
{"run_id":str,"scenario":"kill|hang-container|hang-process|partition-replicas|partition-manager|
  cost|replica-loss|switchover|disk-full|kill-two","mode":"dbguard|naive|orchestrator",
 "rs":"rs1","ts":<iso>,"host":"Apple M3 Pro, Docker Desktop <ver>, <n> cpu <mem>",
 "mysql_version":"8.4.x","docker_version":str,"detect_window_s":f,"probe_timeout_s":f,
 "clients":n,"workload_s":f,"inject_ts":<unix>,
 "failover_s":f|null,               (inject -> first successful write through HAProxy)
 "first_error_ts":f|null,"first_ok_after_ts":f|null,
 "acked_writes":n,"lost_acked_writes":n,"lost_list":[[client,seq],...][:20],
 "phantom_writes":n,"single_writer_violations":n,"converged":bool,"converge_s":f|null,
 "false_failover":bool,"stall_s":f|null,
 "writes_on_woken_primary":n|null,
 "rejoin":{"branch":str,"phantom_gtids":n,"duration_s":f}|null,
 "event":<Event>|null,
 "rs2_state_changes":n,               (fleet isolation: must be 0)
 "commit_p50_ms":f,"commit_p99_ms":f,"writes_per_s":f,
 "clone":{"bytes":n,"duration_s":f,"mb_per_s":f}|null,
 "notes":str}
```

## Workload and checker

`bin/workload --host 127.0.0.1 --port 13306 --clients 8 --duration 60 --run-id X
--ack-log results/tmp/<run>.ack.jsonl` writes rows through HAProxy, appends
`{"client":c,"seq":s,"t_ok":<unix>,"latency_ms":f}` only when INSERT returned OK, appends
`{"client":c,"seq":s,"error":"...","t_err":<unix>}` on error (then reconnects, continues
with the next seq). Prints a JSON summary on exit (acked, errors, first_error_ts,
first_ok_after_error_ts, p50/p99 latency, writes/s).

`bin/checker --run-id X --ack-log ... --nodes mysql-a1:13311,...` connects directly to each
node, finds the primary (super_read_only=0), asserts lossless / no-phantom / single-writer /
convergence, prints a JSON verdict with counts and lists.

## Config, deploy/fleet.yaml

```yaml
mode: dbguard            # or naive
detect_window_s: 5
probe_timeout_s: 1
probe_failures: 3
fence_deadline_s: 3
catchup_deadline_s: 30
rebuild_after_s: 60
cooldown_s: 20           # no second automatic failover of the same set within this window
rejoin: auto             # or manual
poll_interval_s: 0.5
mysql: {user: dbguard, password: dbguard, repl_user: repl, repl_password: repl, port: 3306}
agent_port: 8080
sets:
  rs1: {nodes: [mysql-a1, mysql-a2, mysql-a3], spare: mysql-a4}
  rs2: {nodes: [mysql-b1, mysql-b2, mysql-b3], spare: mysql-b4}
```

## Bootstrap

The manager bootstraps a set whose nodes are all read-only with empty or identical
`gtid_executed` and no replication configured: promote `nodes[0]`, repoint the rest.
Until the manager exists, `bin/bootstrap` (harness) does the same with direct SQL.

## Logging

structlog JSON to stdout everywhere. Every SQL statement the agent runs at role change is
logged at INFO with `sql=` and `duration_ms=`.
