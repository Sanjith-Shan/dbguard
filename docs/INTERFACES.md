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
- Every account uses `caching_sha2_password` (8.4 default). Decision by the agent, verified
  against mysql 8.4.11. Python clients connect with TLS and no certificate check,
  `ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE`,
  passed as `ssl=ctx` to aiomysql. mysqld auto-generates its certificates at first boot.
  Plain TCP fails the first full authentication with "'cryptography' package is required"
  because PyMySQL needs RSA for the password exchange, and it only appears to work after
  some TLS login has warmed the server's auth cache, which a restart flushes. Replication
  uses `SOURCE_SSL=1` for the same reason. `dbguard/agent/db.py` has the agent's copy
  (`insecure_tls()`).
  The fleet re-verified it against the running compose fleet (a fresh `repl` login over
  plain TCP fails, TLS succeeds). `dbguard/mysqlx.py` and `dbguard/mysqlx_sync.py` connect
  with TLS by default (`tls=False` exists only for tests), and `bin/bootstrap`, `bin/workload`
  and `bin/checker` go through them.
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
SOURCE_CONNECT_RETRY=1, SOURCE_RETRY_COUNT=86400, SOURCE_SSL=1` (the auth decision above),
user `repl`.

Naive mode (`DBGUARD_SEMISYNC=0`): semi-sync plugins loaded but never enabled. Everything
else identical.

Fleet deviations from the block above, all in `deploy/mysql/my.cnf`.

| Setting | Why |
|---|---|
| `loose_rpl_semi_sync_source_*` | `mysqld --initialize` ignores `plugin_load_add`, so without `loose_` the first boot aborts on an unknown variable. `bin/bootstrap` checks the plugins are ACTIVE instead |
| one `plugin_load_add` line per plugin | same effect as the `;` list, easier to read |
| `relay_log_recovery=ON` | crash-safe replica. A replica that crashed refetches its relay log from the source by GTID auto-position |
| `binlog_expire_logs_seconds=3600` | the laptop disk is small |
| `innodb_buffer_pool_size=128M`, `innodb_redo_log_capacity=128M`, `mem_limit: 700m` | eight nodes fit in Docker Desktop's 8 GB |
| `skip_name_resolve`, `mysqlx=0`, `bind_address=0.0.0.0`, `report_port=3306` | plumbing |

Node startup. `/entrypoint.sh` (under tini) renders `/etc/mysql/conf.d/dbguard-node.cnf`
(`server_id`, `report_host`) from env, initialises an empty datadir itself with a writable
temporary server (see BUGS.md), then execs `dbguard-agent`. The agent's child command
`docker-entrypoint.sh mysqld` therefore always finds an initialised datadir and just starts
mysqld read-only. The init SQL is not binlogged and ends with `RESET BINARY LOGS AND GTIDS`,
so every node starts with an empty `gtid_executed`. If `dbguard.agent.main` is not
importable the entrypoint falls back to `docker-entrypoint.sh mysqld`.

Compose. Project name `dbguard`, network `dbguard`, `container_name` equals the service name
(`docker kill mysql-a1` works). Volumes `dbguard_<node>-data` (datadir) and
`dbguard_<node>-state` (`/var/lib/dbguard`, the fence file). The manager mounts the compose
file at `/deploy/docker-compose.yml` and the docker socket, with
`DBGUARD_COMPOSE_PROJECT=dbguard`. The spare services have no bind mounts, so the manager
can start them from inside its container. HAProxy lists the spares too, resolved lazily
(`init-addr none`), so they show `MAINT (resolution)` until they exist.

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
 "self_fence":{"enabled":bool,"manager_unreachable_s":float,"semisync_clients":int|null},
 "agent_uptime_s":float,"agent_version":str}
```

Fence semantics: set the flag (persisted at `/var/lib/dbguard/fenced`) FIRST so `/primary`
fails immediately, then `SET GLOBAL super_read_only=1` with a 2 s deadline, then kill every
non-system, non-replication thread. If the SET does not return in 2 s the agent SIGKILLs
mysqld and reports `method:"kill"`. The agent restarts mysqld afterwards (it boots read-only
by my.cnf). The flag is cleared only by `/promote` or `/unfence`.

Agent additions to the shapes above. Extra keys only, nothing removed.

- `/status` also carries `"role":"primary"|"replica"|"fenced"|"unknown"`,
  `semisync.wait_sessions` (Rpl_semi_sync_source_wait_sessions), `semisync.mode` (the agent's
  semi-sync setting, changed by `/configure`), `heartbeat.stalled_s` (how long the in-flight
  heartbeat INSERT has been waiting, 0.0 when none is in flight, grows during a semi-sync
  stall), and `error` when mysqld did not answer. In `--no-supervise` mode `mysqld_pid` is
  null and `mysqld_alive` mirrors `mysqld_responsive`.
- `/promote` enables semi-sync on the source side BEFORE it clears `super_read_only`, so no
  write is ever accepted on the new primary without the ack requirement. The executed order is
  `STOP REPLICA`, `RESET REPLICA ALL`, `SET GLOBAL rpl_semi_sync_replica_enabled=0`,
  `SET GLOBAL rpl_semi_sync_source_enabled=1`, `SET GLOBAL super_read_only=0`,
  `SET GLOBAL read_only=0`, then the fence flag is cleared.
- `/fence` answers 500 with `"method":"failed"` when the SET missed its deadline and the agent
  has no mysqld pid to kill (only possible with `--no-supervise`). The flag is still set.
- `/configure` answers `{"ok":true,"semisync":bool,"role":str,"source_enabled":bool,
  "replica_enabled":bool}`. It never switches the source side off while
  `Rpl_semi_sync_source_wait_sessions > 0`, because that would release waiting sessions and
  acknowledge writes no replica has. The agent also runs it after startup and after every
  mysqld restart.
- `/rebuild` also returns `gtid_executed` after the clone. `/kill-mysqld` and `/hang-mysqld`
  answer `{"ok":true,"pid":n}` (plus `seconds`) and 409 without a supervised mysqld.
- `/primary` answers 503 `unknown` until the startup guard has run, and a request that
  arrives after a monotonic gap over 3 s waits (up to 2.5 s) for the wake guard first.
- The agent passes `MYSQLD_PARENT_PID=<agent pid>` to mysqld, which makes mysqld treat the
  agent as its monitoring process. `RESTART` and the restart after `CLONE INSTANCE` then exit
  with code 16 and the agent restarts mysqld at once.
- Extra agent env. `DBGUARD_MYSQLD_CMD` (default `docker-entrypoint.sh mysqld`),
  `DBGUARD_MYSQL_HOST`/`DBGUARD_MYSQL_PORT` (default 127.0.0.1/3306),
  `DBGUARD_MYSQL_USER`/`DBGUARD_MYSQL_PASSWORD` (default dbguard/dbguard, falls back to root
  and `MYSQL_ROOT_PASSWORD` on access denied), `DBGUARD_REPL_USER`/`DBGUARD_REPL_PASSWORD`
  (default repl/repl), `DBGUARD_AGENT_PORT` (8080), `DBGUARD_STATE_DIR` (/var/lib/dbguard).
  Flag `--no-supervise` attaches to a mysqld the agent did not start.

Self-fence lease. A primary cut off from BOTH the manager and every semi-sync replica, but
still reachable by HAProxy and clients, would otherwise be protected only by the one hour
semi-sync timeout, after which it falls back to async and acknowledges writes that exist
nowhere else. Every second the primary's agent (super_read_only=0, not fenced) reads
`Rpl_semi_sync_source_clients` and asks the manager `GET /v1/sets/<rs>/primary` (1 s timeout).
When the client count is 0 AND the manager has been unreachable for
`DBGUARD_SELF_FENCE_AFTER_S` continuous seconds (default 10, 0 disables), the agent fences
itself through the same code path as `/fence` and logs event `self_fence` with both
observations. A reachable manager or at least one semi-sync client resets the timer. It runs
only while semi-sync mode is on and `DBGUARD_MANAGER_URL` is set. Experiment 4 (manager
partitioned from the primary, replicas fine) never triggers it because the client count stays
at 2. `/status` shows the timer under `self_fence`.

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

Manager additions to the event row. Extra keys and values only, nothing removed.

- `detect.replica_total` counts witnesses, the replicas the manager could reach that are
  configured with `source_host` equal to the primary, and `replica_votes` those of them
  reporting loss. The primary is declared dead only with at least one vote and votes
  strictly more than half of the witnesses. Zero witnesses stays SUSPECT. A DEGRADED set
  with one live replica can therefore fail over on that single witness.
- `type` may also be `bootstrap` (the manager promoted `nodes[0]` of a brand new set).
- `rejoin` also carries `"node"`, the node that rejoined (for a rejoin event `old_primary`
  is that node too, and `new_primary` the primary it now follows).
- `"clone":{"bytes":n,"duration_s":f,"mb_per_s":f|null,"donor":str}|null` on events whose
  work included a `/rebuild` (a `rejoin` with branch `rebuild`, and `replace`). The donor
  is always a replica, never the primary.
- A `stall` event is written when writes stall on the primary (semi-sync source enabled
  with no replica connected, or the manager's heartbeat write blocks while `SELECT 1`
  answers) and another when they resume, with the stall length in `note`.
- A failover that ends HALTED is still recorded as `type:"failover"` with
  `new_primary:null`, the steps done so far, and `note` starting with the halt reason. A
  `halt` event follows.

Manager additions to the HTTP API. `POST /v1/sets/{rs}/halt` accepts an optional body
`{"reason":str}`. `POST /v1/sets/{rs}/failover` answers 409 `{"error":str,"state":str}` when
the switchover is refused or aborted (the old primary is made writable again on abort).
`POST /v1/sets/{rs}/rejoin` answers `{"event":<Event>|null,"state":str,"note":str|null}`,
where a rebuild runs in the background and its event lands in `/v1/events` when done.

Running the manager on the host (outside compose), `dbguard --host-ports` or
`DBGUARD_HOST_PORTS=1` reaches every node on its published ports (mysql-a1 at
127.0.0.1:13311 and its agent at 127.0.0.1:18011), and
`DBGUARD_HOST_MAP="mysql-a1=127.0.0.1:13311:18011,..."` overrides single nodes. Names sent to
agents (repoint source, clone donor) stay container names. Stop the `dbguard` container
first, two managers on one fleet is a split brain.

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

Harness additions to the row. Extra keys only, nothing removed.

- Every row. `errors` (client errors in the ack log), `primary_at_inject`, `new_primary`,
  `config` (the fleet.yaml knobs), `heal` (`{"hard_reset":bool,"heal_s":f}`), `duration_s`,
  `commands` (every mutating docker command with its timestamp), `workload_summary` (the
  workload's own JSON summary, for cross-checking), `checker_nodes`.
- `failover_s`, `stall_s` and the commit percentiles are computed by the harness from the ack
  log on the host clock. When the failover produced no client error (commits only blocked),
  `failover_s` is inject to the end of the largest ack gap that follows the injection.
  `stall_s` is that largest gap between consecutive acknowledged writes, all clients merged.
- cost. `semisync` (bool), `netem_ms` (f), `semisync_effective` (the primary's
  `Rpl_semi_sync_source_status` after `/configure`).
- switchover. `switchover_api_s`. `failover_s` is null (a planned switchover is not a failure),
  `errors` counts only errors after the call.
- hang-container and hang-process. `wake_ts` and `woken`
  (`{"node","samples","missing_on_new_primary","written_after_wake","primary_200_seen",
  "super_read_only_0_seen","super_read_only_now","primary_code_now","fenced_now"}`),
  polled every 0.5 s for 15 s after SIGCONT or unpause. hang-container also records
  `agent_answered_while_frozen` and `docker_exec_while_frozen_rc`. hang-process records
  `mysqld_pid`, `mysqld_restarted` and `fence_method`.
- partition-replicas. `old_primary_fenced`, `heal_ts`. partition-manager. `states_seen`.
- replica-loss. `resume_s`, `degraded_seen`, `writes_while_degraded`, `stall_event`,
  `dataset_mb`, `killed`. kill-two. `killed` (the primary and the replica with the largest
  Retrieved_Gtid_Set).
- `rejoin` also carries `since_restart_s`, and `rejoin_event` holds the manager's event. In
  orchestrator mode the branch is `harness-repoint` or `harness-rebuild`, because Orchestrator
  does not rejoin a dead primary and the harness does it through the agents.
- A run that raised is not written to the results file. It goes to
  `results/<scenario>_<mode>.errors.jsonl` with the traceback.
- Only the row survives a run. The ack log and the workload output are deleted once the
  checker has read them (set `DBGUARD_KEEP_ACKS=1` to keep them).

### Fault injection mechanics

- kill. `docker kill -s KILL <primary>`.
- hang-container. `docker pause` (cgroup freezer). `docker kill -s STOP` reaches only PID 1
  (tini) and leaves the agent and mysqld running, so it cannot be used.
- hang-process. `docker exec <c> kill -STOP <mysqld pid>`, the agent stays alive.
- iptables and tc netem run in a helper container that joins the target's network namespace
  (`docker run --rm --net container:<c> --cap-add NET_ADMIN dbguard-nettools:1`, alpine
  plus iptables and iproute2, built on first use). Same kernel state as running them inside
  the target, and no dependency on the target image.

### disk-full (not implemented yet)

Filling a node's datadir would fill the Docker VM disk that every container shares. The
scenario needs a small per-node tmpfs for the binary logs, for example
`tmpfs: /var/lib/mysql-binlog:size=256m` on every node in deploy/docker-compose.yml and
`log_bin=/var/lib/mysql-binlog/binlog` in deploy/mysql/my.cnf. Both files belong to the
fleet. Until they carry it, `bin/chaos --scenario disk-full` raises NotImplementedError.

### Orchestrator baseline

Image `percona/percona-orchestrator:3.2.6-24` (multi-arch, native arm64). The upstream
`openarkcode/orchestrator` image stops at v3.2.4 (2021, amd64 only) and issues
`SHOW SLAVE STATUS`, which MySQL 8.4 removed. The compose service (profile `orchestrator`)
mounts `deploy/orchestrator/orchestrator.conf.json` at `/etc/orchestrator/orchestrator.conf.json`
and `deploy/orchestrator/hooks` at `/usr/local/orchestrator/hooks` (read-only). The image runs
as uid 1001, so the hooks write `/var/lib/orchestrator/hooks/events.jsonl` inside the
container, and the harness reads it with `docker exec orchestrator cat`. The harness stops
the `dbguard` container for the whole orchestrator campaign so only one automation acts.

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

Workload details. Each INSERT is `(client_id, seq, NOW(6), run_id, 256 random bytes)` as user
`chaos` with TLS. A seq still in flight when the workload is stopped (SIGTERM or SIGINT) gets
an error row `"error":"in-flight at shutdown"`, because its outcome is unknown. The summary
keys are `acked, errors, in_flight_at_stop, first_error_ts, first_ok_after_error_ts,
latency_p50_ms, latency_p99_ms, latency_max_ms, writes_per_s, duration_s` (nearest-rank
percentiles). `--query-timeout` is off by default, so a semi-sync stall blocks the client.

Checker verdict keys (flat, for the result row) are `pass, primary, acked, rows,
lost_acked_writes, lost_list, phantom_writes, phantom_list, single_writer_violations,
converged, converge_s, unreachable`, and `properties.{lossless,no_phantom,single_writer,
convergence}` hold the detail. Single writer means exactly one node with
`super_read_only=0 AND read_only=0`, and on every other node a root `INSERT` inside a
rolled-back transaction is rejected. Convergence compares GTID sets (not strings) of the
reachable nodes and waits up to `--converge-timeout` (default 60 s). Exit 0 only if all
four hold. The logic lives in `dbguard/harness/{workload,checker,bootstrap}.py`.

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
