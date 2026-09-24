# Bugs

Every real bug found while building DBGuard. Symptom, how it was found, fix.

## Build notes

What the `mysql:8.4` image turned out to be, checked on this machine (Apple M3 Pro, Docker
Desktop 29.4.1, Compose v5.1.3).

| Question | Answer |
|---|---|
| Base OS | Oracle Linux Server 9.8, mysqld 8.4.11 (aarch64) |
| Package manager | `microdnf` (no `dnf`, no `yum`, no `which`) |
| python3.12 | in `ol9_appstream`, 3.12.14. No need to fall back to 3.11 |
| `tc` | package `iproute-tc` (not part of `iproute` on EL9) |
| `iptables` | `iptables-nft` 1.8.10, nf_tables backend, works in a container with `NET_ADMIN` |
| `ps` | `procps-ng` 3.3.17 |
| tini | not in the OL9 repos (it is in EPEL). The static release binary v0.19.0 is fetched by `TARGETARCH` |
| Build time | the first `microdnf install` takes 6 to 15 minutes because the OL9 appstream metadata is about 250 MB. The Dockerfile keeps it in a BuildKit cache mount and puts the OS layer first, so later builds reuse it |
| Manager image | `python:3.12-slim` plus the static docker CLI 27.5.1 and compose plugin v2.32.4, so the manager can start a spare through the mounted docker socket |

The node image puts the package in a venv at `/opt/dbguard` so pip never touches the
system python that microdnf owns.

A `# syntax=docker/dockerfile:1.x` line pulls the dockerfile frontend image and changes the
cache key of every step. The Dockerfiles use the built-in frontend instead (it supports
heredocs and cache mounts already).

## Fleet

### mysqld --initialize aborts on the semi-sync variables in my.cnf

- Symptom. Every node died on first boot with
  `unknown variable 'rpl_semi_sync_source_wait_point=AFTER_SYNC'` followed by
  `The designated data directory /var/lib/mysql/ is unusable`.
- How found. The first `make up` with the INTERFACES.md my.cnf.
- Fix. `mysqld --initialize` ignores `plugin_load_add` (it logs "Ignoring --plugin-load[_add]
  list"), so the plugin variables do not exist yet. The three `rpl_semi_sync_source_*`
  settings in my.cnf carry the `loose_` prefix. Because `loose_` would also hide a plugin
  that failed to load at a real start, `bin/bootstrap` refuses to continue when semi-sync
  is wanted and `rpl_semi_sync_source_wait_point` is missing.

### super_read_only in my.cnf breaks the image's own initialisation

- Symptom. Same trap the agent section records for the command line. The stock
  `docker-entrypoint.sh` starts its temporary init server with the same config, so the root
  setup and our init SQL fail with ERROR 1290.
- How found. Reading the stock entrypoint before the first boot (its temporary server runs
  `"$@" --daemonize --skip-networking`).
- Fix. `/entrypoint.sh` initialises an empty datadir itself with
  `deploy/mysql/dbguard-initdb.sh`, which sources the stock entrypoint's functions and runs
  the same steps with the temporary server started as `--super-read-only=OFF
  --read-only=OFF`. After that `docker-entrypoint.sh mysqld` (the agent's child command)
  finds an initialised datadir and only execs mysqld, which boots read-only as my.cnf says.
  The init SQL runs with `sql_log_bin=0` and ends with `RESET BINARY LOGS AND GTIDS`, so
  every node starts with an empty and identical `gtid_executed`.

### Replicas fell 44 s behind in five minutes

- Symptom. The first clean five-minute run (8 clients, about 720 writes/s, semi-sync on)
  passed the checker, but convergence took 43.9 s. Sampling `Seconds_Behind_Source` every
  5 s during a 40 s run showed the lag growing by about 1.4 s per 5 s on both replicas.
  Semi-sync did not hide it because AFTER_SYNC waits for the relay log write, not the apply.
  With the manager's `catchup_deadline_s: 30`, a failover after a few minutes of load would
  have timed out waiting for the winner to apply its relay log.
- How found. The checker's `converge_s` on the acceptance run, then the lag sampling.
- Fix. The 8.4 default of `replica_parallel_workers=4` was the limit. Setting it to 16 on
  one replica during the same load brought that replica to 0 to 1 s of lag while the other
  kept growing. my.cnf now sets `replica_parallel_workers=16` and
  `replica_preserve_commit_order=ON`.

### The init wrapper died on an unset variable

- Symptom. First boot stopped at `dbguard-initdb.sh: line 17: DATABASE_ALREADY_EXISTS:
  unbound variable`.
- How found. `docker logs mysql-a1` on the first `make up`.
- Fix. The stock entrypoint functions are not written for `set -u`. The wrapper runs with
  `set -eo pipefail` only.

## Agent

### caching_sha2_password over plain TCP works only sometimes

- Symptom. The agent's first connection to a fresh mysql 8.4 failed with
  "'cryptography' package is required for sha256_password or caching_sha2_password auth
  methods". After any TLS login by the same account the very same plain connection
  succeeded, so the failure looked intermittent.
- How found. Connecting aiomysql to a dev `mysql:8.4` container three ways in a row
  (plain, TLS, plain again). The second plain login rode the server's fast-auth cache,
  which a mysqld restart flushes.
- Fix. Every agent connection uses TLS with certificate checks off (mysqld auto-generates
  its certs), and replication uses `SOURCE_SSL=1`. Recorded in INTERFACES.md.

### super_read_only on the command line breaks the image's first boot

- Symptom. `mysql:8.4 --super-read-only=1` exited during initialisation with
  `ERROR 1290 ... running with the --super-read-only option`.
- How found. Starting the agent's dev container with the flags from the brief.
- Fix. The official entrypoint runs its init SQL on a temporary server started with the same
  options, so read-only must not be on during init. For the dev container the flag was
  dropped and `SET GLOBAL super_read_only=1` run after init. The fleet's my.cnf has the
  same trap on an empty datadir.

### A semi-sync stall makes the SQL fence impossible

- Symptom. `/fence` on a primary whose heartbeat INSERT was waiting for a semi-sync ACK hit
  the 2 s deadline. `SHOW PROCESSLIST` showed `SET GLOBAL super_read_only=1` in state
  "Waiting for global read lock" behind the INSERT in "Waiting for semi-sync ACK from
  replica". `KILL` on the waiting session changed its command to `Killed` but it kept
  waiting.
- How found. Promoting a lone dev node with `rpl_semi_sync_source_timeout=3600000` and
  fencing it while `/status` showed `heartbeat.stalled_s` growing.
- Fix. None needed in SQL, and none possible without losing data. The only SQL that
  releases the waiters is turning `rpl_semi_sync_source_enabled` off, which would
  acknowledge writes no replica has. So a partitioned primary is always fenced by the kill
  path, which is why the agent owns the mysqld process. `/configure` refuses to switch the
  source side off while `Rpl_semi_sync_source_wait_sessions > 0` for the same reason.

### The fence killed the agent's own connections

- Symptom. The first `/fence` reported `killed_threads:3` for one client, and the next
  `/status` failed with "Lost connection" from a pooled link.
- How found. Fencing the dev node with one `SELECT SLEEP(300)` client open.
- Fix. The fence closes the agent's idle pool and its heartbeat connection before it lists
  threads, and pooled requests retry once on a lost link (2006, 2013).

### CLONE refuses a super_read_only recipient

- Symptom. `/rebuild` failed at once with `(1290, 'The MySQL server is running with the
  --super-read-only option so it cannot execute this statement')` on `CLONE INSTANCE`.
- How found. Rebuilding a node with three phantom GTIDs in a private two-node pair built
  from the dbguard-node image. Every node boots read-only by design, so every rebuild hit it.
- Fix. The agent sets `super_read_only=0` right before `CLONE INSTANCE` and holds a
  `rebuilding` flag that keeps `/primary` at 503 and keeps the heartbeat, the wake guard and
  the self-fence lease from treating the node as a primary. The clone overwrites anything
  written in that window, and the restarted mysqld boots read-only again. Verified end to
  end afterwards, phantom_gtids 3, 79 MB cloned in 7 s, mysqld exited with code 16 and the
  agent restarted it (the `MYSQLD_PARENT_PID` handshake works).

### A clone from a stalled semi-sync primary hangs

- Symptom. `/rebuild` from a donor whose heartbeat INSERT was waiting for a semi-sync ACK
  never finished. The donor showed clone threads in state `starting` for five minutes.
- How found. The first rebuild attempt in the private pair, where the only replica was the
  recipient, so the donor had no semi-sync replica left.
- Fix. None in the agent. Clone from a donor that is not stalled, which is what the
  manager does (a replica for replacement, or a new primary that has an acking replica for
  rejoin). Worth a line in the runbook.

## Harness

### docker kill -s STOP froze nothing but tini

- Symptom. The hang-container scenario as specified (`docker kill -s STOP` on the primary)
  would have left mysqld and the agent running. Only PID 1 stopped.
- How found. Before the fleet existed, on a scratch container with `--init` and two child
  processes. After `docker kill -s STOP`, `ps` inside showed `docker-init` in state `T` and
  both children still in `S`. Docker delivers the signal to PID 1 only, and tini cannot
  forward SIGSTOP because SIGSTOP cannot be caught.
- Fix. hang-container uses `docker pause`, the cgroup freezer, which stops every process in
  the container. The row records the mechanism, whether the agent still answered `/status`
  while frozen, and the exit code of a `docker exec` into the frozen container. hang-process
  keeps the per-pid `kill -STOP` on mysqld alone.

### The upstream Orchestrator image cannot manage MySQL 8.4

- Symptom. `openarkcode/orchestrator` stops at v3.2.4 (2021, amd64 only) and the last
  upstream release is 3.2.6. Both issue `SHOW SLAVE STATUS` and `SHOW MASTER STATUS`, which
  MySQL 8.4 removed, so discovery cannot work against this fleet.
- How found. Reading the Docker Hub tag list and the release dates while choosing the
  baseline image.
- Fix. The baseline runs `percona/percona-orchestrator:3.2.6-24`, Percona's maintained fork,
  which speaks the 8.4 statements and ships a native arm64 image. The config keeps
  Orchestrator's own detection and recovery and only adds the hooks that tell our agents
  about a fence and a promotion.

## Manager

### Retrieved_Gtid_Set is not cumulative, so "largest retrieved set" can pick the wrong winner

- Symptom. In the fake fleet a replica that had been repointed once showed a Retrieved_Gtid_Set
  of 3 transactions while its partner, never repointed, showed 50. Picking the largest
  retrieved set would promote the second although the first held one more transaction.
- How found. Writing the fake agent's `/repoint` the way MySQL behaves. `CHANGE REPLICATION
  SOURCE TO` purges the relay logs, and so does `relay_log_recovery` at restart, and
  Retrieved_Gtid_Set restarts from empty with them. A unit test in tests/test_selection.py
  pins the case.
- Fix. Selection ranks candidates by everything they hold, `gtid_executed` union
  Retrieved_Gtid_Set, and the subset check uses the same union. The catch-up step waits until
  the retrieved set is a subset of the executed set rather than comparing the two for
  equality.

### The fenced old primary voted the new primary dead

- Symptom. Right after a failover in the partition scenario the manager logged "replicas
  cannot see primary but manager can" about the brand new primary.
- How found. The fake fleet end-to-end test for Experiment 3. The vote counter looked at
  every member except the primary, and the old primary, fenced and replicating from nobody,
  had a heartbeat row seconds old, which counted as "heartbeat stale".
- Fix. Only a replica configured to replicate from the believed primary votes. A node
  replicating from elsewhere, or from nothing, knows nothing about the primary. The same
  rule is what orchestrator applies when it looks only at the master's own replicas.

### A write stall during SUSPECT was never recorded

- Symptom. In the partition scenario the event log showed the failover but no `stall` event,
  although the manager's heartbeat write had been blocked for the whole detection window.
- How found. The Experiment 3 simulation asserted a stall event and failed.
- Fix. The stall check ran only in the reconcile pass, which a SUSPECT set skips. It now runs
  on every tick where a primary is known, before the verdict is acted on.

### The empty set is a subset of everything

- Symptom. A node whose replication was never configured has an empty retrieved and executed
  set. As a candidate it passes every subset check vacuously, and as the only reachable node
  it would be promoted with no data.
- How found. Named in the spec as a bug to expect, confirmed with a hypothesis fleet
  generator that includes unconfigured nodes.
- Fix. A node with no replication configured is not a candidate at all, the reason is kept in
  the choose step (`excluded`), and with no candidate left the set HALTs with
  "no promotable replica" instead of promoting.

### A torn last line in events.jsonl swallowed the next event

- Symptom. After a simulated crash mid-write, the next event appended after restart was
  glued onto the torn line, and both were skipped when the log was read back.
- How found. The event log round-trip test in tests/test_manager_client.py, written to check
  that a torn line does not stop startup, also appended after it.
- Fix. On open, the log checks the last byte and writes a newline if the file does not end
  with one. Every append is flushed and fsync'd.

### Lagging replicas voted the primary dead through the heartbeat

- Symptom. Under the 8-client workload on the real fleet, doctor said "2 of 2 replicas report
  heartbeat stale for 97.9 s" about a healthy primary. The replicas were 94 and 98 s behind.
  The primary survived only because the manager's own probe was passing. With the manager
  partitioned (Experiment 4) that would have been a false failover.
- How found. Reading `/v1/sets/rs1/doctor` on the real fleet during a five minute workload.
- Fix. The heartbeat row is read after the SQL thread applied it, so its age includes the
  apply lag. The vote now uses the age minus `Seconds_Behind_Source`. The IO thread and the
  TCP check still vote directly. Orchestrator calls this case
  UnreachableMasterWithLaggingReplicas and does not fail over either.

### Cold start false failover

- Symptom. After the whole fleet was stopped and started again with its volumes, rs2 failed
  over from mysql-b1 to mysql-b2 on startup. The manager believed mysql-b1 was primary (both
  replicas pointed at it), its probe failed because mysql-b1 had booted with
  super_read_only=1 from my.cnf, and both replicas voted because the heartbeat row was
  32545 s old from before the pause.
- How found. The coordinator watching `make up` after a pause.
- Fix. Two changes. At discovery, if no node is writable and the replicas agree on a source
  that is reachable, not fenced, not itself replicating, and holds every transaction any
  replica holds, the manager promotes it in place and records a `cold_start` event. A fenced
  node is never promoted this way. And a heartbeat older than ten detection windows is no
  longer a vote, since it was not written by a primary that was alive recently.

### Two bootstraps at once on make up

- Symptom. On `make up` the manager logged "no writable node, believing replicas' source"
  for rs1 and a SUSPECT with error 1290 (read-only) on mysql-a1, two seconds before it went
  HEALTHY.
- How found. The rs1 event log on the real fleet. `bin/bootstrap` had configured the
  replicas but not yet promoted mysql-a1 when the manager's first poll ran.
- Fix. It was harmless (the SUSPECT cleared and no action was taken), and with the cold start
  rule the manager now promotes mysql-a1 itself, which is idempotent with the script's
  promote. Only one of them should bootstrap: when the manager container runs, `make up`
  should leave bootstrapping to it.

### Switchover stalled writes until the first replica reattached

- Symptom. A planned switchover on the real fleet (rs1, mysql-a2 to mysql-a3, 4 clients)
  stalled clients for 3.8 s although fence, catch-up and promote took 0.9 s together. The
  new primary had semi-sync on and no replica connected, and the repoint of mysql-a1 spent
  2.4 s in `START REPLICA`.
- How found. Comparing the workload's first error and first success after the error with
  the step durations in the switchover event.
- Fix. The switchover now repoints every other node to the candidate before promoting it.
  The candidate is read-only and already holds all of the old primary's transactions, so
  replicas can attach early. The next switchover stalled clients for 0.89 s.

### Switchover refused because two snapshots were taken at different moments

- Symptom. `POST /v1/sets/rs1/failover {"to":"mysql-a2"}` answered 409 "mysql-a2 has 9
  transactions the primary lacks" while every node's gtid_executed was identical a second
  later.
- How found. The second real switchover under the workload.
- Fix. The errant transaction check compared the candidate's status with the primary's
  status read a few milliseconds earlier while writes were flowing. It now runs after the
  quiesce, against the final gtid_executed the fence returned, and rolls back by promoting
  the old primary again if the candidate really holds something extra. A unit test then
  showed that an empty final set ("") was treated as unknown and skipped the check, which
  is also fixed.

### A replacement replica was forgotten after a manager restart

- Symptom. After the manager container was rebuilt, rs1 status showed mysql-a4, which had
  been cloned in as a replacement, with role "spare", and a later switchover did not repoint
  it, so it kept replicating from the old primary.
- How found. `/v1/status` after restarting the dbguard container on the real fleet.
- Fix. Membership lived only in memory. The reconcile pass now adopts the spare as a member
  whenever it replicates from a member, so a restart rediscovers it.
