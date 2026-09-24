# Bugs

Every real bug found while building DBGuard. Symptom, how it was found, fix.

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
