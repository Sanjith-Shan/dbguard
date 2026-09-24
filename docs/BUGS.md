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
