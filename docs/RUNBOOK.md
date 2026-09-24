# DBGuard runbook

Procedures for the person on call for a DBGuard fleet. Each entry has the same four parts, the symptom, how to confirm it, what DBGuard did or will do, and what the human does. Commands assume the lab's published ports (`docs/INTERFACES.md`). In the lab, run SQL with `docker compose exec <node> mysql -udbguard -pdbguard`, or from the host against port 1331X for `a` nodes and 1332X for `b` nodes.

The first command in every entry is the same.

```
dbgctl status
dbgctl doctor rs1
```

`doctor` prints the verdict, the evidence behind it, what the manager would do next, and the GTID gap between the primary and every other node. If it and the rest of this runbook disagree, believe the raw SQL and file a bug in `docs/BUGS.md`.

Fields that come up everywhere, in `SHOW REPLICA STATUS\G` on a replica.

| Field | Meaning |
|---|---|
| `Replica_IO_Running` | `Yes`, `Connecting` or `No`. The receiver thread that pulls binlog into the relay log |
| `Replica_SQL_Running` | `Yes` or `No`. The applier thread |
| `Source_Host` | who this replica pulls from. Must be the set's primary |
| `Retrieved_Gtid_Set` | transactions received into the relay log |
| `Executed_Gtid_Set` | transactions applied, equal to `@@gtid_executed` |
| `Seconds_Behind_Source` | applier lag estimate. `NULL` when the SQL or IO thread is stopped |
| `Last_IO_Error`, `Last_SQL_Error` | the last error text for each thread |
| `Auto_Position` | must be 1 |

And on the primary.

```
SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_%';
-- Rpl_semi_sync_source_status        ON while semi-sync is operational, OFF if disabled or fell back to async
-- Rpl_semi_sync_source_clients       semi-sync replicas currently connected
-- Rpl_semi_sync_source_wait_sessions sessions currently waiting for a replica ack
-- Rpl_semi_sync_source_no_tx         commits NOT acknowledged by a replica (must not grow)
-- Rpl_semi_sync_source_no_times      times the source turned semi-sync off (must stay 0)
-- Rpl_semi_sync_source_tx_avg_wait_time  microseconds per transaction spent waiting for the ack
SELECT @@super_read_only, @@read_only, @@gtid_executed;
```

---

## 1. A set is HALTED because replicas diverged

**Symptom.** `dbgctl status` shows `HALTED` with a reason starting `replicas diverged`. There is no writable primary, or the old one is fenced. HAProxy shows the backend with no server up. Writes to that set fail. The other set is unaffected.

**Confirm.**

```
dbgctl doctor rs1                       # names the two nodes and prints both GTID_SUBTRACTs
curl -s localhost:19090/v1/events?rs=rs1 | jq '.events[-1]'   # steps.choose.subset_ok == false
```

On each replica, read `Executed_Gtid_Set` and `Retrieved_Gtid_Set`, then compute the difference in both directions on any server.

```
SELECT GTID_SUBTRACT('<set of a2>', '<set of a3>') AS only_on_a2,
       GTID_SUBTRACT('<set of a3>', '<set of a2>') AS only_on_a3;
```

Look at which UUID the extra GTIDs carry. `SELECT @@server_uuid` on each node. A GTID with a replica's own UUID means someone wrote directly to that replica. A GTID with an old primary's UUID means one replica received more from it than the other, which is the normal case and should never halt, so check whether the chosen winner was the one with the larger retrieved set.

**What DBGuard did.** Fenced the old primary, found that no candidate's set contained every other candidate's, and stopped rather than pick one. It will not act on this set again until `dbgctl resume rs1`.

**What the human does.**

1. Decide which node's history is the truth. Usually it is the node that holds every GTID from the old primary's UUID, and the extra transactions on the other are local writes that should never have happened.
2. Inspect the extra transactions before discarding them. `mysqlbinlog --include-gtids='<extra set>' <binlog files>` on the node that has them shows the row events. Save the output.
3. Promote the chosen node by hand with `dbgctl failover rs1 --to <node>`, or through the agent's `/promote` if the manager refuses.
4. Rebuild the other node with the agent's `/rebuild` from a healthy donor. Repointing it would either error or keep the extra transactions.
5. `dbgctl resume rs1`, then confirm `doctor` shows every gap empty.
6. Write down who wrote to the replica. `super_read_only` should have made that impossible, so find out how.

---

## 2. The primary is stalling writes because no semi-sync replica is acking

**Symptom.** Writes through HAProxy hang rather than fail. Clients time out. Reads still work. The manager has logged a `stall` event. The set may be `DEGRADED`.

**Confirm.**

```
SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_clients';        -- 0, or fewer than needed
SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_wait_sessions';  -- growing
SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_status';         -- still ON, it has not fallen back
SHOW PROCESSLIST;   -- client sessions sitting in a semi-sync ACK wait state
```

On each replica, `SHOW REPLICA STATUS\G` for the IO thread state and `SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_replica_status'`. If a replica is up but `Rpl_semi_sync_replica_status` is OFF, its IO thread was started before semi-sync was enabled. The manual says a replica enabled at runtime keeps using asynchronous replication until the IO thread is restarted.

**What DBGuard did or will do.** Nothing that would unblock the commits by weakening the guarantee. The timeout is one hour on purpose, so the primary does not quietly fall back to asynchronous replication. If the replicas are down, the manager replaces one after `rebuild_after_s`. If the primary is partitioned from replicas that are otherwise healthy, the replicas vote, the probe write fails, and the manager fails over.

**What the human does.**

1. Get a replica acking. Restart a dead replica container, or fix its IO thread (`STOP REPLICA IO_THREAD; START REPLICA IO_THREAD;` on the replica, via the agent's `/configure` so the semi-sync variables are re-asserted first).
2. Do not set `rpl_semi_sync_source_enabled=0` or shorten the timeout to get writes flowing. That converts every write from now on into one that can be lost, and the business owner has to agree to that explicitly. If they do, write it down with the time, because every write between then and re-enable is outside the guarantee.
3. Once `Rpl_semi_sync_source_clients` is at least 1, the waiting sessions complete on their own.

---

## 3. A replica is lagging

**Symptom.** `dbgctl status` shows a lag for one replica, or `doctor` reports a growing gap. Commits are unaffected as long as the other replica acks.

**Confirm.** On the replica, `SHOW REPLICA STATUS\G`. If `Retrieved_Gtid_Set` is close to the primary's `gtid_executed` but `Executed_Gtid_Set` is behind, the applier is slow (receipt is fine, applying is not). If `Retrieved_Gtid_Set` is behind too, the IO thread or the network is the problem. The heartbeat row gives lag independent of `Seconds_Behind_Source`.

```
SELECT TIMESTAMPDIFF(MICROSECOND, ts, NOW(6))/1e6 AS hb_age_s FROM dbguard.heartbeat WHERE rs='rs1';
SELECT GTID_SUBTRACT('<primary gtid_executed>', @@gtid_executed) AS missing;
```

Then on the replica host, `docker stats`, and inside the container `top -H -p $(pidof mysqld)` to see whether the SQL thread is CPU bound, and `df -h /var/lib/mysql`.

**What DBGuard did or will do.** Nothing, as long as the replica is receiving. A lagging replica is still a valid failover candidate, because selection uses the retrieved set and step 3 waits for it to apply. It does extend failover time if it wins, up to `catchup_deadline_s`.

**What the human does.** Find the cause (a large transaction, a missing index on a row-based apply, a starved container). If it is chronic, it belongs in the capacity plan, not in a tweak.

---

## 4. Clone is failing

**Symptom.** A rebuild or replacement event with an error, the set stuck in `REBUILDING`, or the agent log showing `CLONE INSTANCE` failing.

**Confirm.** On the recipient.

```
SELECT STATE, ERROR_NO, ERROR_MESSAGE, SOURCE FROM performance_schema.clone_status;
SELECT STAGE, STATE, DATA, NETWORK FROM performance_schema.clone_progress;
SELECT @@clone_valid_donor_list;
```

The usual causes, each from the manual's prerequisites.

| Cause | Check |
|---|---|
| Plugin list differs | `SELECT PLUGIN_NAME, PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_STATUS='ACTIVE'` on donor and recipient and diff them. Every active donor plugin must be active on the recipient |
| Donor not in the list | `clone_valid_donor_list` must contain `donor:3306` exactly as the recipient connects |
| Version or platform mismatch | `SELECT @@version, @@version_compile_os, @@version_compile_machine` on both |
| Privileges | the donor user needs `BACKUP_ADMIN`, the recipient user `CLONE_ADMIN` |
| Disk | `df -h /var/lib/mysql` on the recipient. It needs room for the whole donor dataset |
| Another clone running | only one clone may run at a time |
| `max_allowed_packet` | at least 2 MB on both |
| Error 3707 | not a failure. The clone completed and `mysqld` was not restarted by a supervisor. The agent should have restarted it, so check `supervisor` lines in the agent log |

**What DBGuard did or will do.** Retries a rebuild on the next loop, choosing a donor among healthy replicas. It never falls back to cloning from the primary without a human saying so.

**What the human does.** Fix the mismatch. Plugin mismatches usually mean someone ran `INSTALL PLUGIN` on one node by hand, and the fix is to make `my.cnf` the only source of plugin loading. Then trigger `POST /v1/sets/rs1/rejoin` or wait for the next replacement attempt.

---

## 5. The manager is down

**Symptom.** `dbgctl status` cannot connect. `curl localhost:19090/health` fails. Nothing fails over.

**Confirm.** `docker compose ps dbguard`, `docker compose logs --tail 200 dbguard`. Under systemd, `systemctl status dbguard` and `journalctl -u dbguard -n 200`.

**What DBGuard did or will do.** Nothing, and that is the safe state. Every set behaves as if `HALTED`. Replication and semi-sync keep running without the manager, so a healthy set keeps serving and keeps its guarantee. The agents keep answering HAProxy on their own. A woken primary whose agent cannot reach the manager fences itself only if its fence file already exists.

**What the human does.** Restart the manager. Before relying on it, check `dbgctl status` shows every set in the state you expect. If a primary died while the manager was down, the manager handles it on its first loops after start. If you must fail over by hand while it is down, use the agents directly in the documented order, fence first (`POST :80XX/fence` on the old primary), compare retrieved sets with `GTID_SUBSET`, wait for the winner to apply its relay log, `/promote` the winner, `/repoint` the others. Never skip the fence or the subset check because you are in a hurry.

---

## 6. A node keeps getting fenced

**Symptom.** A node's role flips to `fenced` repeatedly, or a newly promoted primary is fenced shortly after promotion.

**Confirm.** The agent's log for `fence_flag`, `wake_gap` and `fence` lines, with their `reason`. `cat /var/lib/dbguard/fenced` in the container to see whether the flag file exists. `dbgctl status` for what the manager thinks the primary is.

```
docker compose logs mysql-a2 | grep -E 'wake_gap|fence|guard'
curl -s localhost:19090/v1/sets/rs1/primary
```

Common causes. `wake_gap` lines mean the agent's loop is stalling for more than 3 s, from a starved container or the host sleeping, and the wake guard is asking the manager and being told another node is primary. A manager whose view of the primary is stale after a manual change will tell a legitimate primary to fence itself. A stale flag file from a previous fence survives restarts by design.

**What DBGuard did or will do.** Each fence is the agent doing its job given what the manager told it. The manager does not unfence nodes except through `/promote`.

**What the human does.** If the manager's idea of the primary is wrong, fix that first (`dbgctl failover` to the node you want, or `halt` and investigate). If the host is starving the agent, fix the host. Clear a stale flag only with `POST /unfence` on a node you have confirmed should be writable, never by deleting the file by hand while the agent runs.

---

## 7. HAProxy shows no server up

**Symptom.** Clients get connection refused or immediate disconnects on 13306. The stats page at `http://localhost:18404` shows every server in the `rs1` backend red.

**Confirm.**

```
for p in 18011 18012 18013; do curl -s -w ' %{http_code}\n' localhost:$p/primary; done
curl -s 'localhost:18404/<stats uri from haproxy.cfg>;csv' | cut -d, -f1,2,18,37   # pxname, svname, status, check_status
```

Every agent answering 503 means no node believes it is the unfenced writable primary. The `role` in each body says why (`replica`, `fenced`, `unknown`).

**What DBGuard did or will do.** This is the expected state during a failover, between fence and promote, and during `HALTED`. It is not expected in `HEALTHY`.

**What the human does.** If the set is `HEALTHY` and still no server is up, check whether HAProxy can reach the agents at all (`docker compose exec haproxy wget -qO- mysql-a1:8080/primary`), whether the primary's `mysqld` is answering its agent within 1 s, and whether `super_read_only` was set on the primary by hand. If the set is `FAILING_OVER` for more than the failover budget in `docs/CAPACITY.md`, read the last event to see which step it is in.

---

## 8. The woken primary

**Symptom.** An old primary that was frozen (container paused, VM suspended, long GC-like stall) comes back after a failover and still has `super_read_only=0`.

**Confirm.** On the old node, `SELECT @@super_read_only`. In its agent log, a `wake_gap` line followed by a guard decision. On HAProxy, confirm the node is down. The checker's `writes_on_woken_primary` counts rows written to it after the failover.

**What DBGuard did or will do.** The agent sees the monotonic clock gap on its next tick, asks the manager who the primary is, and fences itself before answering HAProxy. HAProxy's `on-marked-down shutdown-sessions` cuts any client connections that survived the freeze. The manager then runs the rejoin logic, repoint if its `gtid_executed` is a subset of the new primary's, rebuild otherwise.

**What the human does.** Verify there were no writes on the woken node after the failover timestamp (`GTID_SUBTRACT` of its `gtid_executed` against the new primary's, restricted to its own UUID). If there were, they are writes that clients may have seen acknowledged and that the set does not have. Export them with `mysqlbinlog --include-gtids` before the rebuild destroys them, and raise it as a bug.

---

## 9. Disk full

**Symptom.** Writes stall or the primary restarts. `df` shows the data volume at 100 percent.

**Confirm.** `df -h /var/lib/mysql` in the container, the MySQL error log for disk-full messages, `dmesg | tail` on the host for filesystem errors, and `du -sh /var/lib/mysql/*binlog*` to see how much is binary log.

**On the primary.** Per the manual, MySQL waits on a full disk and rechecks every minute, and this also applies to binlog writes. If a binlog write, flush or sync fails outright, `binlog_error_action=ABORT_SERVER` (the default) shuts the server down. Either way commits stop. The manager's probe write fails, and because the agent's heartbeat write stalls too, the replicas see the heartbeat go stale and vote once it is older than `detect_window_s`. Expect a failover. Which of the two behaviours the lab showed is `[[N: disk-full primary behaviour and failover outcome, results/disk-full_dbguard.jsonl]]`.

**On a replica.** The IO thread cannot write the relay log, so it stops acknowledging. With two replicas the other one acks and commits continue. With one, the primary stalls (entry 2). The replica shows `Last_IO_Error` or `Last_SQL_Error` mentioning the disk.

**What the human does.** Free space without deleting files MySQL owns. On the primary, `PURGE BINARY LOGS BEFORE NOW() - INTERVAL 1 DAY` only after checking that every replica's `Retrieved_Gtid_Set` covers what is being purged, because purging GTIDs a replica still needs breaks its auto-positioning. On a replica, rebuilding it with clone onto a larger volume is often faster than cleaning up.

---

## 10. Planned switchover

**When.** Maintenance on the primary's host, a MySQL minor upgrade, moving the primary.

**Procedure.**

1. `dbgctl status`. The set must be `HEALTHY` with both replicas at `Replica_IO_Running: Yes` and `Rpl_semi_sync_source_clients` at least 1.
2. `dbgctl doctor rs1`. Every gap should be small or empty.
3. `dbgctl failover rs1 --to mysql-a2` (or without `--to` to let the manager choose). The manager sets `super_read_only` on the old primary, waits for the candidate to apply everything, promotes it, and repoints the old primary and the other replica.
4. `dbgctl status` shows the new primary and `HEALTHY`. The event row has the stall duration.
5. Clients see one short stall, `[[N: switchover stall p50, results/switchover_dbguard.jsonl]]` median, and a single retry covers it.

**Abort.** If step 3 fails before promotion, the old primary is read-only and nothing is promoted. Calling `/promote` on the old primary's agent clears its fence flag and `super_read_only` and puts it back in service. Then `dbgctl resume rs1`.

---

## 11. Replacing a host by hand

1. `dbgctl halt rs1` if the host is the primary, after a switchover away from it. Never replace the current primary directly.
2. Stop the old node's container or host.
3. Start the new node with the same `my.cnf` and plugin list, empty data directory, `super_read_only=1`.
4. On the new node's agent, `POST /rebuild {"donor":"mysql-a3"}` using a healthy replica as donor.
5. `POST /repoint {"source":"<current primary>"}`.
6. Confirm `SHOW REPLICA STATUS\G` shows both threads `Yes`, `Auto_Position: 1`, and `Rpl_semi_sync_replica_status` ON.
7. Update `fleet.yaml` if the node name changed, restart the manager, `dbgctl resume rs1`.

---

## 12. "Writes are failing, go"

In order. Stop at the first step that explains it.

1. **Scope.** `dbgctl status`. One set or both? If both, suspect HAProxy, the network, or the clients, not MySQL.
2. **Verdict.** `dbgctl doctor rs1`. Is a failover in progress, is the set `HALTED`, `SUSPECT`, `DEGRADED`? Read the last event.
3. **Failing or hanging?** A fast error points at routing or read-only. A hang points at semi-sync waits or locks.
4. **The endpoint.** HAProxy stats at `:18404`. Is exactly one server up in the backend? None up is entry 7. Two up is a bug and needs `dbgctl halt` now.
5. **The primary's role.** On the node HAProxy points at, `SELECT @@super_read_only, @@read_only`. A 1 here with HAProxy still sending traffic means a fence just landed or someone set it by hand.
6. **Semi-sync.** `SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_%'`. `clients` at 0 and `wait_sessions` growing is entry 2. `status` OFF means the source fell back to async and the guarantee is off, which should never happen with the one-hour timeout and deserves a bug.
7. **Replication.** `SHOW REPLICA STATUS\G` on each replica. IO thread state, `Last_IO_Error`, `Source_Host` pointing at the right node.
8. **Agent logs.** `docker compose logs --since 10m mysql-a1` for `fence`, `wake_gap`, `sql=` lines with long `duration_ms`.
9. **Connections on the primary.** Inside the container, `ss -tnp` to see who is connected to 3306. Many connections from HAProxy in `ESTAB` with nothing moving, or none at all. `ss -tn state syn-recv` for a full backlog. `SHOW PROCESSLIST` for what those sessions are waiting on.
10. **The kernel.** `dmesg | tail -50` on the host for OOM kills of `mysqld`, filesystem errors, dropped packets.
11. **Disk.** `df -h` and `df -i` on the primary and each replica. Entry 9.
12. **Still nothing.** Take a snapshot of the evidence (`doctor`, `SHOW REPLICA STATUS` from every node, the last events) before changing anything, then `dbgctl halt` the set so automation does not act on a situation you do not understand, and fix by hand using the entries above.
