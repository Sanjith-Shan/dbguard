# DBGuard design

DBGuard manages a small fleet of MySQL replica sets and fails a set over to a replica when its primary dies, hangs, or is cut off, without losing any write that a client was told had committed. It is a lab, six `mysqld` in Docker containers on one laptop as two replica sets of one primary and two replicas, plus a spare per set, one HAProxy, one manager and one agent beside each `mysqld`. Nothing here has run in a datacenter or seen real traffic. What it has is a harness that kills, freezes and partitions those containers under a write workload, and a checker that counts every acknowledged write afterwards. The choices below are argued from the MySQL 8.4 manual and from public write-ups credited at the end.

`docs/INTERFACES.md` is the contract the code is built against, `docs/CAPACITY.md` has the costs, `docs/RUNBOOK.md` the operator procedures and `docs/BUGS.md` what went wrong. A tag of the form `[[N: what, source file]]` marks a number whose rows had not landed when this was written. Numbers are from the campaign in `results/SUMMARY.md`, and `results/CONFIG_HISTORY.md` records the configuration each table ran under.

## 1. What DBGuard is and what it is not

DBGuard is three programs.

- `dbguard-agent` runs in each node container under `tini` and starts `mysqld` as its own child. It is the only thing that changes its node's role. It answers HAProxy's health check and can fence its node, by SQL if `mysqld` answers and by `SIGKILL` if it does not.
- `dbguard` is the manager. One process, one asyncio loop per replica set, one poller per node. It decides whether a primary is dead and drives the failover by calling agents.
- `dbgctl` is the operator CLI, with `status`, `doctor`, `failover`, `halt` and `resume`.

It is not a proxy, a query router, a sharding layer, a backup system, a consensus system or a MySQL patch. There is one manager, and if it dies nothing fails over (section 12).

## 2. The replica set

`rs1` is `mysql-a1`, `mysql-a2`, `mysql-a3` with spare `mysql-a4`, and `rs2` is the same with `b` names. Every node boots with `super_read_only=1` from `my.cnf`, so a node only ever becomes writable because the manager promoted it.

```
 client                     PRIMARY (mysql-a1)                          REPLICA (mysql-a2)
   |  INSERT ... COMMIT       +-----------------------+
   |------------------------->| (1) InnoDB prepare    |
   |                          | (2) write binlog,     |   binlog dump     +----------------------+
   |                          |     fsync (sync_binlog=1) -------------->| (3) IO (receiver)    |
   |                          |                       |   events          |     thread writes    |
   |                          | (5) waits for ack     |<------------------|     relay log, flush |
   |                          |     (AFTER_SYNC)      |   (4) semi-sync   |     then acks        |
   |                          | (6) InnoDB commit,    |       ack         |                      |
   |<-------------------------|     gtid_executed     |                   | (7) SQL (applier)    |
   |  OK                      |     advances          |                   |     workers apply    |
                              +-----------------------+                   |     relay log        |
                                                                          +----------------------+
```

At (2) a transaction is on the primary's disk but invisible. The manual says the replica "acknowledges receipt of a transaction's events only after the events have been written to its relay log and flushed to disk", which is (4). Only then, at (6), does the primary commit in InnoDB and answer OK. At (7), possibly much later, the replica's applier workers replay it. The gap between (4) and (7) is apply lag, and semi-sync does nothing to bound it.

A GTID names a transaction as the originating server's UUID plus a sequence number, and a GTID set is ranges per UUID.

```
3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:11:47-49
```

| Name | Where you read it | What it contains |
|---|---|---|
| `@@GLOBAL.gtid_executed` | any server | every transaction this server has committed |
| `Retrieved_Gtid_Set` | `SHOW REPLICA STATUS` | transactions the IO thread has received into the current relay log |
| `Executed_Gtid_Set` | `SHOW REPLICA STATUS` | the same as that replica's `gtid_executed` |

The retrieved set is not cumulative. `CHANGE REPLICATION SOURCE TO` and `relay_log_recovery` purge the relay log and it starts over, so what a replica holds is the union of its executed and retrieved sets, and that union is what DBGuard compares. With `SOURCE_AUTO_POSITION=1` a replica tells its new source which GTIDs it has and receives the rest, so no binlog file or offset is ever named, which is how the Meta GTID post repoints a recovered primary instead of copying data. `dbguard/gtid.py` compares sets as sets, never as strings, and `hypothesis` tests hold it to `GTID_SUBSET` and `GTID_SUBTRACT`.

## 3. Semi-synchronous replication

Asynchronous replication answers the client before shipping the binlog, so a crash in between loses an acknowledged write. Semi-sync adds a wait for at least one replica's ack, and `rpl_semi_sync_source_wait_point` chooses where.

With `AFTER_SYNC` (the default) the source syncs the binlog, waits for the ack, then commits to InnoDB and returns. The manual says that on source failure "all transactions committed on the source have been replicated to the replica (saved to its relay log)". With `AFTER_COMMIT` it commits to InnoDB first and waits afterwards, so other sessions can read a transaction no replica has, and the manual warns that after failover "it is possible for such clients to see a loss of data relative to what they saw on the source."

```
time -->          t0 binlog fsync     t1 crash (no ack yet)        t2 failover to replica

AFTER_SYNC
  writer          waiting             connection error             retries, may or may not find row
  other readers   cannot see row      -                            new primary has no row
  outcome         the write was never acknowledged and never visible. Nothing a client saw is lost.

AFTER_COMMIT
  writer          waiting             connection error             same as above
  other readers   CAN see row (t0+)   -                            row is gone on the new primary
  outcome         a reader acted on a row that no longer exists. That is a lost visible write.
```

DBGuard sets `AFTER_SYNC`, `rpl_semi_sync_source_wait_for_replica_count=1` and `rpl_semi_sync_source_timeout=3600000`, one hour.

The timeout is the part people get wrong. The manual default is 10000 ms, and on expiry "the source reverts to asynchronous replication" until "at least one semisynchronous replica catches up". Only `Rpl_semi_sync_source_status` going OFF tells you. A primary that loses its replicas for ten seconds then acknowledges writes that exist nowhere else. Matsunobu lists the timeout among the ways semi-sync quietly turns itself off and recommends an "infinite or very long timeout", and Heckel uses one hour. With an hour, a primary with no replica able to ack stops completing commits, the manager logs a `stall` event, and failover or a human deals with it. The stall is the design, an availability cost chosen on purpose and measured in `docs/CAPACITY.md`. `rpl_semi_sync_source_wait_no_replica` stays at its default of ON, because OFF reverts to async the moment the replica count drops, which would undo the hour.

**What lossless promises.** Every write that returned OK was in at least one replica's relay log on disk when it returned, so a failover that promotes the replica holding the most transactions, after it has applied its relay log, loses no acknowledged write. It is claimed only for acknowledged writes, only under this configuration, and only as measured, where DBGuard lost 0 acknowledged writes across 30 primary kills.

**What it does not promise.** With `wait_for_replica_count=1`, if the primary and the one replica that acked a write both die, or that replica is unreachable when the manager chooses, the write exists nowhere the manager can see and it is lost.

## 4. The agent

### Why it exists and why it supervises `mysqld`

Failover is only safe if the old primary has stopped accepting writes before the new one starts, and a manager across a network cannot guarantee that about a process it cannot reach. The agent is `mysqld`'s parent, so it can always stop it. That is STONITH ("shoot the other node in the head") at the process level instead of by cutting power to a host.

Supervising also means the agent knows the PID to kill and decides when `mysqld` comes back, always read-only from `my.cnf` since nothing uses `SET PERSIST`. And the clone plugin needs a supervisor. The manual says a recipient restarts automatically after a clone only if "a monitoring process" is there, and otherwise ends with error 3707 and a stopped server.

### The fence sequence

`POST /fence` in `dbguard/agent/core.py` does this, in this order.

1. Write the fence flag file `/var/lib/dbguard/fenced` (temp file, fsync, `os.replace`). From this instant `GET /primary` returns 503.
2. Close the agent's own pooled connections, then `SET GLOBAL super_read_only=1` with a 2 s deadline.
3. If that returned, kill every client thread that is not replication, and read the final `gtid_executed` to report back.
4. If it did not return, `SIGKILL` `mysqld` and report `method: kill`.

The flag goes first because the SQL may never return. A frozen process executes nothing, and a primary cut off from its replicas has sessions stuck waiting for an ack. The manual says enabling `read_only` "blocks while other clients have any ongoing statement ... or ongoing commit", and `docs/BUGS.md` records the `SET` waiting behind a heartbeat `INSERT` in the semi-sync wait, with `KILL` unable to release it. The only SQL that would is turning semi-sync off, which acknowledges writes no replica has. So a partitioned primary is always fenced by the kill path, which is why the agent owns the process. How often each path ran is 30 of 30 by kill, 0 by sql. The flag file survives an agent restart, so a fenced node stays fenced until `/promote` or `/unfence`.

### Why `super_read_only` and not `read_only`

`read_only` still lets any user with `CONNECTION_ADMIN` or `SUPER` write, per the manual, and admin accounts, migration tools and DBGuard's own user all hold that. A single stray admin write on a fenced primary is a transaction the new primary does not have. `super_read_only` blocks those users too, and replication threads keep working under it.

### The restart hold after a kill

`SIGKILL` works on a frozen process too. The transactions it had binlogged but not had acknowledged stay on disk, and crash recovery commits them at the next start. Those are the phantoms of section 7, and a restarted old primary would serve them to any replica still pointed at it, since replicas reconnect every second. So after a kill fence the agent holds `mysqld` down for 20 s (`DBGUARD_RESTART_HOLD_S`), and `/promote`, `/repoint` or `/rebuild` end the hold early once the manager has decided what the node is. Stopping the candidates' IO threads before comparing them (section 6) closes the same window from the other side.

### The wake guard

If the whole container is frozen, the agent is frozen with `mysqld` and the manager fails over around it. When it thaws, `mysqld` is still writable and clients that were mid-write resume. That is the woken-primary window.

A 500 ms ticker compares the monotonic clock against the last tick. A gap over 3 s means a freeze, and the agent asks the manager `GET /v1/sets/<rs>/primary`. If the answer is another node, or `null` because a failover is running, and this node is writable, it fences itself. `/primary` and the heartbeat writer wait for that check after a gap, so HAProxy cannot get a stale 200 and the heartbeat cannot add a phantom GTID. A 503 with `primary: unknown` means the manager has just started and not found the primary yet, and the agent then leaves state alone whatever the fence file says, because an earlier version fenced a healthy primary during exactly that window. An unreachable manager means fence only if the fence file exists. Writes that landed on a woken primary are not yet measured. The hang scenarios that count them were not part of this campaign, and four archived `hang-container` runs found the woken-primary rejoin divergence bug in `docs/BUGS.md`, now fixed, with the rerun pending.

### The self-fence lease

A primary cut off from the manager and every replica, but still reachable by clients, is held back only by semi-sync, and after the one-hour timeout it would fall back to async. So every second the primary's agent reads `Rpl_semi_sync_source_clients` and asks the manager who is primary. When the client count is 0 and the manager has been unreachable for 10 continuous seconds (`DBGUARD_SELF_FENCE_AFTER_S`), it fences itself and logs `self_fence`. Either observation alone resets the timer, which is why the manager-partition experiment does not trigger it. There the manager is partitioned from a healthy primary, both replicas stay connected, and the client count stays at 2.

## 5. Detection

### Why the manager's own probe is not enough

If the manager fails over because it cannot reach the primary, any fault on its own path fails over a healthy primary, which costs every in-flight transaction, seconds with no writer, a rejoin of the old primary, and a cooldown. Orchestrator's documentation separates `DeadMaster` ("All of master's replicas are failing replication") from `UnreachableMaster`, where the master still "has replicating replicas", which "does not make for a recovery process".

### The two conditions

`dbguard/manager/detector.py` declares a primary dead only when both hold continuously for `detect_window_s` (5 s).

1. The manager's probe failed `probe_failures` (3) times in a row, each within `probe_timeout_s` (1 s). The probe is `SELECT 1` plus a write to `dbguard.manager_probe`. It must include a write, because a primary cut off from its replicas answers `SELECT 1` instantly while every commit blocks on semi-sync.
2. A strict majority of the witnesses, and at least one, vote that the primary is gone. A witness is a replica the manager can reach whose `Source_Host` is the believed primary. It votes if its IO thread is not `Yes`, if its agent cannot open a TCP connection to the primary within 500 ms, or if the heartbeat row the primary's agent writes every 500 ms is stale.

The witness rule came from a bug in which the fenced old primary, replicating from nobody, voted the new primary dead through its own stale heartbeat row. Zero witnesses means nobody can confirm, and the set stays `SUSPECT`. A `DEGRADED` set with one live replica fails over on that single witness.

The heartbeat vote has two filters. The row is read after the replica applied it, so its age includes apply lag, and on the real fleet a healthy primary with replicas 94 and 98 s behind drew two votes. So the age counts only after subtracting `Seconds_Behind_Source`, as Orchestrator does with `UnreachableMasterWithLaggingReplicas`. And an age over ten detection windows does not vote, because after a whole-fleet restart a row 32545 s old made both replicas vote against a primary that had simply booted read-only.

Probe failure without the vote is `SUSPECT`, the manager is probably the one partitioned, and it does nothing. The vote without the probe is logged and left alone. Naive mode decides from the probe alone.

### What each failure looks like

| Injection | What TCP does | Manager probe | Replicas' view |
|---|---|---|---|
| `docker kill -s KILL` | the kernel closes the sockets, so peers get FIN or RST at once, then the address disappears | fails, agent unreachable too | IO thread errors at once and goes to `Connecting` |
| `SIGSTOP` of `mysqld`, or `docker pause` | nothing. The kernel still completes handshakes into the listen backlog and ACKs data, but no byte comes back | times out | no events and no heartbeats, then after `replica_net_timeout` the IO thread reconnects and hangs in `Connecting` |
| `iptables -j DROP` between primary and replicas | silent drop, retransmits into nothing, no RST | `SELECT 1` works, the write times out | as a hang, plus the agents' TCP connect to the primary fails |

`SOURCE_HEARTBEAT_PERIOD=0.5` makes a quiet primary send a heartbeat every half second, and `replica_net_timeout=2` makes the replica give up on a silent connection after two seconds instead of the default 60. A container hang uses `docker pause`, the cgroup freezer, because `docker kill -s STOP` signals only PID 1, which is `tini`, and SIGSTOP cannot be forwarded.

When the manager is the partitioned one, the probe fails and the replicas keep streaming, so the set sits in `SUSPECT`. DBGuard's false failover count there is 0 in 30 runs. The naive and Orchestrator runs of this scenario are not yet measured.

### States

Every set starts in `DISCOVERING` with no primary and leaves it through discovery, bootstrap or cold start. `HEALTHY` requires a reachable primary with `super_read_only=0` and every other member streaming from it with both threads `Yes`, so a set is never `HEALTHY` without a primary. Too few streaming replicas is `DEGRADED`. The others are `SUSPECT`, `FAILING_OVER`, `REBUILDING` and `HALTED`.

When every node boots read-only, which is what a whole-fleet restart looks like, and the replicas agree on a source that is reachable, not fenced, not itself replicating and holding everything any replica holds, the manager promotes it in place and records `cold_start`. Before that rule, `rs2` failed over from a primary that was merely rebooting.

## 6. Failover

In dbguard mode the steps are these, each timed into one event row (`dbguard/events.py`).

1. **Fence** the old primary through its agent with a 3 s deadline. The fence starts first and runs concurrently with steps 2 to 4 and the repoint.
2. **Stop IO threads.** `STOP REPLICA IO_THREAD` on every candidate over SQL, so nothing new arrives while they are compared. If any cannot be stopped, the manager waits for the fence before choosing.
3. **Choose** among reachable, responsive replicas that are configured and had semi-sync on. The winner holds the most transactions by `gtid_executed` union `Retrieved_Gtid_Set` (`dbguard/manager/selection.py`). Every other candidate's union must be a subset of the winner's, or the set goes to `HALTED`.
4. **Catch up.** Wait up to `catchup_deadline_s` (30 s) until the winner's retrieved set is a subset of what it applied, or, with its IO thread stopped, until `Replica_SQL_Running_State` says it has read all of its relay log. The second exit covers a partial transaction the dead source never finished sending, which would otherwise hold catch-up until the deadline. Its GTIDs are named in the event.
5. **Repoint** every other replica to the still read-only winner with `SOURCE_AUTO_POSITION=1`.
6. **Promote**, after waiting for the fence to finish or give up. `STOP REPLICA`, `RESET REPLICA ALL`, semi-sync replica off, semi-sync source on, then `super_read_only=0, read_only=0` as the last statement, then the fence flag is cleared and HAProxy sees 200.
7. **Record** the event, including the watermark, the winner's `gtid_executed` at promotion.

**Why fencing precedes promotion.** Two writable primaries mean HAProxy may send clients to either, and writes on the old side exist nowhere else. The fence runs concurrently with the steps before promotion because none of them can make a write acknowledged. Only promotion can, so only promotion waits. When the agent does not answer, the old primary is dead or cut off, and it cannot get an ack, because its replicas' IO threads are stopped and then repointed away.

**Why repoint before promote.** The spec promoted first. On the real fleet the new primary then had semi-sync on and no replica attached, so every commit stalled until the first repoint finished, and `START REPLICA` took 1.7 to 2.4 s. The winner is caught up and every other set is a subset of its own, so attaching replicas while it is read-only is safe. The same reorder cut a planned switchover's client stall from 3.8 s to 0.89 s (`docs/BUGS.md`). The failover version has not had a clean measured run yet.

**Why the union and why wait for the relay log.** An acknowledged write is guaranteed to be in a relay log, not applied, so a slow applier can hold the only copy of the last one, and the retrieved set alone resets on repoint. Promoting before the relay log is applied would let new writes land before old acknowledged ones. Orchestrator's `DelayMasterPromotionIfSQLThreadNotUpToDate` is the same idea.

**Why the subset check, and what `HALTED` means.** One primary feeding everyone the same binlog should give sets that form a chain. When they do not, someone wrote to a replica or an earlier failover went wrong, and either choice throws away transactions the other node has. So the manager records the failover with `new_primary: null` and sets `HALTED` with both differences in the reason, and a human decides (`docs/RUNBOOK.md`). A node with no replication configured is never a candidate, because the empty set is a subset of everything.

**Budgets and cooldown.** The agent bounds a whole `/promote` or `/repoint` to 30 s and answers 504 with the step it was on, and the manager waits 35 s, so it never gives up on a promote the agent is still finishing. After a failover the set will not fail over again for `cooldown_s` (20 s), like Orchestrator's `RecoveryPeriodBlockSeconds`.

Failover time, from injection to the first successful write through HAProxy, is 8.83 s median and 9.75 s p99 for a killed primary and 11.58 s median for one partitioned from its replicas.

## 7. Rejoin

**Phantom transactions.** A primary killed mid-commit can hold binlogged transactions that were waiting for an ack. No client got OK, but crash recovery commits them on restart. The manual says such a source "cannot be restarted in this scenario and must be discarded, because its binary log might contain uncommitted transactions that would cause a conflict with the replica when externalized after binary log recovery", and Heckel rebuilds it every time. If they survived, a later failover could promote that node and rows no client was told about would appear.

**Repoint or rebuild.** Always discarding is safe but expensive, and the Meta GTID post repoints a recovered primary when "our automation detects its data is consistent". DBGuard makes that check exact. Once the old primary's `mysqld` is back and crash recovery has run, the manager compares its `gtid_executed` with the new primary's. A subset means it has nothing the new one lacks, and it is repointed with auto-positioning, with no data copy. Otherwise `GTID_SUBTRACT(old, new)` is the phantom set, its count is logged, and the node is rebuilt by clone. The split was 5 repoint, 25 rebuild, with 4 (median) and 8 (max) phantoms per rebuild. `rejoin: manual` records the need and does nothing else. In every mode, a member that is writable while it is not the primary is fenced first.

**How the clone works.** `POST /rebuild` sets `clone_valid_donor_list` and runs `CLONE INSTANCE FROM` the donor. The plugin copies InnoDB data physically and by default "removes existing user-created data (schemas, tables, tablespaces) and binary logs from the recipient data directory", which destroys the phantoms. It transfers the donor's `gtid_executed`, the agent restarts `mysqld`, and the manager repoints it. CLONE refuses a `super_read_only` recipient, so the agent clears it just before the clone while a `rebuilding` flag keeps `/primary` at 503 and the heartbeat, wake guard and lease off. The manual's prerequisites (same series, platform, active plugins and character set, one clone at a time, disk for the copy) are why every node loads the same plugins from `my.cnf`.

**Why the donor is a replica.** A clone reads the whole dataset and blocks DDL on the donor, which should not land on the node serving writes. A replica has the same data minus a few seconds, and the manual confirms auto-positioned channels "can resume replication automatically after the cloning operation". Also, a clone from a primary stalled on semi-sync never finished.

## 8. Replacement and the fleet

With two replicas and `wait_for_replica_count=1`, a set that lost one replica still commits, but it has no margin, and losing the survivor stalls every write. If a set stays `DEGRADED` for longer than `rebuild_after_s`, the manager provisions a replacement. In the lab that is starting the spare container, cloning it from a healthy replica and repointing it, and a restarted manager adopts a spare that replicates from a member. In a real fleet this is a call to the host allocator, the one place DBGuard stands in for infrastructure it does not have. The lab uses 120 s, not the spec's 60, because the hang experiments freeze the old primary for 90 s and a replacement must not start before it thaws. A real fleet picks the number from how long a host takes to reboot. No replacement starts while a set is `FAILING_OVER`, `SUSPECT` or `HALTED`, or within the cooldown. The replacement took 7.53 s on 200.0 MB of data, at 128.3 MB/s.

Two sets make "fleet" true and expose bugs in per-set state. Every chaos run against `rs1` counts state changes in `rs2`, which must be zero, and measured 58.

## 9. Operations

**HAProxy is the endpoint and never decides.** Each backend health-checks every node's agent with `GET /primary`, which answers 200 only if the fence flag is clear, `super_read_only` is 0 and `mysqld` answered within a second, so fencing and promotion are each one state change on one agent. `on-marked-down shutdown-sessions` closes every proxied connection to a server the moment it is marked down, so clients on a fenced primary reconnect through the proxy.

**Metrics and doctor.** The manager and every agent serve Prometheus `/metrics` (state per set, failovers by outcome, step durations, phantoms discarded, rebuilds, `Rpl_semi_sync_source_tx_avg_wait_time`). `dbgctl doctor` states the verdict, the evidence, what the manager would do next, and the `GTID_SUBTRACT` gap from the primary to every node, and while discovering it says no primary has been found yet.

**Planned switchover.** `dbgctl failover rs1 [--to node]` quiesces the old primary with the same fence, repoints everyone to the candidate, waits for it to apply everything, checks against the final `gtid_executed` the fence returned that the candidate holds nothing extra, and promotes. If anything fails before promotion the old primary is promoted back. The client stall is 1.44 s median and 6.03 s p99.

**How the harness measures.** Failover ends at the first acknowledged write whose request started after the first error, because eight clients timestamp independently and a write committed just before a kill could otherwise end a failover 3 ms after it began. Single-writer is sampled every second from every agent during the run, and the old primary is probed directly even after it left the set, because it is the node that can break the property.

**The binlog tmpfs.** The disk-full scenario fills a 256 MB tmpfs holding one node's binlog, since filling a datadir would fill the VM disk every container shares. It is opt-in (`DBGUARD_BINLOG_DIR`, an overlay for `mysql-a1` only) because a tmpfs is wiped when its container stops. A killed primary would come back without its binlog, crash recovery would roll back its prepared transactions instead of committing them, and the phantoms the kill experiments measure would vanish.

**The agent token.** The agent API can fence, promote, rebuild and kill `mysqld`, so anything that can reach port 8080 can take a set down. `DBGUARD_AGENT_TOKEN` is the minimum guard against that: when set, every agent route except `GET /health`, `GET /metrics` and `GET /primary` (HAProxy must keep checking without it) needs a shared bearer token, compared in constant time, and refusals are counted in `dbguard_agent_unauthorized_total`. It stops a misconfigured script or a stray container from fencing a primary. It does not protect against anyone who can read the traffic, because the lab speaks plaintext HTTP on the compose network, and one token shared by the manager, the harness and the hook gives no per-caller identity, so a refusal cannot say who tried and a leaked token cannot be revoked for one caller. It is off by default and every published number ran without it. A real fleet would put the agents behind mutual TLS with service identities (SPIFFE-style certificates or the platform's workload identity), authorize by caller (only the manager may promote, only operators may kill), and rotate credentials without a restart.

## 10. The baselines

**Naive mode.** `--mode naive` runs replication asynchronously, detects from the probe alone, does not fence before promotion, promotes the largest executed set and skips the subset check. The primary acknowledges before any replica has the write, so a well-timed kill loses it. On one host it lost 0 acknowledged writes in 30 kills, because replication between containers on one machine finishes before a kill can land in the window, so the lab hides the async window rather than showing async is safe. Under a host-death injection with a 2 ms replication delay the baseline lost 1160 acknowledged writes in 10 kills and DBGuard lost 0 in 10 under the identical injection. Naive mode has no safety net at all, it records a split_brain event when two nodes are writable and leaves them alone, so the harness can count writes on a woken primary.

**Orchestrator.** openark/orchestrator, originally by Shlomi Noach, is the industry tool and the model for DBGuard's detection. The baseline runs Percona's maintained fork, because upstream issues `SHOW SLAVE STATUS`, which MySQL 8.4 removed. Its pre-failover hook flips the agent's fence flag, and the agents' lease and wake guard are off for its runs. It discovers arbitrary topologies, "attempts to promote a replica that will retain the most serving capacity", and has datacenter-aware rules, graceful takeover, a UI and a raft HA mode. DBGuard, by default and unlike it, fences the old primary itself down to `SIGKILL`, refuses to promote across diverged replicas, decides repoint versus rebuild by GTID subset, rebuilds with clone, and provisions replacements. Its runs are built but not yet measured, and when they are, any run where it was faster will be reported.

## 11. The cost of losslessness

Semi-sync adds a round trip and a replica relay log fsync to every commit, and turns "no replica can ack" into "no commit completes". `docs/CAPACITY.md` has the table. The headline cost is +1.6 ms (5.9 ms on vs 4.2 ms off) at the median and +16.7 ms (43.1 ms on vs 26.5 ms off) at p99, and the stall with no replica lasted 22.44 s. The hidden cost is apply. Semi-sync bounds receipt, and failover waits for apply. With 4 applier workers the replicas fell 44 s behind in five minutes, past the 30 s catch-up deadline, and 16 workers got `mysqld` OOM-killed at a 700 MiB limit, so the fleet runs 8 workers in 1 GiB.

## 12. What DBGuard cannot do

- It cannot survive the double loss of section 3. Three replicas with `wait_for_replica_count=2` close that window at a cost, and that is a stretch item.
- It cannot protect a write acked by a replica it cannot reach when it chooses. Refusing to promote because an unseen replica might be ahead would turn every replica outage into a stuck set.
- It has one manager. If it dies nothing fails over, which is safe because no action cannot lose data, and the self-fence lease still protects a primary cut off from everything but its clients.
- The lease itself trusts two observations, and a primary that keeps one semi-sync replica but loses the manager is left alone by design.
- It sees only GTID sets, so corruption, filters and drift that GTIDs do not show are invisible.
- It has only run in Docker on one laptop, where every network is a Linux bridge and every disk is the same disk.

Out of scope, and not argued back in, are sharding and query routing, a proxy other than HAProxy, backups beyond the clone plugin, a MySQL fork, a web UI, multi-manager consensus or Raft, Postgres, Kubernetes operators, and the online schema change tool before the stretch milestone.

## 13. Credits and sources

The approach is assembled from the following, all read while building it.

- Evan Elias and Santosh Praneeth, Meta Engineering, *Lessons from deploying MySQL GTID at scale*, 2014-09-18. GTID with semi-sync, promotion "within 30 seconds without losing data", repointing a recovered primary when its data is consistent, and the `gtid_purged` and auto-position bugs. <https://engineering.fb.com/2014/09/18/core-infra/lessons-from-deploying-mysql-gtid-at-scale/>
- Yoshinori Matsunobu, *Semi-Synchronous Replication at Facebook*, 2014-04. Loss-less semi-sync, the ways semi-sync silently turns itself off, the advice to use a very long timeout, semisync binlog servers, and the commit latency cost. <http://yoshinorimatsunobu.blogspot.com/2014/04/semi-synchronous-replication-at-facebook.html>
- Philipp C. Heckel, *Lossless MySQL semi-sync replication and automated failover*, 2021-10-19. `AFTER_SYNC`, a one-hour timeout, one ack, fencing through the proxy before promotion with Orchestrator, and discarding the failed source. <https://heckel.io/blog/lossless-mysql-semi-sync-replication-and-automated-failover/>
- MySQL 8.4 Reference Manual, *Semisynchronous Replication* and its subpages on installation, configuration and monitoring. <https://dev.mysql.com/doc/refman/8.4/en/replication-semisync.html>, <https://dev.mysql.com/doc/refman/8.4/en/replication-semisync-installation.html>, <https://dev.mysql.com/doc/refman/8.4/en/replication-semisync-interface.html>, <https://dev.mysql.com/doc/refman/8.4/en/replication-semisync-monitoring.html>
- MySQL 8.4 Reference Manual, *Replication Source Options and Variables* (the definitions of `rpl_semi_sync_source_wait_point`, `_timeout`, `_wait_for_replica_count` and `_wait_no_replica`). <https://dev.mysql.com/doc/refman/8.4/en/replication-options-source.html>
- MySQL 8.4 Reference Manual, *GTID Format and Storage*. <https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html>
- MySQL 8.4 Reference Manual, *Functions Used with Global Transaction Identifiers* (`GTID_SUBSET`, `GTID_SUBTRACT`, `WAIT_FOR_EXECUTED_GTID_SET`). <https://dev.mysql.com/doc/refman/8.4/en/gtid-functions.html>
- MySQL 8.4 Reference Manual, *The Clone Plugin*, *Cloning Remote Data* and *Cloning for Replication*. <https://dev.mysql.com/doc/refman/8.4/en/clone-plugin.html>, <https://dev.mysql.com/doc/refman/8.4/en/clone-plugin-remote.html>, <https://dev.mysql.com/doc/refman/8.4/en/clone-plugin-replication.html>
- MySQL 8.4 Reference Manual, *Server System Variables*, `read_only` and `super_read_only`. <https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html#sysvar_super_read_only>
- MySQL 8.4 Reference Manual, *CHANGE REPLICATION SOURCE TO* and *Replica Server Options and Variables*, for `SOURCE_HEARTBEAT_PERIOD` and `replica_net_timeout`. <https://dev.mysql.com/doc/refman/8.4/en/change-replication-source-to.html>, <https://dev.mysql.com/doc/refman/8.4/en/replication-options-replica.html>
- MySQL 8.4 Reference Manual, *How MySQL Handles a Full Disk* and `binlog_error_action`. <https://dev.mysql.com/doc/refman/8.4/en/full-disk.html>, <https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html>
- openark/orchestrator documentation, *Failure detection*, *Topology recovery* and *Configuration, recovery*. The `DeadMaster` versus `UnreachableMaster` split that DBGuard's detection copies. <https://github.com/openark/orchestrator/blob/master/docs/failure-detection.md>, <https://github.com/openark/orchestrator/blob/master/docs/topology-recovery.md>, <https://github.com/openark/orchestrator/blob/master/docs/configuration-recovery.md>
- Peter Alvaro and Kyle Kingsbury, Jepsen, *MySQL 8.0.34*, 2023-12-19. The model for checking a recorded history against the database afterwards instead of trusting the system under test. That analysis checks isolation with Elle and states that it "made no attempt to promote nodes from secondaries to primaries", so it does not cover failover. DBGuard's checker borrows its discipline of classifying every operation as acknowledged, failed or indeterminate, and applies it to a narrower property, that no acknowledged insert is missing after failover. <https://jepsen.io/analyses/mysql-8.0.34>
