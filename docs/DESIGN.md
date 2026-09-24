# DBGuard design

DBGuard manages a small fleet of MySQL replica sets and fails a set over to a replica when its primary dies, hangs, or is cut off, without losing any write that a client was told had committed. It is a lab. It runs as six `mysqld` in Docker containers on one laptop, two replica sets of one primary and two replicas each, plus one HAProxy, one manager process and one small agent beside each `mysqld`. Nothing here has run in a datacenter, and nothing here has seen real traffic. What it does have is a harness that kills, freezes and partitions those containers while a workload writes through the proxy, and a checker that counts every acknowledged write afterwards. The design choices below are argued from the MySQL 8.4 manual and from public write-ups by people who run MySQL at scale, which are credited at the end.

`docs/INTERFACES.md` is the contract the code is built against, `docs/CAPACITY.md` has the costs, `docs/RUNBOOK.md` the operator procedures and `docs/BUGS.md` what went wrong. A tag of the form `[[N: what, source file]]` marks a number that has not been measured yet.

## 1. What DBGuard is and what it is not

DBGuard is three programs.

- `dbguard-agent` runs as the main process of each node container and starts `mysqld` as its own child. It is the only thing that changes its node's role. It answers HAProxy's health check and it can fence its node, by SQL if `mysqld` answers and by `SIGKILL` if it does not.
- `dbguard` is the manager. One process, one asyncio loop per replica set. It polls every `mysqld` and every agent, decides whether a primary is dead, and drives the failover by calling agents in order.
- `dbgctl` is the operator CLI, with `status`, `doctor`, `failover`, `halt` and `resume`.

It is not a proxy, a query router, a sharding layer, a backup system, a consensus system or a MySQL patch. There is one manager. If the manager dies nothing fails over, and that is the intended safe state (section 12).

## 2. The replica set

A replica set is one primary that accepts writes and some replicas that copy them. In DBGuard `rs1` is `mysql-a1`, `mysql-a2`, `mysql-a3` with a spare `mysql-a4`, and `rs2` is the same shape with `b` names. Every node boots with `super_read_only=1` from `my.cnf`, so a node only ever becomes writable because the manager promoted it.

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
   |  OK                      |     advances          |                   |     thread applies   |
                              +-----------------------+                   |     relay log, its   |
                                                                          |     gtid_executed    |
                                                                          |     advances         |
                                                                          +----------------------+
```

A transaction moves through that picture in order. At (1) it exists only inside the primary. At (2) it is in the primary's binary log on disk but not yet visible to other sessions. At (3) the replica's IO thread, which is a client of the primary, has received the events. The manual says the replica "acknowledges receipt of a transaction's events only after the events have been written to its relay log and flushed to disk", which is (4). At (6) the primary commits in InnoDB, the transaction becomes visible and the client gets OK. At (7), possibly much later, the replica's SQL thread replays the relay log and the transaction becomes visible on the replica.

A GTID names a transaction as the originating server's UUID plus a sequence number. A GTID set is a collection of ranges per UUID, for example the set below.

```
3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:11:47-49
```

Three sets matter and they are easy to confuse.

| Name | Where you read it | What it contains |
|---|---|---|
| `@@GLOBAL.gtid_executed` | any server | every transaction this server has committed, from clients or from replication |
| `Retrieved_Gtid_Set` | `SHOW REPLICA STATUS` on a replica | every transaction the IO thread has received into the relay log |
| `Executed_Gtid_Set` | `SHOW REPLICA STATUS` on a replica | the same as that replica's `gtid_executed` |

On a healthy replica `Executed_Gtid_Set` is a subset of `Retrieved_Gtid_Set` plus what it had before replication started, and the difference between them is the relay log that has not been applied yet. That difference is what step 3 of failover waits on. GTIDs also make repointing cheap. With `SOURCE_AUTO_POSITION=1` a replica tells its new source which GTIDs it already has and receives the rest, so DBGuard never names a binlog file or offset anywhere. The Meta GTID post describes using exactly this to repoint a recovered primary instead of copying data to replace it.

`dbguard/gtid.py` compares GTID sets as sets, never as strings, and `tests/test_gtid.py` checks it against the semantics of `GTID_SUBSET` and `GTID_SUBTRACT` with `hypothesis`.

## 3. Semi-synchronous replication

Plain replication is asynchronous. The primary answers the client and ships the binlog later, so a crash in between loses an acknowledged write. Semi-sync adds a wait for at least one replica's ack, and `rpl_semi_sync_source_wait_point` chooses where the wait goes.

With `AFTER_SYNC` (the default) the source syncs the binlog, waits for the ack, then commits to InnoDB and returns. The manual says that on source failure "all transactions committed on the source have been replicated to the replica (saved to its relay log)". With `AFTER_COMMIT` it commits to InnoDB first and waits afterwards, so other sessions can read a transaction no replica has, and the manual warns that after failover "it is possible for such clients to see a loss of data relative to what they saw on the source."

Here is the same crash on the same timeline in both modes. The primary dies after the binlog write and before any ack arrives.

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

DBGuard sets `AFTER_SYNC`, `rpl_semi_sync_source_wait_for_replica_count=1` and `rpl_semi_sync_source_timeout=3600000`, which is one hour.

The timeout is the part people get wrong. The manual default is 10000 ms, and on expiry "the source reverts to asynchronous replication" until "at least one semisynchronous replica catches up". Only `Rpl_semi_sync_source_status` going OFF tells you. A primary that loses its replicas for ten seconds then acknowledges writes that exist nowhere else. Matsunobu lists the timeout among the ways semi-sync quietly turns itself off and recommends an "infinite or very long timeout", and Heckel uses one hour. With an hour, a primary with no replica able to ack stops completing commits, the manager logs a `stall` event, and failover or a human deals with it. The stall is the design, an availability cost chosen on purpose and measured in `docs/CAPACITY.md`.

`rpl_semi_sync_source_wait_no_replica` is left at its default of ON. The manual says that with ON the replica count may drop below `wait_for_replica_count` during the timeout period and semi-sync continues as long as enough acks arrive before the timeout. With OFF the source reverts to asynchronous replication the moment the count drops. OFF would undo the one-hour timeout, so DBGuard keeps ON.

**What lossless promises.** Under this configuration, every write that returned OK to a client was in at least one replica's relay log on disk at the moment it returned, so a failover that promotes the replica holding the most received transactions, after that replica has applied its relay log, loses no acknowledged write. It is claimed only for acknowledged writes, only under the configuration above, and only as measured by the harness, where DBGuard lost `[[N: kill lost acked writes total, results/kill_dbguard.jsonl]]` acknowledged writes across `[[N: kill run count, results/kill_dbguard.jsonl]]` primary kills.

**What it does not promise.** With `wait_for_replica_count=1`, if the primary and the one replica that acked a write both die, or that replica dies or becomes unreachable before the manager reads its retrieved set, the write exists nowhere the manager can see and it is lost.

## 4. The agent

### Why it exists

Failover is only safe if the old primary has stopped accepting writes before the new one starts. The manager is on the other side of a network and cannot guarantee that about a process it cannot reach. The agent sits in the same container as `mysqld` and is its parent process, so it can always stop it. That is STONITH ("shoot the other node in the head") done at the process level instead of by cutting power to a host. When the manager says fence, the agent makes the node stop taking writes, and if `mysqld` does not cooperate within the deadline the agent sends `SIGKILL` to its own child.

### Why it supervises `mysqld`

In each node container the agent is PID 1's child under `tini`, and it starts `mysqld` itself (`dbguard/agent/supervisor.py`). Three things follow. The agent knows the PID it has to kill without searching for it. It restarts `mysqld` after a kill, and `mysqld` comes back read-only because `my.cnf` says so. And the clone plugin needs a supervisor. The manual says that after a remote clone "the recipient MySQL server instance is restarted (stopped and started) automatically", and that "for an automatic restart to occur, a monitoring process must be available on the recipient to detect server shutdowns". Without one the clone ends with error 3707 and a stopped server. The agent is that monitoring process.

### The fence sequence

`POST /fence` in `dbguard/agent/core.py` does this, in this order.

1. Write the fence flag file `/var/lib/dbguard/fenced` (atomically, through a temp file and `os.replace`) and set the in-memory flag. From this instant `GET /primary` returns 503.
2. `SET GLOBAL super_read_only=1` with a 2 s deadline.
3. If that returned, kill every client thread that is not replication or DBGuard's own, and read the final `gtid_executed` to report back.
4. If it did not return, `SIGKILL` `mysqld` and report `method: kill`. The supervisor restarts it read-only.

The flag goes first because the SQL may never return. The cases where fencing matters most are the ones where `mysqld` is least able to answer. A frozen process does not execute `SET`. A primary partitioned from its replicas has sessions stuck in commit waiting for an ack, and the manual says an attempt to enable `read_only` (and so `super_read_only`) "blocks while other clients have any ongoing statement ... or ongoing commit". Writing the flag first means HAProxy's next health check fails regardless of what `mysqld` is doing, and the file survives an agent restart, so a node that was fenced stays fenced until `/promote` or `/unfence` clears it. The manual's note about blocking on in-flight commits also predicts that a fence during a replica partition will usually take the kill path. How often it did is `[[N: fence outcome counts sql vs kill, results/partition-replicas_dbguard.jsonl]]`.

### Why `super_read_only` and not `read_only`

`read_only` stops ordinary users only. The manual says the server "permits no client updates except from users who have the CONNECTION_ADMIN privilege (or the deprecated SUPER privilege)". Admin accounts, migration tools and DBGuard's own `dbguard` user all hold that privilege, so on a `read_only` node they can still write, and a single stray admin write on a fenced primary is a transaction the new primary does not have. `super_read_only` blocks those users too. Setting it forces `read_only` on, and replication threads keep working under it, which is what a replica needs.

### The kill path

`SIGKILL` works on a frozen process too. The kernel reclaims it, closes its sockets and releases its file locks. Transactions it had written to its binlog but not had acknowledged stay on disk, and crash recovery commits them when the supervisor restarts `mysqld`. Those are the phantoms of section 7.

### The wake guard and the woken-primary window

A frozen container is the nasty case. If the whole node container is frozen, the agent is frozen with `mysqld`, cannot answer `/fence`, and the manager fails over around it. When the container thaws, `mysqld` still has `super_read_only=0` and clients that were mid-write resume talking to it, and the agent's in-memory state is from before the freeze. That gap, between thaw and the old primary being made read-only, is the woken-primary window.

The agent closes it in `ensure_awake` and its ticker. A 500 ms loop compares the monotonic clock against the last tick. A gap greater than 3 s means the process was frozen, and the agent then asks the manager `GET /v1/sets/<rs>/primary`. If the answer names another node and this node is writable, the agent fences itself before anything else. `/primary` calls the same check before answering, so HAProxy cannot get a stale 200 from a freshly thawed agent. If the manager is unreachable the agent fences if the fence file exists and otherwise logs and leaves state alone, because a node that cannot reach the manager and was never fenced is most likely still the legitimate primary. The number of writes that landed on a woken primary in the hang experiment is `[[N: writes on woken primary total, results/hang-container_dbguard.jsonl]]`.

## 5. Detection

### Why the manager's own probe is not enough

If the manager declares a primary dead because the manager cannot reach it, then any fault between the manager and the primary causes a failover of a healthy primary. A false failover is not free. Every in-flight transaction errors, the set spends seconds without a writer, the old primary has to be rejoined or rebuilt, and the set enters a cooldown during which a real failure would not be handled automatically. The openark/orchestrator documentation makes the same argument. It distinguishes `DeadMaster` ("Master MySQL access failure" and "All of master's replicas are failing replication") from `UnreachableMaster`, where the master is unreachable but still "has replicating replicas", which "does not make for a recovery process".

### The two conditions

`dbguard/manager/detector.py` declares a primary dead only when both of these hold continuously for `detect_window_s` (default 5 s).

1. The manager's probe failed `probe_failures` (3) times in a row, each with `probe_timeout_s` (1 s). The probe is `SELECT 1` plus a write to `dbguard.manager_probe`. It has to include a write. A primary cut off from its replicas still answers `SELECT 1` instantly, but every commit on it blocks on semi-sync, so only a write notices.
2. A strict majority of the set's configured replicas vote that the primary is gone. A replica votes if its IO thread is not `Yes` while pointed at the primary, or its agent cannot open a TCP connection to the primary's port within 500 ms, or the heartbeat row the primary's agent writes every 500 ms is older than `detect_window_s`. A replica whose own agent or `mysqld` is unreachable does not vote.

If (1) holds and (2) does not, the set is `SUSPECT`. The manager is probably the partitioned one, and it logs and does nothing. If (2) holds and (1) does not, the verdict is `REPLICAS_ONLY`, which is also logged and left alone. Naive mode decides from (1) alone, which is exactly why it fails over when the manager is partitioned.

### What each failure looks like

The replicas learn about a dead primary through their IO thread's TCP connection, so what TCP does decides what they see.

| Injection | What TCP does | Manager probe | Replicas' view |
|---|---|---|---|
| `kill -9` of `mysqld` (agent alive) | the kernel closes the process's sockets, so peers get FIN or RST at once, and new connects to 3306 are refused with RST | fails fast with connection refused | IO thread errors immediately and goes to `Connecting`, TCP connect from the agent fails |
| `docker kill -s KILL` of the container | as above for existing connections, then the address disappears and new connects time out or report no route | fails, agent also unreachable | same, and the fence step records `unreachable` |
| `SIGSTOP` of `mysqld`, or a frozen container | nothing. The kernel still completes TCP handshakes into the listen backlog and still ACKs data into receive buffers, but no byte comes back | times out (connect succeeds, the MySQL handshake never arrives) | no events and no heartbeats. After `replica_net_timeout` the IO thread gives up and reconnects, and then hangs in `Connecting`. The heartbeat row stops advancing |
| `iptables -j DROP` between primary and replicas | silent drop. Senders retransmit into nothing, no RST, no FIN | `SELECT 1` works, the write times out because commits wait for an ack | same as a hang, the IO thread only notices when the heartbeat stops arriving and `replica_net_timeout` expires |

Two replication settings make the replicas' view fast. `SOURCE_HEARTBEAT_PERIOD=0.5` makes the primary send a heartbeat event when it has had nothing else to send for half a second. `replica_net_timeout=2` makes the replica, in the manual's words, wait that many seconds "for more data or a heartbeat signal from the source before the replica considers the connection broken, aborts the read, and tries to reconnect". The MySQL default is 60 s, which would put a minute of silence in front of every hang and partition verdict. The heartbeat has to be well under the timeout or a quiet but healthy primary would look dead.

A hung primary reaches the verdict by two routes, the probe timing out and the replicas voting, which is the point of having both.

A note for the harness. `docker kill -s STOP` delivers `SIGSTOP` to the container's PID 1, which is `tini`, and does not stop its children. A whole-container hang has to use the cgroup freezer (`docker pause`), and a process hang uses the agent's `/hang-mysqld` hook.

### The manager partitioned case

When `iptables` inside the manager container drops traffic to the primary, condition (1) holds and condition (2) does not, because the replicas still receive binlog events and the heartbeat still advances. The set goes to `SUSPECT` and stays there until the probe succeeds again. DBGuard's false failover count under this injection is `[[N: false failovers dbguard, results/partition-manager_dbguard.jsonl]]` against `[[N: false failovers naive, results/partition-manager_naive.jsonl]]` for naive mode and `[[N: false failovers orchestrator, results/partition-manager_orchestrator.jsonl]]` for Orchestrator.

## 6. Failover

The manager runs six steps, each timed and written into one event row (`dbguard/events.py`).

1. **Fence** the old primary through its agent, with a 3 s deadline. The outcome is `sql`, `kill` or `unreachable`.
2. **Choose** the winner among reachable, responsive replicas that had semi-sync enabled. The winner holds the most transactions by its retrieved set, counted as the union of `Retrieved_Gtid_Set` and `Executed_Gtid_Set` (`dbguard/manager/selection.py`). Every other candidate's set must be a subset of the winner's, or the set goes to `HALTED`.
3. **Catch up.** Wait until the winner's `Executed_Gtid_Set` contains its `Retrieved_Gtid_Set`, with a 30 s deadline.
4. **Promote.** `STOP REPLICA`, `RESET REPLICA ALL`, `super_read_only=0`, `read_only=0`, semi-sync source on and replica off, clear the fence flag. HAProxy sees 200 on its next checks.
5. **Repoint** every other replica with `CHANGE REPLICATION SOURCE TO SOURCE_HOST=<winner>, SOURCE_AUTO_POSITION=1` and `START REPLICA`.
6. **Record** the event, including the watermark, which is the winner's `gtid_executed` at promotion.

**Why fencing precedes promotion.** If the new primary becomes writable while the old one still is, the set has two writers for however long the overlap lasts, and HAProxy may send clients to either. Writes on the old side exist nowhere else once it is fenced. Fencing first makes the overlap zero when the agent answers. When the agent does not answer, the old primary is dead or cut off from everything, and the protection comes from semi-sync instead. Anything it committed without an ack was never returned to a client, and it cannot get an ack, because its replicas are about to be repointed away. The event row records which of these cases happened.

**Why the largest retrieved set.** An acknowledged write is guaranteed to be in some replica's relay log, not in its executed set. A replica whose SQL thread is behind may look older by `gtid_executed` while holding the only copy of the last acknowledged transaction in its relay log. Picking by retrieved set picks the replica that received the most, and because every replica receives the same binlog in the same order from one primary, the retrieved sets of healthy replicas should form a chain in which each is contained in the next. The code takes the union with the executed set because the retrieved set describes the relay log, which `RESET REPLICA` and relay log recovery discard, so a replica that was reset or restarted can have applied transactions that its retrieved set no longer lists.

**Why wait for the relay log.** A replica promoted before its SQL thread finishes would accept new writes before the old acknowledged ones were applied, which reorders history and can break unique keys. Orchestrator's `DelayMasterPromotionIfSQLThreadNotUpToDate` exists for the same reason. Its documentation says that "even the most up-to-date, promoted replica may yet have unapplied relay logs".

**Why the subset check, and what `HALTED` means.** The chain property above is an assumption. It fails if a replica took a local write, if a previous failover went wrong, or if an operator touched something by hand. If replica B has a transaction that the winner A lacks, promoting A and repointing B makes B either fail with an error or keep a transaction that exists on no other node, and choosing B instead would drop whatever A has that B lacks. There is no automatic answer that is safe, so the manager stops and sets `HALTED` with the reason, for example `replicas diverged` followed by the two node names and the `GTID_SUBTRACT` in each direction. A human decides. `docs/RUNBOOK.md` has the procedure. The subset check is the step a naive tool skips.

**The cooldown.** After a failover the manager will not fail the same set over again automatically for `cooldown_s` (20 s). A second failover right after the first usually means something shared is broken, and flapping only multiplies aborted transactions and rejoin work. Orchestrator's equivalent is `RecoveryPeriodBlockSeconds`, which its documentation example sets to an hour. DBGuard's is short because the lab injects failures back to back.

The measured failover time, from injection to the first successful write through HAProxy, is `[[N: kill failover p50, results/kill_dbguard.jsonl]]` median and `[[N: kill failover p99, results/kill_dbguard.jsonl]]` p99 for a killed primary, `[[N: hang failover p50, results/hang-container_dbguard.jsonl]]` median for a frozen one and `[[N: partition failover p50, results/partition-replicas_dbguard.jsonl]]` median for a primary partitioned from its replicas.

## 7. Rejoin

When the old primary's agent answers again, the manager decides what to do with it.

**Phantom transactions.** At the moment of a crash the primary may have transactions that reached step (2) of the diagram, written and fsynced to its binlog, and were waiting for an ack. No client was told OK, because under `AFTER_SYNC` the OK comes after the ack. But on restart, crash recovery treats a transaction that is in the binlog as committed and commits it in InnoDB. The old primary now has rows that no client ever saw acknowledged and that the new primary does not have. Those are phantoms. The manual says as much. Under `AFTER_SYNC` after a failover "the source cannot be restarted in this scenario and must be discarded, because its binary log might contain uncommitted transactions that would cause a conflict with the replica when externalized after binary log recovery". Heckel's write-up reaches the same conclusion and rebuilds the old source from a backup every time.

They must not survive. If the old primary rejoined with them, it would hold data the rest of the set does not, a later failover could promote it, and the phantom rows would reappear to clients as if they had been committed, possibly conflicting with rows written on the new primary under the same keys.

**Repoint or rebuild.** The manual's instruction to always discard is safe but expensive. The Meta GTID post describes repointing a recovered primary "if ... our automation detects its data is consistent". DBGuard uses GTIDs to make that check exact. After the agent has restarted `mysqld` (so crash recovery has run), the manager reads the old primary's `gtid_executed`. If it is a subset of the new primary's `gtid_executed`, the old primary has nothing the new one lacks, and it is repointed with `SOURCE_AUTO_POSITION=1`. That needs no data copy. Otherwise the difference, `GTID_SUBTRACT(old, new)`, is the phantom set, and the node is rebuilt by cloning. The count of phantom GTIDs discarded is logged in the event. How often each branch was taken is `[[N: rejoin repoint count vs rebuild count, results/kill_dbguard.jsonl]]`, with `[[N: phantom GTIDs per rebuild p50 and max, results/kill_dbguard.jsonl]]` phantoms per rebuild. `rejoin: manual` in `fleet.yaml` turns both off for operators who want to look first.

**How the clone works.** `POST /rebuild` on the agent sets `clone_valid_donor_list` to the donor and runs `CLONE INSTANCE FROM` the donor. The clone plugin copies the donor's InnoDB data physically over the network. By default, per the manual, it "removes existing user-created data (schemas, tables, tablespaces) and binary logs from the recipient data directory", which is what physically destroys the phantoms. It also transfers the donor's `gtid_executed`. `mysqld` then shuts down, the agent restarts it read-only, and the manager repoints it with auto-positioning, so it fetches whatever the donor had not yet received. The manual's prerequisites all matter in practice. Donor and recipient must run the same MySQL series on the same platform, with the same active plugins, the same character set, and at least 2 MB of `max_allowed_packet`, only one clone runs at a time, and the recipient needs disk for the whole copy. Every node loads the same plugins through `plugin-load-add` so the plugin check passes.

**Why the donor is a replica.** The clone reads the donor's whole dataset and holds `BACKUP_ADMIN`, which blocks concurrent DDL on the donor for the duration. Doing that to the primary puts a bulk sequential read and a DDL block on the one node that serves writes, while every client is waiting on its commit latency. A healthy replica has the same data minus at most a few seconds, and the auto-positioned repoint afterwards closes that gap. The manual confirms a replica works as a donor and that "replication channels on the recipient that use GTID auto-positioning can resume replication automatically after the cloning operation".

## 8. Replacement and the fleet

A set with fewer healthy replicas than configured is `DEGRADED`. With two replicas and `wait_for_replica_count=1`, a set that has lost one replica still commits, because the survivor acks, but it now has no margin. Losing the survivor stalls every write.

If the set stays `DEGRADED` for longer than `rebuild_after_s` (60 s), the manager provisions a replacement. In the lab that means starting the spare container (`mysql-a4` or `mysql-b4`), cloning it from a healthy replica, never the primary, for the reasons in section 7, and repointing it. In a real fleet this step is a call to the host allocator, the service that hands out a machine with the right hardware in the right failure domain, and everything after it is the same. That hook is the one place DBGuard stands in for infrastructure it does not have. The replacement took `[[N: time to replace p50, results/replica-loss_dbguard.jsonl]]` on `[[N: dataset size MB, results/replica-loss_dbguard.jsonl]]` of data, at `[[N: clone MB/s p50, results/replica-loss_dbguard.jsonl]]`.

There are two sets so that "fleet" is true and a bug in per-set state shows up. Each set has its own loop, state machine, cooldown and event stream. Every chaos run against `rs1` counts state changes in `rs2`, which must be zero, and measured `[[N: rs2 state changes total across all rs1 injections, results/*.jsonl]]`.

## 9. Operations

**HAProxy is the endpoint and never decides.** Clients connect to `haproxy:3306` for `rs1` and `haproxy:3307` for `rs2`. Each backend lists every node of the set and health-checks each node's agent with `GET /primary`, which returns 200 only if the fence flag is clear, `super_read_only` is 0 and `mysqld` answered within a second. HAProxy has no opinion about which node should be primary. It only asks the agents, so fencing and promotion are each one state change on one agent, and there is one source of truth. `on-marked-down shutdown-sessions` makes HAProxy close every proxied connection to a server the moment it is marked down, so clients holding a connection to a fenced primary get an error and reconnect through the proxy instead of lingering on the old node.

**Metrics.** The manager and every agent serve Prometheus text on `/metrics`, including state per set, failovers by outcome, step durations, phantom GTIDs discarded, replicas rebuilt, and `Rpl_semi_sync_source_tx_avg_wait_time` ("the average time in microseconds the source waited for each transaction").

**`dbgctl doctor`** turns the manager's view into sentences. It states the verdict, the evidence (how long the probe has failed, which replicas vote and why), what the manager would do next, and the `GTID_SUBTRACT` gap between the primary and every other node. It is the first command in every runbook entry.

**Planned switchover.** `dbgctl failover rs1 [--to node]` runs the same steps as a failover but with a live, cooperative old primary. It sets `super_read_only` on the old primary first, waits for the candidate to apply everything, promotes and repoints. No acknowledged write can be lost and the client sees a short stall, measured at `[[N: switchover stall p50, results/switchover_dbguard.jsonl]]` median and `[[N: switchover stall p99, results/switchover_dbguard.jsonl]]` p99.

## 10. The baselines

**Naive mode.** `--mode naive` is a deliberately careless version of the same program. It runs replication asynchronously (the semi-sync plugins are loaded and never enabled), detects from the manager's probe alone, does not fence, promotes the most advanced replica and skips the subset check. It loses writes because the primary acknowledges a commit before any replica has received it, so a kill that lands in that window leaves an acknowledged row only on a dead node. Across the same primary kills it lost `[[N: kill naive lost acked writes total, results/kill_naive.jsonl]]` acknowledged writes where DBGuard lost `[[N: kill lost acked writes total, results/kill_dbguard.jsonl]]`. A baseline that lost nothing would mean the harness was not landing the crash between commit and replication, and the harness would be the thing to fix.

**Orchestrator.** openark/orchestrator, originally by Shlomi Noach, is the industry tool and DBGuard's model for detection. It runs from its own image with its own defaults, and its pre-failover hook flips the agent's fence flag. It does much that DBGuard does not. It discovers arbitrary topologies including intermediate masters, it picks a candidate by more than recency (it "attempts to promote a replica that will retain the most serving capacity"), and it has datacenter-aware rules, graceful takeover, a web UI and a raft-based HA mode. What DBGuard does by default that Orchestrator does not do on its own is fence the old primary itself, down to killing the process, refuse to promote when replicas' GTID sets have diverged, decide by GTID subset whether a crashed primary may be repointed or must be rebuilt, rebuild it with the clone plugin, and provision a replacement. Orchestrator leaves fencing to hooks and a crashed master to the operator. Its numbers are `[[N: kill failover p50 orchestrator, results/kill_orchestrator.jsonl]]` failover median and `[[N: kill lost acked writes orchestrator, results/kill_orchestrator.jsonl]]` lost acknowledged writes, and any run where it was faster is reported as such.

## 11. The cost of losslessness

Semi-sync adds a round trip to a replica, plus the replica's relay log fsync, to every commit, and the one-hour timeout converts "no replica can ack" into "no commit completes". `docs/CAPACITY.md` has the measured table of commit latency p50 and p99 and writes per second with semi-sync off and on, at 0, 2 and 20 ms of added latency between primary and replicas. The headline cost is `[[N: commit p50 semisync on vs off at 0 ms netem, results/cost_dbguard.jsonl]]` at the median and `[[N: commit p99 semisync on vs off at 0 ms netem, results/cost_dbguard.jsonl]]` at p99. The stall when the last replica dies lasted `[[N: stall_s with no semi-sync replica, results/replica-loss_dbguard.jsonl]]`, which is simply how long the harness left the replica down. For comparison, Matsunobu's post reported roughly 2500 commits per second with semi-sync against roughly 10000 without on their hardware. Those are their numbers, not DBGuard's.

## 12. What DBGuard cannot do

- It cannot survive the double loss of section 3. Sets with `wait_for_replica_count=2` and three replicas close that window at a latency and availability cost, and that is a stretch item.
- It cannot protect a write acked by a replica that is unreachable at the moment the manager chooses. The manager can only compare the replicas it can see. It does not refuse to promote because an unseen replica might be ahead, since that would turn every replica outage into a stuck set.
- It depends on a single manager. If the manager dies, nothing fails over, and every set is effectively `HALTED` until it returns. That is safe because no action is the one action that cannot lose data, and a second manager would need the consensus machinery that is out of scope.
- A fenced-unreachable primary that is still reachable by clients is only held back by semi-sync. It cannot complete a commit because its replicas have been repointed, but after `rpl_semi_sync_source_timeout` (one hour) it would fall back to asynchronous and start acknowledging writes that exist nowhere else. The agent's wake guard catches the frozen case. A primary partitioned from the manager and its replicas but not from its clients for over an hour is not handled.
- It does not detect silent corruption, replication filters, or data drift that does not show up in GTID sets. Two nodes with the same `gtid_executed` are treated as identical.
- It has only been run in Docker on one laptop, so every network is a Linux bridge and every disk is the same disk.

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
