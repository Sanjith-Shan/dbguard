# DBGuard

MySQL 8.4 replica-set manager with lossless automated failover, in Python.

DBGuard watches a fleet of MySQL replica sets, each one primary and two replicas on GTID
and semi-synchronous replication. When a primary dies, hangs or is cut off, it fences the
old primary (down to `SIGKILL` of `mysqld` when SQL will not answer), promotes the replica
holding the most received transactions after checking by GTID set arithmetic that the
replicas have not diverged, repoints the other replicas with auto-positioning, and later
either repoints the old primary or rebuilds it with the clone plugin, depending on whether
it holds transactions nobody acknowledged. It comes with a chaos harness that kills,
freezes and partitions nodes while clients write through the proxy, and a checker that
counts every acknowledged write afterwards, so "lossless" is a measurement here rather
than a claim.

It is a lab in Docker. Six `mysqld` in containers on one laptop, two replica sets, one
HAProxy, one manager and one agent beside each `mysqld`. Nothing here has run in a
datacenter or carried real traffic. Lossless is claimed only for acknowledged writes, only
under the configuration below, and only as measured. With
`rpl_semi_sync_source_wait_for_replica_count=1`, losing the primary and the one replica
that acknowledged a write at the same time loses that write, and the `kill-two` experiment
below publishes exactly that boundary.

## Results

Every number is produced by `bin/chaos` and summarised by `bin/report` into
`results/SUMMARY.md`. A tag of the form `[[N: what, source file]]` marks a number that has
not been filled in from those files yet. Percentiles are nearest-rank. The tables below
use the column names `bin/report` prints, with only the columns each experiment needs.

Environment. `[[N: host, Docker Desktop and MySQL version, results/SUMMARY.md]]`.
`detect_window_s=5`, `probe_timeout_s=1`, three failed probes, 8 clients doing one
autocommit `INSERT` at a time through HAProxy.

### At a glance

| | |
|---|---|
| Primary kills, DBGuard | `[[N: kill run count, results/kill_dbguard.jsonl]]` runs |
| Acknowledged writes lost by DBGuard across every injection | **`[[N: dbguard lost acked writes total all scenarios except kill-two, results/*_dbguard.jsonl]]`** |
| Acknowledged writes lost by the asynchronous baseline across the same kills | **`[[N: kill naive lost acked writes total, results/kill_naive.jsonl]]`** |
| Failover after a primary kill, median | **`[[N: kill failover p50, results/kill_dbguard.jsonl]]` s** |
| False failovers with the manager partitioned from the primary | **`[[N: false failovers dbguard, results/partition-manager_dbguard.jsonl]]`** |
| Commit latency cost of semi-sync at the median, no added delay | `[[N: commit p50 semisync on vs off at 0 ms netem, results/cost_dbguard.jsonl]]` |

### Experiment 1. Primary killed

`docker kill -s KILL` on the primary while the workload runs. Failover time is injection to
the first successful write through HAProxy, measured on the clients' clock.

| mode | runs | failover p50 s | failover p99 s | lost acked writes | runs with loss | phantom writes | single-writer violations | converged |
|---|---|---|---|---|---|---|---|---|
| dbguard | `[[N: kill run count, results/kill_dbguard.jsonl]]` | `[[N: kill failover p50, results/kill_dbguard.jsonl]]` | `[[N: kill failover p99, results/kill_dbguard.jsonl]]` | `[[N: kill lost acked writes total, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard runs with loss, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard phantom writes, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard single-writer violations, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard converged runs, results/kill_dbguard.jsonl]]` |
| naive | `[[N: kill naive run count, results/kill_naive.jsonl]]` | `[[N: kill naive failover p50, results/kill_naive.jsonl]]` | `[[N: kill naive failover p99, results/kill_naive.jsonl]]` | `[[N: kill naive lost acked writes total, results/kill_naive.jsonl]]` | `[[N: kill naive runs with loss, results/kill_naive.jsonl]]` | `[[N: kill naive phantom writes, results/kill_naive.jsonl]]` | `[[N: kill naive single-writer violations, results/kill_naive.jsonl]]` | `[[N: kill naive converged runs, results/kill_naive.jsonl]]` |
| orchestrator | `[[N: kill orchestrator run count, results/kill_orchestrator.jsonl]]` | `[[N: kill failover p50 orchestrator, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator failover p99, results/kill_orchestrator.jsonl]]` | `[[N: kill lost acked writes orchestrator, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator runs with loss, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator phantom writes, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator single-writer violations, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator converged runs, results/kill_orchestrator.jsonl]]` |

The row to read first is the naive one. It runs asynchronous replication with no fence and
no subset check, and it has to lose writes, because a baseline that loses nothing would mean
the harness never landed a crash between commit and replication. Naive mode also decides on
the manager's own probe alone and skips the fence, so its failover time is expected to be
shorter than DBGuard's. That speed is what it bought with the lost writes, and the table
shows both. If the Orchestrator row is faster than DBGuard's, that is reported as it stands.

Rejoin of the killed primary once its container is back.

| scenario | mode | rejoins | repoint | rebuild | other | phantom GTIDs mean | phantom GTIDs max | rejoin p50 s | rejoin p99 s |
|---|---|---|---|---|---|---|---|---|---|
| kill | dbguard | `[[N: kill dbguard rejoins, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard rejoin repoint count, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard rejoin rebuild count, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard rejoin other count, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard phantom GTIDs per rebuild mean, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard phantom GTIDs per rebuild max, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard rejoin p50, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard rejoin p99, results/kill_dbguard.jsonl]]` |
| kill | naive | `[[N: kill naive rejoins, results/kill_naive.jsonl]]` | `[[N: kill naive rejoin repoint count, results/kill_naive.jsonl]]` | `[[N: kill naive rejoin rebuild count, results/kill_naive.jsonl]]` | `[[N: kill naive rejoin other count, results/kill_naive.jsonl]]` | `[[N: kill naive phantom GTIDs mean, results/kill_naive.jsonl]]` | `[[N: kill naive phantom GTIDs max, results/kill_naive.jsonl]]` | `[[N: kill naive rejoin p50, results/kill_naive.jsonl]]` | `[[N: kill naive rejoin p99, results/kill_naive.jsonl]]` |
| kill | orchestrator | `[[N: kill orchestrator rejoins, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator rejoin repoint count, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator rejoin rebuild count, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator rejoin other count, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator phantom GTIDs mean, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator phantom GTIDs max, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator rejoin p50, results/kill_orchestrator.jsonl]]` | `[[N: kill orchestrator rejoin p99, results/kill_orchestrator.jsonl]]` |

Repoint means the old primary's `gtid_executed` was a subset of the new primary's after
crash recovery, so it rejoined with no data copy. Rebuild means it held transactions that
reached its binary log but were never acknowledged to a client, and the phantom columns
count how many of those the clone discarded. How often a crashed primary needs a rebuild is
a property of the workload and the kill timing, not a constant. Orchestrator does not rejoin
a dead primary itself, so in that row the harness did it through the agents and the runs
land in `other`.

### Experiment 2. Primary hung

The process is alive and TCP connects still succeed, which is the case that breaks tools
that trust a connect. Two ways of freezing it. `hang-container` pauses the whole container
with the cgroup freezer, agent included. `hang-process` sends `SIGSTOP` to `mysqld` alone,
so the agent stays up and has to fence a `mysqld` that will not answer SQL. Both are woken
90 s later.

| scenario | mode | runs | failover p50 s | failover p99 s | lost acked writes | single-writer violations | converged | writes on woken primary |
|---|---|---|---|---|---|---|---|---|
| hang-container | dbguard | `[[N: hang-container dbguard run count, results/hang-container_dbguard.jsonl]]` | `[[N: hang failover p50, results/hang-container_dbguard.jsonl]]` | `[[N: hang-container dbguard failover p99, results/hang-container_dbguard.jsonl]]` | `[[N: hang-container dbguard lost acked writes, results/hang-container_dbguard.jsonl]]` | `[[N: hang-container dbguard single-writer violations, results/hang-container_dbguard.jsonl]]` | `[[N: hang-container dbguard converged runs, results/hang-container_dbguard.jsonl]]` | `[[N: hang-container dbguard writes on woken primary, results/hang-container_dbguard.jsonl]]` |
| hang-container | naive | `[[N: hang-container naive run count, results/hang-container_naive.jsonl]]` | `[[N: hang-container naive failover p50, results/hang-container_naive.jsonl]]` | `[[N: hang-container naive failover p99, results/hang-container_naive.jsonl]]` | `[[N: hang-container naive lost acked writes, results/hang-container_naive.jsonl]]` | `[[N: hang-container naive single-writer violations, results/hang-container_naive.jsonl]]` | `[[N: hang-container naive converged runs, results/hang-container_naive.jsonl]]` | `[[N: hang-container naive writes on woken primary, results/hang-container_naive.jsonl]]` |
| hang-container | orchestrator | `[[N: hang-container orchestrator run count, results/hang-container_orchestrator.jsonl]]` | `[[N: hang-container orchestrator failover p50, results/hang-container_orchestrator.jsonl]]` | `[[N: hang-container orchestrator failover p99, results/hang-container_orchestrator.jsonl]]` | `[[N: hang-container orchestrator lost acked writes, results/hang-container_orchestrator.jsonl]]` | `[[N: hang-container orchestrator single-writer violations, results/hang-container_orchestrator.jsonl]]` | `[[N: hang-container orchestrator converged runs, results/hang-container_orchestrator.jsonl]]` | `[[N: hang-container orchestrator writes on woken primary, results/hang-container_orchestrator.jsonl]]` |
| hang-process | dbguard | `[[N: hang-process dbguard run count, results/hang-process_dbguard.jsonl]]` | `[[N: hang-process dbguard failover p50, results/hang-process_dbguard.jsonl]]` | `[[N: hang-process dbguard failover p99, results/hang-process_dbguard.jsonl]]` | `[[N: hang-process dbguard lost acked writes, results/hang-process_dbguard.jsonl]]` | `[[N: hang-process dbguard single-writer violations, results/hang-process_dbguard.jsonl]]` | `[[N: hang-process dbguard converged runs, results/hang-process_dbguard.jsonl]]` | `[[N: hang-process dbguard writes on woken primary, results/hang-process_dbguard.jsonl]]` |
| hang-process | naive | `[[N: hang-process naive run count, results/hang-process_naive.jsonl]]` | `[[N: hang-process naive failover p50, results/hang-process_naive.jsonl]]` | `[[N: hang-process naive failover p99, results/hang-process_naive.jsonl]]` | `[[N: hang-process naive lost acked writes, results/hang-process_naive.jsonl]]` | `[[N: hang-process naive single-writer violations, results/hang-process_naive.jsonl]]` | `[[N: hang-process naive converged runs, results/hang-process_naive.jsonl]]` | `[[N: hang-process naive writes on woken primary, results/hang-process_naive.jsonl]]` |
| hang-process | orchestrator | `[[N: hang-process orchestrator run count, results/hang-process_orchestrator.jsonl]]` | `[[N: hang-process orchestrator failover p50, results/hang-process_orchestrator.jsonl]]` | `[[N: hang-process orchestrator failover p99, results/hang-process_orchestrator.jsonl]]` | `[[N: hang-process orchestrator lost acked writes, results/hang-process_orchestrator.jsonl]]` | `[[N: hang-process orchestrator single-writer violations, results/hang-process_orchestrator.jsonl]]` | `[[N: hang-process orchestrator converged runs, results/hang-process_orchestrator.jsonl]]` | `[[N: hang-process orchestrator writes on woken primary, results/hang-process_orchestrator.jsonl]]` |

The last column is the one this experiment exists for. A frozen primary wakes up still
believing it is the primary, with client connections that were mid-write. Its agent notices
the gap in its own monotonic clock, asks the manager who the primary is and fences itself
before HAProxy's next check, so any nonzero count in the DBGuard rows is a bug. In the
`hang-process` rows the fence has to go through the kill path, because `SET GLOBAL
super_read_only=1` never returns from a stopped process.

### Experiment 3. Primary partitioned from its replicas

`iptables` drops the primary's traffic to both replicas, while the manager and HAProxy can
still reach it. With semi-sync every commit on the primary now blocks.

| mode | runs | failover p50 s | failover p99 s | stall p50 s | stall p99 s | lost acked writes | single-writer violations | old primary fenced |
|---|---|---|---|---|---|---|---|---|
| dbguard | `[[N: partition-replicas dbguard run count, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition failover p50, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition failover p99, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition stall_s p50, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition stall_s p99, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition-replicas dbguard lost acked writes, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition-replicas dbguard single-writer violations, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition-replicas dbguard runs with old primary fenced and fence outcome, results/partition-replicas_dbguard.jsonl]]` |
| naive | `[[N: partition-replicas naive run count, results/partition-replicas_naive.jsonl]]` | `[[N: partition-replicas naive failover p50, results/partition-replicas_naive.jsonl]]` | `[[N: partition-replicas naive failover p99, results/partition-replicas_naive.jsonl]]` | `[[N: partition-replicas naive stall p50, results/partition-replicas_naive.jsonl]]` | `[[N: partition-replicas naive stall p99, results/partition-replicas_naive.jsonl]]` | `[[N: partition-replicas naive lost acked writes, results/partition-replicas_naive.jsonl]]` | `[[N: partition-replicas naive single-writer violations, results/partition-replicas_naive.jsonl]]` | `[[N: partition-replicas naive runs with old primary fenced, results/partition-replicas_naive.jsonl]]` |

The stall columns are the availability cost the design chose. For that long, clients were
waiting on commits that the primary refused to acknowledge because no replica could hold
them. The naive primary keeps acknowledging writes that exist only on itself, so it may show
no stall at all, and whatever it accepted during the partition is what goes missing after
the failover. The last column comes from each row's `old_primary_fenced` field and the
event's `steps.fence.outcome` (`sql` or `kill`), which `bin/report` does not tabulate.

### Experiment 4. Manager partitioned from the primary

`iptables` in the manager's container cuts it off from the primary. The replicas and the
clients are fine, so the correct action is no action.

| mode | runs | false failovers | lost acked writes | converged | rs2 changes |
|---|---|---|---|---|---|
| dbguard | `[[N: partition-manager dbguard run count, results/partition-manager_dbguard.jsonl]]` | `[[N: false failovers dbguard, results/partition-manager_dbguard.jsonl]]` | `[[N: partition-manager dbguard lost acked writes, results/partition-manager_dbguard.jsonl]]` | `[[N: partition-manager dbguard converged runs, results/partition-manager_dbguard.jsonl]]` | `[[N: partition-manager dbguard rs2 changes, results/partition-manager_dbguard.jsonl]]` |
| naive | `[[N: partition-manager naive run count, results/partition-manager_naive.jsonl]]` | `[[N: false failovers naive, results/partition-manager_naive.jsonl]]` | `[[N: partition-manager naive lost acked writes, results/partition-manager_naive.jsonl]]` | `[[N: partition-manager naive converged runs, results/partition-manager_naive.jsonl]]` | `[[N: partition-manager naive rs2 changes, results/partition-manager_naive.jsonl]]` |
| orchestrator | `[[N: partition-manager orchestrator run count, results/partition-manager_orchestrator.jsonl]]` | `[[N: false failovers orchestrator, results/partition-manager_orchestrator.jsonl]]` | `[[N: partition-manager orchestrator lost acked writes, results/partition-manager_orchestrator.jsonl]]` | `[[N: partition-manager orchestrator converged runs, results/partition-manager_orchestrator.jsonl]]` | `[[N: partition-manager orchestrator rs2 changes, results/partition-manager_orchestrator.jsonl]]` |

DBGuard declares a primary dead only when its own probe fails and a majority of the
replicas that can be asked also report losing it, so here it sits in `SUSPECT` and logs.
Naive mode fails over on its own probe alone, and every false failover it makes is a real
failover of a healthy primary, with the aborted transactions and the rejoin work that come
with one. Orchestrator uses the same holistic idea, so it is expected to stay put too.

### Experiment 5. The cost of losslessness

The same workload against the same fleet with semi-sync on and with
`rpl_semi_sync_source_enabled=0`, with `tc netem delay` of 0, 2 and 20 ms added between the
primary and its replicas. Each cell is the median across runs of that run's percentile.

| mode | semi-sync | netem ms | runs | commit p50 ms | commit p99 ms | writes/s |
|---|---|---|---|---|---|---|
| dbguard | on | 0.00 | `[[N: cost runs on 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost p50 on 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 on 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps on 0ms, results/cost_dbguard.jsonl]]` |
| dbguard | on | 2.00 | `[[N: cost runs on 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost p50 on 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 on 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps on 2ms, results/cost_dbguard.jsonl]]` |
| dbguard | on | 20.00 | `[[N: cost runs on 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost p50 on 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 on 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps on 20ms, results/cost_dbguard.jsonl]]` |
| dbguard | off | 0.00 | `[[N: cost runs off 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost p50 off 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 off 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps off 0ms, results/cost_dbguard.jsonl]]` |
| dbguard | off | 2.00 | `[[N: cost runs off 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost p50 off 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 off 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps off 2ms, results/cost_dbguard.jsonl]]` |
| dbguard | off | 20.00 | `[[N: cost runs off 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost p50 off 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 off 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps off 20ms, results/cost_dbguard.jsonl]]` |

This is the price of the guarantee and it is not small. Every semi-sync commit waits for one
replica to receive the transaction, flush its relay log and send the ack, so the added delay
shows up in every commit, and with one write in flight per client the throughput falls with
it. The off rows are the noise floor, since an asynchronous primary never waits for a
replica. The 20 ms row is why a semi-sync replica belongs in the same region as its primary.
[docs/CAPACITY.md](docs/CAPACITY.md) turns this table into a sizing guide.

### Experiment 6. Replica loss and replacement

Kill one replica of a two-replica set, and the set runs `DEGRADED` on the survivor. Kill the
survivor too, and writes stall. Restore one, and writes resume. Then let `rebuild_after_s`
(60 s) expire and the manager starts the spare, clones it from a healthy replica and joins
it.

| mode | runs | stall p50 s (no replica) | resume p50 s | clones | clone MB | clone s | clone MB/s | lost acked writes |
|---|---|---|---|---|---|---|---|---|
| dbguard | `[[N: replica-loss dbguard run count, results/replica-loss_dbguard.jsonl]]` | `[[N: stall_s with no semi-sync replica, results/replica-loss_dbguard.jsonl]]` | `[[N: replica-loss resume p50, results/replica-loss_dbguard.jsonl]]` | `[[N: replica-loss clones, results/replica-loss_dbguard.jsonl]]` | `[[N: clone MB p50, results/replica-loss_dbguard.jsonl]]` | `[[N: clone duration p50, results/replica-loss_dbguard.jsonl]]` | `[[N: clone MB/s p50, results/replica-loss_dbguard.jsonl]]` | `[[N: replica-loss dbguard lost acked writes, results/replica-loss_dbguard.jsonl]]` |

Time to replace a lost replica, excluding the 60 s `rebuild_after_s` policy wait, was
`[[N: time to replace p50, results/replica-loss_dbguard.jsonl]]` on
`[[N: dataset size MB, results/replica-loss_dbguard.jsonl]]` of data.

The stall column is deliberate and it is unflattering. With no replica alive to acknowledge,
the primary stops completing commits instead of falling back to asynchronous replication,
and the stall lasts exactly as long as the harness keeps both replicas down. The manager logs
it as a `stall` event and `dbgctl doctor` says so in plain words. Clone throughput here is
one SSD shared by donor and recipient inside one Docker VM, so it is a floor for this laptop
and not a prediction for real hosts.

### Experiment 7. Planned switchover

`dbgctl failover rs1` while the workload runs. The old primary goes read-only first, the
candidate applies everything, then it is promoted.

| mode | runs | stall p50 s | stall p99 s | client errors | runs with errors | lost acked writes |
|---|---|---|---|---|---|---|
| dbguard | `[[N: switchover run count, results/switchover_dbguard.jsonl]]` | `[[N: switchover stall p50, results/switchover_dbguard.jsonl]]` | `[[N: switchover stall p99, results/switchover_dbguard.jsonl]]` | `[[N: switchover client errors, results/switchover_dbguard.jsonl]]` | `[[N: switchover runs with errors, results/switchover_dbguard.jsonl]]` | `[[N: switchover lost acked writes, results/switchover_dbguard.jsonl]]` |

The stall is the longest gap between consecutive acknowledged writes across all clients.
Client errors are the connections HAProxy closed when the old primary was marked down, and a
client that retries once reconnects to the new primary. A planned switchover cannot lose an
acknowledged write, so a nonzero last column would be a bug.

### Disk full

The primary's binary log volume fills up while the workload runs.

| scenario | mode | runs | failover p50 s | failover p99 s | lost acked writes | single-writer violations | converged |
|---|---|---|---|---|---|---|---|
| disk-full | dbguard | `[[N: disk-full dbguard run count, results/disk-full_dbguard.jsonl]]` | `[[N: disk-full dbguard failover p50, results/disk-full_dbguard.jsonl]]` | `[[N: disk-full dbguard failover p99, results/disk-full_dbguard.jsonl]]` | `[[N: disk-full dbguard lost acked writes, results/disk-full_dbguard.jsonl]]` | `[[N: disk-full dbguard single-writer violations, results/disk-full_dbguard.jsonl]]` | `[[N: disk-full dbguard converged runs, results/disk-full_dbguard.jsonl]]` |

MySQL either waits on a full disk or, if a binlog write fails outright, aborts the server
under the default `binlog_error_action=ABORT_SERVER`. Either way commits stop, the heartbeat
goes stale on the replicas and the manager fails over. Which of the two the lab showed is
`[[N: disk-full primary behaviour and failover outcome, results/disk-full_dbguard.jsonl]]`.

### Two simultaneous losses, the published boundary

The primary and the replica with the largest `Retrieved_Gtid_Set` are killed at the same
instant.

| scenario | mode | runs | failover p50 s | failover p99 s | lost acked writes | runs with loss | phantom writes | converged |
|---|---|---|---|---|---|---|---|---|
| kill-two | dbguard | `[[N: kill-two dbguard run count, results/kill-two_dbguard.jsonl]]` | `[[N: kill-two dbguard failover p50, results/kill-two_dbguard.jsonl]]` | `[[N: kill-two dbguard failover p99, results/kill-two_dbguard.jsonl]]` | `[[N: kill-two dbguard lost acked writes, results/kill-two_dbguard.jsonl]]` | `[[N: kill-two dbguard runs with loss, results/kill-two_dbguard.jsonl]]` | `[[N: kill-two dbguard phantom writes, results/kill-two_dbguard.jsonl]]` | `[[N: kill-two dbguard converged runs, results/kill-two_dbguard.jsonl]]` |

This is the case semi-sync with `wait_for_replica_count=1` does not cover, and it is here so
the limit is measured rather than asserted. A write acknowledged by the replica that died
with the primary may exist on no surviving node. Nonzero losses in this row are the expected
result and not a failure of the implementation. Closing the window takes three replicas and
`wait_for_replica_count=2`, which costs the second-fastest ack in every commit and is not
built here.

### Fleet isolation

Every injection runs against `rs1` while the manager also runs `rs2`. Across
`[[N: total rs1 injections, results/*.jsonl]]` injections, `rs2` changed state
`[[N: rs2 state changes total across all rs1 injections, results/*.jsonl]]` times. Each set
has its own loop, state machine, cooldown and event stream, and this is the number that
shows a bug in per-set state would have been caught.

## How it works

```
  workload ----> haproxy:3306 (rs1)  ----> the one node whose agent answers GET /primary 200
                 haproxy:3307 (rs2)
                                          |
       rs1:  mysql-a1 (P)  mysql-a2 (R)  mysql-a3 (R)   spare mysql-a4   each with dbguard-agent :8080
       rs2:  mysql-b1 (P)  mysql-b2 (R)  mysql-b3 (R)   spare mysql-b4   each with dbguard-agent :8080
                                          ^
       dbguard (manager, :9090) ----------+   polls every mysqld and every agent, decides, acts
```

Every node runs GTID replication with semi-sync `AFTER_SYNC`, one required ack and a one
hour timeout, so a primary never acknowledges a write that is not already in a replica's
relay log, and never quietly falls back to asynchronous replication
([DESIGN.md section 3](docs/DESIGN.md#3-semi-synchronous-replication)). The agent beside each
`mysqld` supervises it as a child process and is the only thing that changes its role. HAProxy
never decides anything. It sends writes to whichever agent answers `GET /primary` with 200,
so a fence is one state change on one agent
([section 4](docs/DESIGN.md#4-the-agent)).

The manager declares a primary dead only when its own probe fails three times and a majority
of the replicas it can reach also report losing the primary or a stale heartbeat, both
holding for `detect_window_s`. A manager that is the one cut off sees only half of that and
stays in `SUSPECT` ([section 5](docs/DESIGN.md#5-detection)). Then, in order
([section 6](docs/DESIGN.md#6-failover)),

1. **Fence** the old primary through its agent, by `super_read_only` if `mysqld` answers
   within 2 s and by `SIGKILL` of `mysqld` if it does not.
2. **Choose** the replica holding the most received transactions, and halt instead of
   guessing if any other candidate's GTID set is not a subset of the winner's.
3. **Catch up**, waiting until the winner has applied its whole relay log.
4. **Promote** it, with semi-sync on the source side switched on before it becomes writable.
5. **Repoint** the other replicas with `SOURCE_AUTO_POSITION=1`, never naming a binlog file.
6. **Record** one event with every step's duration, the fence outcome and the watermark.

When the old primary comes back, its `gtid_executed` decides its fate. A subset of the new
primary's is repointed. Anything else holds transactions no client was ever told committed,
and it is rebuilt with `CLONE INSTANCE` from a replica
([section 7](docs/DESIGN.md#7-rejoin)). A set short of replicas for 60 s gets the spare,
cloned from a replica and never from the primary
([section 8](docs/DESIGN.md#8-replacement-and-the-fleet)).

## Run it

Prerequisites are Docker Desktop with at least 8 GB of memory assigned to it, Python 3.12
and `make`.

```sh
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
pytest -q -m "not integration"           # unit and hypothesis suites, no Docker needed

make build                               # images dbguard-node and dbguard-manager
make up                                  # six mysqld, HAProxy, the manager, then bin/bootstrap
dbgctl status                            # both sets HEALTHY
```

Write through HAProxy and check the result by hand.

```sh
bin/workload --port 13306 --clients 8 --duration 60 --run-id smoke \
    --ack-log results/tmp/smoke.ack.jsonl
bin/checker --run-id smoke --ack-log results/tmp/smoke.ack.jsonl \
    --nodes mysql-a1:13311,mysql-a2:13312,mysql-a3:13313
```

(`make workload` and `make check` do the same two steps.) Run one experiment, then all of
them, then build the tables.

```sh
bin/chaos --scenario kill --runs 30                # one row per run in results/kill_dbguard.jsonl
bin/chaos --all --runs 30                          # every scenario and every cost variant
bin/report                                         # writes results/SUMMARY.md and summary.json
```

The asynchronous baseline is the same fleet restarted in naive mode.

```sh
make down && make up-naive
bin/chaos --all --runs 30 --mode naive
```

The Orchestrator baseline runs on the normal semi-sync fleet. The harness starts the
`orchestrator` compose profile, stops the DBGuard manager for the whole campaign so only one
automation acts, and puts it back afterwards. Orchestrator skips `cost` and `replica-loss`,
where it has nothing to compare.

```sh
make down && make up
bin/chaos --all --runs 30 --mode orchestrator
```

The three-minute narrated demo, which kills the primary under load and shows the rejoin and
the checker's verdict, is `bin/demo`. Its script is [docs/DEMO.md](docs/DEMO.md). `make down`
removes everything, volumes included.

## Operating it

`dbgctl` talks only to the manager's HTTP API (`http://127.0.0.1:19090` by default, or
`--manager`). The outputs below are illustrative, showing the format and not measured
values.

```
$ dbgctl status
mode: dbguard
SET  STATE    PRIMARY   REPLICAS                                                          LAST EVENT
---  -------  --------  ----------------------------------------------------------------  -----------------------------------------------
rs1  HEALTHY  mysql-a3  mysql-a1(lag 0.0s, ss replica), mysql-a2(lag 0.1s, ss replica)  14:02:11 rs1 rejoin mysql-a1 -> mysql-a3 trigger=dead rejoin=repoint phantom=0 in 3.10s
rs2  HEALTHY  mysql-b1  mysql-b2(lag 0.0s, ss replica), mysql-b3(lag 0.0s, ss replica)  -
```

```
$ dbgctl doctor rs1
rs1: primary mysql-a1 unreachable from manager for 3.2 s (connection refused), 2 of 2 replicas report heartbeat stale for 4.1 s, verdict FAILED, would fence and promote mysql-a3 (retrieved set is 12 transactions ahead of mysql-a2).
mysql-a2 replicates from mysql-a1, IO Connecting, SQL Yes, heartbeat 4.1 s old, missing 12 transactions of the primary.
verdict: FAILED
GTID gaps versus the primary (GTID_SUBTRACT(primary, node)):
  mysql-a2: 5c1e...:4190-4201
  mysql-a3: none
```

```
$ dbgctl failover rs1 --to mysql-a2
switchover done in 0.61s: 14:05:40 rs1 switchover mysql-a3 -> mysql-a2 trigger=planned fence=sql total=0.48s
```

```
$ dbgctl halt rs1
rs1: state HALTED
$ dbgctl resume rs1
rs1: state HEALTHY
```

```
$ dbgctl events --rs rs1 --since 10m
14:01:52 rs1 failover mysql-a1 -> mysql-a3 trigger=dead fence=unreachable total=6.84s
14:02:11 rs1 rejoin mysql-a1 -> mysql-a3 trigger=dead rejoin=repoint phantom=0 in 3.10s
```

`dbgctl rejoin rs1 mysql-a1` triggers a rejoin by hand, which is what an operator runs with
`rejoin: manual` in `fleet.yaml`. `dbgctl events --follow` tails the event log.

**Metrics.** The manager serves Prometheus text on `:9090/metrics` (host port 19090) with
state per set (`dbguard_set_state`), failovers by outcome, detect, fence, promote and repoint
durations, phantom GTIDs discarded, replicas rebuilt, replica lag, heartbeat age and the
primary's semi-sync wait from `Rpl_semi_sync_source_tx_avg_wait_time`. Every agent serves its
own on `:8080/metrics` (host ports 18011 to 18024) with its role, fence flag, fences by
method, restarts of `mysqld`, heartbeat age and wake-guard firings.

**systemd.** `deploy/systemd/dbguard.service` runs the manager as an unprivileged user and
`deploy/systemd/dbguard-agent.service` runs the agent as root on each MySQL host, replacing
`mysql.service`, because it has to be able to `SIGKILL` and restart a `mysqld` that will not
take a fence in SQL. Both were tested under systemd in an Ubuntu 24.04 container.
[deploy/systemd/README.md](deploy/systemd/README.md) has the install steps.

**When something is wrong**, [docs/RUNBOOK.md](docs/RUNBOOK.md) has a procedure for each case,
a set `HALTED` because replicas diverged, a primary stalling writes, a lagging replica, a
failing clone, a dead manager, the woken primary, a full disk, and "writes are failing, go".
[docs/CAPACITY.md](docs/CAPACITY.md) has the failover budget and how to pick
`detect_window_s`.

## Baselines

**Naive mode** (`--mode naive`, fleet started with `make up-naive`) is a deliberately careless
version of the same program. Asynchronous replication, detection from the manager's probe
alone, no fence, promote the most advanced replica, no subset check. It is real and runnable,
and it exists to show that each of those steps changes a number. Its losses in Experiment 1
come from the one thing asynchronous replication cannot do, which is keep a write the primary
acknowledged before any replica received it.

**Orchestrator** (openark/orchestrator, originally by Shlomi Noach, run from
`percona/percona-orchestrator:3.2.6-24` because the upstream image predates MySQL 8.4) is the
industry tool and the model for DBGuard's holistic detection. It runs with its own defaults,
and its pre-failover hook flips the agent's fence flag so it uses the same HAProxy pattern. It
does a great deal DBGuard does not, including arbitrary topologies with intermediate
primaries, promotion rules aware of datacenters and serving capacity, graceful takeover, a web
UI and a Raft mode for its own availability. What DBGuard adds by default is fencing the old
primary itself down to killing the process, refusing to promote when replicas have diverged,
deciding by GTID subset whether a crashed primary may be repointed or must be rebuilt, the
clone rebuild, and replacement of a lost replica. The comparison is in
[DESIGN.md section 10](docs/DESIGN.md#10-the-baselines).

## Bugs found while building it

[docs/BUGS.md](docs/BUGS.md) has `[[N: BUGS.md entry count, docs/BUGS.md]]` entries, each with
the symptom, how it was found and the fix. Among them are a `Retrieved_Gtid_Set` that is not
cumulative, so "largest retrieved set" could pick the wrong winner, a fenced old primary that
voted the new primary dead, the empty GTID set passing every subset check, a semi-sync stall
that made the SQL fence impossible, and `docker kill -s STOP` freezing nothing but `tini`.

## What it does not do

- Survive the loss of the primary and the replica that acknowledged a write at the same
  moment. That needs `wait_for_replica_count=2` with three replicas, which is not built.
- Protect a write acknowledged by a replica that is unreachable when the manager chooses the
  winner. The manager compares only the replicas it can see.
- Fail over without the manager. There is one manager, and if it dies nothing fails over,
  which is safe because no action cannot lose data.
- Detect silent corruption or drift that does not show up in GTID sets.
- Run anywhere but Docker on one laptop, so every network is a Linux bridge and every disk is
  the same disk.

Out of scope and not argued back in are sharding or query routing, a proxy other than HAProxy,
backups beyond the clone plugin, a MySQL fork or patch, a web UI, multi-manager consensus or
Raft, Postgres, Kubernetes operators, and an online schema change tool. The full list with the
reasons is [DESIGN.md section 12](docs/DESIGN.md#12-what-dbguard-cannot-do).

## Sources

The design is assembled from public write-ups and the MySQL 8.4 reference manual, all credited
with links in [DESIGN.md section 13](docs/DESIGN.md#13-credits-and-sources). That includes the
2014 posts on GTID and semi-sync at large MySQL deployments, Philipp Heckel's write-up of
lossless semi-sync with automated failover, the openark/orchestrator documentation on failure
detection and topology recovery, and Jepsen's discipline of checking a recorded history
against the database afterwards.

## Layout

```
dbguard/
  gtid.py            GTID sets as sets, hypothesis-tested against GTID_SUBSET and GTID_SUBTRACT
  config.py          fleet.yaml models
  events.py          the JSON event row
  agent/             dbguard-agent, supervises mysqld, HTTP :8080, fence, promote, repoint, rebuild
  manager/           dbguard, detection, failover, rejoin, replacement, switchover, doctor, :9090
  cli/               dbgctl
  harness/           chaos, workload, checker, report, bootstrap
bin/                 chaos, workload, checker, report, demo, bootstrap
deploy/              compose fleet, Dockerfiles, my.cnf and init SQL, HAProxy, Orchestrator, systemd
docs/                DESIGN, CAPACITY, RUNBOOK, BUGS, INTERFACES, DEMO
tests/               pytest and hypothesis
results/             one JSONL row per chaos run, SUMMARY.md from bin/report
```
