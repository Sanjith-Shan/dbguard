# DBGuard capacity plan

This is the structure of the capacity plan and the arithmetic behind it. Every measured number is a tag of the form `[[N: what, source file]]` until `bin/report` fills it from `results/`. The lab is six `mysqld` in Docker Desktop on one Apple M3 Pro laptop, so absolute numbers describe that laptop. The ratios and the shape of the curves are the part that transfers.

## 1. What losslessness costs per commit

Experiment 5 runs the standard workload (8 clients, one autocommit `INSERT` at a time, through HAProxy) against the same fleet with semi-sync on and with `rpl_semi_sync_source_enabled=0`, and with `tc netem delay` of 0, 2 and 20 ms added between the primary and its replicas. Each cell is `[[N: ...]]` from `results/cost_dbguard.jsonl`, 30 runs per cell.

| Semi-sync | Added delay | Commit p50 (ms) | Commit p99 (ms) | Writes/s |
|---|---|---|---|---|
| off | 0 ms | `[[N: cost p50 off 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 off 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps off 0ms, results/cost_dbguard.jsonl]]` |
| on | 0 ms | `[[N: cost p50 on 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 on 0ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps on 0ms, results/cost_dbguard.jsonl]]` |
| off | 2 ms | `[[N: cost p50 off 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 off 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps off 2ms, results/cost_dbguard.jsonl]]` |
| on | 2 ms | `[[N: cost p50 on 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 on 2ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps on 2ms, results/cost_dbguard.jsonl]]` |
| off | 20 ms | `[[N: cost p50 off 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 off 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps off 20ms, results/cost_dbguard.jsonl]]` |
| on | 20 ms | `[[N: cost p50 on 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost p99 on 20ms, results/cost_dbguard.jsonl]]` | `[[N: cost wps on 20ms, results/cost_dbguard.jsonl]]` |

The primary also reports its own view of the wait, `Rpl_semi_sync_source_tx_avg_wait_time` in microseconds, which was `[[N: avg semi-sync wait us per netem level, results/cost_dbguard.jsonl]]`.

### How to read it

With semi-sync off, the added delay should barely move commit latency, because the primary never waits for the replica. Any movement there is the cost of the netem qdisc and of the shared laptop, and it is the noise floor for the rest of the table.

With semi-sync on, each commit waits for one replica to receive the transaction, write it to the relay log, flush it and send the ack. So the expected cost is roughly one round trip plus one replica fsync on top of the asynchronous latency, and the 2 ms and 20 ms rows should show the added delay appearing once per commit (netem on the primary's egress delays one direction, so check which direction the harness shaped before reading the rows as a full round trip). The 0 ms row is the cost of the fsync and the ack path alone.

Throughput for this workload is clients divided by latency, because each client has one write in flight. Eight clients at a 20 ms commit cannot exceed about 400 writes per second no matter how fast the disks are. A real application with more concurrency gets more throughput from the same latency, because commits waiting for acks are grouped. So the writes/s column describes this workload, and the latency columns are the ones to carry to another system.

What the table means for placement. A replica in the same zone (the 2 ms row) is a latency cost most OLTP applications can pay. A semi-sync replica in another region (the 20 ms row) puts a cross-region round trip in every commit. That is why large deployments ack from something close and cheap, such as the semisync binlog servers in Matsunobu's post, and replicate across regions asynchronously behind it.

### The stall

With the one-hour timeout, a primary with no replica able to ack does not fall back to asynchronous replication. Commits stop. In Experiment 6 the stall lasted `[[N: stall_s with no semi-sync replica, results/replica-loss_dbguard.jsonl]]`, which is exactly as long as the harness kept both replicas down. In Experiment 3 (primary partitioned from its replicas) clients were stalled for `[[N: partition stall_s p50, results/partition-replicas_dbguard.jsonl]]` median and `[[N: partition stall_s p99, results/partition-replicas_dbguard.jsonl]]` p99 before the failover restored writes. These are the availability price of the guarantee and they are published as such.

## 2. The failover budget

Failover time is measured from injection to the first acknowledged write whose request started after the first client error. The second half of that rule matters. Eight clients take their timestamps independently, and a write that committed just before a kill can be recorded just after the first error, which made some early failovers look like 3 ms. When a failover produces no client error at all (commits only blocked), it ends at the end of the largest gap between acknowledged writes after the injection.

In dbguard mode the fence runs concurrently with stopping IO threads, choosing, catching up and repointing, and only the promote waits for it. So the terms add up like this.

```
T_failover = T_evidence                  time until both detection conditions have started
           + detect_window_s             both conditions must hold this long
           + max( T_fence,               bounded by fence_deadline_s
                  T_stopio + T_choose    STOP REPLICA IO_THREAD on candidates, one /status round
                  + T_catchup            bounded by catchup_deadline_s
                  + T_repoint )          repoint the others to the read-only winner
           + T_promote                   a handful of SQL statements on one node
           + T_proxy                     HAProxy rise * inter before it routes to the new primary
           + T_client                    the client noticing the error and reconnecting
```

`T_evidence` depends on the failure. For the manager's probe it is at most one poll interval plus one probe timeout. For the replicas it is about one poll interval after a kill (the IO thread sees the connection close), about `replica_net_timeout` after a hang (no data and no heartbeat until the timeout), and one agent TCP connect timeout after a partition (the agent's connect to the primary gets no answer). Three consecutive probe failures fit inside the detect window, so `probe_failures` does not add time with the defaults.

With the defaults in `deploy/fleet.yaml` and `my.cnf`, and HAProxy's `inter 500ms rise 1 fall 1` from `deploy/haproxy/haproxy.cfg`.

| Term | Default bound | Source of the bound |
|---|---|---|
| `T_evidence` | 2.5 s | max(poll 0.5 + probe timeout 1.0, poll 0.5 + `replica_net_timeout` 2) |
| `detect_window_s` | 5.0 s | `fleet.yaml` |
| `T_fence` | 3.0 s | `fence_deadline_s` |
| `T_choose` | `[[N: choose step p99, results/kill_dbguard.jsonl]]` | measured |
| `T_catchup` | 30.0 s | `catchup_deadline_s`, usually close to zero |
| `T_repoint` | 30 s | the agent's role-change budget (the manager waits 35 s). Measured `[[N: repoint step p50, results/kill_dbguard.jsonl]]` |
| `T_promote` | `[[N: promote step p99, results/kill_dbguard.jsonl]]` | measured, bounded by the agent's 30 s role-change budget |
| `T_proxy` | 0.5 s | rise 1 times inter 0.5 s |
| `T_client` | `[[N: client reconnect gap p50, results/kill_dbguard.jsonl]]` | measured |

In the common case the winner has no relay backlog and the repoint finishes inside the fence deadline, so the budget is 2.5 + 5 + 3 + 0.5 = 11 s plus the promote and the client reconnect. The bounded worst case, with catch-up and repoint both using their whole budgets and the promote its 30 s, is 2.5 + 5 + 30 + 30 + 30 + 0.5 = 98 s plus the stop and choose terms, which is where a failover would end rather than a number anyone should plan around. Each step's duration comes from the event rows, so a slow failover can be attributed to one term.

The order matters for the client more than the sum does. Before the reorder the new primary was promoted first and repointed its replicas afterwards, so it had semi-sync on and nobody to ack, and every commit waited out the repoint. `START REPLICA` took 1.7 to 2.4 s on the real fleet, and a planned switchover stalled clients 3.8 s while its steps took 0.9 s. Repointing to the read-only winner first brought the switchover stall to 0.89 s (`docs/BUGS.md`). The failover budget above assumes the same reorder.

The measured distribution follows.

| Scenario | Failover p50 | Failover p99 | Runs |
|---|---|---|---|
| primary killed | `[[N: kill failover p50, results/kill_dbguard.jsonl]]` | `[[N: kill failover p99, results/kill_dbguard.jsonl]]` | `[[N: kill run count, results/kill_dbguard.jsonl]]` |
| primary frozen | `[[N: hang failover p50, results/hang-container_dbguard.jsonl]]` | `[[N: hang failover p99, results/hang-container_dbguard.jsonl]]` | `[[N: hang run count, results/hang-container_dbguard.jsonl]]` |
| primary partitioned from replicas | `[[N: partition failover p50, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition failover p99, results/partition-replicas_dbguard.jsonl]]` | `[[N: partition run count, results/partition-replicas_dbguard.jsonl]]` |
| planned switchover (client stall) | `[[N: switchover stall p50, results/switchover_dbguard.jsonl]]` | `[[N: switchover stall p99, results/switchover_dbguard.jsonl]]` | `[[N: switchover run count, results/switchover_dbguard.jsonl]]` |
| naive, primary killed | `[[N: kill failover p50 naive, results/kill_naive.jsonl]]` | `[[N: kill failover p99 naive, results/kill_naive.jsonl]]` | `[[N: kill run count naive, results/kill_naive.jsonl]]` |
| Orchestrator, primary killed | `[[N: kill failover p50 orchestrator, results/kill_orchestrator.jsonl]]` | `[[N: kill failover p99 orchestrator, results/kill_orchestrator.jsonl]]` | `[[N: kill run count orchestrator, results/kill_orchestrator.jsonl]]` |

## 3. Picking detect_window and probe_timeout

The two failure modes pull in opposite directions.

A short window recovers faster, and every second of it is a second of write unavailability on a real failure. A long window rides out more transient trouble without a failover. A false failover costs every in-flight transaction, a few seconds with no writer, a rejoin or a rebuild of the old primary, and a cooldown during which a real failure is not handled.

Holistic detection changes the trade. Because a failover needs both the manager's probe and a majority of the reachable replicas that replicate from the primary, a short manager-side network blip cannot cause one alone, and replica lag no longer looks like a dead primary, because heartbeat age votes only after subtracting `Seconds_Behind_Source`. One consequence to plan for is that a `DEGRADED` set with one live replica fails over on that single witness. What remains is the case where the primary itself is briefly unable to serve, such as a long stall on disk, a big transaction's commit or a CPU-starved container, and both views see it. So the window should be longer than the longest stall the primary is expected to survive on its own, and no longer.

A procedure for a team.

1. Measure the primary's longest normal commit stall and heartbeat gap under peak load and during the heaviest routine job (backup, schema change). The agent's heartbeat age and the manager's probe latency on `/metrics` give both.
2. Set `probe_timeout_s` above the p99.9 of a probe write under peak load, so a slow write is not a failed probe.
3. Set `detect_window_s` to a margin above the longest normal stall from step 1.
4. Check the budget in section 2 against the recovery objective. If the sum is too long, the fix is usually to shorten the stalls, not the window.
5. Run the manager-partition experiment and a hang shorter than the window. The false failover count has to stay zero. It was `[[N: false failovers dbguard, results/partition-manager_dbguard.jsonl]]` for DBGuard and `[[N: false failovers naive, results/partition-manager_naive.jsonl]]` for naive mode.

## 4. Sizing a replica set

With `R` semi-sync replicas and `W = rpl_semi_sync_source_wait_for_replica_count`.

| Question | Answer |
|---|---|
| How many replica losses stall writes | `R - W + 1` at once. With `R=2, W=1`, both replicas |
| How many losses can lose an acknowledged write | the primary plus the `W` replicas that acked it, before a failover reads them. With `W=1`, two nodes |
| Commit latency | the `W`-th fastest ack among the connected replicas |
| Failover candidates after a primary loss | `R`, and at least `W` of them hold every acknowledged write |

The trade in words. Raising `W` closes the double-loss window and makes commits wait for a slower ack. Raising `R` at a fixed `W` makes stalls rarer and costs a host, and it also speeds up commits a little because the `W`-th fastest of more replicas is faster. DBGuard's default, `R=2, W=1`, tolerates one replica loss without a stall and one node loss without data loss. `R=3, W=2` tolerates one replica loss without a stall and any two node losses without data loss, at the price of the second-fastest ack in every commit. That configuration is a stretch milestone and has not been measured here.

How long a set is exposed matters as much as the counts. A set that has lost a replica runs `DEGRADED` with no margin until a replacement joins, so the double-loss window is roughly the replacement time in section 6, and shortening `rebuild_after_s` or speeding up the clone shrinks it directly.

## 5. Applier parallelism, catch-up and memory

Semi-sync bounds how far behind a replica's relay log can be, not how far behind its applier is, and failover waits for the applier. So the replica's apply rate is part of the failover budget. If a replica cannot apply as fast as the primary commits, its lag grows without bound, and a failover after enough load cannot finish catch-up inside `catchup_deadline_s`.

The lab hit both sides of this (`docs/BUGS.md`, measured on the M3 Pro).

| Applier workers | Container limit | What happened |
|---|---|---|
| 4 (the 8.4 default) | 700 MiB | lag grew about 1.4 s per 5 s under 8 clients at about 720 writes/s, and convergence took 43.9 s after five minutes, past the 30 s catch-up deadline |
| 16 | 700 MiB | lag 0 to 1 s, but replicas sat at 655 to 697 MiB, and `mysqld` was OOM-killed as `START REPLICA` spawned the workers during repoints |
| 8 | 1 GiB | lag 0 to 1 s, replica memory plateaued near 549 MiB under 60 s of load (975 writes/s acked) |

The rule a team can use has two halves.

1. **Enough parallelism.** Pick `replica_parallel_workers` so the replica's sustained apply rate exceeds the primary's peak commit rate with margin, measured under the real write mix. `replica_preserve_commit_order=ON` keeps replicas' commit order equal to the primary's, which the GTID subset reasoning assumes. Check it by sampling `Seconds_Behind_Source` (or heartbeat age) through a peak. It must stay flat, not merely small. A lag that grows at r seconds per second leaves `catchup_deadline_s / r` seconds of peak load before a failover can no longer finish.
2. **Memory for that parallelism.** Every worker has its own buffers, and they are allocated at `START REPLICA`, which is exactly when a failover or rejoin runs. A replica sized for its idle footprint dies at the worst moment. Size the container for the loaded peak with the chosen worker count plus headroom, and remember that a repoint restarts the workers. The replica's memory under load with 8 workers is `[[N: node mem loaded MB, docker stats]]` in section 7.

## 6. Clone throughput and time to replace

```
T_replace = rebuild_after_s                         (120 s in the lab, a policy choice)
          + T_provision                             (lab, start a spare container; fleet, the allocator)
          + dataset_bytes / clone_throughput
          + T_restart                               (mysqld restart after clone, driven by the agent)
          + T_catchup                               (binlog written on the primary during the clone)
          + T_repoint
```

| Measure | Value |
|---|---|
| dataset size in the experiment | `[[N: dataset size MB, results/replica-loss_dbguard.jsonl]]` |
| clone throughput | `[[N: clone MB/s p50, results/replica-loss_dbguard.jsonl]]` |
| clone duration | `[[N: clone duration p50, results/replica-loss_dbguard.jsonl]]` |
| restart after clone | `[[N: post-clone restart p50, results/replica-loss_dbguard.jsonl]]` |
| total time to replace, excluding `rebuild_after_s` | `[[N: time to replace p50, results/replica-loss_dbguard.jsonl]]` |
| rejoin by rebuild after a primary kill | `[[N: rejoin rebuild duration p50, results/kill_dbguard.jsonl]]` |
| rejoin by repoint after a primary kill | `[[N: rejoin repoint duration p50, results/kill_dbguard.jsonl]]` |

For a larger dataset, extrapolate linearly from the measured throughput only as a first guess. In the lab donor and recipient share one SSD and one Docker VM, so the clone competes with itself for disk. Between real hosts the limit is usually the network or the donor's read rate, and the clone's effect on the donor matters, which is why DBGuard always clones from a replica. Catch-up grows with the primary's write rate times the clone duration, so a very busy primary can make a large clone chase its tail.

## 7. Footprint of the lab

Read from `docker stats --no-stream` with the fleet idle and under the standard workload.

| Component | Count | Memory each, idle | Memory each, loaded | CPU each, loaded |
|---|---|---|---|---|
| node container (`mysqld` + agent) | 8 (6 active, 2 spares) | `[[N: node mem idle MB, docker stats]]` | `[[N: node mem loaded MB, docker stats]]` | `[[N: node cpu loaded pct, docker stats]]` |
| manager | 1 | `[[N: manager mem MB, docker stats]]` | `[[N: manager mem loaded MB, docker stats]]` | `[[N: manager cpu loaded pct, docker stats]]` |
| HAProxy | 1 | `[[N: haproxy mem MB, docker stats]]` | `[[N: haproxy mem loaded MB, docker stats]]` | `[[N: haproxy cpu loaded pct, docker stats]]` |
| agent process alone | 8 | `[[N: agent RSS MB, ps inside container]]` | | |

The `mysqld` numbers are dominated by `innodb_buffer_pool_size` (128M here), the per-connection buffers and the applier workers, so they reflect the lab's `my.cnf`, not what MySQL needs in production. Each node runs with `mem_limit: 1g`. The fleet's own spot checks in `docs/BUGS.md` put an idle node at 397.6 MiB and a loaded replica with 8 workers at a plateau near 549 MiB, which is why seven active nodes, HAProxy, the manager and Orchestrator fit in the 7.7 GB Docker VM even though seven limits add up to 7 GiB.
