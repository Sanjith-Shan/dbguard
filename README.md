# DBGuard

![CI](https://github.com/Sanjith-Shan/dbguard/actions/workflows/ci.yml/badge.svg)

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
below publishes exactly that boundary. The naive asynchronous baseline also lost nothing in
30 kills on this single host, because replication between containers on one machine is
faster than the crash window, so the lab shows the async window only once a replica delay is
added (the 2 ms table below).

## Results

Every number is produced by `bin/chaos` and summarised by `bin/report` into
`results/SUMMARY.md`. A tag of the form `[[N: what, source file]]` marks a number whose rows
had not landed when this page was written, and `bin/fill-docs` fills it from those rows.
Percentiles are nearest-rank. The tables use the column names `bin/report` prints, with only
the columns each experiment needs.

Environment. MySQL 8.4.11 on Docker Desktop 4.71 (engine 29.4.1), Apple M3 Pro, a Docker VM
with 12 CPUs and 7.7 GB of memory. `detect_window_s=5`, `probe_timeout_s=1`, three failed
probes, 8 clients doing one autocommit `INSERT` at a time through HAProxy, a 30 s workload
with the fault injected at 8 s. The campaign ran on 2026-09-24 and its configuration is
tagged `campaign-config-2026-09-24`. The configuration changed twice during the campaign and
[results/CONFIG_HISTORY.md](results/CONFIG_HISTORY.md) records which one each table ran
under. The sentence under each table says the same.

### At a glance

| | |
|---|---|
| Primary kills, DBGuard | 30 runs |
| Acknowledged writes lost by DBGuard across every measured injection | **0** |
| Acknowledged writes lost by the asynchronous baseline, 0 ms and 2 ms replica delay | **0** of 30 kills at 0 ms, 2 ms rows pending |
| Failover after a primary kill, median | **8.83 s** |
| False failovers with the manager partitioned from the primary | **0** of 30 |
| Commit latency cost of semi-sync at the median, no added delay | +1.6 ms (5.9 ms on vs 4.2 ms off) |

### Primary killed

`docker kill -s KILL` on the primary while the workload runs. Failover time is injection to
the first successful write through HAProxy, measured on the clients' clock.

| mode | runs | failover p50 s | failover p99 s | lost acked writes | runs with loss | phantom writes | single-writer violations | converged |
|---|---|---|---|---|---|---|---|---|
| dbguard | 30 | 8.83 | 9.75 | 0 | 0 | 0 | 0 | 30/30 |
| naive | 30 | 5.45 | 5.85 | 0 | 0 | 0 | 0 | 30/30 |

The dbguard row ran under the earlier `agent-live-primary-check` configuration (the agent
answered HAProxy's health check with a live SQL query, HAProxy `fall 1`, before the rejoin
quiescence fix), and its planned rerun was dropped at the deadline. About 9 s is the detection
window (three failed probes plus `detect_window_s=5` of agreement from the replicas) followed
by the fence, the catch-up and the promotion. The naive row runs asynchronous replication with
no fence and no subset check, under the final configuration, and it decides on the manager's
own probe alone, so it fails over about 3 s faster. It also lost nothing. That does not say
asynchronous replication is safe. It says that on one host the replicas receive each
transaction far sooner than a `docker kill` can land between the commit and its replication,
so the single-host lab hides the async window. The table below adds the delay a real network
has.

#### Kill with a 2 ms replica delay

`tc netem delay 2ms` between the primary and its replicas, the same kill, 10 runs per mode,
both under the final configuration.

| mode | runs | failover p50 s | failover p99 s | lost acked writes | runs with loss |
|---|---|---|---|---|---|
| naive | pending | | | | |
| dbguard | pending | | | | |

The 2 ms rows are still being measured. When they land, the naive row is expected to show the async window that the single-host 0 ms runs hide.

Rejoin of the killed primary once its container is back.

| scenario | mode | rejoins | repoint | rebuild | other | phantom GTIDs mean | phantom GTIDs max | rejoin p50 s | rejoin p99 s |
|---|---|---|---|---|---|---|---|---|---|
| kill | dbguard | 30 | 5 | 25 | 0 | 3.37 | 8 | 4.46 | 16.55 |
| kill | naive | 30 | 8 | 22 | 0 | 2.33 | 8 | 6.64 | 8.62 |

Same configuration as the table above. Repoint means the old primary's `gtid_executed` was a
subset of the new primary's after crash recovery, so it rejoined with no data copy. Rebuild
means it held transactions that reached its binary log but were never acknowledged to a
client, and the phantom columns count how many of those the clone discarded (the mean is over
all 30 rejoins, repoints count as 0). Under 8 clients a crashed primary usually has a few
unacknowledged transactions in its binary log, so 25 of 30 needed a clone.

### Primary partitioned from its replicas

`iptables` drops the primary's traffic to both replicas, while the manager and HAProxy can
still reach it. With semi-sync every commit on the primary now blocks.

| mode | runs | failover p50 s | failover p99 s | stall p50 s | stall p99 s | lost acked writes | single-writer violations | old primary fenced |
|---|---|---|---|---|---|---|---|---|
| dbguard | 30 | 11.58 | 14.23 | 9.98 | 10.54 | 0 | 0 | 30/30 (0 sql, 30 kill) |

| scenario | mode | rejoins | repoint | rebuild | other | phantom GTIDs mean | phantom GTIDs max | rejoin p50 s | rejoin p99 s |
|---|---|---|---|---|---|---|---|---|---|
| partition-replicas | dbguard | 30 | 0 | 30 | 0 | 9.93 | 10 | 7.63 | 10.07 |

Measured under the final configuration (in-memory `/primary` check with a 2 s staleness
bound, HAProxy `fall 2`, the rejoin quiescence fix). The stall columns are the availability
cost the design chose. For about 10 s, clients waited on commits the primary refused to
acknowledge because no replica could hold them. Failover takes about 2.5 s longer than after a
kill because the primary still answers, so the replicas' votes carry the decision. Every fence
went through the kill path, because a primary stuck in a semi-sync wait cannot run `SET
GLOBAL super_read_only`. Every old primary was rebuilt, each holding 8 to 10 transactions that
had reached its binary log during the stall and that no client was ever told committed. The
spec asks the naive baseline only for kill and partition-manager, so there is no naive row.

### Manager partitioned from the primary

`iptables` in the manager's container cuts it off from the primary. The replicas and the
clients are fine, so the correct action is no action.

| mode | runs | false failovers | lost acked writes | converged |
|---|---|---|---|---|
| dbguard | 30 | 0 | 0 | 30/30 |

The dbguard row ran under the earlier `agent-live-primary-check` configuration. In 9 of its
30 runs the clients saw 8 or more errors although this scenario must see none. The cause was
the health-check bug that configuration had (a slow live-SQL `/primary` answer made HAProxy
mark a healthy primary down and cut every session), since fixed, and not a failover. DBGuard
declares a primary dead only when its own probe fails and a majority of the replicas that can
be asked also report losing it, so here it sits in `SUSPECT` and logs. Naive mode would fail over
on its own probe alone, but its runs of this scenario were dropped for time and are listed
under Not yet measured.

### Planned switchover

`dbgctl failover rs1` while the workload runs. The old primary goes read-only first, the
candidate applies everything, then it is promoted.

| mode | runs | stall p50 s | stall p99 s | client errors | runs with errors | lost acked writes |
|---|---|---|---|---|---|---|
| dbguard | 10 | 1.44 | 6.03 | 80 | 10 | 0 |

Measured under the final configuration (a 10-run rerun). No acknowledged write was lost and no run left errant GTIDs. Every run saw one HAProxy cut, 8 client errors, one per client. The stall is bimodal. Six runs stalled 0.53 to 1.65 s and four stalled 5.2 to 6.0 s, while the manager's own switchover took 0.39 to 1.94 s in every run, so the slow mode happens on the client side after the manager has finished. The likely cause, not yet verified, is that the cut clients reconnect through HAProxy before it has marked the new primary UP and each burns the lab client's 2 s handshake timeout. An earlier 30-run table under the
`agent-live-primary-check` configuration lost no acknowledged write, but its stall grew from
0.6 s in the first runs to 24 s as the host ran into swap (p50 2.06 s, p99 24.02 s), which
is why the health check was changed and the table rerun. Those rows are archived in
`results/old-config/agent-live-primary-check/`. The stall is the longest gap between
consecutive acknowledged writes across all clients. Client errors are the connections HAProxy
closed when the old primary was marked down, and a client that retries once reconnects to the
new primary. A nonzero last column would be a bug.

### The cost of losslessness

The same workload against the same fleet with semi-sync on and with
`rpl_semi_sync_source_enabled=0`, with `tc netem delay` of 0, 2 and 20 ms added between the
primary and its replicas, 60 s per run. Each cell is the median across runs of that run's
percentile.

| mode | semi-sync | netem ms | runs | commit p50 ms | commit p99 ms | writes/s |
|---|---|---|---|---|---|---|
| dbguard | on | 0 | 3 | 5.9 | 43.1 | 953 |
| dbguard | on | 2 | 3 | 9.8 | 52.1 | 615 |
| dbguard | on | 20 | 3 | 23.9 | 56.6 | 298 |
| dbguard | off | 0 | 3 | 4.2 | 26.5 | 1389 |
| dbguard | off | 2 | 3 | 4.4 | 43.6 | 1216 |
| dbguard | off | 20 | 3 | 4.4 | 29.6 | 1309 |

Measured under the final configuration. This is the price of the guarantee and it is not
small. With a local replica, semi-sync costs about 1.6 ms at the median and 31% of the
throughput. With 2 ms of delay to the replicas the median commit rises by 5.4 ms and
throughput halves. With 20 ms, the delay lands on every commit (23.9 ms against 4.4 ms) and
throughput falls by 77%, because each client has one write in flight. The off rows are the
noise floor, since an asynchronous primary never waits for a replica, and their p99 moves
between 26 and 44 ms with the host's own noise. The 20 ms row is why a semi-sync replica
belongs in the same region as its primary. [docs/CAPACITY.md](docs/CAPACITY.md) turns this
table into a sizing guide.

### Two simultaneous losses, the published boundary

The primary and the replica with the largest `Retrieved_Gtid_Set` are killed at the same
instant.

| scenario | mode | runs | lost acked writes | runs with loss | phantom writes | failover | converged after heal |
|---|---|---|---|---|---|---|---|
| kill-two | dbguard | 11 | 0 | 0 | 0 | none, writes stall until an operator acts | 11/11 |

Measured under the final configuration. This is the case semi-sync with
`wait_for_replica_count=1` does not cover, and it is here so the limit is measured rather
than asserted. In these 11 runs no acknowledged write was lost, but no run failed over
either. The manager promotes the last survivor, which has no replica to acknowledge its
commits, so every write stalls without limit. Both restarted nodes hold transactions the
survivor lacks (9 of 11 runs recorded errant GTIDs on them), they need a clone, and a clone
needs a replica donor that does not exist. Nothing moves until a human acts, here the harness
heal after 120 s. A write acknowledged only by the replica that died with the primary can
still exist on no surviving node, and these runs did not happen to land one. Closing the
window takes three replicas and `wait_for_replica_count=2`, which is not built here.

### Not yet measured

These scenarios are built and described, and the harness runs them, but the campaign above
did not measure them. Each will get a table when it has been run.

- **Primary hung** (`hang-container` freezes the whole container with the cgroup freezer,
  `hang-process` sends `SIGSTOP` to `mysqld` alone so the agent must fence through the kill
  path). The measurement is writes accepted by the woken primary. Four archived
  `hang-container` runs found the woken-primary rejoin divergence bug described below. It is
  fixed and the rerun is pending.
- **Replica loss and replacement** (both replicas lost, writes stall, one restored, the spare
  cloned in after `rebuild_after_s`). One run exists in `results/`, not enough for a table.
- **Disk full** (the primary's binary log volume fills up while the workload runs).
- **Every Orchestrator baseline** (kill, hang-container, partition-manager).
- **Naive partition-manager**, dropped from the campaign for time.
- **kill-two with recovery**, the rerun with the donor-of-last-resort recovery, where the
  stalled survivor itself serves as the clone donor so the set heals without an operator.

### Fleet isolation

Every injection runs against `rs1` while the manager also runs `rs2`. `rs2` changed state 54
times, all of them in the kill and partition-manager tables run under the earlier
configuration, where the live-SQL health check flapped on the untouched set too. Under the
final configuration it changed state 0 times across the other tables.

### Bugs found while building it

[docs/BUGS.md](docs/BUGS.md) has 36 entries, each with the symptom, the cause and the fix. The
three that mattered most. A primary frozen and then woken committed its clients' in-flight
transactions after the manager had already judged it a subset of the new primary, so it was
repointed while holding errant GTIDs (silent divergence, now caught by quiescing first and
checking again after the repoint). The agent answered HAProxy's health check with a live SQL
query, and one slow answer under load made HAProxy mark a healthy primary down and cut every
client. And `mysqld` was OOM-killed at its 700 MiB container limit the moment `START REPLICA` spawned
16 applier workers, which once left a new primary with no replica to acknowledge its writes.

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
and it exists to show that each of those steps changes a number. Its losses in the kill table
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
[DESIGN.md section 10](docs/DESIGN.md#10-the-baselines). The Orchestrator runs are built but
were not part of this campaign, so there are no Orchestrator numbers yet.

## Bugs found while building it

[docs/BUGS.md](docs/BUGS.md) has 36 entries, each with
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
