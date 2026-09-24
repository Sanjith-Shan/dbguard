# Campaign configuration history

Every table in SUMMARY.md is built from rows produced under one configuration. When the
configuration changed mid-campaign, the affected entries were rerun and the earlier rows
were moved to `results/old-config/<tag>/`. The earlier rows are kept as evidence.

## agent-live-primary-check (rows in results/old-config/agent-live-primary-check/)

- Scope. kill/dbguard x30 and partition-manager/dbguard x30 (from 10:22 to 11:43 on
  2026-09-24), plus switchover/dbguard x30 (started 11:43).
- Configuration. The agent answered HAProxy's `GET /primary` with a live SQL round trip
  to mysqld on every check. HAProxy checked every 100 ms with `fall 1`,
  `timeout check 1s` and `on-marked-down shutdown-sessions`.
- What went wrong. Under load, `/primary` sometimes took longer than 1 s. One slow check
  marked the primary DOWN and HAProxy cut every client session, then the next check marked
  it UP again. In 9 of the 30 partition-manager runs the workload saw 8 client errors (27 in
  one run), although that scenario must see none, and the errors fell anywhere from 0 to 44
  s after the injection. The same spurious cut can land inside kill and switchover runs and
  inflate their error counts and stalls.
- Change. The agent answers `/primary` from a state it refreshes every 100 ms in the
  background (503 when that state is older than 0.5 s or the node is fenced, so a hung
  mysqld still fails the check). HAProxy is unchanged (inter 100ms, fall 1). The node
  image was rebuilt and the fleet recreated on fresh volumes. See the CONFIG line in
  campaign.log for the commit.
- Rerun. The three entries were appended to the end of the queue with the archive tag. The
  first rerun run moves the old rows here.

## Queue after the node deploy (d405ec4)

- hang-container and hang-process run with `--hang-seconds 45` (the pilots used 90). A
  failover completes in about 10 s, so a 45 s hang still wakes the old primary long after
  the new one is serving, which is the window the woken-primary measurement needs. It also
  stays well under `rebuild_after_s` (120), so no spare is provisioned during a hang. Rows
  record `hang_seconds`.
- replica-loss/dbguard and disk-full/dbguard run 10 times each, not 30 (each replica-loss
  run waits out rebuild_after_s and a clone). Their tables are reported as 10 runs.
- naive runs only kill and partition-manager, the two experiments the spec asks of the
  baseline. Orchestrator runs kill, hang-container and partition-manager.
- cost is 6 cells (semi-sync on/off x netem 0/2/20 ms) x 3 runs x 60 s. kill-two is 30.

## rejoin-quiesce (rows in results/old-config/rejoin-quiesce/)

- Scope. The first hang-container/dbguard runs under the d405ec4 node image (12:09 onward).
- What went wrong. When the frozen primary woke, its clients' in-flight INSERTs (binlogged,
  waiting for a semi-sync ack no replica would send, never acknowledged) committed locally.
  The manager's rejoin decided "gtid_executed is a subset of the new primary's" from a view
  taken before those commits landed, and repointed the node instead of rebuilding it. The
  node then held GTIDs the new primary lacked (silent divergence). The checker did not see it
  because the old primary is not in its node list until it rejoins. The harness heal caught
  it ("errant") and hard-reset the set. No acknowledged write was lost.
- Change. The manager quiesces the woken node before the subset check and checks for errant
  GTIDs after a repoint (manager fix, commit named in campaign.log when deployed). Every row
  now records `errant_gtids` right after the scenario and at heal, so this cannot hide again.
- Rerun. hang-container, hang-process and orchestrator hang-container moved to the end of the
  queue, before the three agent-live-primary-check reruns.

## final fleet configuration (fe17789)

- Agent `DBGUARD_PRIMARY_STALE_S=2` (was 0.5) and HAProxy `fall 2` (was `fall 1`), with
  inter 100ms, downinter 100ms, rise 1 and on-marked-down shutdown-sessions unchanged.
- Why. Under host memory pressure (macOS swap 10 to 11 GB) the agents' event loop stalled for
  0.5 to 3 s. The /primary sample went stale, the agent answered 503, and with `fall 1`
  HAProxy marked a healthy primary DOWN and cut every client session. rs2/mysql-b1, which
  receives no faults, was marked DOWN 6 times in 13 minutes. A hung mysqld is still caught
  by the 2 s staleness bound, and the manager fences it independently. Worst-case routing
  detection of a dead primary is now about 200 ms of checks after the agent stops answering.
- Applied with `docker compose up -d` (only the nodes and HAProxy were recreated, volumes
  kept). Every entry from partition-replicas onward, and every rerun, runs under this
  configuration.

## kill-two reduced to 10 runs (11 recorded)

- Why each run took about 7 minutes. After the primary and the most advanced replica are
  killed together, the manager promotes the last survivor. That node has no semi-sync replica,
  so every commit stalls (the one-hour timeout, by design). The manager's own heartbeat write
  then blocks too, so it sits in SUSPECT, and it never rejoins the two restarted nodes: both
  hold transactions the survivor lacks, so they need a clone rebuild, and a rebuild needs a
  replica donor, which does not exist. Nothing moves until a human (here the harness heal, 120
  s, then a hard reset) intervenes. Per run: about 30 s of workload, a 240 s wait for a rejoin
  event that never comes, 120 s of heal, 20 s of hard reset. No spare was provisioned (the set
  never reached DEGRADED long enough), so replica-loss and disk-full do not share this cost.
- Result. 11 runs, 0 lost acknowledged writes. Clients saw an unbounded write stall instead
  of a failover time, so `failover_s` is null in every row. The table reports it as a stall
  that needs an operator, which is the honest boundary of `wait_for_replica_count=1`.
- orchestrator hang-container reduced to 15 runs.

## Deadline cut (14:39 on 2026-09-24)

The campaign was cut to fit a one-hour deadline. The final queue was switchover/dbguard 10
(rerun under the final configuration, the 30 old-agent rows archived), naive kill 30 and
naive partition-manager 10.

Configuration each table in SUMMARY.md ran under:

- Final configuration (node 97d68f6 with d405ec4, fleet fe17789, manager 8465b68 then
  fd6a9db): partition-replicas/dbguard 30, kill-two/dbguard 11, cost/dbguard 18,
  replica-loss/dbguard 1, switchover/dbguard 10, naive kill, naive partition-manager.
- Earlier configuration agent-live-primary-check (live SQL /primary, HAProxy fall 1, manager
  156c49e): kill/dbguard 30 and partition-manager/dbguard 30. Their planned reruns were
  dropped by the deadline, so these two tables stand as measured under that configuration.
  kill and partition-manager did not depend on the spurious-DOWN issue for correctness (0 lost,
  0 false failovers), but their client error counts and stalls include spurious HAProxy cuts
  (partition-manager: 9 of 30 runs with 8 or more client errors).

Not run in this campaign: hang-container (dbguard and orchestrator), hang-process,
replica-loss beyond 1 run, disk-full, every orchestrator entry, the kill and partition-manager
reruns, and kill-two stall-recovery. The rows in results/old-config/ are kept as evidence.
