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
