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
