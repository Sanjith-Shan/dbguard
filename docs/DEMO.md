# DBGuard demo script

Two parts. The first is the three-minute screen share that `bin/demo` drives, act by act,
with what to say while each act runs. The second is the walkthrough for the question "the
site is down, what do you do", done live on the same fleet.

## Before the call

```sh
source .venv/bin/activate
make down && make up          # fresh fleet, both sets HEALTHY
dbgctl status
```

Open three windows side by side.

1. The terminal that will run `bin/demo`.
2. The HAProxy stats page at <http://localhost:18404>, which refreshes every 2 s.
3. A spare terminal for the questions afterwards.

Run `bin/demo` once before the call to warm the images. It pauses for Enter between acts,
so the pace is yours. `bin/demo --no-pause` runs it straight through.

## Part 1. The three-minute demo

### Act 1. The fleet (about 20 s)

`bin/demo` runs `dbgctl status`.

Say this. "Two MySQL 8.4 replica sets, each one primary and two replicas, on GTID and
semi-synchronous replication. It is a lab, six mysqld in Docker on this laptop. Beside each
mysqld is a small agent, and that agent is the only thing allowed to change its node's role.
HAProxy sends writes to whichever agent says it is the primary, so the proxy never decides
anything. The manager polls every mysqld and every agent and is the one that decides."

Point at the HAProxy stats page. Exactly one server is green in each backend.

### Act 2. Load (about 20 s)

Eight clients start inserting rows through HAProxy, and the count of acknowledged writes
appears.

Say this. "Each client inserts one row at a time and logs it as acknowledged only when MySQL
returned OK. That log is the ground truth. At the end a checker compares it with what is
actually in the database, so I do not have to trust the system I am testing."

### Act 3. Crash (about 40 s)

`bin/demo` sends `SIGKILL` to the primary's container and streams the manager's events as
they arrive.

Say this while it runs. "No clean shutdown. The manager's own probe fails, but that alone is
not enough, because the manager could be the one that is cut off. It also asks the replicas,
and only when a majority of them have lost the primary too does it act. Then six steps.
Fence the old primary so it can never take another write. Choose the replica that received
the most transactions, and check by GTID set arithmetic that the other one holds nothing the
winner lacks. Wait for the winner to apply its relay log. Promote it. Repoint the other
replica by GTID auto-positioning, so no binlog file or offset is ever named. Record all of it
as one event."

When the new primary line prints, point at the stats page. The green server moved.

Say this. "Semi-sync with AFTER_SYNC means the old primary never told a client OK until a
replica had the write on disk. So every acknowledged write is on a replica right now."

### Act 4. Diagnose (about 25 s)

`bin/demo` runs `dbgctl doctor rs1` while the set runs on two nodes.

Say this. "This is the first thing I would open in an incident. It says in sentences what
the manager sees, what it decided and why, and the GTID gap between the primary and every
other node, computed with GTID_SUBTRACT. The old primary shows as down, and the set is
running on the new primary and one replica, still with a semi-sync replica to acknowledge."

### Act 5. Heal (about 40 s)

`bin/demo` starts the old primary's container again and waits for the rejoin event, then
runs `dbgctl status`.

Say this. "The old primary comes back from a crash. Crash recovery may have committed
transactions that were in its binary log but were never acknowledged to anyone, because
the ack never arrived. The manager compares its gtid_executed with the new primary's. If it
is a subset, there is nothing extra, and it is repointed as a replica with no data copied. If
it is not, those extra transactions are phantoms that must not survive, so it is rebuilt
with the clone plugin from the new primary and the phantom count is logged."

Read out which branch the event shows, `rejoin=repoint` or `rejoin=rebuild` with its phantom
count.

### Act 6. Verdict (about 35 s)

The workload stops and the checker reads every node directly.

Say this. "Four properties. Every acknowledged write is on the new primary. No row exists that
was not acknowledged or in flight when its client saw an error. Exactly one node is writable.
Every node converged to the same GTID set. And the write outage as the clients saw it."

Then the one line that frames the results. "That is one run. The README has the same thing
across 30 runs per scenario, next to an asynchronous baseline that loses writes on the same
kill, and next to Orchestrator. It also publishes what this costs, the commit latency of
semi-sync and the stall when no replica is alive, and the case it does not cover, which is
losing the primary and the acknowledging replica at the same instant."

If the checker prints a nonzero count, do not explain it away. Say it is a finding and that
it goes in `docs/BUGS.md`, then show the event with `dbgctl events --rs rs1 --since 5m`.

## Part 2. "The site is down, what do you do"

Set it up with one of these in the spare terminal, or let the interviewer pick.

```sh
bin/chaos --scenario hang-process --runs 1        # mysqld frozen, agent alive
bin/chaos --scenario partition-replicas --runs 1  # primary cut off from its replicas
docker stop mysql-a2 mysql-a3                     # no replica left, writes stall
```

Narrate the order out loud. Stop at the first step that explains it. It is the same list as
entry 12 of [RUNBOOK.md](RUNBOOK.md).

### 1. Scope

```sh
dbgctl status
```

Say this. "One set or both? If both, I suspect HAProxy, the network or the clients before
MySQL. If one, I look at that set."

### 2. The manager's verdict

```sh
dbgctl doctor rs1
dbgctl events --rs rs1 --since 10m
```

Say this. "Is a failover in progress, is the set HALTED, SUSPECT or DEGRADED, and what was the
last event? Doctor also tells me whether writes are stalled for lack of a semi-sync replica,
which is a very different problem from a dead primary."

### 3. The endpoint

Open <http://localhost:18404>, or ask each agent directly.

```sh
for p in 18011 18012 18013; do curl -s -w ' %{http_code}\n' localhost:$p/primary; done
```

Say this. "Exactly one server should be up in the backend. None up means no node believes it
is the unfenced writable primary, which is normal between fence and promote and wrong in
HEALTHY. Two up is a split brain and I would halt the set immediately with dbgctl halt."

### 4. The primary itself

```sh
docker compose -f deploy/docker-compose.yml exec mysql-a1 mysql -udbguard -pdbguard -e "
  SELECT @@super_read_only, @@read_only, @@gtid_executed;
  SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_%';
  SHOW PROCESSLIST;"
```

Say this. "super_read_only at 1 on the node HAProxy points at means a fence just landed or
someone set it by hand. Semi-sync clients at 0 with wait_sessions growing means every commit
is waiting for an ack that cannot come. That is the stall, and it is deliberate. The fix is a
replica, not turning semi-sync off. Status OFF would mean it fell back to asynchronous, which
the one-hour timeout exists to prevent. The process list shows what the sessions are
waiting on."

### 5. Replication

```sh
docker compose -f deploy/docker-compose.yml exec mysql-a2 mysql -udbguard -pdbguard -e "SHOW REPLICA STATUS\G"
```

Say this. "Replica_IO_Running, Last_IO_Error, Source_Host pointing at the right node,
Retrieved_Gtid_Set against Executed_Gtid_Set for the unapplied relay log, and
Seconds_Behind_Source for lag. An IO thread in Connecting with a connect error means it cannot
reach its source, which is what the replicas report as their vote."

### 6. Agent logs

```sh
docker compose -f deploy/docker-compose.yml logs --since 10m mysql-a1 | grep -E 'fence|wake_gap|self_fence|sql='
```

Say this. "The agent logs every SQL statement it runs at a role change with its duration, and
every fence with its method, sql or kill. A wake_gap line means the container was frozen and
the agent checked who the primary is before answering HAProxy."

### 7. Connections on the primary

```sh
docker compose -f deploy/docker-compose.yml exec mysql-a1 ss -tnp
docker compose -f deploy/docker-compose.yml exec mysql-a1 ss -tn state syn-recv
```

Say this. "Who is connected to 3306. Many established connections from HAProxy with nothing
moving points at a stall inside MySQL. None at all points at the proxy or the network. A full
SYN backlog means mysqld is not accepting, which is what a frozen process looks like from
outside, since the kernel still completes the handshake for it."

### 8. The kernel

```sh
docker run --rm --privileged --pid=host alpine dmesg | tail -50
```

Say this. "dmesg on the Docker VM, since that is the kernel here. I am looking for the OOM
killer taking mysqld, filesystem errors, and dropped packets."

### 9. Disk

```sh
docker compose -f deploy/docker-compose.yml exec mysql-a1 df -h /var/lib/mysql
docker compose -f deploy/docker-compose.yml exec mysql-a1 df -i /var/lib/mysql
```

Say this. "A full disk on the primary stops commits, either by waiting or by aborting the
server if a binlog write fails. On a replica it stops that replica acknowledging. I would not
delete files MySQL owns, and I would purge binary logs only after checking every replica's
retrieved set covers them."

### 10. When nothing explains it

Say this. "I snapshot the evidence first, the doctor output, SHOW REPLICA STATUS from every
node and the last events. Then dbgctl halt rs1, so the automation does not act on a situation
I do not understand, and I fix it by hand from the runbook. No action is the one action that
cannot lose data."

Clean up afterwards with `dbgctl resume rs1` if halted, and `make down && make up` for a fresh
fleet.
