# Interview defense

The eleven questions from the spec in the order an interviewer would escalate, answered the way I would answer them out loud, then the Linux round. Numbers come from `results/SUMMARY.md`. A `[[N: ...]]` tag marks one whose rows had not landed when this was written, and it must not be said out loud until it is filled.

## 1. Draw a replica set

I draw the primary on the left with its binary log, and each replica on the right with two threads. The IO thread (the manual now calls it the receiver) is a client of the primary that pulls binlog events and writes them to the relay log. The SQL thread (the applier) reads the relay log and replays it.

A transaction goes through five places. It is prepared in InnoDB on the primary, written and fsynced to the primary's binlog, received into a replica's relay log and flushed, committed in InnoDB on the primary and returned to the client, and then applied by the replica's SQL thread some time later. Under `AFTER_SYNC` the third step has to happen before the fourth, and that ordering is the whole guarantee.

A GTID is the originating server's UUID and a sequence number. `gtid_executed` is everything a server has committed. On a replica, `Retrieved_Gtid_Set` is everything the IO thread has received and `Executed_Gtid_Set` is everything applied, so the difference is the relay log backlog. In DBGuard these come from each agent's `/status` and are compared as sets in `dbguard/gtid.py`, never as strings. Two sets can print differently and be equal, and `hypothesis` tests hold the class to the semantics of `GTID_SUBSET` and `GTID_SUBTRACT`.

## 2. Semi-sync, AFTER_SYNC versus AFTER_COMMIT

Both make the primary wait for one replica to acknowledge that the transaction is in its relay log on disk. The difference is whether InnoDB commits before or after the wait.

With `AFTER_COMMIT` the primary commits in InnoDB and then waits. During the wait other sessions can already read the row, but no replica has it. If the primary dies there, readers saw a row that the promoted replica does not have. The writer never got its OK, but other clients acted on the data, so it is a visible loss.

With `AFTER_SYNC` the wait is between the binlog fsync and the InnoDB commit. Nobody can see the row until a replica has it. If the primary dies in the wait, the writer gets a connection error and no one saw the row. From the client's side the write simply did not happen, or is indeterminate, which is allowed.

The timeout decides what happens when no ack comes. The manual default is 10 seconds and on expiry the source "reverts to asynchronous replication" without failing anything. I set it to an hour, `rpl_semi_sync_source_timeout=3600000`, because a silent fallback is a quiet data-loss mode that looks healthy on every dashboard. With an hour, a primary that nobody can ack just stops completing commits. That is loud, it pages someone, and the failover logic can act. I also keep `rpl_semi_sync_source_wait_no_replica=ON`, because OFF would fall back to async the moment the replica count drops, which undoes the hour.

## 3. What lossless promises and what it does not

It promises that every write a client got an OK for is in at least one replica's relay log, and that DBGuard promotes the replica with the largest retrieved set after it has applied its relay log, so no acknowledged write is lost across a failover. That is only claimed under `AFTER_SYNC`, one-hour timeout, `wait_for_replica_count=1`, and only as measured. In the lab it was 0 lost acknowledged writes over 30 kills. The async baseline also lost 0 in 26 kills on one host, which says the single-host lab hides the async window, not that async is safe. With a 2 ms replica delay the baseline and DBGuard runs are still being measured.

How it is measured matters as much as the claim. The workload logs a write as acknowledged only when the INSERT returned OK, and the checker looks for every one of them on the new primary afterwards, which is the Jepsen habit of checking a recorded history instead of trusting the system. Two harness rules came out of an integration review. Failover time ends at the first OK whose request started after the first error, because clients timestamp independently and a write committed just before the kill once ended a failover 3 ms after it started. And single-writer is sampled from every agent once a second during the run, and the old primary is probed directly even after it left the set, because it is the only node that can break the property.

It does not survive two losses at once. With one required ack, the write may live on exactly one replica. If that replica dies with the primary, or dies or becomes unreachable before the manager reads its retrieved set, the write is gone, and the manager cannot even tell, because it only compares the replicas it can see. `wait_for_replica_count=2` with three replicas closes that window for a latency cost.

## 4. Detection

The manager's own probe is not enough because it tests one network path, the manager's. If I fail over on that, a partition around the manager fails over a healthy primary. A false failover costs every in-flight transaction, seconds with no writer, a rejoin or rebuild of the old primary, and a cooldown during which a real failure would not be handled.

So `dbguard/manager/detector.py` needs two things for `detect_window_s`. The manager's probe, which is a `SELECT 1` and a write, failed three times in a row. And a strict majority of the witnesses, and at least one, vote the primary gone. A witness is a replica I can reach whose `Source_Host` is the primary I believe in. It votes if its IO thread is not running, if its agent cannot TCP connect to the primary, or if the heartbeat row is stale. Probe without votes is `SUSPECT` and nothing happens. I took that split from Orchestrator's `DeadMaster` versus `UnreachableMaster`.

The probe includes a write because of the partition case. A primary cut off from its replicas answers `SELECT 1` instantly and hangs every commit on semi-sync.

Three of the vote rules came from bugs, and they are the stories I would tell. The fenced old primary once voted the new primary dead through its own old heartbeat row, so only replicas of the believed primary vote. On the real fleet two replicas 94 and 98 s behind voted a healthy primary dead through the heartbeat, because the row is read after it is applied, so heartbeat age now counts only after subtracting `Seconds_Behind_Source`. And after a whole-fleet restart, a heartbeat row 32545 s old voted against a primary that had just booted read-only, so an age over ten windows does not vote, and a fleet where every node booted read-only gets a cold start that promotes the replicas' common source in place.

What each injection does to TCP.

- `kill -9` of `mysqld`. The kernel closes the process's sockets, so peers get a FIN or RST right away, and new connects are refused. Replicas notice immediately.
- `SIGSTOP`. Nothing happens on the wire. The kernel still completes handshakes into the listen backlog and ACKs data into buffers, so connects succeed and then hang. Replicas notice only when no heartbeat arrives within `replica_net_timeout`, which I set to 2 s instead of 60, with `SOURCE_HEARTBEAT_PERIOD=0.5` so a quiet primary still sends something.
- `iptables -j DROP`. Silent. Senders retransmit into nothing, no RST, no FIN. Same detection path as the hang, through the heartbeat and the timeout, plus the replicas' agents failing a TCP connect.

A hung process looks alive from outside at the TCP level and dead at the protocol level, which is why I need a protocol-level probe and a heartbeat, not a port check.

## 5. Fencing

Fencing comes before promotion because otherwise there is a moment with two writable primaries, and HAProxy can send clients to either. Writes that land on the old one exist nowhere else.

STONITH means "shoot the other node in the head". Before you let a new node take over, you make certain the old one cannot act. At the hardware level that is a power switch. In DBGuard it is the agent, which is `mysqld`'s parent process in the same container.

`POST /fence` in `dbguard/agent/core.py` writes the fence flag file first, so HAProxy's health check fails immediately even if `mysqld` never answers again. Then it sets `super_read_only=1` with a 2 s deadline and kills client threads. If the `SET` does not return, the agent sends `SIGKILL` to `mysqld` and reports `method: kill`. The `SET` really does hang. I reproduced it. It sat in "Waiting for global read lock" behind a heartbeat `INSERT` waiting for a semi-sync ack, and `KILL` on the waiting session did not release it. The only SQL that would is turning semi-sync off, which acknowledges writes no replica has. So a partitioned primary is always fenced by the kill path, and that is why the agent owns the process. It happened in 30 of 30 by kill, 0 by sql runs.

`super_read_only` and not `read_only` because `read_only` still lets any user with `CONNECTION_ADMIN` or `SUPER` write, which includes every admin account and DBGuard's own user. One stray admin write on a fenced primary is a transaction the new primary will never have. Replication threads keep working under `super_read_only`, so it is also right for replicas.

After a kill fence the agent holds `mysqld` down for 20 s instead of restarting it at once. A restarted old primary has committed its unacknowledged binlog tail in crash recovery, and replicas that still point at it reconnect every second and would pull those phantoms. `/promote`, `/repoint` or `/rebuild` end the hold early.

Two more agent rules cover what the manager cannot. The wake guard asks the manager who is primary after the agent's loop sees a gap over 3 s, and fences if the answer is someone else or `null` because a failover is running. If the manager answers `unknown`, it has just restarted and is still discovering, and the agent leaves state alone. An earlier version fenced a healthy primary in that window. The self-fence lease fences a primary whose agent has not reached the manager for 10 s while `Rpl_semi_sync_source_clients` is 0. Without it a primary cut off from everything but its clients would fall back to async after the one-hour timeout. The manager-partition experiment does not trigger it, because there the replicas are fine and the client count stays at 2.

## 6. Promotion

First I stop every candidate's IO thread over SQL, so nothing arrives while I compare them, including the phantom tail of an old primary that came back. Then the winner is the candidate holding the most transactions by `gtid_executed` union `Retrieved_Gtid_Set`. The retrieved part matters because an acknowledged write is guaranteed to be in a relay log, not applied, so a replica with a slow applier can hold the only copy of the last one. The executed part matters because the retrieved set is not cumulative. `CHANGE REPLICATION SOURCE TO` and relay log recovery purge it, and I found in the fake fleet that "largest retrieved set" picks the wrong winner after a repoint.

Then I wait until its retrieved set is contained in what it applied, or, with its IO thread stopped, until the SQL thread says it has read all of its relay log. The second exit exists because a source that died mid-send can leave a partial transaction whose GTID is in the retrieved set and will never be applied. If I made the winner writable earlier, new writes would land before old acknowledged ones. Orchestrator's `DelayMasterPromotionIfSQLThreadNotUpToDate` is the same idea.

The subset check is that every other candidate's set must be contained in the winner's. With one primary feeding everyone the same binlog in order, the sets form a chain, so the check should always pass. When it does not, someone wrote to a replica or an earlier failover went wrong, and there is no safe automatic choice. Either promotion throws away transactions that exist on the loser. So the set goes to `HALTED` with both `GTID_SUBTRACT`s in the reason, and a human looks at the extra transactions with `mysqlbinlog --include-gtids`, picks the truth, and rebuilds the other node. The runbook has the steps. An early bug was the subset check passing vacuously on empty sets, which is in `docs/BUGS.md`.

The order is not the spec's. The fence starts first and runs concurrently with stopping IO threads, choosing, catching up and repointing, because none of those can acknowledge a write, and only the promote waits for the fence. The other replicas are repointed to the winner while it is still read-only, before promotion. When I promoted first, the new primary had semi-sync on and no replica attached, so every commit waited for the first repoint, and `START REPLICA` took up to 2.4 s. Repointing first cut a planned switchover's client stall from 3.8 s to 0.89 s. The agent bounds a whole promote or repoint to 30 s and the manager waits 35 s, so the manager never gives up on a promote the agent is still finishing.

## 7. Rejoin

When a primary is killed mid-commit under `AFTER_SYNC`, it can have transactions in its binlog that no replica acked, so no client got an OK. On restart, crash recovery commits them because they are in the binlog. Those are phantoms. They have to go, because if that node is ever promoted again, rows that no client was told were committed would appear, possibly clashing with rows written on the new primary. The restart hold and the stopped IO threads keep them from leaking to replicas in the meantime.

The manual is blunt about it and says the old source "must be discarded". I only discard it when I have to. After the agent restarts `mysqld` and recovery has run, the manager checks whether the old primary's `gtid_executed` is a subset of the new primary's. If it is, there are no phantoms and it is repointed with `SOURCE_AUTO_POSITION=1` and no data copy. That is the check the Meta GTID post describes, repointing when "our automation detects its data is consistent". If it is not, `GTID_SUBTRACT(old, new)` is exactly the phantom set, logged with its count, and the node is rebuilt with `CLONE INSTANCE FROM` a healthy replica. The branch split was 5 repoint, 25 rebuild.

The clone plugin copies the donor's InnoDB data physically, removes the recipient's existing data and binary logs (which is what destroys the phantoms), transfers the donor's `gtid_executed`, and then restarts the server. That restart needs a supervising process or it ends in error 3707, and the agent is that supervisor. Two things bit me. CLONE refuses a `super_read_only` recipient, and every node boots that way, so the agent clears it just before the clone under a `rebuilding` flag that keeps `/primary` at 503. And a clone from a primary stalled on semi-sync never finished. The donor is a replica anyway, because a clone is a full sequential read plus a DDL block on the donor, and I do not want either on the node serving writes.

## 8. The cost

Semi-sync puts a replica round trip and a replica relay log fsync into every commit. The table in `docs/CAPACITY.md` has p50 and p99 with semi-sync off and on at 0, 2 and 20 ms of added delay. At 0 ms the cost was +1.6 ms (5.9 ms on vs 4.2 ms off) at the median.

When no replica can ack, writes stall for up to an hour. In the one replica-loss run so far the stall lasted as long as I kept the replicas down, 22.44 s. That is correct behaviour. The alternative is accepting writes that exist on one disk while telling clients they are safe.

The hidden cost is apply. Semi-sync bounds receipt, not apply, and failover waits for apply. With the default 4 applier workers my replicas fell 44 s behind in five minutes of 8-client load, past the 30 s catch-up deadline. With 16 workers the lag went away but `mysqld` was OOM-killed at a 700 MiB container limit, at `START REPLICA`, which is exactly when a failover repoints. The fleet runs 8 workers in 1 GiB. So a replica needs enough parallelism to keep lag flat at peak, and the memory to start that many workers during a failover.

A team sizes a set by `R` replicas and `W` required acks. Writes stall when `R - W + 1` replicas are down, an acknowledged write can be lost when the primary and its `W` ackers go together, and commit latency is the `W`-th fastest ack. They pick `detect_window_s` above the longest stall the primary normally survives, measured from heartbeat age and probe latency on `/metrics`, and check the result against the failover budget, which with defaults is 11 s plus the promote and the client reconnect when there is no relay backlog.

## 9. Linux

This one has its own section below.

## 10. Debugging live, "writes are failing"

I would go in this order and stop at the first thing that explains it.

1. `dbgctl status`. One set or both. Both means HAProxy, network or clients.
2. `dbgctl doctor rs1`. Is a failover running, is the set `HALTED` or `SUSPECT`, what is the last event.
3. Are writes erroring or hanging. Errors point at routing or read-only, hangs at semi-sync or locks.
4. HAProxy stats page. Exactly one server up in the backend. None up means no agent says primary. Two up is a bug and I halt the set immediately.
5. On the primary, `SELECT @@super_read_only`. Then `SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_%'`. Zero clients and growing wait sessions is the no-ack stall. Status OFF means it fell back to async, which should be impossible with my timeout.
6. `SHOW REPLICA STATUS\G` on each replica. IO and SQL thread state, `Last_IO_Error`, `Source_Host`, retrieved versus executed.
7. The agent logs for `fence`, `wake_gap` and slow `sql=` lines.
8. `ss -tnp` inside the primary's container to see who holds connections to 3306, and whether HAProxy's connections are there at all.
9. `dmesg` on the host for an OOM kill of `mysqld` or I/O errors. This one is real. `mysqld` died with SIGKILL from the kernel during `START REPLICA` when replicas sat at their memory limit, and the agent log showed `mysqld_exited` with returncode -9.
10. `df -h` and `df -i` on every node.

If none of that explains it, I capture the evidence and `dbgctl halt` the set before touching anything, so automation does not act on a state I do not understand.

## 11. What DBGuard does not do, and what the industry tool does

It does not do sharding or query routing, its own proxy, backups beyond clone, a MySQL patch, a web UI, multiple managers or Raft, Postgres, or Kubernetes. One manager is a deliberate choice. If it dies nothing fails over, and that is safe because doing nothing never loses data. Making the manager highly available is a consensus problem and a different project.

It also cannot survive the double loss in question 3, cannot see a write acked by a replica it cannot reach, and has only run on one laptop. The gap I used to name here, a primary cut off from the manager and its replicas but still reachable by clients for over an hour, is now covered by the self-fence lease. What is left is a primary cut off from the manager that keeps one semi-sync replica. The lease leaves it alone on purpose, and that is correct, since its writes are still protected.

Orchestrator does much more on topology. It discovers arbitrary topologies with intermediate masters, picks a candidate by serving capacity rather than just recency, has datacenter-aware rules, graceful takeover, a UI and a raft HA mode. By default it leaves fencing to hooks and the crashed master to the operator. DBGuard fences itself down to `SIGKILL`, refuses to promote across diverged replicas, decides repoint versus rebuild by GTID subset, rebuilds with clone, and provisions replacements. On failover time I have not yet measured Orchestrator against my 8.83 s, and when I do I will report it either way.

---

## The Linux round

**What is a daemon.** A long-running process with no controlling terminal that serves requests in the background. The classic way was to fork, `setsid`, fork again, `chdir /`, close or redirect stdin, stdout and stderr, and write a pidfile. Under systemd none of that is needed, and it is better not to. `dbguard` and `dbguard-agent` stay in the foreground, log JSON to stdout, and let the supervisor own the lifecycle. In the containers the supervisor is `tini` as PID 1, which reaps zombies and forwards signals.

**What a systemd unit does.** It describes how to start, stop and restart a service and what it depends on. `deploy/systemd/dbguard-agent.service` runs the agent in the foreground, restarts it on failure, sends logs to the journal and orders it after the network. The agent is the parent of `mysqld`, so stop semantics matter. `systemctl stop` sends `SIGTERM` to the main process, and the agent must turn that into a clean `mysqld` shutdown and wait for it, with `TimeoutStopSec` long enough for InnoDB to shut down before systemd escalates to `SIGKILL` for the whole cgroup. I tested the units in a privileged systemd container (`deploy/systemd/README.md`), not on the Mac. The agent unit's `TimeoutStopSec` is 150 s.

**`mysqld` on SIGTERM.** A clean shutdown. It stops accepting connections, finishes or rolls back what it can, flushes, and shuts InnoDB down. The next start needs no crash recovery. This is what the agent sends when it stops.

**On SIGKILL.** The process ends immediately with no cleanup. The next start runs InnoDB crash recovery from the redo log, and binlog recovery commits prepared transactions that reached the binlog. That is where phantoms come from. The agent sends `SIGKILL` on the fence kill path, and `docker kill -s KILL` does it to the whole container.

**On SIGSTOP.** The process is frozen and cannot catch or ignore it. Its sockets stay open, the kernel keeps completing handshakes into the backlog and ACKing data, and nothing comes back. It looks up from a port check and dead from a protocol check. `SIGCONT` resumes it exactly where it was, which is the woken-primary problem. One practical trap is that `docker kill -s STOP` stops only the container's PID 1, which here is `tini`, so a whole-container freeze uses `docker pause` and a `mysqld`-only hang uses the agent's `/hang-mysqld`.

**What happens to its file descriptors.** When the process dies, however it dies, the kernel closes every descriptor. TCP sockets send FIN, or RST if there was unread data, and peers see the connection end. Advisory locks are released. InnoDB takes a lock on its data files so two `mysqld` cannot open the same datadir, and that lock goes away with the process, which is what lets the agent restart it at once. Data written with `write` but not `fsync`ed is still in the page cache and survives a process kill. It does not survive a host power loss, which is why `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`. On a hung process the descriptors stay open, which I would check with `ls -l /proc/<pid>/fd | wc -l` against `ulimit -n` if the symptom were "too many connections" or `EMFILE`.

**A full disk on the primary.** The manual says MySQL waits on a full disk and rechecks every minute, and this applies to binlog writes. If a binlog write, flush or sync fails, `binlog_error_action=ABORT_SERVER` shuts the server down. Either way commits stop, the heartbeat goes stale, the replicas vote, and DBGuard fails over. Which of the two the lab shows is not yet measured. To test it without filling the Docker VM disk every container shares, the disk-full scenario moves one node's binlog onto a 256 MB tmpfs through an overlay. It is opt-in and off by default, because a tmpfs is wiped when the container stops, and a killed primary would come back without its binlog, so crash recovery would roll back the prepared transactions the kill experiments are measuring.

**A full disk on a replica.** It cannot write its relay log, so it stops acking. With two replicas the other acks and the primary is fine. With one, the primary stalls on semi-sync. I would not free space on a primary by purging binlogs without first checking every replica's retrieved set, because purging GTIDs a replica still needs breaks auto-positioning.

**The OOM killer.** A container's memory limit is a cgroup limit, and when it is hit the kernel kills a process in that cgroup, which is `mysqld`, with SIGKILL. From outside it looks exactly like `kill -9`, so crash recovery, phantoms and the rejoin logic all apply. `docker inspect -f '{{.State.OOMKilled}}'` and `dmesg` tell it apart from a fence. It hit me at `START REPLICA`, because that is when the applier workers allocate.

**`strace -p` on a hung `mysqld`.** First, the containers do not have `SYS_PTRACE`, so I would attach from the Docker VM or a debug container with `--pid=container:<node> --cap-add SYS_PTRACE`. `mysqld` is one process with many threads, so it has to be `strace -f -p <pid>`. If it was stopped by `SIGSTOP`, strace shows it stopped and no syscalls. If it is hung for real, the threads usually sit in `futex` waits (lock contention, or sessions waiting on the semi-sync ack condition), in `fsync` or `fdatasync` on a slow or full disk, in `io_getevents` for InnoDB's async I/O, or in `poll` on sockets. `strace -c` for a few seconds gives the syscall mix, and `cat /proc/<pid>/task/*/stack` or `wchan` shows where each thread is blocked in the kernel without ptrace.

**`ss -tnp`.** Lists TCP sockets with state, queues and owning process. On the primary I look for HAProxy's connections to 3306 in `ESTAB`, the replicas' binlog dump connections, and a nonzero `Send-Q` toward a replica, which means the primary is sending and the replica is not reading (a hung or partitioned replica). On a replica I look for its connection to the primary's 3306 and whether its `Recv-Q` is growing. `ss -tn state syn-recv` shows a flooded listen backlog, which is what a `SIGSTOP`ped `mysqld` accumulates as clients keep connecting.
