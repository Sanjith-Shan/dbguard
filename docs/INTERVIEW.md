# Interview defense

The eleven questions from the spec in the order an interviewer would escalate, answered the way I would answer them out loud, then the Linux round. Numbers in `[[N: ...]]` tags are not measured yet and must not be said out loud until they are.

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

It promises that every write a client got an OK for is in at least one replica's relay log, and that DBGuard promotes the replica with the largest retrieved set after it has applied its relay log, so no acknowledged write is lost across a failover. That is only claimed under `AFTER_SYNC`, one-hour timeout, `wait_for_replica_count=1`, and only as measured. In the lab it was `[[N: kill lost acked writes total, results/kill_dbguard.jsonl]]` lost acknowledged writes over `[[N: kill run count, results/kill_dbguard.jsonl]]` kills, against `[[N: kill naive lost acked writes total, results/kill_naive.jsonl]]` for the async baseline.

It does not survive two losses at once. With one required ack, the write may live on exactly one replica. If that replica dies with the primary, or dies or becomes unreachable before the manager reads its retrieved set, the write is gone, and the manager cannot even tell, because it only compares the replicas it can see. `wait_for_replica_count=2` with three replicas closes that window for a latency cost.

## 4. Detection

The manager's own probe is not enough because it tests one network path, the manager's. If I fail over on that, a partition around the manager fails over a healthy primary. A false failover costs every in-flight transaction, seconds with no writer, a rejoin or rebuild of the old primary, and a cooldown during which a real failure would not be handled.

So `dbguard/manager/detector.py` needs two things for `detect_window_s`. The manager's probe, which is a `SELECT 1` and a write, failed three times in a row, and a majority of replicas vote the primary gone, by IO thread not running, by their agent failing a TCP connect to it, or by the heartbeat row going stale. Probe without replicas is `SUSPECT` and nothing happens. I took that split from Orchestrator's `DeadMaster` versus `UnreachableMaster`.

The probe includes a write because of the partition case. A primary cut off from its replicas answers `SELECT 1` instantly and hangs every commit on semi-sync.

What each injection does to TCP.

- `kill -9` of `mysqld`. The kernel closes the process's sockets, so peers get a FIN or RST right away, and new connects are refused. Replicas notice immediately.
- `SIGSTOP`. Nothing happens on the wire. The kernel still completes handshakes into the listen backlog and ACKs data into buffers, so connects succeed and then hang. Replicas notice only when no heartbeat arrives within `replica_net_timeout`, which I set to 2 s instead of 60, with `SOURCE_HEARTBEAT_PERIOD=0.5` so a quiet primary still sends something.
- `iptables -j DROP`. Silent. Senders retransmit into nothing, no RST, no FIN. Same detection path as the hang, through the heartbeat and the timeout, plus the replicas' agents failing a TCP connect.

A hung process looks alive from outside at the TCP level and dead at the protocol level, which is why I need a protocol-level probe and a heartbeat, not a port check.

## 5. Fencing

Fencing comes before promotion because otherwise there is a moment with two writable primaries, and HAProxy can send clients to either. Writes that land on the old one exist nowhere else.

STONITH means "shoot the other node in the head". Before you let a new node take over, you make certain the old one cannot act. At the hardware level that is a power switch. In DBGuard it is the agent, which is `mysqld`'s parent process in the same container.

`POST /fence` in `dbguard/agent/core.py` writes the fence flag file first, so HAProxy's health check fails immediately even if `mysqld` never answers again. Then it sets `super_read_only=1` with a 2 s deadline and kills client threads. If the `SET` does not return, the agent sends `SIGKILL` to `mysqld` and reports `method: kill`. The `SET` really can hang. The manual says enabling `read_only` blocks while other sessions have an ongoing commit, and a primary cut off from its replicas has commits stuck waiting for acks, so in that case I expect the kill path. It happened in `[[N: fence outcome counts sql vs kill, results/partition-replicas_dbguard.jsonl]]` runs.

`super_read_only` and not `read_only` because `read_only` still lets any user with `CONNECTION_ADMIN` or `SUPER` write, which includes every admin account and DBGuard's own user. One stray admin write on a fenced primary is a transaction the new primary will never have. Replication threads keep working under `super_read_only`, so it is also right for replicas.

## 6. Promotion

The winner is the replica with the largest retrieved set, not the largest executed set, because an acknowledged write is guaranteed to be in a relay log, not applied. A replica with a slow applier can hold the only copy of the last acknowledged transaction. In `dbguard/manager/selection.py` I count the union of retrieved and executed, because the retrieved set describes the relay log, and a replica whose relay log was reset can have applied transactions its retrieved set no longer lists.

Then I wait until its executed set contains its retrieved set. If I made it writable earlier, new writes would be applied before old acknowledged ones, which reorders history and can collide on keys. Orchestrator's `DelayMasterPromotionIfSQLThreadNotUpToDate` is the same idea.

The subset check is that every other candidate's set must be contained in the winner's. With one primary feeding everyone the same binlog in order, the sets form a chain, so the check should always pass. When it does not, someone wrote to a replica or an earlier failover went wrong, and there is no safe automatic choice. Either promotion throws away transactions that exist on the loser. So the set goes to `HALTED` with both `GTID_SUBTRACT`s in the reason, and a human looks at the extra transactions with `mysqlbinlog --include-gtids`, picks the truth, and rebuilds the other node. The runbook has the steps. An early bug was the subset check passing vacuously on empty sets, which is in `docs/BUGS.md`.

## 7. Rejoin

When a primary is killed mid-commit under `AFTER_SYNC`, it can have transactions in its binlog that no replica acked, so no client got an OK. On restart, crash recovery commits them because they are in the binlog. Those are phantoms. They have to go, because if that node is ever promoted again, rows that no client was told were committed would appear, possibly clashing with rows written on the new primary.

The manual is blunt about it and says the old source "must be discarded". I only discard it when I have to. After the agent restarts `mysqld` and recovery has run, the manager checks whether the old primary's `gtid_executed` is a subset of the new primary's. If it is, there are no phantoms and it is repointed with `SOURCE_AUTO_POSITION=1` and no data copy. That is the check the Meta GTID post describes, repointing when "our automation detects its data is consistent". If it is not, `GTID_SUBTRACT(old, new)` is exactly the phantom set, logged with its count, and the node is rebuilt with `CLONE INSTANCE FROM` a healthy replica. The branch split was `[[N: rejoin repoint count vs rebuild count, results/kill_dbguard.jsonl]]`.

The clone plugin copies the donor's InnoDB data physically, removes the recipient's existing data and binary logs (which is what destroys the phantoms), transfers the donor's `gtid_executed`, and then restarts the server. That restart needs a supervising process or it ends in error 3707, and the agent is that supervisor. After it comes back, auto-positioning fetches whatever the donor had not received yet. The donor is a replica because a clone is a full sequential read plus a DDL block on the donor, and I do not want either on the node serving writes.

## 8. The cost

Semi-sync puts a replica round trip and a replica relay log fsync into every commit. The table in `docs/CAPACITY.md` has p50 and p99 with semi-sync off and on at 0, 2 and 20 ms of added delay. At 0 ms the cost was `[[N: commit p50 semisync on vs off at 0 ms netem, results/cost_dbguard.jsonl]]` at the median.

When no replica can ack, writes stall for up to an hour. In Experiment 6 the stall lasted as long as I kept the replicas down, `[[N: stall_s with no semi-sync replica, results/replica-loss_dbguard.jsonl]]`. That is correct behaviour. The alternative is accepting writes that exist on one disk while telling clients they are safe.

A team sizes a set by `R` replicas and `W` required acks. Writes stall when `R - W + 1` replicas are down, an acknowledged write can be lost when the primary and its `W` ackers go together, and commit latency is the `W`-th fastest ack. They pick `detect_window_s` above the longest stall the primary normally survives, measured from heartbeat age and probe latency on `/metrics`, and check the result against the failover budget, which with defaults is 11.5 s plus measured terms when there is no relay backlog.

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
9. `dmesg` on the host for an OOM kill of `mysqld` or I/O errors.
10. `df -h` and `df -i` on every node.

If none of that explains it, I capture the evidence and `dbgctl halt` the set before touching anything, so automation does not act on a state I do not understand.

## 11. What DBGuard does not do, and what the industry tool does

It does not do sharding or query routing, its own proxy, backups beyond clone, a MySQL patch, a web UI, multiple managers or Raft, Postgres, or Kubernetes. One manager is a deliberate choice. If it dies nothing fails over, and that is safe because doing nothing never loses data. Making the manager highly available is a consensus problem and a different project.

It also cannot survive the double loss in question 3, cannot see a write acked by a replica it cannot reach, and has only run on one laptop. One gap I know about is a primary that is cut off from the manager and its replicas but still reachable by clients for more than an hour. Semi-sync holds its commits for the hour and then it would fall back to async.

Orchestrator does much more on topology. It discovers arbitrary topologies with intermediate masters, picks a candidate by serving capacity rather than just recency, has datacenter-aware rules, graceful takeover, a UI and a raft HA mode. By default it leaves fencing to hooks and the crashed master to the operator. DBGuard fences itself down to `SIGKILL`, refuses to promote across diverged replicas, decides repoint versus rebuild by GTID subset, rebuilds with clone, and provisions replacements. On failover time Orchestrator measured `[[N: kill failover p50 orchestrator, results/kill_orchestrator.jsonl]]` median against my `[[N: kill failover p50, results/kill_dbguard.jsonl]]`, and I report it either way.

---

## The Linux round

**What is a daemon.** A long-running process with no controlling terminal that serves requests in the background. The classic way was to fork, `setsid`, fork again, `chdir /`, close or redirect stdin, stdout and stderr, and write a pidfile. Under systemd none of that is needed, and it is better not to. `dbguard` and `dbguard-agent` stay in the foreground, log JSON to stdout, and let the supervisor own the lifecycle. In the containers the supervisor is `tini` as PID 1, which reaps zombies and forwards signals.

**What a systemd unit does.** It describes how to start, stop and restart a service and what it depends on. `deploy/systemd/dbguard-agent.service` runs the agent in the foreground, restarts it on failure, sends logs to the journal and orders it after the network. The agent is the parent of `mysqld`, so stop semantics matter. `systemctl stop` sends `SIGTERM` to the main process, and the agent must turn that into a clean `mysqld` shutdown and wait for it, with `TimeoutStopSec` long enough for InnoDB to shut down before systemd escalates to `SIGKILL` for the whole cgroup. I tested the units in the Lima VM, not on the Mac.

**`mysqld` on SIGTERM.** A clean shutdown. It stops accepting connections, finishes or rolls back what it can, flushes, and shuts InnoDB down. The next start needs no crash recovery. This is what the agent sends when it stops.

**On SIGKILL.** The process ends immediately with no cleanup. The next start runs InnoDB crash recovery from the redo log, and binlog recovery commits prepared transactions that reached the binlog. That is where phantoms come from. The agent sends `SIGKILL` on the fence kill path, and `docker kill -s KILL` does it to the whole container.

**On SIGSTOP.** The process is frozen and cannot catch or ignore it. Its sockets stay open, the kernel keeps completing handshakes into the backlog and ACKing data, and nothing comes back. It looks up from a port check and dead from a protocol check. `SIGCONT` resumes it exactly where it was, which is the woken-primary problem. One practical trap is that `docker kill -s STOP` stops only the container's PID 1, which here is `tini`, so a whole-container freeze uses `docker pause` and a `mysqld`-only hang uses the agent's `/hang-mysqld`.

**What happens to its file descriptors.** When the process dies, however it dies, the kernel closes every descriptor. TCP sockets send FIN, or RST if there was unread data, and peers see the connection end. Advisory locks are released. InnoDB takes a lock on its data files so two `mysqld` cannot open the same datadir, and that lock goes away with the process, which is what lets the agent restart it at once. Data written with `write` but not `fsync`ed is still in the page cache and survives a process kill. It does not survive a host power loss, which is why `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`. On a hung process the descriptors stay open, which I would check with `ls -l /proc/<pid>/fd | wc -l` against `ulimit -n` if the symptom were "too many connections" or `EMFILE`.

**A full disk on the primary.** The manual says MySQL waits on a full disk and rechecks every minute, and this applies to binlog writes. If a binlog write, flush or sync fails, `binlog_error_action=ABORT_SERVER` shuts the server down. Either way commits stop, the heartbeat goes stale, the replicas vote, and DBGuard fails over. The lab result is `[[N: disk-full primary behaviour and failover outcome, results/disk-full_dbguard.jsonl]]`.

**A full disk on a replica.** It cannot write its relay log, so it stops acking. With two replicas the other acks and the primary is fine. With one, the primary stalls on semi-sync. I would not free space on a primary by purging binlogs without first checking every replica's retrieved set, because purging GTIDs a replica still needs breaks auto-positioning.

**`strace -p` on a hung `mysqld`.** First, the containers do not have `SYS_PTRACE`, so I would attach from the Docker VM or a debug container with `--pid=container:<node> --cap-add SYS_PTRACE`. `mysqld` is one process with many threads, so it has to be `strace -f -p <pid>`. If it was stopped by `SIGSTOP`, strace shows it stopped and no syscalls. If it is hung for real, the threads usually sit in `futex` waits (lock contention, or sessions waiting on the semi-sync ack condition), in `fsync` or `fdatasync` on a slow or full disk, in `io_getevents` for InnoDB's async I/O, or in `poll` on sockets. `strace -c` for a few seconds gives the syscall mix, and `cat /proc/<pid>/task/*/stack` or `wchan` shows where each thread is blocked in the kernel without ptrace.

**`ss -tnp`.** Lists TCP sockets with state, queues and owning process. On the primary I look for HAProxy's connections to 3306 in `ESTAB`, the replicas' binlog dump connections, and a nonzero `Send-Q` toward a replica, which means the primary is sending and the replica is not reading (a hung or partitioned replica). On a replica I look for its connection to the primary's 3306 and whether its `Recv-Q` is growing. `ss -tn state syn-recv` shows a flooded listen backlog, which is what a `SIGSTOP`ped `mysqld` accumulates as clients keep connecting.
