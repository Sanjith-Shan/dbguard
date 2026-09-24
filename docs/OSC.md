The numbers on this page are preliminary. They come from a single throwaway container while a chaos campaign shared the CPU.

# Online schema change (`dbgctl osc`)

`dbgctl osc` changes the schema of a live table without blocking writes for the length of the copy. It follows the shadow table recipe of Facebook's OnlineSchemaChange (OSC) and of Percona's pt-online-schema-change. It builds an altered empty copy of the table, keeps it current with triggers, copies the existing rows in small primary key chunks, checksums the two tables chunk by chunk, and swaps them with a single `RENAME TABLE`. When MySQL 8.4 can do the change as `ALGORITHM=INSTANT`, the tool does that instead and says so, because a metadata-only change beats any copy.

This is the M7 stretch item of the spec. The numbers at the end come from one throwaway `mysql:8.4` container on the lab laptop, not from the compose fleet, and they are stated with their settings.

## Usage

```
dbgctl osc run --host 127.0.0.1 --port 3306 --user dbguard --password dbguard \
    --db chaos --table writes --alter "ADD COLUMN note VARCHAR(32) NULL" \
    [--chunk-size 1000] [--chunk-time 0.1] [--max-lag-s 2] [--max-load-threads 20] \
    [--manager http://127.0.0.1:19090 --rs rs1] [--dry-run] [--no-instant] \
    [--drop-old] [--allow-type-change] [--json]

dbgctl osc status  --host ... [--db chaos]         # progress from dbguard.osc_progress
dbgctl osc cleanup --host ... --db chaos --table writes [--old]
dbgctl osc bench   --host ... [--rows 500000] [--threads 8] [--repeat 3]
```

`--host` must be the primary. Every DBGuard replica runs with `super_read_only=1`, and preflight refuses a read-only server and points at `dbgctl status` to find the primary. Connections use the fleet's TLS decision from `dbguard/mysqlx_sync.py` (TLS without certificate verification, see docs/INTERFACES.md). `--alter` takes the clauses only, the text that would follow `ALTER TABLE t`. The progress lines and log go to stderr, and `--json` prints the result object on stdout. Exit codes are 0 for success, 1 for a refused or failed change (cleaned up), and 130 for Ctrl-C or SIGTERM (cleaned up).

## What a run does

### 1. Preflight

Preflight reads `information_schema` and refuses, before it changes anything, when any of these holds.

| Check | Why |
|---|---|
| server has `super_read_only` or `read_only` set | the change must run on the primary so replicas receive it through replication |
| no PRIMARY KEY | chunks, triggers and the checksum are all keyed by it |
| the ALTER drops, adds or modifies the PRIMARY KEY or one of its columns | the copy walks a key that must mean the same thing in both tables |
| the table already has triggers | the tool adds its own three and does not merge with existing ones |
| a leftover `__osc_new_<t>`, `__osc_old_<t>` or `__osc_*` trigger exists | a previous run failed, `dbgctl osc cleanup` removes them |
| the table has or is referenced by foreign keys | after the rename, child tables would point at the old table |
| the ALTER renames a column or the table | the column mapping of the copy would not follow the rename |
| the engine is not InnoDB, or the table name is too long for the derived names | |

It also warns, without refusing, about two things. A UNIQUE index added by the ALTER can make `INSERT IGNORE` drop duplicate rows silently, and the checksum's row counts then abort the change before the swap. And free disk space cannot be read over SQL, so it prints the table's data plus index size from `information_schema` and the `datadir`, and leaves the `df` to the operator. The copy needs about that much again, plus the binlog of every copied row.

### 2. INSTANT when it applies

MySQL 8.4 can add a column by changing only the data dictionary (`ALGORITHM=INSTANT`), with no rows rewritten. The manual's limits are that INSTANT cannot be combined in one statement with an operation that does not support it, is not available for tables with a FULLTEXT index or `ROW_FORMAT=COMPRESSED`, and a table can take at most 64 instant row versions before it needs a rebuild (`TOTAL_ROW_VERSIONS` in `information_schema.INNODB_TABLES`). Since 8.0.29 INSTANT also covers `AFTER`/`FIRST` and `DROP COLUMN`.

The tool uses a conservative rule. When every clause is a plain `ADD COLUMN` at the end of the table and none of the limits above applies, it runs `ALTER TABLE t <clauses>, ALGORITHM=INSTANT` and stops. If MySQL still refuses (errors 1845, 1846 or 4092), it logs the refusal and falls back to the copy. `--no-instant` forces the copy, which the bench uses to compare the two. Anything else in the ALTER, an index for example, goes through the copy.

### 3. Shadow table

`CREATE TABLE __osc_new_<t> LIKE <t>` and then the ALTER on the empty copy, where it is instant because there are no rows. The tool then compares the two tables and refuses when the primary key changed, when a new column is `NOT NULL` without a `DEFAULT` (the trigger's `REPLACE` would fail the application's own write in strict mode), or when the type of an existing column changed and `--allow-type-change` was not given (a value that does not fit would fail the application's write in the trigger). Columns present in both tables and not generated are the copy columns. Columns whose type did not change are the checksum columns.

### 4. Triggers

Three AFTER triggers on the source mirror every change into the shadow by primary key, in the pt-osc shape.

```sql
AFTER INSERT  REPLACE INTO shadow (cols) VALUES (NEW.cols)
AFTER UPDATE  DELETE IGNORE FROM shadow WHERE NOT (OLD.pk <=> NEW.pk) AND shadow.pk <=> OLD.pk;
              REPLACE INTO shadow (cols) VALUES (NEW.cols)
AFTER DELETE  DELETE IGNORE FROM shadow WHERE shadow.pk <=> OLD.pk
```

The UPDATE trigger handles a primary key change as a delete of the old key and a write of the new row. `REPLACE` makes the triggers idempotent against rows the copy has already written or has not reached yet.

**Why triggers write the shadow directly, and not a change log.** OSC's triggers write each change into a change log table (`__osc_chg_<t>`, with an auto-increment id and the DML type), and a separate loop replays the log into the shadow until it is nearly caught up, then locks the table for a last replay before the cut-over. That design keeps the trigger's work small and constant (one append), lets the copy use `SELECT ... INTO OUTFILE` and `LOAD DATA` without fighting the triggers for the same rows, and lets OSC run with `sql_log_bin=0` on each server separately. It costs a replay loop with its own correctness problems (ordering, catching up under load, the final locked replay). The direct approach here has no replay loop, and the shadow is current the moment the copy ends, so the swap needs no lock and no final catch-up. The price is paid by the application. Each trigger runs inside the writer's own transaction, so every INSERT, UPDATE and DELETE on the table does a second index write into the shadow and holds its locks until commit. That is extra latency on every foreground write for the whole length of the change, and it is visible in the bench below. A trigger failure fails the application's statement, which is why preflight refuses the ALTERs that could make the `REPLACE` fail.

gh-ost avoids triggers altogether. It reads the primary's binlog as a fake replica and applies the row events to the shadow from outside, so the foreground pays nothing per write, at the price of depending on ROW binlog and a more delicate cut-over. That is the design to reach for when trigger overhead is the problem.

### 5. Chunked copy

The copy walks the primary key in half-open ranges `(lo, hi]`. The maximum key is read once, after the triggers exist, because every row inserted after that point is written to the shadow by the INSERT trigger. For each chunk the next boundary is the key of the n-th row after `lo` (`ORDER BY pk LIMIT 1 OFFSET n-1`, capped at the maximum), and the rows are copied with

```sql
INSERT IGNORE INTO shadow (cols) SELECT cols FROM t FORCE INDEX (PRIMARY)
WHERE <lo < pk <= hi> FOR SHARE
```

in autocommit, one transaction per chunk. `IGNORE` skips rows the triggers already wrote, which are newer. `FOR SHARE` matters. Without it the SELECT is a consistent read, and a row that a writer deletes after the read and before the insert would be deleted from the shadow by the trigger first and then resurrected by the copy. With shared locks on the chunk's rows the writer's DELETE or UPDATE waits until the chunk commits, and its trigger then runs against the copied row. pt-osc does the same with `LOCK IN SHARE MODE`. Any warning other than 1062 (duplicate key) after a chunk aborts the change, because it means a value was altered on the way into the shadow.

Composite keys such as `chaos.writes (client_id, seq, run_id)` compare in tuple order. The first version wrote that as a row constructor, `(a, b, c) > (x, y, z)`, which is the obvious SQL and is wrong for performance. MySQL 8.4 does not turn a row constructor inequality into a range (`EXPLAIN` showed `type: index`), so every chunk scanned the primary key from the beginning and the copy was quadratic. On the 500k-row bench table it had collapsed to 10-row chunks at about 60 rows per second before it was stopped. The predicate is now written out column by column the way pt-osc does it, `a >= x AND (a > x OR (a = x AND b > y) OR (a = x AND b = y AND c > z))`, where the leading `a >= x` bounds the scan, and `EXPLAIN` shows `type: range`. A unit test checks the expanded predicate against Python tuple comparison for keys of one, two and three columns.

The chunk size starts at `--chunk-size` and adapts after each chunk toward `--chunk-time` (100 ms by default), changing by at most a factor of two per chunk so one slow chunk does not collapse it. Before every chunk the throttle pauses while `Threads_running` is above `--max-load-threads`, or while the worst replica `lag_s` in the manager's `GET /v1/sets/{rs}` is above `--max-lag-s`. Lag is only checked when `--manager` and `--rs` are given, and the tool says so when they are not. When the manager is unreachable or a replica reports no lag, the copy pauses too (pt-osc does the same), and Ctrl-C aborts cleanly if that lasts. A progress line is printed every 2 s and the same numbers go to one row of `dbguard.osc_progress`, which `dbgctl osc status` reads and flags as stale when it has not moved for 10 s.

### 6. Checksum

After the copy, each copy range plus one open range past the maximum key is checksummed on both tables inside one transaction.

```sql
SELECT COUNT(*), BIT_XOR(CRC32(CONCAT_WS('#', c1, c2, ..., CONCAT(ISNULL(c1), ISNULL(c2), ...))))
FROM t WHERE <range> FOR SHARE
```

The NULL bitmap is there because `CONCAT_WS` skips NULLs, which would make NULL and an empty string hash the same. `BIT_XOR` makes the value independent of row order. The locking reads block writers of that range for the moment of the two queries, so both tables are compared at the same logical point. Any mismatch in count or checksum aborts the change and cleans up before the swap. Columns whose type the ALTER changed are covered by the row counts only, and the result lists which columns were compared.

### 7. Atomic swap

```sql
RENAME TABLE t TO __osc_old_t, __osc_new_t TO t
```

One `RENAME TABLE` statement with several pairs is atomic in MySQL 8.4 (atomic DDL, all pairs or none), and it takes exclusive metadata locks on every table before renaming, so no statement sees a moment with no `t`. There is no `LOCK TABLES` and no final replay, because the triggers have kept the shadow current. The rename waits for transactions that have already touched `t`, and new statements on `t` queue behind it while it waits, so the tool sets `lock_wait_timeout` to 2 s and retries up to 10 times instead of letting one long transaction stall the application. OSC cannot do this step in one statement in its design and uses two renames with a short window in between, which its wiki describes.

The triggers belong to the table object and move with it to `__osc_old_t`. Their bodies still name `__osc_new_t`, but nothing writes to the old table any more. The tool drops them right after the swap and then verifies that no `__osc_*` trigger is left on either table. The old table is kept by default and the tool prints the `DROP TABLE` to run once the change has proven itself. `--drop-old` drops it at once.

### 8. Failure and Ctrl-C

Any exception, Ctrl-C or SIGTERM before the swap triggers the cleanup. It opens a fresh connection, because the run's own connection may be stuck in the middle of a chunk, kills the old connection's thread, drops the three triggers and only then drops the shadow. The order matters. Dropping the shadow while the triggers still pointed at it would make every application write fail with "table doesn't exist". After the swap the shadow is the live table and the cleanup never drops it, it only finishes dropping triggers. The progress row is marked `failed` with the error through the cleanup connection. If the cleanup itself fails, or the process is killed with SIGKILL, the next run's preflight finds the leftovers and refuses, and `dbgctl osc cleanup` removes them (triggers first, again) and can be run any number of times. A dry run (`--dry-run`) runs preflight, creates the empty shadow, applies the ALTER to it to prove it parses and to show the resulting columns, and drops it, with no triggers, copy or swap.

### 9. Replication and GTID

Everything runs on the primary as ordinary binlogged statements, unlike OSC, which runs on each server separately with `sql_log_bin=0`. The fleet uses GTID, ROW binlog and `log_replica_updates`, so this is what the replicas see.

- `CREATE TABLE ... LIKE`, the ALTER of the empty shadow, `CREATE TRIGGER`, `RENAME TABLE`, `DROP TRIGGER` and `DROP TABLE` are statement-logged DDL, each with its own GTID, and the replicas run them.
- Each copy chunk is an `INSERT ... SELECT`, which in ROW format is logged as the row images inserted into the shadow. The replica does not rerun the SELECT.
- A foreground write on the primary fires the trigger there, and the ROW event group of that transaction contains the row changes of both tables. On the replica the triggers exist (the DDL replicated) but are not fired by row events, which is the documented ROW behaviour and exactly right, because the row changes to the shadow are already in the event group. Nothing is applied twice.
- The swap replicates as one `RENAME TABLE`, so a replica's view switches at the same point in the transaction order as the primary's.

So replicas apply the same change in small transactions, and replication lag stays bounded by the chunk size, which the lag throttle watches. That is the main argument for this whole tool over MySQL's own online DDL. `ALTER TABLE ... ALGORITHM=INPLACE, LOCK=NONE` is online on the primary, but the replica applies the ALTER as one statement that takes as long as it took on the primary, and every transaction after it waits behind it, so the replica lags by the full ALTER time. The bench below runs on a single container and does not measure that replica lag, it only states it.

One consequence for DBGuard itself. A failover during a run leaves the triggers and the shadow on the new primary (they replicated), with the copy stopped part way, and the tool's connection to the old primary broken. The run's cleanup then fails to reconnect to the old primary, and `dbgctl osc cleanup` against the new primary removes the leftovers. The tool does not follow a failover and resume. That was not tested on the fleet (the chaos campaign was running on it during this work), and it is listed as unverified below.

## Measurements

These numbers are preliminary. They come from one throwaway container, and the run that was meant to repeat them three times did not finish. Treat them as a first look at the shape, not as results. They are not in README.md.

### Setup

- One `mysql:8.4` container (8.4.11) started with `--cpus 1 --memory 700m`, `--gtid-mode=ON --enforce-gtid-consistency=ON --binlog-format=ROW --log-bin`, `--innodb-buffer-pool-size=256M`, and the image defaults otherwise (`sync_binlog=1`, `innodb_flush_log_at_trx_commit=1`). There were no replicas, so replica lag was not measured and the lag throttle was not exercised.
- The host was the lab laptop (Apple M3 Pro, Docker Desktop) while the DBGuard chaos campaign was running on the same machine. The container's single CPU was shared with that load, which is the main reason the baselines below vary from 628 to 1801 writes per second between runs of the same workload.
- Table `osc_bench.writes` in the `chaos.writes` shape with a composite primary key `(client_id, seq, run_id)`, 500,000 seed rows of 256 random payload bytes, about 157 MiB of data plus index by `information_schema`. It was rebuilt from a seed copy before each mode.
- Foreground writer `dbgctl osc bench` with 8 threads, each doing one autocommit statement at a time, half single-row INSERTs of new rows and half single-row UPDATEs of random seed rows. It ran 15 s before the change, during it, and 15 s after. The writer ran on the host, outside the container.
- ALTER for the copy and INPLACE modes was `ADD COLUMN note VARCHAR(32) NULL, ADD INDEX idx_ts (ts)`. For the INSTANT modes it was `ADD COLUMN note VARCHAR(32) NULL`. The osc runs used the defaults (100 ms chunk target, `--max-load-threads 20`, which 8 writers never reach).

### Results, one run per mode

| Mode | Change took | QPS before | QPS during | QPS drop | p99 before | p99 during | Worst write during |
|---|---|---|---|---|---|---|---|
| `dbgctl osc run` (copy) | 28.4 s | 855 | 548 | 36% | 57 ms | 82 ms | 1467 ms |
| `ALTER ... ALGORITHM=INPLACE, LOCK=NONE` | 7.9 s | 1801 | 682 | 62% | 21 ms | 57 ms | 137 ms |
| `ALTER ... ALGORITHM=INSTANT` | 0.11 s | 949 | window too short | not meaningful | 37 ms | 73 ms | 75 ms |
| `dbgctl osc run` picking INSTANT itself | 0.13 s | 1002 | window too short | not meaningful | 45 ms | 23 ms | 23 ms |

The osc copy moved 504,755 rows (the seed rows plus what the writer inserted during the copy) at about 34,000 rows per second, in adaptive chunks that settled between about 1,400 and 7,100 rows. The checksum compared 131 chunks with no mismatch and the swap took 67 ms. Copy throughput in MB/s was not recorded for this run. Using the 157 MiB `information_schema` estimate and the 28.4 s total gives at most about 5.5 MB/s, and it should be read as that rough bound. In every mode the writer lost no acknowledged insert (checked by counting the writer's rows in the final table).

The first repetition of the three-repeat run measured the osc copy again, with 20 s phases, and got 628 QPS before, 336 during (a 47% drop), p99 96 ms before and 167 ms during, a worst write of 928 ms, a change time of 50.6 s, and no lost inserts. The machine was busier at that point. That run then tried a throttled osc with `--max-load-threads 4`. Eight writers keep `Threads_running` above 4 almost all the time, so the copy paused for 555 s and crawled at about 220 rows per second, and it was stopped by hand. That limit was simply set below the workload's own concurrency, and it shows that the Threads_running throttle trades copy time for foreground headroom one to one. The run did not finish, so there are no repeat numbers for INPLACE and INSTANT.

### What the numbers say, with the caveats

- INSTANT is the answer whenever it applies. It changed the table in about a tenth of a second with no copy, and `dbgctl osc` detected that case and used it without being asked.
- For a change that needs a rebuild (the index here), MySQL's own INPLACE, LOCK=NONE finished in 7.9 s and the osc copy took 28.4 s, three to four times longer. On this one container INPLACE was also the better choice for the foreground's worst case (137 ms against 1.5 s).
- The osc copy cost the foreground a 36% to 47% QPS drop and roughly 1.5 to 1.7 times the p99 for the length of the change. Two things add up. The copy competes for the single CPU, and every foreground write also runs the trigger, which writes the shadow inside the same transaction. The worst single write, 0.9 to 1.5 s, was not attributed to a phase in these runs (the per-phase attribution was added to the bench after them). The likeliest candidates are the metadata lock of the RENAME and a writer waiting on a chunk's shared locks.
- The case for OSC is not visible on one container. INPLACE replicates as one ALTER that each replica applies in one piece, stalling its applier for the full ALTER time, and the osc copy replicates as small transactions that the lag throttle can pace. That is the reason pt-osc, gh-ost and OSC exist, and measuring it needs the replica set. It has not been measured here.
- One run per mode on a shared, noisy CPU. The INPLACE baseline was about twice the osc baseline, which is noise, not a property of either method, and it makes the drop percentages rough. The numbers are to be rerun with `--repeat 3` on a quiet machine before any of them is used anywhere.

### How to reproduce

```
docker run -d --name osc-dev --cpus 1 --memory 700m -e MYSQL_ROOT_PASSWORD=root -p 23306:3306 \
  mysql:8.4 --gtid-mode=ON --enforce-gtid-consistency=ON --binlog-format=ROW --log-bin \
  --innodb-buffer-pool-size=256M
dbgctl osc bench --port 23306 --user root --password root --rows 500000 --threads 8 \
  --phase-s 20 --repeat 3 --out results/osc_bench.json
docker rm -fv osc-dev
```

`pytest -m integration tests/test_osc_integration.py` runs against the same container (20,000 rows, a writer inserting, updating and deleting during the change, every acknowledged write checked afterwards) and skips when nothing answers on port 23306.

## Unverified

- Everything on the real fleet. No run has gone through HAProxy or against a DBGuard primary, the lag throttle through `GET /v1/sets/{rs}` has only been unit-tested, and replica lag during a copy has not been measured.
- A failover in the middle of a run. The expected behaviour (leftovers on the new primary, removed by `dbgctl osc cleanup`) is reasoned, not tested.
- The repeated bench (`--repeat 3`) and the per-phase attribution of slow foreground writes, which were added after the one complete run.
- Tables with generated columns, UNIQUE indexes added by the ALTER, and `--allow-type-change`. These are covered by unit tests of the decisions only.
