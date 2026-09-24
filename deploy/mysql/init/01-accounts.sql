-- DBGuard accounts (docs/INTERFACES.md "MySQL accounts and schema").
-- Runs once at datadir initialisation, but is idempotent anyway.
-- Not binlogged: every node initialises identically and starts with an empty gtid_executed.
SET SESSION sql_log_bin = 0;

CREATE USER IF NOT EXISTS 'dbguard'@'%' IDENTIFIED WITH caching_sha2_password BY 'dbguard';
GRANT ALL PRIVILEGES ON *.* TO 'dbguard'@'%';
GRANT BACKUP_ADMIN, CLONE_ADMIN, REPLICATION SLAVE, REPLICATION CLIENT,
      SYSTEM_VARIABLES_ADMIN, CONNECTION_ADMIN ON *.* TO 'dbguard'@'%';

CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH caching_sha2_password BY 'repl';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';

CREATE USER IF NOT EXISTS 'chaos'@'%' IDENTIFIED WITH caching_sha2_password BY 'chaos';
GRANT ALL PRIVILEGES ON chaos.* TO 'chaos'@'%';
