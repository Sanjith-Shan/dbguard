-- DBGuard schemas. Idempotent, not binlogged (see 01-accounts.sql).
SET SESSION sql_log_bin = 0;

CREATE DATABASE IF NOT EXISTS dbguard;
CREATE TABLE IF NOT EXISTS dbguard.heartbeat (
  rs     VARCHAR(16) NOT NULL PRIMARY KEY,
  ts     TIMESTAMP(6) NULL,
  writer VARCHAR(64) NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dbguard.manager_probe (
  rs VARCHAR(16) NOT NULL PRIMARY KEY,
  ts TIMESTAMP(6) NULL
) ENGINE=InnoDB;

CREATE DATABASE IF NOT EXISTS chaos;
CREATE TABLE IF NOT EXISTS chaos.writes (
  client_id INT NOT NULL,
  seq       BIGINT NOT NULL,
  ts        TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  run_id    VARCHAR(64) NOT NULL,
  payload   VARBINARY(512) NULL,
  PRIMARY KEY (client_id, seq, run_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS chaos.blob (
  id   BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  data VARBINARY(4096) NULL
) ENGINE=InnoDB;
