#!/bin/bash
# DBGuard node entrypoint. Runs under tini as PID 1's child.
#
# 1. Render /etc/mysql/conf.d/dbguard-node.cnf from DBGUARD_SERVER_ID / DBGUARD_NODE.
# 2. If the datadir is empty, initialise it here, with the temporary server forced writable
#    (my.cnf boots every node super_read_only=1, which would make the image's own init and
#    our init SQL fail). After this, `docker-entrypoint.sh mysqld` only execs mysqld.
# 3. exec dbguard-agent (which starts `docker-entrypoint.sh mysqld` as its child), or,
#    when the agent is not installed, exec the stock entrypoint directly.
set -euo pipefail

: "${DBGUARD_NODE:?DBGUARD_NODE must be set}"
: "${DBGUARD_SERVER_ID:?DBGUARD_SERVER_ID must be set}"
DBGUARD_SEMISYNC="${DBGUARD_SEMISYNC:-1}"

log() { echo "{\"event\":\"entrypoint\",\"node\":\"${DBGUARD_NODE}\",\"msg\":\"$*\"}"; }

cat > /etc/mysql/conf.d/dbguard-node.cnf <<CNF
# Rendered by /entrypoint.sh at container start. Do not edit.
[mysqld]
server_id=${DBGUARD_SERVER_ID}
report_host=${DBGUARD_NODE}
report_port=3306
# DBGUARD_SEMISYNC=${DBGUARD_SEMISYNC} (the agent enables semi-sync per role only when 1)
CNF

# Optional binlog location (disk-full scenario, deploy/compose.diskfull.yml). Unset means
# the datadir, as in my.cnf. conf.d files are read in order, so this one overrides base.
if [ -n "${DBGUARD_BINLOG_DIR:-}" ]; then
    mkdir -p "${DBGUARD_BINLOG_DIR}"
    chown mysql:mysql "${DBGUARD_BINLOG_DIR}"
    cat >> /etc/mysql/conf.d/dbguard-node.cnf <<CNF
log_bin=${DBGUARD_BINLOG_DIR}/binlog
log_bin_index=${DBGUARD_BINLOG_DIR}/binlog.index
CNF
    log "binlog in ${DBGUARD_BINLOG_DIR}"
fi
log "rendered dbguard-node.cnf server_id=${DBGUARD_SERVER_ID} semisync=${DBGUARD_SEMISYNC}"

if [ ! -d /var/lib/mysql/mysql ]; then
    log "datadir empty, initialising"
    /usr/local/bin/dbguard-initdb.sh
    log "datadir initialised"
fi

mkdir -p /var/lib/dbguard

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

if command -v dbguard-agent >/dev/null 2>&1 && python3 -c 'import dbguard.agent.main' 2>/dev/null; then
    log "starting dbguard-agent"
    exec dbguard-agent
fi

log "dbguard-agent not importable, falling back to plain mysqld"
exec docker-entrypoint.sh mysqld
