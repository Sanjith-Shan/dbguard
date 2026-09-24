#!/bin/bash
# First-boot datadir initialisation, reusing the stock image's entrypoint functions.
# Identical to the image's _main init branch except that the temporary server is started
# with --super-read-only=OFF --read-only=OFF, and nothing is exec'd afterwards.
set -eo pipefail   # not -u: the stock entrypoint functions are not nounset-safe
source /usr/local/bin/docker-entrypoint.sh

set -- mysqld
mysql_check_config "$@"
docker_setup_env "$@"
docker_create_db_directories "$@"

if [ "$(id -u)" = "0" ]; then
    exec gosu mysql "$BASH_SOURCE"
fi

if [ -n "${DATABASE_ALREADY_EXISTS:-}" ]; then
    exit 0
fi

docker_verify_minimum_env
docker_init_database_dir "$@"
docker_temp_server_start "$@" --super-read-only=OFF --read-only=OFF
mysql_socket_fix
docker_setup_db
docker_process_init_files /docker-entrypoint-initdb.d/*
docker_temp_server_stop
mysql_note "DBGuard datadir init done"
