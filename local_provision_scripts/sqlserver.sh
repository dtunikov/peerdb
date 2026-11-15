#!/bin/sh
set -Eeu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../.env
. "$SCRIPT_DIR/../.env"
. "$SCRIPT_DIR/../ancillary.env"

DOCKER="docker"
CONTAINER="peerdb-sqlserver"

sqlcmd() {
  # shellcheck disable=SC2016 # expanded inside the container
  $DOCKER exec -e SQLCMDPASSWORD="$CI_SQLSERVER_PASSWORD" "$CONTAINER" sh -c '
    if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then exec /opt/mssql-tools18/bin/sqlcmd -C "$@"; fi
    exec /opt/mssql-tools/bin/sqlcmd "$@"
  ' sqlcmd -S localhost -U "$CI_SQLSERVER_USER" -b "$@"
}

# PeerDB never enables CDC itself, so the test database is prepared here
sqlcmd -Q "
IF DB_ID('$CI_SQLSERVER_DATABASE') IS NULL CREATE DATABASE [$CI_SQLSERVER_DATABASE];
"
sqlcmd -d "$CI_SQLSERVER_DATABASE" -Q "
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 EXEC sys.sp_cdc_enable_db;
"
sqlcmd -Q "SELECT @@VERSION; SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%';"
