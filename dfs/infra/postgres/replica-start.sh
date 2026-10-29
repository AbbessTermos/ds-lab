#!/bin/bash
# hot-standby replica: copy the primary once with pg_basebackup, then follow it with streaming replication
set -e
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  mkdir -p "$PGDATA"; chmod 700 "$PGDATA"
  until PGPASSWORD=dfs pg_basebackup -h postgres -U dfs -D "$PGDATA" -R -X stream; do
    echo "waiting for primary..."; rm -rf "$PGDATA"/*; sleep 3
  done
  chmod 700 "$PGDATA"
fi
exec postgres
