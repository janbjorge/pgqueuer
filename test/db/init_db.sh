#!/bin/bash
set -e

# Create the testdb database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE testdb;
EOSQL

# Install PgQueuer
export PATH="/opt/venv/bin:$PATH"
PGUSER=$POSTGRES_USER PGDATABASE=testdb python3 -m PgQueuer install
