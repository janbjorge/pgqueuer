#!/bin/bash
set -e

# Create the testdb database
PGUSER="$POSTGRES_USER" psql -v ON_ERROR_STOP=1 <<-EOSQL
    CREATE DATABASE testdb;
EOSQL

export PATH="/opt/venv/bin:$PATH"
pgq install --dry-run | PGUSER="$POSTGRES_USER"  PGDATABASE=testdb psql -v ON_ERROR_STOP=1
