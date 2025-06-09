#!/bin/bash

# This script sets up a PostgreSQL server on Ubuntu with default user and database.
# It is designed to work in environments without systemd.

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Updating package lists..."
sudo apt update

echo "Installing PostgreSQL..."
sudo apt install -y postgresql postgresql-contrib

echo "Starting PostgreSQL service..."
sudo service postgresql start

echo "Setting up PostgreSQL user and database with defaults..."

# Default PostgreSQL username, database name, and password
pg_user="pgquser"
pg_db="pgqdb"
pg_password="pgqpw"

# Create a PostgreSQL user and database
sudo -u postgres psql <<EOF
CREATE USER $pg_user WITH PASSWORD '$pg_password';
CREATE DATABASE $pg_db OWNER $pg_user;
ALTER USER $pg_user CREATEDB;
EOF

echo "Configuring PostgreSQL to accept connections from any host..."

# Update postgresql.conf to listen on all IP addresses
sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/*/main/postgresql.conf

# Update pg_hba.conf to allow all IP connections
echo "host all all 0.0.0.0/0 trust" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf
echo "host all all ::/0 trust" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf

echo "Restarting PostgreSQL service to apply changes..."
sudo service postgresql restart

echo "PostgreSQL setup is complete."
echo "User '$pg_user' and database '$pg_db' have been created."
echo "You can now connect to the database using the following environment variables:"
echo "PGUSER=$pg_user"
echo "PGDATABASE=$pg_db"
echo "PGPASSWORD=$pg_password"
echo "PGHOST=localhost"
echo "PGPORT=5432"
