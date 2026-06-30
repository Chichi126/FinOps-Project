#!/bin/bash
set -e

# Automatically connect to PostgreSQL using the master environment variables 
# defined in your docker-compose.yml file and run the SQL command.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE fintech_transactions;
EOSQL