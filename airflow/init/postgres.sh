#!/usr/bin/env bash
# Одноразовый шаг перед Airflow: роль и база для его метаданных в существующем Postgres.
# Debezium читает только базу shop, поэтому метаданные Airflow в CDC не попадают.
# Запускается при каждом старте профиля orch, поэтому всё идемпотентно.
set -euo pipefail
: "${AIRFLOW_DB_PASSWORD:?задайте в .env (см. .env.example)}"

# psql -v + :'var' экранирует пароль; \gexec нужен, потому что CREATE DATABASE нельзя выполнить в DO-блоке
psql -v ON_ERROR_STOP=1 -v pw="$AIRFLOW_DB_PASSWORD" -d postgres <<'SQL'
SELECT format('CREATE ROLE airflow LOGIN PASSWORD %L', :'pw')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') \gexec

SELECT format('ALTER ROLE airflow PASSWORD %L', :'pw') \gexec

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec
SQL
echo "postgres: роль и база airflow готовы"
