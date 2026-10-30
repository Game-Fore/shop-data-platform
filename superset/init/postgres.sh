#!/usr/bin/env bash
# Одноразовый шаг перед Superset: роль и база для его метаданных в существующем Postgres.
# Debezium читает только базу shop, поэтому метаданные Superset в CDC не попадают.
# Запускается при каждом старте профиля bi, поэтому всё идемпотентно.
set -euo pipefail
: "${SUPERSET_DB_PASSWORD:?задайте в .env (см. .env.example)}"

# psql -v + :'var' экранирует пароль; \gexec нужен, потому что CREATE DATABASE нельзя выполнить в DO-блоке
psql -v ON_ERROR_STOP=1 -v pw="$SUPERSET_DB_PASSWORD" -d postgres <<'SQL'
SELECT format('CREATE ROLE superset LOGIN PASSWORD %L', :'pw')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'superset') \gexec

SELECT format('ALTER ROLE superset PASSWORD %L', :'pw') \gexec

SELECT 'CREATE DATABASE superset OWNER superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset') \gexec
SQL
echo "postgres: роль и база superset готовы"
