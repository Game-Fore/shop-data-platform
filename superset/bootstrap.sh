#!/usr/bin/env bash
# Запуск Superset: миграции метабазы, администратор, подключение к ClickHouse, затем веб-сервер.
# Все шаги идемпотентны: контейнер можно перезапускать.
set -euo pipefail

: "${SUPERSET_SECRET_KEY:?задайте в .env (см. .env.example)}"
: "${SUPERSET_DB_PASSWORD:?задайте в .env (см. .env.example)}"
: "${SUPERSET_ADMIN_PASSWORD:?задайте в .env (см. .env.example)}"
: "${CLICKHOUSE_BI_PASSWORD:?задайте в .env (см. .env.example)}"

superset db upgrade

superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname Local \
    --email admin@example.com \
    --password "$SUPERSET_ADMIN_PASSWORD"

superset init

# Подключение к ClickHouse (пользователь bi только на чтение marts), датасеты, чарты и дашборд - из git
python /app/import_assets.py

exec /usr/bin/run-server.sh
