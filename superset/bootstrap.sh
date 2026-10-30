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

# Пользователь bi может только читать базу marts (см. superset/init/clickhouse.sh)
uri=$(python -c '
import os
from sqlalchemy.engine import URL
print(URL.create("clickhousedb", username="bi", password=os.environ["CLICKHOUSE_BI_PASSWORD"],
                 host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), port=8123,
                 database="marts").render_as_string(hide_password=False))
')
superset set-database-uri --database-name "ClickHouse (витрины)" --uri "$uri"

exec /usr/bin/run-server.sh
