#!/usr/bin/env bash
# Одноразовый шаг перед Superset: пользователь ClickHouse только на чтение витрин.
# BI видит только базу marts (без персональных данных: email и ФИО остались в staging)
# и не может ничего изменить, даже если в SQL Lab напишут DROP TABLE.
# Запускается при каждом старте профиля bi, поэтому всё идемпотентно.
set -euo pipefail
: "${CLICKHOUSE_BI_PASSWORD:?задайте в .env (см. .env.example)}"

# экранирование для строкового литерала ClickHouse: \ -> \\, ' -> \'
pw=$(printf '%s' "$CLICKHOUSE_BI_PASSWORD" | sed -e 's/\\/\\\\/g' -e "s/'/\\\\'/g")

clickhouse-client --host clickhouse --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE USER OR REPLACE bi IDENTIFIED BY '${pw}' SETTINGS max_execution_time = 60;
GRANT SELECT ON marts.* TO bi;
SQL
echo "clickhouse: пользователь bi готов"
