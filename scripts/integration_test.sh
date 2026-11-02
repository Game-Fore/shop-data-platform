#!/usr/bin/env bash
# Интеграционный тест на настоящем стенде: Postgres -> Debezium -> Kafka -> consumer -> ClickHouse -> dbt.
# Удаляет контейнеры и данные стенда! Запускается в CI и локально: bash scripts/integration_test.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python}"
# История воспроизводима (seed и --end-at), курсы из фикстуры, поэтому итог известен заранее
EXPECTED_ORDERS=16430
EXPECTED_REVENUE_RUB=1355955176

[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a

consumer_pid=""
cleanup() {
    status=$?
    [ -n "$consumer_pid" ] && kill "$consumer_pid" 2>/dev/null || true
    if [ "$status" -ne 0 ]; then
        echo "::group::consumer log"; tail -n 50 consumer.log 2>/dev/null || true; echo "::endgroup::"
        echo "::group::docker logs"; docker compose logs --tail=50 connect clickhouse 2>/dev/null || true; echo "::endgroup::"
    fi
    rm -f consumer.log
    exit "$status"
}
trap cleanup EXIT

clickhouse() {
    docker compose exec -T clickhouse clickhouse-client \
        --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "$1"
}

echo "--- конфигурация всех профилей корректна"
docker compose --profile bi --profile orch --profile obs --profile tools config -q

echo "--- стенд с нуля"
docker compose --profile bi --profile orch --profile obs --profile tools down -v
docker compose up -d --wait postgres kafka connect clickhouse
curl -fsS -X POST -H "Content-Type: application/json" \
    --data @connectors/postgres-shop.json http://localhost:8083/connectors > /dev/null

echo "--- consumer и история за 180 дней"
"$PYTHON" -u consumer/consumer.py > consumer.log 2>&1 &
consumer_pid=$!
"$PYTHON" generator.py --backfill-days 180

echo "--- сверка источника и приёмника"
"$PYTHON" verify.py --wait 180

kill -INT "$consumer_pid"
for _ in $(seq 1 20); do kill -0 "$consumer_pid" 2>/dev/null || break; sleep 1; done
kill -9 "$consumer_pid" 2>/dev/null || true
consumer_pid=""

echo "--- витрины и тесты dbt"
# Курсы из фикстуры: CI не должен зависеть от доступности cbr.ru
CLICKHOUSE_HOST=localhost "$PYTHON" airflow/include/cbr_rates.py --from-file tests/fixtures/cbr_usd_2026.xml
docker compose --profile tools build dbt
docker compose run --rm dbt deps
# Окно 0: consumer остановлен, поток не идёт. С окном по умолчанию сверки пропустили бы
# все заказы (они только что загружены) и проверяли бы пустое множество.
docker compose run --rm dbt build --vars '{settling_window_minutes: 0}'

echo "--- итог совпадает с ожидаемым"
orders=$(clickhouse "SELECT sum(orders) FROM marts.mart_sales_daily" | tr -d '\r')
revenue=$(clickhouse "SELECT round(sum(revenue_rub)) FROM marts.mart_sales_daily" | tr -d '\r')
echo "заказов: $orders (ожидалось $EXPECTED_ORDERS), выручка: $revenue (ожидалось $EXPECTED_REVENUE_RUB)"
# через if: set -e не срабатывает на непоследней команде в цепочке &&
if [ "$orders" != "$EXPECTED_ORDERS" ] || [ "$revenue" != "$EXPECTED_REVENUE_RUB" ]; then
    echo "::error::итог пайплайна не совпал с ожидаемым"
    exit 1
fi

echo "--- успех"
