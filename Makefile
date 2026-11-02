# Типовые команды для Linux и macOS. На Windows то же самое: .\scripts\dev.ps1 <команда>
PYTHON ?= python
ALL_PROFILES = --profile bi --profile orch --profile obs --profile tools

.DEFAULT_GOAL := help
.PHONY: help env reset down up consumer backfill verify rates dbt docs bi orch obs lint test integration

help:  ## показать команды
	@grep -hE '^[a-z]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | expand -t 14

env:  ## создать .env из шаблона, если его нет
	@test -f .env || cp .env.example .env

reset: env  ## стереть данные и поднять стенд с коннектором
	docker compose $(ALL_PROFILES) down -v
	docker compose up -d --wait
	curl -fsS -X POST -H "Content-Type: application/json" \
		--data @connectors/postgres-shop.json http://localhost:8083/connectors > /dev/null

down:  ## остановить стенд, данные сохраняются (продолжить: make up)
	docker compose $(ALL_PROFILES) stop

up: env  ## запустить основной стенд
	docker compose up -d --wait

consumer:  ## consumer: Kafka -> ClickHouse (занимает терминал)
	$(PYTHON) consumer/consumer.py

backfill:  ## история за 180 дней, consumer должен работать
	$(PYTHON) generator.py --backfill-days 180

verify:  ## сверка Postgres и ClickHouse
	$(PYTHON) verify.py

rates:  ## загрузить курсы ЦБ
	docker compose run --rm --entrypoint python dbt /include/cbr_rates.py

dbt:  ## собрать витрины и прогнать тесты dbt
	docker compose --profile tools build dbt
	docker compose run --rm dbt deps
	docker compose run --rm dbt build

docs:  ## документация dbt: http://localhost:8081
	docker compose run --rm dbt docs generate
	docker compose run --rm --service-ports dbt docs serve --host 0.0.0.0 --port 8081

bi:  ## Superset: http://localhost:8088
	docker compose --profile bi up -d --wait superset

orch:  ## Airflow: http://localhost:8082
	docker compose --profile orch up -d --wait airflow-api-server

obs:  ## Prometheus: http://localhost:9090, дашборд: http://localhost:3000/d/cdc-consumer
	docker compose --profile obs up -d --wait prometheus

lint:  ## линтер
	$(PYTHON) -m ruff check .

test:  ## unit-тесты, сервисы не нужны
	$(PYTHON) -m pytest

integration:  ## полный прогон на настоящем стенде, СТИРАЕТ данные
	PYTHON=$(PYTHON) bash scripts/integration_test.sh
