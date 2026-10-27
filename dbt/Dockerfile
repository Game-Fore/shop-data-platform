# Образ с dbt для ClickHouse. Проект монтируется в /dbt как volume (см. docker-compose.yml),
# поэтому после правки моделей пересобирать образ не нужно.
FROM python:3.12-slim

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DBT_PROFILES_DIR=/dbt

# git нужен dbt deps/debug для пакетов из git; для dbt Hub не обязателен, но debug без него ругается
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# dbt-clickhouse 1.10 требует dbt-core>=1.9; 1.10 уже вне поддержки, поэтому core 1.11
RUN pip install dbt-core==1.11.16 dbt-clickhouse==1.10.3

WORKDIR /dbt
ENTRYPOINT ["dbt"]
CMD ["--help"]
