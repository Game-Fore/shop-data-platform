# Airflow 3 с dbt. LocalExecutor выполняет задачи внутри контейнера scheduler, поэтому dbt должен быть в образе.
# Контекст сборки - каталог dbt/ (см. docker-compose.yml): нужны packages.yml и dbt_project.yml.
FROM apache/airflow:3.3.2-python3.12

# Отдельный venv: зависимости dbt и Airflow конфликтуют, а смешивать их в одном окружении
# Airflow прямо не рекомендует. Версии те же, что в dbt/Dockerfile.
# /opt недоступен пользователю airflow на запись, поэтому каталог создаётся под root.
USER root
RUN mkdir /opt/dbt-venv && chown airflow /opt/dbt-venv
USER airflow
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.11.16 dbt-clickhouse==1.10.3

# Пакеты dbt ставятся при сборке, а не в каждом запуске DAG: ночной пайплайн не зависит от
# доступности dbt Hub и не трогает смонтированный каталог проекта.
# В домашнем каталоге: dbt deps удаляет каталог пакетов целиком, нужна запись в родительский.
ENV DBT_PACKAGES_PATH=/home/airflow/dbt-packages
# package-lock.yml фиксирует версию dbt_utils; --chown: dbt deps пишет в каталог проекта
COPY --chown=airflow dbt_project.yml packages.yml package-lock.yml /tmp/dbt-deps/
RUN cd /tmp/dbt-deps && /opt/dbt-venv/bin/dbt deps && rm -rf /tmp/dbt-deps
