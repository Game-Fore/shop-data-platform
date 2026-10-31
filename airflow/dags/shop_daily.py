"""Ежедневный пересчёт витрин: проверка свежести источников и загрузка курсов ЦБ, затем dbt build.

Порядок важен: если CDC стоит, источник устарел, и пересчёт только перезаписал бы витрины теми же
старыми данными, создав видимость обновления. Лучше упавший DAG, чем тихо устаревший дашборд.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT = "/opt/dbt-venv/bin/dbt"
DBT_ENV = {
    "DBT_PROFILES_DIR": "/opt/dbt",
    "DBT_PROJECT_DIR": "/opt/dbt",
    # артефакты отдельно от проекта: каталог смонтирован и его же использует сервис dbt из профиля tools
    "DBT_TARGET_PATH": "/tmp/dbt/target",
    "DBT_LOG_PATH": "/tmp/dbt/logs",
}

with DAG(
    dag_id="shop_daily",
    description="Свежесть источников и пересборка витрин dbt",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Moscow"),
    catchup=False,  # витрины пересобираются целиком: догонять пропущенные дни незачем
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["dbt", "marts"],
) as dag:
    # append_env: иначе задача не увидит креды ClickHouse из окружения контейнера
    freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"{DBT} source freshness",
        env=DBT_ENV,
        append_env=True,
        retries=0,  # повтор через 5 минут не сделает устаревшие данные свежими
    )
    # dbt-venv: там уже есть clickhouse-connect, в окружение Airflow лишнее не ставим.
    # ЦБ может быть недоступен: повторяем дольше, но при отказе витрины не собираем - иначе рубли
    # молча считались бы по устаревшему курсу
    rates = BashOperator(
        task_id="load_cbr_rates",
        bash_command="/opt/dbt-venv/bin/python /opt/airflow/include/cbr_rates.py",
        append_env=True,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )
    build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build", env=DBT_ENV, append_env=True)

    [freshness, rates] >> build
