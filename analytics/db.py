"""Запросы к ClickHouse из ноутбуков. Креды берутся из .env в корне репозитория."""
import os
from decimal import Decimal
from functools import lru_cache

import clickhouse_connect
import pandas as pd
from dotenv import find_dotenv, load_dotenv

# usecwd: ноутбук запускается из notebooks/, а .env лежит уровнем выше - ищем вверх от cwd
load_dotenv(find_dotenv(usecwd=True))


@lru_cache(maxsize=1)
def client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="marts",
    )


def query(sql: str, **params) -> pd.DataFrame:
    """Выполнить SELECT и вернуть DataFrame.

    Decimal-колонки (деньги) приводятся к float: для аналитики точности float64 хватает,
    а pandas и scipy с Decimal работают медленно и не везде.
    """
    df = client().query_df(sql, parameters=params or None)
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna()
            if not sample.empty and isinstance(sample.iloc[0], Decimal):
                df[col] = df[col].astype(float)
    return df
