"""Загрузка официального курса доллара ЦБ РФ в ClickHouse (external.cbr_rates).

Каждый запуск перезаписывает весь нужный диапазон одним запросом: диапазон небольшой, а
ReplacingMergeTree по (currency_code, rate_date) делает повторную загрузку идемпотентной.
Заодно подхватываются исправления курса задним числом, и не нужно хранить "где остановились".

ЦБ не публикует курс на воскресенье и понедельник (и праздники), здесь записей за эти дни нет:
последний известный курс подставляет dbt-модель dim_exchange_rates.
Запуск: из Airflow (DAG shop_daily) или docker compose run --rm --entrypoint python dbt /include/cbr_rates.py
"""
import logging
import os
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from decimal import Decimal

import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cbr_rates")

CURRENCY_CODE = "USD"
CBR_CURRENCY_ID = "R01235"  # внутренний код доллара в API ЦБ
URL = "https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start:%d/%m/%Y}&date_req2={end:%d/%m/%Y}&VAL_NM_RQ={id}"
# запас до первого заказа: он мог прийтись на длинные праздники, когда курс не публикуется
LOOKBACK = timedelta(days=14)

DDL = """
CREATE TABLE IF NOT EXISTS external.cbr_rates
(
    rate_date     Date,
    currency_code LowCardinality(String),
    nominal       UInt32,
    rub_per_unit  Decimal(12, 4),
    loaded_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (currency_code, rate_date)
"""


def parse_rates(xml_bytes: bytes) -> list[tuple[date, str, int, Decimal]]:
    """XML ответа ЦБ (windows-1251, запятая в дробях) -> строки таблицы."""
    rows = []
    for record in ET.fromstring(xml_bytes).iter("Record"):
        rate = Decimal(record.findtext("VunitRate").replace(",", "."))
        if rate <= 0:
            raise ValueError(f"некорректный курс: {record.attrib} = {rate}")
        rows.append((datetime.strptime(record.attrib["Date"], "%d.%m.%Y").date(), CURRENCY_CODE,
                     int(record.findtext("Nominal")), rate))
    return rows


def fetch(start: date, end: date) -> bytes:
    url = URL.format(start=start, end=end, id=CBR_CURRENCY_ID)
    request = urllib.request.Request(url, headers={"User-Agent": "shop-data-platform"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def main():
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )
    client.command("CREATE DATABASE IF NOT EXISTS external")
    client.command(DDL)

    first_order = client.command("SELECT toDate(min(created_at)) FROM shop.orders WHERE _is_deleted = 0")
    end = date.today()
    # заказов ещё нет: min() вернёт 1970-01-01, а такой диапазон у ЦБ запрашивать незачем
    start = max(date.fromisoformat(str(first_order)) - LOOKBACK, end - timedelta(days=400))

    rows = parse_rates(fetch(start, end))
    if not rows:  # пустой ответ - сбой источника: нельзя молча считать, что всё загружено
        raise RuntimeError(f"ЦБ не вернул курс за {start} - {end}")
    client.insert("external.cbr_rates", rows, column_names=["rate_date", "currency_code", "nominal", "rub_per_unit"])
    log.info("загружено %d курсов %s: %s - %s, последний %s", len(rows), CURRENCY_CODE, rows[0][0], rows[-1][0], rows[-1][3])


if __name__ == "__main__":
    main()
