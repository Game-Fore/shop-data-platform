"""Тесты работают без Docker, Kafka и ClickHouse. Скрипты читают креды при импорте, поэтому здесь фиктивные значения."""
import os

for name in ("POSTGRES_USER", "POSTGRES_PASSWORD", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"):
    os.environ.setdefault(name, "test")
