"""Метрики consumer'а для Prometheus.

Лага consumer group здесь намеренно нет: его считает kafka-exporter снаружи, иначе при падении
consumer'а метрика пропала бы ровно тогда, когда нужнее всего.
"""
import logging
import os

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger("consumer")

MESSAGES = Counter("cdc_consumer_messages_total", "Прочитанные сообщения: ok или bad (не удалось разобрать)",
                   ["topic", "status"])
ROWS_INSERTED = Counter("cdc_consumer_rows_inserted_total", "Строки, записанные в ClickHouse", ["table"])
INSERT_ERRORS = Counter("cdc_consumer_insert_errors_total",
                        "Неудачные попытки вставки, включая те, что прошли при повторе", ["table"])
DLQ_MESSAGES = Counter("cdc_consumer_dlq_messages_total", "Сообщения, отправленные в DLQ", ["source_topic"])
KAFKA_ERRORS = Counter("cdc_consumer_kafka_errors_total", "Ошибки, полученные от Kafka в poll()")
BATCH_ROWS = Histogram("cdc_consumer_batch_rows", "Строк в одном сбросе буферов",
                       buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500))
FLUSH_SECONDS = Histogram("cdc_consumer_flush_seconds", "Длительность сброса буферов, включая повторы",
                          buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60))
E2E_LAG_SECONDS = Histogram("cdc_consumer_e2e_lag_seconds", "От коммита в Postgres до записи в ClickHouse",
                            buckets=(0.1, 0.25, 0.5, 1, 1.5, 2, 2.5, 3, 5, 10, 30, 60, 300, 1800))
LAST_FLUSH = Gauge("cdc_consumer_last_flush_timestamp_seconds", "Время последнего успешного сброса (unix)")


def start_metrics_server():
    """Метрики вспомогательны: занятый порт не должен останавливать доставку данных."""
    port = int(os.getenv("CONSUMER_METRICS_PORT", "8000"))
    try:
        start_http_server(port)
        log.info("metrics on :%d/metrics", port)
    except OSError as e:
        log.warning("metrics server not started on port %d: %s", port, e)
