import json
import logging
import signal
import time
import clickhouse_connect
import argparse
import os


from datetime import datetime, timezone
from decimal import Decimal
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

import metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

KAFKA_CONF = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "cdc-clickhouse",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
    # Debezium создаёт топик при первом событии таблицы. Если consumer стартовал раньше,
    # librdkafka увидит новый топик только при обновлении метаданных (по умолчанию раз в 5 мин).
    "topic.metadata.refresh.interval.ms": 5000,
}
load_dotenv()  # ищет .env вверх от этого файла, т.е. в корне репозитория
CH_CONF = dict(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
    username=os.environ["CLICKHOUSE_USER"],
    password=os.environ["CLICKHOUSE_PASSWORD"],
    database="shop",
)

BATCH_SIZE = 1000
FLUSH_INTERVAL_SEC = 1.0
MAX_RETRIES = 5


# --- конвертеры полей Debezium -> Python-типы для clickhouse-connect ---
def raw(v):
    return v

def dec(v):
    return Decimal(v) if v is not None else None

def ts(v):
    # Debezium отдаёт timestamptz как ISO-строку: 2026-10-05T08:33:39.947123Z
    return datetime.fromisoformat(v) if v else None

def ts_ms(v):
    return datetime.fromtimestamp(v / 1000, tz=timezone.utc)


# топик -> (таблица в ClickHouse, {колонка: конвертер})
TABLES = {
    "shop.public.customers": ("customers", {
        "id": raw, "email": raw, "full_name": raw, "city": raw, "created_at": ts,
    }),
    "shop.public.products": ("products", {
        "id": raw, "name": raw, "category": raw, "price": dec,
    }),
    "shop.public.orders": ("orders", {
        "id": raw, "customer_id": raw, "status": raw, "total": dec,
        "created_at": ts, "updated_at": ts,
    }),
    "shop.public.order_items": ("order_items", {
        "id": raw, "order_id": raw, "product_id": raw, "quantity": raw, "price": dec,
    }),
    "shop.public.experiment_assignments": ("experiment_assignments", {
        "experiment_id": raw, "customer_id": raw, "variant": raw, "assigned_at": ts,
    }),
}
META_COLUMNS = ["_op", "_lsn", "_source_ts", "_is_deleted"]


def to_row(value: dict, fields: dict) -> list:
    row = [conv(value.get(col)) for col, conv in fields.items()]
    row += [
        value["__op"],
        value["__lsn"],
        ts_ms(value["__source_ts_ms"]),
        1 if value.get("__deleted") == "true" else 0,
    ]
    return row


def flush(ch, buffers: dict) -> int:
    started = time.monotonic()
    total = 0
    for topic, rows in buffers.items():
        if not rows:
            continue
        table, fields = TABLES[topic]
        columns = list(fields) + META_COLUMNS

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                ch.insert(table, rows, column_names=columns)
                break
            except Exception as e:
                metrics.INSERT_ERRORS.labels(table).inc()
                log.warning("insert into %s failed (attempt %d/%d): %s", table, attempt, MAX_RETRIES, e)
                time.sleep(2 ** attempt)
        else:
            raise RuntimeError(f"giving up on insert into {table}")

        metrics.ROWS_INSERTED.labels(table).inc(len(rows))
        total += len(rows)
        rows.clear()
    metrics.BATCH_ROWS.observe(total)
    metrics.FLUSH_SECONDS.observe(time.monotonic() - started)
    metrics.LAST_FLUSH.set_to_current_time()
    return total


def observe_lag(source_ts_ms: list[int]):
    """Сквозная задержка записанных строк: от коммита в Postgres до этого момента."""
    now = time.time()
    for ts in source_ts_ms:
        metrics.E2E_LAG_SECONDS.observe(now - ts / 1000)
    source_ts_ms.clear()


def commit(consumer):
    """Синхронный коммит позиций. _NO_OFFSET - не ошибка: после ребаланса (например,
    когда Debezium создал новый топик) позиций ещё нет и коммитить нечего."""
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        if e.args[0].code() != KafkaError._NO_OFFSET:
            raise
        log.info("nothing to commit (no offsets stored after rebalance)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash-after-flush", type=int, default=0,
                        help="упасть после N-го flush, до коммита offset'ов (тест отказа)")
    args = parser.parse_args()
    log.info("pid=%d", os.getpid())
    metrics.start_metrics_server()
    flushes = 0
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(list(TABLES))
    ch = clickhouse_connect.get_client(**CH_CONF)

    buffers = {topic: [] for topic in TABLES}
    pending = 0
    pending_ts = []
    last_flush = time.monotonic()

    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)

    log.info("consuming %s", list(TABLES))
    try:
        while running:
            msg = consumer.poll(0.5)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        metrics.KAFKA_ERRORS.inc()
                        log.error("kafka error: %s", msg.error())
                elif msg.value() is not None:  # tombstone пропускаем
                    try:
                        value = json.loads(msg.value())
                        _, fields = TABLES[msg.topic()]
                        buffers[msg.topic()].append(to_row(value, fields))
                        pending += 1
                        pending_ts.append(value["__source_ts_ms"])
                        metrics.MESSAGES.labels(msg.topic(), "ok").inc()
                    except Exception as e:
                        metrics.MESSAGES.labels(msg.topic(), "bad").inc()
                        log.error("bad message %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), e)

            if pending >= BATCH_SIZE or (pending and time.monotonic() - last_flush >= FLUSH_INTERVAL_SEC):
                n = flush(ch, buffers)
                observe_lag(pending_ts)
                flushes += 1
                if args.crash_after_flush and flushes >= args.crash_after_flush:
                    log.warning("simulating crash after flush #%d: data inserted, offsets NOT committed", flushes)
                    os._exit(1)  # мгновенный выход без finally и без commit
                commit(consumer)
                log.info("flushed %d rows", n)
                pending = 0
                last_flush = time.monotonic()
    finally:
        if pending:
            flush(ch, buffers)
            commit(consumer)
        consumer.close()
        log.info("stopped")


if __name__ == "__main__":
    main()