import argparse
import json
import logging
import os
import signal
import time
from datetime import UTC, datetime
from decimal import Decimal

import clickhouse_connect
import metrics
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

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

DLQ_TOPIC = "shop.dlq"
BATCH_SIZE = 1000
FLUSH_INTERVAL_SEC = 1.0
MAX_RETRIES = 5


# --- конвертеры полей Debezium -> Python-типы для clickhouse-connect ---
# Строгие: значение неверного типа должно упасть здесь и уйти в DLQ. Если пропустить его до вставки,
# упадёт весь батч, а после перезапуска consumer прочитает то же сообщение: бесконечный цикл падений.
def integer(v):
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f"expected integer, got {v!r}")
    return v

def text(v):
    if not isinstance(v, str):
        raise ValueError(f"expected string, got {v!r}")
    return v

def optional_text(v):
    return None if v is None else text(v)

def dec(v):
    return Decimal(text(v))

def ts(v):
    # Debezium отдаёт timestamptz как ISO-строку: 2026-10-05T08:33:39.947123Z
    return datetime.fromisoformat(text(v))

def unsigned(v):
    v = integer(v)
    if v < 0:
        raise ValueError(f"expected non-negative integer, got {v!r}")
    return v

DEBEZIUM_OPS = {"c", "u", "d", "r"}  # create, update, delete, read (снапшот)

def op(v):
    if v not in DEBEZIUM_OPS:
        raise ValueError(f"unknown Debezium op {v!r}")
    return v

def deleted_flag(v):
    # ExtractNewRecordState с delete.tombstone.handling.mode=rewrite пишет "true"/"false"
    if v not in ("true", "false", None):
        raise ValueError(f"unexpected __deleted {v!r}")
    return 1 if v == "true" else 0

def ts_ms(v):
    return datetime.fromtimestamp(unsigned(v) / 1000, tz=UTC)


# топик -> (таблица в ClickHouse, {колонка: конвертер})
TABLES = {
    "shop.public.customers": ("customers", {
        "id": integer, "email": text, "full_name": text, "city": optional_text, "created_at": ts,
    }),
    "shop.public.products": ("products", {
        "id": integer, "name": text, "category": text, "price": dec,
    }),
    "shop.public.orders": ("orders", {
        "id": integer, "customer_id": integer, "status": text, "total": dec,
        "created_at": ts, "updated_at": ts,
    }),
    "shop.public.order_items": ("order_items", {
        "id": integer, "order_id": integer, "product_id": integer, "quantity": integer, "price": dec,
    }),
    "shop.public.experiment_assignments": ("experiment_assignments", {
        "experiment_id": text, "customer_id": integer, "variant": text, "assigned_at": ts,
    }),
}
META_COLUMNS = ["_op", "_lsn", "_source_ts", "_is_deleted"]


def to_row(value: dict, fields: dict) -> list:
    row = [conv(value.get(col)) for col, conv in fields.items()]
    # служебные поля проверяются так же строго: битый __lsn иначе уронил бы весь батч при вставке
    row += [
        op(value["__op"]),
        unsigned(value["__lsn"]),
        ts_ms(value["__source_ts_ms"]),
        deleted_flag(value.get("__deleted")),
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
    if total:  # батч из одних битых сообщений строк не записывает
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


class DeadLetterQueue:
    """Битые сообщения: исходные байты без изменений, причина и источник - в заголовках.

    Сообщение считается обработанным только после подтверждения от Kafka: flush() вызывается
    до коммита offset'а, иначе при сбое сообщение пропало бы и из топика-источника, и из DLQ.
    """

    def __init__(self, conf: dict):
        self.producer = Producer({"bootstrap.servers": conf["bootstrap.servers"], "acks": "all"})
        self.errors = []

    def send(self, msg, error: Exception):
        self.producer.produce(
            DLQ_TOPIC,
            key=msg.key(),
            value=msg.value(),
            headers={
                "dlq.source.topic": msg.topic(),
                "dlq.source.partition": str(msg.partition()),
                "dlq.source.offset": str(msg.offset()),
                "dlq.error": f"{type(error).__name__}: {error}"[:500],
            },
            on_delivery=lambda err, _: err and self.errors.append(err),
        )
        self.producer.poll(0)

    def flush(self):
        undelivered = self.producer.flush(10)
        if undelivered or self.errors:
            raise RuntimeError(f"DLQ undelivered: {undelivered}, errors: {self.errors[:3]}")


class Runner:
    """Цикл чтения Kafka -> буферы -> ClickHouse, вынесенный из main() для тестов с моками."""

    def __init__(self, consumer, ch, dlq: DeadLetterQueue):
        self.consumer, self.ch, self.dlq = consumer, ch, dlq
        self.buffers = {topic: [] for topic in TABLES}
        self.pending = 0      # прочитанные, но ещё не записанные сообщения (включая ушедшие в DLQ)
        self.pending_ts = []
        self.last_flush = time.monotonic()
        self.flushes = 0
        self.running = True
        # После окончательной ошибки буфер больше не дописываем: вставка уже исчерпала повторы.
        # Повторная попытка (при выходе или в on_revoke, который вызывает consumer.close())
        # удвоила бы ожидание и подменила бы исходную ошибку. Offset не закоммичен, данные не потеряны.
        self.failed = False

    def stop(self, *_):
        self.running = False

    def handle(self, msg):
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                metrics.KAFKA_ERRORS.inc()
                log.error("kafka error: %s", msg.error())
            return
        if msg.value() is None:  # tombstone пропускаем
            return
        try:
            value = json.loads(msg.value())
            _, fields = TABLES[msg.topic()]
            row = to_row(value, fields)
        except Exception as e:
            metrics.MESSAGES.labels(msg.topic(), "bad").inc()
            metrics.DLQ_MESSAGES.labels(msg.topic()).inc()
            log.error("bad message %s[%d]@%d -> %s: %s",
                      msg.topic(), msg.partition(), msg.offset(), DLQ_TOPIC, e)
            self.dlq.send(msg, e)
        else:
            self.buffers[msg.topic()].append(row)
            self.pending_ts.append(value["__source_ts_ms"])
            metrics.MESSAGES.labels(msg.topic(), "ok").inc()
        self.pending += 1

    def flush_all(self) -> int:
        try:
            n = flush(self.ch, self.buffers)
            self.dlq.flush()
        except Exception:
            self.failed = True
            raise
        observe_lag(self.pending_ts)
        self.pending = 0
        self.last_flush = time.monotonic()
        return n

    def on_revoke(self, c, partitions):
        # Партиции уходят другому consumer'у: дописать буфер и зафиксировать offset'ы сейчас,
        # иначе новый владелец прочитает эти сообщения повторно.
        if self.failed or not self.pending:
            return
        n = self.flush_all()
        try:
            commit(c)
        except KafkaException as e:
            # группа уже пересобрана: дубли безопасны, ReplacingMergeTree схлопнет их по _lsn
            log.warning("commit on revoke failed: %s", e)
        log.info("rebalance: flushed %d rows before revoke", n)

    def run(self, crash_after_flush: int = 0):
        log.info("consuming %s", list(TABLES))
        try:
            while self.running:
                msg = self.consumer.poll(0.5)
                if msg is not None:
                    self.handle(msg)
                due = time.monotonic() - self.last_flush >= FLUSH_INTERVAL_SEC
                if self.pending >= BATCH_SIZE or (self.pending and due):
                    n = self.flush_all()
                    self.flushes += 1
                    if crash_after_flush and self.flushes >= crash_after_flush:
                        log.warning("simulating crash after flush #%d: data inserted, offsets NOT committed",
                                    self.flushes)
                        os._exit(1)  # мгновенный выход без finally и без commit
                    commit(self.consumer)
                    log.info("flushed %d rows", n)
            # штатная остановка (SIGINT/SIGTERM): дописать хвост и зафиксировать offset'ы
            if self.pending:
                self.flush_all()
                commit(self.consumer)
        except Exception:
            self.failed = True
            raise
        finally:
            self.consumer.close()
            log.info("stopped")


def install_signal_handlers(runner: Runner):
    # SIGTERM присылает docker stop: без обработчика процесс умрёт, не дописав буфер
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, runner.stop)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash-after-flush", type=int, default=0,
                        help="упасть после N-го flush, до коммита offset'ов (тест отказа)")
    args = parser.parse_args()
    log.info("pid=%d", os.getpid())
    metrics.start_metrics_server()
    for topic, (table, _) in TABLES.items():
        # создать серии с нулём заранее: increase() в алертах не видит первого роста с "нет метрики" до 1
        metrics.INSERT_ERRORS.labels(table)
        metrics.DLQ_MESSAGES.labels(topic)

    runner = Runner(Consumer(KAFKA_CONF), clickhouse_connect.get_client(**CH_CONF), DeadLetterQueue(KAFKA_CONF))
    runner.consumer.subscribe(list(TABLES), on_revoke=runner.on_revoke)
    install_signal_handlers(runner)
    runner.run(args.crash_after_flush)


if __name__ == "__main__":
    main()