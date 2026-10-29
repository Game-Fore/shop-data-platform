CREATE DATABASE IF NOT EXISTS shop;

CREATE TABLE shop.customers
(
    id           UInt32,
    email        String,
    full_name    String,
    city         Nullable(String),
    created_at   DateTime64(6, 'UTC'),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;



CREATE TABLE shop.products
(
    id           UInt32,
    name         String,
    category     LowCardinality(String),
    price        Decimal(10, 2),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;



CREATE TABLE shop.orders
(
    id           UInt32,
    customer_id  UInt32,
    status       LowCardinality(String),
    total        Decimal(12, 2),
    created_at   DateTime64(6, 'UTC'),
    updated_at   DateTime64(6, 'UTC'),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;



CREATE TABLE shop.order_items
(
    id           UInt32,
    order_id     UInt32,
    product_id   UInt32,
    quantity     UInt32,
    price        Decimal(10, 2),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;


-- Журнал всех CDC-событий по заказам.
-- shop.orders (ReplacingMergeTree) после мержа хранит только последнюю версию заказа,
-- а здесь остаётся каждая версия: все переходы статусов и удалённые заказы.
-- Нужен для воронки и времени между статусами (dbt: int_order_status_events).
--
-- ReplacingMergeTree без версии + ORDER BY (order_id, _lsn): consumer даёт at-least-once,
-- и при повторной доставке одно и то же событие вставится дважды. Ключ (order_id, _lsn)
-- уникален для события, поэтому мерж схлопнет такие копии, а разные события не тронет.
CREATE TABLE shop.orders_changelog
(
    order_id     UInt32,
    customer_id  UInt32,
    status       LowCardinality(String),
    total        Decimal(12, 2),
    created_at   DateTime64(6, 'UTC'),
    updated_at   DateTime64(6, 'UTC'),
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (order_id, _lsn);

-- MV в ClickHouse - это триггер на INSERT: каждый блок, вставленный consumer'ом
-- в shop.orders, тем же запросом пишется и в журнал. Consumer менять не нужно.
CREATE MATERIALIZED VIEW shop.orders_changelog_mv TO shop.orders_changelog AS
SELECT
    id AS order_id,
    customer_id,
    status,
    total,
    created_at,
    updated_at,
    _op,
    _lsn,
    _source_ts,
    _is_deleted,
    _ingested_at
FROM shop.orders;



-- Назначения в группы A/B-экспериментов. Ключ (experiment_id, customer_id) повторяет
-- составной первичный ключ Postgres.
CREATE TABLE shop.experiment_assignments
(
    experiment_id LowCardinality(String),
    customer_id   UInt32,
    variant       LowCardinality(String),
    assigned_at   DateTime64(6, 'UTC'),
    _op           LowCardinality(String),
    _lsn          UInt64,
    _source_ts    DateTime64(3, 'UTC'),
    _is_deleted   UInt8,
    _ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY (experiment_id, customer_id);
