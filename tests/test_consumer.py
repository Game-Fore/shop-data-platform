import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException

import consumer as c


def order_event(**overrides) -> dict:
    event = {
        "id": 1, "customer_id": 2, "status": "new", "total": "10.50",
        "created_at": "2026-10-05T08:33:39.947123Z", "updated_at": "2026-10-05T08:33:52.011540Z",
        "__op": "c", "__lsn": 100, "__source_ts_ms": 1791189232011, "__deleted": "false",
    }
    return {**event, **overrides}


ORDER_FIELDS = c.TABLES["shop.public.orders"][1]


class TestConverters:
    def test_valid_values(self):
        assert c.integer(5) == 5
        assert c.text("a") == "a"
        assert c.optional_text(None) is None
        assert c.dec("10.50") == Decimal("10.50")
        assert c.ts("2026-10-05T08:33:39Z") == datetime(2026, 10, 5, 8, 33, 39, tzinfo=UTC)

    @pytest.mark.parametrize("value", ["abc", "5", None, 1.5, True])
    def test_integer_rejects(self, value):
        # bool - подкласс int: True молча записался бы как 1
        with pytest.raises(ValueError):
            c.integer(value)

    @pytest.mark.parametrize("converter", [c.text, c.dec, c.ts])
    @pytest.mark.parametrize("value", [None, 5])
    def test_required_text_fields_reject(self, converter, value):
        with pytest.raises(ValueError):
            converter(value)

    def test_dec_rejects_garbage(self):
        with pytest.raises(ArithmeticError):
            c.dec("not a number")


class TestToRow:
    def test_columns_in_table_order(self):
        row = c.to_row(order_event(), ORDER_FIELDS)
        assert row[:4] == [1, 2, "new", Decimal("10.50")]
        assert row[-4:-2] == ["c", 100]
        assert row[-1] == 0

    def test_deleted_flag(self):
        assert c.to_row(order_event(__deleted="true", __op="d"), ORDER_FIELDS)[-1] == 1

    @pytest.mark.parametrize("bad", [
        {"id": "abc"},        # неверный тип: без проверки уронил бы весь батч при вставке
        {"total": None},
        {"status": None},
        {"created_at": None},
    ])
    def test_poison_values_fail_before_insert(self, bad):
        with pytest.raises(ValueError):
            c.to_row(order_event(**bad), ORDER_FIELDS)

    def test_missing_meta_field_fails(self):
        event = order_event()
        del event["__lsn"]
        with pytest.raises(KeyError):
            c.to_row(event, ORDER_FIELDS)

    def test_every_table_accepts_its_own_event(self):
        # маппинг каждого топика согласован с собственными конвертерами
        values = {"id": 1, "customer_id": 1, "order_id": 1, "product_id": 1, "quantity": 1,
                  "price": "1.00", "total": "1.00",
                  "email": "a", "full_name": "a", "city": None, "name": "a", "category": "a", "status": "new",
                  "experiment_id": "e", "variant": "control", "created_at": "2026-10-05T08:33:39Z",
                  "updated_at": "2026-10-05T08:33:39Z", "assigned_at": "2026-10-05T08:33:39Z",
                  "__op": "c", "__lsn": 1, "__source_ts_ms": 1}
        for _, fields in c.TABLES.values():
            assert len(c.to_row(values, fields)) == len(fields) + len(c.META_COLUMNS)


class TestFlush:
    def buffers(self):
        return {topic: [] for topic in c.TABLES} | {"shop.public.orders": [["row1"], ["row2"]]}

    def test_inserts_and_clears_buffer(self):
        ch, buffers = MagicMock(), self.buffers()
        assert c.flush(ch, buffers) == 2
        ch.insert.assert_called_once()
        assert ch.insert.call_args.args[0] == "orders"
        assert buffers["shop.public.orders"] == []

    def test_retries_then_succeeds(self, monkeypatch):
        monkeypatch.setattr(c.time, "sleep", lambda _: None)
        ch, buffers = MagicMock(), self.buffers()
        ch.insert.side_effect = [ConnectionError("down"), None]
        assert c.flush(ch, buffers) == 2
        assert ch.insert.call_count == 2

    def test_gives_up_and_keeps_rows(self, monkeypatch):
        # упасть, не потеряв строки: offset не коммитится, после перезапуска прочитаем снова
        monkeypatch.setattr(c.time, "sleep", lambda _: None)
        ch, buffers = MagicMock(), self.buffers()
        ch.insert.side_effect = ConnectionError("down")
        with pytest.raises(RuntimeError, match="giving up"):
            c.flush(ch, buffers)
        assert ch.insert.call_count == c.MAX_RETRIES
        assert len(buffers["shop.public.orders"]) == 2

    def test_empty_buffers_do_not_touch_clickhouse(self):
        ch = MagicMock()
        assert c.flush(ch, {topic: [] for topic in c.TABLES}) == 0
        ch.insert.assert_not_called()


class TestCommit:
    def test_nothing_to_commit_is_not_an_error(self):
        # после ребаланса позиций нет: раньше из-за этого consumer падал
        consumer = MagicMock()
        consumer.commit.side_effect = KafkaException(KafkaError(KafkaError._NO_OFFSET))
        c.commit(consumer)

    def test_other_errors_propagate(self):
        consumer = MagicMock()
        consumer.commit.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
        with pytest.raises(KafkaException):
            c.commit(consumer)


class TestDeadLetterQueue:
    def make(self, monkeypatch):
        producer = MagicMock()
        producer.flush.return_value = 0
        monkeypatch.setattr(c, "Producer", lambda conf: producer)
        return c.DeadLetterQueue({"bootstrap.servers": "x"}), producer

    def message(self):
        msg = MagicMock()
        msg.key.return_value = b"k"
        msg.value.return_value = b"not json"
        msg.topic.return_value = "shop.public.orders"
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        return msg

    def test_keeps_original_bytes_and_source_in_headers(self, monkeypatch):
        dlq, producer = self.make(monkeypatch)
        try:
            json.loads(b"not json")
        except ValueError as e:
            dlq.send(self.message(), e)
        kwargs = producer.produce.call_args.kwargs
        assert producer.produce.call_args.args[0] == c.DLQ_TOPIC
        assert kwargs["value"] == b"not json"
        assert kwargs["key"] == b"k"
        assert kwargs["headers"]["dlq.source.topic"] == "shop.public.orders"
        assert kwargs["headers"]["dlq.source.offset"] == "42"
        assert kwargs["headers"]["dlq.error"].startswith("JSONDecodeError")

    def test_flush_ok(self, monkeypatch):
        dlq, _ = self.make(monkeypatch)
        dlq.flush()

    def test_undelivered_blocks_commit(self, monkeypatch):
        # исключение до коммита: иначе сообщение пропало бы и из источника, и из DLQ
        dlq, producer = self.make(monkeypatch)
        producer.flush.return_value = 1
        with pytest.raises(RuntimeError, match="undelivered"):
            dlq.flush()

    def test_delivery_error_blocks_commit(self, monkeypatch):
        dlq, producer = self.make(monkeypatch)
        dlq.send(self.message(), ValueError("x"))
        producer.produce.call_args.kwargs["on_delivery"]("broker down", None)
        with pytest.raises(RuntimeError):
            dlq.flush()


def test_observe_lag_clears_pending(monkeypatch):
    monkeypatch.setattr(c.time, "time", lambda: 100.0)
    observed = []
    monkeypatch.setattr(c.metrics.E2E_LAG_SECONDS, "observe", observed.append)
    pending = [98_000, 99_500]
    c.observe_lag(pending)
    assert observed == [2.0, 0.5]
    assert pending == []
