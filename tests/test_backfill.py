import hashlib
import random
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

import backfill as b

END_AT = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
PRODUCTS = list(range(1, 9))


def build(seed=42, days=60, end_at=END_AT):
    random.seed(seed)
    return b.build_events(days, PRODUCTS, end_at)


def fingerprint(events) -> str:
    parts = []
    for e in events:
        if e.kind == "order":
            detail = (e.payload[0].lines, str(e.payload[0].discount))
        elif e.kind == "status":
            detail = e.payload[1]
        elif e.kind == "price":
            detail = e.payload
        else:
            detail = e.payload[0].variant
        parts.append(f"{e.ts.isoformat()}|{e.kind}|{detail}")
    return hashlib.md5("\n".join(parts).encode()).hexdigest()


@pytest.fixture(scope="module")
def events():
    return build()


class TestReproducibility:
    def test_same_seed_and_end_give_identical_history(self):
        assert fingerprint(build()) == fingerprint(build())

    def test_other_seed_differs(self):
        assert fingerprint(build(seed=1)) != fingerprint(build(seed=2))

    def test_other_end_differs(self):
        assert fingerprint(build()) != fingerprint(build(end_at=END_AT - timedelta(days=30)))


class TestEventStream:
    def test_sorted_and_inside_window(self, events):
        assert [e.ts for e in events] == sorted(e.ts for e in events)
        # час выбирается внутри календарного дня, поэтому первый день может начаться раньше end_at - days
        assert events[0].ts.date() >= (END_AT - timedelta(days=60)).date()
        assert events[-1].ts < END_AT

    def test_references_are_written_before_use(self, events):
        # порядок записи: клиент раньше заказа, заказ раньше смены его статуса
        seen_customers, seen_orders = set(), set()
        for e in events:
            if e.kind == "customer":
                seen_customers.add(id(e.payload[0]))
            elif e.kind == "assignment":
                assert id(e.payload[0]) in seen_customers
            elif e.kind == "order":
                assert id(e.payload[0].customer) in seen_customers
                seen_orders.add(id(e.payload[0]))
            elif e.kind == "status":
                assert id(e.payload[0]) in seen_orders

    def test_every_customer_is_assigned_once(self, events):
        kinds = Counter(e.kind for e in events)
        assert kinds["customer"] == kinds["assignment"] > 0


class TestExperiment:
    def test_discount_only_for_treatment_in_window(self, events):
        start = END_AT - timedelta(days=b.EXPERIMENT_DAYS)
        for e in events:
            if e.kind == "order":
                order = e.payload[0]
                treated = order.customer.variant == "treatment" and order.created_at >= start
                assert order.discount == (b.DISCOUNT if treated else Decimal("0"))

    def test_assignment_not_before_experiment_start(self, events):
        start = END_AT - timedelta(days=b.EXPERIMENT_DAYS)
        assert all(e.ts >= start for e in events if e.kind == "assignment")

    def test_variant_is_deterministic(self):
        assert b.assign_variant(123) == b.assign_variant(123)

    def test_split_is_balanced(self):
        counts = Counter(b.assign_variant(i) for i in range(20_000))
        assert abs(counts["treatment"] / 20_000 - b.TREATMENT_SHARE) < 0.015


class TestLifecycle:
    CREATED = datetime(2026, 1, 1, tzinfo=UTC)

    def test_unpaid_order_is_only_cancelled(self):
        assert [s for _, s in b.lifecycle(self.CREATED, unpaid_prob=1.0)] == ["cancelled"]

    def test_paid_order_goes_through_stages_in_time_order(self):
        random.seed(1)
        for _ in range(200):
            steps = b.lifecycle(self.CREATED, unpaid_prob=0.0)
            assert steps[0][1] == "paid"
            assert steps[-1][1] in ("delivered", "cancelled")
            times = [self.CREATED] + [t for t, _ in steps]
            assert times == sorted(times)

    def test_unpaid_share_matches_probability(self):
        random.seed(2)
        n = 20_000
        cancelled = sum(b.lifecycle(self.CREATED, 0.12)[0][1] == "cancelled" for _ in range(n))
        assert abs(cancelled / n - 0.12) < 0.01


def test_poisson_mean_and_variance():
    random.seed(3)
    sample = [b.poisson(4.0) for _ in range(20_000)]
    mean = sum(sample) / len(sample)
    variance = sum((x - mean) ** 2 for x in sample) / len(sample)
    assert abs(mean - 4.0) < 0.1
    assert abs(variance - 4.0) < 0.2


def test_repeat_orders_stay_between_signup_and_end():
    random.seed(4)
    signup = END_AT - timedelta(days=90)
    for _ in range(300):
        for ts in b.repeat_order_times(signup, END_AT, lambda _: 1.0):
            assert signup < ts < END_AT


def test_boost_increases_repeat_orders():
    def total(boost):
        random.seed(5)
        signup = END_AT - timedelta(days=90)
        return sum(len(b.repeat_order_times(signup, END_AT, lambda _, x=boost: x)) for _ in range(4000))
    assert total(b.REPEAT_BOOST) / total(1.0) == pytest.approx(b.REPEAT_BOOST, abs=0.05)


class TestParseEndAt:
    def test_utc_suffix(self):
        assert b.parse_end_at("2026-10-29T12:00:00Z") == END_AT

    def test_garbage_is_rejected(self):
        with pytest.raises(ValueError):
            b.parse_end_at("вчера")
