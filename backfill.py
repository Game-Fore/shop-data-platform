"""Историческая нагрузка: клиенты и заказы за последние N дней.

Идея: сначала в памяти строится вся история как список событий с бизнес-временем
(регистрация клиента, заказ, смена статуса, смена цены), затем события сортируются
по времени и пишутся в PostgreSQL обычными INSERT/UPDATE. Поэтому backfill идёт
через тот же CDC-пайплайн, что и живая нагрузка, а LSN растёт вместе со временем событий.

Модель поведения:
- новые клиенты приходят пуассоновским потоком с линейным ростом и недельной сезонностью;
- первый заказ - вскоре после регистрации (с вероятностью FIRST_ORDER_PROB);
- повторные заказы - неоднородный пуассоновский процесс с интенсивностью
  lambda_i * exp(-age / REPEAT_DECAY_DAYS): активность клиента затухает (это даёт
  реалистичный retention), а lambda_i ~ Gamma (клиенты сильно различаются - нужно для RFM);
- статусы идут new -> paid -> shipped -> delivered с отменами; переходы, которые
  "ещё не наступили" (позже now), не применяются - свежие заказы остаются в пути.

Бизнес-время лежит в created_at/updated_at. _source_ts в ClickHouse - это время
загрузки через CDC, для аналитики backfill-данных его использовать нельзя.
"""
import logging
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from faker import Faker

log = logging.getLogger("backfill")

NEW_CUSTOMERS_PER_DAY = 38        # среднее за период; итого ~6800 клиентов и ~16k заказов за 180 дней
GROWTH = (0.7, 1.3)               # множитель потока клиентов: начало -> конец периода
FIRST_ORDER_PROB = 0.85
REPEAT_MEAN = 3.0                 # ожидаемое число повторных заказов на бесконечном горизонте
REPEAT_SHAPE = 0.6                # форма Gamma: < 1 -> много "разовых" и немного лояльных
REPEAT_DECAY_DAYS = 60.0          # характерное время затухания активности клиента
PRICE_CHANGE_EVERY_DAYS = 14.0    # в среднем раз в две недели на товар
BATCH_SIZE = 500                  # событий на транзакцию

WEEKDAY_WEIGHT = [0.9, 0.85, 0.85, 0.9, 1.05, 1.25, 1.2]  # пн..вс
HOUR_WEIGHT = [1.0, 0.5, 0.3, 0.2, 0.2, 0.3, 0.6, 1.2, 2.0, 2.5, 3.0, 3.2,
               3.5, 3.4, 3.2, 3.0, 3.0, 3.2, 3.6, 4.0, 4.2, 3.8, 3.0, 2.0]


@dataclass
class Customer:
    created_at: datetime
    id: int | None = None


@dataclass
class Order:
    customer: Customer
    created_at: datetime
    lines: list[tuple[int, int]]  # (product_id, quantity); цена берётся на момент заказа
    id: int | None = None


@dataclass(order=True)
class Event:
    ts: datetime
    seq: int                                  # стабильный порядок при равном ts
    kind: str = field(compare=False)          # customer | order | status | price
    payload: tuple = field(compare=False)


def poisson(lam: float) -> int:
    # алгоритм Кнута; для lam порядка десятков точности double хватает
    threshold, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= random.random()
        if p <= threshold:
            return k
        k += 1


def at_random_hour(day: datetime) -> datetime:
    hour = random.choices(range(24), HOUR_WEIGHT)[0]
    return day.replace(hour=hour, minute=0, second=0, microsecond=0) + timedelta(
        seconds=random.uniform(0, 3600)
    )


def hours(h: float) -> timedelta:
    return timedelta(hours=h)


def lifecycle(created: datetime) -> list[tuple[datetime, str]]:
    """Переходы статусов заказа после создания (без учёта now)."""
    if random.random() < 0.12:  # не оплатили
        return [(created + hours(random.expovariate(1 / 20)), "cancelled")]
    paid = created + hours(random.expovariate(1 / 3))
    if random.random() < 0.04:  # отмена после оплаты, до отгрузки
        return [(paid, "paid"), (paid + hours(random.expovariate(1 / 24)), "cancelled")]
    shipped = paid + hours(random.uniform(12, 72))
    delivered = shipped + hours(random.uniform(48, 144))
    return [(paid, "paid"), (shipped, "shipped"), (delivered, "delivered")]


def repeat_order_times(signup: datetime, now: datetime) -> list[datetime]:
    """Повторные заказы: интенсивность lambda_i * exp(-t / tau) на [signup, now]."""
    tau = REPEAT_DECAY_DAYS
    horizon = (now - signup).total_seconds() / 86400
    lam_i = random.gammavariate(REPEAT_SHAPE, REPEAT_MEAN / REPEAT_SHAPE) / tau
    mass = 1 - math.exp(-horizon / tau)       # доля "активности", попадающая в окно
    times = []
    for _ in range(poisson(lam_i * tau * mass)):
        # обратная функция распределения усечённой экспоненты на [0, horizon]
        offset_days = -tau * math.log(1 - random.random() * mass)
        day = signup + timedelta(days=offset_days)
        # недельная сезонность прореживанием, время суток - по профилю HOUR_WEIGHT
        if random.random() > WEEKDAY_WEIGHT[day.weekday()] / max(WEEKDAY_WEIGHT):
            continue
        ts = at_random_hour(day)
        if signup < ts < now:
            times.append(ts)
    return times


def build_events(days: int, product_ids: list[int], now: datetime) -> list[Event]:
    start = now - timedelta(days=days)
    events: list[Event] = []

    def add(ts, kind, *payload):
        events.append(Event(ts, len(events), kind, payload))

    def add_order(customer: Customer, ts: datetime):
        picked = random.sample(product_ids, random.randint(1, min(4, len(product_ids))))
        order = Order(customer, ts, [(pid, random.randint(1, 3)) for pid in picked])
        add(ts, "order", order)
        for status_ts, status in lifecycle(ts):
            if status_ts < now:
                add(status_ts, "status", order, status)

    for d in range(days):
        day = start + timedelta(days=d)
        growth = GROWTH[0] + (GROWTH[1] - GROWTH[0]) * d / max(days - 1, 1)
        lam = NEW_CUSTOMERS_PER_DAY * growth * WEEKDAY_WEIGHT[day.weekday()]
        for _ in range(poisson(lam)):
            signup = at_random_hour(day)
            if signup >= now:
                continue
            customer = Customer(signup)
            add(signup, "customer", customer)
            if random.random() < FIRST_ORDER_PROB:
                first = signup + timedelta(minutes=5 + random.expovariate(1 / 30))
                if first < now:
                    add_order(customer, first)
            for ts in repeat_order_times(signup, now):
                add_order(customer, ts)

    for pid in product_ids:
        ts = start + timedelta(days=random.expovariate(1 / PRICE_CHANGE_EVERY_DAYS))
        while ts < now:
            add(ts, "price", pid, random.uniform(0.9, 1.1))
            ts += timedelta(days=random.expovariate(1 / PRICE_CHANGE_EVERY_DAYS))

    events.sort()
    return events


def apply(cur, ev: Event, prices: dict[int, Decimal], fake: Faker):
    if ev.kind == "customer":
        (customer,) = ev.payload
        while customer.id is None:  # email уникален: при редкой коллизии берём другой
            email = f"{fake.user_name()}{random.randint(1000, 999999)}@{fake.free_email_domain()}"
            cur.execute(
                "INSERT INTO customers (email, full_name, city, created_at) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (email) DO NOTHING RETURNING id",
                (email, fake.name(), fake.city(), customer.created_at),
            )
            row = cur.fetchone()
            customer.id = row[0] if row else None

    elif ev.kind == "order":
        (order,) = ev.payload
        lines = [(pid, qty, prices[pid]) for pid, qty in order.lines]
        total = sum((price * qty for _, qty, price in lines), Decimal("0"))
        # в отличие от живого генератора total пишется сразу: один INSERT вместо c -> u
        cur.execute(
            "INSERT INTO orders (customer_id, status, total, created_at, updated_at) "
            "VALUES (%s, 'new', %s, %s, %s) RETURNING id",
            (order.customer.id, total, order.created_at, order.created_at),
        )
        order.id = cur.fetchone()[0]
        cur.executemany(
            "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)",
            [(order.id, pid, qty, price) for pid, qty, price in lines],
        )

    elif ev.kind == "status":
        order, status = ev.payload
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = %s WHERE id = %s",
            (status, ev.ts, order.id),
        )

    elif ev.kind == "price":
        pid, factor = ev.payload
        prices[pid] = (prices[pid] * Decimal(str(factor))).quantize(Decimal("0.01"))
        cur.execute("UPDATE products SET price = %s WHERE id = %s", (prices[pid], pid))


def run_backfill(conn, days: int, seed: int):
    random.seed(seed)
    fake = Faker("ru_RU")
    fake.seed_instance(seed)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM orders")
        if cur.fetchone()[0] > 0:
            raise SystemExit("backfill: в orders уже есть данные. Сначала сбросьте стенд (reset.ps1).")
        cur.execute("SELECT id, price FROM products ORDER BY id")
        prices = dict(cur.fetchall())

    now = datetime.now(timezone.utc)
    events = build_events(days, list(prices), now)
    counts = {k: sum(e.kind == k for e in events) for k in ("customer", "order", "status", "price")}
    log.info("backfill %d days (seed=%d): %s", days, seed, counts)

    for i in range(0, len(events), BATCH_SIZE):
        with conn.transaction(), conn.cursor() as cur:
            for ev in events[i:i + BATCH_SIZE]:
                apply(cur, ev, prices, fake)
        done = min(i + BATCH_SIZE, len(events))
        if done == len(events) or (i // BATCH_SIZE) % 20 == 0:
            log.info("backfill: %d / %d events (up to %s)", done, len(events), events[done - 1].ts.date())
    log.info("backfill done")
