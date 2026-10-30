"""Историческая нагрузка: клиенты и заказы за N дней до конца истории (end_at).

Идея: сначала в памяти строится вся история как список событий с бизнес-временем
(регистрация клиента, заказ, смена статуса, смена цены), затем события сортируются
по времени и пишутся в PostgreSQL обычными INSERT/UPDATE. Поэтому backfill идёт
через тот же CDC-пайплайн, что и живая нагрузка, а LSN растёт вместе со временем событий.

История отсчитывается от фиксированной даты end_at, а не от момента запуска: один seed и один
end_at дают одни и те же данные, поэтому числа в ноутбуках и README можно воспроизвести.

Модель поведения:
- новые клиенты приходят пуассоновским потоком с линейным ростом и недельной сезонностью;
- первый заказ - вскоре после регистрации (с вероятностью FIRST_ORDER_PROB);
- повторные заказы - неоднородный пуассоновский процесс с интенсивностью
  lambda_i * exp(-age / REPEAT_DECAY_DAYS): активность клиента затухает (это даёт
  реалистичный retention), а lambda_i ~ Gamma (клиенты сильно различаются - нужно для RFM);
- статусы идут new -> paid -> shipped -> delivered с отменами; переходы, которые
  "ещё не наступили" (позже end_at), не применяются - свежие заказы остаются в пути.

A/B-эксперимент (последние EXPERIMENT_DAYS дней): каждый клиент назначается в группу
детерминированным хэшем; в группе treatment заказы в окне эксперимента получают скидку,
реже остаются неоплаченными, а повторные покупки случаются чаще. Это "истинные" эффекты,
которые ноутбук notebooks/05_ab_test должен восстановить по данным.

Бизнес-время лежит в created_at/updated_at. _source_ts в ClickHouse - это время
загрузки через CDC, для аналитики backfill-данных его использовать нельзя.
"""
import hashlib
import logging
import math
import random
from collections.abc import Callable
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
# конец истории по умолчанию: начало истории и окно эксперимента считаются от него
DEFAULT_END_AT = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)

# A/B-эксперимент. Длительность должна совпадать с dbt/seeds/experiments.csv.
EXPERIMENT_ID = "discount_10"
EXPERIMENT_DAYS = 42
TREATMENT_SHARE = 0.5
UNPAID_PROB = 0.12                # доля заказов, которые так и не оплатили (вне эксперимента и в control)
# истинные эффекты для treatment в окне эксперимента
DISCOUNT = Decimal("0.10")        # цена ниже на 10%
TREATMENT_UNPAID_PROB = 0.08      # со скидкой чаще доходят до оплаты
REPEAT_BOOST = 1.15               # повторные покупки в 1.15 раза чаще

WEEKDAY_WEIGHT = [0.9, 0.85, 0.85, 0.9, 1.05, 1.25, 1.2]  # пн..вс
HOUR_WEIGHT = [1.0, 0.5, 0.3, 0.2, 0.2, 0.3, 0.6, 1.2, 2.0, 2.5, 3.0, 3.2,
               3.5, 3.4, 3.2, 3.0, 3.0, 3.2, 3.6, 4.0, 4.2, 3.8, 3.0, 2.0]


@dataclass
class Customer:
    created_at: datetime
    variant: str = "control"
    id: int | None = None


@dataclass
class Order:
    customer: Customer
    created_at: datetime
    lines: list[tuple[int, int]]  # (product_id, quantity); цена берётся на момент заказа
    discount: Decimal = Decimal("0")
    id: int | None = None


@dataclass(order=True)
class Event:
    ts: datetime
    seq: int                                  # стабильный порядок при равном ts
    kind: str = field(compare=False)          # customer | assignment | order | status | price
    payload: tuple = field(compare=False)


def parse_end_at(value: str) -> datetime:
    """ISO-дата или дата со временем; без таймзоны считается UTC."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def assign_variant(customer_key: int) -> str:
    """Детерминированное назначение в группу: хэш от (эксперимент, клиент), как в продакшене.
    Соль = id эксперимента, поэтому в разных экспериментах разбиения независимы.
    customer_key - порядковый номер клиента в симуляции: настоящий id выдаёт SERIAL только при записи."""
    h = int(hashlib.md5(f"{EXPERIMENT_ID}:{customer_key}".encode()).hexdigest(), 16)
    return "treatment" if h % 10_000 < TREATMENT_SHARE * 10_000 else "control"


def lifecycle(created: datetime, unpaid_prob: float = UNPAID_PROB) -> list[tuple[datetime, str]]:
    """Переходы статусов заказа после создания (без учёта end_at)."""
    if random.random() < unpaid_prob:  # не оплатили
        return [(created + hours(random.expovariate(1 / 20)), "cancelled")]
    paid = created + hours(random.expovariate(1 / 3))
    if random.random() < 0.04:  # отмена после оплаты, до отгрузки
        return [(paid, "paid"), (paid + hours(random.expovariate(1 / 24)), "cancelled")]
    shipped = paid + hours(random.uniform(12, 72))
    delivered = shipped + hours(random.uniform(48, 144))
    return [(paid, "paid"), (shipped, "shipped"), (delivered, "delivered")]


def repeat_order_times(
    signup: datetime, end_at: datetime, boost: Callable[[datetime], float]
) -> list[datetime]:
    """Повторные заказы: интенсивность lambda_i * exp(-t / tau) * boost(t) на [signup, end_at].

    boost(t) in [1, REPEAT_BOOST] - эффект эксперимента. Реализован прореживанием: процесс
    генерируется с интенсивностью, умноженной на максимум REPEAT_BOOST, и каждый заказ
    остаётся с вероятностью boost(t) / REPEAT_BOOST. Так получается точный процесс с нужной
    интенсивностью, а случайные числа для control и treatment тратятся одинаково.
    """
    tau = REPEAT_DECAY_DAYS
    horizon = (end_at - signup).total_seconds() / 86400
    lam_i = random.gammavariate(REPEAT_SHAPE, REPEAT_MEAN / REPEAT_SHAPE) / tau
    mass = 1 - math.exp(-horizon / tau)       # доля "активности", попадающая в окно
    times = []
    for _ in range(poisson(lam_i * tau * mass * REPEAT_BOOST)):
        # обратная функция распределения усечённой экспоненты на [0, horizon]
        offset_days = -tau * math.log(1 - random.random() * mass)
        day = signup + timedelta(days=offset_days)
        # недельная сезонность прореживанием, время суток - по профилю HOUR_WEIGHT
        if random.random() > WEEKDAY_WEIGHT[day.weekday()] / max(WEEKDAY_WEIGHT):
            continue
        ts = at_random_hour(day)
        if not signup < ts < end_at:
            continue
        if random.random() < boost(ts) / REPEAT_BOOST:
            times.append(ts)
    return times


def build_events(days: int, product_ids: list[int], end_at: datetime) -> list[Event]:
    start = end_at - timedelta(days=days)
    experiment_start = max(start, end_at - timedelta(days=EXPERIMENT_DAYS))
    events: list[Event] = []

    def add(ts, kind, *payload):
        events.append(Event(ts, len(events), kind, payload))

    def treated(customer: Customer, ts: datetime) -> bool:
        return customer.variant == "treatment" and ts >= experiment_start

    def add_order(customer: Customer, ts: datetime):
        picked = random.sample(product_ids, random.randint(1, min(4, len(product_ids))))
        is_treated = treated(customer, ts)
        order = Order(customer, ts, [(pid, random.randint(1, 3)) for pid in picked],
                      discount=DISCOUNT if is_treated else Decimal("0"))
        add(ts, "order", order)
        unpaid_prob = TREATMENT_UNPAID_PROB if is_treated else UNPAID_PROB
        for status_ts, status in lifecycle(ts, unpaid_prob):
            if status_ts < end_at:
                add(status_ts, "status", order, status)

    customer_key = 0
    for d in range(days):
        day = start + timedelta(days=d)
        growth = GROWTH[0] + (GROWTH[1] - GROWTH[0]) * d / max(days - 1, 1)
        lam = NEW_CUSTOMERS_PER_DAY * growth * WEEKDAY_WEIGHT[day.weekday()]
        for _ in range(poisson(lam)):
            signup = at_random_hour(day)
            if signup >= end_at:
                continue
            customer_key += 1
            customer = Customer(signup, variant=assign_variant(customer_key))
            add(signup, "customer", customer)
            # участвуют все клиенты: зарегистрированные раньше назначаются в момент старта
            # эксперимента, новые - при регистрации (intention-to-treat)
            add(max(signup, experiment_start), "assignment", customer)
            if random.random() < FIRST_ORDER_PROB:
                first = signup + timedelta(minutes=5 + random.expovariate(1 / 30))
                if first < end_at:
                    add_order(customer, first)
            boost = lambda ts, c=customer: REPEAT_BOOST if treated(c, ts) else 1.0  # noqa: E731
            for ts in repeat_order_times(signup, end_at, boost):
                add_order(customer, ts)

    for pid in product_ids:
        ts = start + timedelta(days=random.expovariate(1 / PRICE_CHANGE_EVERY_DAYS))
        while ts < end_at:
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

    elif ev.kind == "assignment":
        (customer,) = ev.payload
        cur.execute(
            "INSERT INTO experiment_assignments (experiment_id, customer_id, variant, assigned_at) "
            "VALUES (%s, %s, %s, %s)",
            (EXPERIMENT_ID, customer.id, customer.variant, ev.ts),
        )

    elif ev.kind == "order":
        (order,) = ev.payload
        # скидка эксперимента уже в цене позиции: как в реальном магазине, где в заказ
        # попадает итоговая цена
        lines = [(pid, qty, (prices[pid] * (1 - order.discount)).quantize(Decimal("0.01")))
                 for pid, qty in order.lines]
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


def run_backfill(conn, days: int, seed: int, end_at: datetime = DEFAULT_END_AT):
    random.seed(seed)
    fake = Faker("ru_RU")
    fake.seed_instance(seed)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM orders")
        if cur.fetchone()[0] > 0:
            raise SystemExit("backfill: в orders уже есть данные. Сначала сбросьте стенд (reset.ps1).")
        cur.execute("SELECT id, price FROM products ORDER BY id")
        prices = dict(cur.fetchall())

    events = build_events(days, list(prices), end_at)
    counts = {k: sum(e.kind == k for e in events)
              for k in ("customer", "assignment", "order", "status", "price")}
    log.info("backfill %d days to %s (seed=%d): %s", days, end_at.isoformat(), seed, counts)

    for i in range(0, len(events), BATCH_SIZE):
        with conn.transaction(), conn.cursor() as cur:
            for ev in events[i:i + BATCH_SIZE]:
                apply(cur, ev, prices, fake)
        done = min(i + BATCH_SIZE, len(events))
        if done == len(events) or (i // BATCH_SIZE) % 20 == 0:
            log.info("backfill: %d / %d events (up to %s)", done, len(events), events[done - 1].ts.date())
    log.info("backfill done")
