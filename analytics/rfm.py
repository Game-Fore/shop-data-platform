"""RFM-сегментация покупателей.

Та же логика реализована в dbt (marts.mart_customer_rfm) - витрина для BI. Python-версия
нужна ноутбуку notebooks/04_rfm: выбрать правила, посчитать RFM на прошлую дату для
бэктеста и сверить результат с витриной.

Баллы 1-5, больше = лучше:
- R (давность последней покупки) и M (сумма покупок) - по квинтилям: группы равного размера,
  баллы не зависят от масштаба и не сдвигаются, если данные "устарели" на несколько дней;
- F (число покупок) - по фиксированным порогам: половина клиентов купила ровно один раз,
  квинтили F совпадают ([1, 1, 2, 3]) и равных групп не получается.
"""
import numpy as np
import pandas as pd

QUANTILES = [0.2, 0.4, 0.6, 0.8]
# F: 1 | 2 | 3-4 | 5-7 | 8+  ->  балл = 1 + число порогов, которые частота достигла
F_THRESHOLDS = [2, 3, 5, 8]

# порядок важен: первое подходящее правило задаёт сегмент
SEGMENTS = [
    ("Чемпионы", "покупают часто и недавно"),
    ("Лояльные", "покупают регулярно"),
    ("Новички", "купили недавно, но пока 1-2 раза"),
    ("Под угрозой", "покупали часто, но давно не возвращались"),
    ("Требуют внимания", "средняя давность, мало покупок"),
    ("Засыпают", "давно и мало"),
    ("Потерянные", "очень давно и мало"),
]
SEGMENT_ORDER = [name for name, _ in SEGMENTS]


def _count_above(values: np.ndarray, bounds: np.ndarray) -> np.ndarray:
    """Сколько границ значение строго превышает (значение на границе уходит в нижнюю группу)."""
    return (values[:, None] > bounds[None, :]).sum(axis=1)


def rfm_scores(df: pd.DataFrame) -> pd.DataFrame:
    """df: recency_days, frequency, monetary -> те же колонки + r_score, f_score, m_score, segment.

    Квантили - с линейной интерполяцией (np.quantile по умолчанию); в ClickHouse этому
    соответствует quantilesExactInclusive, поэтому границы в ноутбуке и в витрине совпадают.
    """
    recency = df.recency_days.to_numpy(dtype=float)
    frequency = df.frequency.to_numpy(dtype=float)
    monetary = df.monetary.to_numpy(dtype=float)

    r_bounds = np.quantile(recency, QUANTILES)
    m_bounds = np.quantile(monetary, QUANTILES)

    out = df.copy()
    out["r_score"] = 5 - _count_above(recency, r_bounds)          # меньше дней = лучше
    out["f_score"] = 1 + (frequency[:, None] >= np.array(F_THRESHOLDS)[None, :]).sum(axis=1)
    out["m_score"] = 1 + _count_above(monetary, m_bounds)
    out["segment"] = segment(out.r_score, out.f_score)
    return out


def segment(r: pd.Series, f: pd.Series) -> pd.Series:
    rules = [
        (r >= 4) & (f >= 4),
        (r >= 3) & (f >= 3),
        (r >= 4) & (f <= 2),
        (r <= 2) & (f >= 3),
        (r == 3) & (f <= 2),
        (r == 2) & (f <= 2),
    ]
    return pd.Series(np.select(rules, SEGMENT_ORDER[:-1], default=SEGMENT_ORDER[-1]), index=r.index)
