"""Статистика для A/B-тестов: дизайн (MDE), проверки (SRM), тесты и доверительные интервалы.

Соглашения: группа A - control, B - treatment; эффект = B - A; тесты двусторонние.
Все функции принимают пользовательские значения (одна строка = один участник эксперимента),
потому что рандомизировали пользователей: наблюдения независимы именно на этом уровне.
"""
from dataclasses import dataclass

import numpy as np
from scipy import stats
from statsmodels.stats.proportion import confint_proportions_2indep, test_proportions_2indep


@dataclass
class Effect:
    metric: str
    control: float
    treatment: float
    diff: float          # treatment - control
    ci_low: float        # доверительный интервал для diff
    ci_high: float
    p_value: float

    @property
    def rel(self) -> float:
        return self.diff / self.control

    @property
    def rel_ci(self) -> tuple[float, float]:
        # относительный ДИ как ДИ разности, делённый на контроль: приближение, игнорирующее
        # неопределённость самого контроля (при n в тысячи это мелочь); точнее - бутстреп
        return self.ci_low / self.control, self.ci_high / self.control

    @property
    def significant(self) -> bool:
        return self.ci_low > 0 or self.ci_high < 0


# ---------------------------------------------------------------- дизайн

def _z(alpha: float, power: float) -> float:
    return stats.norm.ppf(1 - alpha / 2) + stats.norm.ppf(power)


def mde_proportion(p: float, n_per_group: int, alpha: float = 0.05, power: float = 0.8) -> float:
    """Минимальный обнаружимый эффект для доли (абсолютный, в долях)."""
    return _z(alpha, power) * np.sqrt(2 * p * (1 - p) / n_per_group)


def mde_mean(std: float, n_per_group: int, alpha: float = 0.05, power: float = 0.8) -> float:
    """Минимальный обнаружимый эффект для среднего (абсолютный, в единицах метрики)."""
    return _z(alpha, power) * std * np.sqrt(2 / n_per_group)


def power_mean(effect: float, std: float, n_per_group: int, alpha: float = 0.05) -> float:
    """Мощность двустороннего z-теста для разности средних при заданном истинном эффекте."""
    se = std * np.sqrt(2 / n_per_group)
    z = stats.norm.ppf(1 - alpha / 2)
    return stats.norm.cdf(abs(effect) / se - z) + stats.norm.cdf(-abs(effect) / se - z)


# ---------------------------------------------------------------- проверки

def srm_test(n_control: int, n_treatment: int, expected_treatment_share: float = 0.5) -> tuple[float, float]:
    """Sample Ratio Mismatch: хи-квадрат согласия размеров групп с планом. Возвращает (chi2, p)."""
    total = n_control + n_treatment
    expected = [total * (1 - expected_treatment_share), total * expected_treatment_share]
    chi2, p = stats.chisquare([n_control, n_treatment], expected)
    return float(chi2), float(p)


# ---------------------------------------------------------------- тесты

def proportion_test(x_a: int, n_a: int, x_b: int, n_b: int, metric: str = "conversion",
                    alpha: float = 0.05) -> Effect:
    """Разность долей: z-тест (score) и интервал Ньюкомба.

    Ньюкомб строится из интервалов Уилсона для каждой доли и, в отличие от интервала Вальда,
    сохраняет номинальное покрытие при долях, близких к 0 или 1, и небольших выборках.
    """
    low, high = confint_proportions_2indep(x_b, n_b, x_a, n_a, method="newcomb", compare="diff", alpha=alpha)
    p_value = test_proportions_2indep(x_b, n_b, x_a, n_a, method="score", compare="diff").pvalue
    p_a, p_b = x_a / n_a, x_b / n_b
    return Effect(metric, p_a, p_b, p_b - p_a, float(low), float(high), float(p_value))


def welch_test(a: np.ndarray, b: np.ndarray, metric: str = "mean", alpha: float = 0.05) -> Effect:
    """Разность средних, t-тест Уэлча (без предположения о равных дисперсиях).

    Нормальность самих данных не нужна: при n в тысячи по ЦПТ нормально распределено среднее.
    Проверить, что на тяжёлом хвосте этого достаточно, можно A/A-симуляцией (см. ноутбук 05).
    """
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    va, vb = a.var(ddof=1) / len(a), b.var(ddof=1) / len(b)
    se = np.sqrt(va + vb)
    df = (va + vb) ** 2 / (va ** 2 / (len(a) - 1) + vb ** 2 / (len(b) - 1))
    diff = b.mean() - a.mean()
    t_crit = stats.t.ppf(1 - alpha / 2, df)
    p_value = 2 * stats.t.sf(abs(diff / se), df)
    return Effect(metric, a.mean(), b.mean(), diff, diff - t_crit * se, diff + t_crit * se, float(p_value))


def bootstrap_test(a: np.ndarray, b: np.ndarray, metric: str = "mean", n_boot: int = 10_000,
                   alpha: float = 0.05, seed: int = 42) -> Effect:
    """Разность средних с перцентильным бутстреп-интервалом (пользователи ресемплируются
    внутри каждой группы). p-value - доля бутстреп-разностей по другую сторону от нуля, x2."""
    rng = np.random.default_rng(seed)
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    diffs = np.empty(n_boot)
    chunk = 500  # блоками: матрица индексов n_boot x n целиком заняла бы сотни МБ
    for start in range(0, n_boot, chunk):
        k = min(chunk, n_boot - start)
        diffs[start:start + k] = (b[rng.integers(0, len(b), (k, len(b)))].mean(axis=1)
                                  - a[rng.integers(0, len(a), (k, len(a)))].mean(axis=1))
    low, high = np.quantile(diffs, [alpha / 2, 1 - alpha / 2])
    p_value = min(1.0, 2 * min((diffs <= 0).mean(), (diffs >= 0).mean()))
    return Effect(metric, a.mean(), b.mean(), b.mean() - a.mean(), float(low), float(high), float(p_value))


def cuped(y: np.ndarray, x: np.ndarray) -> tuple[np.ndarray, float]:
    """CUPED: y' = y - theta * (x - mean(x)), theta = cov(x, y) / var(x).

    x - ковариата, измеренная ДО эксперимента (например, выручка за предпериод), поэтому
    от воздействия не зависит, и среднее y' в каждой группе остаётся несмещённой оценкой
    эффекта. Дисперсия падает в (1 - corr(x, y)^2) раз. theta считается по обеим группам вместе.
    """
    y, x = np.asarray(y, dtype=float), np.asarray(x, dtype=float)
    theta = np.cov(x, y, ddof=1)[0, 1] / x.var(ddof=1)
    return y - theta * (x - x.mean()), float(theta)


def _ratio_and_var(num: np.ndarray, den: np.ndarray) -> tuple[float, float]:
    """Отношение сумм R = sum(num) / sum(den) и дисперсия его оценки по delta-методу."""
    n = len(num)
    mu_n, mu_d = num.mean(), den.mean()
    ratio = mu_n / mu_d
    var_n, var_d = num.var(ddof=1), den.var(ddof=1)
    cov = np.cov(num, den, ddof=1)[0, 1]
    var_ratio = (var_n - 2 * ratio * cov + ratio ** 2 * var_d) / (mu_d ** 2 * n)
    return ratio, var_ratio


def ratio_test(num_a: np.ndarray, den_a: np.ndarray, num_b: np.ndarray, den_b: np.ndarray,
               metric: str = "ratio", alpha: float = 0.05) -> Effect:
    """Разность метрик-отношений (средний чек = выручка / покупки) по delta-методу.

    Единица рандомизации - пользователь, а средний чек считается по заказам: заказы одного
    клиента зависимы, поэтому обычный t-тест по заказам занижает дисперсию и даёт ложную
    значимость. Delta-метод считает дисперсию отношения из пользовательских сумм.
    """
    arr = [np.asarray(v, dtype=float) for v in (num_a, den_a, num_b, den_b)]
    r_a, v_a = _ratio_and_var(arr[0], arr[1])
    r_b, v_b = _ratio_and_var(arr[2], arr[3])
    diff, se = r_b - r_a, np.sqrt(v_a + v_b)
    z = stats.norm.ppf(1 - alpha / 2)
    p_value = 2 * stats.norm.sf(abs(diff / se))
    return Effect(metric, r_a, r_b, diff, diff - z * se, diff + z * se, float(p_value))
