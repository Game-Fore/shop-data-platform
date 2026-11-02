import numpy as np
import pandas as pd
import pytest

from analytics.rfm import F_THRESHOLDS, SEGMENT_ORDER, rfm_scores, segment


def customers(n=100):
    return pd.DataFrame({
        "customer_id": range(n),
        "recency_days": np.arange(n),
        "frequency": np.ones(n),
        "monetary": np.arange(n, dtype=float) + 1,
    })


class TestScores:
    def test_equal_quintiles(self):
        scored = rfm_scores(customers())
        assert set(scored.r_score.value_counts()) == {20}
        assert set(scored.m_score.value_counts()) == {20}

    def test_recent_is_better_and_rich_is_better(self):
        scored = rfm_scores(customers())
        assert scored.r_score.iloc[0] == 5 and scored.r_score.iloc[-1] == 1
        assert scored.m_score.iloc[0] == 1 and scored.m_score.iloc[-1] == 5

    @pytest.mark.parametrize("frequency, expected", [(1, 1), (2, 2), (3, 3), (4, 3), (5, 4), (7, 4), (8, 5), (29, 5)])
    def test_frequency_thresholds(self, frequency, expected):
        df = customers(10).assign(frequency=float(frequency))
        assert (rfm_scores(df).f_score == expected).all()

    def test_frequency_quintiles_would_degenerate(self):
        # причина фиксированных порогов: половина клиентов купила один раз
        frequency = np.array([1] * 50 + [2] * 20 + [3] * 15 + [5] * 10 + [9] * 5)
        assert len(set(np.quantile(frequency, [0.2, 0.4]))) == 1
        assert F_THRESHOLDS == [2, 3, 5, 8]

    def test_value_on_boundary_goes_to_lower_group(self):
        # строгое >, как в dbt-витрине mart_customer_rfm: иначе реализации разойдутся.
        # Квинтиль 0.2 от [10, 20, ..., 60] с линейной интерполяцией равен ровно 20
        values = [10.0, 20, 30, 40, 50, 60]
        assert np.quantile(values, 0.2) == 20
        scored = rfm_scores(pd.DataFrame({"recency_days": values, "frequency": [1.0] * 6, "monetary": values}))
        assert scored.m_score.iloc[1] == 1
        assert scored.r_score.iloc[1] == 5

    def test_recency_shift_does_not_change_scores(self):
        # данные "устарели" на неделю: давность выросла у всех одинаково, баллы те же
        base = rfm_scores(customers())
        shifted = rfm_scores(customers().assign(recency_days=lambda d: d.recency_days + 7))
        assert (base.r_score == shifted.r_score).all()

    def test_input_is_not_modified(self):
        df = customers()
        rfm_scores(df)
        assert "segment" not in df


class TestSegments:
    @pytest.mark.parametrize("r, f, expected", [
        (5, 5, "Чемпионы"), (4, 4, "Чемпионы"),
        (3, 3, "Лояльные"), (5, 3, "Лояльные"), (3, 5, "Лояльные"),
        (5, 1, "Новички"), (4, 2, "Новички"),
        (2, 5, "Под угрозой"), (1, 3, "Под угрозой"),
        (3, 1, "Требуют внимания"), (3, 2, "Требуют внимания"),
        (2, 1, "Засыпают"), (2, 2, "Засыпают"),
        (1, 1, "Потерянные"), (1, 2, "Потерянные"),
    ])
    def test_rules(self, r, f, expected):
        assert segment(pd.Series([r]), pd.Series([f])).iloc[0] == expected

    def test_every_score_pair_gets_a_known_segment(self):
        pairs = [(r, f) for r in range(1, 6) for f in range(1, 6)]
        result = segment(pd.Series([p[0] for p in pairs]), pd.Series([p[1] for p in pairs]))
        assert set(result) <= set(SEGMENT_ORDER)
        assert set(result) == set(SEGMENT_ORDER)  # и пустых сегментов в правилах нет
