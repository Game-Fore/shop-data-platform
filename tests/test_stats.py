import numpy as np
import pytest
from scipy import stats as st

from analytics import stats as ab


@pytest.fixture
def rng():
    return np.random.default_rng(0)


class TestDesign:
    def test_mde_proportion_known_value(self):
        # (1.96 + 0.8416) * sqrt(2 * 0.5 * 0.5 / 1000)
        assert ab.mde_proportion(0.5, 1000) == pytest.approx(0.06265, abs=1e-4)

    def test_mde_shrinks_with_sqrt_n(self):
        assert ab.mde_mean(10, 400) == pytest.approx(ab.mde_mean(10, 100) / 2)

    def test_power_at_mde_is_nominal(self):
        assert ab.power_mean(ab.mde_mean(10, 500), 10, 500) == pytest.approx(0.8, abs=1e-3)

    def test_power_without_effect_equals_alpha(self):
        assert ab.power_mean(0, 10, 500) == pytest.approx(0.05, abs=1e-3)


class TestSrm:
    def test_balanced(self):
        assert ab.srm_test(500, 500) == pytest.approx((0.0, 1.0))

    def test_skewed_known_value(self):
        chi2, p = ab.srm_test(450, 550)
        assert chi2 == pytest.approx(10.0)
        assert p == pytest.approx(0.00157, abs=1e-4)

    def test_custom_share(self):
        assert ab.srm_test(800, 200, expected_treatment_share=0.2)[0] == pytest.approx(0.0)


class TestProportion:
    def test_direction_is_treatment_minus_control(self):
        e = ab.proportion_test(400, 1000, 450, 1000)
        assert e.diff == pytest.approx(0.05)
        assert e.ci_low < 0.05 < e.ci_high
        assert e.significant

    def test_no_difference(self):
        e = ab.proportion_test(400, 1000, 400, 1000)
        assert e.diff == 0
        assert not e.significant
        assert e.p_value == pytest.approx(1.0)


class TestWelch:
    def test_matches_scipy(self, rng):
        a, b = rng.exponential(100, 500), rng.exponential(110, 700)
        e = ab.welch_test(a, b)
        assert e.p_value == pytest.approx(st.ttest_ind(b, a, equal_var=False).pvalue)
        assert e.diff == pytest.approx(b.mean() - a.mean())

    def test_rel_uses_control_as_base(self):
        e = ab.welch_test(np.array([9.0, 10.0, 11.0]), np.array([10.0, 11.0, 12.0]))
        assert e.rel == pytest.approx(0.1)


def test_bootstrap_agrees_with_welch(rng):
    a, b = rng.normal(100, 20, 2000), rng.normal(103, 20, 2000)
    boot, welch = ab.bootstrap_test(a, b, n_boot=2000), ab.welch_test(a, b)
    assert boot.diff == pytest.approx(welch.diff)
    assert boot.ci_low == pytest.approx(welch.ci_low, abs=0.3)
    assert boot.ci_high == pytest.approx(welch.ci_high, abs=0.3)


def test_bootstrap_is_reproducible(rng):
    a, b = rng.normal(0, 1, 200), rng.normal(0, 1, 200)
    assert ab.bootstrap_test(a, b, n_boot=500).ci_low == ab.bootstrap_test(a, b, n_boot=500).ci_low


class TestCuped:
    def test_variance_reduction_is_one_minus_rho_squared(self, rng):
        y = rng.normal(0, 1, 5000)
        x = 0.6 * y + rng.normal(0, 0.8, 5000)
        adjusted, _ = ab.cuped(y, x)
        rho = np.corrcoef(x, y)[0, 1]
        assert adjusted.var() / y.var() == pytest.approx(1 - rho ** 2)

    def test_keeps_mean_and_effect(self):
        # ковариата измерена до воздействия: эффект несмещён. Несмещённость - свойство среднего
        # по выборкам, поэтому усредняем по seed: на одном seed отклонение в 3 сигмы возможно.
        effects = []
        for seed in range(50):
            rng = np.random.default_rng(seed)
            x = rng.normal(0, 1, 5000)
            group = rng.random(5000) < 0.5
            y = x + rng.normal(0, 1, 5000) + 0.5 * group
            adjusted, _ = ab.cuped(y, x)
            assert adjusted.mean() == pytest.approx(y.mean())
            effects.append(adjusted[group].mean() - adjusted[~group].mean())
        assert np.mean(effects) == pytest.approx(0.5, abs=0.015)

    def test_useless_covariate_changes_nothing(self, rng):
        y, x = rng.normal(0, 1, 20_000), rng.normal(0, 1, 20_000)
        assert ab.cuped(y, x)[0].var() == pytest.approx(y.var(), rel=0.01)


class TestRatio:
    def test_point_estimate_is_ratio_of_sums(self):
        e = ab.ratio_test(np.array([10.0, 30.0]), np.array([1.0, 2.0]), np.array([20.0, 20.0]), np.array([1.0, 1.0]))
        assert e.control == pytest.approx(40 / 3)
        assert e.treatment == pytest.approx(20.0)

    def test_se_matches_bootstrap(self, rng):
        n = rng.poisson(1.5, 3000).astype(float)
        revenue = rng.gamma(2, 50, 3000) * n
        n2 = rng.poisson(1.5, 3000).astype(float)
        revenue2 = rng.gamma(2, 50, 3000) * n2
        e = ab.ratio_test(revenue, n, revenue2, n2)
        delta_se = (e.ci_high - e.ci_low) / (2 * 1.96)

        def ratio(r, d, idx):
            return r[idx].sum() / d[idx].sum()
        boots = [ratio(revenue2, n2, rng.integers(0, 3000, 3000)) - ratio(revenue, n, rng.integers(0, 3000, 3000))
                 for _ in range(1000)]
        assert delta_se == pytest.approx(np.std(boots), rel=0.1)

    def test_false_positive_rate_is_nominal(self, rng):
        hits = 0
        for _ in range(300):
            n = rng.poisson(1.5, (2, 1000)).astype(float)
            revenue = rng.gamma(2, 50, (2, 1000)) * n
            hits += ab.ratio_test(revenue[0], n[0], revenue[1], n[1]).p_value < 0.05
        assert 0.01 < hits / 300 < 0.10
