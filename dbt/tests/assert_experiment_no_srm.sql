-- Sample Ratio Mismatch: размеры групп должны соответствовать плану 50/50.
-- Статистика хи-квадрат для двух групп при ожидаемом 50/50: (n_a - n_b)^2 / (n_a + n_b).
-- Порог 10.83 = квантиль хи-квадрат(1) для p = 0.001: такой строгий уровень принят для SRM,
-- потому что проверка гоняется постоянно и ложная тревога останавливает анализ эксперимента.
-- SRM почти всегда означает поломку в назначении или логировании, а не случайность,
-- и результаты такого эксперимента нельзя интерпретировать.
select
    experiment_id,
    countIf(variant = 'control') as n_control,
    countIf(variant = 'treatment') as n_treatment,
    pow(n_control - n_treatment, 2) / (n_control + n_treatment) as chi2
from {{ ref('mart_ab_users') }}
group by experiment_id
having chi2 > 10.83
