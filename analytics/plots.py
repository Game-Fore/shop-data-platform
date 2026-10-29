"""Единое оформление графиков matplotlib для всех ноутбуков.

Статичные картинки, а не plotly: ноутбуки коммитятся с выводом, и GitHub показывает
их прямо в браузере. Палитра - фиксированный порядок категориальных цветов
(проверена на различимость при нарушениях цветовосприятия); один график - одна ось Y.
"""
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, PercentFormatter

# категориальные цвета: назначаются по порядку, сущность сохраняет цвет между графиками
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
SERIES = [BLUE, ORANGE, AQUA, YELLOW]
GRAY = "#8a8984"          # второстепенные элементы: справочные линии, "прочее"
TEXT = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
SEQUENTIAL_CMAP = "Blues"  # величина: один оттенок от светлого к тёмному


def setup():
    plt.rcParams.update({
        "figure.figsize": (10, 4),
        "figure.dpi": 110,
        "axes.prop_cycle": plt.cycler(color=SERIES),
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.edgecolor": GRAY,
        "axes.labelcolor": TEXT_SECONDARY,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.grid": True,
        "axes.axisbelow": True,
        "grid.color": "#e4e3df",
        "grid.linewidth": 0.8,
        "xtick.color": TEXT_SECONDARY,
        "ytick.color": TEXT_SECONDARY,
        "lines.linewidth": 2,
        "legend.frameon": False,
        "font.size": 10,
    })


def money(x: float) -> str:
    """Сумма для подписи: 1 250 000 -> '1.25 млн', 45 000 -> '45 тыс.', 1 050 -> '1 050'.
    До 10 тыс. число пишется целиком: иначе соседние деления (1 000 и 1 050) сливаются в '1 тыс.'."""
    if abs(x) >= 1e6:
        return f"{x / 1e6:.2f} млн"
    if abs(x) >= 1e4:
        return f"{x / 1e3:.0f} тыс."
    return f"{x:,.0f}".replace(",", " ")


def money_axis(ax, axis: str = "y"):
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(FuncFormatter(lambda x, _: money(x)))


def percent_axis(ax, axis: str = "y", decimals: int = 0):
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(PercentFormatter(1.0, decimals=decimals))
