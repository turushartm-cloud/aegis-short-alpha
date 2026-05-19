"""
M1: S/R Clustering — кластеризация ключевых уровней поддержки/сопротивления.

Алгоритм:
  1. Собирает swing highs и swing lows из OHLCV
  2. Кластеризует уровни методом K-Median (без numpy)
  3. Оценивает силу каждого кластера (кол-во касаний + объём)
  4. Возвращает score бонус за ближайший сильный кластер у цены

SHORT: сильный кластер-сопротивление ВЫШЕ цены → бонус +8/+12
LONG:  сильный кластер-поддержка НИЖЕ цены   → бонус +8/+12

ENV:
  SR_CLUSTER_PCT       = 1.0   % для объединения уровней в кластер
  SR_PROXIMITY_PCT     = 2.5   % близости к цене для триггера
  SR_MIN_TOUCHES       = 2     мин касаний уровня для валидности
  SR_SWING_LOOKBACK    = 5     свечей для определения pivot
  SR_BONUS_STRONG      = 12    бонус за кластер с 3+ касаниями
  SR_BONUS_NORMAL      = 8     бонус за кластер с 2 касаниями
"""
import os
import logging
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)

_CLUSTER_PCT    = float(os.getenv("SR_CLUSTER_PCT",     "1.0"))
_PROXIMITY_PCT  = float(os.getenv("SR_PROXIMITY_PCT",   "2.5"))
_MIN_TOUCHES    = int(os.getenv("SR_MIN_TOUCHES",        "2"))
_SWING_LB       = int(os.getenv("SR_SWING_LOOKBACK",     "5"))
_BONUS_STRONG   = int(os.getenv("SR_BONUS_STRONG",      "12"))
_BONUS_NORMAL   = int(os.getenv("SR_BONUS_NORMAL",       "8"))


def _collect_pivots(candles, lookback: int) -> Tuple[List[float], List[float]]:
    """Возвращает (swing_highs, swing_lows) из candles."""
    highs, lows = [], []
    n = len(candles)
    for i in range(lookback, n - lookback):
        h = candles[i].high
        lo = candles[i].low
        if all(h >= candles[j].high for j in range(i - lookback, i + lookback + 1) if j != i):
            highs.append(h)
        if all(lo <= candles[j].low for j in range(i - lookback, i + lookback + 1) if j != i):
            lows.append(lo)
    return highs, lows


def _kmedian_clusters(levels: List[float], cluster_pct: float) -> List[Tuple[float, int]]:
    """
    Простая кластеризация: группирует уровни в пределах cluster_pct%.
    Итеративно сортирует и объединяет соседние уровни.
    Возвращает [(median_price, count), ...] отсортированные по убыванию count.
    """
    if not levels:
        return []

    sorted_lvls = sorted(levels)
    clusters: List[List[float]] = [[sorted_lvls[0]]]

    for lvl in sorted_lvls[1:]:
        anchor = clusters[-1][0]
        if abs(lvl - anchor) / anchor * 100 <= cluster_pct:
            clusters[-1].append(lvl)
        else:
            clusters.append([lvl])

    result = []
    for c in clusters:
        c.sort()
        median = c[len(c) // 2]
        result.append((median, len(c)))

    result.sort(key=lambda x: x[1], reverse=True)
    return result


class SRCluster:
    """
    M1: Кластеризатор S/R уровней. Находит сильные зоны поддержки/сопротивления.
    """

    def __init__(self, candles, lookback: int = _SWING_LB):
        self._resistance_clusters: List[Tuple[float, int]] = []
        self._support_clusters:    List[Tuple[float, int]] = []

        if not candles or len(candles) < lookback * 2 + 2:
            return
        try:
            highs, lows = _collect_pivots(candles, lookback)

            res_raw = _kmedian_clusters(highs, _CLUSTER_PCT)
            sup_raw = _kmedian_clusters(lows,  _CLUSTER_PCT)

            # Оставляем только кластеры с минимальным кол-вом касаний
            self._resistance_clusters = [(p, c) for p, c in res_raw if c >= _MIN_TOUCHES]
            self._support_clusters    = [(p, c) for p, c in sup_raw if c >= _MIN_TOUCHES]

        except Exception as e:
            logger.debug(f"[SRCluster] build error: {e}")

    def score_bonus(self, price: float, direction: str) -> Tuple[int, str]:
        """
        Возвращает (bonus, reason) по ближайшему сильному кластеру.

        SHORT: resistance выше цены в пределах _PROXIMITY_PCT
        LONG:  support ниже цены в пределах _PROXIMITY_PCT
        """
        if price <= 0:
            return 0, ""
        try:
            if direction == "short":
                candidates = [
                    (p, cnt) for p, cnt in self._resistance_clusters
                    if p > price and (p - price) / price * 100 <= _PROXIMITY_PCT
                ]
                if not candidates:
                    return 0, ""
                nearest_p, nearest_cnt = min(candidates, key=lambda x: x[0] - price)
                dist_pct = (nearest_p - price) / price * 100
                bonus = _BONUS_STRONG if nearest_cnt >= 3 else _BONUS_NORMAL
                reason = (f"🎯 [SR-CLUSTER] Resistance×{nearest_cnt} "
                          f"@{nearest_p:.5g} (+{dist_pct:.1f}%) → +{bonus}")
                return bonus, reason

            else:  # long
                candidates = [
                    (p, cnt) for p, cnt in self._support_clusters
                    if p < price and (price - p) / price * 100 <= _PROXIMITY_PCT
                ]
                if not candidates:
                    return 0, ""
                nearest_p, nearest_cnt = max(candidates, key=lambda x: x[0])
                dist_pct = (price - nearest_p) / price * 100
                bonus = _BONUS_STRONG if nearest_cnt >= 3 else _BONUS_NORMAL
                reason = (f"🎯 [SR-CLUSTER] Support×{nearest_cnt} "
                          f"@{nearest_p:.5g} (-{dist_pct:.1f}%) → +{bonus}")
                return bonus, reason

        except Exception as e:
            logger.debug(f"[SRCluster] score_bonus: {e}")
            return 0, ""

    @property
    def resistance_levels(self) -> List[float]:
        return [p for p, _ in self._resistance_clusters]

    @property
    def support_levels(self) -> List[float]:
        return [p for p, _ in self._support_clusters]
