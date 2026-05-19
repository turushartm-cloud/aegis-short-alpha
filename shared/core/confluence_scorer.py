"""
M4: Confluence Scoring — cross-TF подтверждение S/R уровней.

Идея: если несколько таймфреймов указывают на один ценовой уровень
(swing high/low, POC, Value Area) → это мощная зона S/R → confluence бонус.

SHORT: кластер сопротивления ВЫШЕ цены с 2+ ТФ → +8 за 2 ТФ, +14 за 3 ТФ
LONG:  кластер поддержки НИЖЕ цены с 2+ ТФ  → +8 за 2 ТФ, +14 за 3 ТФ

Использование:
    cs = ConfluenceScorer()
    cs.add_levels("15m", swing_highs_15m, swing_lows_15m)
    cs.add_levels("4h",  swing_highs_4h,  swing_lows_4h)
    cs.add_poc("4h", poc_price)
    bonus, reason = cs.score_bonus(price, direction="short")
"""
import os
import logging
from typing import List, Tuple, Optional, Dict

logger = logging.getLogger(__name__)

_CLUSTER_PCT    = float(os.getenv("CONFLUENCE_CLUSTER_PCT",  "0.8"))  # % группировки уровней
_PROXIMITY_PCT  = float(os.getenv("CONFLUENCE_PROXIMITY_PCT", "2.0"))  # % до цены для триггера
_BONUS_2TF      = int(os.getenv("CONFLUENCE_BONUS_2TF",  "8"))
_BONUS_3TF      = int(os.getenv("CONFLUENCE_BONUS_3TF",  "14"))


def _swing_highs(candles, lookback: int = 5) -> List[float]:
    """Находит локальные максимумы (pivot highs) среди последних свечей."""
    result = []
    data = candles[-50:] if len(candles) > 50 else candles
    n = len(data)
    for i in range(lookback, n - lookback):
        h = data[i].high
        if all(h >= data[j].high for j in range(i - lookback, i + lookback + 1) if j != i):
            result.append(h)
    return result


def _swing_lows(candles, lookback: int = 5) -> List[float]:
    """Находит локальные минимумы (pivot lows) среди последних свечей."""
    result = []
    data = candles[-50:] if len(candles) > 50 else candles
    n = len(data)
    for i in range(lookback, n - lookback):
        lo = data[i].low
        if all(lo <= data[j].low for j in range(i - lookback, i + lookback + 1) if j != i):
            result.append(lo)
    return result


class ConfluenceScorer:
    """
    Собирает ценовые уровни с разных ТФ и находит зоны confluence.
    """

    def __init__(self):
        # {tf_name: {"resistance": [prices], "support": [prices]}}
        self._levels: Dict[str, Dict[str, List[float]]] = {}

    def add_candles(self, tf_name: str, candles, poc: Optional[float] = None,
                    lookback: int = 5) -> None:
        """
        Извлекает swing highs/lows из свечей и добавляет как уровни ТФ.
        Опционально добавляет POC из Volume Profile.
        """
        if not candles or len(candles) < lookback * 2 + 1:
            return
        try:
            highs = _swing_highs(candles, lookback)
            lows  = _swing_lows(candles, lookback)
            if poc is not None:
                highs.append(poc)
                lows.append(poc)
            self._levels[tf_name] = {"resistance": highs, "support": lows}
        except Exception as e:
            logger.debug(f"[Confluence] add_candles {tf_name}: {e}")

    def add_poc(self, tf_name: str, poc: float) -> None:
        """Добавляет POC как уровень поддержки И сопротивления."""
        if poc <= 0:
            return
        entry = self._levels.setdefault(tf_name, {"resistance": [], "support": []})
        entry["resistance"].append(poc)
        entry["support"].append(poc)

    def _cluster_levels(self, levels: List[Tuple[str, float]]) -> List[List[Tuple[str, float]]]:
        """
        Группирует уровни в кластеры по близости (_CLUSTER_PCT%).
        levels: [(tf_name, price), ...]
        """
        if not levels:
            return []
        sorted_lvls = sorted(levels, key=lambda x: x[1])
        clusters: List[List[Tuple[str, float]]] = [[sorted_lvls[0]]]

        for tf, price in sorted_lvls[1:]:
            anchor = clusters[-1][0][1]
            if abs(price - anchor) / anchor * 100 <= _CLUSTER_PCT:
                clusters[-1].append((tf, price))
            else:
                clusters.append([(tf, price)])
        return clusters

    def score_bonus(self, price: float, direction: str) -> Tuple[int, str]:
        """
        Находит confluence зоны относительно цены и возвращает (bonus, reason).

        SHORT: ищет resistance кластеры выше цены
        LONG:  ищет support кластеры ниже цены
        """
        if not self._levels or price <= 0:
            return 0, ""

        try:
            if direction == "short":
                kind = "resistance"
                candidate_fn = lambda lvl: lvl > price and (lvl - price) / price * 100 <= _PROXIMITY_PCT
            else:
                kind = "support"
                candidate_fn = lambda lvl: lvl < price and (price - lvl) / price * 100 <= _PROXIMITY_PCT

            # Собираем все уровни нужного типа в пределах близости
            all_levels: List[Tuple[str, float]] = []
            for tf, tf_data in self._levels.items():
                for lvl in tf_data.get(kind, []):
                    if candidate_fn(lvl):
                        all_levels.append((tf, lvl))

            if not all_levels:
                return 0, ""

            clusters = self._cluster_levels(all_levels)

            # Ищем кластер с наибольшим количеством уникальных ТФ
            best_cluster = max(clusters, key=lambda c: len({tf for tf, _ in c}))
            unique_tfs = {tf for tf, _ in best_cluster}
            n_tfs = len(unique_tfs)

            if n_tfs < 2:
                return 0, ""

            avg_price = sum(p for _, p in best_cluster) / len(best_cluster)
            dist_pct  = abs(avg_price - price) / price * 100
            bonus = _BONUS_3TF if n_tfs >= 3 else _BONUS_2TF

            arrow = "↑" if direction == "short" else "↓"
            reason = (f"🔗 [CONFLUENCE] {n_tfs}TF {arrow}@{avg_price:.5g} "
                      f"({dist_pct:.1f}% {'above' if direction=='short' else 'below'}) "
                      f"[{','.join(sorted(unique_tfs))}] → +{bonus}")
            return bonus, reason

        except Exception as e:
            logger.debug(f"[Confluence] score_bonus: {e}")
            return 0, ""


def build_confluence_scorer(
    price: float,
    ohlcv_15m=None,
    ohlcv_1h=None,
    ohlcv_4h=None,
    poc_4h: Optional[float] = None,
) -> ConfluenceScorer:
    """
    Фабрика: создаёт и заполняет ConfluenceScorer из доступных OHLCV данных.
    poc_4h — POC из VolumeProfileAnalyzer (если уже вычислен).
    """
    cs = ConfluenceScorer()
    if ohlcv_15m and len(ohlcv_15m) >= 11:
        cs.add_candles("15m", ohlcv_15m, lookback=3)
    if ohlcv_1h and len(ohlcv_1h) >= 11:
        cs.add_candles("1h", ohlcv_1h, lookback=4)
    if ohlcv_4h and len(ohlcv_4h) >= 11:
        cs.add_candles("4h", ohlcv_4h, lookback=5)
        if poc_4h:
            cs.add_poc("4h", poc_4h)
    return cs
