"""
BSL Scanner v1.0 — Buy Side Liquidity Scanner для LONG
Находит зоны где стоят стопы шортистов = ликвидность выше цены.

BSL кластеры:
  - Equal Highs (равные максимумы) — стопы шортистов
  - Предыдущие структурные максимумы
  - Order Block выше текущей цены
  Логика: цена притягивается к BSL → Long target

Для LONG: текущая цена НИЖЕ BSL → есть куда расти.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
logger = logging.getLogger("aegis.bsl_scanner")


class BSLScanner:
    """Buy Side Liquidity Scanner"""

    def __init__(self, lookback: int = 50, equal_high_tolerance: float = 0.003):
        self.lookback   = lookback
        self.eh_tol     = equal_high_tolerance  # 0.3% — "равные" максимумы

    def _find_equal_highs(self, ohlcv: list) -> List[Tuple[float, int]]:
        """Находит Equal Highs (стопы шортистов выше)"""
        recent = ohlcv[-self.lookback:]
        highs  = [(c.high, i) for i, c in enumerate(recent)]
        eq_highs: List[Tuple[float, int]] = []

        for i in range(len(highs)):
            for j in range(i + 3, len(highs)):
                h1, h2 = highs[i][0], highs[j][0]
                if h1 > 0 and abs(h1 - h2) / h1 < self.eh_tol:
                    # Equal High найден
                    if not any(abs(eh[0] - h1) / h1 < self.eh_tol for eh in eq_highs):
                        eq_highs.append((round(h1, 8), i))

        return sorted(eq_highs, key=lambda x: x[0], reverse=True)[:5]

    def _find_structural_highs(self, ohlcv: list, window: int = 5) -> List[float]:
        """Структурные максимумы (swing highs)"""
        recent = ohlcv[-self.lookback:]
        highs  = []
        for i in range(window, len(recent) - window):
            c = recent[i]
            if all(c.high >= recent[j].high
                   for j in range(i - window, i + window + 1) if j != i):
                highs.append(c.high)
        return sorted(set(round(h, 8) for h in highs), reverse=True)[:6]

    async def analyze(self, symbol: str, market_data: Any, ohlcv: list) -> Dict:
        """Анализ BSL зон выше текущей цены"""
        reasons: List[str] = []
        score   = 0.0

        current_price = getattr(market_data, 'price', 0) or (ohlcv[-1].close if ohlcv else 0)
        if not current_price or not ohlcv or len(ohlcv) < 20:
            return {"score": 20.0, "reasons": reasons,
                    "metadata": {"bsl_zones": [], "current_price": current_price}}

        try:
            # BSL зоны выше текущей цены
            eq_highs = self._find_equal_highs(ohlcv)
            sw_highs = self._find_structural_highs(ohlcv)

            bsl_zones = []

            # Equal Highs выше цены = стопы шортов
            for level, idx in eq_highs:
                if level > current_price:
                    dist_pct = (level - current_price) / current_price * 100
                    bsl_zones.append({"level": level, "type": "EqualHigh",
                                      "dist_pct": round(dist_pct, 2)})

            # Structural Highs выше
            for level in sw_highs:
                if level > current_price * 1.005:  # На 0.5%+ выше
                    dist_pct = (level - current_price) / current_price * 100
                    bsl_zones.append({"level": level, "type": "SwingHigh",
                                      "dist_pct": round(dist_pct, 2)})

            # Сортируем по близости
            bsl_zones.sort(key=lambda x: x["dist_pct"])

            # Скоринг: BSL близко = цена магнитится вверх
            if bsl_zones:
                nearest = bsl_zones[0]
                if nearest["dist_pct"] < 2.0:
                    score += 40
                    reasons.append(f"BSL {nearest['type']} @ +{nearest['dist_pct']:.1f}% — магнит")
                elif nearest["dist_pct"] < 4.0:
                    score += 25
                    reasons.append(f"BSL {nearest['type']} @ +{nearest['dist_pct']:.1f}%")
                elif nearest["dist_pct"] < 7.0:
                    score += 12
                    reasons.append(f"BSL зона @ +{nearest['dist_pct']:.1f}%")

                eq_count = sum(1 for z in bsl_zones if z["type"] == "EqualHigh")
                if eq_count >= 2:
                    score += 20
                    reasons.append(f"{eq_count} Equal Highs — сильный BSL кластер")
                elif eq_count == 1:
                    score += 10

            else:
                reasons.append("BSL зоны не найдены выше цены")

            score = min(max(score, 0), 100)

            return {
                "score":   round(score, 1),
                "reasons": reasons,
                "metadata": {
                    "bsl_zones":     bsl_zones[:4],
                    "current_price": round(current_price, 8),
                    "nearest_bsl":   bsl_zones[0]["level"] if bsl_zones else None,
                    "bsl_count":     len(bsl_zones),
                }
            }

        except Exception as e:
            logger.warning(f"BSL error {symbol}: {e}")
            return {"score": 15.0, "reasons": [], "metadata": {}}
