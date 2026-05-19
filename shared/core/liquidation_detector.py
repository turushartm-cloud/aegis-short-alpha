"""
Liquidation dataclasses — shared/core/liquidation_detector.py

LiquidationCluster и LiquidationAnalysis используются scorer.py.
LiquidationZoneDetector (требовал Coinglass API) удалён.
"""

from typing import List, Optional
from dataclasses import dataclass


@dataclass
class LiquidationCluster:
    """Кластер ликвидаций = магнит для цены"""
    price_level: float
    volume: float  # Суммарный объём ликвидаций в USD
    side: str  # "long" | "short" — чьи позиции ликвидировались
    strength: float  # 0.0-1.0 относительная сила
    distance_pct: float  # Расстояние от текущей цены в %

    @property
    def is_above(self) -> bool:
        return self.distance_pct > 0

    @property
    def is_below(self) -> bool:
        return self.distance_pct < 0


@dataclass
class LiquidationAnalysis:
    """Результат анализа ликвидаций для символа"""
    symbol: str
    current_price: float
    clusters: List[LiquidationCluster]

    nearest_above: Optional[LiquidationCluster]
    nearest_below: Optional[LiquidationCluster]
    strongest_above: Optional[LiquidationCluster]
    strongest_below: Optional[LiquidationCluster]

    long_liq_dominance: float  # 0.0-1.0 сколько ликвидаций было лонгов

    @property
    def has_targets(self) -> bool:
        return len(self.clusters) >= 1

    def get_recommended_tp(self, direction: str, default_tp: float) -> float:
        if direction == "long" and self.nearest_above:
            return self.nearest_above.price_level * 0.998
        elif direction == "short" and self.nearest_below:
            return self.nearest_below.price_level * 1.002
        return default_tp

    def get_recommended_sl(self, direction: str, default_sl: float) -> float:
        if direction == "long" and self.nearest_below:
            return self.nearest_below.price_level * 0.995
        elif direction == "short" and self.nearest_above:
            return self.nearest_above.price_level * 1.005
        return default_sl

    def get_score_bonus(self, direction: str) -> int:
        bonus = 0
        if direction == "long":
            if self.nearest_above:
                dist = abs(self.nearest_above.distance_pct)
                if 2 <= dist <= 8:
                    bonus += 15
                elif 8 < dist <= 15:
                    bonus += 10
            if self.nearest_below and abs(self.nearest_below.distance_pct) < 1.5:
                bonus -= 10
        else:  # short
            if self.nearest_below:
                dist = abs(self.nearest_below.distance_pct)
                if 2 <= dist <= 8:
                    bonus += 15
                elif 8 < dist <= 15:
                    bonus += 10
            if self.nearest_above and abs(self.nearest_above.distance_pct) < 1.5:
                bonus -= 10
        return max(-20, min(20, bonus))
