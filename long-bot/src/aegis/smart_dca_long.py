"""
Smart DCA Long v1.0 — Aegis Long Alpha
ATR-based DCA сетка для LONG позиций.

Отличие от Short:
  - Уровни DCA НИЖЕ входа (Long: усредняем при падении)
  - TP уровни ВЫШЕ входа
  - Trailing Stop активация +1.5% (vs Short: +1.0%)
  - TP_WEIGHTS: меньше на TP1-2 (ждём большего движения)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Tuple


class GridTypeLong(Enum):
    ATR_BASED   = "atr_based"
    FIBONACCI   = "fibonacci"
    UNIFORM     = "uniform"
    EXPONENTIAL = "exponential"


@dataclass
class GridConfigLong:
    grid_type:        GridTypeLong = GridTypeLong.ATR_BASED
    dca_levels:       int   = 4
    atr_multiplier:   float = 1.5
    size_multiplier:  float = 1.5
    max_exposure_pct: float = 0.60
    min_distance_pct: float = 0.8
    max_distance_pct: float = 8.0
    trail_activation_pct: float = 1.5   # Long: +1.5% (vs Short: +1.0%)


@dataclass
class DCALevelLong:
    level:        int
    price:        float           # НИЖЕ входа для Long
    size_pct:     float
    size_usd:     float
    distance_pct: float           # % ниже входа
    cumulative_exposure: float
    new_avg_entry: float          # Усреднённая цена входа


@dataclass
class DCAGridLong:
    symbol:       str
    direction:    str = "long"
    entry_price:  float = 0.0
    initial_size: float = 0.0
    capital:      float = 0.0
    atr:          float = 0.0
    grid_type:    GridTypeLong = GridTypeLong.ATR_BASED
    levels:       List[DCALevelLong] = field(default_factory=list)
    total_exposure: float = 0.0
    weighted_avg: float = 0.0    # Средняя цена после всех DCA


class SmartDCALongEngine:
    """DCA движок для LONG — усредняем НИЖЕ точки входа"""

    def __init__(self, config: Optional[GridConfigLong] = None):
        self.config = config or GridConfigLong()

    def calculate_atr(self, ohlcv: list, period: int = 14) -> float:
        if not ohlcv or len(ohlcv) < period + 1:
            return 0.0
        try:
            trs = []
            for i in range(1, min(period + 1, len(ohlcv))):
                c = ohlcv[-i]; p = ohlcv[-i - 1]
                tr = max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close))
                trs.append(tr)
            return sum(trs) / len(trs) if trs else 0.0
        except Exception:
            return 0.0

    def _atr_spacing(self, atr: float, level: int) -> float:
        return atr * (self.config.atr_multiplier ** level)

    def _fib_spacing(self, base: float, level: int) -> float:
        fib = [1.0, 1.618, 2.618, 4.236, 6.854, 11.09]
        return base * fib[min(level - 1, len(fib) - 1)]

    def calculate_grid(
        self,
        symbol:          str,
        entry_price:     float,
        capital:         float,
        initial_risk_pct: float = 0.001,
        atr:             float = 0.0,
        ohlcv:           list  = None,
        sl_price:        float = None,   # Ниже входа для Long
    ) -> DCAGridLong:

        if not atr and ohlcv:
            atr = self.calculate_atr(ohlcv)
        if not atr or atr <= 0:
            atr = entry_price * 0.02

        initial_size_usd = capital * initial_risk_pct
        max_exposure_usd = capital * self.config.max_exposure_pct

        # SL расстояние — ниже входа для Long
        sl_distance_pct = 0.0
        if sl_price and sl_price < entry_price:
            sl_distance_pct = (entry_price - sl_price) / entry_price * 100

        levels: List[DCALevelLong] = []
        cumulative_usd = initial_size_usd
        cumulative_qty = initial_size_usd / entry_price
        current_avg    = entry_price

        for i in range(1, self.config.dca_levels + 1):
            if self.config.grid_type == GridTypeLong.ATR_BASED:
                dist_pct = self._atr_spacing(atr, i) / entry_price * 100
            elif self.config.grid_type == GridTypeLong.FIBONACCI:
                base_dist = atr * self.config.atr_multiplier
                dist_pct  = self._fib_spacing(base_dist, i) / entry_price * 100
            elif self.config.grid_type == GridTypeLong.EXPONENTIAL:
                dist_pct = self.config.min_distance_pct * (1.8 ** i)
            else:
                dist_pct = self.config.min_distance_pct * i

            dist_pct = max(dist_pct, self.config.min_distance_pct * i)
            dist_pct = min(dist_pct, self.config.max_distance_pct)

            # Не идём ниже SL
            if sl_distance_pct > 0 and dist_pct >= sl_distance_pct * 0.90:
                break

            # DCA цена НИЖЕ входа (Long усредняем на падении)
            dca_price = entry_price * (1 - dist_pct / 100)
            if dca_price <= 0:
                break

            level_size_usd = initial_size_usd * (self.config.size_multiplier ** i)
            if cumulative_usd + level_size_usd > max_exposure_usd:
                level_size_usd = max_exposure_usd - cumulative_usd
                if level_size_usd < initial_size_usd * 0.3:
                    break

            level_qty    = level_size_usd / dca_price
            new_total_qty = cumulative_qty + level_qty
            new_total_usd = cumulative_usd + level_size_usd
            new_avg       = new_total_usd / new_total_qty

            levels.append(DCALevelLong(
                level=i,
                price=round(dca_price, 8),
                size_pct=round(level_size_usd / initial_size_usd * 100, 1),
                size_usd=round(level_size_usd, 2),
                distance_pct=round(dist_pct, 2),
                cumulative_exposure=round(new_total_usd, 2),
                new_avg_entry=round(new_avg, 8),
            ))

            cumulative_usd = new_total_usd
            cumulative_qty = new_total_qty
            current_avg    = new_avg

        return DCAGridLong(
            symbol=symbol, direction="long",
            entry_price=entry_price,
            initial_size=round(initial_size_usd, 2),
            capital=capital, atr=round(atr, 8),
            grid_type=self.config.grid_type,
            levels=levels,
            total_exposure=round(cumulative_usd, 2),
            weighted_avg=round(current_avg, 8),
        )

    def calculate_tp_levels(
        self,
        entry_price:  float,
        sl_price:     float,
        num_tps:      int   = 4,
        rr_ratio:     float = 2.5,    # Long: 1:2.5 (трендовые движения длиннее)
        funding_rate: float = 0.0,
        atr:          float = 0.0,
    ) -> List[Tuple[float, int]]:
        """TP уровни ВЫШЕ входа для Long"""
        if not atr:
            atr = entry_price * 0.02

        sl_dist = abs(entry_price - sl_price)
        fib = [1.0, 1.618, 2.618, 4.236, 6.854, 10.0]

        tps = []
        # Long TP weights: меньше на TP1-2 (ждём большего движения вверх)
        weights_map = {
            4: [15, 20, 20, 15],
            5: [15, 20, 20, 15, 15],
            6: [15, 20, 20, 15, 15, 15],
        }
        weights = weights_map.get(num_tps, [20] * num_tps)

        for i in range(num_tps):
            tp_dist  = sl_dist * fib[min(i, len(fib) - 1)]
            tp_price = entry_price + tp_dist     # ВЫШЕ входа для Long
            tps.append((round(tp_price, 8), weights[i]))

        return tps

    def get_trail_config(
        self,
        entry_price:    float,
        atr:            float,
        activation_pct: float = 1.5,   # Long: +1.5% (vs Short: +1.0%)
    ) -> dict:
        activation_price = entry_price * (1 + activation_pct / 100)
        trail_distance   = max(atr * 0.6, entry_price * 0.018)  # мин 1.8% для Long

        return {
            "activation_price": round(activation_price, 8),
            "trail_distance":   round(trail_distance, 8),
            "trail_pct":        round(trail_distance / entry_price * 100, 3),
            "activation_pct":   activation_pct,
        }

    def format_grid_report(self, grid: DCAGridLong) -> str:
        lines = [
            f"📐 <b>LONG DCA Grid ({grid.grid_type.value})</b>",
            f"Entry: ${grid.entry_price:,.6f} | ATR: ${grid.atr:.6f}",
            f"Initial: ${grid.initial_size:.2f} | Max: ${grid.total_exposure:.2f}",
            "",
            "<b>DCA Levels (НИЖЕ входа):</b>",
        ]
        for lvl in grid.levels:
            lines.append(
                f"  L{lvl.level}: ${lvl.price:,.6f} (-{lvl.distance_pct:.1f}%) "
                f"${lvl.size_usd:.0f} → avg ${lvl.new_avg_entry:,.6f}"
            )
        lines.append(f"\nWeighted Avg Entry: ${grid.weighted_avg:,.6f}")
        return "\n".join(lines)
