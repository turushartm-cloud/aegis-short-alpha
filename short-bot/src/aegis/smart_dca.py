"""
Aegis Smart DCA Engine v1.0
ATR-based динамическая сетка усреднения для SHORT позиций.

Концепция:
  - Сетка строится ВВЕРХ от точки входа (цена идёт против нас → DCA)
  - Каждый уровень разнесён на ATR × multiplier
  - Размер позиции на каждом DCA уровне растёт экспоненциально (anti-martingale capped)
  - Circuit breaker: максимум DCA_MAX_LEVELS уровней, не более MAX_EXPOSURE капитала

FREE TIER: 4 уровня DCA, 15 уровней сетки
PAID TIER: 6 уровней DCA, 20 уровней сетки
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Tuple


class GridType(Enum):
    UNIFORM      = "uniform"       # Равные расстояния
    ATR_BASED    = "atr_based"     # Разнесено по ATR
    FIBONACCI    = "fibonacci"     # Фибоначчи-spacing
    EXPONENTIAL  = "exponential"   # Экспоненциальное расширение


@dataclass
class DCALevel:
    level:        int              # Номер уровня (1 = первый DCA)
    price:        float            # Цена DCA ордера
    size_pct:     float            # % от начальной позиции
    size_usd:     float            # Абсолютный USD размер
    distance_pct: float            # % расстояние от входа
    cumulative_exposure: float     # Накопленный USD (включая предыдущие)
    new_avg_entry: float           # Усреднённая цена входа после этого DCA


@dataclass
class GridConfig:
    grid_type:       GridType = GridType.ATR_BASED
    dca_levels:      int      = 4       # Количество DCA уровней
    atr_multiplier:  float    = 1.5     # ATR × multiplier = шаг сетки
    size_multiplier: float    = 1.5     # Рост размера на каждом DCA (1.5x)
    max_exposure_pct: float   = 0.40   # Максимум 40% капитала на символ
    min_distance_pct: float   = 0.8    # Минимальный шаг сетки (% от входа)
    max_distance_pct: float   = 8.0    # Максимальный шаг до SL
    trail_activation_pct: float = 1.0  # Активация трейлинга после +1%


@dataclass
class DCAGrid:
    symbol:       str
    direction:    str                  # "short"
    entry_price:  float
    initial_size: float                # USD
    capital:      float
    atr:          float
    grid_type:    GridType
    levels:       List[DCALevel] = field(default_factory=list)
    total_exposure: float = 0.0
    weighted_avg:   float = 0.0        # Итоговая средняя после всех DCA


class SmartDCAEngine:
    """
    Движок Smart DCA + Grid для SHORT позиций.
    
    Сетка строится ВЫШЕ точки входа (для SHORT — цена растёт = против нас).
    При срабатывании уровня DCA — усредняемся вниз по средней цене входа.
    """

    def __init__(self, config: Optional[GridConfig] = None):
        self.config = config or GridConfig()

    def calculate_atr(self, ohlcv: list, period: int = 14) -> float:
        """Расчёт ATR из списка свечей CandleData"""
        if not ohlcv or len(ohlcv) < period + 1:
            return 0.0
        try:
            trs = []
            for i in range(1, min(period + 1, len(ohlcv))):
                c = ohlcv[-i]
                p = ohlcv[-i - 1]
                tr = max(
                    c.high - c.low,
                    abs(c.high - p.close),
                    abs(c.low  - p.close),
                )
                trs.append(tr)
            return sum(trs) / len(trs) if trs else 0.0
        except Exception:
            return 0.0

    def _atr_spacing(self, atr: float, level: int) -> float:
        """
        ATR-based расстояние между уровнями.
        Level 1: ATR × 1.5
        Level 2: ATR × 2.25 (cumulative)
        Level 3: ATR × 3.375
        """
        return atr * (self.config.atr_multiplier ** level)

    def _fibonacci_spacing(self, base_dist: float, level: int) -> float:
        """Фибоначчи: 1.0, 1.618, 2.618, 4.236, ..."""
        fib = [1.0, 1.618, 2.618, 4.236, 6.854, 11.09]
        mult = fib[min(level - 1, len(fib) - 1)]
        return base_dist * mult

    def calculate_grid(
        self,
        symbol:       str,
        entry_price:  float,
        capital:      float,
        initial_risk_pct: float = 0.01,   # 1% капитала на начальную позицию
        atr:          float = 0.0,
        ohlcv:        list  = None,
        sl_price:     float = None,
    ) -> DCAGrid:
        """
        Рассчитывает DCA сетку для SHORT позиции.
        
        Args:
            entry_price:      Цена входа (шорт)
            capital:          Доступный капитал (USD)
            initial_risk_pct: % капитала на первый вход
            atr:              Предрассчитанный ATR (опц.)
            ohlcv:            Свечи для расчёта ATR
            sl_price:         Stop Loss (выше входа для SHORT)
        
        Returns:
            DCAGrid с уровнями DCA
        """
        if not atr and ohlcv:
            atr = self.calculate_atr(ohlcv)

        # Fallback ATR: 2% от цены
        if not atr or atr <= 0:
            atr = entry_price * 0.02

        initial_size_usd = capital * initial_risk_pct
        max_exposure_usd = capital * self.config.max_exposure_pct

        # SL расстояние (для ограничения последнего DCA уровня)
        sl_distance_pct = 0.0
        if sl_price and sl_price > entry_price:
            sl_distance_pct = (sl_price - entry_price) / entry_price * 100

        levels: List[DCALevel] = []
        cumulative_usd = initial_size_usd
        cumulative_qty = initial_size_usd / entry_price   # кол-во монет
        current_avg = entry_price

        for i in range(1, self.config.dca_levels + 1):
            # Рассчитываем цену DCA уровня (ВЫШЕ входа, т.к. SHORT)
            if self.config.grid_type == GridType.ATR_BASED:
                raw_dist = self._atr_spacing(atr, i)
                dist_pct = raw_dist / entry_price * 100
            elif self.config.grid_type == GridType.FIBONACCI:
                base_dist = atr * self.config.atr_multiplier
                raw_dist  = self._fibonacci_spacing(base_dist, i)
                dist_pct  = raw_dist / entry_price * 100
            elif self.config.grid_type == GridType.EXPONENTIAL:
                dist_pct  = self.config.min_distance_pct * (1.8 ** i)
            else:  # UNIFORM
                dist_pct  = self.config.min_distance_pct * i

            # Ограничения
            dist_pct = max(dist_pct, self.config.min_distance_pct * i)
            dist_pct = min(dist_pct, self.config.max_distance_pct)

            # Не создаём уровень за SL
            if sl_distance_pct > 0 and dist_pct >= sl_distance_pct * 0.90:
                break

            dca_price = entry_price * (1 + dist_pct / 100)

            # Размер на этом уровне (растёт с каждым)
            level_size_usd = initial_size_usd * (self.config.size_multiplier ** i)

            # Проверка exposure лимита
            if cumulative_usd + level_size_usd > max_exposure_usd:
                level_size_usd = max_exposure_usd - cumulative_usd
                if level_size_usd < initial_size_usd * 0.3:
                    break  # Слишком мало — не открываем

            level_qty = level_size_usd / dca_price
            new_total_qty = cumulative_qty + level_qty
            new_total_usd = cumulative_usd + level_size_usd

            # Новая средняя цена входа после DCA
            new_avg = (cumulative_usd + level_size_usd) / new_total_qty

            levels.append(DCALevel(
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

        return DCAGrid(
            symbol=symbol,
            direction="short",
            entry_price=entry_price,
            initial_size=round(initial_size_usd, 2),
            capital=capital,
            atr=round(atr, 8),
            grid_type=self.config.grid_type,
            levels=levels,
            total_exposure=round(cumulative_usd, 2),
            weighted_avg=round(current_avg, 8),
        )

    def calculate_tp_levels(
        self,
        entry_price: float,
        sl_price:    float,
        num_tps:     int   = 4,
        rr_ratio:    float = 2.0,      # Минимум 1:2 R/R
        funding_rate: float = 0.0,
        atr:         float = 0.0,
    ) -> List[Tuple[float, int]]:
        """
        Рассчитывает динамические TP уровни для SHORT.
        
        TP цены НИЖЕ точки входа.
        Распределение: больший вес на первых TP (conservative).
        """
        if not atr:
            atr = entry_price * 0.02

        sl_dist = abs(sl_price - entry_price)  # Расстояние до SL (в USD)
        min_tp_dist = sl_dist * rr_ratio / num_tps  # Минимальный шаг TP

        # Базовые TP через ATR и R/R
        tp_distances: List[float] = []
        for i in range(1, num_tps + 1):
            # Fibonacci-style: 1.0, 1.5, 2.5, 4.0 × R
            fib_mult = [1.0, 1.5, 2.5, 4.0, 6.0, 10.0]
            tp_dist = sl_dist * fib_mult[min(i - 1, len(fib_mult) - 1)]
            tp_distances.append(tp_dist)

        # Корректировка для высокого funding (берём прибыль быстрее)
        if funding_rate > 0.1:
            tp_distances = [d * 0.8 for d in tp_distances]  # Уменьшаем расстояния

        # Веса TP (убывающие — больше на первых)
        weights_map = {
            4: [15, 20, 20, 15],
            5: [15, 20, 20, 15, 15],
            6: [15, 20, 20, 15, 15, 15],
        }
        weights = weights_map.get(num_tps, [25] * num_tps)

        tps: List[Tuple[float, int]] = []
        for i, (dist, weight) in enumerate(zip(tp_distances, weights)):
            tp_price = entry_price - dist   # SHORT: TP ниже входа
            tp_price = max(tp_price, entry_price * 0.5)  # Не ниже 50% входа
            tps.append((round(tp_price, 8), weight))

        return tps

    def get_trail_config(
        self,
        entry_price:    float,
        atr:            float,
        activation_pct: float = 1.0,    # % прибыли до активации
    ) -> dict:
        """Конфигурация трейлинг-стопа для SHORT"""
        activation_price = entry_price * (1 - activation_pct / 100)
        trail_distance   = max(atr * 0.5, entry_price * 0.012)  # мин 1.2%

        return {
            "activation_price": round(activation_price, 8),
            "trail_distance":   round(trail_distance, 8),
            "trail_pct":        round(trail_distance / entry_price * 100, 3),
            "activation_pct":   activation_pct,
        }

    def format_grid_report(self, grid: DCAGrid) -> str:
        """Форматирование сетки для Telegram"""
        lines = [
            f"📐 <b>DCA Grid ({grid.grid_type.value})</b>",
            f"Entry: ${grid.entry_price:,.6f} | ATR: ${grid.atr:.6f}",
            f"Initial: ${grid.initial_size:.2f} | Max: ${grid.total_exposure:.2f}",
            "",
            "<b>DCA Levels:</b>",
        ]
        for lvl in grid.levels:
            lines.append(
                f"  L{lvl.level}: ${lvl.price:,.6f} (+{lvl.distance_pct:.1f}%) "
                f"${lvl.size_usd:.0f} → avg ${lvl.new_avg_entry:,.6f}"
            )
        lines.append(f"\nWeighted Avg Entry: ${grid.weighted_avg:,.6f}")
        return "\n".join(lines)
