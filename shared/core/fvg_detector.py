"""
FVG Detector — A4 (унификация)
Единственная реализация Fair Value Gap вместо трёх дублей:
  - smc_ict_detector.find_bearish_fvg / find_bullish_fvg
  - pattern_detector.detect_fvg_long / detect_fvg_short

Оба модуля импортируют отсюда базовую логику.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Union


class _RawCandle:
    """Duck-type адаптер для List[List[float]] → объект с .open/.high/.low/.close/.volume"""
    __slots__ = ("open", "high", "low", "close", "volume")
    def __init__(self, row):
        self.open   = row[0]
        self.high   = row[1]
        self.low    = row[2]
        self.close  = row[3]
        self.volume = row[4] if len(row) > 4 else 0.0


def _wrap_candles(candles) -> list:
    """Оборачивает list-of-lists в duck-typed объекты, пропускает уже объекты."""
    if not candles:
        return []
    if isinstance(candles[0], (list, tuple)):
        return [_RawCandle(c) for c in candles]
    return candles  # уже объекты с атрибутами


@dataclass
class FVGZone:
    direction: str          # "bullish" | "bearish"
    upper: float
    lower: float
    gap_pct: float          # размер гэпа в %
    impulse_atr_mult: float # насколько импульс кратен ATR (0 если нет)
    index: int              # индекс средней свечи
    filled: bool = False

    @property
    def mid(self) -> float:
        return (self.upper + self.lower) / 2


def _atr(candles, period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    trs = [max(c.high - c.low,
               abs(c.high - candles[i - 1].close),
               abs(c.low  - candles[i - 1].close))
           for i, c in enumerate(candles[1:], 1)]
    recent = trs[-period:]
    return sum(recent) / len(recent) if recent else 0.0


def scan_fvg_zones(
    candles,                         # List[CandleObj] или List[List[float]]
    direction: str,
    lookback: int = 20,
    min_gap_pct: float = 0.1,
    require_impulse: bool = False,
    impulse_atr_min: float = 1.2,
) -> List[FVGZone]:
    """
    Ядро FVG-детектора. Возвращает незаполненные зоны, отсортированные
    по близости к последней цене.

    Args:
        direction:        "bullish" или "bearish"
        lookback:         сколько свечей назад смотреть
        min_gap_pct:      минимальный размер гэпа (%)
        require_impulse:  True → только гэпы с сильным импульсом (для PatternDetector)
        impulse_atr_min:  порог ATR для импульса
    """
    candles = _wrap_candles(candles)
    if len(candles) < 4:
        return []

    atr_v = _atr(candles, 14) if require_impulse else 0.0
    current_price = candles[-1].close
    start = max(0, len(candles) - lookback - 2)
    zones: List[FVGZone] = []

    for i in range(start, len(candles) - 2):
        c1, c2, c3 = candles[i], candles[i + 1], candles[i + 2]

        if direction == "bearish":
            # Медвежий FVG: low[i] > high[i+2]
            upper = c1.low
            lower = c3.high
            is_impulse_candle = (c2.open - c2.close) > 0  # медвежья свеча
        else:
            # Бычий FVG: high[i+2] > low[i]  →  low[i] < high[i+2]
            lower = c1.high
            upper = c3.low
            is_impulse_candle = (c2.close - c2.open) > 0  # бычья свеча

        if lower >= upper:
            continue

        gap_pct = (upper - lower) / c2.close * 100 if c2.close > 0 else 0
        if gap_pct < min_gap_pct:
            continue

        # Проверка импульса
        impulse_mult = 0.0
        if require_impulse:
            if not is_impulse_candle:
                continue
            body = abs(c2.close - c2.open)
            if atr_v > 0:
                impulse_mult = body / atr_v
            if impulse_mult < impulse_atr_min:
                continue

        # Заполнен ли гэп
        if direction == "bearish":
            filled = any(candles[j].high >= upper for j in range(i + 3, len(candles)))
        else:
            filled = any(candles[j].low <= lower for j in range(i + 3, len(candles)))

        zones.append(FVGZone(
            direction=direction,
            upper=upper,
            lower=lower,
            gap_pct=round(gap_pct, 3),
            impulse_atr_mult=round(impulse_mult, 2),
            index=i + 1,
            filled=filled,
        ))

    # Только незаполненные, ближайшие к цене
    zones = [z for z in zones if not z.filled]
    zones.sort(key=lambda z: abs(z.mid - current_price))
    return zones[:3]


def nearest_fvg(
    candles,
    direction: str,
    current_price: float,
    tolerance_pct: float = 2.0,
    **kwargs,
) -> Optional[FVGZone]:
    """
    Возвращает ближайший незаполненный FVG в пределах tolerance_pct от цены.
    Удобная обёртка для SMCDetector.
    """
    zones = scan_fvg_zones(candles, direction, **kwargs)
    for z in zones:
        lo_bound = z.lower * (1 - tolerance_pct / 100)
        hi_bound = z.upper * (1 + tolerance_pct / 100)
        if lo_bound <= current_price <= hi_bound:
            return z
    return None


def price_in_fvg(zone: FVGZone, price: float) -> bool:
    return zone.lower <= price <= zone.upper
