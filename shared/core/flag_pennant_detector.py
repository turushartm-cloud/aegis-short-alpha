"""
Flag / Pennant Detector v1.0
Лучший continuation паттерн: 78% точность (стабильный рынок), 55% (кризис)

Алгоритм:
  1. impulse   = abs(price_change(last 5 candles 4H)) > 3%  ← импульс
  2. flag_body = range(next 5-10 candles) < 2.5%            ← флаг
  3. breakout  = price > flag_high * 1.005 (LONG)           ← пробой
                 price < flag_low  * 0.995 (SHORT)
  4. PENNANT   = консолидация сужается (HH снижаются, HL повышаются)

Returns:
  FLAG_LONG  / FLAG_SHORT: score_bonus +15, extended TP
  PENNANT_LONG / PENNANT_SHORT: score_bonus +12
"""
import os
import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)

_ENABLE_FLAG_PENNANT = os.getenv("ENABLE_FLAG_PENNANT", "true").lower() == "true"
_IMPULSE_PCT         = float(os.getenv("FLAG_IMPULSE_PCT", "3.0"))   # % для impulse
_FLAG_RANGE_PCT      = float(os.getenv("FLAG_RANGE_PCT", "2.5"))     # % макс ширина флага
_BREAKOUT_PCT        = float(os.getenv("FLAG_BREAKOUT_PCT", "0.5"))  # % для пробоя


@dataclass
class FlagPennantResult:
    pattern_type: str        # "FLAG_LONG", "FLAG_SHORT", "PENNANT_LONG", "PENNANT_SHORT", "NONE"
    score_bonus: int         # +15 для Flag, +12 для Pennant, 0 для NONE
    has_signal: bool
    impulse_pct: float       # Размер импульса %
    flag_range_pct: float    # Ширина флага %
    is_breakout: bool        # Цена пробила флаг
    direction: str           # "long" | "short" | ""
    description: str
    extend_tp: bool          # True = расширенные TP (6 уровней)


def detect_flag_pennant(
    candles_4h: List,        # список 4H свечей (нужно ≥15)
    current_price: float,
    direction: str = "long",
) -> FlagPennantResult:
    """
    Детектирует Flag и Pennant паттерны на 4H свечах.

    candles_4h: список объектов с атрибутами open, high, low, close
    """
    _empty = FlagPennantResult(
        pattern_type="NONE", score_bonus=0, has_signal=False,
        impulse_pct=0, flag_range_pct=0, is_breakout=False,
        direction="", description="Нет паттерна", extend_tp=False
    )

    if not _ENABLE_FLAG_PENNANT:
        return _empty

    if not candles_4h or len(candles_4h) < 12:
        return _empty

    try:
        # Берём последние 15 свечей
        candles = candles_4h[-15:]
        n = len(candles)

        # === 1. IMPULSE — первые 5 свечей ===
        impulse_candles = candles[:5]
        impulse_open  = float(impulse_candles[0].open)
        impulse_close = float(impulse_candles[-1].close)

        if impulse_open <= 0:
            return _empty

        impulse_pct = (impulse_close - impulse_open) / impulse_open * 100

        # Определяем направление импульса
        if abs(impulse_pct) < _IMPULSE_PCT:
            return _empty  # нет значимого импульса

        impulse_dir = "long" if impulse_pct > 0 else "short"

        # Нам нужен импульс в нужном направлении
        if impulse_dir != direction:
            return _empty

        # === 2. FLAG BODY — следующие 5-10 свечей ===
        flag_candles = candles[5:]
        if len(flag_candles) < 3:
            return _empty

        flag_highs = [float(c.high) for c in flag_candles]
        flag_lows  = [float(c.low)  for c in flag_candles]
        flag_high  = max(flag_highs)
        flag_low   = min(flag_lows)
        flag_mid   = (flag_high + flag_low) / 2

        if flag_mid <= 0:
            return _empty

        flag_range_pct = (flag_high - flag_low) / flag_mid * 100

        if flag_range_pct > _FLAG_RANGE_PCT:
            return _empty  # слишком широкий диапазон — не флаг

        # === 3. BREAKOUT — текущая цена пробивает флаг ===
        breakout_threshold_pct = _BREAKOUT_PCT / 100

        if direction == "long":
            is_breakout = current_price > flag_high * (1 + breakout_threshold_pct)
        else:
            is_breakout = current_price < flag_low * (1 - breakout_threshold_pct)

        # === 4. PENNANT — консолидация сужается? ===
        is_pennant = False
        if len(flag_candles) >= 4:
            # Highs должны снижаться, Lows должны повышаться (для LONG pennant)
            h_trend = flag_highs[-1] < flag_highs[0]  # хаи снижаются
            l_trend = flag_lows[-1]  > flag_lows[0]   # лои повышаются
            is_pennant = h_trend and l_trend            # клин = пеннант

        # === 5. Score и тип паттерна ===
        if is_pennant:
            score_bonus = 12
            pattern_type = f"PENNANT_{direction.upper()}"
            desc = f"🔺 PENNANT {direction.upper()}: импульс {abs(impulse_pct):.1f}%, клин {flag_range_pct:.1f}%"
        else:
            score_bonus = 15
            pattern_type = f"FLAG_{direction.upper()}"
            desc = f"🚩 FLAG {direction.upper()}: импульс {abs(impulse_pct):.1f}%, флаг {flag_range_pct:.1f}%"

        # Бонус если уже есть пробой
        if is_breakout:
            score_bonus = min(score_bonus + 3, 20)
            desc += " ✅ BREAKOUT"

        logger.info(f"[FLAG/PENNANT] {pattern_type} | impulse={impulse_pct:.1f}% flag={flag_range_pct:.1f}% breakout={is_breakout}")

        return FlagPennantResult(
            pattern_type=pattern_type,
            score_bonus=score_bonus,
            has_signal=True,
            impulse_pct=abs(impulse_pct),
            flag_range_pct=flag_range_pct,
            is_breakout=is_breakout,
            direction=direction,
            description=desc,
            extend_tp=True,  # флаги/пеннанты → расширенные TP
        )

    except Exception as e:
        logger.warning(f"[FlagPennant] Ошибка: {e}")
        return _empty
