"""
M5: False Breakout Detector — фильтр ложных пробоев ключевых уровней.

Ложный пробой = цена кратковременно пробивает уровень S/R,
но закрывается ОБРАТНО внутри — сигнал разворота.

Паттерны:

  Bearish False Breakout (для SHORT):
    Свеча пробила resistance вверх (high > level),
    но закрылась НИЖЕ уровня (close < level) → rejection → +15

  Bullish False Breakout (для LONG):
    Свеча пробила support вниз (low < level),
    но закрылась ВЫШЕ уровня (close > level) → rejection → +15

Правила валидации:
  • Пробой должен быть реальным: high/low выходит за уровень на > 0.1%
  • Тело свечи должно закрыться обратно (не просто хвост)
  • Уровень должен быть проверен ранее (2+ касаний через SRCluster)
  • Смотрим последние 3 свечи

ENV:
  FBD_LOOKBACK      = 3     последних свечей для проверки
  FBD_MIN_BREAK_PCT = 0.1   % минимального пробоя за уровень
  FBD_BONUS         = 15    бонус за подтверждённый false breakout
"""
import os
import logging
from typing import Tuple, List, Optional

logger = logging.getLogger(__name__)

_LOOKBACK      = int(os.getenv("FBD_LOOKBACK",       "3"))
_MIN_BREAK_PCT = float(os.getenv("FBD_MIN_BREAK_PCT", "0.1"))
_BONUS         = int(os.getenv("FBD_BONUS",          "15"))


def detect_false_breakout(
    candles,
    price: float,
    direction: str,
    resistance_levels: Optional[List[float]] = None,
    support_levels:    Optional[List[float]] = None,
) -> Tuple[int, str]:
    """
    Определяет ложный пробой на последних свечах относительно ключевых уровней.

    Args:
        candles:           OHLCV свечи (15m или 4h)
        price:             текущая цена
        direction:         "short" или "long"
        resistance_levels: список уровней сопротивления (из SRCluster)
        support_levels:    список уровней поддержки (из SRCluster)

    Returns:
        (bonus, reason) или (0, "")
    """
    if not candles or price <= 0:
        return 0, ""

    try:
        recent = candles[-_LOOKBACK:] if len(candles) >= _LOOKBACK else candles

        if direction == "short":
            levels = resistance_levels or []
            for level in levels:
                if level <= price:
                    continue
                # Смотрим последние свечи на false breakout выше уровня
                for c in reversed(recent):
                    min_break = level * (1 + _MIN_BREAK_PCT / 100)
                    if c.high >= min_break and c.close < level:
                        break_pct = (c.high - level) / level * 100
                        reason = (
                            f"🔻 [FALSE BREAK] Rejection от сопротивления "
                            f"@{level:.5g} (wick +{break_pct:.2f}%) → SHORT +{_BONUS}"
                        )
                        return _BONUS, reason

        else:  # long
            levels = support_levels or []
            for level in levels:
                if level >= price:
                    continue
                # Смотрим последние свечи на false breakout ниже уровня
                for c in reversed(recent):
                    min_break = level * (1 - _MIN_BREAK_PCT / 100)
                    if c.low <= min_break and c.close > level:
                        break_pct = (level - c.low) / level * 100
                        reason = (
                            f"🔺 [FALSE BREAK] Rejection от поддержки "
                            f"@{level:.5g} (wick -{break_pct:.2f}%) → LONG +{_BONUS}"
                        )
                        return _BONUS, reason

    except Exception as e:
        logger.debug(f"[FBD] error: {e}")

    return 0, ""


def detect_false_breakout_from_sr(
    candles,
    price: float,
    direction: str,
    sr_cluster=None,
) -> Tuple[int, str]:
    """
    Удобная обёртка: принимает SRCluster и делегирует detect_false_breakout.
    """
    if sr_cluster is None:
        return detect_false_breakout(candles, price, direction)

    return detect_false_breakout(
        candles=candles,
        price=price,
        direction=direction,
        resistance_levels=sr_cluster.resistance_levels,
        support_levels=sr_cluster.support_levels,
    )
