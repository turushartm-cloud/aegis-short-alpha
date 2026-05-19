"""
M7: Absorption Pattern Detector — детектор поглощения объёма.

Absorption (поглощение) = большой объём на свече с маленьким движением цены.
Означает: одна сторона активно поглощает давление другой стороны у ключевого уровня.

Паттерны:

  Bearish Absorption (для SHORT):
    У сопротивления: большой объём на бычьей свече, но цена почти не двигается.
    Продавцы поглощают покупателей → разворот вниз вероятен.

  Bullish Absorption (для LONG):
    У поддержки: большой объём на медвежьей свече, но цена почти не двигается.
    Покупатели поглощают продавцов → разворот вверх вероятен.

Критерии:
  1. Объём свечи > avg_volume × VOL_MULTIPLIER (значительный объём)
  2. Тело свечи < диапазон × BODY_RATIO_MAX  (маленькое движение)
  3. Свеча у ключевого уровня (в пределах LEVEL_PROXIMITY_PCT от S/R)
  4. Направление свечи против ожидаемого движения (SHORT: бычья; LONG: медвежья)

ENV:
  ABSORP_VOL_MULT      = 2.0    × средний объём для сигнала
  ABSORP_BODY_RATIO    = 0.3    тело/диапазон < 30% = слабое движение
  ABSORP_LOOKBACK      = 20     свечей для подсчёта avg volume
  ABSORP_LEVEL_PCT     = 1.5    % близости к S/R уровню
  ABSORP_BONUS         = 12     бонус за подтверждённую абсорбцию
"""
import os
import logging
from typing import Tuple, List, Optional

logger = logging.getLogger(__name__)

_VOL_MULT   = float(os.getenv("ABSORP_VOL_MULT",   "2.0"))
_BODY_RATIO = float(os.getenv("ABSORP_BODY_RATIO",  "0.3"))
_LOOKBACK   = int(os.getenv("ABSORP_LOOKBACK",     "20"))
_LEVEL_PCT  = float(os.getenv("ABSORP_LEVEL_PCT",   "1.5"))
_BONUS      = int(os.getenv("ABSORP_BONUS",         "12"))


def _avg_volume(candles, lookback: int) -> float:
    """Средний объём за последние N свечей."""
    sample = candles[-lookback:] if len(candles) >= lookback else candles
    vols = [getattr(c, "quote_volume", 0) or getattr(c, "volume", 0) for c in sample]
    vols = [v for v in vols if v > 0]
    return sum(vols) / len(vols) if vols else 0.0


def _candle_is_bullish(c) -> bool:
    return c.close >= c.open


def _near_level(price: float, level: float, pct: float) -> bool:
    if level <= 0 or price <= 0:
        return False
    return abs(price - level) / price * 100 <= pct


def detect_absorption(
    candles,
    price: float,
    direction: str,
    resistance_levels: Optional[List[float]] = None,
    support_levels:    Optional[List[float]] = None,
) -> Tuple[int, str]:
    """
    Определяет паттерн поглощения объёма на последних свечах.

    Args:
        candles:           OHLCV свечи (15m или 30m)
        price:             текущая цена
        direction:         "short" или "long"
        resistance_levels: уровни сопротивления (из SRCluster)
        support_levels:    уровни поддержки (из SRCluster)

    Returns:
        (bonus, reason) или (0, "")
    """
    if not candles or len(candles) < 5 or price <= 0:
        return 0, ""

    try:
        avg_vol = _avg_volume(candles, _LOOKBACK)
        if avg_vol <= 0:
            return 0, ""

        # Смотрим последние 3 завершённые свечи (не текущую)
        recent = candles[-4:-1] if len(candles) >= 4 else candles[:-1]
        if not recent:
            return 0, ""

        if direction == "short":
            levels = resistance_levels or []
            for c in reversed(recent):
                vol = getattr(c, "quote_volume", 0) or getattr(c, "volume", 0)
                if vol <= 0:
                    continue
                rng = c.high - c.low
                if rng <= 0:
                    continue
                body = abs(c.close - c.open)
                body_ratio = body / rng

                # Бычья свеча с высоким объёмом и маленьким телом = bearish absorption
                if (_candle_is_bullish(c)
                        and vol >= avg_vol * _VOL_MULT
                        and body_ratio <= _BODY_RATIO):
                    # Проверяем близость к уровню сопротивления
                    near_res = any(_near_level(c.high, lvl, _LEVEL_PCT) for lvl in levels)
                    if not near_res and levels:
                        # Без конкретных уровней — проверяем общую зону (цена высоко)
                        continue
                    lvl_str = ""
                    if levels:
                        nearby = [lvl for lvl in levels if _near_level(c.high, lvl, _LEVEL_PCT)]
                        if nearby:
                            lvl_str = f"@{min(nearby, key=lambda x: abs(x-c.high)):.5g}"
                    reason = (
                        f"🧲 [ABSORPTION] Bearish — vol×{vol/avg_vol:.1f} "
                        f"body={body_ratio:.0%} {lvl_str} → SHORT +{_BONUS}"
                    )
                    return _BONUS, reason

        else:  # long
            levels = support_levels or []
            for c in reversed(recent):
                vol = getattr(c, "quote_volume", 0) or getattr(c, "volume", 0)
                if vol <= 0:
                    continue
                rng = c.high - c.low
                if rng <= 0:
                    continue
                body = abs(c.close - c.open)
                body_ratio = body / rng

                # Медвежья свеча с высоким объёмом и маленьким телом = bullish absorption
                if (not _candle_is_bullish(c)
                        and vol >= avg_vol * _VOL_MULT
                        and body_ratio <= _BODY_RATIO):
                    near_sup = any(_near_level(c.low, lvl, _LEVEL_PCT) for lvl in levels)
                    if not near_sup and levels:
                        continue
                    lvl_str = ""
                    if levels:
                        nearby = [lvl for lvl in levels if _near_level(c.low, lvl, _LEVEL_PCT)]
                        if nearby:
                            lvl_str = f"@{max(nearby, key=lambda x: abs(x-c.low)):.5g}"
                    reason = (
                        f"🧲 [ABSORPTION] Bullish — vol×{vol/avg_vol:.1f} "
                        f"body={body_ratio:.0%} {lvl_str} → LONG +{_BONUS}"
                    )
                    return _BONUS, reason

    except Exception as e:
        logger.debug(f"[Absorption] error: {e}")

    return 0, ""


def detect_absorption_from_sr(
    candles,
    price: float,
    direction: str,
    sr_cluster=None,
) -> Tuple[int, str]:
    """Удобная обёртка: принимает SRCluster."""
    res_lvls = sr_cluster.resistance_levels if sr_cluster else None
    sup_lvls = sr_cluster.support_levels    if sr_cluster else None
    return detect_absorption(candles, price, direction, res_lvls, sup_lvls)
