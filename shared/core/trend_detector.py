"""
Trend Following Detector v2.1  (#33 + #34)
Детектирует трендовые движения БЕЗ лагающих индикаторов (no EMA, no SMA, no ADX).

Метрики (leading/coincident only):
  1. 4H momentum : |price_change_4h| ≥ TREND_MOMENTUM_4H %
                   (вычисляется из свечей если не передан)
  2. Volume surge : текущий объём > avg(20 баров) × TREND_VOLUME_MULT
  3. 1H alignment : |price_change_1h| ≥ TREND_MOMENTUM_1H % (тот же знак)
  4. 1D alignment : |price_change_1d| ≥ TREND_MOMENTUM_1D % (#34 Multi-TF)
                    (proxy из 6×4H свечей ≈ 24H, если 1D не передан)

2/4 условий → тренд подтверждён → score_bonus + extend_tp
3/4 условий → сильный тренд   → score_bonus × 1.3
4/4 условий → очень сильный   → score_bonus × 1.5 (cap 15)

Counter-trend PENALTY (v2.1):
  ≥ 2/4 условий ПРОТИВ направления → score_penalty (отрицательный score_bonus)
  ≥ 3/4 условий против            → penalty × 1.5
  Защищает от входа в лонг при падающем 4H+1H+1D тренде и наоборот.

Интеграция:
  → score_bonus добавляется к base_score ДО финального фильтра
  → score_bonus отрицательный = штраф (counter-trend)
  → extend_tp=True разрешает 6 TP уровней (вместо 4) для этой сделки

ENV:
  ENABLE_TREND_DETECTOR = true    включить модуль (дефолт true)
  TREND_MOMENTUM_4H     = 1.5     мин изменение за 4H (%)
  TREND_MOMENTUM_1H     = 0.3     мин изменение за 1H (%)
  TREND_MOMENTUM_1D     = 2.5     мин изменение за 1D (%)  [#34]
  TREND_VOLUME_MULT     = 1.3     порог объёма (× avg20)
  TREND_SCORE_BONUS     = 8       бонус очков при 2/4 условиях
  TREND_SCORE_PENALTY   = 10      штраф очков при 2/4 условиях ПРОТИВ направления
  TREND_EXTEND_TP       = true    разрешить 6 TP при подтверждённом тренде
"""
import os
import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)

_ENABLE        = os.getenv("ENABLE_TREND_DETECTOR", "true").lower() == "true"
_MOMENTUM_4H   = float(os.getenv("TREND_MOMENTUM_4H", "1.5"))
_MOMENTUM_1H   = float(os.getenv("TREND_MOMENTUM_1H", "0.3"))
_MOMENTUM_1D   = float(os.getenv("TREND_MOMENTUM_1D", "2.5"))
_VOLUME_MULT   = float(os.getenv("TREND_VOLUME_MULT", "1.3"))
_SCORE_BONUS   = int(os.getenv("TREND_SCORE_BONUS", "8"))
_SCORE_PENALTY = int(os.getenv("TREND_SCORE_PENALTY", "10"))
_EXTEND_TP     = os.getenv("TREND_EXTEND_TP", "true").lower() == "true"


@dataclass
class TrendResult:
    has_trend:    bool  = False
    score_bonus:  int   = 0     # положительный = бонус, отрицательный = штраф
    extend_tp:    bool  = False
    conditions:   int   = 0     # 0-4 сколько из 4 условий выполнено (по направлению)
    counter:      int   = 0     # 0-4 сколько условий ПРОТИВ направления
    description:  str   = ""
    is_penalty:   bool  = False  # True если это штраф за контртренд


_NO_TREND = TrendResult()


def _candle_change_pct(candles, lookback: int) -> float:
    """Вычисляет % изменение цены за последние `lookback` баров."""
    try:
        if not candles or len(candles) < lookback + 1:
            return 0.0
        old_close = candles[-(lookback + 1)].close
        new_close = candles[-1].close
        if old_close <= 0:
            return 0.0
        return (new_close - old_close) / old_close * 100
    except Exception:
        return 0.0


def detect_trend(
    candles_4h,
    price_change_1h:    float = 0.0,
    price_change_4h:    float = 0.0,
    price_change_1d:    float = 0.0,
    volume_spike_ratio: float = 1.0,
    direction:          str   = "long",
) -> TrendResult:
    """
    Детектирует трендовое движение по price momentum + volume (no EMA).

    4 условия: 4H momentum, volume surge, 1H alignment, 1D alignment (#34).
    Если price_change_4h / price_change_1d не переданы — вычисляются из свечей.

    Args:
        candles_4h:         список CandleData 4H (old→new), мин 10 свечей
        price_change_1h:    % изменение цены за 1H (из MarketData, может быть 0)
        price_change_4h:    % изменение цены за 4H (если 0 — считается из свечей)
        price_change_1d:    % изменение цены за 1D (если 0 — proxy из 6×4H свечей)
        volume_spike_ratio: текущий объём / avg объём (из MarketData)
        direction:          "long" или "short"

    Returns:
        TrendResult
    """
    if not _ENABLE:
        return _NO_TREND

    if not candles_4h or len(candles_4h) < 5:
        return _NO_TREND

    try:
        sign = 1 if direction == "long" else -1
        met:     List[str] = []  # условия В направлении сделки
        counter: List[str] = []  # условия ПРОТИВ направления

        # ── 1. 4H momentum ──────────────────────────────────────────────
        p4h = price_change_4h or 0.0
        if p4h == 0.0:
            p4h = _candle_change_pct(candles_4h, 1)
        if (p4h * sign) >= _MOMENTUM_4H:
            met.append(f"4H={p4h:+.1f}%")
        elif (p4h * sign) <= -_MOMENTUM_4H:
            counter.append(f"4H={p4h:+.1f}%↙" if direction == "long" else f"4H={p4h:+.1f}%↗")

        # ── 2. Volume surge ─────────────────────────────────────────────
        vol_ratio = volume_spike_ratio or 0.0
        if vol_ratio <= 0 and len(candles_4h) >= 21:
            current_vol = candles_4h[-1].volume
            avg_vol     = sum(c.volume for c in candles_4h[-21:-1]) / 20
            vol_ratio   = current_vol / avg_vol if avg_vol > 0 else 1.0
        # Volume подтверждает движение — засчитывается в counter только если цена идёт против
        if vol_ratio >= _VOLUME_MULT:
            if p4h * sign >= 0:
                met.append(f"Vol×{vol_ratio:.1f}")
            else:
                counter.append(f"Vol×{vol_ratio:.1f}↙" if direction == "long" else f"Vol×{vol_ratio:.1f}↗")

        # ── 3. 1H alignment ─────────────────────────────────────────────
        p1h = price_change_1h or 0.0
        p1h_signed = p1h * sign
        if p1h_signed >= _MOMENTUM_1H:
            met.append(f"1H={p1h:+.1f}%")
        elif p1h_signed <= -_MOMENTUM_1H:
            counter.append(f"1H={p1h:+.1f}%↙" if direction == "long" else f"1H={p1h:+.1f}%↗")

        # ── 4. 1D alignment (#34 Multi-TF) ──────────────────────────────
        p1d = price_change_1d or 0.0
        if p1d == 0.0 and len(candles_4h) >= 7:
            p1d = _candle_change_pct(candles_4h, 6)
        p1d_signed = p1d * sign
        if p1d_signed >= _MOMENTUM_1D:
            met.append(f"1D={p1d:+.1f}%")
        elif p1d_signed <= -_MOMENTUM_1D:
            counter.append(f"1D={p1d:+.1f}%↙" if direction == "long" else f"1D={p1d:+.1f}%↗")

        n  = len(met)
        nc = len(counter)

        # ── Counter-trend PENALTY (входим против тренда — штраф) ────────
        if nc >= 2 and n < 2:
            if nc >= 3:
                penalty = -min(int(_SCORE_PENALTY * 1.5), 18)
            else:
                penalty = -_SCORE_PENALTY
            emoji = "⛔" if direction == "long" else "⛔"
            desc  = (f"{emoji} [COUNTER-TREND {nc}/4] {' | '.join(counter)} → {penalty} к score")
            logger.info(f"[TrendDetector] counter-trend {direction}: {nc}/4 против → penalty={penalty}")
            return TrendResult(
                has_trend=True,
                score_bonus=penalty,
                extend_tp=False,
                conditions=n,
                counter=nc,
                description=desc,
                is_penalty=True,
            )

        # ── Нет чёткого тренда ни в ту ни в другую сторону ─────────────
        if n < 2:
            return _NO_TREND

        # ── Тренд В направлении сделки → бонус ──────────────────────────
        if n == 2:
            bonus = _SCORE_BONUS
        elif n == 3:
            bonus = min(int(_SCORE_BONUS * 1.3), 13)
        else:  # n == 4
            bonus = min(int(_SCORE_BONUS * 1.5), 15)

        emoji = "📈" if direction == "long" else "📉"
        desc  = f"{emoji} [TREND {n}/4] {' | '.join(met)} → +{bonus}"

        return TrendResult(
            has_trend=True,
            score_bonus=bonus,
            extend_tp=_EXTEND_TP,
            conditions=n,
            counter=nc,
            description=desc,
            is_penalty=False,
        )

    except Exception as e:
        logger.debug(f"[TrendDetector] {e}")
        return _NO_TREND
