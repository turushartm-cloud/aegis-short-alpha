"""
PrePumpDetector v1.1 — Детектор тихого накопления/распределения

Ловит setup ДО взрывного движения (APE/KAITO/EDEN/FHE тип):
  LONG setup:  ATR сжатие + узкий канал + OI растёт + funding нейтральный/отрицательный
               + цена в нижних 40% диапазона (накопление)
  SHORT setup: ATR сжатие + узкий канал + OI растёт + funding положительный (лонги переплачивают)
               + цена в верхних 40% диапазона (распределение) + RSI бонус ≥ 60/70

Интегрирован в AegisSignalEngine как 4-й bypass для z_volume gate:
  score 60-74 → z_gate ослабляется до 60%
  score ≥ 75 + OI подтверждён → z_gate полностью отключается

ENV параметры:
  ENABLE_PRE_PUMP_DETECTOR  (default: true)
  PRE_PUMP_ATR_SQUEEZE      (default: 0.65)  — ATR/SMA_ATR < X = сжатие
  PRE_PUMP_RANGE_PCT        (default: 3.5)   — (MaxH-MinL)/Close < X% за 20 баров
  PRE_PUMP_OI_MIN_PCT       (default: 2.0)   — oi_change_4h ≥ X% для подтверждения OI
  PRE_PUMP_SOFT_SCORE       (default: 60)    — score ≥ X → soften z_gate × 0.6
  PRE_PUMP_BYPASS_SCORE     (default: 75)    — score ≥ X + OI → полный bypass z_gate
  PRE_PUMP_LONG_FUND_MAX    (default: 0.01)  — funding ≤ X% считается нейтральным для LONG
  PRE_PUMP_SHORT_FUND_MIN   (default: 0.03)  — funding ≥ X% для SHORT (перегрев лонгов)
  PRE_PUMP_PRICE_POSITION   (default: 0.40)  — LONG: цена в нижних X%; SHORT: в верхних X%
"""

from __future__ import annotations

import os
import logging
from typing import Dict, Any, List

logger = logging.getLogger("aegis.pre_pump")

# ── ENV config ──────────────────────────────────────────────────────────────
ENABLED             = os.getenv("ENABLE_PRE_PUMP_DETECTOR", "true").lower() == "true"
ATR_SQUEEZE_RATIO   = float(os.getenv("PRE_PUMP_ATR_SQUEEZE",    "0.65"))
RANGE_PCT_MAX       = float(os.getenv("PRE_PUMP_RANGE_PCT",      "3.5"))
OI_MIN_PCT          = float(os.getenv("PRE_PUMP_OI_MIN_PCT",     "2.0"))
SOFT_SCORE          = int(float(os.getenv("PRE_PUMP_SOFT_SCORE",     "60")))
BYPASS_SCORE        = int(float(os.getenv("PRE_PUMP_BYPASS_SCORE",   "75")))
LONG_FUND_MAX       = float(os.getenv("PRE_PUMP_LONG_FUND_MAX",  "0.01"))   # funding ≤ +0.01% = нейтральный
SHORT_FUND_MIN      = float(os.getenv("PRE_PUMP_SHORT_FUND_MIN", "0.03"))   # funding ≥ +0.03% = перегрев
PRICE_POSITION_PCT  = float(os.getenv("PRE_PUMP_PRICE_POSITION", "0.40"))   # LONG: нижние 40%; SHORT: верхние 40%
MIN_CANDLES         = 20


class PrePumpDetector:
    """
    Детектор тихого накопления/распределения.
    Вызывается из generate_signal() обоих signal engines.
    """

    def detect(self, ohlcv: list, market_data: Any, direction: str = "long") -> Dict[str, Any]:
        """
        ohlcv: список [ts, open, high, low, close, volume] — обычно 15m свечи
        market_data: объект MarketData (oi_change_1h/4h, funding_rate)
        direction: "long" | "short"

        Возвращает:
          detected:         bool
          score:            int  0-100
          oi_confirmed:     bool
          funding_aligned:  bool
          atr_squeeze:      float
          range_pct:        float
          z_gate_action:    "bypass" | "soften" | "none"
          reasons:          list[str]
        """
        result: Dict[str, Any] = {
            "detected": False, "score": 0,
            "oi_confirmed": False, "funding_aligned": False,
            "atr_squeeze": 0.0, "range_pct": 0.0,
            "price_position_ratio": 0.0,
            "z_gate_action": "none",
            "reasons": [],
        }

        if not ENABLED or not ohlcv or len(ohlcv) < MIN_CANDLES:
            return result

        try:
            closes = [float(c[4]) for c in ohlcv]
            highs  = [float(c[2]) for c in ohlcv]
            lows   = [float(c[3]) for c in ohlcv]
            opens  = [float(c[1]) for c in ohlcv]
        except (IndexError, ValueError, TypeError):
            return result

        reasons: List[str] = []
        score = 0

        # ── 1. ATR squeeze ──────────────────────────────────────────────────
        atr_vals = _calc_atr(highs, lows, closes, period=14)
        atr_ok = False
        if len(atr_vals) >= MIN_CANDLES:
            current_atr = atr_vals[-1]
            sma_atr = sum(atr_vals[-MIN_CANDLES:]) / MIN_CANDLES
            if sma_atr > 0:
                atr_ratio = current_atr / sma_atr
                result["atr_squeeze"] = round(atr_ratio, 3)
                if atr_ratio < ATR_SQUEEZE_RATIO:
                    atr_ok = True
                    score += 35
                    reasons.append(f"ATR сжатие: {atr_ratio:.2f}×SMA → цена консолидирует")

        # ── 2. Узкий диапазон (20 баров) ────────────────────────────────────
        range_ok = False
        if closes:
            recent_h = max(highs[-MIN_CANDLES:])
            recent_l = min(lows[-MIN_CANDLES:])
            cur_close = closes[-1]
            if cur_close > 0:
                range_pct = (recent_h - recent_l) / cur_close * 100
                result["range_pct"] = round(range_pct, 2)
                if range_pct < RANGE_PCT_MAX:
                    range_ok = True
                    score += 25
                    reasons.append(f"Узкий канал: {range_pct:.1f}% за {MIN_CANDLES} баров")

        # ── 3. Позиция цены в диапазоне (жёсткое условие) ──────────────────
        position_ok = False
        if closes and range_ok:
            recent_h = max(highs[-MIN_CANDLES:])
            recent_l = min(lows[-MIN_CANDLES:])
            range_width = recent_h - recent_l
            if range_width > 0:
                pos_ratio = (closes[-1] - recent_l) / range_width
                result["price_position_ratio"] = round(pos_ratio, 3)
                if direction == "long" and pos_ratio < PRICE_POSITION_PCT:
                    position_ok = True
                    score += 15
                    reasons.append(
                        f"Позиция цены: {pos_ratio:.0%} от дна диапазона — зона накопления LONG"
                    )
                elif direction == "short" and pos_ratio > (1.0 - PRICE_POSITION_PCT):
                    position_ok = True
                    score += 15
                    reasons.append(
                        f"Позиция цены: {pos_ratio:.0%} от дна диапазона — зона распределения SHORT"
                    )
                else:
                    if direction == "long":
                        reasons.append(
                            f"Позиция цены {pos_ratio:.0%} слишком высоко для LONG (нужно < {PRICE_POSITION_PCT:.0%})"
                        )
                    else:
                        reasons.append(
                            f"Позиция цены {pos_ratio:.0%} слишком низко для SHORT (нужно > {1.0 - PRICE_POSITION_PCT:.0%})"
                        )

        # ── 4. Свечи-дожи (тихое торможение) ─────────────────────────────
        doji_count = 0
        for i in range(-5, 0):
            try:
                h, l, c, o = highs[i], lows[i], closes[i], opens[i]
                candle_range = h - l
                if candle_range > 0 and abs(c - o) / candle_range < 0.35:
                    doji_count += 1
            except IndexError:
                pass
        if doji_count >= 3:
            score += 10
            reasons.append(f"Свечи-дожи: {doji_count}/5 последних баров — равновесие покупок/продаж")

        # ── 5. OI — растёт при тихой цене ──────────────────────────────────
        oi_1h = getattr(market_data, "oi_change_1h", 0.0) or 0.0
        oi_4h = getattr(market_data, "oi_change_4h", 0.0) or 0.0
        oi_confirmed = (
            oi_4h >= OI_MIN_PCT
            or (oi_1h >= 0.8 and oi_4h >= 1.2)  # оба положительных
        )
        result["oi_confirmed"] = oi_confirmed
        if oi_confirmed:
            score += 20
            reasons.append(f"OI растёт: 4h={oi_4h:+.1f}% 1h={oi_1h:+.1f}% — накопление позиций")

        # ── 6. Funding aligned ──────────────────────────────────────────────
        funding = getattr(market_data, "funding_rate", 0.0) or 0.0
        funding_aligned = (
            (direction == "long"  and funding <= LONG_FUND_MAX)   # нейтральный/отрицательный = шорты не переплачивают
            or (direction == "short" and funding >= SHORT_FUND_MIN)   # лонги переплачивают
        )
        result["funding_aligned"] = funding_aligned
        if funding_aligned:
            score += 10
            if direction == "long":
                reasons.append(f"Funding нейтральный: {funding:.4f}% — нет overheated longs")
            else:
                reasons.append(f"Funding перегрет: {funding:+.4f}% — лонги переплачивают")

        # ── 7. RSI бонус для SHORT (лёгкий бонус, не блокирующее условие) ───
        if direction == "short":
            rsi_1h = getattr(market_data, "rsi_1h", 50) or 50
            if rsi_1h >= 70:
                score += 15
                reasons.append(f"RSI перекуплен: {rsi_1h:.0f} ≥ 70 — зона разворота SHORT")
            elif rsi_1h >= 60:
                score += 10
                reasons.append(f"RSI повышен: {rsi_1h:.0f} ≥ 60 — умеренный потенциал SHORT")

        # ── Итог ────────────────────────────────────────────────────────────
        result["score"] = min(score, 100)

        # Detected = хотя бы 2 из 3 структурных условий + OI + позиция цены (жёсткое требование)
        structural_ok = sum([atr_ok, range_ok, doji_count >= 3]) >= 2
        result["detected"] = structural_ok and oi_confirmed and position_ok

        if result["detected"]:
            # Определяем действие с z_gate
            if score >= BYPASS_SCORE and oi_confirmed:
                result["z_gate_action"] = "bypass"
            elif score >= SOFT_SCORE:
                result["z_gate_action"] = "soften"

        result["reasons"] = reasons
        if result["detected"]:
            logger.info(
                f"[PrePump] {direction.upper()} setup: score={score} "
                f"atr_ok={atr_ok} range_ok={range_ok} pos={position_ok} "
                f"oi={oi_confirmed} funding={funding_aligned} → {result['z_gate_action']}"
            )

        return result


# ── ATR helper ───────────────────────────────────────────────────────────────

def _calc_atr(highs: list, lows: list, closes: list, period: int = 14) -> list:
    """Wilder's ATR."""
    if len(closes) < period + 1:
        return []
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return []
    current = sum(trs[:period]) / period
    atr_vals = [current]
    for i in range(period, len(trs)):
        current = (current * (period - 1) + trs[i]) / period
        atr_vals.append(current)
    return atr_vals


# Singleton (один экземпляр на процесс)
_detector: PrePumpDetector | None = None

def get_pre_pump_detector() -> PrePumpDetector:
    global _detector
    if _detector is None:
        _detector = PrePumpDetector()
    return _detector
