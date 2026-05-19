"""
Delta Analyzer v1.0
Анализ дисбаланса потока ордеров через свечные данные.

Используем CVD (Cumulative Volume Delta) суррогат через OHLCV:
  Bear delta candle: close < open → продавцы доминируют
  Bull delta candle: close > open → покупатели доминируют
  Delta = Volume × (close - open) / (high - low)

Для SHORT: ищем нарастающий медвежий дельта поток.
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
logger = logging.getLogger("aegis.delta_analyzer")


class DeltaAnalyzer:
    """Order Flow Delta через OHLCV суррогат"""

    async def analyze(self, symbol: str, ohlcv: list) -> Dict:
        reasons: List[str] = []
        score = 0.0

        if not ohlcv or len(ohlcv) < 10:
            logger.warning(f"[Delta] {symbol}: недостаточно данных (len={len(ohlcv) if ohlcv else 0})")
            return {"score": 25.0, "reasons": ["Delta: недостаточно данных"],
                    "metadata": {}}

        try:
            # CVD суррогат (последние 20 свечей)
            recent = ohlcv[-20:]
            deltas = []
            for c in recent:
                rng = c.high - c.low
                if rng > 0:
                    body = c.close - c.open
                    vol = getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0)
                    delta = vol * (body / rng)
                else:
                    delta = 0.0
                deltas.append(delta)

            # Суммарный CVD за период
            cvd_total = sum(deltas)
            cvd_5     = sum(deltas[-5:])
            cvd_10    = sum(deltas[-10:])

            # Нормализованный медвежий поток
            total_vol = sum(abs(d) for d in deltas) or 1
            bear_flow_pct = abs(min(cvd_total, 0)) / total_vol * 100

            # Медвежьих свечей из последних 10
            bear_candles = sum(1 for c in ohlcv[-10:] if c.close < c.open)
            bear_ratio   = bear_candles / 10

            logger.debug(
                f"[Delta] {symbol}: cvd_5={cvd_5:.0f} cvd_10={cvd_10:.0f} cvd_total={cvd_total:.0f} "
                f"bear={bear_candles}/10 bear_flow={bear_flow_pct:.1f}%"
            )

            # Скоринг
            if cvd_5 < 0 and cvd_10 < 0:
                score += 40; reasons.append(f"CVD медвежий: 5c={cvd_5:.0f} 10c={cvd_10:.0f}")
            elif cvd_5 < 0:
                score += 25; reasons.append(f"Краткосрочный медвежий поток CVD={cvd_5:.0f}")
            elif cvd_total < 0:
                score += 15; reasons.append("Накопленный медвежий CVD")

            if bear_ratio >= 0.8:
                score += 35; reasons.append(f"80%+ медвежьих свечей ({bear_candles}/10)")
            elif bear_ratio >= 0.6:
                score += 22; reasons.append(f"60%+ медвежьих свечей")
            elif bear_ratio >= 0.4:
                score += 10; reasons.append(f"Нейтральный поток {bear_candles}/10 медвежьих")
            else:
                score += 0;  reasons.append("Бычий поток — осторожно для SHORT")

            # Bear flow dominance
            if bear_flow_pct > 70:
                score += 20; reasons.append(f"Медвежий поток {bear_flow_pct:.0f}% объёма")

            score = min(max(score, 0), 100)

            logger.debug(f"[Delta] {symbol}: итоговый score={score:.1f}")

            return {
                "score":   round(score, 1),
                "reasons": reasons,
                "metadata": {
                    "cvd_5":    round(cvd_5, 0),
                    "cvd_10":   round(cvd_10, 0),
                    "cvd_total": round(cvd_total, 0),
                    "bear_ratio": bear_ratio,
                    "bear_flow_pct": round(bear_flow_pct, 1),
                }
            }

        except Exception as e:
            logger.warning(f"[Delta] {symbol}: ошибка расчёта — {type(e).__name__}: {e}")
            return {"score": 25.0, "reasons": ["Delta: ошибка расчёта"], "metadata": {}}

    def detect_divergence(self, ohlcv: list, lookback: int = 10) -> dict:
        """
        #21 Delta Divergence:
        Price Higher-High + CVD Lower-High → скрытая медвежья дивергенция (SHORT сигнал).
        Price Lower-Low  + CVD Higher-Low  → скрытая бычья дивергенция (LONG сигнал, для справки).

        Returns: {"bearish": bool, "bullish": bool, "score_bonus": int, "reason": str}
        """
        result = {"bearish": False, "bullish": False, "score_bonus": 0, "reason": ""}
        if not ohlcv or len(ohlcv) < lookback + 2:
            return result
        try:
            recent = ohlcv[-lookback:]
            prices = [c.close for c in recent]
            # CVD суррогат
            cvd_series = []
            running = 0.0
            for c in recent:
                rng = c.high - c.low
                body = c.close - c.open
                vol  = getattr(c, "volume", 0) or getattr(c, "quote_volume", 0)
                delta = vol * (body / rng) if rng > 0 else 0.0
                running += delta
                cvd_series.append(running)

            # Ищем два пика в ценах и CVD
            half = len(prices) // 2
            price_first_half_max = max(prices[:half])
            price_second_half_max = max(prices[half:])
            cvd_first_half_max   = max(cvd_series[:half])
            cvd_second_half_max  = max(cvd_series[half:])

            price_first_half_min = min(prices[:half])
            price_second_half_min = min(prices[half:])
            cvd_first_half_min   = min(cvd_series[:half])
            cvd_second_half_min  = min(cvd_series[half:])

            # Медвежья дивергенция: цена HH, CVD LH
            if (price_second_half_max > price_first_half_max * 1.002 and
                    cvd_second_half_max < cvd_first_half_max * 0.98):
                result["bearish"]     = True
                result["score_bonus"] = 18
                result["reason"]      = (
                    f"📉 [DELTA_DIV] Медвежья дивергенция: "
                    f"Price HH={price_second_half_max:.4f} > {price_first_half_max:.4f}, "
                    f"CVD LH={cvd_second_half_max:.0f} < {cvd_first_half_max:.0f}"
                )
                logger.info(result["reason"])

            # Бычья дивергенция: цена LL, CVD HL
            elif (price_second_half_min < price_first_half_min * 0.998 and
                    cvd_second_half_min > cvd_first_half_min * 1.02):
                result["bullish"]     = True
                result["score_bonus"] = 18
                result["reason"]      = (
                    f"📈 [DELTA_DIV] Бычья дивергенция: "
                    f"Price LL={price_second_half_min:.4f} < {price_first_half_min:.4f}, "
                    f"CVD HL={cvd_second_half_min:.0f} > {cvd_first_half_min:.0f}"
                )

        except Exception as e:
            logger.debug(f"[DeltaDiv]: {e}")
        return result


# ─────────────────────────────────────────────────────────────────────
# __init__ for detectors package
# ─────────────────────────────────────────────────────────────────────
