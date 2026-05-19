"""
Delta Analyzer Long v1.0
Анализ бычьего потока ордеров через OHLCV суррогат CVD.

Bull delta candle: close > open → покупатели доминируют
Delta = Volume × (close - open) / (high - low)

Для LONG: ищем нарастающий бычий CVD поток.
"""

from __future__ import annotations
import logging
from typing import Dict, List

logger = logging.getLogger("aegis.delta_analyzer_long")


class DeltaAnalyzerLong:
    """Order Flow Delta через OHLCV суррогат — бычья версия для LONG"""

    async def analyze(self, symbol: str, ohlcv: list) -> Dict:
        reasons: List[str] = []
        score = 0.0

        if not ohlcv or len(ohlcv) < 10:
            logger.warning(f"[DeltaLong] {symbol}: недостаточно данных (len={len(ohlcv) if ohlcv else 0})")
            return {"score": 25.0, "reasons": ["Delta: недостаточно данных"], "metadata": {}}

        try:
            recent = ohlcv[-20:]
            deltas = []
            for c in recent:
                rng = c.high - c.low
                if rng > 0:
                    body = c.close - c.open
                    vol = getattr(c, "volume", 0) or getattr(c, "quote_volume", 0)
                    delta = vol * (body / rng)
                else:
                    delta = 0.0
                deltas.append(delta)

            cvd_total = sum(deltas)
            cvd_5     = sum(deltas[-5:])
            cvd_10    = sum(deltas[-10:])

            total_vol = sum(abs(d) for d in deltas) or 1
            bull_flow_pct = max(cvd_total, 0) / total_vol * 100

            bull_candles = sum(1 for c in ohlcv[-10:] if c.close > c.open)
            bull_ratio   = bull_candles / 10

            logger.debug(
                f"[DeltaLong] {symbol}: cvd_5={cvd_5:.0f} cvd_10={cvd_10:.0f} cvd_total={cvd_total:.0f} "
                f"bull={bull_candles}/10 bull_flow={bull_flow_pct:.1f}%"
            )

            if cvd_5 > 0 and cvd_10 > 0:
                score += 40
                reasons.append(f"CVD бычий: 5c={cvd_5:.0f} 10c={cvd_10:.0f}")
            elif cvd_5 > 0:
                score += 25
                reasons.append(f"Краткосрочный бычий поток CVD={cvd_5:.0f}")
            elif cvd_total > 0:
                score += 15
                reasons.append("Накопленный бычий CVD")

            if bull_ratio >= 0.8:
                score += 35
                reasons.append(f"80%+ бычьих свечей ({bull_candles}/10)")
            elif bull_ratio >= 0.6:
                score += 22
                reasons.append("60%+ бычьих свечей")
            elif bull_ratio >= 0.4:
                score += 10
                reasons.append(f"Нейтральный поток {bull_candles}/10 бычьих")
            else:
                reasons.append("Медвежий поток — осторожно для LONG")

            if bull_flow_pct > 70:
                score += 20
                reasons.append(f"Бычий поток {bull_flow_pct:.0f}% объёма")

            score = min(max(score, 0), 100)

            return {
                "score":   round(score, 1),
                "reasons": reasons,
                "metadata": {
                    "cvd_5":        round(cvd_5, 0),
                    "cvd_10":       round(cvd_10, 0),
                    "cvd_total":    round(cvd_total, 0),
                    "bull_ratio":   bull_ratio,
                    "bull_flow_pct": round(bull_flow_pct, 1),
                },
            }

        except Exception as e:
            logger.warning(f"[DeltaLong] {symbol}: ошибка расчёта — {type(e).__name__}: {e}")
            return {"score": 25.0, "reasons": ["Delta: ошибка расчёта"], "metadata": {}}

    def detect_divergence(self, ohlcv: list, lookback: int = 10) -> dict:
        """#21: Delta Divergence — бычья дивергенция для LONG."""
        result = {"bearish": False, "bullish": False, "score_bonus": 0, "reason": ""}
        if not ohlcv or len(ohlcv) < lookback + 2:
            return result
        try:
            recent = ohlcv[-lookback:]
            prices = [c.close for c in recent]
            cvd_series, running = [], 0.0
            for c in recent:
                rng  = c.high - c.low
                body = c.close - c.open
                vol  = getattr(c, "volume", 0) or getattr(c, "quote_volume", 0)
                running += vol * (body / rng) if rng > 0 else 0.0
                cvd_series.append(running)
            half = len(prices) // 2
            p_fmin, p_smin = min(prices[:half]), min(prices[half:])
            c_fmin, c_smin = min(cvd_series[:half]), min(cvd_series[half:])
            # Бычья дивергенция: цена LL, CVD HL
            if p_smin < p_fmin * 0.998 and c_smin > c_fmin * 1.02:
                result["bullish"]     = True
                result["score_bonus"] = 18
                result["reason"]      = (
                    f"📈 [DELTA_DIV] Бычья дивергенция: "
                    f"Price LL={p_smin:.4f} < {p_fmin:.4f}, CVD HL={c_smin:.0f} > {c_fmin:.0f}"
                )
                logger.info(result["reason"])
        except Exception as e:
            logger.debug(f"[DeltaDiv long]: {e}")
        return result
