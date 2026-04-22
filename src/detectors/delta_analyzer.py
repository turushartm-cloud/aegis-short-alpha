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
            logger.warning(f"delta_analyze error {symbol}: {e}")
            return {"score": 25.0, "reasons": ["Delta: ошибка расчёта"], "metadata": {}}


# ─────────────────────────────────────────────────────────────────────
# __init__ for detectors package
# ─────────────────────────────────────────────────────────────────────
