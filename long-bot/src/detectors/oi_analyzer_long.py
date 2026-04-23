"""
OI Analyzer Long v1.0 — Aegis Long Alpha
Зеркало OI Analyzer Short — фокус на отрицательном funding rate.

Логика для LONG:
  Funding < -0.03% → лонги субсидируются шортами → LONG дёшев
  Funding < -0.08% → сильный сигнал LONG (шорты перегреты → squeeze вероятен)
  Funding < -0.15% → экстремальный (Short Squeeze неизбежен)

OI сценарии для LONG:
  Цена ↓ + OI ↑ = шорты накапливаются → потенциальный squeeze (LONG!)
  Цена ↓ + OI ↓ = лонги закрываются → возможный отскок (осторожно)
  Цена ↑ + OI ↑ = лонги открываются → тренд вверх (LONG подтверждение)
  Цена ↑ + OI ↓ = шорты закрываются (short covering) → LONG драйвер
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger("aegis.oi_analyzer_long")


@dataclass
class FundingConfigLong:
    lookback_hours:       int   = 24
    oi_change_threshold:  float = 10.0
    funding_threshold:    float = -0.03   # Порог (отрицательный)
    funding_spike:        float = -0.08   # Spike (перегрет SHORT side)
    funding_extreme:      float = -0.15   # Экстремальный (squeeze неизбежен)


class OIAnalyzerLong:
    """OI + Funding Rate анализатор для LONG стратегии"""

    def __init__(self, config: Optional[FundingConfigLong] = None, binance_client=None):
        self.cfg    = config or FundingConfigLong()
        self.binance = binance_client

    async def _get_oi_history(self, symbol: str) -> List[Dict]:
        if self.binance:
            try:
                return await self.binance.get_open_interest_history(symbol, "1h", 24)
            except Exception:
                pass
        return []

    def _analyze_oi_trend(self, oi_history: List[Dict]) -> Dict:
        if not oi_history or len(oi_history) < 3:
            return {"trend": "unknown", "change_pct": 0.0}
        ois   = [float(h.get("sumOpenInterest", 0)) for h in oi_history]
        first, last = ois[0], ois[-1]
        change_pct = (last - first) / first * 100 if first > 0 else 0.0
        trend = "rising" if change_pct > 2 else "falling" if change_pct < -2 else "flat"
        return {"trend": trend, "change_pct": round(change_pct, 2)}

    async def analyze(self, symbol: str, market_data: Any) -> Dict:
        reasons: List[str] = []
        score   = 0.0

        funding  = getattr(market_data, "funding_rate", 0) or 0
        oi_4d    = getattr(market_data, "oi_change_4d", 0) or 0
        ls_ratio = getattr(market_data, "long_short_ratio", 50) or 50
        price_1h = getattr(market_data, "price_change_1h", 0) or 0

        # ── Funding Rate (главный триггер для LONG) ──────────────────
        if funding <= self.cfg.funding_extreme:
            # Short Squeeze неизбежен — максимальный сигнал
            score += 45
            reasons.append(f"🚨 Funding EXTREME {funding:+.3f}% — Short Squeeze вероятен")
        elif funding <= self.cfg.funding_spike:
            score += 35
            reasons.append(f"🟢 Funding SPIKE {funding:+.3f}% — шорты перегреты, LONG дёшев")
        elif funding <= self.cfg.funding_threshold:
            score += 20
            reasons.append(f"Funding отрицательный {funding:+.3f}% — LONG субсидируется")
        elif funding < 0:
            score += 8
            reasons.append(f"Funding умеренно отрицательный {funding:+.3f}%")
        elif funding > 0.10:
            score -= 15
            reasons.append(f"Funding высокий +{funding:.3f}% — лонги перегреты, против LONG")
        elif funding > 0.05:
            score -= 8
            reasons.append(f"Funding повышен +{funding:.3f}% — осторожно для LONG")

        # ── OI + Price сценарии для LONG ─────────────────────────────
        oi_history = await self._get_oi_history(symbol)
        oi_trend   = self._analyze_oi_trend(oi_history)

        price_falling = price_1h < -0.5
        price_rising  = price_1h > 0.5
        oi_rising     = oi_trend["trend"] == "rising"
        oi_falling    = oi_trend["trend"] == "falling"

        if price_falling and oi_rising:
            # Шорты накапливаются при падении = Short Squeeze setup
            score += 30
            reasons.append(f"⚡ Short Squeeze setup: цена↓ + OI↑{oi_trend['change_pct']:+.1f}%")
        elif price_falling and oi_falling:
            # Лонги закрываются — возможный отскок
            score += 10
            reasons.append("Лонги закрываются — отскок возможен, осторожно")
        elif price_rising and oi_rising:
            # Лонги открываются = тренд продолжается
            score += 20
            reasons.append(f"Тренд вверх: цена↑ + OI↑{oi_trend['change_pct']:+.1f}%")
        elif price_rising and oi_falling:
            # Short covering = LONG дополнительный драйвер
            score += 15
            reasons.append("Short covering: цена↑ при OI↓ — шорты закрываются")

        # ── OI 4d абсолютный ─────────────────────────────────────────
        if oi_4d < -25:
            score += 20; reasons.append(f"OI -{abs(oi_4d):.1f}% за 4д — массовое закрытие шортов")
        elif oi_4d < -10:
            score += 10; reasons.append(f"OI -{abs(oi_4d):.1f}% — шорты закрываются")
        elif oi_4d > 20 and price_falling:
            # Шорты накапливаются на падении = squeeze setup
            score += 25; reasons.append(f"OI +{oi_4d:.1f}% при падении — Short Squeeze setup")

        # ── L/S Ratio (для LONG хотим перекос в SHORT side) ──────────
        short_ratio = 100 - ls_ratio  # % шортов
        if short_ratio > 65:
            score += 20; reasons.append(f"Short dominance {short_ratio:.0f}% — squeeze вероятен")
        elif short_ratio > 55:
            score += 10; reasons.append(f"Short bias {short_ratio:.0f}%")
        elif ls_ratio > 65:
            score -= 8;  reasons.append(f"Long overload {ls_ratio:.0f}% — осторожно")

        score = min(max(score, 0.0), 100.0)

        return {
            "score":   round(score, 1),
            "reasons": reasons,
            "metadata": {
                "funding":   funding,
                "oi_4d":     oi_4d,
                "ls_ratio":  ls_ratio,
                "oi_trend":  oi_trend,
                "price_1h":  price_1h,
                "short_pct": round(100 - ls_ratio, 1),
            }
        }
