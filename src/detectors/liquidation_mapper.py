"""
Liquidation Mapper v1.0
Анализ кластеров ликвидаций через L/S Ratio + Funding + Price Structure.
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
logger = logging.getLogger("aegis.liq_mapper")


class LiquidationMapper:
    """
    Картирует потенциальные зоны ликвидации лонгов.
    Для SHORT нас интересуют LONG liquidation clusters (цена падает → лонги ликвидируются).
    """

    async def analyze(self, symbol: str, market_data: Any) -> Dict:
        reasons: List[str] = []
        score = 0.0

        try:
            funding   = getattr(market_data, "funding_rate", 0) or 0
            ls_ratio  = getattr(market_data, "long_short_ratio", 50) or 50
            price_4d  = getattr(market_data, "price_change_4d", 0) or 0
            price_1h  = getattr(market_data, "price_change_1h", 0) or 0
            oi_4d     = getattr(market_data, "oi_change_4d", 0) or 0

            # ── Зона ликвидации лонгов ──────────────────────────────
            # Высокий L/S + памп = лонги на верхушке = потенциальные ликвидации при падении
            long_liq_score = 0.0

            if ls_ratio > 65 and price_4d > 15:
                long_liq_score = 80
                reasons.append(f"💥 LONG LIQ CLUSTER: {ls_ratio:.0f}% лонгов после +{price_4d:.1f}%")
            elif ls_ratio > 60 and price_4d > 8:
                long_liq_score = 60
                reasons.append(f"⚠️ Long exposure high: {ls_ratio:.0f}% после +{price_4d:.1f}%")
            elif ls_ratio > 55:
                long_liq_score = 35
                reasons.append(f"Long bias: {ls_ratio:.0f}%")

            # Funding + Long = перегрет → ликвидации вероятны
            if funding > 0.08 and ls_ratio > 60:
                long_liq_score = min(long_liq_score + 20, 100)
                reasons.append(f"Funding+Long combo: классический pump-dump setup")

            # OI рост при цене вверх = открываются новые лонги = будущие жертвы
            if oi_4d > 20 and price_4d > 10:
                long_liq_score = min(long_liq_score + 15, 100)
                reasons.append(f"OI+Price up: новые лонги на верхушке — prime SHORT target")

            score = long_liq_score

            # Negative factors
            if price_1h < -3:
                # Уже падает быстро — часть ликвидаций уже прошла
                score *= 0.8
                reasons.append("⬇️ Падение уже идёт — часть ликвидаций прошла")

        except Exception as e:
            logger.warning(f"liq_mapper error {symbol}: {e}")
            score = 20.0

        return {
            "score":   round(min(score, 100), 1),
            "reasons": reasons,
            "metadata": {
                "ls_ratio": getattr(market_data, "long_short_ratio", 50),
                "price_4d": getattr(market_data, "price_change_4d", 0),
                "funding":  getattr(market_data, "funding_rate", 0),
            }
        }
