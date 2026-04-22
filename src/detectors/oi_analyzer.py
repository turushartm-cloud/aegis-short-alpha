"""
OI Analyzer v1.0
Анализ открытого интереса (OI) и ставки финансирования (Funding Rate).

Институциональная логика:
  Сценарий 1: Цена ↑ + OI ↑ = новые длинные = ловушка (SHORT готовится)
  Сценарий 2: Цена ↑ + OI ↓ = шорты закрываются = слабость (не шортим)
  Сценарий 3: Цена ↓ + OI ↑ = новые шорты = ПОДТВЕРЖДЕНИЕ SHORT
  Сценарий 4: Цена ↓ + OI ↓ = длинные закрываются = возможный отскок

Funding Rate Спайк > 0.1% = перегретые лонги = сильный SHORT сигнал
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger("aegis.oi_analyzer")


@dataclass
class FundingConfig:
    lookback_hours:        int   = 24
    oi_change_threshold:   float = 10.0   # % изменения OI для сигнала
    funding_threshold:     float = 0.03   # Базовый порог funding
    funding_spike:         float = 0.10   # Spike порог (очень высокий)
    funding_extreme:       float = 0.20   # Экстремальный (шортить осторожно - уже насыщено)


class OIAnalyzer:
    """
    Анализатор OI + Funding Rate.
    """

    def __init__(self, config: Optional[FundingConfig] = None, binance_client=None):
        self.cfg    = config or FundingConfig()
        self.binance = binance_client

    async def _get_oi_history(self, symbol: str) -> List[Dict]:
        """Получить историю OI из binance клиента"""
        if self.binance:
            try:
                return await self.binance.get_open_interest_history(symbol, "1h", 24)
            except Exception as e:
                logger.debug(f"OI history error {symbol}: {e}")
        return []

    def _analyze_oi_trend(self, oi_history: List[Dict]) -> Dict:
        """Анализ тренда OI"""
        if not oi_history or len(oi_history) < 3:
            return {"trend": "unknown", "change_pct": 0.0, "momentum": 0.0}

        ois = [float(h.get("sumOpenInterest", 0)) for h in oi_history]
        first, last = ois[0], ois[-1]

        change_pct = (last - first) / first * 100 if first > 0 else 0.0

        # OI momentum (ускорение роста)
        mid = ois[len(ois) // 2]
        first_half_chg = (mid - first) / first * 100 if first > 0 else 0
        second_half_chg = (last - mid) / mid * 100 if mid > 0 else 0
        momentum = second_half_chg - first_half_chg   # Ускорение

        trend = "rising" if change_pct > 2 else "falling" if change_pct < -2 else "flat"

        return {
            "trend":       trend,
            "change_pct":  round(change_pct, 2),
            "momentum":    round(momentum, 2),
            "oi_first":    ois[0],
            "oi_last":     ois[-1],
        }

    async def analyze(self, symbol: str, market_data: Any) -> Dict:
        """
        Полный анализ OI + Funding Rate.
        
        Returns: dict с полями:
            score:   0-100
            reasons: List[str]
            metadata: dict
        """
        reasons: List[str] = []
        score   = 0.0

        funding   = getattr(market_data, "funding_rate", 0) or 0
        oi_4d     = getattr(market_data, "oi_change_4d", 0) or 0
        ls_ratio  = getattr(market_data, "long_short_ratio", 50) or 50
        price_1h  = getattr(market_data, "price_change_1h", 0) or 0
        price_4d  = getattr(market_data, "price_change_4d", 0) or 0

        # ── Funding Rate анализ ──────────────────────────────────────
        if funding >= self.cfg.funding_extreme:
            # Экстремальный funding = позиции уже перенасыщены шортами
            # Будем осторожны — возможен short squeeze
            score += 20
            reasons.append(f"⚠️ Funding EXTREME {funding:+.3f}% — риск squeeze")
        elif funding >= self.cfg.funding_spike:
            score += 40
            reasons.append(f"🔴 Funding SPIKE {funding:+.3f}% — лонги перегреты")
        elif funding >= self.cfg.funding_threshold:
            score += 25
            reasons.append(f"Funding повышен {funding:+.3f}%")
        elif funding >= 0.01:
            score += 12
            reasons.append(f"Funding умеренный {funding:+.3f}%")
        elif funding < -0.05:
            score -= 15
            reasons.append(f"Funding отрицательный {funding:+.3f}% — лонги дешевеют, короткий не торопиться")

        # ── OI Dynamic Analysis ──────────────────────────────────────
        oi_history = await self._get_oi_history(symbol)
        oi_trend   = self._analyze_oi_trend(oi_history)

        # Ключевые сценарии из Тезиса
        price_rising = price_1h > 0.5
        price_falling = price_1h < -0.5
        oi_rising  = oi_trend["trend"] == "rising"
        oi_falling = oi_trend["trend"] == "falling"

        if price_rising and oi_rising:
            # Сценарий 1: Ловушка — готовимся к SHORT
            score += 30
            reasons.append(f"🎯 Ловушка: цена+{price_1h:.1f}% + OI+{oi_trend['change_pct']:.1f}%")
        elif price_rising and oi_falling:
            # Сценарий 2: Слабость роста — не шортим агрессивно
            score += 5
            reasons.append(f"↗ Слабый рост: цена+{price_1h:.1f}% + OI-{abs(oi_trend['change_pct']):.1f}%")
        elif price_falling and oi_rising:
            # Сценарий 3: Подтверждение SHORT
            score += 35
            reasons.append(f"✅ SHORT подтверждён: цена{price_1h:.1f}% + OI+{oi_trend['change_pct']:.1f}%")
        elif price_falling and oi_falling:
            # Сценарий 4: Закрытие лонгов — возможен отскок
            score -= 10
            reasons.append(f"⚠️ Лонги закрываются: отскок возможен")

        # OI 4d абсолютный
        if oi_4d > 30:
            score += 25; reasons.append(f"OI +{oi_4d:.1f}% за 4д — massive inflow")
        elif oi_4d > 15:
            score += 15; reasons.append(f"OI +{oi_4d:.1f}% за 4д")
        elif oi_4d > 5:
            score += 8;  reasons.append(f"OI умеренный рост +{oi_4d:.1f}%")
        elif oi_4d < -20:
            score -= 10; reasons.append(f"OI падает {oi_4d:.1f}% — шорты закрываются")

        # L/S Ratio
        if ls_ratio > 70:
            score += 20; reasons.append(f"Long dominance {ls_ratio:.0f}% — массовая ликвидация вероятна")
        elif ls_ratio > 60:
            score += 12; reasons.append(f"Long bias {ls_ratio:.0f}%")
        elif ls_ratio > 55:
            score += 5;  reasons.append(f"L/S {ls_ratio:.0f}% лонгов")
        elif ls_ratio < 35:
            score -= 8;  reasons.append(f"Short overload {ls_ratio:.0f}% лонгов — возможен squeeze")

        # OI momentum (ускорение)
        if oi_trend.get("momentum", 0) > 5:
            score += 10; reasons.append("OI accelerating — institutional inflow")

        score = min(max(score, 0.0), 100.0)

        return {
            "score":   round(score, 1),
            "reasons": reasons,
            "metadata": {
                "funding":     funding,
                "oi_4d":       oi_4d,
                "ls_ratio":    ls_ratio,
                "oi_trend":    oi_trend,
                "price_1h":    price_1h,
            }
        }
