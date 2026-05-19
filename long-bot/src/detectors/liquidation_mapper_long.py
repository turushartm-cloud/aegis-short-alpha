"""
Liquidation Mapper Long v1.1
Анализ зон потенциальных SHORT-сквизов + liquidation cluster proximity для LONG бота.

Для LONG нас интересуют SHORT liquidation clusters:
  цена растёт → шорты ликвидируются → цена ускоряется.

Метрики:
  - Short ratio (= 100 - ls_ratio): высокий = толпа шортит = squeeze fuel
  - Funding rate < 0: шорты платят лонгам → накапливается давление squeeze
  - OI рост при падении цены = новые шорты на дне = prime squeeze target
  - price_4d < -15%: рынок давили → много шортов открыто

Cluster Proximity (v1.1):
  Шорты с плечом 50x стопятся на +2%, 20x на +5%, 10x на +10% выше входа.
  Если short_ratio высокий + цена недавно упала = шорты открыты ниже текущей цены
  → их стопы находятся ВЫШЕ текущей цены = price magnets для LONG TP.
  Близость кластера (2-4%) = сильный магнит, (4-6%) = средний, (6-8%) = слабый.
"""
from __future__ import annotations
import logging
from typing import Any, Dict, List

logger = logging.getLogger("aegis.liq_mapper_long")


class LiquidationMapperLong:
    """
    Картирует потенциальные зоны ликвидации шортов (short squeeze).
    SHORT liquidation clusters = магниты для LONG — цена притягивается к ним снизу вверх.
    """

    async def analyze(self, symbol: str, market_data: Any) -> Dict:
        reasons: List[str] = []
        score = 0.0

        try:
            funding    = getattr(market_data, "funding_rate", 0) or 0
            ls_ratio   = getattr(market_data, "long_short_ratio", 50) or 50
            price_4d   = getattr(market_data, "price_change_4d", 0) or 0
            price_1h   = getattr(market_data, "price_change_1h", 0) or 0
            oi_4d      = getattr(market_data, "oi_change_4d", 0) or 0
            short_ratio = 100 - ls_ratio  # % открытых шортов

            # ── Зона ликвидации шортов ───────────────────────────────────
            # Высокий short ratio + dump = шорты на дне = potential squeeze
            short_liq_score = 0.0

            if short_ratio > 65 and price_4d < -15:
                short_liq_score = 80
                reasons.append(
                    f"💥 SHORT SQUEEZE ZONE: {short_ratio:.0f}% шортов после -{abs(price_4d):.1f}%"
                )
            elif short_ratio > 60 and price_4d < -8:
                short_liq_score = 60
                reasons.append(
                    f"⚠️ Short exposure high: {short_ratio:.0f}% после -{abs(price_4d):.1f}%"
                )
            elif short_ratio > 55:
                short_liq_score = 35
                reasons.append(f"Short bias: {short_ratio:.0f}% — толпа против рынка")

            # Отрицательный funding + шортовый перекос = шорты переплачивают → сквиз нарастает
            if funding < -0.05 and short_ratio > 55:
                short_liq_score = min(short_liq_score + 20, 100)
                reasons.append(
                    f"Funding {funding:.3f}% (neg) + short bias — short squeeze давление"
                )

            # OI растёт при падении = новые шорты открываются = будущие жертвы сквиза
            if oi_4d > 20 and price_4d < -10:
                short_liq_score = min(short_liq_score + 15, 100)
                reasons.append(
                    f"OI +{oi_4d:.1f}% при цене -{abs(price_4d):.1f}% — новые шорты на дне"
                )

            score = short_liq_score

            # Негативный фактор: уже растём быстро — часть сквизов прошла
            if price_1h > 3:
                score *= 0.8
                reasons.append("⬆️ Рост уже идёт — часть short squeeze прошла")

            # ── Cluster Proximity: SHORT liq clusters ВЫШЕ текущей цены = TP magnets ──
            # Шорты открыты ниже. Их стопы (на общих плечах) — выше. Эти кластеры притягивают цену.
            # Эффект тем сильнее, чем больше шортов и чем ближе их стопы к текущей цене.
            cluster_bonus = 0.0
            if short_ratio > 55 and price_4d < -5:
                # Оцениваем распределение плечей и близость кластеров
                # 50x → стопы +2%, 20x → +5%, 10x → +10% от текущей цены
                # При высоком short_ratio распределяем условно 25% на 50x, 40% на 20x, 35% на 10x

                # Самый сильный магнит — кластер 50x стопов (+2%)
                if short_ratio > 65:
                    cluster_bonus += 18  # Плотный кластер на +2% — очень сильный магнит
                    reasons.append(f"🎯 LIQ CLUSTER +2%: {short_ratio:.0f}% шортов → плотный кластер стопов")
                elif short_ratio > 60:
                    cluster_bonus += 12
                    reasons.append(f"🎯 LIQ CLUSTER +2-5%: {short_ratio:.0f}% шортов на дне")
                else:
                    cluster_bonus += 6
                    reasons.append(f"🎯 Кластер стопов шортов в зоне +2-8%")

                # Дополнительный бонус за глубину дампа (больше шортов = плотнее кластер)
                if price_4d < -20:
                    cluster_bonus += 8
                    reasons.append(f"Дамп -{abs(price_4d):.0f}% создал плотный кластер шортов")
                elif price_4d < -10:
                    cluster_bonus += 4

                score = min(score + cluster_bonus, 100)

            # Estimated TP magnet distance (для использования в main.py)
            tp_magnet_pct = None
            if short_ratio > 55 and price_4d < -5:
                if short_ratio > 65:
                    tp_magnet_pct = 2.5   # 50x stops
                elif short_ratio > 60:
                    tp_magnet_pct = 4.5   # 20x stops
                else:
                    tp_magnet_pct = 7.0   # 10x stops

        except Exception as e:
            logger.warning(f"liq_mapper_long error {symbol}: {e}")
            score = 20.0
            tp_magnet_pct = None

        return {
            "score":   round(min(score, 100), 1),
            "reasons": reasons,
            "metadata": {
                "ls_ratio":      getattr(market_data, "long_short_ratio", 50),
                "short_ratio":   100 - (getattr(market_data, "long_short_ratio", 50) or 50),
                "price_4d":      getattr(market_data, "price_change_4d", 0),
                "funding":       getattr(market_data, "funding_rate", 0),
                "tp_magnet_pct": tp_magnet_pct,   # % выше цены = TP магнит
            }
        }
