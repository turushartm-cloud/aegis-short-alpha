"""
Aegis Signal Engine v1.0
Агрегация всех детекторов в единый взвешенный score для SHORT стратегии.

Веса компонентов (из Master TZ):
  pump_dump:      0.25  — Z-Score deviation от VWAP
  oi_funding:     0.20  — OI + Funding rate dynamics
  liquidation:    0.20  — Long liq clusters proximity
  smc_structure:  0.25  — CHoCH + FVG + Order Blocks
  delta_flow:     0.10  — Order flow imbalance

Пороги: ULTRA ≥85 | STRONG ≥70 | MODERATE ≥60 | WATCH ≥50
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("aegis.signal_engine")


# ─────────────────────────────────────────────────────────────────────
# Enums & Dataclasses
# ─────────────────────────────────────────────────────────────────────

class SignalStrength(Enum):
    ULTRA    = "ULTRA"     # ≥85
    STRONG   = "STRONG"    # 70–84
    MODERATE = "MODERATE"  # 60–69
    WATCH    = "WATCH"     # 50–59
    NOISE    = "NOISE"     # <50


@dataclass
class ComponentScore:
    name:        str
    raw_score:   float        # 0–100
    weight:      float        # доля в итоговом
    weighted:    float        # raw * weight
    reasons:     List[str]    # факторы
    metadata:    Dict = field(default_factory=dict)


@dataclass
class AegisSignal:
    symbol:          str
    direction:       str              # "short"
    total_score:     float            # 0–100
    strength:        SignalStrength
    components:      Dict[str, ComponentScore]
    entry_price:     float
    stop_loss:       float
    sl_pct:          float
    take_profits:    List[Tuple[float, int]]  # (price, weight%)
    reasons:         List[str]
    metadata:        Dict = field(default_factory=dict)
    timestamp:       str = ""

    @property
    def grade(self) -> str:
        if self.total_score >= 85: return "A+"
        if self.total_score >= 75: return "A"
        if self.total_score >= 65: return "B"
        if self.total_score >= 55: return "C"
        return "D"


# ─────────────────────────────────────────────────────────────────────
# AegisSignalEngine
# ─────────────────────────────────────────────────────────────────────

class AegisSignalEngine:
    """
    Институциональный агрегатор сигналов для SHORT торговли.
    Интегрирует все детекторы с нормализованными весами.
    """

    WEIGHTS: Dict[str, float] = {
        "pump_dump":     0.25,
        "oi_funding":    0.20,
        "liquidation":   0.20,
        "smc_structure": 0.25,
        "delta_flow":    0.10,
    }

    THRESHOLDS = {
        SignalStrength.ULTRA:    85.0,
        SignalStrength.STRONG:   70.0,
        SignalStrength.MODERATE: 60.0,
        SignalStrength.WATCH:    50.0,
    }

    # Минимальные компоненты для валидного сигнала
    MIN_COMPONENTS_VALID = 2        # ≥2 детектора должны дать score>30
    MIN_COMPONENT_SCORE  = 30.0

    def __init__(
        self,
        pump_detector=None,
        oi_analyzer=None,
        liq_mapper=None,
        smc_detector=None,
        delta_analyzer=None,
        min_score: float = 60.0,
    ):
        self.pump_detector  = pump_detector
        self.oi_analyzer    = oi_analyzer
        self.liq_mapper     = liq_mapper
        self.smc_detector   = smc_detector
        self.delta_analyzer = delta_analyzer
        self.min_score      = min_score

    def _score_to_strength(self, score: float) -> SignalStrength:
        for strength, threshold in sorted(
            self.THRESHOLDS.items(), key=lambda x: x[1], reverse=True
        ):
            if score >= threshold:
                return strength
        return SignalStrength.NOISE

    async def _get_pump_score(
        self, symbol: str, ohlcv_15m: list, market_data: Any
    ) -> ComponentScore:
        """Оценка паттерна Pump/Dump через Z-Score + Volume Spike"""
        reasons: List[str] = []
        score = 0.0
        meta: Dict = {}

        try:
            if self.pump_detector and ohlcv_15m and len(ohlcv_15m) >= 20:
                result = await self.pump_detector.detect(ohlcv_15m, market_data)
                score = result.get("score", 0.0)
                meta  = {
                    "z_score":      result.get("z_score", 0),
                    "volume_ratio": result.get("volume_ratio", 1),
                    "rsi":          result.get("rsi", 50),
                    "detected":     result.get("detected", False),
                }
                if result.get("detected"):
                    reasons.append(
                        f"PUMP EXHAUSTION: Z={meta['z_score']:.2f}σ "
                        f"Vol×{meta['volume_ratio']:.1f} RSI={meta['rsi']:.0f}"
                    )
                    if meta["z_score"] > 3.5:
                        score = min(score + 10, 100)
                        reasons.append("Экстремальное Z-Score отклонение >3.5σ")
            else:
                # Fallback: используем данные из market_data
                rsi = getattr(market_data, "rsi_1h", 50) or 50
                vol_spike = getattr(market_data, "volume_spike_ratio", 1.0) or 1.0
                price_chg = getattr(market_data, "price_change_1h", 0) or 0

                if rsi > 75 and vol_spike > 2.5:
                    score = 75.0
                    reasons.append(f"RSI перегрет {rsi:.0f} + Vol spike ×{vol_spike:.1f}")
                elif rsi > 70 and vol_spike > 2.0:
                    score = 55.0
                    reasons.append(f"RSI высокий {rsi:.0f} + объёмный всплеск")
                elif rsi > 65:
                    score = 35.0
                    reasons.append(f"RSI умеренно перегрет {rsi:.0f}")
                elif rsi < 30:
                    score = 10.0   # Шорт в перепроданности = рискованно
                    reasons.append(f"RSI перепродан {rsi:.0f} — низкая уверенность для SHORT")

                meta = {"rsi": rsi, "vol_spike": vol_spike, "price_chg_1h": price_chg}

        except Exception as e:
            logger.warning(f"pump_score error {symbol}: {e}")
            score = 20.0

        return ComponentScore(
            name="pump_dump", raw_score=score, weight=self.WEIGHTS["pump_dump"],
            weighted=score * self.WEIGHTS["pump_dump"], reasons=reasons, metadata=meta
        )

    async def _get_oi_score(
        self, symbol: str, market_data: Any, ohlcv_15m: list
    ) -> ComponentScore:
        """OI + Funding Rate анализ"""
        reasons: List[str] = []
        score = 0.0
        meta: Dict = {}

        try:
            if self.oi_analyzer:
                result = await self.oi_analyzer.analyze(symbol, market_data)
                score = result.get("score", 0.0)
                meta  = result.get("metadata", {})
                reasons.extend(result.get("reasons", []))
            else:
                # Fallback на данные market_data
                funding = getattr(market_data, "funding_rate", 0) or 0
                oi_4d   = getattr(market_data, "oi_change_4d", 0) or 0
                ls_ratio = getattr(market_data, "long_short_ratio", 50) or 50

                # Funding rate spike — сильнейший SHORT сигнал
                if funding > 0.15:
                    score += 40; reasons.append(f"Funding spike +{funding:.3f}% 🔴")
                elif funding > 0.08:
                    score += 28; reasons.append(f"Funding повышен +{funding:.3f}%")
                elif funding > 0.03:
                    score += 15; reasons.append(f"Funding умеренный +{funding:.3f}%")
                elif funding < -0.05:
                    score -= 10; reasons.append(f"Funding отрицательный {funding:.3f}% (против шорта)")

                # OI рост = новые шорты открываются = продолжение падения
                if oi_4d > 30:
                    score += 30; reasons.append(f"OI вырос +{oi_4d:.1f}% за 4д")
                elif oi_4d > 15:
                    score += 20; reasons.append(f"OI рост +{oi_4d:.1f}%")
                elif oi_4d > 5:
                    score += 10; reasons.append(f"OI умеренный рост +{oi_4d:.1f}%")
                elif oi_4d < -20:
                    score -= 10; reasons.append(f"OI падает {oi_4d:.1f}% (закрытие шортов)")

                # L/S Ratio > 60% лонгов = много позиций к сносу
                if ls_ratio > 65:
                    score += 20; reasons.append(f"Long bias {ls_ratio:.0f}% — лонги под угрозой")
                elif ls_ratio > 55:
                    score += 10; reasons.append(f"Long bias {ls_ratio:.0f}%")
                elif ls_ratio < 40:
                    score -= 5; reasons.append(f"Short bias {ls_ratio:.0f}% — рынок уже шортит")

                score = min(max(score, 0), 100)
                meta = {"funding": funding, "oi_4d": oi_4d, "ls_ratio": ls_ratio}

        except Exception as e:
            logger.warning(f"oi_score error {symbol}: {e}")
            score = 20.0

        return ComponentScore(
            name="oi_funding", raw_score=score, weight=self.WEIGHTS["oi_funding"],
            weighted=score * self.WEIGHTS["oi_funding"], reasons=reasons, metadata=meta
        )

    async def _get_liquidation_score(
        self, symbol: str, market_data: Any, ohlcv_15m: list
    ) -> ComponentScore:
        """Кластеры ликвидаций — proximity анализ"""
        reasons: List[str] = []
        score = 0.0
        meta: Dict = {}

        try:
            if self.liq_mapper:
                result = await self.liq_mapper.analyze(symbol, market_data)
                score = result.get("score", 0.0)
                meta  = result.get("metadata", {})
                reasons.extend(result.get("reasons", []))
            else:
                # Fallback: оцениваем через косвенные признаки
                ls_ratio   = getattr(market_data, "long_short_ratio", 50) or 50
                price_chg  = getattr(market_data, "price_change_4d", 0) or 0
                funding    = getattr(market_data, "funding_rate", 0) or 0

                # Высокий L/S ratio = много лонгов = кластер длинных ликвидаций ниже
                if ls_ratio > 65 and price_chg > 10:
                    score = 70; reasons.append(f"Длинные ликвидации возможны: L/S={ls_ratio:.0f}% Памп={price_chg:.1f}%")
                elif ls_ratio > 60:
                    score = 50; reasons.append(f"Умеренный риск длинных ликвидаций L/S={ls_ratio:.0f}%")
                elif ls_ratio > 55:
                    score = 30; reasons.append(f"L/S ratio смещён в длинные {ls_ratio:.0f}%")
                else:
                    score = 15

                # Высокий funding = перегретые лонги, вероятны каскадные ликвидации
                if funding > 0.1 and ls_ratio > 60:
                    score = min(score + 20, 100)
                    reasons.append("Funding + Long bias = каскадные ликвидации вероятны")

                meta = {"ls_ratio": ls_ratio, "price_chg_4d": price_chg}

        except Exception as e:
            logger.warning(f"liq_score error {symbol}: {e}")
            score = 15.0

        return ComponentScore(
            name="liquidation", raw_score=score, weight=self.WEIGHTS["liquidation"],
            weighted=score * self.WEIGHTS["liquidation"], reasons=reasons, metadata=meta
        )

    async def _get_smc_score(
        self, symbol: str, ohlcv_15m: list, price: float,
        sl_buffer_pct: float = 2.5
    ) -> ComponentScore:
        """SMC / ICT структурный анализ"""
        reasons: List[str] = []
        score = 0.0
        meta: Dict = {}

        try:
            if self.smc_detector:
                result = await self.smc_detector.analyze(ohlcv_15m, "short")
                score = result.get("score", 0.0)
                meta  = result.get("metadata", {})
                reasons.extend(result.get("reasons", []))
            else:
                # Fallback: используем smc_ict_detector из shared/core
                try:
                    from core.smc_ict_detector import get_smc_result
                    ohlcv_raw = [[c.open, c.high, c.low, c.close, c.volume]
                                 for c in ohlcv_15m] if ohlcv_15m else []
                    if len(ohlcv_raw) >= 20:
                        smc = get_smc_result(ohlcv_raw, "short",
                                             base_sl_pct=sl_buffer_pct,
                                             base_entry=price)
                        bonus = getattr(smc, "score_bonus", 0)
                        has_ob = getattr(smc, "has_ob", False)
                        has_fvg = getattr(smc, "has_fvg", False)

                        if bonus >= 15:
                            score = 85; reasons.append("SMC: OB + FVG + CHoCH подтверждены")
                        elif bonus >= 10:
                            score = 70; reasons.append("SMC: Order Block + CHoCH")
                        elif bonus >= 5:
                            score = 55; reasons.append("SMC: FVG или Order Block обнаружен")
                        elif bonus > 0:
                            score = 40; reasons.append("SMC: слабый структурный сигнал")
                        else:
                            score = 20

                        meta = {"has_ob": has_ob, "has_fvg": has_fvg, "bonus": bonus}
                except Exception as inner_e:
                    logger.debug(f"SMC fallback error: {inner_e}")
                    score = 25.0; reasons.append("SMC: нет данных, нейтральный")

        except Exception as e:
            logger.warning(f"smc_score error {symbol}: {e}")
            score = 20.0

        return ComponentScore(
            name="smc_structure", raw_score=score, weight=self.WEIGHTS["smc_structure"],
            weighted=score * self.WEIGHTS["smc_structure"], reasons=reasons, metadata=meta
        )

    async def _get_delta_score(
        self, symbol: str, ohlcv_15m: list, market_data: Any
    ) -> ComponentScore:
        """Order Flow Delta — дисбаланс покупок/продаж"""
        reasons: List[str] = []
        score = 0.0
        meta: Dict = {}

        try:
            if self.delta_analyzer:
                result = await self.delta_analyzer.analyze(symbol, ohlcv_15m)
                score = result.get("score", 0.0)
                meta  = result.get("metadata", {})
                reasons.extend(result.get("reasons", []))
            else:
                # Fallback: суррогат через свечи (bear_body / range)
                if ohlcv_15m and len(ohlcv_15m) >= 5:
                    recent = ohlcv_15m[-5:]
                    bear_count = sum(1 for c in recent if c.close < c.open)
                    total_body = sum(abs(c.close - c.open) for c in recent)
                    total_range = sum(c.high - c.low for c in recent)
                    bear_pct = bear_count / 5
                    body_ratio = total_body / total_range if total_range > 0 else 0

                    if bear_pct >= 0.8 and body_ratio > 0.6:
                        score = 75; reasons.append(f"Медвежий поток: {bear_count}/5 медвежьих свечей, тела большие")
                    elif bear_pct >= 0.6:
                        score = 55; reasons.append(f"Умеренный медвежий поток: {bear_count}/5")
                    elif bear_pct >= 0.4:
                        score = 35; reasons.append("Нейтральный поток ордеров")
                    else:
                        score = 15; reasons.append("Бычий поток — осторожно для SHORT")

                    meta = {"bear_candles": bear_count, "body_ratio": round(body_ratio, 2)}
                else:
                    score = 30; reasons.append("Delta: недостаточно данных")

        except Exception as e:
            logger.warning(f"delta_score error {symbol}: {e}")
            score = 25.0

        return ComponentScore(
            name="delta_flow", raw_score=score, weight=self.WEIGHTS["delta_flow"],
            weighted=score * self.WEIGHTS["delta_flow"], reasons=reasons, metadata=meta
        )

    # ─────────────────────────────────────────────────────────────────
    # MAIN: generate_signal
    # ─────────────────────────────────────────────────────────────────

    async def generate_signal(
        self,
        symbol:       str,
        market_data:  Any,
        ohlcv_15m:    list,
        entry_price:  float,
        stop_loss:    float,
        sl_pct:       float,
        take_profits: List[Tuple[float, int]],
        base_score:   float = 0.0,
    ) -> Optional[AegisSignal]:
        """
        Агрегирует все детекторы → итоговый AegisSignal или None.

        Args:
            base_score: скор из существующего scorer.py (добавляется как контекст)
        """
        # Параллельный расчёт всех компонентов
        tasks = [
            self._get_pump_score(symbol, ohlcv_15m, market_data),
            self._get_oi_score(symbol, market_data, ohlcv_15m),
            self._get_liquidation_score(symbol, market_data, ohlcv_15m),
            self._get_smc_score(symbol, ohlcv_15m, entry_price, sl_pct),
            self._get_delta_score(symbol, ohlcv_15m, market_data),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        components: Dict[str, ComponentScore] = {}
        total_weighted = 0.0
        all_reasons: List[str] = []
        valid_components = 0

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Component error: {result}")
                continue
            cs: ComponentScore = result
            components[cs.name] = cs
            total_weighted += cs.weighted
            all_reasons.extend(cs.reasons)
            if cs.raw_score >= self.MIN_COMPONENT_SCORE:
                valid_components += 1

        # Нормализуем (сумма весов = 1.0 всегда)
        total_weight = sum(cs.weight for cs in components.values())
        final_score = (total_weighted / total_weight * 100) if total_weight > 0 else 0
        # Нормируем обратно к 0-100 (так как raw_score уже 0-100)
        final_score = total_weighted  # Это уже 0-100 (веса суммируются в 1.0)

        # Интеграция с base_score из исходного scorer (бонус при совпадении)
        if base_score > 0:
            # Blended: 70% Aegis + 30% существующий scorer
            final_score = final_score * 0.70 + base_score * 0.30

        # Проверка минимального порога и минимума компонентов
        if final_score < self.min_score:
            return None

        if valid_components < self.MIN_COMPONENTS_VALID:
            logger.debug(f"{symbol}: only {valid_components} valid components — skip")
            return None

        strength = self._score_to_strength(final_score)
        if strength == SignalStrength.NOISE:
            return None

        from datetime import datetime
        return AegisSignal(
            symbol=symbol,
            direction="short",
            total_score=round(final_score, 2),
            strength=strength,
            components=components,
            entry_price=entry_price,
            stop_loss=stop_loss,
            sl_pct=sl_pct,
            take_profits=take_profits,
            reasons=all_reasons[:12],   # топ-12 причин
            metadata={
                "base_score":        base_score,
                "valid_components":  valid_components,
                "component_scores":  {k: round(v.raw_score, 1)
                                      for k, v in components.items()},
            },
            timestamp=datetime.utcnow().isoformat(),
        )

    def format_signal_report(self, signal: AegisSignal) -> str:
        """Форматирование для Telegram"""
        grade_emoji = {"A+": "💎", "A": "🥇", "B": "🥈", "C": "🥉", "D": "⚠️"}
        strength_emoji = {
            SignalStrength.ULTRA:    "🔥 ULTRA",
            SignalStrength.STRONG:   "🔴 STRONG",
            SignalStrength.MODERATE: "🟠 MODERATE",
            SignalStrength.WATCH:    "🟡 WATCH",
        }

        comp_lines = ""
        for name, cs in signal.components.items():
            bar = "█" * int(cs.raw_score / 10) + "░" * (10 - int(cs.raw_score / 10))
            comp_lines += f"  {bar} {name}: {cs.raw_score:.0f}\n"

        return (
            f"{grade_emoji.get(signal.grade,'⚠️')} <b>Aegis {signal.grade}</b> | "
            f"{strength_emoji.get(signal.strength,'📊')} | Score: {signal.total_score:.1f}%\n"
            f"Components:\n{comp_lines}"
        )
