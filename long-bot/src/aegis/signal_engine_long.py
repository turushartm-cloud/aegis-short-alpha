"""
Aegis Long Signal Engine v1.0
Зеркало Short Signal Engine — взвешенный 5-компонентный скорер для LONG.

Веса (зеркало Short):
  dump_exhaustion: 0.25  — Z-Score < -2.5σ (oversold climax)
  oi_funding:      0.20  — Negative funding + Short Squeeze setup
  bsl_liquidity:   0.20  — BSL кластеры выше цены (магнит вверх)
  smc_structure:   0.25  — Bullish CHoCH + Bullish OB + Spring
  delta_flow:      0.10  — Bullish CVD

Пороги: ULTRA ≥85 | STRONG ≥70 | MODERATE ≥60 | WATCH ≥50
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("aegis.signal_engine_long")


class SignalStrengthLong(Enum):
    ULTRA    = "ULTRA"
    STRONG   = "STRONG"
    MODERATE = "MODERATE"
    WATCH    = "WATCH"
    NOISE    = "NOISE"


@dataclass
class ComponentScoreLong:
    name:     str
    raw_score: float
    weight:   float
    weighted: float
    reasons:  List[str]
    metadata: Dict = field(default_factory=dict)


@dataclass
class AegisLongSignal:
    symbol:       str
    direction:    str = "long"
    total_score:  float = 0.0
    strength:     SignalStrengthLong = SignalStrengthLong.NOISE
    components:   Dict[str, ComponentScoreLong] = field(default_factory=dict)
    entry_price:  float = 0.0
    stop_loss:    float = 0.0
    sl_pct:       float = 0.0
    take_profits: List[Tuple[float, int]] = field(default_factory=list)
    reasons:      List[str] = field(default_factory=list)
    metadata:     Dict = field(default_factory=dict)
    timestamp:    str = ""

    @property
    def grade(self) -> str:
        if self.total_score >= 85: return "A+"
        if self.total_score >= 75: return "A"
        if self.total_score >= 65: return "B"
        if self.total_score >= 55: return "C"
        return "D"


class AegisLongSignalEngine:
    """Институциональный агрегатор сигналов для LONG торговли."""

    WEIGHTS: Dict[str, float] = {
        "dump_exhaustion": 0.25,
        "oi_funding":      0.20,
        "bsl_liquidity":   0.20,
        "smc_structure":   0.25,
        "delta_flow":      0.10,
    }

    THRESHOLDS = {
        SignalStrengthLong.ULTRA:    85.0,
        SignalStrengthLong.STRONG:   70.0,
        SignalStrengthLong.MODERATE: 60.0,
        SignalStrengthLong.WATCH:    50.0,
    }

    MIN_COMPONENTS_VALID = 2
    MIN_COMPONENT_SCORE  = 30.0

    def __init__(
        self,
        dump_detector=None,
        oi_analyzer=None,
        bsl_scanner=None,
        wyckoff_detector=None,
        delta_analyzer=None,
        min_score: float = 58.0,
    ):
        self.dump_detector   = dump_detector
        self.oi_analyzer     = oi_analyzer
        self.bsl_scanner     = bsl_scanner
        self.wyckoff_detector = wyckoff_detector
        self.delta_analyzer  = delta_analyzer
        self.min_score       = min_score

    def _score_to_strength(self, score: float) -> SignalStrengthLong:
        for strength, threshold in sorted(
            self.THRESHOLDS.items(), key=lambda x: x[1], reverse=True
        ):
            if score >= threshold:
                return strength
        return SignalStrengthLong.NOISE

    async def _get_dump_score(self, symbol: str, ohlcv: list, md: Any) -> ComponentScoreLong:
        reasons, score, meta = [], 0.0, {}
        try:
            if self.dump_detector and ohlcv and len(ohlcv) >= 20:
                result = await self.dump_detector.detect(ohlcv, md)
                score  = result.get("score", 0.0)
                meta   = result
                if result.get("detected"):
                    reasons.append(
                        f"DUMP EXHAUSTION: Z={result.get('z_score', 0):.2f}σ "
                        f"Vol×{result.get('volume_ratio', 1):.1f} RSI={result.get('rsi', 50):.0f}"
                    )
            else:
                rsi      = getattr(md, "rsi_1h", 50) or 50
                vol_spike = getattr(md, "volume_spike_ratio", 1.0) or 1.0
                if rsi < 20 and vol_spike > 2.0:
                    score = 75; reasons.append(f"RSI перепродан {rsi:.0f} + Vol×{vol_spike:.1f}")
                elif rsi < 25:
                    score = 55; reasons.append(f"RSI сильно перепродан {rsi:.0f}")
                elif rsi < 35:
                    score = 35; reasons.append(f"RSI перепродан {rsi:.0f}")
                elif rsi > 70:
                    score = 5;  reasons.append(f"RSI перекуплен {rsi:.0f} — не для LONG отскока")
        except Exception as e:
            logger.warning(f"dump_score {symbol}: {e}"); score = 20.0
        return ComponentScoreLong(
            "dump_exhaustion", score, self.WEIGHTS["dump_exhaustion"],
            score * self.WEIGHTS["dump_exhaustion"], reasons, meta
        )

    async def _get_oi_score(self, symbol: str, md: Any, ohlcv: list) -> ComponentScoreLong:
        reasons, score, meta = [], 0.0, {}
        try:
            if self.oi_analyzer:
                result = await self.oi_analyzer.analyze(symbol, md)
                score  = result.get("score", 0.0)
                meta   = result.get("metadata", {})
                reasons.extend(result.get("reasons", []))
            else:
                funding  = getattr(md, "funding_rate", 0) or 0
                ls_ratio = getattr(md, "long_short_ratio", 50) or 50
                if funding < -0.08:
                    score += 40; reasons.append(f"Funding SPIKE {funding:+.3f}% — шорты перегреты")
                elif funding < -0.03:
                    score += 25; reasons.append(f"Funding отрицательный {funding:+.3f}%")
                elif funding > 0.08:
                    score -= 15; reasons.append(f"Funding высокий +{funding:.3f}%")
                if (100 - ls_ratio) > 60:
                    score += 20; reasons.append(f"Short dominance {100-ls_ratio:.0f}%")
                score = min(max(score, 0), 100)
        except Exception as e:
            logger.warning(f"oi_score_long {symbol}: {e}"); score = 20.0
        return ComponentScoreLong(
            "oi_funding", score, self.WEIGHTS["oi_funding"],
            score * self.WEIGHTS["oi_funding"], reasons, meta
        )

    async def _get_bsl_score(self, symbol: str, md: Any, ohlcv: list) -> ComponentScoreLong:
        reasons, score, meta = [], 0.0, {}
        try:
            if self.bsl_scanner and ohlcv:
                result = await self.bsl_scanner.analyze(symbol, md, ohlcv)
                score  = result.get("score", 0.0)
                meta   = result.get("metadata", {})
                reasons.extend(result.get("reasons", []))

            # Wyckoff бонус
            if self.wyckoff_detector and ohlcv:
                wy = await self.wyckoff_detector.analyze(symbol, ohlcv, md)
                wy_score = wy.get("score", 0)
                if wy_score > 50:
                    score = max(score, wy_score * 0.8)
                    reasons.extend(wy.get("reasons", [])[:2])
                    meta["wyckoff"] = wy.get("event", "")

            # Fallback: L/S и позиция в диапазоне
            if score == 0:
                price_4d = getattr(md, "price_change_4d", 0) or 0
                ls_ratio = getattr(md, "long_short_ratio", 50) or 50
                if price_4d < -20 and (100 - ls_ratio) > 55:
                    score = 55; reasons.append("Дамп + Short bias = BSL выше")
                elif price_4d < -10:
                    score = 35; reasons.append("Цена упала — BSL выше вероятно")

        except Exception as e:
            logger.warning(f"bsl_score {symbol}: {e}"); score = 20.0
        return ComponentScoreLong(
            "bsl_liquidity", score, self.WEIGHTS["bsl_liquidity"],
            score * self.WEIGHTS["bsl_liquidity"], reasons, meta
        )

    async def _get_smc_score(
        self, symbol: str, ohlcv: list, price: float, sl_pct: float
    ) -> ComponentScoreLong:
        reasons, score, meta = [], 0.0, {}
        try:
            from core.smc_ict_detector import get_smc_result
            ohlcv_raw = [[c.open, c.high, c.low, c.close, c.volume]
                         for c in ohlcv] if ohlcv else []
            if len(ohlcv_raw) >= 20:
                smc = get_smc_result(ohlcv_raw, "long",
                                     base_sl_pct=sl_pct, base_entry=price)
                bonus   = getattr(smc, "score_bonus", 0)
                has_ob  = getattr(smc, "has_ob", False)
                has_fvg = getattr(smc, "has_fvg", False)
                if bonus >= 15:
                    score = 85; reasons.append("SMC: Bullish OB + FVG + CHoCH вверх")
                elif bonus >= 10:
                    score = 70; reasons.append("SMC: Bullish Order Block + CHoCH")
                elif bonus >= 5:
                    score = 55; reasons.append("SMC: Bullish FVG или OB")
                elif bonus > 0:
                    score = 40; reasons.append("SMC: слабый бычий сигнал")
                else:
                    score = 20
                meta = {"has_ob": has_ob, "has_fvg": has_fvg, "bonus": bonus}
        except Exception as e:
            logger.debug(f"SMC long {symbol}: {e}"); score = 25.0
        return ComponentScoreLong(
            "smc_structure", score, self.WEIGHTS["smc_structure"],
            score * self.WEIGHTS["smc_structure"], reasons, meta
        )

    async def _get_delta_score(self, symbol: str, ohlcv: list) -> ComponentScoreLong:
        reasons, score, meta = [], 0.0, {}
        try:
            if not ohlcv or len(ohlcv) < 10:
                return ComponentScoreLong("delta_flow", 25.0, self.WEIGHTS["delta_flow"],
                                          25.0 * self.WEIGHTS["delta_flow"],
                                          ["Delta: недостаточно данных"], {})

            # CVD суррогат — для LONG ищем БЫЧИЙ поток
            recent = ohlcv[-20:]
            deltas = []
            for c in recent:
                rng = c.high - c.low
                if rng > 0:
                    body = c.close - c.open
                    vol  = getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0)
                    deltas.append(vol * (body / rng))
                else:
                    deltas.append(0.0)

            cvd_5  = sum(deltas[-5:])
            cvd_10 = sum(deltas[-10:])
            cvd_total = sum(deltas)

            bull_candles = sum(1 for c in ohlcv[-10:] if c.close > c.open)
            bull_ratio   = bull_candles / 10

            # LONG хочет бычий CVD
            if cvd_5 > 0 and cvd_10 > 0:
                score += 40; reasons.append(f"CVD бычий: 5c={cvd_5:.0f} 10c={cvd_10:.0f}")
            elif cvd_5 > 0:
                score += 25; reasons.append("Краткосрочный бычий поток")
            elif cvd_total > 0:
                score += 15; reasons.append("Накопленный бычий CVD")
            else:
                # Медвежий CVD при поиске LONG — проверяем исчерпание
                if abs(cvd_5) > abs(cvd_10) * 0.8:
                    score += 10; reasons.append("CVD медвежий но замедляется")

            if bull_ratio >= 0.7:
                score += 30; reasons.append(f"{bull_candles}/10 бычьих свечей — momentum up")
            elif bull_ratio >= 0.5:
                score += 15; reasons.append(f"Бычий momentum {bull_candles}/10")
            elif bull_ratio < 0.3:
                reasons.append("Медвежий поток — следим за разворотом")

            score = min(max(score, 0), 100)
            meta = {"cvd_5": round(cvd_5, 0), "bull_ratio": bull_ratio}

        except Exception as e:
            logger.warning(f"delta_long {symbol}: {e}"); score = 25.0
        return ComponentScoreLong(
            "delta_flow", score, self.WEIGHTS["delta_flow"],
            score * self.WEIGHTS["delta_flow"], reasons, meta
        )

    async def generate_signal(
        self,
        symbol: str,
        market_data: Any,
        ohlcv_15m: list,
        entry_price: float,
        stop_loss: float,
        sl_pct: float,
        take_profits: List[Tuple[float, int]],
        base_score: float = 0.0,
    ) -> Optional["AegisLongSignal"]:

        tasks = [
            self._get_dump_score(symbol, ohlcv_15m, market_data),
            self._get_oi_score(symbol, market_data, ohlcv_15m),
            self._get_bsl_score(symbol, market_data, ohlcv_15m),
            self._get_smc_score(symbol, ohlcv_15m, entry_price, sl_pct),
            self._get_delta_score(symbol, ohlcv_15m),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        components: Dict[str, ComponentScoreLong] = {}
        total_weighted = 0.0
        all_reasons: List[str] = []
        valid_components = 0

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Component error: {result}"); continue
            cs: ComponentScoreLong = result
            components[cs.name] = cs
            total_weighted += cs.weighted
            all_reasons.extend(cs.reasons)
            if cs.raw_score >= self.MIN_COMPONENT_SCORE:
                valid_components += 1

        total_weight = sum(cs.weight for cs in components.values())
        final_score  = total_weighted if total_weight > 0 else 0.0

        if base_score > 0:
            final_score = final_score * 0.70 + base_score * 0.30

        if final_score < self.min_score or valid_components < self.MIN_COMPONENTS_VALID:
            return None

        strength = self._score_to_strength(final_score)
        if strength == SignalStrengthLong.NOISE:
            return None

        from datetime import datetime
        return AegisLongSignal(
            symbol=symbol, direction="long",
            total_score=round(final_score, 2),
            strength=strength,
            components=components,
            entry_price=entry_price,
            stop_loss=stop_loss, sl_pct=sl_pct,
            take_profits=take_profits,
            reasons=all_reasons[:12],
            metadata={
                "base_score": base_score,
                "valid_components": valid_components,
                "component_scores": {k: round(v.raw_score, 1) for k, v in components.items()},
            },
            timestamp=datetime.utcnow().isoformat(),
        )
