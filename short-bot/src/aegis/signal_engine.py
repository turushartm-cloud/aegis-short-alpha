"""
Aegis SHORT Signal Engine v2.0
Иерархия сигналов (mean-reversion SHORT):

  #1+#2  z_volume:      0.30  — Z-Score от VWAP + Volume Spike
  #3     oi_change:     0.20  — OI динамика + L/S ratio
  #4     funding_rate:  0.15  — Funding rate (лонги перегреты)
  #5     smc_structure: 0.20  — CHoCH + FVG + Order Blocks
         delta_flow:    0.10  — Order flow подтверждение
  #6     rsi_aux:       0.05  — RSI вспомогательный (НЕ gate)

RSI НИКОГДА не блокирует сигнал. Только +/- корректирует.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("aegis.signal_engine")


class SignalStrength(Enum):
    ULTRA    = "ULTRA"
    STRONG   = "STRONG"
    MODERATE = "MODERATE"
    WATCH    = "WATCH"
    NOISE    = "NOISE"


@dataclass
class ComponentScore:
    name:      str
    raw_score: float
    weight:    float
    weighted:  float
    reasons:   List[str]
    metadata:  Dict = field(default_factory=dict)


@dataclass
class AegisSignal:
    symbol:       str
    direction:    str
    total_score:  float
    strength:     SignalStrength
    components:   Dict[str, ComponentScore]
    entry_price:  float
    stop_loss:    float
    sl_pct:       float
    take_profits: List[Tuple[float, int]]
    reasons:      List[str]
    metadata:     Dict = field(default_factory=dict)
    timestamp:    str = ""

    @property
    def grade(self) -> str:
        if self.total_score >= 85: return "A+"
        if self.total_score >= 75: return "A"
        if self.total_score >= 65: return "B"
        if self.total_score >= 55: return "C"
        return "D"


class AegisSignalEngine:
    """SHORT mean-reversion: Z-score → Volume → OI → Funding → Structure → RSI"""

    WEIGHTS: Dict[str, float] = {
        "z_volume":      0.30,
        "oi_change":     0.20,
        "funding_rate":  0.15,
        "smc_structure": 0.20,
        "delta_flow":    0.10,
        "rsi_aux":       0.05,
    }

    THRESHOLDS = {
        SignalStrength.ULTRA:    85.0,
        SignalStrength.STRONG:   70.0,
        SignalStrength.MODERATE: 60.0,
        SignalStrength.WATCH:    50.0,
    }

    MIN_COMPONENTS_VALID = 1
    MIN_COMPONENT_SCORE  = 30.0

    def __init__(
        self,
        pump_detector=None,
        oi_analyzer=None,
        liq_mapper=None,
        smc_detector=None,
        delta_analyzer=None,
        min_score: float = 54.0,
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

    # ── #1+#2 Z-SCORE + VOLUME (0.30) ───────────────────────────────
    async def _get_z_volume_score(
        self, symbol: str, ohlcv_15m: list, market_data: Any
    ) -> ComponentScore:
        """ГЛАВНЫЙ: цена >VWAP + объёмный всплеск = истощение покупателей."""
        reasons, score, meta = [], 0.0, {}
        try:
            if self.pump_detector and ohlcv_15m and len(ohlcv_15m) >= 20:
                r = await self.pump_detector.detect(ohlcv_15m, market_data)
                score = r.get("score", 0.0)
                meta  = {k: r.get(k) for k in ("z_score", "volume_ratio", "velocity_pct", "detected")}
                if r.get("detected"):
                    reasons.append(
                        f"PUMP EXHAUSTION: Z={meta['z_score']:.2f}σ "
                        f"Vol×{meta['volume_ratio']:.1f} — mean-reversion SHORT"
                    )
                    if meta["z_score"] > 3.5:
                        score = min(score + 10, 100)
                        reasons.append(f"Z={meta['z_score']:.2f}σ >3.5 — высокая вероятность отката")
                elif meta.get("z_score", 0) > 2.0:
                    reasons.append(f"Z={meta['z_score']:.2f}σ Vol×{meta['volume_ratio']:.1f}")
            else:
                vol  = getattr(market_data, "volume_spike_ratio", 1.0) or 1.0
                p1h  = getattr(market_data, "price_change_1h", 0) or 0
                p4h  = getattr(market_data, "price_change_4h", 0) or 0
                if   p1h > 15 and vol > 3.0: score = 75; reasons.append(f"Памп +{p1h:.1f}%/1H Vol×{vol:.1f} — истощение")
                elif p1h > 10 and vol > 2.5: score = 62; reasons.append(f"+{p1h:.1f}%/1H Vol×{vol:.1f}")
                elif p4h > 20 and vol > 2.0: score = 55; reasons.append(f"+{p4h:.1f}%/4H Vol×{vol:.1f}")
                elif p1h > 5  and vol > 2.0: score = 45; reasons.append(f"+{p1h:.1f}%/1H Vol×{vol:.1f}")
                elif vol > 2.0:              score = 35; reasons.append(f"Vol×{vol:.1f} (без ценового подтв.)")
                else:                        score = 15; reasons.append("Нет Z-score/Volume сигнала")
                meta = {"vol_spike": vol, "price_chg_1h": p1h, "price_chg_4h": p4h}
        except Exception as e:
            logger.warning(f"z_volume {symbol}: {e}"); score = 20.0
        return ComponentScore("z_volume", score, self.WEIGHTS["z_volume"],
                              score * self.WEIGHTS["z_volume"], reasons, meta)

    # ── #3 OI CHANGE + L/S RATIO (0.20) ─────────────────────────────
    async def _get_oi_score(
        self, symbol: str, market_data: Any, ohlcv_15m: list
    ) -> ComponentScore:
        """OI рост при памп = новые лонги = будущие стоп-лоссы. L/S bias."""
        reasons, score, meta = [], 0.0, {}
        try:
            if self.oi_analyzer:
                r = await self.oi_analyzer.analyze(symbol, market_data)
                score = r.get("oi_score", r.get("score", 0.0))
                meta  = r.get("metadata", {})
                reasons.extend([x for x in r.get("reasons", [])
                                 if "fund" not in x.lower() and "фанд" not in x.lower()])
            else:
                oi_4d    = getattr(market_data, "oi_change_4d", 0) or 0
                ls_ratio = getattr(market_data, "long_short_ratio", 50) or 50

                if   oi_4d > 40: score += 45; reasons.append(f"OI +{oi_4d:.1f}% — массовое открытие лонгов 🔴")
                elif oi_4d > 25: score += 35; reasons.append(f"OI +{oi_4d:.1f}% — сильный рост")
                elif oi_4d > 15: score += 25; reasons.append(f"OI +{oi_4d:.1f}% — лонги набираются")
                elif oi_4d > 5:  score += 15; reasons.append(f"OI +{oi_4d:.1f}%")
                elif oi_4d < -20: score -= 10; reasons.append(f"OI {oi_4d:.1f}% падает (против шорта)")
                elif oi_4d < -5:  score -= 5

                if   ls_ratio > 70: score += 40; reasons.append(f"Long bias {ls_ratio:.0f}% — экстремальный перекос 🔴")
                elif ls_ratio > 65: score += 30; reasons.append(f"Long bias {ls_ratio:.0f}%")
                elif ls_ratio > 60: score += 20; reasons.append(f"Long bias {ls_ratio:.0f}%")
                elif ls_ratio > 55: score += 10; reasons.append(f"Long bias {ls_ratio:.0f}%")
                elif ls_ratio < 40: score -= 8;  reasons.append(f"Short bias {ls_ratio:.0f}% — рынок уже шортит")
                elif ls_ratio < 45: score -= 3

                score = min(max(score, 0), 100)
                meta  = {"oi_4d": oi_4d, "ls_ratio": ls_ratio}
        except Exception as e:
            logger.warning(f"oi_score {symbol}: {e}"); score = 20.0
        return ComponentScore("oi_change", score, self.WEIGHTS["oi_change"],
                              score * self.WEIGHTS["oi_change"], reasons, meta)

    # ── #4 FUNDING RATE (0.15) ───────────────────────────────────────
    async def _get_funding_score(
        self, symbol: str, market_data: Any
    ) -> ComponentScore:
        """Funding rate — лонги переплачивают шортам = mean-reversion сигнал."""
        reasons, score, meta = [], 0.0, {}
        try:
            f   = getattr(market_data, "funding_rate", 0) or 0
            acc = getattr(market_data, "funding_accumulated", 0) or 0

            if   f > 0.15:  score += 55; reasons.append(f"FUNDING SPIKE +{f:.3f}% 🔴")
            elif f > 0.10:  score += 45; reasons.append(f"Funding высокий +{f:.3f}%")
            elif f > 0.05:  score += 35; reasons.append(f"Funding повышен +{f:.3f}%")
            elif f > 0.02:  score += 20; reasons.append(f"Funding умеренный +{f:.3f}%")
            elif f > 0.005: score += 10; reasons.append(f"Funding слабый +{f:.3f}%")
            elif f < -0.05: score -= 15; reasons.append(f"Funding отрицательный {f:.3f}% (шорты платят)")
            elif f < -0.02: score -= 8;  reasons.append(f"Funding отрицательный {f:.3f}%")
            else:           score  = 15; reasons.append(f"Funding нейтральный {f:.3f}%")

            if   acc > 0.5: score = min(score + 25, 100); reasons.append(f"Накопл. фандинг +{acc:.2f}%/4д 🔴")
            elif acc > 0.3: score = min(score + 15, 100); reasons.append(f"Накопл. фандинг +{acc:.2f}%")
            elif acc > 0.1: score = min(score + 8,  100); reasons.append(f"Накопл. +{acc:.2f}%")
            elif acc < -0.3: score = max(score - 10, 0);  reasons.append(f"Накопл. фандинг {acc:.2f}% (против)")

            score = min(max(score, 0), 100)
            meta  = {"funding": f, "funding_acc": acc}
        except Exception as e:
            logger.warning(f"funding {symbol}: {e}"); score = 15.0
        return ComponentScore("funding_rate", score, self.WEIGHTS["funding_rate"],
                              score * self.WEIGHTS["funding_rate"], reasons, meta)

    # ── #5 SMC STRUCTURE (0.20) ──────────────────────────────────────
    async def _get_smc_score(
        self, symbol: str, ohlcv_15m: list, price: float, sl_pct: float = 2.5
    ) -> ComponentScore:
        """CHoCH + FVG + Order Blocks — структурное подтверждение разворота."""
        reasons, score, meta = [], 0.0, {}
        try:
            if self.smc_detector:
                r = await self.smc_detector.analyze(ohlcv_15m, "short")
                score = r.get("score", 0.0); meta = r.get("metadata", {})
                reasons.extend(r.get("reasons", []))
            else:
                try:
                    from core.smc_ict_detector import get_smc_result
                    raw = [[c.open, c.high, c.low, c.close, c.volume]
                           for c in ohlcv_15m] if ohlcv_15m else []
                    if len(raw) >= 20:
                        smc = get_smc_result(raw, "short", base_sl_pct=sl_pct, base_entry=price)
                        b = getattr(smc, "score_bonus", 0)
                        meta = {"has_ob": getattr(smc, "has_ob", False),
                                "has_fvg": getattr(smc, "has_fvg", False), "bonus": b}
                        if   b >= 15: score = 90; reasons.append("SMC: Bearish OB + FVG + CHoCH ✅")
                        elif b >= 10: score = 75; reasons.append("SMC: Order Block + Bearish CHoCH")
                        elif b >= 5:  score = 55; reasons.append("SMC: Bearish FVG/OB")
                        elif b >  0:  score = 40; reasons.append("SMC: слабый медвежий сигнал")
                        else:         score = 20; reasons.append("SMC: нет подтверждения")
                    else:
                        score = 20; reasons.append("SMC: мало данных")
                except Exception as ie:
                    logger.debug(f"SMC: {ie}"); score = 20; reasons.append("SMC недоступен")
        except Exception as e:
            logger.warning(f"smc {symbol}: {e}"); score = 20.0
        return ComponentScore("smc_structure", score, self.WEIGHTS["smc_structure"],
                              score * self.WEIGHTS["smc_structure"], reasons, meta)

    # ── DELTA FLOW (0.10) ────────────────────────────────────────────
    async def _get_delta_score(
        self, symbol: str, ohlcv_15m: list, market_data: Any
    ) -> ComponentScore:
        """Медвежий order flow — подтверждение направления."""
        reasons, score, meta = [], 0.0, {}
        try:
            if self.delta_analyzer:
                r = await self.delta_analyzer.analyze(symbol, ohlcv_15m)
                score = r.get("score", 0.0); meta = r.get("metadata", {})
                reasons.extend(r.get("reasons", []))
            elif ohlcv_15m and len(ohlcv_15m) >= 5:
                recent     = ohlcv_15m[-5:]
                bear_count = sum(1 for c in recent if c.close < c.open)
                t_body     = sum(abs(c.close - c.open) for c in recent)
                t_range    = sum(c.high - c.low for c in recent)
                br         = t_body / t_range if t_range > 0 else 0
                bp         = bear_count / 5
                if   bp >= 0.8 and br > 0.6: score = 80; reasons.append(f"Медвежий поток {bear_count}/5 + крупные тела")
                elif bp >= 0.6:              score = 60; reasons.append(f"Умеренный медвежий поток {bear_count}/5")
                elif bp >= 0.4:              score = 35; reasons.append("Нейтральный поток")
                else:                        score = 15; reasons.append("Бычий поток (осторожно)")
                meta = {"bear_candles": bear_count, "body_ratio": round(br, 2)}
            else:
                score = 30; reasons.append("Delta: нет данных")
        except Exception as e:
            logger.warning(f"delta {symbol}: {e}"); score = 25.0
        return ComponentScore("delta_flow", score, self.WEIGHTS["delta_flow"],
                              score * self.WEIGHTS["delta_flow"], reasons, meta)

    # ── #6 RSI AUXILIARY (0.05) ──────────────────────────────────────
    async def _get_rsi_score(
        self, symbol: str, market_data: Any
    ) -> ComponentScore:
        """RSI — последний в иерархии. Вклад max 5pts в итог. Никогда не блокирует."""
        reasons, meta = [], {}
        try:
            rsi = getattr(market_data, "rsi_1h", 50) or 50
            if   rsi >= 80: score = 100; reasons.append(f"RSI {rsi:.0f} — экстремальная перекупленность ✅")
            elif rsi >= 70: score = 80;  reasons.append(f"RSI {rsi:.0f} — перекуплен")
            elif rsi >= 60: score = 60;  reasons.append(f"RSI {rsi:.0f} — верхняя зона")
            elif rsi >= 50: score = 45;  reasons.append(f"RSI {rsi:.0f} — нейтральная")
            elif rsi >= 40: score = 35;  reasons.append(f"RSI {rsi:.0f} — нейтральная нижняя")
            elif rsi >= 30: score = 25;  reasons.append(f"RSI {rsi:.0f} — низкий")
            else:           score = 15;  reasons.append(f"RSI {rsi:.0f} — перепродан (мягкий штраф)")
            meta = {"rsi": rsi}
        except Exception as e:
            logger.warning(f"rsi {symbol}: {e}"); score = 45.0
        return ComponentScore("rsi_aux", score, self.WEIGHTS["rsi_aux"],
                              score * self.WEIGHTS["rsi_aux"], reasons, meta)

    # ── GENERATE SIGNAL ──────────────────────────────────────────────
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

        results = await asyncio.gather(
            self._get_z_volume_score(symbol, ohlcv_15m, market_data),
            self._get_oi_score(symbol, market_data, ohlcv_15m),
            self._get_funding_score(symbol, market_data),
            self._get_smc_score(symbol, ohlcv_15m, entry_price, sl_pct),
            self._get_delta_score(symbol, ohlcv_15m, market_data),
            self._get_rsi_score(symbol, market_data),
            return_exceptions=True,
        )

        components: Dict[str, ComponentScore] = {}
        total_weighted = 0.0
        all_reasons: List[str] = []
        valid_components = 0

        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Component error: {r}"); continue
            cs: ComponentScore = r
            components[cs.name] = cs
            total_weighted += cs.weighted
            all_reasons.extend(cs.reasons)
            if cs.raw_score >= self.MIN_COMPONENT_SCORE:
                valid_components += 1

        final_score = total_weighted  # веса = 1.0, raw_score 0-100

        if base_score > 0:
            if base_score >= 65:
                final_score = base_score + min(final_score * 0.15, 15)
            else:
                final_score = final_score * 0.55 + base_score * 0.45

        if final_score < self.min_score:
            cs_str = " | ".join(f"{k}={v.raw_score:.0f}" for k, v in components.items())
            logger.info(f"[AEGIS REJECT] {symbol}: {final_score:.1f} < {self.min_score} | {cs_str}")
            return None

        if valid_components < self.MIN_COMPONENTS_VALID:
            logger.info(f"[AEGIS REJECT] {symbol}: valid={valid_components}")
            return None

        strength = self._score_to_strength(final_score)
        if strength == SignalStrength.NOISE:
            return None

        from datetime import datetime
        return AegisSignal(
            symbol=symbol, direction="short",
            total_score=round(final_score, 2), strength=strength,
            components=components, entry_price=entry_price,
            stop_loss=stop_loss, sl_pct=sl_pct, take_profits=take_profits,
            reasons=all_reasons[:12],
            metadata={
                "base_score": base_score,
                "valid_components": valid_components,
                "hierarchy": {k: round(components[k].raw_score, 1)
                              for k in self.WEIGHTS if k in components},
            },
            timestamp=datetime.utcnow().isoformat(),
        )

    def format_signal_report(self, signal: AegisSignal) -> str:
        grade_emoji    = {"A+": "💎", "A": "🥇", "B": "🥈", "C": "🥉", "D": "⚠️"}
        strength_emoji = {
            SignalStrength.ULTRA:    "🔥 ULTRA",
            SignalStrength.STRONG:   "🔴 STRONG",
            SignalStrength.MODERATE: "🟠 MODERATE",
            SignalStrength.WATCH:    "🟡 WATCH",
        }
        order  = ["z_volume", "oi_change", "funding_rate", "smc_structure", "delta_flow", "rsi_aux"]
        labels = {"z_volume": "#1+2 Z+Vol  ", "oi_change": "#3  OI      ",
                  "funding_rate": "#4  Funding ", "smc_structure": "#5  SMC     ",
                  "delta_flow": "    Delta   ", "rsi_aux": "#6  RSI aux "}
        lines = ""
        for k in order:
            cs = signal.components.get(k)
            if cs:
                bar = "█" * int(cs.raw_score / 10) + "░" * (10 - int(cs.raw_score // 10))
                lines += f"  {labels.get(k, k)}: {bar} {cs.raw_score:.0f}\n"
        return (
            f"{grade_emoji.get(signal.grade,'⚠️')} <b>Aegis SHORT {signal.grade}</b> | "
            f"{strength_emoji.get(signal.strength,'📊')} | Score: {signal.total_score:.1f}%\n"
            f"{lines}"
        )
