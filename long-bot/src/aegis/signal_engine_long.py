"""
Aegis LONG Signal Engine v2.0
Иерархия сигналов (mean-reversion LONG):

  #1+#2  z_volume:      0.30  — Z-Score ниже VWAP + Volume Spike (капитуляция)
  #3     oi_change:     0.20  — OI падает + Short bias (шорты закрываются)
  #4     funding_rate:  0.15  — Отрицательный Funding (шорты переплачивают)
  #5     smc_structure: 0.20  — Bullish CHoCH + Spring + OB
         delta_flow:    0.10  — Бычий CVD/order flow
  #6     rsi_aux:       0.05  — RSI вспомогательный (НЕ gate)

RSI НИКОГДА не блокирует сигнал. Только +/- корректирует.
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
    name:      str
    raw_score: float
    weight:    float
    weighted:  float
    reasons:   List[str]
    metadata:  Dict = field(default_factory=dict)


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
    """LONG mean-reversion: Z-score↓ → Volume → OI → Funding → Structure → RSI"""

    WEIGHTS: Dict[str, float] = {
        "z_volume":      0.30,
        "oi_change":     0.20,
        "funding_rate":  0.15,
        "smc_structure": 0.20,
        "delta_flow":    0.10,
        "rsi_aux":       0.05,
    }

    THRESHOLDS = {
        SignalStrengthLong.ULTRA:    85.0,
        SignalStrengthLong.STRONG:   70.0,
        SignalStrengthLong.MODERATE: 60.0,
        SignalStrengthLong.WATCH:    50.0,
    }

    MIN_COMPONENTS_VALID = 1
    MIN_COMPONENT_SCORE  = 15.0

    def __init__(
        self,
        dump_detector=None,
        oi_analyzer=None,
        bsl_scanner=None,
        wyckoff_detector=None,
        delta_analyzer=None,
        min_score: float = 50.0,
    ):
        self.dump_detector    = dump_detector
        self.oi_analyzer      = oi_analyzer
        self.bsl_scanner      = bsl_scanner
        self.wyckoff_detector = wyckoff_detector
        self.delta_analyzer   = delta_analyzer
        self.min_score        = min_score

    def _score_to_strength(self, score: float) -> SignalStrengthLong:
        for strength, threshold in sorted(
            self.THRESHOLDS.items(), key=lambda x: x[1], reverse=True
        ):
            if score >= threshold:
                return strength
        return SignalStrengthLong.NOISE

    # ── #1+#2 Z-SCORE НИЖЕ VWAP + VOLUME (0.30) ─────────────────────
    async def _get_z_volume_score(
        self, symbol: str, ohlcv: list, md: Any
    ) -> ComponentScoreLong:
        """ГЛАВНЫЙ: цена экстремально <VWAP + объём = selling climax → отскок."""
        reasons, score, meta = [], 0.0, {}
        try:
            if self.dump_detector and ohlcv and len(ohlcv) >= 20:
                r = await self.dump_detector.detect(ohlcv, md)
                score = r.get("score", 0.0)
                meta  = {k: r.get(k) for k in ("z_score", "volume_ratio", "velocity_pct", "detected")}
                if r.get("detected"):
                    reasons.append(
                        f"DUMP EXHAUSTION: Z={meta['z_score']:.2f}σ "
                        f"Vol×{meta['volume_ratio']:.1f} — mean-reversion LONG"
                    )
                    if abs(meta.get("z_score", 0)) > 3.5:
                        score = min(score + 10, 100)
                        reasons.append(f"|Z|={abs(meta['z_score']):.2f}σ >3.5 — капитуляция")
                elif abs(meta.get("z_score", 0)) > 2.0:
                    reasons.append(f"Z={meta['z_score']:.2f}σ Vol×{meta['volume_ratio']:.1f} — частичный сигнал")
            else:
                vol  = getattr(md, "volume_spike_ratio", 1.0) or 1.0
                p1h  = getattr(md, "price_change_1h", 0) or 0
                p4h  = getattr(md, "price_change_4h", 0) or 0
                if   p1h < -15 and vol > 3.0: score = 75; reasons.append(f"Дамп {p1h:.1f}%/1H Vol×{vol:.1f} — капитуляция")
                elif p1h < -10 and vol > 2.5: score = 62; reasons.append(f"{p1h:.1f}%/1H Vol×{vol:.1f}")
                elif p4h < -20 and vol > 2.0: score = 55; reasons.append(f"{p4h:.1f}%/4H Vol×{vol:.1f}")
                elif p1h < -5  and vol > 2.0: score = 45; reasons.append(f"{p1h:.1f}%/1H Vol×{vol:.1f}")
                elif vol > 2.0:               score = 35; reasons.append(f"Vol×{vol:.1f} без ценового подтв.")
                else:                         score = 15; reasons.append("Нет Z-score/Volume сигнала")
                meta = {"vol_spike": vol, "price_chg_1h": p1h, "price_chg_4h": p4h}
        except Exception as e:
            logger.warning(f"z_volume_long {symbol}: {e}"); score = 20.0
        return ComponentScoreLong("z_volume", score, self.WEIGHTS["z_volume"],
                                  score * self.WEIGHTS["z_volume"], reasons, meta)

    # ── #3 OI CHANGE + L/S RATIO (0.20) ─────────────────────────────
    async def _get_oi_score(
        self, symbol: str, md: Any, ohlcv: list
    ) -> ComponentScoreLong:
        """OI падает = шорты закрываются → шорт-сквиз. Short bias = толпа шортит."""
        reasons, score, meta = [], 0.0, {}
        try:
            if self.oi_analyzer:
                r = await self.oi_analyzer.analyze(symbol, md)
                score = r.get("oi_score", r.get("score", 0.0))
                meta  = r.get("metadata", {})
                reasons.extend([x for x in r.get("reasons", [])
                                 if "fund" not in x.lower() and "фанд" not in x.lower()])
            else:
                oi_4d    = getattr(md, "oi_change_4d", 0) or 0
                ls_ratio = getattr(md, "long_short_ratio", 50) or 50
                short_pct = 100 - ls_ratio

                if   oi_4d < -30: score += 45; reasons.append(f"OI {oi_4d:.1f}% — массовое закрытие шортов 🟢")
                elif oi_4d < -15: score += 35; reasons.append(f"OI {oi_4d:.1f}% — шорты выходят")
                elif oi_4d < -5:  score += 20; reasons.append(f"OI {oi_4d:.1f}% — снижение")
                elif oi_4d > 20:  score -= 10; reasons.append(f"OI +{oi_4d:.1f}% растёт (против лонга)")
                elif oi_4d > 5:   score -= 5;  reasons.append(f"OI +{oi_4d:.1f}%")

                if   short_pct > 65: score += 40; reasons.append(f"Short bias {short_pct:.0f}% — толпа шортит 🟢")
                elif short_pct > 60: score += 30; reasons.append(f"Short bias {short_pct:.0f}%")
                elif short_pct > 55: score += 20; reasons.append(f"Short bias {short_pct:.0f}%")
                elif short_pct > 50: score += 10; reasons.append(f"Short bias {short_pct:.0f}%")
                elif ls_ratio > 65:  score -= 8;  reasons.append(f"Long bias {ls_ratio:.0f}% (против)")
                elif ls_ratio > 60:  score -= 3

                # Wyckoff Spring bonus
                if self.bsl_scanner and self.wyckoff_detector and ohlcv:
                    try:
                        wy = await self.wyckoff_detector.analyze(symbol, ohlcv, md)
                        if wy.get("score", 0) > 50:
                            score = min(score + wy["score"] * 0.3, 100)
                            reasons.extend(wy.get("reasons", [])[:1])
                    except Exception:
                        pass

                score = min(max(score, 0), 100)
                meta  = {"oi_4d": oi_4d, "ls_ratio": ls_ratio}
        except Exception as e:
            logger.warning(f"oi_long {symbol}: {e}"); score = 20.0
        return ComponentScoreLong("oi_change", score, self.WEIGHTS["oi_change"],
                                  score * self.WEIGHTS["oi_change"], reasons, meta)

    # ── #4 FUNDING RATE (0.15) ───────────────────────────────────────
    async def _get_funding_score(
        self, symbol: str, md: Any
    ) -> ComponentScoreLong:
        """Отрицательный funding = шорты переплачивают = шорт-сквиз вероятен."""
        reasons, score, meta = [], 0.0, {}
        try:
            f   = getattr(md, "funding_rate", 0) or 0
            acc = getattr(md, "funding_accumulated", 0) or 0

            if   f < -0.15:  score += 55; reasons.append(f"FUNDING SPIKE {f:.3f}% — шорты сильно переплачивают 🟢")
            elif f < -0.10:  score += 45; reasons.append(f"Funding низкий {f:.3f}%")
            elif f < -0.05:  score += 35; reasons.append(f"Funding отрицательный {f:.3f}%")
            elif f < -0.02:  score += 20; reasons.append(f"Funding умеренно отрицательный {f:.3f}%")
            elif f < -0.005: score += 10; reasons.append(f"Funding слабо отрицательный {f:.3f}%")
            elif f > 0.05:   score -= 15; reasons.append(f"Funding позитивный +{f:.3f}% (лонги платят — против)")
            elif f > 0.02:   score -= 8;  reasons.append(f"Funding +{f:.3f}% (умеренно против)")
            else:            score  = 15; reasons.append(f"Funding нейтральный {f:.3f}%")

            if   acc < -0.5: score = min(score + 25, 100); reasons.append(f"Накопл. фандинг {acc:.2f}%/4д 🟢")
            elif acc < -0.3: score = min(score + 15, 100); reasons.append(f"Накопл. фандинг {acc:.2f}%")
            elif acc < -0.1: score = min(score + 8,  100)
            elif acc > 0.3:  score = max(score - 10, 0);   reasons.append(f"Накопл. фандинг +{acc:.2f}% (против)")

            score = min(max(score, 0), 100)
            meta  = {"funding": f, "funding_acc": acc}
        except Exception as e:
            logger.warning(f"funding_long {symbol}: {e}"); score = 15.0
        return ComponentScoreLong("funding_rate", score, self.WEIGHTS["funding_rate"],
                                  score * self.WEIGHTS["funding_rate"], reasons, meta)

    # ── #5 SMC STRUCTURE (0.20) ──────────────────────────────────────
    async def _get_smc_score(
        self, symbol: str, ohlcv: list, price: float, sl_pct: float
    ) -> ComponentScoreLong:
        """Bullish CHoCH + Spring + OB — структурное подтверждение разворота."""
        reasons, score, meta = [], 0.0, {}
        try:
            from core.smc_ict_detector import get_smc_result
            raw = [[c.open, c.high, c.low, c.close, c.volume] for c in ohlcv] if ohlcv else []
            if len(raw) >= 20:
                smc = get_smc_result(raw, "long", base_sl_pct=sl_pct, base_entry=price)
                b = getattr(smc, "score_bonus", 0)
                meta = {"has_ob": getattr(smc, "has_ob", False),
                        "has_fvg": getattr(smc, "has_fvg", False), "bonus": b}
                if   b >= 15: score = 90; reasons.append("SMC: Bullish OB + FVG + CHoCH ✅")
                elif b >= 10: score = 75; reasons.append("SMC: Bullish OB + CHoCH")
                elif b >= 5:  score = 55; reasons.append("SMC: Bullish FVG/OB")
                elif b >  0:  score = 40; reasons.append("SMC: слабый бычий сигнал")
                else:         score = 20; reasons.append("SMC: нет подтверждения")
            else:
                score = 20; reasons.append("SMC: мало данных")
        except Exception as e:
            logger.debug(f"SMC long {symbol}: {e}"); score = 25.0
        return ComponentScoreLong("smc_structure", score, self.WEIGHTS["smc_structure"],
                                  score * self.WEIGHTS["smc_structure"], reasons, meta)

    # ── DELTA FLOW (0.10) ────────────────────────────────────────────
    async def _get_delta_score(
        self, symbol: str, ohlcv: list, md: Any
    ) -> ComponentScoreLong:
        """Бычий order flow — CVD растёт, покупатели доминируют."""
        reasons, score, meta = [], 0.0, {}
        try:
            if self.delta_analyzer:
                r = await self.delta_analyzer.analyze(symbol, ohlcv)
                score = r.get("score", 0.0); meta = r.get("metadata", {})
                reasons.extend(r.get("reasons", []))
            elif ohlcv and len(ohlcv) >= 10:
                recent = ohlcv[-20:]
                deltas = []
                for c in recent:
                    rng = c.high - c.low
                    if rng > 0:
                        body = abs(c.close - c.open)
                        bull = body / rng if c.close >= c.open else -body / rng
                        deltas.append(bull)
                if deltas:
                    avg_d = sum(deltas) / len(deltas)
                    pos_d = sum(1 for d in deltas if d > 0)
                    if   avg_d > 0.4 and pos_d > len(deltas) * 0.7: score = 80; reasons.append("Бычий CVD — покупатели доминируют")
                    elif avg_d > 0.2:                                 score = 60; reasons.append("Умеренный бычий поток")
                    elif avg_d > 0:                                   score = 40; reasons.append("Слабо бычий поток")
                    elif avg_d > -0.2:                                score = 30; reasons.append("Нейтральный поток")
                    else:                                              score = 15; reasons.append("Медвежий поток (осторожно)")
                    meta = {"avg_delta": round(avg_d, 3), "pos_bars": pos_d}
            else:
                score = 30; reasons.append("Delta: нет данных")
        except Exception as e:
            logger.warning(f"delta_long {symbol}: {e}"); score = 25.0
        return ComponentScoreLong("delta_flow", score, self.WEIGHTS["delta_flow"],
                                  score * self.WEIGHTS["delta_flow"], reasons, meta)

    # ── #6 RSI AUXILIARY (0.05) ──────────────────────────────────────
    async def _get_rsi_score(
        self, symbol: str, md: Any
    ) -> ComponentScoreLong:
        """RSI — последний. Max вклад 5pts. Никогда не блокирует LONG."""
        reasons, meta = [], {}
        try:
            rsi = getattr(md, "rsi_1h", 50) or 50
            if   rsi <= 20: score = 100; reasons.append(f"RSI {rsi:.0f} — экстремальная перепроданность ✅")
            elif rsi <= 25: score = 85;  reasons.append(f"RSI {rsi:.0f} — сильно перепродан")
            elif rsi <= 30: score = 75;  reasons.append(f"RSI {rsi:.0f} — перепродан")
            elif rsi <= 40: score = 60;  reasons.append(f"RSI {rsi:.0f} — нижняя зона")
            elif rsi <= 50: score = 45;  reasons.append(f"RSI {rsi:.0f} — нейтральная")
            elif rsi <= 60: score = 35;  reasons.append(f"RSI {rsi:.0f} — нейтральная верхняя")
            elif rsi <= 70: score = 25;  reasons.append(f"RSI {rsi:.0f} — высокий")
            else:           score = 15;  reasons.append(f"RSI {rsi:.0f} — перекуплен (мягкий штраф)")
            meta = {"rsi": rsi}
        except Exception as e:
            logger.warning(f"rsi_long {symbol}: {e}"); score = 45.0
        return ComponentScoreLong("rsi_aux", score, self.WEIGHTS["rsi_aux"],
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
    ) -> Optional[AegisLongSignal]:

        results = await asyncio.gather(
            self._get_z_volume_score(symbol, ohlcv_15m, market_data),
            self._get_oi_score(symbol, market_data, ohlcv_15m),
            self._get_funding_score(symbol, market_data),
            self._get_smc_score(symbol, ohlcv_15m, entry_price, sl_pct),
            self._get_delta_score(symbol, ohlcv_15m, market_data),
            self._get_rsi_score(symbol, market_data),
            return_exceptions=True,
        )

        components: Dict[str, ComponentScoreLong] = {}
        total_weighted = 0.0
        all_reasons: List[str] = []
        valid_components = 0

        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Component error: {r}"); continue
            cs: ComponentScoreLong = r
            components[cs.name] = cs
            total_weighted += cs.weighted
            all_reasons.extend(cs.reasons)
            if cs.raw_score >= self.MIN_COMPONENT_SCORE:
                valid_components += 1

        final_score = total_weighted

        if base_score > 0:
            if base_score >= 65:
                final_score = base_score + min(final_score * 0.15, 15)
            else:
                final_score = final_score * 0.55 + base_score * 0.45

        if final_score < self.min_score or valid_components < self.MIN_COMPONENTS_VALID:
            cs_str = " | ".join(f"{k}={v.raw_score:.0f}" for k, v in components.items())
            logger.info(f"[AEGIS REJECT LONG] {symbol}: {final_score:.1f} < {self.min_score} | {cs_str}")
            return None

        strength = self._score_to_strength(final_score)
        if strength == SignalStrengthLong.NOISE:
            return None

        from datetime import datetime
        return AegisLongSignal(
            symbol=symbol, direction="long",
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

    def format_signal_report(self, signal: AegisLongSignal) -> str:
        grade_emoji    = {"A+": "💎", "A": "🥇", "B": "🥈", "C": "🥉", "D": "⚠️"}
        strength_emoji = {
            SignalStrengthLong.ULTRA:    "🚀 ULTRA",
            SignalStrengthLong.STRONG:   "🟢 STRONG",
            SignalStrengthLong.MODERATE: "🟡 MODERATE",
            SignalStrengthLong.WATCH:    "⚪ WATCH",
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
            f"{grade_emoji.get(signal.grade,'⚠️')} <b>Aegis LONG {signal.grade}</b> | "
            f"{strength_emoji.get(signal.strength,'📊')} | Score: {signal.total_score:.1f}%\n"
            f"{lines}"
        )
