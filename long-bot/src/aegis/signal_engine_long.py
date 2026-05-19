"""
Aegis LONG Signal Engine v2.1
Иерархия сигналов (mean-reversion + momentum LONG):

  #1+#2  z_volume:      0.30  — Z-Score ниже VWAP + Volume Spike (капитуляция)
  #3     oi_change:     0.20  — OI падает + Short bias (шорты закрываются)
  #4     funding_rate:  0.15  — Отрицательный Funding (шорты переплачивают)
  #5     smc_structure: 0.20  — Bullish CHoCH + Spring + OB
         delta_flow:    0.10  — Бычий CVD/order flow
  #6     rsi_aux:       0.05  — RSI вспомогательный (НЕ gate)

RSI НИКОГДА не блокирует сигнал. Только +/- корректирует.

v2.1: ENV-управление z_volume gate + Momentum LONG bypass
  Z_VOLUME_GATE_MIN (default=8): порог dump-exhaustion (было хардкод 20)
  ENABLE_MOMENTUM_LONG (default=true): разрешить Momentum-обход z_volume
  MOMENTUM_RSI_MIN (default=58): мин RSI для momentum mode
  MOMENTUM_VOL_MIN (default=1.8): мин volume spike для momentum mode
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("aegis.signal_engine_long")

# ── ENV-конфиг z_volume gate + Momentum ──────────────────────────────────────
_Z_VOLUME_GATE_MIN    = int(float(os.getenv("Z_VOLUME_GATE_MIN", "8")))      # было хардкод 20
_ENABLE_MOMENTUM_LONG = os.getenv("ENABLE_MOMENTUM_LONG", "true").lower() == "true"
_MOMENTUM_RSI_MIN     = float(os.getenv("MOMENTUM_RSI_MIN", "58"))           # RSI мин для momentum
_MOMENTUM_VOL_MIN     = float(os.getenv("MOMENTUM_VOL_MIN", "1.8"))          # Volume spike мин
# Extreme funding — когда шорты переплачивают критически → LONG (short squeeze сигнал)
_FUNDING_EXTREME_LONG = float(os.getenv("FUNDING_EXTREME_LONG", "-0.05"))   # % за 8ч (отрицательное значение)
# ✅ FIX #4: Extreme Funding LONG bypass — при экстремальном отрицательном funding
#   шорты переплачивают настолько, что z_volume gate можно обойти (ALGOUSDT, CETUS тип)
_ENABLE_EF_BYPASS_LONG  = os.getenv("ENABLE_EXTREME_FUNDING_BYPASS", "true").lower() == "true"
_EF_THRESHOLD_LONG      = abs(float(os.getenv("EXTREME_FUNDING_BYPASS_THRESHOLD", "0.05")))  # 0.05 → |funding| ≥ 0.05%
_EF_MIN_BASE_LONG       = float(os.getenv("EXTREME_FUNDING_BYPASS_MIN_BASE", "65"))          # base ≥ 65
# C6 Fix: дополнительные bypass-уровни z_volume gate
_C6_NEAR_MISS_Z        = float(os.getenv("Z_GATE_NEAR_MISS_Z_MIN",       "6.0"))  # z≥6 + base≥65 → bypass
_C6_HIGH_SCORE_MIN     = float(os.getenv("Z_GATE_HIGH_SCORE_BYPASS_MIN", "80"))   # base≥80 + z≥2 → bypass
_C6_SYSTEMIC_BTC_PCT   = float(os.getenv("Z_GATE_SYSTEMIC_BTC_PCT",      "5.0"))  # BTC -X%/h → systemic crash
# Pre-pump detector: тихая консолидация + OI рост = bypass z_gate
_PRE_PUMP_SOFT_SCORE   = int(float(os.getenv("PRE_PUMP_SOFT_SCORE",   "60")))     # score≥60 → z_gate × 0.6
_PRE_PUMP_BYPASS_SCORE = int(float(os.getenv("PRE_PUMP_BYPASS_SCORE", "75")))     # score≥75 + OI → full bypass
# Variant B: z_volume=0 (dump_detector ничего не нашёл) → score-weighted bypass
# Случай: цена в аптренде (выше VWAP), нет dump exhaustion — но сигнал прошёл все остальные фильтры
_Z_NODATA_BYPASS_MIN  = int(float(os.getenv("Z_NODATA_BYPASS_MIN",      "85")))  # base≥85 → полный bypass
_Z_NODATA_SOFT_MIN    = int(float(os.getenv("Z_NODATA_SOFT_BYPASS_MIN", "75")))  # base≥75 → bypass −5pts


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

    MIN_COMPONENTS_VALID = 2
    MIN_COMPONENT_SCORE  = 20.0

    def __init__(
        self,
        dump_detector=None,
        oi_analyzer=None,
        bsl_scanner=None,
        wyckoff_detector=None,
        delta_analyzer=None,
        liq_mapper=None,
        netflow_analyzer=None,
        pre_pump_detector=None,
        min_score: float = 50.0,
    ):
        self.dump_detector     = dump_detector
        self.oi_analyzer       = oi_analyzer
        self.bsl_scanner       = bsl_scanner
        self.wyckoff_detector  = wyckoff_detector
        self.delta_analyzer    = delta_analyzer
        self.liq_mapper        = liq_mapper
        self.netflow_analyzer  = netflow_analyzer
        self.pre_pump_detector = pre_pump_detector
        self.min_score         = min_score

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
                if self.wyckoff_detector and ohlcv:
                    try:
                        wy = await self.wyckoff_detector.analyze(symbol, ohlcv, md)
                        wy_s = wy.get("score", 0)
                        meta["wyckoff_score"] = wy_s
                        if wy_s > 50:
                            score = min(score + wy_s * 0.3, 100)
                            reasons.extend(wy.get("reasons", [])[:1])
                    except Exception:
                        pass

                score = min(max(score, 0), 100)
                meta  = {"oi_4d": oi_4d, "ls_ratio": ls_ratio}
        except Exception as e:
            logger.warning(f"oi_long {symbol}: {e}"); score = 20.0

        # LiquidationMapperLong: бонус за зоны short squeeze (кластеры шортов на дне)
        if self.liq_mapper:
            try:
                lm = await self.liq_mapper.analyze(symbol, md)
                lm_s = lm.get("score", 0)
                if lm_s > 40:
                    liq_bonus = min((lm_s - 40) * 0.5, 25)
                    score = min(score + liq_bonus, 100)
                    reasons.extend(lm.get("reasons", [])[:2])
                    meta["liq_cluster_score"] = lm_s
            except Exception:
                pass

        # NetflowAnalyzerLong: outflow с бирж = институциональное накопление = LONG
        if self.netflow_analyzer:
            try:
                nf = await self.netflow_analyzer.analyze(symbol)
                nf_s = nf.get("score", 40)
                if nf_s > 55:
                    nf_bonus = min((nf_s - 55) * 0.4, 18)
                    score = min(score + nf_bonus, 100)
                    reasons.extend(nf.get("reasons", [])[:1])
                    meta["netflow_score"] = nf_s
                    meta["netflow_signal"] = nf.get("metadata", {}).get("signal", "")
            except Exception:
                pass

        # BSLScanner: зоны Buy-Side Liquidity выше = магниты для LONG движения
        if self.bsl_scanner and ohlcv:
            try:
                bsl = await self.bsl_scanner.analyze(symbol, md, ohlcv)
                bsl_s = bsl.get("score", 0)
                if bsl_s > 30:
                    bsl_bonus = min((bsl_s - 30) * 0.5, 20)
                    score = min(score + bsl_bonus, 100)
                    reasons.extend(bsl.get("reasons", [])[:2])
                    meta["bsl_score"] = bsl_s
            except Exception:
                pass

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

            # Экстремальный отрицательный фандинг — шорты переплачивают критически → LONG (short squeeze)
            if f <= _FUNDING_EXTREME_LONG * 3:   # < -0.15% дефолт
                score = 90; reasons.append(f"🔥 FUNDING EXTREME {f:.3f}% — шорты сгорают, short squeeze!")
                logger.info(f"[FUNDING EXTREME LONG] {symbol}: funding={f:.4f}% <= {_FUNDING_EXTREME_LONG * 3:.4f}% → score=90")
            elif f <= _FUNDING_EXTREME_LONG * 2:  # < -0.10%
                score = 75; reasons.append(f"🟢 FUNDING HIGH {f:.3f}% — шорты сильно переплачивают")
                logger.info(f"[FUNDING HIGH LONG] {symbol}: funding={f:.4f}% <= {_FUNDING_EXTREME_LONG * 2:.4f}% → score=75")
            elif f <= _FUNDING_EXTREME_LONG:      # < -0.05%
                score = 60; reasons.append(f"Funding экстремальный {f:.3f}% — шорты переплачивают")
                logger.info(f"[FUNDING ELEVATED LONG] {symbol}: funding={f:.4f}% <= {_FUNDING_EXTREME_LONG:.4f}% → score=60")
            elif f < -0.02:  score = 20; reasons.append(f"Funding умеренно отрицательный {f:.3f}%")
            elif f < -0.005: score = 10; reasons.append(f"Funding слабо отрицательный {f:.3f}%")
            elif f > 0.05:   score  = 0; reasons.append(f"Funding позитивный +{f:.3f}% (лонги платят — против)")
            elif f > 0.02:   score  = 5; reasons.append(f"Funding +{f:.3f}% (умеренно против)")
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
        symbol:        str,
        market_data:   Any,
        ohlcv_15m:     list,
        entry_price:   float,
        stop_loss:     float,
        sl_pct:        float,
        take_profits:  List[Tuple[float, int]],
        base_score:    float = 0.0,
        btc_change_1h: float = 0.0,
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

        # OI + Funding COMBO: одновременные экстремумы усиливают LONG сигнал
        _oi_c   = components.get("oi_change")
        _fund_c = components.get("funding_rate")
        if _oi_c and _fund_c and _oi_c.raw_score >= 65 and _fund_c.raw_score >= 65:
            _avg = (_oi_c.raw_score + _fund_c.raw_score) / 2
            _combo_bonus = round(min((_avg - 60) * 0.3, 12), 1)
            final_score = min(final_score + _combo_bonus, 100)
            all_reasons.append(
                f"⚡ OI+Funding COMBO +{_combo_bonus:.1f}pts "
                f"(OI={_oi_c.raw_score:.0f} Fund={_fund_c.raw_score:.0f})"
            )

        # Pre-pump: тихая консолидация + OI рост — может ослабить z_gate
        _pre_pump = None
        if self.pre_pump_detector and ohlcv_15m:
            try:
                _pre_pump = self.pre_pump_detector.detect(ohlcv_15m, market_data, "long")
            except Exception:
                pass

        # Wyckoff Spring бонус к pre-pump score (Вариант A: +15 max)
        if _pre_pump and _pre_pump.get("detected"):
            _oi_comp = components.get("oi_change")
            if _oi_comp:
                _wy_s = _oi_comp.metadata.get("wyckoff_score", 0)
                if _wy_s > 50:
                    _wy_bonus = min(int((_wy_s - 50) * 0.3), 15)
                    _pp_new = min(_pre_pump["score"] + _wy_bonus, 100)
                    _pre_pump["score"] = _pp_new
                    _pre_pump["reasons"].append(f"Wyckoff Spring: +{_wy_bonus}pts (wy={_wy_s:.0f})")
                    if _pp_new >= _PRE_PUMP_BYPASS_SCORE and _pre_pump["oi_confirmed"]:
                        _pre_pump["z_gate_action"] = "bypass"
                    elif _pp_new >= _PRE_PUMP_SOFT_SCORE:
                        _pre_pump["z_gate_action"] = "soften"

        # GATE: z_volume — главный индикатор mean-reversion LONG.
        z_vol = components.get("z_volume")
        # ── BUG-4 FIX: Adaptive z_gate — при системных условиях снижаем порог до 3 ──
        # При BTC краше -5%/h или экстремальном funding объём аномально низкий → z_gate = 3
        # FIDAUSDT score=98 z=1 был убит при системном сливе — эта логика это исправляет
        _z_effective = _Z_VOLUME_GATE_MIN
        _funding_now = getattr(market_data, "funding_rate", 0.0) or 0.0
        if (_Z_VOLUME_GATE_MIN > 3
                and (btc_change_1h <= -_C6_SYSTEMIC_BTC_PCT                   # BTC краш -5%/h
                     or abs(_funding_now) >= _EF_THRESHOLD_LONG)):             # экстремальный funding
            _z_effective = 3
            logger.debug(
                f"[Z_GATE_ADAPTIVE] {symbol}: BTC={btc_change_1h:+.1f}% fund={_funding_now:.4f}% "
                f"→ z_gate adaptive 3 (normal={_Z_VOLUME_GATE_MIN})"
            )
        # Pre-pump: ослабляем z_gate при умеренном паттерне (Вариант C)
        if _pre_pump and _pre_pump.get("detected"):
            _pp_score = _pre_pump.get("score", 0)
            _pp_action = _pre_pump.get("z_gate_action", "none")
            if _pp_action == "soften" and _z_effective > 1:
                _z_effective = max(_z_effective * 0.6, 1.0)
                logger.info(
                    f"[PRE-PUMP LONG] {symbol}: score={_pp_score} → z_gate softened to {_z_effective:.1f}"
                )
            elif _pp_action == "bypass":
                _z_effective = 0  # полный bypass обрабатывается ниже в bypass-секции

        _z_gate_failed = z_vol and z_vol.raw_score < _z_effective
        if _z_gate_failed:
            _momentum_bypass = False
            if _ENABLE_MOMENTUM_LONG:
                _rsi      = getattr(market_data, "rsi_1h", 50)     or 50
                _vol_spk  = getattr(market_data, "volume_spike_ratio", 1.0) or 1.0
                _p1h      = getattr(market_data, "price_change_1h", 0)      or 0
                _p4h      = getattr(market_data, "price_change_4h", 0)      or 0
                _p24h     = getattr(market_data, "price_change_24h", 0)     or 0
                # Momentum: RSI высокий + volume spike + цена растёт
                if (_rsi >= _MOMENTUM_RSI_MIN
                        and _vol_spk >= _MOMENTUM_VOL_MIN
                        and (_p1h > 0.3 or _p4h > 1.0 or _p24h > 2.0)):
                    _momentum_bypass = True
                    all_reasons.append(
                        f"MOMENTUM LONG bypass: RSI={_rsi:.0f} Vol×{_vol_spk:.1f} "
                        f"1H={_p1h:+.1f}% 4H={_p4h:+.1f}%"
                    )
                    logger.info(
                        f"[AEGIS MOMENTUM] {symbol}: z_volume={z_vol.raw_score:.0f} < {_z_effective} "
                        f"→ Momentum bypass (RSI={_rsi:.0f} Vol×{_vol_spk:.1f})"
                    )
                    # Бонус за momentum в score
                    if _p24h > 10: final_score = min(final_score + 10, 100)
                    elif _p24h > 5: final_score = min(final_score + 5, 100)

            # ✅ FIX #4: Extreme Funding LONG bypass
            # При экстремально отрицательном funding шорты переплачивают критически.
            # Это сигнал накопления позиций — даже без dump exhaustion z_volume.
            # Примеры из логов: ALGOUSDT funding=-0.0565% score=77 z=2 → REJECTED (убытки)
            #                   CETUSUSDT score=95 z=2 → REJECTED, BANANAUSDT z=3 → REJECTED
            if not _momentum_bypass and _ENABLE_EF_BYPASS_LONG:
                _funding_ef = getattr(market_data, "funding_rate", 0.0) or 0.0
                _pats_ef    = getattr(market_data, "patterns", [])       or []
                _n_pats     = len(_pats_ef)
                if (_funding_ef <= -_EF_THRESHOLD_LONG      # funding ≤ -0.05% (шорты переплачивают)
                        and base_score >= _EF_MIN_BASE_LONG):  # база ≥ 65
                    _momentum_bypass = True
                    all_reasons.append(
                        f"EXTREME FUNDING LONG bypass: funding={_funding_ef:.4f}% "
                        f"pats={_n_pats} base={base_score:.0f}"
                    )
                    logger.info(
                        f"[AEGIS EXTREME FUNDING LONG] {symbol}: z_volume={z_vol.raw_score:.0f} "
                        f"< {_z_effective} → bypass (funding={_funding_ef:.4f}% "
                        f"base={base_score:.0f})"
                    )

            # C6 Fix: Near-miss / High-Score / Systemic Crash bypass
            if not _momentum_bypass:
                _z_raw = z_vol.raw_score if z_vol else 0.0
                if _z_raw >= _C6_NEAR_MISS_Z and base_score >= 65:
                    # z близко к порогу (например 6.0/7.0 из 8) + высокий score
                    _momentum_bypass = True
                    all_reasons.append(f"C6 NEAR-MISS bypass: z={_z_raw:.1f}≥{_C6_NEAR_MISS_Z} base={base_score:.0f}")
                    logger.info(f"[AEGIS C6 NEAR-MISS] {symbol}: z={_z_raw:.1f} near gate {_z_effective}, base={base_score:.0f}")
                elif base_score >= _C6_HIGH_SCORE_MIN and _z_raw >= 2.0:
                    # Исключительный score (≥80) перевешивает слабый volume при наличии минимальной активности
                    _momentum_bypass = True
                    all_reasons.append(f"C6 HIGH-SCORE bypass: base={base_score:.0f}≥{_C6_HIGH_SCORE_MIN} z={_z_raw:.1f}")
                    logger.info(f"[AEGIS C6 HIGH-SCORE] {symbol}: base={base_score:.0f} exceptional → bypass z_gate")
                elif btc_change_1h <= -_C6_SYSTEMIC_BTC_PCT and _z_raw >= 1.5:
                    # Системный краш BTC → объём на лонгируемых монетах нарастает, z_gate слишком строгий
                    _momentum_bypass = True
                    all_reasons.append(f"C6 SYSTEMIC CRASH bypass: BTC 1H={btc_change_1h:+.1f}% z={_z_raw:.1f}")
                    logger.info(f"[AEGIS C6 SYSTEMIC] {symbol}: BTC {btc_change_1h:+.1f}% crash → volume building → bypass")

            # Pre-pump bypass: сильная консолидация + OI подтверждён
            if not _momentum_bypass and _pre_pump and _pre_pump.get("detected"):
                _pp_score = _pre_pump.get("score", 0)
                _pp_oi    = _pre_pump.get("oi_confirmed", False)
                if _pp_score >= _PRE_PUMP_BYPASS_SCORE and _pp_oi:
                    _momentum_bypass = True
                    all_reasons.append(
                        f"PRE-PUMP LONG bypass: консолидация score={_pp_score} + OI↑ "
                        f"({'; '.join(_pre_pump.get('reasons', [])[:2])})"
                    )
                    final_score = min(final_score + 8, 100)
                    logger.info(
                        f"[PRE-PUMP BYPASS LONG] {symbol}: score={_pp_score} oi_ok={_pp_oi} "
                        f"z={z_vol.raw_score:.0f} < {_z_effective} → bypass"
                    )
                elif _pp_score >= _PRE_PUMP_SOFT_SCORE:
                    # Умеренный паттерн — уже ослабили z_gate выше, повторная попытка не нужна
                    pass

            # ── Variant B: z_volume≤1 + высокий base_score → score-weighted bypass ──
            # dump_detector вернул 0-1 — не потому что сигнал плохой, а потому что:
            #   • цена выше VWAP (uptrend — нет dump exhaustion по определению)
            #   • low volume или velocity penalty → итог ≤1
            # Сигнал уже прошёл: BASE_SCORER, REALTIME, MTF, PatternML, Consolidation,
            # Trend, KillZone, SMC, SRCluster — если base_score≥75, dump_detector не должен блокировать.
            if not _momentum_bypass and z_vol and z_vol.raw_score <= 1:
                _z_raw_b = z_vol.raw_score
                if base_score >= _Z_NODATA_BYPASS_MIN:
                    _momentum_bypass = True
                    all_reasons.append(
                        f"🔓 Z_NODATA bypass: base={base_score:.0f}≥{_Z_NODATA_BYPASS_MIN} "
                        f"(z_raw={_z_raw_b:.0f} — uptrend, нет dump exhaustion)"
                    )
                    logger.info(
                        f"[Z_NODATA BYPASS LONG] {symbol}: z_raw={_z_raw_b:.0f}≤1 "
                        f"base={base_score:.0f}≥{_Z_NODATA_BYPASS_MIN} → bypass z_gate"
                    )
                elif base_score >= _Z_NODATA_SOFT_MIN:
                    _momentum_bypass = True
                    final_score = max(final_score - 5, 0)
                    all_reasons.append(
                        f"🔓 Z_NODATA soft bypass: base={base_score:.0f}≥{_Z_NODATA_SOFT_MIN} "
                        f"(z_raw={_z_raw_b:.0f}) — −5pts неопределённость"
                    )
                    logger.info(
                        f"[Z_NODATA SOFT BYPASS LONG] {symbol}: z_raw={_z_raw_b:.0f}≤1 "
                        f"base={base_score:.0f}≥{_Z_NODATA_SOFT_MIN} → soft bypass (−5pts)"
                    )

            if not _momentum_bypass:
                logger.info(
                    f"[AEGIS REJECT LONG] {symbol}: z_volume={z_vol.raw_score:.0f} < {_z_effective} "
                    f"— нет признаков dump exhaustion или momentum, сигнал отклонён"
                )
                return None

        if base_score > 0:
            if base_score >= 70:
                final_score = total_weighted * 0.45 + base_score * 0.55
            elif base_score >= 58:
                final_score = total_weighted * 0.30 + base_score * 0.70
            else:
                final_score = total_weighted * 0.50 + base_score * 0.50

        # ── HTF TREND ALIGNMENT: штраф если 4H+1H оба медвежьи ──────────
        # Предотвращает "ловлю падающего ножа" при нисходящем HTF тренде (WLD, FHE паттерн)
        _p4h_chk = getattr(market_data, "price_change_4h", 0.0) or 0.0
        _p1h_chk = getattr(market_data, "price_change_1h", 0.0) or 0.0
        _htf_str_chk = getattr(market_data, "htf_structure", "") or ""
        _htf_bearish = (
            "bear" in _htf_str_chk.lower()
            or (_p4h_chk < -1.5 and _p1h_chk < -0.3)
        )
        if _htf_bearish:
            _has_reversal = any(
                kw in r.lower()
                for r in all_reasons
                for kw in ("bos", "choch", "spring", "sweep", "reversal", "разворот", "чо-чо", "восстановление")
            )
            if not _has_reversal:
                _htf_penalty = float(os.environ.get("HTF_BEARISH_PENALTY", "5"))
                final_score = max(final_score - _htf_penalty, 0)
                all_reasons.append(
                    f"⚠️ HTF BEARISH penalty −{_htf_penalty:.0f}: 4H={_p4h_chk:+.1f}% 1H={_p1h_chk:+.1f}% "
                    f"— нет подтверждения разворота"
                )
                logger.debug(
                    f"[HTF_PENALTY] {symbol}: 4H={_p4h_chk:+.1f}% 1H={_p1h_chk:+.1f}% bearish "
                    f"→ −{_htf_penalty:.0f}pts (нет BOS/CHoCH/Spring)"
                )
            else:
                all_reasons.append(f"✅ HTF bearish но есть подтверждение разворота")

        # Порог: используем min_score из конфига напрямую
        effective_min = self.min_score

        if final_score < effective_min or valid_components < self.MIN_COMPONENTS_VALID:
            cs_str = " | ".join(f"{k}={v.raw_score:.0f}" for k, v in components.items())
            logger.info(f"[AEGIS REJECT LONG] {symbol}: {final_score:.1f} < {effective_min} | {cs_str}")
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

