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
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("aegis.signal_engine")

# ✅ FIX: Z_VOLUME_GATE_MIN теперь читается из ENV (было хардкод 15)
_Z_VOLUME_GATE_MIN      = int(float(os.getenv("Z_VOLUME_GATE_MIN_SHORT", os.getenv("Z_VOLUME_GATE_MIN", "15"))))
# Momentum SHORT bypass — при сильном даунтренде обходим z_volume gate (RSI низкий + цена падает)
_ENABLE_MOMENTUM_SHORT  = os.getenv("ENABLE_MOMENTUM_SHORT", "true").lower() == "true"
_MOMENTUM_RSI_MAX_SHORT = float(os.getenv("MOMENTUM_RSI_MAX_SHORT", "48"))   # RSI ниже этого = падение
_MOMENTUM_VOL_MIN_SHORT = float(os.getenv("MOMENTUM_VOL_MIN_SHORT", "1.3"))  # мин volume spike
_MOMENTUM_DOWNTREND_1H  = float(os.getenv("MOMENTUM_DOWNTREND_1H",  "-1.5")) # мин падение 1H %
# Overbought SHORT bypass — при перекупленности + Premium zone + медвежий паттерн (RSI высокий, нет резкого памп)
_ENABLE_OVERBOUGHT_SHORT  = os.getenv("ENABLE_OVERBOUGHT_SHORT", "true").lower() == "true"
_OVERBOUGHT_RSI_MIN_SHORT = float(os.getenv("OVERBOUGHT_RSI_MIN_SHORT", "63"))  # RSI выше = перекуплен
# Bearish Continuation bypass — для плавных даунтрендов без volume spike (CETUS/AGI тип)
_ENABLE_BEARISH_CONT_SHORT  = os.getenv("ENABLE_BEARISH_CONT_SHORT", "true").lower() == "true"
_BEARISH_CONT_RSI_MIN       = float(os.getenv("BEARISH_CONT_RSI_MIN_SHORT", "30"))   # RSI выше этого (не на дне)
_BEARISH_CONT_RSI_MAX       = float(os.getenv("BEARISH_CONT_RSI_MAX_SHORT", "68"))   # RSI ниже этого (68 = начало даунтренда с перекупленного пика)
# Extreme Funding + Multi-Pattern bypass (AGIUSDT тип)
_ENABLE_EXTREME_FUNDING_BYPASS  = os.getenv("ENABLE_EXTREME_FUNDING_BYPASS", "true").lower() == "true"
_EXTREME_FUNDING_THRESHOLD      = float(os.getenv("EXTREME_FUNDING_BYPASS_THRESHOLD", "0.05"))  # funding ≥ 0.05%
_EXTREME_FUNDING_MIN_PATTERNS   = int(os.getenv("EXTREME_FUNDING_BYPASS_MIN_PATTERNS", "3"))    # мин паттернов
_EXTREME_FUNDING_MIN_BASE       = float(os.getenv("EXTREME_FUNDING_BYPASS_MIN_BASE", "70"))     # мин base_score
_BEARISH_CONT_BASE_MIN      = float(os.getenv("BEARISH_CONT_BASE_MIN_SHORT", "50"))  # минимальный BASE score
# Extreme funding — когда лонги переплачивают слишком много: funding spike = шортовый сигнал
_FUNDING_EXTREME_SHORT      = float(os.getenv("FUNDING_EXTREME_SHORT", "0.05"))  # % за 8ч
# C6 Fix: дополнительные bypass-уровни z_volume gate
_C6_NEAR_MISS_Z        = float(os.getenv("Z_GATE_NEAR_MISS_Z_MIN",       "6.0"))  # z≥6 + base≥65 → bypass
_C6_HIGH_SCORE_MIN     = float(os.getenv("Z_GATE_HIGH_SCORE_BYPASS_MIN", "80"))   # base≥80 + z≥2 → bypass
_C6_SYSTEMIC_BTC_PCT   = float(os.getenv("Z_GATE_SYSTEMIC_BTC_PCT",      "5.0"))  # BTC -X%/h → systemic crash (SHORT)
# Pre-pump detector: тихая консолидация + OI рост = bypass z_gate
_PRE_PUMP_SOFT_SCORE   = int(float(os.getenv("PRE_PUMP_SOFT_SCORE",   "60")))     # score≥60 → z_gate × 0.6
_PRE_PUMP_BYPASS_SCORE = int(float(os.getenv("PRE_PUMP_BYPASS_SCORE", "75")))     # score≥75 + OI → full bypass


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

    MIN_COMPONENTS_VALID = 2
    MIN_COMPONENT_SCORE  = 30.0

    def __init__(
        self,
        pump_detector=None,
        oi_analyzer=None,
        liq_mapper=None,
        smc_detector=None,
        delta_analyzer=None,
        pre_pump_detector=None,
        min_score: float = 54.0,
    ):
        self.pump_detector     = pump_detector
        self.oi_analyzer       = oi_analyzer
        self.liq_mapper        = liq_mapper
        self.smc_detector      = smc_detector
        self.delta_analyzer    = delta_analyzer
        self.pre_pump_detector = pre_pump_detector
        self.min_score         = min_score

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

        # LiquidationMapper: бонус за кластеры ликвидаций лонгов выше текущей цены
        if self.liq_mapper:
            try:
                lm = await self.liq_mapper.analyze(symbol, market_data)
                lm_s = lm.get("score", 0)
                if lm_s > 40:
                    liq_bonus = min((lm_s - 40) * 0.5, 25)
                    score = min(score + liq_bonus, 100)
                    reasons.extend(lm.get("reasons", [])[:2])
                    meta["liq_cluster_score"] = lm_s
            except Exception:
                pass

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

            # Экстремальный фандинг — лонги переплачивают критически → SHORT сигнал высокой силы
            if f >= _FUNDING_EXTREME_SHORT * 3:  # > 0.15% дефолт
                score = 90; reasons.append(f"🔥 FUNDING EXTREME +{f:.3f}% — лонги сгорают, short squeeze обратный")
                logger.info(f"[FUNDING EXTREME SHORT] {symbol}: funding={f:.4f}% >= {_FUNDING_EXTREME_SHORT * 3:.4f}% → score=90")
            elif f >= _FUNDING_EXTREME_SHORT * 2:  # > 0.10%
                score = 75; reasons.append(f"🔴 FUNDING HIGH +{f:.3f}% — лонги сильно переплачивают")
                logger.info(f"[FUNDING HIGH SHORT] {symbol}: funding={f:.4f}% >= {_FUNDING_EXTREME_SHORT * 2:.4f}% → score=75")
            elif f >= _FUNDING_EXTREME_SHORT:  # > 0.05%
                score = 60; reasons.append(f"Funding экстремальный +{f:.3f}% — лонги переплачивают")
                logger.info(f"[FUNDING ELEVATED SHORT] {symbol}: funding={f:.4f}% >= {_FUNDING_EXTREME_SHORT:.4f}% → score=60")
            elif f > 0.02:  score = 20; reasons.append(f"Funding умеренный +{f:.3f}%")
            elif f > 0.005: score = 10; reasons.append(f"Funding слабый +{f:.3f}%")
            elif f < -0.05: score  = 0; reasons.append(f"Funding отрицательный {f:.3f}% (шорты платят)")
            elif f < -0.02: score  = 5; reasons.append(f"Funding {f:.3f}%")
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
        symbol:        str,
        market_data:   Any,
        ohlcv_15m:     list,
        entry_price:   float,
        stop_loss:     float,
        sl_pct:        float,
        take_profits:  List[Tuple[float, int]],
        base_score:    float = 0.0,
        btc_change_1h: float = 0.0,
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

        # OI + Funding COMBO: одновременные экстремумы усиливают сигнал
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

        # Pre-pump (pre-dump для SHORT): тихая консолидация на вершине + OI + перегретый funding
        _pre_pump = None
        if self.pre_pump_detector and ohlcv_15m:
            try:
                _pre_pump = self.pre_pump_detector.detect(ohlcv_15m, market_data, "short")
            except Exception:
                pass

        # Wyckoff Upthrust бонус к pre-pump score (Вариант A: +15 если паттерн подтверждает SHORT)
        if _pre_pump and _pre_pump.get("detected"):
            _pats_wy = getattr(market_data, "patterns", []) or []
            _DIST_PATTERNS = {"WYCKOFF_UPTHRUST", "TRAP_LONG"}
            if any(any(d in p for d in _DIST_PATTERNS) for p in _pats_wy):
                _pp_new = min(_pre_pump["score"] + 15, 100)
                _pre_pump["score"] = _pp_new
                _pre_pump["reasons"].append("Wyckoff Upthrust паттерн подтверждает SHORT: +15pts")
                if _pp_new >= _PRE_PUMP_BYPASS_SCORE and _pre_pump["oi_confirmed"]:
                    _pre_pump["z_gate_action"] = "bypass"
                elif _pp_new >= _PRE_PUMP_SOFT_SCORE:
                    _pre_pump["z_gate_action"] = "soften"

        # HARD GATE: z_volume — главный индикатор SHORT (памп/перекупленность).
        z_vol = components.get("z_volume")
        # ── BUG-3 FIX: Adaptive z_gate — при системных условиях снижаем порог до 3 ──
        # При BTC краше -5%/h или экстремальном funding объём может быть ниже нормы
        # (институционалы уже вышли, паники ещё нет) → z_gate = 3 вместо полного отказа
        _z_effective = _Z_VOLUME_GATE_MIN
        _funding_now = getattr(market_data, "funding_rate", 0.0) or 0.0
        if (_Z_VOLUME_GATE_MIN > 3
                and (btc_change_1h <= -_C6_SYSTEMIC_BTC_PCT                   # BUG-1: BTC краш (было >= памп)
                     or abs(_funding_now) >= _EXTREME_FUNDING_THRESHOLD)):     # экстремальный funding
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
                    f"[PRE-PUMP SHORT] {symbol}: score={_pp_score} → z_gate softened to {_z_effective:.1f}"
                )
            elif _pp_action == "bypass":
                _z_effective = 0

        _bearish_cont_bypass = False  # v1: тренд-шорт без pump spike
        _z_gate_failed = z_vol and z_vol.raw_score < _z_effective
        if _z_gate_failed:
            _momentum_bypass = False
            _bearish_cont_bypass = False
            if _ENABLE_MOMENTUM_SHORT:
                _rsi     = getattr(market_data, "rsi_1h", 50)              or 50
                _vol_spk = getattr(market_data, "volume_spike_ratio", 1.0) or 1.0
                _p1h     = getattr(market_data, "price_change_1h", 0)      or 0
                _p4h     = getattr(market_data, "price_change_4h", 0)      or 0
                _p24h    = getattr(market_data, "price_change_24h", 0)     or 0
                # Momentum SHORT: RSI низкий + volume spike + цена падает
                if (_rsi <= _MOMENTUM_RSI_MAX_SHORT
                        and _vol_spk >= _MOMENTUM_VOL_MIN_SHORT
                        and (_p1h < _MOMENTUM_DOWNTREND_1H or _p4h < -5.0 or _p24h < -5.0)):
                    # ANTI-CATASTROPHE: не даём Momentum bypass при RSI + пост-дамп условиях
                    # SUI-паттерн: RSI<30 + 4H уже упал >7% = шортим дно, не продолжение тренда
                    _ac_rsi_block  = float(os.getenv("SHORT_RSI_OVERSOLD_BLOCK",    "30"))
                    _ac_drop_block = float(os.getenv("SHORT_BLOCK_AFTER_DROP_PCT", "7.0"))
                    if _rsi < _ac_rsi_block and _p4h < -_ac_drop_block:
                        logger.info(
                            f"[AEGIS MOMENTUM CANCEL] {symbol}: RSI={_rsi:.0f} < {_ac_rsi_block} "
                            f"+ 4H={_p4h:+.1f}% < -{_ac_drop_block}% — post-dump protection, bypass отменён"
                        )
                    else:
                        _momentum_bypass = True
                        all_reasons.append(
                            f"MOMENTUM SHORT bypass: RSI={_rsi:.0f} Vol×{_vol_spk:.1f} "
                            f"1H={_p1h:+.1f}% 4H={_p4h:+.1f}%"
                        )
                        logger.info(
                            f"[AEGIS MOMENTUM SHORT] {symbol}: z_volume={z_vol.raw_score:.0f} < {_z_effective} "
                            f"→ Momentum bypass (RSI={_rsi:.0f} Vol×{_vol_spk:.1f} 1H={_p1h:+.1f}%)"
                        )
                        if _p24h < -15: final_score = min(final_score + 10, 100)
                        elif _p24h < -8: final_score = min(final_score + 5, 100)

            # Overbought SHORT bypass: RSI высокий + цена растёт + медвежий паттерн
            # Для случаев когда памп медленный (нет резкого volume spike, но перекуплено)
            if not _momentum_bypass and _ENABLE_OVERBOUGHT_SHORT:
                _rsi_ob  = getattr(market_data, "rsi_1h", 50)             or 50
                _ls_ob   = getattr(market_data, "long_short_ratio", 50)   or 50
                _trend   = getattr(market_data, "price_trend", "")        or ""
                _pats    = getattr(market_data, "patterns", [])           or []
                _BEARISH = {"MEGA_SHORT", "TRAP_LONG", "REJECTION_SHORT", "WYCKOFF_UPTHRUST"}
                _has_bear = any(any(b in p for b in _BEARISH) for p in _pats)
                if (_rsi_ob >= _OVERBOUGHT_RSI_MIN_SHORT
                        and _ls_ob >= 55              # лонги доминируют
                        and _trend == "up"            # цена на подъёме
                        and _has_bear):               # есть медвежий паттерн
                    _momentum_bypass = True
                    all_reasons.append(
                        f"OVERBOUGHT SHORT bypass: RSI={_rsi_ob:.0f} L/S={_ls_ob:.0f}% "
                        f"trend=up patterns={[p for p in _pats if any(b in p for b in _BEARISH)][:2]}"
                    )
                    logger.info(
                        f"[AEGIS OVERBOUGHT SHORT] {symbol}: z_volume={z_vol.raw_score:.0f} < {_z_effective} "
                        f"→ Overbought bypass (RSI={_rsi_ob:.0f} L/S={_ls_ob:.0f}% pattern)"
                    )

            # Bearish Continuation bypass: плавный даунтренд без pump spike (CETUS/AGI тип)
            if not _momentum_bypass and _ENABLE_BEARISH_CONT_SHORT:
                _rsi_bc  = getattr(market_data, "rsi_1h", 50)           or 50
                _p1h_bc  = getattr(market_data, "price_change_1h", 0)   or 0
                _p4h_bc  = getattr(market_data, "price_change_4h", 0)   or 0
                _p24h_bc = getattr(market_data, "price_change_24h", 0)  or 0
                _htf_bc  = getattr(market_data, "htf_structure", "")    or ""
                # RANGING = нейтральный HTF, тоже допускает bearish cont (цена может идти в любую сторону)
                _htf_is_bear_bc = "bear" in _htf_bc.lower() or "ranging" in _htf_bc.lower()
                # Fix #2: если htf_structure пустая (BingX offline) но цена явно падает — treat as ranging
                if not _htf_is_bear_bc and not _htf_bc and _p4h_bc < -2.0:
                    _htf_is_bear_bc = True
                if (_BEARISH_CONT_RSI_MIN <= _rsi_bc <= _BEARISH_CONT_RSI_MAX
                        and (_p4h_bc < -2.0 or _p24h_bc < -8.0)
                        and _htf_is_bear_bc
                        and base_score >= _BEARISH_CONT_BASE_MIN):
                    # ANTI-CATASTROPHE: DYM-паттерн — цена растёт 1H при 24H дампе = шортим отскок, НЕ продолжение
                    _ac_drop_block_bc  = float(os.getenv("SHORT_BLOCK_AFTER_DROP_PCT", "7.0"))
                    _ac_bounce_1h      = 1.0  # 1H рост > 1% = активный отскок
                    _is_shorting_bounce = _p1h_bc > _ac_bounce_1h and _p24h_bc < -(_ac_drop_block_bc * 1.5)
                    if _is_shorting_bounce:
                        logger.info(
                            f"[AEGIS BEARISH CONT CANCEL] {symbol}: 1H={_p1h_bc:+.1f}% растёт "
                            f"при 24H={_p24h_bc:+.1f}% — DYM-паттерн, шортим отскок, bypass отменён"
                        )
                    else:
                        _momentum_bypass = True
                        _bearish_cont_bypass = True  # v1: смещаем вес в сторону base_score
                        all_reasons.append(
                            f"BEARISH CONT bypass: RSI={_rsi_bc:.0f} 4H={_p4h_bc:+.1f}% "
                            f"24H={_p24h_bc:+.1f}% HTF={_htf_bc[:20]}"
                        )
                        logger.info(
                            f"[AEGIS BEARISH CONT] {symbol}: z_volume={z_vol.raw_score:.0f} < {_z_effective} "
                            f"→ Bearish continuation bypass (RSI={_rsi_bc:.0f} 4H={_p4h_bc:+.1f}% "
                            f"24H={_p24h_bc:+.1f}%)"
                        )

            # Extreme Funding + Multi-Pattern bypass: шорты платят лонгистам + структурные паттерны
            if not _momentum_bypass and _ENABLE_EXTREME_FUNDING_BYPASS:
                _funding_ef  = getattr(market_data, "funding_rate", 0.0)     or 0.0
                _pats_ef     = getattr(market_data, "patterns", [])           or []
                _n_pats_ef   = len(_pats_ef)
                if (_funding_ef >= _EXTREME_FUNDING_THRESHOLD
                        and _n_pats_ef >= _EXTREME_FUNDING_MIN_PATTERNS
                        and base_score >= _EXTREME_FUNDING_MIN_BASE):
                    _momentum_bypass = True
                    all_reasons.append(
                        f"EXTREME FUNDING bypass: funding={_funding_ef:.4f}% "
                        f"patterns={_n_pats_ef} base={base_score:.0f}"
                    )
                    logger.info(
                        f"[AEGIS EXTREME FUNDING] {symbol}: z_volume={z_vol.raw_score:.0f} < {_z_effective} "
                        f"→ Extreme funding bypass (funding={_funding_ef:.4f}% pats={_n_pats_ef} base={base_score:.0f})"
                    )

            # C6 Fix: Near-miss / High-Score / Systemic Pump bypass
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
                    # BUG-1 FIX: Системный КРАШ BTC (было: памп — логическая инверсия!)
                    # При BTC -5%/h весь рынок падает → шорты актуальны, z_gate слишком строгий
                    _momentum_bypass = True
                    all_reasons.append(f"C6 SYSTEMIC CRASH bypass: BTC 1H={btc_change_1h:+.1f}% z={_z_raw:.1f}")
                    logger.info(f"[AEGIS C6 SYSTEMIC] {symbol}: BTC {btc_change_1h:+.1f}% crash → systemic dump → bypass")

            # Pre-pump bypass: сильная консолидация + OI подтверждён + перегретый funding
            if not _momentum_bypass and _pre_pump and _pre_pump.get("detected"):
                _pp_score = _pre_pump.get("score", 0)
                _pp_oi    = _pre_pump.get("oi_confirmed", False)
                if _pp_score >= _PRE_PUMP_BYPASS_SCORE and _pp_oi:
                    _momentum_bypass = True
                    all_reasons.append(
                        f"PRE-PUMP SHORT bypass: консолидация score={_pp_score} + OI↑ "
                        f"({'; '.join(_pre_pump.get('reasons', [])[:2])})"
                    )
                    final_score = min(final_score + 8, 100)
                    logger.info(
                        f"[PRE-PUMP BYPASS SHORT] {symbol}: score={_pp_score} oi_ok={_pp_oi} "
                        f"z={z_vol.raw_score:.0f} < {_z_effective} → bypass"
                    )

            if not _momentum_bypass:
                logger.info(
                    f"[AEGIS REJECT] {symbol}: z_volume={z_vol.raw_score:.0f} < {_z_effective} "
                    f"— нет pump exhaustion или momentum downtrend, сигнал отклонён"
                )
                return None

        if base_score > 0:
            if base_score >= 70:
                if _bearish_cont_bypass:
                    # v1: тренд-шорт без pump spike → base_score главный (80/20)
                    final_score = total_weighted * 0.20 + base_score * 0.80
                    logger.info(
                        f"[AEGIS BEARISH CONT SCORE] {symbol}: "
                        f"tw={total_weighted:.1f}×0.20 + base={base_score:.0f}×0.80 = {final_score:.1f}"
                    )
                else:
                    # Aegis — качественный фильтр: реальное взвешенное среднее 45/55
                    final_score = total_weighted * 0.45 + base_score * 0.55
            elif base_score >= 58:
                # Хороший базовый скор — base_score главный (70%), Aegis — фильтр
                final_score = total_weighted * 0.30 + base_score * 0.70
            else:
                # Слабый базовый скор — Aegis компенсирует (50/50)
                final_score = total_weighted * 0.50 + base_score * 0.50

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

