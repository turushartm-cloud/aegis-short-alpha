"""
Wyckoff Accumulation Detector v1.0 — Aegis Long Alpha
Детектор институционального накопления по методу Вайкоффа.

Фазы накопления (Schematic 1):
  Phase A: Selling Climax (SC) + Automatic Rally (AR) + Secondary Test (ST)
  Phase B: Building Cause (консолидация, OI/объём падают)
  Phase C: Spring (прощупывание дна, ложный пробой поддержки)
  Phase D: Sign of Strength (SOS) + Last Point of Support (LPS)
  Phase E: Markup (старт восходящего тренда)

Сигнал LONG входа:
  Лучший: Spring + объём падает → SOS + объём растёт (конец Phase C)
  Хороший: LPS в Phase D (подтверждённое накопление)
  Средний: AR после SC (начало накопления — осторожно)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("aegis.wyckoff_detector")


@dataclass
class WyckoffPhase:
    phase:       str    = "unknown"   # A, B, C, D, E
    event:       str    = ""          # SC, AR, ST, Spring, SOS, LPS
    score:       float  = 0.0
    confidence:  str    = "LOW"
    reasons:     List[str] = field(default_factory=list)
    metadata:    Dict = field(default_factory=dict)


class WyckoffAccumulationDetector:
    """
    Институциональный детектор Wyckoff Accumulation.
    Работает на 15m/30m/1h свечах.
    """

    def __init__(self, lookback: int = 50):
        self.lookback = lookback

    def _find_swing_lows(self, ohlcv: list, window: int = 5) -> List[Tuple[int, float]]:
        """Находит локальные минимумы (swing lows)"""
        lows = []
        for i in range(window, len(ohlcv) - window):
            c = ohlcv[i]
            if all(c.low <= ohlcv[j].low for j in range(i - window, i + window + 1) if j != i):
                lows.append((i, c.low))
        return lows

    def _find_swing_highs(self, ohlcv: list, window: int = 5) -> List[Tuple[int, float]]:
        """Находит локальные максимумы (swing highs)"""
        highs = []
        for i in range(window, len(ohlcv) - window):
            c = ohlcv[i]
            if all(c.high >= ohlcv[j].high for j in range(i - window, i + window + 1) if j != i):
                highs.append((i, c.high))
        return highs

    def _avg_volume(self, ohlcv: list, start: int, end: int) -> float:
        if start >= end or end > len(ohlcv):
            return 0.0
        vols = [getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0)
                for c in ohlcv[start:end]]
        return sum(vols) / len(vols) if vols else 0.0

    def _detect_selling_climax(self, ohlcv: list) -> Optional[Dict]:
        """
        Selling Climax (SC): Резкое падение с аномальным объёмом,
        после которого идёт Automatic Rally (AR).
        """
        if len(ohlcv) < 20:
            return None

        # Ищем свечу с максимальным объёмом в зоне перепроданности
        recent = ohlcv[-30:]
        vols = [getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0) for c in recent]
        avg_vol = sum(vols) / len(vols) if vols else 1

        sc_candidates = []
        for i, c in enumerate(recent):
            vol = getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0)
            price_drop = (c.open - c.close) / c.open * 100 if c.open > 0 else 0
            vol_ratio  = vol / avg_vol if avg_vol > 0 else 1

            # SC: большой медвежий бар + аномальный объём
            if price_drop > 2.0 and vol_ratio > 2.0:
                sc_candidates.append({
                    "idx": i, "price": c.low,
                    "drop": price_drop, "vol_ratio": vol_ratio
                })

        if not sc_candidates:
            return None

        # Берём самый значительный SC
        sc = max(sc_candidates, key=lambda x: x["vol_ratio"])

        # Проверяем Automatic Rally (AR) после SC — рост после SC
        if sc["idx"] < len(recent) - 3:
            ar_slice = recent[sc["idx"]:]
            if ar_slice:
                ar_high = max(c.high for c in ar_slice)
                ar_ratio = (ar_high - sc["price"]) / sc["price"] * 100
                if ar_ratio > 1.5:
                    return {
                        "event": "SC+AR",
                        "sc_price": sc["price"],
                        "ar_high": ar_high,
                        "ar_ratio": ar_ratio,
                        "vol_ratio": sc["vol_ratio"],
                        "score": min(40 + sc["vol_ratio"] * 5, 70),
                    }

        return {"event": "SC", "sc_price": sc["price"],
                "vol_ratio": sc["vol_ratio"], "score": 25.0}

    def _detect_spring(self, ohlcv: list, support_level: float) -> Optional[Dict]:
        """
        Spring: Ложный пробой уровня поддержки вниз с быстрым возвратом.
        Phase C — ключевой момент для входа в LONG.
        """
        if len(ohlcv) < 10 or not support_level:
            return None

        # Смотрим последние 15 свечей
        recent = ohlcv[-15:]
        for i, c in enumerate(recent):
            # Пробой поддержки
            if c.low < support_level * 0.995:  # Пробой на 0.5%+
                # Быстрый возврат выше поддержки
                if c.close > support_level:
                    poke_depth = (support_level - c.low) / support_level * 100
                    recovery   = (c.close - c.low) / (c.high - c.low) if c.high > c.low else 0

                    if recovery > 0.5:  # Закрылся в верхней половине бара
                        vol = getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0)
                        vols_before = [getattr(x, 'volume', 0) or getattr(x, 'quote_volume', 0)
                                       for x in recent[:max(i, 1)]]
                        avg_vol = sum(vols_before) / len(vols_before) if vols_before else 1
                        vol_ratio = vol / avg_vol if avg_vol > 0 else 1

                        score = 60.0
                        if poke_depth > 1.0: score += 10
                        if recovery > 0.7:   score += 10
                        if vol_ratio < 1.0:  score += 10  # Объём падает на Spring = хорошо

                        return {
                            "event": "Spring",
                            "spring_price": c.low,
                            "support": support_level,
                            "poke_depth_pct": round(poke_depth, 2),
                            "recovery": round(recovery, 2),
                            "vol_ratio": round(vol_ratio, 2),
                            "score": min(score, 90.0),
                        }
        return None

    def _detect_sos_lps(self, ohlcv: list, resistance_level: float) -> Optional[Dict]:
        """
        Sign of Strength (SOS): Пробой сопротивления с объёмом.
        Last Point of Support (LPS): Тест SOS с падением объёма.
        Phase D — подтверждённое накопление.
        """
        if len(ohlcv) < 10 or not resistance_level:
            return None

        recent = ohlcv[-20:]
        vols = [getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0) for c in recent]
        avg_vol = sum(vols) / len(vols) if vols else 1

        for i, c in enumerate(recent):
            if c.close > resistance_level:  # Пробой сопротивления
                vol = getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0)
                vol_ratio = vol / avg_vol if avg_vol > 0 else 1

                if vol_ratio > 1.3:  # Пробой на объёме = SOS
                    # Проверяем LPS — откат к пробитому уровню без обновления лоу
                    lps_found = False
                    for j in range(i + 1, len(recent)):
                        rc = recent[j]
                        if (resistance_level * 0.99 <= rc.low <= resistance_level * 1.03
                                and rc.close > resistance_level):
                            lps_vol = getattr(rc, 'volume', 0) or getattr(rc, 'quote_volume', 0)
                            if lps_vol < vol:  # Откат на меньшем объёме = LPS
                                lps_found = True
                                break

                    event = "SOS+LPS" if lps_found else "SOS"
                    score = 75.0 if lps_found else 55.0
                    return {
                        "event": event,
                        "resistance": resistance_level,
                        "breakout_price": c.close,
                        "vol_ratio": round(vol_ratio, 2),
                        "score": score,
                    }
        return None

    async def analyze(self, symbol: str, ohlcv: list, market_data: Any = None) -> Dict:
        """
        Полный Wyckoff анализ.
        Returns: dict с полями: score, phase, event, reasons, metadata
        """
        reasons: List[str] = []
        score   = 0.0
        phase   = "unknown"
        event   = ""
        meta: Dict = {}

        if not ohlcv or len(ohlcv) < 30:
            return {"score": 20.0, "phase": phase, "event": event,
                    "reasons": ["Wyckoff: недостаточно данных"], "metadata": {}}

        try:
            closes = [c.close for c in ohlcv]
            lows   = [c.low for c in ohlcv]

            # Уровень поддержки — минимум за последние N свечей
            lookback_slice = ohlcv[-self.lookback:]
            support    = min(c.low for c in lookback_slice)
            resistance = max(c.high for c in lookback_slice)
            price_range = resistance - support

            if price_range <= 0:
                return {"score": 15.0, "phase": "flat", "event": "",
                        "reasons": ["Нет диапазона для Wyckoff"], "metadata": {}}

            # Текущая позиция в диапазоне
            current_price = closes[-1]
            range_position = (current_price - support) / price_range  # 0 = дно, 1 = верх

            # ── Фаза A: SC + AR ──────────────────────────────────────────
            sc_result = self._detect_selling_climax(ohlcv)
            if sc_result:
                phase = "A"
                event = sc_result["event"]
                score = sc_result["score"]
                reasons.append(
                    f"Wyckoff {event}: Vol×{sc_result.get('vol_ratio', 0):.1f}"
                )
                meta.update(sc_result)

            # ── Фаза C: Spring ───────────────────────────────────────────
            spring = self._detect_spring(ohlcv, support)
            if spring and spring["score"] > score:
                phase = "C"
                event = "Spring"
                score = spring["score"]
                reasons.append(
                    f"Wyckoff Spring: пробой на -{spring['poke_depth_pct']:.1f}% "
                    f"→ возврат {spring['recovery']:.2f}"
                )
                meta.update(spring)

            # ── Фаза D: SOS + LPS ────────────────────────────────────────
            sos = self._detect_sos_lps(ohlcv, resistance * 0.97)
            if sos and sos["score"] > score:
                phase = "D"
                event = sos["event"]
                score = sos["score"]
                reasons.append(f"Wyckoff {event}: пробой Vol×{sos['vol_ratio']:.1f}")
                meta.update(sos)

            # ── Позиция в диапазоне ──────────────────────────────────────
            if range_position < 0.20:
                score += 15
                reasons.append(f"Цена в нижних 20% диапазона — зона накопления")
            elif range_position < 0.35:
                score += 8
                reasons.append(f"Цена в нижней трети диапазона")
            elif range_position > 0.80:
                score -= 10  # Уже почти у вершины — поздно для LONG

            # ── Объёмный тренд (сжатие = накопление) ────────────────────
            vol_recent = self._avg_volume(ohlcv, -10, len(ohlcv))
            vol_older  = self._avg_volume(ohlcv, -30, -10)
            if vol_older > 0:
                vol_ratio = vol_recent / vol_older
                if vol_ratio < 0.7:
                    score += 10
                    reasons.append("OBV/Volume сжимается — накопление")
                elif vol_ratio < 0.85:
                    score += 5
                    reasons.append("Объём умеренно снижается в диапазоне")

            score = min(max(score, 0.0), 100.0)

            return {
                "score":    round(score, 1),
                "phase":    phase,
                "event":    event,
                "reasons":  reasons,
                "metadata": {
                    "support":       round(support, 8),
                    "resistance":    round(resistance, 8),
                    "range_pos":     round(range_position, 3),
                    "current_price": round(current_price, 8),
                    **meta,
                }
            }

        except Exception as e:
            logger.warning(f"Wyckoff error {symbol}: {e}")
            return {"score": 15.0, "phase": "error", "event": "",
                    "reasons": [], "metadata": {}}
