"""
Dual Scoring System: ShortScorer + LongScorer  v2.1

ИЗМЕНЕНИЯ v2.1:
  ✅ Новые паттерны добавлены в calculate_pattern_component:
     BREAKOUT_LONG/SHORT, MOMENTUM_LONG/SHORT,
     LIQUIDITY_SWEEP_LONG/SHORT, CONSOLIDATION_BREAK_LONG,
     DISTRIBUTION_BREAK, WYCKOFF_SPRING, WYCKOFF_UPTHRUST
  ✅ Веса новых паттернов: Liquidity Sweep (28), Wyckoff (26),
     Breakout (24), Momentum (22), Consolidation (18)
  ✅ volume_spike_ratio в скоре: бонус до +8 при spike > 2x
  ✅ atr_14_pct в скоре: штраф за чрезмерную волатильность
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from enum import Enum
from datetime import datetime

# 🆕 Liquidation zones support
try:
    from .liquidation_detector import LiquidationAnalysis
except ImportError:
    LiquidationAnalysis = None


class Direction(Enum):
    SHORT = "short"
    LONG  = "long"


class Confidence(Enum):
    VERY_LOW  = "very_low"
    LOW       = "low"
    MEDIUM    = "medium"
    HIGH      = "high"
    VERY_HIGH = "very_high"


@dataclass
class ScoreComponent:
    name:        str
    score:       int
    max_score:   int
    description: str
    raw_value:   Optional[float] = None


@dataclass
class ScoreResult:
    total_score:  int
    max_possible: int
    direction:    Direction
    is_valid:     bool
    confidence:   Confidence
    grade:        str
    components:   List[ScoreComponent]
    reasons:      List[str]
    timestamp:    datetime = field(default_factory=datetime.utcnow)

    @property
    def percentage(self) -> float:
        if self.max_possible == 0:
            return 0.0
        return round(self.total_score / self.max_possible * 100, 1)


@dataclass
class Pattern:
    name:              str
    direction:         Direction
    strength:          int
    candles_ago:       int
    freshness:         int
    volume_multiplier: float
    delta_at_trigger:  float
    entry_price:       float
    stop_loss:         float
    confidence:        str
    description:       str


# ============================================================================
# PATTERN STRENGTH MAP — все паттерны и их базовая сила
# ============================================================================

LONG_PATTERN_STRENGTHS = {
    # SMC/ICT паттерны (высокий win rate)
    "LIQUIDITY_SWEEP_LONG":     28,
    "WYCKOFF_SPRING":           26,
    "BREAKOUT_LONG":            24,
    "MOMENTUM_LONG":            22,
    "CONSOLIDATION_BREAK_LONG": 18,
    # Классические
    "MEGA_LONG":                20,
    "TRAP_SHORT":               18,
    "REJECTION_LONG":           15,
    "ACCUMULATION":             22,
}

SHORT_PATTERN_STRENGTHS = {
    # 🆕 Pump & Dump (как у DUMP Signals) — высокий потенциал
    "PUMP_DUMP_SHORT":          26,
    # SMC/ICT паттерны
    "LIQUIDITY_SWEEP_SHORT":    28,
    "WYCKOFF_UPTHRUST":         26,
    "BREAKOUT_SHORT":           24,
    "MOMENTUM_SHORT":           22,
    "DISTRIBUTION_BREAK":       18,
    # Классические
    "MEGA_SHORT":               20,
    "TRAP_LONG":                18,
    "REJECTION_SHORT":          15,
    "DISTRIBUTION":             22,
}


class BaseScorer:
    COMPONENT_WEIGHTS = {
        "rsi": 20, "funding": 15, "long_short_ratio": 15,
        "open_interest": 15, "delta": 20, "pattern": 30
    }

    def __init__(self, min_score: int = 65, direction: Direction = Direction.SHORT):
        self.min_score = min_score
        self.direction = direction

    def calculate_grade(self, score: int) -> str:
        if score >= 90: return "S"
        if score >= 80: return "A"
        if score >= 70: return "B"
        if score >= 60: return "C"
        if score >= 50: return "D"
        return "F"

    def determine_confidence(self, score: int) -> Confidence:
        if score >= 85: return Confidence.VERY_HIGH
        if score >= 75: return Confidence.HIGH
        if score >= 65: return Confidence.MEDIUM
        if score >= 50: return Confidence.LOW
        return Confidence.VERY_LOW

    def _volume_spike_bonus(self, volume_spike_ratio: float) -> Tuple[int, str]:
        """
        Бонус за volume spike.
        Применяется к итоговому скору ДОПОЛНИТЕЛЬНО к паттернам.
        """
        if volume_spike_ratio >= 5.0:
            return 8, f"🔥 Экстремальный volume spike {volume_spike_ratio:.1f}x +8"
        if volume_spike_ratio >= 3.0:
            return 6, f"Volume spike {volume_spike_ratio:.1f}x +6"
        if volume_spike_ratio >= 2.0:
            return 4, f"Volume spike {volume_spike_ratio:.1f}x +4"
        if volume_spike_ratio >= 1.5:
            return 2, f"Volume spike {volume_spike_ratio:.1f}x +2"
        return 0, ""

    def _atr_penalty(self, atr_pct: float) -> Tuple[int, str]:
        """
        Штраф за чрезмерную волатильность.
        Очень волатильные монеты = непредсказуемые движения.
        """
        if atr_pct > 3.0:
            return -5, f"Экстремальная волатильность ATR={atr_pct:.1f}% -5"
        if atr_pct > 2.0:
            return -3, f"Высокая волатильность ATR={atr_pct:.1f}% -3"
        return 0, ""

    def calculate_liquidation_component(
        self, 
        liq_analysis: Optional[LiquidationAnalysis]
    ) -> ScoreComponent:
        """
        🆕 Бонус/штраф от магнитов ликвидации.
        
        LONG: +15 если магнит выше на 2-8%, -10 если магнит ниже <1.5%
        SHORT: +15 если магнит ниже на 2-8%, -10 если магнит выше <1.5%
        """
        if not liq_analysis or not liq_analysis.has_targets:
            return ScoreComponent("Liquidation", 0, 15, "Нет данных")
        
        direction_str = "long" if self.direction == Direction.LONG else "short"
        bonus = liq_analysis.get_score_bonus(direction_str)
        
        reasons = []
        if bonus > 0:
            if direction_str == "long" and liq_analysis.nearest_above:
                dist = abs(liq_analysis.nearest_above.distance_pct)
                reasons.append(f"🧲 Магнит +{dist:.1f}% — цель для TP")
            elif direction_str == "short" and liq_analysis.nearest_below:
                dist = abs(liq_analysis.nearest_below.distance_pct)
                reasons.append(f"🧲 Магнит -{dist:.1f}% — цель для TP")
        elif bonus < 0:
            if direction_str == "long" and liq_analysis.nearest_below:
                dist = abs(liq_analysis.nearest_below.distance_pct)
                reasons.append(f"⚠️ Магнит -{dist:.1f}% близко — риск стопа")
            elif direction_str == "short" and liq_analysis.nearest_above:
                dist = abs(liq_analysis.nearest_above.distance_pct)
                reasons.append(f"⚠️ Магнит +{dist:.1f}% близко — риск стопа")
        else:
            reasons.append("🎯 Магниты в нейтральной зоне")
        
        return ScoreComponent(
            "Liquidation", 
            bonus, 
            15, 
            " | ".join(reasons),
            liq_analysis.long_liq_dominance if liq_analysis else None
        )


class ShortScorer(BaseScorer):

    def __init__(self, min_score: int = 65):
        super().__init__(min_score, Direction.SHORT)

    def calculate_rsi_component(self, rsi_1h: float) -> ScoreComponent:
        if rsi_1h >= 80:   score, desc = 20, f"RSI {rsi_1h:.1f} — Экстремальная перекупленность"
        elif rsi_1h >= 75: score, desc = 18, f"RSI {rsi_1h:.1f} — Сильная перекупленность"
        elif rsi_1h >= 70: score, desc = 15, f"RSI {rsi_1h:.1f} — Перекупленность"
        elif rsi_1h >= 65: score, desc = 12, f"RSI {rsi_1h:.1f} — Начало перекупленности"
        elif rsi_1h >= 60: score, desc = 8,  f"RSI {rsi_1h:.1f} — Близко к перекупленности"
        elif rsi_1h >= 55: score, desc = 4,  f"RSI {rsi_1h:.1f} — Нейтрально-bullish"
        elif rsi_1h >= 50: score, desc = 3,  f"RSI {rsi_1h:.1f} — Нейтраль 50-55"  # ✅ FIX
        elif rsi_1h < 30:  score, desc = 0,  f"RSI {rsi_1h:.1f} — Перепроданность (плохо для шорта)"
        else:              score, desc = 2,  f"RSI {rsi_1h:.1f} — Нейтральная зона"
        return ScoreComponent("RSI", score, 20, desc, rsi_1h)

    def calculate_funding_component(self, current_funding: float,
                                    accumulated_4d: float) -> ScoreComponent:
        score, reasons = 0, []
        if current_funding >= 0.1:    score += 8;  reasons.append(f"Высокий фандинг {current_funding:.3f}%")
        elif current_funding >= 0.05: score += 5;  reasons.append(f"Повышенный фандинг {current_funding:.3f}%")
        elif current_funding > 0:     score += 3;  reasons.append(f"Позитивный фандинг {current_funding:.3f}%")
        elif current_funding <= -0.05: score += 0; reasons.append(f"Отрицательный фандинг (плохо)")
        else:                         score += 1;  reasons.append(f"Нейтральный фандинг {current_funding:.3f}%")
        if accumulated_4d >= 0.5:     score += 7;  reasons.append(f"Высокий накопл. фандинг {accumulated_4d:.2f}%")
        elif accumulated_4d >= 0.3:   score += 5;  reasons.append(f"Накопл. фандинг {accumulated_4d:.2f}%")
        elif accumulated_4d >= 0.1:   score += 3
        elif accumulated_4d < 0:      score += 0;  reasons.append(f"Отрицательный накопл. {accumulated_4d:.2f}%")
        else:                         score += 1
        return ScoreComponent("Funding", min(score, 15), 15, " | ".join(reasons) or "Нейтральный фандинг", current_funding)

    def calculate_ratio_component(self, long_ratio: float) -> ScoreComponent:
        if long_ratio >= 70:   score, desc = 15, f"{long_ratio:.0f}% лонгов — толпа в лонгах"
        elif long_ratio >= 65: score, desc = 12, f"{long_ratio:.0f}% лонгов — много лонгистов"
        elif long_ratio >= 60: score, desc = 10, f"{long_ratio:.0f}% лонгов — лонгисты доминируют"
        elif long_ratio >= 55: score, desc = 7,  f"{long_ratio:.0f}% лонгов — лонг перевес"
        elif long_ratio >= 50: score, desc = 4,  f"{long_ratio:.0f}% лонгов — лёгкий перевес"
        elif long_ratio >= 45: score, desc = 2,  f"{long_ratio:.0f}% лонгов — баланс"
        elif long_ratio >= 35: score, desc = 1,  f"{long_ratio:.0f}% лонгов — шортисты доминируют"
        else:                  score, desc = 0,  f"{long_ratio:.0f}% лонгов — толпа в шортах"
        return ScoreComponent("L/S Ratio", score, 15, desc, long_ratio)

    def calculate_oi_component(self, oi_change_4d: float, price_change_4d: float) -> ScoreComponent:
        score, reasons = 0, []
        if oi_change_4d >= 25:    score += 8;  reasons.append(f"OI +{oi_change_4d:.1f}% — перегрев")
        elif oi_change_4d >= 15:  score += 6;  reasons.append(f"OI +{oi_change_4d:.1f}% — рост")
        elif oi_change_4d >= 5:   score += 3;  reasons.append(f"OI +{oi_change_4d:.1f}%")
        elif oi_change_4d < -10:  score += 5;  reasons.append(f"OI {oi_change_4d:.1f}% — массовое закрытие")
        if price_change_4d >= 15: score += 7;  reasons.append(f"Цена +{price_change_4d:.1f}% за 4д — перегрев")
        elif price_change_4d >= 8: score += 5; reasons.append(f"Цена +{price_change_4d:.1f}% за 4д")
        elif price_change_4d >= 3: score += 2
        elif price_change_4d < -10: score += 3; reasons.append(f"Обвал {price_change_4d:.1f}%")
        return ScoreComponent("OI", min(score, 15), 15, " | ".join(reasons) or "Нейтральный OI", oi_change_4d)

    def calculate_delta_component(self, hourly_deltas: List[float],
                                   price_trend: str) -> ScoreComponent:
        score, reasons = 0, []
        if not hourly_deltas:
            return ScoreComponent("Delta", 0, 20, "Нет данных дельты", 0)
        neg_hours = sum(1 for d in hourly_deltas if d < 0)
        if neg_hours >= 5: score += 8;  reasons.append(f"{neg_hours}ч отрицательной дельты")
        elif neg_hours >= 4: score += 5
        elif neg_hours >= 3: score += 2
        if price_trend == "rising" and neg_hours >= 3:
            score += 12; reasons.append("Медвежья дивергенция (цена растёт, дельта падает)")
        elif price_trend == "rising" and neg_hours >= 2:
            score += 8; reasons.append("Слабая медвежья дивергенция")
        elif price_trend == "sideways" and neg_hours >= 4:
            score += 6; reasons.append("Распределение в боковике")
        return ScoreComponent("Delta", min(score, 20), 20, " | ".join(reasons) or "Нейтральная дельта",
                              sum(hourly_deltas))

    def calculate_pattern_component(self, patterns: List[Pattern]) -> Tuple[ScoreComponent, List[str]]:
        if not patterns:
            return ScoreComponent("Patterns", 0, 30, "Нет паттернов"), []
        best = max(patterns, key=lambda p: SHORT_PATTERN_STRENGTHS.get(p.name, p.strength))
        base = SHORT_PATTERN_STRENGTHS.get(best.name, best.strength)
        bonus = (3 if len(patterns) >= 2 else 0) + (5 if len(patterns) >= 3 else 0)
        fresh = 2 if best.candles_ago == 0 else (1 if best.candles_ago == 1 else 0)
        total = min(base + bonus + fresh, 30)
        names = [p.name for p in patterns]
        desc = f"{best.name} (base={base})"
        if len(patterns) > 1: desc += f" +{len(patterns)-1} паттернов"
        return ScoreComponent("Patterns", total, 30, desc), names

    def calculate_score(self, rsi_1h, funding_current, funding_accumulated,
                        long_ratio, oi_change_4d, price_change_4d,
                        hourly_deltas, price_trend, patterns,
                        volume_spike_ratio: float = 1.0,
                        atr_14_pct: float = 0.5) -> ScoreResult:
        components = []
        components.append(self.calculate_rsi_component(rsi_1h))
        components.append(self.calculate_funding_component(funding_current, funding_accumulated))
        components.append(self.calculate_ratio_component(long_ratio))
        components.append(self.calculate_oi_component(oi_change_4d, price_change_4d))
        components.append(self.calculate_delta_component(hourly_deltas, price_trend))
        pat_comp, pat_names = self.calculate_pattern_component(patterns)
        components.append(pat_comp)
        total = sum(c.score for c in components)
        max_p = sum(c.max_score for c in components)
        # Confluence bonus
        strong = sum(1 for c in components if c.score >= c.max_score * 0.6)
        if strong >= 4: total += 5
        elif strong >= 3: total += 3
        # Volume spike bonus
        vs_bonus, vs_reason = self._volume_spike_bonus(volume_spike_ratio)
        total += vs_bonus
        # ATR penalty
        atr_pen, atr_reason = self._atr_penalty(atr_14_pct)
        total += atr_pen
        total = min(max(total, 0), 100)
        reasons = []
        if components[0].score >= 15: reasons.append(f"RSI перекуплен ({rsi_1h:.1f})")
        if components[1].score >= 8:  reasons.append("Лонги платят фандинг")
        if components[2].score >= 10: reasons.append(f"Толпа в лонгах ({long_ratio:.0f}%)")
        if components[3].score >= 10: reasons.append("Лонги перегружены (OI растёт)")
        if components[4].score >= 10: reasons.append("Медвежья дивергенция")
        if components[5].score >= 20: reasons.append(f"Сильный паттерн: {pat_names[0] if pat_names else 'N/A'}")
        if vs_reason: reasons.append(vs_reason)
        if atr_reason: reasons.append(atr_reason)
        return ScoreResult(
            total_score=total, max_possible=max_p, direction=Direction.SHORT,
            is_valid=total >= self.min_score,
            confidence=self.determine_confidence(total),
            grade=self.calculate_grade(total),
            components=components, reasons=reasons,
        )


class LongScorer(BaseScorer):

    def __init__(self, min_score: int = 65):
        super().__init__(min_score, Direction.LONG)

    def calculate_rsi_component(self, rsi_1h: float) -> ScoreComponent:
        if rsi_1h <= 20:   score, desc = 20, f"RSI {rsi_1h:.1f} — Экстремальная перепроданность"
        elif rsi_1h <= 25: score, desc = 18, f"RSI {rsi_1h:.1f} — Сильная перепроданность"
        elif rsi_1h <= 30: score, desc = 15, f"RSI {rsi_1h:.1f} — Перепроданность"
        elif rsi_1h <= 35: score, desc = 12, f"RSI {rsi_1h:.1f} — Начало перепроданности"
        elif rsi_1h <= 40: score, desc = 10, f"RSI {rsi_1h:.1f} — Близко к перепроданности"
        elif rsi_1h <= 45: score, desc = 7,  f"RSI {rsi_1h:.1f} — Нейтрально-bearish"
        elif rsi_1h <= 55: score, desc = 5,  f"RSI {rsi_1h:.1f} — Нейтральная зона"
        elif rsi_1h > 70:  score, desc = 0,  f"RSI {rsi_1h:.1f} — Перекупленность (плохо для лонга)"
        else:              score, desc = 2,  f"RSI {rsi_1h:.1f} — Умеренно бычий"
        return ScoreComponent("RSI", score, 20, desc, rsi_1h)

    def calculate_funding_component(self, current_funding: float,
                                    accumulated_4d: float) -> ScoreComponent:
        score, reasons = 0, []
        if current_funding <= -0.10:  score += 8;  reasons.append(f"Экстремальный -фандинг {current_funding:.3f}%")
        elif current_funding <= -0.05: score += 5; reasons.append(f"Высокий -фандинг {current_funding:.3f}%")
        elif current_funding < 0:     score += 3;  reasons.append(f"Отрицательный фандинг {current_funding:.3f}%")
        elif current_funding >= 0.10: score += 0;  reasons.append(f"Высокий +фандинг {current_funding:.3f}% (плохо)")
        else:                         score += 1;  reasons.append(f"Нейтральный фандинг {current_funding:.3f}%")
        if accumulated_4d <= -0.5:    score += 7;  reasons.append(f"Глубокий накопл. -фандинг {accumulated_4d:.2f}%")
        elif accumulated_4d <= -0.3:  score += 5;  reasons.append(f"Накопл. -фандинг {accumulated_4d:.2f}%")
        elif accumulated_4d <= -0.1:  score += 3
        elif accumulated_4d >= 0.5:   score += 0;  reasons.append(f"Высокий +накопл. фандинг (плохо)")
        else:                         score += 1
        return ScoreComponent("Funding", min(score, 15), 15, " | ".join(reasons) or "Нейтральный фандинг", current_funding)

    def calculate_ratio_component(self, long_ratio: float) -> ScoreComponent:
        short_ratio = 100 - long_ratio
        if short_ratio >= 65:   score, desc = 15, f"{short_ratio:.0f}% шортов — толпа в шортах"
        elif short_ratio >= 60: score, desc = 12, f"{short_ratio:.0f}% шортов — много шортистов"
        elif short_ratio >= 55: score, desc = 10, f"{short_ratio:.0f}% шортов — шортисты доминируют"
        elif short_ratio >= 50: score, desc = 7,  f"{short_ratio:.0f}% шортов — лёгкий шорт перевес"
        elif short_ratio >= 45: score, desc = 4,  f"Баланс ({long_ratio:.0f}% лонг)"
        elif short_ratio >= 40: score, desc = 2,  f"Лонгисты доминируют"
        elif short_ratio >= 30: score, desc = 1,  f"Лонгисты сильно доминируют"
        else:                   score, desc = 0,  f"Толпа в лонгах — плохо для лонга"
        return ScoreComponent("L/S Ratio", score, 15, desc, long_ratio)

    def calculate_oi_component(self, oi_change_4d: float, price_change_4d: float) -> ScoreComponent:
        score, reasons = 0, []
        if oi_change_4d >= 15:   score += 5;  reasons.append(f"OI +{oi_change_4d:.1f}% — накопление")
        elif oi_change_4d >= 5:  score += 3;  reasons.append(f"OI +{oi_change_4d:.1f}%")
        elif oi_change_4d < -15: score += 8;  reasons.append(f"OI {oi_change_4d:.1f}% — массовое закрытие шортов")
        elif oi_change_4d < -5:  score += 5;  reasons.append(f"OI {oi_change_4d:.1f}% — шорты выходят")
        if price_change_4d <= -15: score += 7; reasons.append(f"Цена {price_change_4d:.1f}% за 4д — перепроданность")
        elif price_change_4d <= -8: score += 5; reasons.append(f"Цена {price_change_4d:.1f}% за 4д")
        elif price_change_4d <= -3: score += 2
        elif price_change_4d >= 10: score += 0; reasons.append(f"Цена +{price_change_4d:.1f}% (перегрев)")
        return ScoreComponent("OI", min(score, 15), 15, " | ".join(reasons) or "Нейтральный OI", oi_change_4d)

    def calculate_delta_component(self, hourly_deltas: List[float],
                                   price_trend: str) -> ScoreComponent:
        score, reasons = 0, []
        if not hourly_deltas:
            return ScoreComponent("Delta", 0, 20, "Нет данных дельты", 0)
        pos_hours = sum(1 for d in hourly_deltas if d > 0)
        if pos_hours >= 5: score += 8;  reasons.append(f"{pos_hours}ч положительной дельты")
        elif pos_hours >= 4: score += 5
        elif pos_hours >= 3: score += 4
        elif pos_hours >= 2: score += 2
        if price_trend == "falling" and pos_hours >= 3:
            score += 12; reasons.append("Бычья дивергенция (цена падает, дельта растёт)")
        elif price_trend == "falling" and pos_hours >= 2:
            score += 8; reasons.append("Слабая бычья дивергенция")
        elif price_trend == "sideways" and pos_hours >= 4:
            score += 6; reasons.append("Накопление в боковике")
        return ScoreComponent("Delta", min(score, 20), 20, " | ".join(reasons) or "Нейтральная дельта",
                              sum(hourly_deltas))

    def calculate_pattern_component(self, patterns: List[Pattern]) -> Tuple[ScoreComponent, List[str]]:
        if not patterns:
            return ScoreComponent("Patterns", 0, 30, "Нет паттернов"), []
        best = max(patterns, key=lambda p: LONG_PATTERN_STRENGTHS.get(p.name, p.strength))
        base = LONG_PATTERN_STRENGTHS.get(best.name, best.strength)
        bonus = (3 if len(patterns) >= 2 else 0) + (5 if len(patterns) >= 3 else 0)
        fresh = 2 if best.candles_ago == 0 else (1 if best.candles_ago == 1 else 0)
        total = min(base + bonus + fresh, 30)
        names = [p.name for p in patterns]
        desc = f"{best.name} (base={base})"
        if len(patterns) > 1: desc += f" +{len(patterns)-1} паттернов"
        return ScoreComponent("Patterns", total, 30, desc), names

    def calculate_score(self, rsi_1h, funding_current, funding_accumulated,
                        long_ratio, oi_change_4d, price_change_4d,
                        hourly_deltas, price_trend, patterns,
                        volume_spike_ratio: float = 1.0,
                        atr_14_pct: float = 0.5) -> ScoreResult:
        components = []
        components.append(self.calculate_rsi_component(rsi_1h))
        components.append(self.calculate_funding_component(funding_current, funding_accumulated))
        components.append(self.calculate_ratio_component(long_ratio))
        components.append(self.calculate_oi_component(oi_change_4d, price_change_4d))
        components.append(self.calculate_delta_component(hourly_deltas, price_trend))
        pat_comp, pat_names = self.calculate_pattern_component(patterns)
        components.append(pat_comp)
        total = sum(c.score for c in components)
        max_p = sum(c.max_score for c in components)
        strong = sum(1 for c in components if c.score >= c.max_score * 0.6)
        if strong >= 4: total += 5
        elif strong >= 3: total += 3
        vs_bonus, vs_reason = self._volume_spike_bonus(volume_spike_ratio)
        total += vs_bonus
        atr_pen, atr_reason = self._atr_penalty(atr_14_pct)
        total += atr_pen
        total = min(max(total, 0), 100)
        reasons = []
        if components[0].score >= 15: reasons.append(f"RSI перепродан ({rsi_1h:.1f})")
        if components[1].score >= 8:  reasons.append("Шорты платят фандинг")
        if components[2].score >= 10: reasons.append(f"Толпа в шортах ({100-long_ratio:.0f}%)")
        if components[3].score >= 10: reasons.append("Шорты закрываются (OI падает)")
        if components[4].score >= 10: reasons.append("Бычья дивергенция")
        if components[5].score >= 20: reasons.append(f"Сильный паттерн: {pat_names[0] if pat_names else 'N/A'}")
        if vs_reason: reasons.append(vs_reason)
        if atr_reason: reasons.append(atr_reason)
        return ScoreResult(
            total_score=total, max_possible=max_p, direction=Direction.LONG,
            is_valid=total >= self.min_score,
            confidence=self.determine_confidence(total),
            grade=self.calculate_grade(total),
            components=components, reasons=reasons,
        )


# ============================================================================
# SINGLETON
# ============================================================================

_short_scorer = None
_long_scorer  = None

def get_short_scorer(min_score: int = 65) -> ShortScorer:
    global _short_scorer
    if _short_scorer is None:
        _short_scorer = ShortScorer(min_score)
    return _short_scorer

def get_long_scorer(min_score: int = 65) -> LongScorer:
    global _long_scorer
    if _long_scorer is None:
        _long_scorer = LongScorer(min_score)
    return _long_scorer
