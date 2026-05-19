"""
Dual Scoring System: ShortScorer + LongScorer  v2.2

ИЗМЕНЕНИЯ v2.2:
  ✅ P4: _funding_extreme_bonus — экстремальные пороги фандинга (+10)
  ✅ P1: orderbook_score param в calculate_score() для обоих скоров

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

import os
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
    funding_info: str = ""   # показывается в TG, не влияет на скор
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
    # P2: Flag / Pennant (78% точность — continuation паттерны)
    "FLAG_LONG":                15,
    "PENNANT_LONG":             12,
    # CASCADE синтетический
    "CASCADE_LONG":             18,
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
    # P2: Flag / Pennant (78% точность — continuation паттерны)
    "FLAG_SHORT":               15,
    "PENNANT_SHORT":            12,
    # CASCADE синтетический
    "CASCADE_SHORT":            18,
}


class BaseScorer:
    COMPONENT_WEIGHTS = {
        "rsi": 12,          # ⬇️ Уменьшили — запоздалый индикатор
        "funding": 15,
        "long_short_ratio": 15,
        "open_interest": 15,
        "delta": 20,
        "pattern": 35,      # ⬆️ Увеличили — структура важнее
        "structure": 18,    # 🆕 Консолидация/поддержка/сопротивление
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

    def _funding_extreme_bonus(self, funding: float, direction: str) -> Tuple[int, str]:
        """
        P4: Бонус за экстремальный фандинг.
        SHORT: очень высокий фандинг → лонги перегреты → +10
        LONG:  очень отрицательный фандинг → шорты перегреты → +10
        Пороги читаются из ENV с безопасными дефолтами.
        """
        try:
            extreme_short = float(os.getenv("FUNDING_EXTREME_SHORT", "0.05"))
            extreme_long  = float(os.getenv("FUNDING_EXTREME_LONG",  "-0.05"))
            if direction == "short":
                if funding >= extreme_short:
                    return 10, f"🔥 Экстремальный фандинг {funding:.4f}% — long liquidation"
            else:
                if funding <= extreme_long:
                    return 10, f"🔥 Экстремальный -фандинг {funding:.4f}% — short squeeze"
        except Exception:
            pass
        return 0, ""

    def calculate_top_trader_component(
        self,
        top_trader_ratio: Optional[float],
        direction: str,
    ) -> ScoreComponent:
        """
        Smart money позиционирование топ-трейдеров (Binance /fapi/v1/topLongShortPositionRatio).
        top_trader_ratio = long_vol / short_vol.
        Значение > 1.0 = топ-трейдеры в лонге, < 1.0 = в шорте.

        Отличие от retail L/S:
          - Retail L/S → contrarian (толпа в лонге = шорти)
          - Top Trader L/S → directional (smart money в шорте = шорти вместе)

        Max score: 10 (дополнительный компонент, не заменяет retail ratio)
        """
        if top_trader_ratio is None:
            return ScoreComponent("TopTrader", 0, 10, "Нет данных топ-трейдеров", 0)

        if direction == "short":
            # Для SHORT: топ-трейдеры в шорте = хороший знак
            if top_trader_ratio <= 0.6:   score, desc = 10, f"🐋 Топ-трейдеры {top_trader_ratio:.2f} — активно шортят"
            elif top_trader_ratio <= 0.8: score, desc = 7,  f"Топ-трейдеры {top_trader_ratio:.2f} — шорт перевес"
            elif top_trader_ratio <= 1.0: score, desc = 4,  f"Топ-трейдеры {top_trader_ratio:.2f} — нейтраль"
            elif top_trader_ratio <= 1.3: score, desc = 2,  f"Топ-трейдеры {top_trader_ratio:.2f} — лёгкий лонг"
            elif top_trader_ratio <= 2.0: score, desc = 4,  f"🔥 Топ-трейдеры {top_trader_ratio:.2f} — перегружены лонгами (контр-сигнал SHORT)"
            else:                          score, desc = 6,  f"🚨 Топ-трейдеры {top_trader_ratio:.2f} — экстремальный лонг-перегрев (сильный SHORT сигнал)"
        else:
            # Для LONG: топ-трейдеры в лонге = хороший знак
            if top_trader_ratio >= 1.7:   score, desc = 10, f"🐋 Топ-трейдеры {top_trader_ratio:.2f} — активно лонгуют"
            elif top_trader_ratio >= 1.3: score, desc = 7,  f"Топ-трейдеры {top_trader_ratio:.2f} — лонг перевес"
            elif top_trader_ratio >= 1.0: score, desc = 4,  f"Топ-трейдеры {top_trader_ratio:.2f} — нейтраль"
            elif top_trader_ratio >= 0.8: score, desc = 2,  f"Топ-трейдеры {top_trader_ratio:.2f} — лёгкий шорт"
            else:                          score, desc = 0,  f"Топ-трейдеры {top_trader_ratio:.2f} — в шорте (осторожно)"

        return ScoreComponent("TopTrader", score, 10, desc, top_trader_ratio)

    def calculate_htf_zone_component(
        self, htf_structure: str, zone: str, direction: str
    ) -> ScoreComponent:
        """
        Market Structure alignment: HTF trend + price zone.
        Заменяет Funding в BASE_SCORER (max 13).

        SHORT: BEARISH+PREMIUM = +13 (идеал), BULLISH+DISCOUNT = -12 (опасно)
        LONG:  BULLISH+DISCOUNT = +13 (идеал), BEARISH+PREMIUM = -10 (опасно)
        """
        htf = (htf_structure or "").upper()
        z   = (zone or "").upper()
        score = 0
        parts = []

        if direction == "short":
            if "BEARISH" in htf:    score += 8; parts.append(f"HTF BEARISH ✅ по тренду")
            elif "RANGING" in htf:  score += 3; parts.append(f"HTF RANGING нейтраль")
            elif "BULLISH" in htf:  score -= 4; parts.append(f"HTF BULLISH ⚠️ против тренда -4")
            else:                   parts.append(f"HTF={htf_structure or 'неизвестно'}")
            if "PREMIUM" in z:      score += 5; parts.append(f"Zone PREMIUM ✅ шорт из хаёв")
            elif "DISCOUNT" in z:   score -= 2; parts.append(f"Zone DISCOUNT ⚠️ шорт из лоёв -2")
            else:                   score += 1; parts.append(f"Zone {z or 'neutral'}")
        else:  # long
            if "BULLISH" in htf:    score += 8; parts.append(f"HTF BULLISH ✅ по тренду")
            elif "RANGING" in htf:  score += 3; parts.append(f"HTF RANGING нейтраль")
            elif "BEARISH" in htf:  score -= 6; parts.append(f"HTF BEARISH ⚠️ против тренда -6")
            else:                   parts.append(f"HTF={htf_structure or 'неизвестно'}")
            if "DISCOUNT" in z:     score += 5; parts.append(f"Zone DISCOUNT ✅ лонг из лоёв")
            elif "PREMIUM" in z:    score -= 4; parts.append(f"Zone PREMIUM ⚠️ лонг из хаёв -4")
            else:                   score += 1; parts.append(f"Zone {z or 'neutral'}")

        score = max(score, -10)  # не более -10 штрафа
        return ScoreComponent("HTF+Zone", score, 13, " | ".join(parts) or "Нет данных MS", None)

    def _funding_info_str(self, current_funding: float, accumulated_4d: float) -> str:
        """Строка для TG-уведомления — не влияет на скор."""
        s = f"{'+'if current_funding>=0 else ''}{current_funding:.4f}%"
        a = f"{'+'if accumulated_4d>=0 else ''}{accumulated_4d:.3f}%"
        return f"Funding: {s} (acc {a})"

    def _taker_bonus(self, taker_ratio: Optional[float], direction: str) -> Tuple[int, str]:
        """
        Бонус за Taker Buy/Sell ratio (агрессивные ордера).
        taker_ratio = buy_volume / (buy_volume + sell_volume), 0.0-1.0.
        > 0.5 = покупатели агрессивнее, < 0.5 = продавцы агрессивнее.

        Добавляется к итоговому скору (не компонент, а бонус — аналог volume spike).
        Max ±5.
        """
        if taker_ratio is None:
            return 0, ""
        if direction == "short":
            # SHORT: продавцы агрессивнее → подтверждение давления
            if taker_ratio <= 0.35:   return 5, f"🔴 Taker sell {(1-taker_ratio)*100:.0f}% — продавцы доминируют +5"
            elif taker_ratio <= 0.42: return 3, f"Taker sell перевес {(1-taker_ratio)*100:.0f}% +3"
            elif taker_ratio <= 0.48: return 1, f"Taker нейтраль +1"
            elif taker_ratio >= 0.60: return -2, f"Taker buy {taker_ratio*100:.0f}% — покупают (осторожно) -2"
            return 0, ""
        else:
            # LONG: покупатели агрессивнее → подтверждение спроса
            if taker_ratio >= 0.65:   return 5, f"🟢 Taker buy {taker_ratio*100:.0f}% — покупатели доминируют +5"
            elif taker_ratio >= 0.58: return 3, f"Taker buy перевес {taker_ratio*100:.0f}% +3"
            elif taker_ratio >= 0.52: return 1, f"Taker нейтраль +1"
            elif taker_ratio <= 0.40: return -2, f"Taker sell {(1-taker_ratio)*100:.0f}% — продают (осторожно) -2"
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

    def __init__(self, min_score: int = 58):  # ✅ FIX: 54→58 убрана dead zone 50-57
        super().__init__(min_score, Direction.SHORT)

    def calculate_rsi_component(self, rsi_1h: float, rsi_15m: float = None, rsi_30m: float = None, rsi_4h: float = None) -> ScoreComponent:
        # RSI — ВСПОМОГАТЕЛЬНЫЙ фактор для mean-reversion SHORT
        # Главное: Z-score + Volume. RSI лишь подтверждает или слегка штрафует.
        # Максимум 12 (было 20) — снизили вес относительно структурных сигналов
        if rsi_1h >= 80:   score, desc = 12, f"RSI {rsi_1h:.1f} — Экстремальная перекупленность ✅"
        elif rsi_1h >= 75: score, desc = 10, f"RSI {rsi_1h:.1f} — Сильная перекупленность"
        elif rsi_1h >= 70: score, desc = 8,  f"RSI {rsi_1h:.1f} — Перекупленность"
        elif rsi_1h >= 65: score, desc = 6,  f"RSI {rsi_1h:.1f} — Начало перекупленности"
        elif rsi_1h >= 55: score, desc = 4,  f"RSI {rsi_1h:.1f} — Нейтрально-bullish (ок)"
        elif rsi_1h >= 40: score, desc = 3,  f"RSI {rsi_1h:.1f} — Нейтральная зона"
        elif rsi_1h >= 30: score, desc = 2,  f"RSI {rsi_1h:.1f} — Нижняя нейтраль"
        else:              score, desc = 1,  f"RSI {rsi_1h:.1f} — Перепродан (осторожнее)"
        # Не даём 0: RSI — не gate, лишь снижает вес при экстремальной перепроданности
        # Multi-TF overbought confirmation for SHORT
        mtf_bonus = 0
        if rsi_15m is not None and rsi_30m is not None:
            if rsi_15m > 72 and rsi_30m > 68:
                mtf_bonus += 3; desc += f" | RSI15m={rsi_15m:.0f} RSI30m={rsi_30m:.0f} — перегрев везде"
            elif rsi_15m > 68 and rsi_30m > 63:
                mtf_bonus += 2; desc += f" | RSI15m={rsi_15m:.0f} подтверждает"
        if rsi_4h is not None and rsi_4h > 65:
            mtf_bonus += 2; desc += f" | RSI4H={rsi_4h:.0f}"
        score = min(score + mtf_bonus, 12)
        return ScoreComponent("RSI", score, 12, desc, rsi_1h)

    def calculate_funding_component(self, current_funding: float,
                                    accumulated_4d: float) -> ScoreComponent:
        score, reasons = 0, []
        # ✅ FIX: пороги пересчитаны под реальный диапазон крипто-фандинга (-0.5%..+0.5%)
        if current_funding >= 0.10:    score += 10; reasons.append(f"🔥 Экстремальный фандинг {current_funding:.3f}% — лонги перегреты")
        elif current_funding >= 0.05:  score += 7;  reasons.append(f"Высокий фандинг {current_funding:.3f}%")
        elif current_funding >= 0.02:  score += 5;  reasons.append(f"Повышенный фандинг {current_funding:.3f}%")
        elif current_funding > 0:      score += 2;  reasons.append(f"Позитивный фандинг {current_funding:.3f}%")
        elif current_funding <= -0.05: score += 0;  reasons.append(f"Отрицательный фандинг (шорты платят — плохо)")
        else:                          score += 1;  reasons.append(f"Нейтральный фандинг {current_funding:.3f}%")
        if accumulated_4d >= 0.30:     score += 7;  reasons.append(f"Высокий накопл. фандинг {accumulated_4d:.2f}%")
        elif accumulated_4d >= 0.15:   score += 5;  reasons.append(f"Накопл. фандинг {accumulated_4d:.2f}%")
        elif accumulated_4d >= 0.05:   score += 3
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

    def calculate_oi_component(self, oi_15m: float = 0.0, oi_30m: float = 0.0,
                               oi_1h: float = 0.0, oi_4h: float = 0.0,
                               price_change_24h: float = 0.0) -> ScoreComponent:
        score, reasons = 0, []
        # OI 15m — ультрабыстрый сигнал (прямо сейчас)
        if oi_15m < -2:   score += 3; reasons.append(f"OI 15m {oi_15m:.1f}% — ликвидации сейчас")
        elif oi_15m < -1: score += 1; reasons.append(f"OI 15m {oi_15m:.1f}%")
        elif oi_15m > 3:  score += 2; reasons.append(f"OI 15m +{oi_15m:.1f}% — лонги открываются (топливо)")
        elif oi_15m > 1:  score += 1; reasons.append(f"OI 15m +{oi_15m:.1f}%")
        # OI 30m — быстрый сигнал
        if oi_30m < -2:   score += 3; reasons.append(f"OI 30m {oi_30m:.1f}% — позиции рушатся")
        elif oi_30m < -1: score += 1; reasons.append(f"OI 30m {oi_30m:.1f}%")
        elif oi_30m > 3:  score -= 1; reasons.append(f"OI 30m +{oi_30m:.1f}% — вход лонгов")
        # OI 1H — основной сигнал
        if oi_1h > 3:    score += 5; reasons.append(f"OI 1H +{oi_1h:.1f}% — позиции открываются")
        elif oi_1h > 1:  score += 2; reasons.append(f"OI 1H +{oi_1h:.1f}%")
        elif oi_1h < -3: score -= 2; reasons.append(f"OI 1H {oi_1h:.1f}% — закрытие (против шорта)")
        # OI 4H — фоновый тренд
        if oi_4h > 8:    score += 3; reasons.append(f"OI 4H +{oi_4h:.1f}% — накопление позиций")
        elif oi_4h > 3:  score += 1; reasons.append(f"OI 4H +{oi_4h:.1f}%")
        # Цена 24H — контекст перегрева
        if price_change_24h >= 15: score += 4; reasons.append(f"Цена +{price_change_24h:.1f}% за 24H — перегрев")
        elif price_change_24h >= 8: score += 2; reasons.append(f"Цена +{price_change_24h:.1f}% за 24H")
        elif price_change_24h < -10: score += 2; reasons.append(f"Обвал {price_change_24h:.1f}% за 24H")
        return ScoreComponent("OI", min(score, 15), 15, " | ".join(reasons) or "Нейтральный OI", oi_15m)

    def calculate_delta_component(self, hourly_deltas: List[float],
                                   price_trend: str,
                                   delta_30m: Optional[List[float]] = None) -> ScoreComponent:
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
        # 30M быстрая дельта — агрессия продавцов прямо сейчас
        if delta_30m and len(delta_30m) >= 2:
            recent = delta_30m[-4:]  # последние 2 часа по 30m
            neg_30m = sum(1 for d in recent if d < 0)
            if neg_30m >= 3:
                score += 5; reasons.append(f"30m дельта: {neg_30m}/4 негат. — продавцы сейчас")
            elif neg_30m >= 2:
                score += 2; reasons.append(f"30m дельта: {neg_30m}/4 негат.")
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
                        long_ratio, price_change_24h: float = 0.0,
                        hourly_deltas = None, price_trend = "sideways", patterns = None,
                        volume_spike_ratio: float = 1.0,
                        atr_14_pct: float = 0.5,
                        top_trader_ratio: Optional[float] = None,
                        taker_ratio: Optional[float] = None,
                        rsi_15m: float = None, rsi_30m: float = None, rsi_4h: float = None,
                        oi_15m: float = 0.0, oi_30m: float = 0.0,
                        oi_1h: float = 0.0, oi_4h: float = 0.0,
                        htf_structure: str = "", zone: str = "",
                        delta_30m: Optional[List[float]] = None,
                        orderbook_score: int = 0,
                        liq_analysis=None) -> ScoreResult:
        if hourly_deltas is None:
            hourly_deltas = []
        if patterns is None:
            patterns = []
        components = []
        components.append(self.calculate_rsi_component(rsi_1h, rsi_15m, rsi_30m, rsi_4h))
        # 🆕 HTF+Zone вместо Funding (в BASE_SCORER)
        components.append(self.calculate_htf_zone_component(htf_structure, zone, "short"))
        components.append(self.calculate_ratio_component(long_ratio))
        components.append(self.calculate_oi_component(oi_15m, oi_30m, oi_1h, oi_4h, price_change_24h))
        components.append(self.calculate_delta_component(hourly_deltas, price_trend, delta_30m=delta_30m))
        pat_comp, pat_names = self.calculate_pattern_component(patterns)
        components.append(pat_comp)
        components.append(self.calculate_top_trader_component(top_trader_ratio, "short"))
        total = sum(c.score for c in components)
        max_p = sum(c.max_score for c in components)
        # Confluence bonus
        strong = sum(1 for c in components if c.score >= c.max_score * 0.6)
        if strong >= 4: total += 5
        elif strong >= 3: total += 3
        vs_bonus, vs_reason = self._volume_spike_bonus(volume_spike_ratio)
        total += vs_bonus
        atr_pen, atr_reason = self._atr_penalty(atr_14_pct)
        total += atr_pen
        tk_bonus, tk_reason = self._taker_bonus(taker_ratio, "short")
        total += tk_bonus
        # P4: Funding extreme bonus
        fe_bonus, fe_reason = self._funding_extreme_bonus(funding_current, "short")
        total += fe_bonus
        # P1: Order book score
        try:
            total += int(orderbook_score)
        except Exception:
            pass
        # S10: Liquidation zone магниты
        liq_comp = self.calculate_liquidation_component(liq_analysis)
        liq_bonus = liq_comp.score
        total += liq_bonus
        total = min(max(total, 0), 100)
        reasons = []
        if components[0].score >= 8:  reasons.append(f"RSI перекуплен ({rsi_1h:.1f})")
        if components[1].score >= 8:  reasons.append(components[1].description)
        if components[2].score >= 10: reasons.append(f"Толпа в лонгах ({long_ratio:.0f}%)")
        if components[3].score >= 10: reasons.append("Лонги перегружены (OI растёт)")
        if components[4].score >= 10: reasons.append("Медвежья дивергенция")
        if components[5].score >= 20: reasons.append(f"Сильный паттерн: {pat_names[0] if pat_names else 'N/A'}")
        if components[6].score >= 7:  reasons.append(components[6].description)
        if vs_reason: reasons.append(vs_reason)
        if atr_reason: reasons.append(atr_reason)
        if tk_reason: reasons.append(tk_reason)
        if fe_reason: reasons.append(fe_reason)
        if orderbook_score >= 6: reasons.append(f"Стакан подтверждает short")
        if liq_bonus != 0 and liq_comp.description: reasons.append(liq_comp.description)
        # Funding — только для уведомления, не в скоре
        f_info = self._funding_info_str(funding_current, funding_accumulated)
        return ScoreResult(
            total_score=total, max_possible=max_p, direction=Direction.SHORT,
            is_valid=total >= self.min_score,
            confidence=self.determine_confidence(total),
            grade=self.calculate_grade(total),
            components=components, reasons=reasons,
            funding_info=f_info,
        )


class LongScorer(BaseScorer):

    def __init__(self, min_score: int = 55):  # Ниже порог для лонгов
        super().__init__(min_score, Direction.LONG)

    def calculate_rsi_component(self, rsi_1h: float, price_change_1h: float = 0.0, rsi_15m: float = None, rsi_30m: float = None, rsi_4h: float = None) -> ScoreComponent:
        # RSI — ВСПОМОГАТЕЛЬНЫЙ фактор. Не блокирует при трендовом движении.
        is_momentum = price_change_1h > 3.0  # цена +3%/час = явный тренд вверх
        if rsi_1h <= 20:   score, desc = 20, f"RSI {rsi_1h:.1f} — Экстремальная перепроданность"
        elif rsi_1h <= 25: score, desc = 18, f"RSI {rsi_1h:.1f} — Сильная перепроданность"
        elif rsi_1h <= 30: score, desc = 15, f"RSI {rsi_1h:.1f} — Перепроданность"
        elif rsi_1h <= 35: score, desc = 12, f"RSI {rsi_1h:.1f} — Начало перепроданности"
        elif rsi_1h <= 40: score, desc = 10, f"RSI {rsi_1h:.1f} — Близко к перепроданности"
        elif rsi_1h <= 45: score, desc = 8,  f"RSI {rsi_1h:.1f} — Нейтрально-bearish"
        elif rsi_1h <= 55: score, desc = 6,  f"RSI {rsi_1h:.1f} — Нейтральная зона (ок для моментума)"
        elif rsi_1h <= 60: score, desc = 4,  f"RSI {rsi_1h:.1f} — Умеренный моментум"
        elif rsi_1h <= 65: score, desc = 3,  f"RSI {rsi_1h:.1f} — Начало перекупленности"
        elif rsi_1h <= 75: score, desc = (5 if is_momentum else 1), f"RSI {rsi_1h:.1f} — {'MOMENTUM тренд' if is_momentum else 'Высокий RSI'}"
        elif rsi_1h <= 85: score, desc = (6 if is_momentum else 0), f"RSI {rsi_1h:.1f} — {'Сильный MOMENTUM — тренд продолжается' if is_momentum else 'Перекупленность'}"
        else:              score, desc = (4 if is_momentum else 0), f"RSI {rsi_1h:.1f} — {'Экстремальный MOMENTUM' if is_momentum else 'Перекупленность крит.'}"
        # Multi-TF oversold confirmation for LONG — разворот начался прямо сейчас?
        mtf_bonus = 0
        mtf_parts = []
        if rsi_15m is not None and rsi_30m is not None:
            if rsi_15m < 25 and rsi_30m < 30:
                mtf_bonus += 4; mtf_parts.append(f"RSI15m={rsi_15m:.0f} RSI30m={rsi_30m:.0f} — дамп сейчас")
            elif rsi_15m < 30 and rsi_30m < 35:
                mtf_bonus += 2; mtf_parts.append(f"RSI15m={rsi_15m:.0f} перепродан")
            # 15m RSI выше 30m = начало отскока
            if rsi_15m > rsi_30m and rsi_30m < 35 and rsi_15m > 30:
                mtf_bonus += 3; mtf_parts.append(f"RSI15m({rsi_15m:.0f})>30m({rsi_30m:.0f}) — отскок")
        elif rsi_15m is not None and rsi_15m < 30:
            mtf_bonus += 2
        if rsi_4h is not None and rsi_4h < 35:
            mtf_bonus += 2; mtf_parts.append(f"RSI4H={rsi_4h:.0f} тоже перепродан")
        score = min(score + mtf_bonus, 20)
        if mtf_parts:
            desc += " | " + " | ".join(mtf_parts)
        return ScoreComponent("RSI", score, 20, desc, rsi_1h)

    def calculate_funding_component(self, current_funding: float,
                                    accumulated_4d: float) -> ScoreComponent:
        score, reasons = 0, []
        # ✅ FIX: пересчитаны под реальный диапазон
        if current_funding <= -0.10:   score += 10; reasons.append(f"🔥 Экстремальный -фандинг {current_funding:.3f}% — шорты перегреты")
        elif current_funding <= -0.05: score += 7;  reasons.append(f"Высокий -фандинг {current_funding:.3f}%")
        elif current_funding <= -0.02: score += 5;  reasons.append(f"Повышенный -фандинг {current_funding:.3f}%")
        elif current_funding < 0:      score += 2;  reasons.append(f"Отрицательный фандинг {current_funding:.3f}%")
        elif current_funding >= 0.10:  score += 0;  reasons.append(f"Высокий +фандинг {current_funding:.3f}% (плохо для лонга)")
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

    def calculate_oi_component(self, oi_15m: float = 0.0, oi_30m: float = 0.0,
                               oi_1h: float = 0.0, oi_4h: float = 0.0,
                               price_change_24h: float = 0.0) -> ScoreComponent:
        score, reasons = 0, []
        # OI 15m — ультрабыстрый (шорты закрываются прямо сейчас = лонг импульс)
        if oi_15m < -2:   score += 3; reasons.append(f"OI 15m {oi_15m:.1f}% — шорты сдаются (сквиз)")
        elif oi_15m < -1: score += 1; reasons.append(f"OI 15m {oi_15m:.1f}%")
        elif oi_15m > 3:  score += 2; reasons.append(f"OI 15m +{oi_15m:.1f}% — лонги входят быстро")
        elif oi_15m > 1:  score += 1; reasons.append(f"OI 15m +{oi_15m:.1f}%")
        # OI 30m — быстрый сигнал
        if oi_30m > 2:    score += 3; reasons.append(f"OI 30m +{oi_30m:.1f}% — быстрый вход")
        elif oi_30m > 1:  score += 1; reasons.append(f"OI 30m +{oi_30m:.1f}%")
        elif oi_30m < -3: score += 2; reasons.append(f"OI 30m {oi_30m:.1f}% — шорты быстро закрываются")
        # OI 1H — главный сигнал раннего входа
        if oi_1h > 3:    score += 5; reasons.append(f"OI 1H +{oi_1h:.1f}% — лонги входят сейчас")
        elif oi_1h > 1:  score += 3; reasons.append(f"OI 1H +{oi_1h:.1f}%")
        elif oi_1h < -3: score += 4; reasons.append(f"OI 1H {oi_1h:.1f}% — шорты закрываются (сквиз)")
        elif oi_1h < -1: score += 2; reasons.append(f"OI 1H {oi_1h:.1f}% — шорты выходят")
        # OI 4H — фоновый тренд
        if oi_4h > 5:    score += 3; reasons.append(f"OI 4H +{oi_4h:.1f}% — накопление")
        elif oi_4h < -5: score += 2; reasons.append(f"OI 4H {oi_4h:.1f}% — разгрузка позиций")
        # Цена 24H — контекст перепроданности
        if price_change_24h <= -15: score += 4; reasons.append(f"Цена {price_change_24h:.1f}% — перепроданность")
        elif price_change_24h <= -8: score += 2; reasons.append(f"Цена {price_change_24h:.1f}% за 24H")
        elif price_change_24h >= 10: score -= 2; reasons.append(f"Цена +{price_change_24h:.1f}% (перегрев)")
        return ScoreComponent("OI", min(score, 15), 15, " | ".join(reasons) or "Нейтральный OI", oi_15m)

    def calculate_delta_component(self, hourly_deltas: List[float],
                                   price_trend: str,
                                   btc_change_1h: float = 0.0,
                                   coin_change_1h: float = 0.0,
                                   delta_30m: Optional[List[float]] = None) -> ScoreComponent:
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
        # ✅ FIX #5: Относительная слабость vs BTC — монета падает сильнее BTC при общем росте
        elif price_trend == "rising" and btc_change_1h > 0 and pos_hours >= 3:
            relative_weakness = btc_change_1h - coin_change_1h
            if relative_weakness > 1.0:
                score += 10; reasons.append(f"Относит. слабость vs BTC (+{btc_change_1h:.1f}% vs +{coin_change_1h:.1f}%) — накопление при росте рынка")
            elif relative_weakness > 0.5:
                score += 6; reasons.append(f"Умеренная слабость vs BTC — дельта растёт")
        # 30M быстрая дельта — покупатели агрессивны прямо сейчас?
        if delta_30m and len(delta_30m) >= 2:
            recent = delta_30m[-4:]  # последние 2 часа по 30m
            pos_30m = sum(1 for d in recent if d > 0)
            if pos_30m >= 3:
                score += 5; reasons.append(f"30m дельта: {pos_30m}/4 позит. — покупатели сейчас")
            elif pos_30m >= 2:
                score += 2; reasons.append(f"30m дельта: {pos_30m}/4 позит.")
        return ScoreComponent("Delta", min(score, 20), 20, " | ".join(reasons) or "Нейтральная дельта",
                              sum(hourly_deltas))

    def calculate_pattern_component(self, patterns: List[Pattern]) -> Tuple[ScoreComponent, List[str]]:
        if not patterns:
            return ScoreComponent("Patterns", 0, 30, "Нет паттернов"), []
        best = max(patterns, key=lambda p: LONG_PATTERN_STRENGTHS.get(p.name, p.strength))
        base = LONG_PATTERN_STRENGTHS.get(best.name, best.strength)
        bonus = (3 if len(patterns) >= 2 else 0) + (5 if len(patterns) >= 3 else 0)
        fresh = 2 if best.candles_ago == 0 else (1 if best.candles_ago == 1 else 0)
        
        # 🆕 Momentum bonus for breakout patterns
        momentum_bonus = 0
        if best.name in ("BREAKOUT_LONG", "MOMENTUM_LONG", "LIQUIDITY_SWEEP_LONG"):
            momentum_bonus = 5  # Extra bonus for momentum entries
        
        total = min(base + bonus + fresh + momentum_bonus, 30)
        names = [p.name for p in patterns]
        desc = f"{best.name} (base={base})"
        if momentum_bonus: desc += f" +momentum{int(momentum_bonus)}"
        if len(patterns) > 1: desc += f" +{len(patterns)-1} паттернов"
        return ScoreComponent("Patterns", total, 30, desc), names

    def calculate_score(self, rsi_1h, funding_current, funding_accumulated,
                        long_ratio, price_change_24h: float = 0.0,
                        hourly_deltas = None, price_trend = "sideways", patterns = None,
                        volume_spike_ratio: float = 1.0,
                        atr_14_pct: float = 0.5,
                        price_change_1h: float = 0.0,
                        top_trader_ratio: Optional[float] = None,
                        taker_ratio: Optional[float] = None,
                        btc_change_1h: float = 0.0,
                        rsi_15m: float = None, rsi_30m: float = None, rsi_4h: float = None,
                        oi_15m: float = 0.0, oi_30m: float = 0.0,
                        oi_1h: float = 0.0, oi_4h: float = 0.0,
                        htf_structure: str = "", zone: str = "",
                        momentum_mode: bool = False,
                        delta_30m: Optional[List[float]] = None,
                        orderbook_score: int = 0,
                        liq_analysis=None) -> ScoreResult:
        if hourly_deltas is None:
            hourly_deltas = []
        if patterns is None:
            patterns = []
        components = []
        components.append(self.calculate_rsi_component(rsi_1h, price_change_1h, rsi_15m, rsi_30m, rsi_4h))
        # 🆕 HTF+Zone вместо Funding (в BASE_SCORER)
        components.append(self.calculate_htf_zone_component(htf_structure, zone, "long"))
        components.append(self.calculate_ratio_component(long_ratio))
        components.append(self.calculate_oi_component(oi_15m, oi_30m, oi_1h, oi_4h, price_change_24h))
        components.append(self.calculate_delta_component(hourly_deltas, price_trend, btc_change_1h=btc_change_1h, coin_change_1h=price_change_1h, delta_30m=delta_30m))
        pat_comp, pat_names = self.calculate_pattern_component(patterns)
        components.append(pat_comp)
        components.append(self.calculate_top_trader_component(top_trader_ratio, "long"))
        total = sum(c.score for c in components)
        max_p = sum(c.max_score for c in components)
        strong = sum(1 for c in components if c.score >= c.max_score * 0.6)
        if strong >= 4: total += 5
        elif strong >= 3: total += 3
        # Momentum Mode — для breakout/continuation сигналов
        if momentum_mode:
            m_bonus = 0
            if 55 <= rsi_1h <= 72:       m_bonus += 10
            if oi_1h > 2:                m_bonus += 8
            elif oi_1h > 1:              m_bonus += 4
            if volume_spike_ratio > 3:   m_bonus += 6
            elif volume_spike_ratio > 2: m_bonus += 3
            if price_change_1h > 4:      m_bonus += 4
            elif price_change_1h > 2:    m_bonus += 2
            total += m_bonus
        vs_bonus, vs_reason = self._volume_spike_bonus(volume_spike_ratio)
        total += vs_bonus
        atr_pen, atr_reason = self._atr_penalty(atr_14_pct)
        total += atr_pen
        tk_bonus, tk_reason = self._taker_bonus(taker_ratio, "long")
        total += tk_bonus
        # P4: Funding extreme bonus
        fe_bonus, fe_reason = self._funding_extreme_bonus(funding_current, "long")
        total += fe_bonus
        # P1: Order book score
        try:
            total += int(orderbook_score)
        except Exception:
            pass
        # S10: Liquidation zone магниты
        liq_comp = self.calculate_liquidation_component(liq_analysis)
        liq_bonus = liq_comp.score
        total += liq_bonus
        total = min(max(total, 0), 100)
        reasons = []
        if components[0].score >= 15: reasons.append(f"RSI перепродан ({rsi_1h:.1f})")
        if components[1].score >= 8:  reasons.append(components[1].description)
        if components[2].score >= 10: reasons.append(f"Толпа в шортах ({100-long_ratio:.0f}%)")
        if components[3].score >= 10: reasons.append("Шорты закрываются (OI падает)")
        if components[4].score >= 10: reasons.append("Бычья дивергенция")
        if components[5].score >= 20: reasons.append(f"Сильный паттерн: {pat_names[0] if pat_names else 'N/A'}")
        if components[6].score >= 7:  reasons.append(components[6].description)
        if vs_reason: reasons.append(vs_reason)
        if atr_reason: reasons.append(atr_reason)
        if tk_reason: reasons.append(tk_reason)
        if fe_reason: reasons.append(fe_reason)
        if orderbook_score >= 6: reasons.append(f"Стакан подтверждает long")
        if liq_bonus != 0 and liq_comp.description: reasons.append(liq_comp.description)
        # Funding — только для уведомления, не в скоре
        f_info = self._funding_info_str(funding_current, funding_accumulated)
        return ScoreResult(
            total_score=total, max_possible=max_p, direction=Direction.LONG,
            is_valid=total >= self.min_score,
            confidence=self.determine_confidence(total),
            grade=self.calculate_grade(total),
            components=components, reasons=reasons,
            funding_info=f_info,
        )


# ============================================================================
# SINGLETON
# ============================================================================

_short_scorer = None
_long_scorer  = None

def get_short_scorer(min_score: int = 58) -> ShortScorer:
    """
    Синглтон ShortScorer.
    Дефолт 58 совпадает с ShortScorer.__init__ — dead zone 50-57 убрана.
    """
    global _short_scorer
    if _short_scorer is None:
        _short_scorer = ShortScorer(min_score)
    return _short_scorer

def get_long_scorer(min_score: int = 55) -> LongScorer:
    """
    Синглтон LongScorer.
    Дефолт 55 совпадает с LongScorer.__init__.
    """
    global _long_scorer
    if _long_scorer is None:
        _long_scorer = LongScorer(min_score)
    return _long_scorer

def reset_scorers() -> None:
    """Сброс синглтонов — для тестов и перезапуска с новым min_score."""
    global _short_scorer, _long_scorer
    _short_scorer = None
    _long_scorer  = None
