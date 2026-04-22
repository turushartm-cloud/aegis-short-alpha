"""
Pattern Detector v3.0 — ЕДИНЫЙ ФАЙЛ (Long + Short)

ЗАМЕНЯЕТ оба файла:
  - pattern_detector.py (старый)
  - pattern_detector_v2.py (удалить!)

ИСПРАВЛЕНИЕ v3.0:
  ✅ PatternResult.strength → alias для score_bonus
     Исправляет: 'PatternResult' object has no attribute 'strength'
     Scorer.py использует p.strength — теперь работает с обоими файлами

ПАТТЕРНЫ:
  LONG:  MEGA_LONG, TRAP_SHORT, REJECTION_LONG,
         BREAKOUT_LONG, MOMENTUM_LONG, LIQUIDITY_SWEEP_LONG,
         CONSOLIDATION_BREAK_LONG, WYCKOFF_SPRING
  SHORT: MEGA_SHORT, TRAP_LONG, REJECTION_SHORT,
         BREAKOUT_SHORT, MOMENTUM_SHORT, LIQUIDITY_SWEEP_SHORT,
         DISTRIBUTION_BREAK, WYCKOFF_UPTHRUST
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


# ============================================================================
# PatternResult — ЕДИНЫЙ датакласс для всех паттернов
# ============================================================================

@dataclass
class PatternResult:
    name: str
    score_bonus: int       # основная метрика силы (0-30)
    confidence: float      # 0.0-1.0
    direction: str         # "long" | "short"
    suggested_sl_pct: float = 0.0
    suggested_tp1_pct: float = 0.0
    reasons: List[str] = field(default_factory=list)

    # ── ОБРАТНАЯ СОВМЕСТИМОСТЬ: scorer.py использует p.strength ─────────────
    # Исправляет: 'PatternResult' object has no attribute 'strength'
    @property
    def strength(self) -> int:
        """Alias для score_bonus (обратная совместимость со старым scorer.py)."""
        return self.score_bonus

    # Поля из старого Pattern датакласса (для scorer.py)
    @property
    def candles_ago(self) -> int:
        return 0

    @property
    def freshness(self) -> int:
        return 2


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def _closes(candles) -> List[float]:
    return [c.close for c in candles]

def _highs(candles) -> List[float]:
    return [c.high for c in candles]

def _lows(candles) -> List[float]:
    return [c.low for c in candles]

def _avg_vol(candles, lookback: int = 20) -> float:
    vols = [c.quote_volume for c in candles]
    if len(vols) < lookback:
        return sum(vols) / len(vols) if vols else 1.0
    return sum(vols[-lookback-1:-1]) / lookback

def _vol_spike(candles, lookback: int = 20) -> float:
    avg = _avg_vol(candles, lookback)
    if avg <= 0:
        return 1.0
    return candles[-1].quote_volume / avg

def _atr(candles, period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        pc  = candles[i-1].close
        tr  = max(candles[i].high - candles[i].low,
                  abs(candles[i].high - pc), abs(candles[i].low - pc))
        trs.append(tr)
    return sum(trs[-period:]) / period

def _body(c) -> float:
    return abs(c.close - c.open)

def _ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return values
    k   = 2 / (period + 1)
    ema = [sum(values[:period]) / period]
    for v in values[period:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def _swing_highs(candles, lookback: int = 3) -> List[float]:
    highs = []
    for i in range(lookback, len(candles) - lookback):
        h = candles[i].high
        if (all(candles[j].high <= h for j in range(i-lookback, i)) and
                all(candles[j].high <= h for j in range(i+1, i+lookback+1))):
            highs.append(h)
    return highs

def _swing_lows(candles, lookback: int = 3) -> List[float]:
    lows = []
    for i in range(lookback, len(candles) - lookback):
        l = candles[i].low
        if (all(candles[j].low >= l for j in range(i-lookback, i)) and
                all(candles[j].low >= l for j in range(i+1, i+lookback+1))):
            lows.append(l)
    return lows


# ============================================================================
# LONG PATTERN DETECTOR
# ============================================================================

class LongPatternDetector:
    """Детектор паттернов для LONG входов."""

    def detect_all(self, candles, hourly_deltas=None, market_data=None) -> List[PatternResult]:
        results = []
        for fn in [
            self.detect_breakout_long,
            self.detect_momentum_long,
            self.detect_liquidity_sweep_long,
            self.detect_consolidation_break_long,
            self.detect_wyckoff_spring,
            self.detect_mega_long,
            self.detect_trap_short,
            self.detect_rejection_long,
        ]:
            try:
                r = fn(candles, hourly_deltas, market_data)
                if r:
                    results.append(r)
            except Exception:
                pass
        results.sort(key=lambda x: x.score_bonus, reverse=True)
        return results

    def _get_price_trend(self, candles) -> str:
        if len(candles) < 20:
            return "flat"
        closes = _closes(candles)
        ema20  = _ema(closes, 20)
        if not ema20:
            return "flat"
        slope = (ema20[-1] - ema20[-5]) / ema20[-5] * 100 if len(ema20) >= 5 else 0
        if closes[-1] > ema20[-1] and slope > 0.1:
            return "up"
        elif closes[-1] < ema20[-1] and slope < -0.1:
            return "down"
        return "flat"

    # ── НОВЫЕ ПАТТЕРНЫ ────────────────────────────────────────────────────────

    def detect_breakout_long(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """BREAKOUT_LONG: Пробой флэта вверх с объёмом."""
        if len(candles) < 25:
            return None
        consolidation = candles[-20:-2]
        last          = candles[-1]
        high_cons     = max(c.high for c in consolidation)
        low_cons      = min(c.low  for c in consolidation)
        cons_range    = (high_cons - low_cons) / low_cons * 100 if low_cons else 999
        if cons_range > 3.0 or last.close <= high_cons:
            return None
        vol_spike = _vol_spike(candles, 20)
        if vol_spike < 1.5:
            return None
        rng = last.high - last.low
        if rng > 0 and (last.close - last.open) / rng < 0.5:
            return None
        breakout_pct = (last.close - high_cons) / high_cons * 100
        bonus = min(25, int(10 + vol_spike * 3 + breakout_pct * 2))
        return PatternResult(
            name="BREAKOUT_LONG", score_bonus=bonus,
            confidence=min(0.9, 0.5 + vol_spike * 0.1), direction="long",
            suggested_sl_pct=round((last.close - low_cons) / last.close * 100, 2),
            reasons=[f"Breakout выше {high_cons:.4f} (флэт {cons_range:.1f}%)",
                     f"Volume spike {vol_spike:.1f}x | +{breakout_pct:.2f}%"],
        )

    def detect_momentum_long(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """MOMENTUM_LONG: Сильная бычья свеча + volume spike ≥2x."""
        if len(candles) < 25:
            return None
        last      = candles[-1]
        atr_v     = _atr(candles, 14)
        vol_spike = _vol_spike(candles, 20)
        if vol_spike < 2.0:
            return None
        rng = last.high - last.low
        if rng <= 0 or (last.close - last.low) / rng < 0.65:
            return None
        body = last.close - last.open
        if body <= 0 or (atr_v > 0 and body < atr_v * 0.5):
            return None
        closes = _closes(candles)
        ema20  = _ema(closes, 20)
        if ema20 and last.close < ema20[-1]:
            return None
        rsi = getattr(md, "rsi_1h", None) if md else None
        if rsi and (rsi < 40 or rsi > 78):
            return None
        pct_move = (last.close - last.open) / last.open * 100 if last.open else 0
        bonus    = min(20, int(12 + vol_spike * 1.5))
        return PatternResult(
            name="MOMENTUM_LONG", score_bonus=bonus,
            confidence=min(0.85, 0.55 + vol_spike * 0.08), direction="long",
            suggested_sl_pct=round((last.close - last.open) / last.close * 100, 2),
            reasons=[f"Momentum свеча +{pct_move:.2f}% | Volume {vol_spike:.1f}x avg"],
        )

    def detect_liquidity_sweep_long(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """LIQUIDITY_SWEEP_LONG: Stop hunt вниз → разворот (ICT/SMC)."""
        if len(candles) < 20:
            return None
        last  = candles[-1]
        sweep = candles[-2]
        atr_v = _atr(candles[:-2], 14)
        if atr_v <= 0:
            return None
        lower_wick = min(sweep.open, sweep.close) - sweep.low
        if lower_wick < atr_v * 1.0:
            return None
        swing_lows_list = _swing_lows(candles[:-3], lookback=3)
        if not swing_lows_list:
            return None
        recent_sl = (min(swing_lows_list[-3:]) if len(swing_lows_list) >= 3
                     else swing_lows_list[-1])
        if sweep.low > recent_sl or sweep.close < recent_sl:
            return None
        if last.close <= last.open:
            return None
        vol_spike    = _vol_spike(candles[:-1], 20)
        sweep_depth  = (recent_sl - sweep.low) / atr_v
        bonus        = min(25, int(18 + sweep_depth * 2 + vol_spike))
        return PatternResult(
            name="LIQUIDITY_SWEEP_LONG", score_bonus=bonus,
            confidence=0.75, direction="long",
            suggested_sl_pct=round((last.close - sweep.low) / last.close * 100 + 0.2, 2),
            reasons=[f"Ликвидность снята под {recent_sl:.4f}",
                     f"Sweep {sweep_depth:.1f}x ATR | Volume {vol_spike:.1f}x",
                     "Цена вернулась выше уровня — разворот"],
        )

    def detect_consolidation_break_long(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """CONSOLIDATION_BREAK_LONG: Выход из боковика вверх."""
        if len(candles) < 25:
            return None
        cons       = candles[-22:-2]
        last       = candles[-1]
        high_cons  = max(c.high for c in cons)
        low_cons   = min(c.low  for c in cons)
        range_pct  = (high_cons - low_cons) / low_cons * 100 if low_cons else 999
        if range_pct > 2.5 or last.close <= high_cons:
            return None
        vol_spike    = _vol_spike(candles, 20)
        if vol_spike < 1.3:
            return None
        breakout_pct = (last.close - high_cons) / high_cons * 100
        bonus        = min(18, int(10 + vol_spike * 2 + breakout_pct))
        return PatternResult(
            name="CONSOLIDATION_BREAK_LONG", score_bonus=bonus,
            confidence=0.65, direction="long",
            suggested_sl_pct=round((last.close - low_cons) / last.close * 100, 2),
            reasons=[f"Флэт {range_pct:.1f}% ({len(cons)} свечей) → пробой +{breakout_pct:.2f}%",
                     f"Volume {vol_spike:.1f}x"],
        )

    def detect_wyckoff_spring(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """WYCKOFF_SPRING: Ложный пробой лоу диапазона накопления."""
        if len(candles) < 30:
            return None
        acc    = candles[-30:-5]
        last   = candles[-1]
        prev   = candles[-2]
        r_high = max(c.high for c in acc)
        r_low  = min(c.low  for c in acc)
        r_pct  = (r_high - r_low) / r_low * 100 if r_low else 999
        if not (1.5 < r_pct < 8.0):
            return None
        if prev.low > r_low or prev.close < r_low:
            return None
        avg_vol = _avg_vol(acc, min(len(acc), 15))
        if prev.quote_volume > avg_vol * 1.5:
            return None
        if last.close < r_low or last.close <= last.open:
            return None
        spring_depth = (r_low - prev.low) / r_low * 100
        return PatternResult(
            name="WYCKOFF_SPRING", score_bonus=22,
            confidence=0.80, direction="long",
            suggested_sl_pct=round((last.close - prev.low) / last.close * 100 + 0.3, 2),
            reasons=[f"Wyckoff Spring: диапазон {r_pct:.1f}%",
                     f"Spring -{spring_depth:.2f}% ниже поддержки",
                     "Низкий объём на spring — ложный пробой"],
        )

    # ── КЛАССИЧЕСКИЕ ПАТТЕРНЫ ─────────────────────────────────────────────────

    def detect_mega_long(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        if len(candles) < 20:
            return None
        rsi = getattr(md, "rsi_1h", None) if md else None
        if rsi and rsi > 45:
            return None
        last       = candles[-1]
        body       = _body(last)
        lower_wick = min(last.open, last.close) - last.low
        if lower_wick < body * 1.5:
            return None
        vol_spike = _vol_spike(candles, 20)
        if vol_spike < 1.2:
            return None
        return PatternResult(
            name="MEGA_LONG", score_bonus=20, confidence=0.6, direction="long",
            reasons=["RSI перепродан" if rsi else "Нижний wick",
                     f"Lower wick {lower_wick:.4f} | Volume {vol_spike:.1f}x"],
        )

    def detect_trap_short(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        if len(candles) < 20:
            return None
        last  = candles[-1]
        prev  = candles[-2]
        atr_v = _atr(candles[:-2], 14)
        prev_lower = min(prev.open, prev.close) - prev.low
        if prev_lower < atr_v * 0.8 or last.close <= last.open:
            return None
        return PatternResult(
            name="TRAP_SHORT", score_bonus=18, confidence=0.58, direction="long",
            reasons=["Шортисты пойманы в ловушку", "Разворот вверх подтверждён"],
        )

    def detect_rejection_long(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        if len(candles) < 10:
            return None
        last       = candles[-1]
        lower_wick = min(last.open, last.close) - last.low
        body       = _body(last)
        if lower_wick < body * 1.0 or last.close < last.open:
            return None
        return PatternResult(
            name="REJECTION_LONG", score_bonus=15, confidence=0.55, direction="long",
            reasons=["Отскок от поддержки", f"Lower wick {lower_wick:.4f}"],
        )


# ============================================================================
# SHORT PATTERN DETECTOR
# ============================================================================

class ShortPatternDetector:
    """Детектор паттернов для SHORT входов."""

    def detect_all(self, candles, hourly_deltas=None, market_data=None) -> List[PatternResult]:
        results = []
        for fn in [
            self.detect_pump_dump_short,  # 🆕 NEW: Pump & Dump паттерн (высокий приоритет)
            self.detect_breakout_short,
            self.detect_momentum_short,
            self.detect_liquidity_sweep_short,
            self.detect_distribution_break,
            self.detect_wyckoff_upthrust,
            self.detect_mega_short,
            self.detect_trap_long,
            self.detect_rejection_short,
        ]:
            try:
                r = fn(candles, hourly_deltas, market_data)
                if r:
                    results.append(r)
            except Exception:
                pass
        results.sort(key=lambda x: x.score_bonus, reverse=True)
        return results

    def _get_price_trend(self, candles) -> str:
        if len(candles) < 20:
            return "flat"
        closes = _closes(candles)
        ema20  = _ema(closes, 20)
        if not ema20:
            return "flat"
        slope = (ema20[-1] - ema20[-5]) / ema20[-5] * 100 if len(ema20) >= 5 else 0
        if closes[-1] < ema20[-1] and slope < -0.1:
            return "down"
        elif closes[-1] > ema20[-1] and slope > 0.1:
            return "up"
        return "flat"

    # ── НОВЫЕ SHORT ПАТТЕРНЫ ──────────────────────────────────────────────────

    def detect_breakout_short(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """BREAKOUT_SHORT: Пробой флэта вниз с объёмом."""
        if len(candles) < 25:
            return None
        cons      = candles[-20:-2]
        last      = candles[-1]
        low_cons  = min(c.low  for c in cons)
        high_cons = max(c.high for c in cons)
        r_pct     = (high_cons - low_cons) / low_cons * 100 if low_cons else 999
        if r_pct > 3.0 or last.close >= low_cons:
            return None
        vol_spike = _vol_spike(candles, 20)
        if vol_spike < 1.5:
            return None
        rng = last.high - last.low
        if rng > 0 and (last.high - last.close) / rng < 0.5:
            return None
        breakdown_pct = (low_cons - last.close) / low_cons * 100
        bonus = min(25, int(10 + vol_spike * 3 + breakdown_pct * 2))
        return PatternResult(
            name="BREAKOUT_SHORT", score_bonus=bonus,
            confidence=min(0.9, 0.5 + vol_spike * 0.1), direction="short",
            suggested_sl_pct=round((high_cons - last.close) / last.close * 100, 2),
            reasons=[f"Пробой ниже {low_cons:.4f} (флэт {r_pct:.1f}%)",
                     f"Volume spike {vol_spike:.1f}x | -{breakdown_pct:.2f}%"],
        )

    def detect_momentum_short(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """MOMENTUM_SHORT: Медвежья импульсная свеча + volume spike ≥2x."""
        if len(candles) < 25:
            return None
        last      = candles[-1]
        atr_v     = _atr(candles, 14)
        vol_spike = _vol_spike(candles, 20)
        if vol_spike < 2.0:
            return None
        rng = last.high - last.low
        if rng <= 0 or (last.high - last.close) / rng < 0.65:
            return None
        body = last.open - last.close
        if body <= 0 or (atr_v > 0 and body < atr_v * 0.5):
            return None
        closes = _closes(candles)
        ema20  = _ema(closes, 20)
        if ema20 and last.close > ema20[-1]:
            return None
        rsi = getattr(md, "rsi_1h", None) if md else None
        if rsi and (rsi > 65 or rsi < 25):
            return None
        bonus = min(20, int(12 + vol_spike * 1.5))
        return PatternResult(
            name="MOMENTUM_SHORT", score_bonus=bonus,
            confidence=min(0.85, 0.55 + vol_spike * 0.08), direction="short",
            suggested_sl_pct=round((last.open - last.close) / last.close * 100, 2),
            reasons=[f"Медвежий импульс | Volume {vol_spike:.1f}x avg"],
        )

    def detect_liquidity_sweep_short(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """LIQUIDITY_SWEEP_SHORT: Stop hunt вверх → разворот вниз (ICT/SMC)."""
        if len(candles) < 20:
            return None
        last  = candles[-1]
        sweep = candles[-2]
        atr_v = _atr(candles[:-2], 14)
        if atr_v <= 0:
            return None
        upper_wick = sweep.high - max(sweep.open, sweep.close)
        if upper_wick < atr_v * 1.0:
            return None
        swing_highs_list = _swing_highs(candles[:-3], lookback=3)
        if not swing_highs_list:
            return None
        recent_sh = (max(swing_highs_list[-3:]) if len(swing_highs_list) >= 3
                     else swing_highs_list[-1])
        if sweep.high < recent_sh or sweep.close > recent_sh:
            return None
        if last.close >= last.open:
            return None
        vol_spike     = _vol_spike(candles[:-1], 20)
        sweep_height  = (sweep.high - recent_sh) / recent_sh * 100
        bonus         = min(25, int(18 + sweep_height * 2 + vol_spike))
        return PatternResult(
            name="LIQUIDITY_SWEEP_SHORT", score_bonus=bonus,
            confidence=0.75, direction="short",
            suggested_sl_pct=round((sweep.high - last.close) / last.close * 100 + 0.2, 2),
            reasons=[f"Ликвидность снята над {recent_sh:.4f}",
                     f"Sweep +{sweep_height:.2f}% выше свинг-хая | Volume {vol_spike:.1f}x",
                     "Разворот вниз подтверждён"],
        )

    def detect_pump_dump_short(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """
        🆕 PUMP_DUMP_SHORT: Резкий pump (+50-200%) → откат → вход в шорт.
        
        Как у DUMP Signals бота:
        1. Резкий pump за 1-3 дня (+50%+)
        2. Вершина сформирована (откат 5-20% от максимума)
        3. Возвращение к зоне вершины для входа в шорт
        4. Ожидаемый dump: -30% до -70%
        """
        if len(candles) < 100:  # Минимум ~1.5 дня на 15m
            return None
        
        # Анализируем последние 3 дня (288 свечей 15m)
        lookback = min(288, len(candles) - 10)
        recent = candles[-lookback:]
        
        # Ищем максимум и минимум за период
        max_high = max(c.high for c in recent)
        min_low = min(c.low for c in recent)
        
        # Проверяем был ли pump
        pump_pct = (max_high - min_low) / min_low * 100 if min_low else 0
        
        # Минимальный pump: +50% (как у DUMP Signals)
        if pump_pct < 50:
            return None
        
        last = candles[-1]
        current_price = last.close
        
        # Проверяем откат от вершины (5-30% — зона входа)
        pullback_pct = (max_high - current_price) / max_high * 100 if max_high else 0
        
        # Слишком рано (< 5% отката) или слишком поздно (> 35% — dump уже начался)
        if pullback_pct < 5 or pullback_pct > 35:
            return None
        
        # Проверяем объём (должен быть высоким)
        vol_spike = _vol_spike(candles, 20)
        if vol_spike < 1.5:
            return None
        
        # Проверяем текущую свечу — должна быть медвежьей или с верхним wick
        upper_wick = last.high - max(last.open, last.close)
        body_size = abs(last.close - last.open)
        
        is_bearish = last.close < last.open
        has_rejection = upper_wick > body_size * 0.8
        
        if not (is_bearish or has_rejection):
            return None
        
        # Бонус зависит от размера pump и качества отката
        bonus = min(30, int(20 + pump_pct * 0.1 + pullback_pct * 0.3))
        
        return PatternResult(
            name="PUMP_DUMP_SHORT",
            score_bonus=bonus,
            confidence=min(0.85, 0.6 + pump_pct / 200),
            direction="short",
            suggested_sl_pct=round((max_high - current_price) / current_price * 100 + 2.0, 2),
            reasons=[
                f"🚀 Pump: +{pump_pct:.0f}% за 3 дня",
                f"📉 Откат: {pullback_pct:.1f}% от вершины",
                f"🎯 SHORT-зона: ${current_price:.6f}+",
                f"Ожидаемый dump: -30% до -70%",
            ],
        )

    def detect_distribution_break(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """DISTRIBUTION_BREAK: Пробой нижней границы зоны распределения."""
        if len(candles) < 25:
            return None
        dist  = candles[-20:-2]
        last  = candles[-1]
        l     = min(c.low  for c in dist)
        h     = max(c.high for c in dist)
        r_pct = (h - l) / l * 100 if l else 999
        if r_pct > 3.0 or r_pct < 0.5 or last.close >= l:
            return None
        vol_spike     = _vol_spike(candles, 20)
        if vol_spike < 1.3:
            return None
        breakdown_pct = (l - last.close) / l * 100
        bonus         = min(18, int(10 + vol_spike * 2 + breakdown_pct))
        return PatternResult(
            name="DISTRIBUTION_BREAK", score_bonus=bonus,
            confidence=0.65, direction="short",
            suggested_sl_pct=round((h - last.close) / last.close * 100, 2),
            reasons=[f"Пробой распределения ниже {l:.4f} | -{breakdown_pct:.2f}%",
                     f"Volume {vol_spike:.1f}x"],
        )

    def detect_wyckoff_upthrust(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        """WYCKOFF_UPTHRUST: Ложный пробой хая зоны распределения."""
        if len(candles) < 30:
            return None
        dist   = candles[-30:-5]
        last   = candles[-1]
        prev   = candles[-2]
        r_h    = max(c.high for c in dist)
        r_l    = min(c.low  for c in dist)
        r_pct  = (r_h - r_l) / r_l * 100 if r_l else 999
        if not (1.5 < r_pct < 8.0):
            return None
        if prev.high < r_h or prev.close > r_h:
            return None
        avg_vol = _avg_vol(dist, min(len(dist), 15))
        if prev.quote_volume > avg_vol * 1.5:
            return None
        if last.close > r_h or last.close >= last.open:
            return None
        uth = (prev.high - r_h) / r_h * 100
        return PatternResult(
            name="WYCKOFF_UPTHRUST", score_bonus=22,
            confidence=0.80, direction="short",
            suggested_sl_pct=round((prev.high - last.close) / last.close * 100 + 0.3, 2),
            reasons=[f"Wyckoff Upthrust: диапазон {r_pct:.1f}%",
                     f"Upthrust +{uth:.2f}% выше сопротивления",
                     "Низкий объём на upthrust — ложный пробой"],
        )

    # ── КЛАССИЧЕСКИЕ ПАТТЕРНЫ ─────────────────────────────────────────────────

    def detect_mega_short(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        if len(candles) < 20:
            return None
        last       = candles[-1]
        upper_wick = last.high - max(last.open, last.close)
        body       = _body(last)
        if upper_wick < body * 1.5:
            return None
        return PatternResult(
            name="MEGA_SHORT", score_bonus=20, confidence=0.6, direction="short",
            reasons=["Верхний wick большой", "Отскок от сопротивления"],
        )

    def detect_trap_long(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        if len(candles) < 20:
            return None
        last  = candles[-1]
        prev  = candles[-2]
        atr_v = _atr(candles[:-2], 14)
        upper_wick = prev.high - max(prev.open, prev.close)
        if upper_wick < atr_v * 0.8 or last.close >= last.open:
            return None
        return PatternResult(
            name="TRAP_LONG", score_bonus=18, confidence=0.58, direction="short",
            reasons=["Лонгисты пойманы в ловушку", "Разворот вниз подтверждён"],
        )

    def detect_rejection_short(self, candles, hourly_deltas=None, md=None) -> Optional[PatternResult]:
        if len(candles) < 10:
            return None
        last       = candles[-1]
        upper_wick = last.high - max(last.open, last.close)
        body       = _body(last)
        if upper_wick < body * 1.0 or last.close > last.open:
            return None
        return PatternResult(
            name="REJECTION_SHORT", score_bonus=15, confidence=0.55, direction="short",
            reasons=["Отскок от сопротивления", f"Upper wick {upper_wick:.4f}"],
        )
