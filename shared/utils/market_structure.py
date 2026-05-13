"""
Market Structure Analyzer v1.0
================================
Полный анализ рыночной структуры на нескольких таймфреймах.

Рассчитывает на основе 30m / 1h / 4h / 1D свечей:
  • PDH/PDL   — Previous Day High/Low
  • PWH/PWL   — Previous Week High/Low (7d)
  • PMH/PML   — Previous Month High/Low (30d)
  • ATH/ATL   — All-Time High/Low (в рамках загруженных 1D данных)
  • Fibonacci — Уровни ретрейса 0.236/0.382/0.5/0.618/0.786 + ext 1.272/1.618
  • POC       — Point of Control (упрощённый, по максимальному объёму)
  • Discount/Premium/Equilibrium — ICT зоны относительно 4H swing
  • CRT       — Candle Range Theory High/Low (пред. свеча 4H и 1D)
  • HTF Structure — Higher High / Higher Low / Lower High / Lower Low (4H)
  • GAP       — Daily / Weekly gaps
  • ATR 30m   — Волатильность на 30m
  • FVG 4H/1D — Fair Value Gap на старших ТФ (через smc_ict_detector)
  • OB 4H/1D  — Order Block на старших ТФ

Используется в get_complete_market_data() и scorer.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# DATACLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FibLevels:
    swing_high:  float = 0.0
    swing_low:   float = 0.0
    # Retracement (from swing_high down)
    r_0:         float = 0.0   # 0% = swing_high
    r_236:       float = 0.0   # 23.6%
    r_382:       float = 0.0   # 38.2%
    r_50:        float = 0.0   # 50% = equilibrium
    r_618:       float = 0.0   # 61.8% = golden pocket
    r_786:       float = 0.0   # 78.6%
    r_100:       float = 0.0   # 100% = swing_low
    # Extension (from swing_low, projected up)
    e_1272:      float = 0.0   # 127.2%
    e_1618:      float = 0.0   # 161.8%
    e_2000:      float = 0.0   # 200%
    # Source TF
    tf:          str   = "4h"


@dataclass
class MarketStructureResult:
    """Результат полного структурного анализа для одного символа."""

    # ── Данные свечей (raw) ──────────────────────────────────────────────────
    has_30m:  bool = False
    has_4h:   bool = False
    has_1d:   bool = False

    # ── Previous Day/Week/Month High/Low ─────────────────────────────────────
    pdh: float = 0.0   # Previous Day High
    pdl: float = 0.0   # Previous Day Low
    pdc: float = 0.0   # Previous Day Close
    pwh: float = 0.0   # Previous Week High (7 trading days)
    pwl: float = 0.0   # Previous Week Low
    pmh: float = 0.0   # Previous Month High (~30d)
    pml: float = 0.0   # Previous Month Low

    # ── ATH/ATL (в рамках загруженного 1D датасета) ──────────────────────────
    ath_1d:  float = 0.0  # All-time high из доступных 1D данных
    atl_1d:  float = 0.0  # All-time low из доступных 1D данных

    # ── Fibonacci ────────────────────────────────────────────────────────────
    fib_4h:  Optional[FibLevels] = None   # Fib от 4H swing
    fib_1d:  Optional[FibLevels] = None   # Fib от 1D swing (недельный)

    # ── POC (Point of Control) ───────────────────────────────────────────────
    poc_4h:  float = 0.0   # Цена с макс. объёмом за последние 20 свечей 4H
    poc_1d:  float = 0.0   # Цена с макс. объёмом за последние 14 дней

    # ── Discount / Equilibrium / Premium ─────────────────────────────────────
    equilibrium_4h: float = 0.0   # 50% уровень 4H swing
    equilibrium_1d: float = 0.0   # 50% уровень недельного swing
    zone_4h:        str   = "neutral"  # "discount" | "equilibrium" | "premium"
    zone_1d:        str   = "neutral"  # "discount" | "equilibrium" | "premium"

    # ── CRT (Candle Range Theory) ─────────────────────────────────────────────
    crt_4h_high: float = 0.0   # Хай предыдущей 4H свечи
    crt_4h_low:  float = 0.0   # Лоу предыдущей 4H свечи
    crt_4h_mid:  float = 0.0   # Середина предыдущей 4H свечи (equilibrium CRT)
    crt_1d_high: float = 0.0   # Хай предыдущего дня (=PDH)
    crt_1d_low:  float = 0.0   # Лоу предыдущего дня (=PDL)
    crt_1d_mid:  float = 0.0   # Середина дня

    # ── HTF Market Structure (4H) ─────────────────────────────────────────────
    htf_structure: str = "unknown"   # "bullish" | "bearish" | "ranging"
    htf_bias:      str = "neutral"   # "long" | "short" | "neutral"
    htf_swing_h:   float = 0.0       # Последний структурный максимум (4H)
    htf_swing_l:   float = 0.0       # Последний структурный минимум (4H)

    # ── GAP ──────────────────────────────────────────────────────────────────
    daily_gap_pct:   float = 0.0    # % гэп от prev_close к curr_open (+ вверх, - вниз)
    weekly_gap_pct:  float = 0.0    # % гэп между неделями
    has_daily_gap:   bool  = False  # Значимый дневной гэп (>0.3%)
    has_weekly_gap:  bool  = False  # Значимый недельный гэп (>0.5%)

    # ── ATR на 30m ───────────────────────────────────────────────────────────
    atr_30m_pct: float = 0.0   # ATR(14) на 30m как % от цены

    # ── FVG на 4H / 1D ───────────────────────────────────────────────────────
    fvg_bearish_4h: Optional[Tuple[float, float]] = None   # (lower, upper) медвежий FVG
    fvg_bullish_4h: Optional[Tuple[float, float]] = None   # бычий FVG
    fvg_bearish_1d: Optional[Tuple[float, float]] = None
    fvg_bullish_1d: Optional[Tuple[float, float]] = None
    has_fvg_4h:     bool = False   # Есть незакрытый FVG на 4H
    has_fvg_1d:     bool = False   # Есть незакрытый FVG на 1D

    # ── Order Block на 4H / 1D ───────────────────────────────────────────────
    ob_bearish_4h: Optional[Tuple[float, float]] = None  # (low, high) медвежий OB
    ob_bullish_4h: Optional[Tuple[float, float]] = None
    ob_bearish_1d: Optional[Tuple[float, float]] = None
    ob_bullish_1d: Optional[Tuple[float, float]] = None
    has_ob_4h:     bool = False
    has_ob_1d:     bool = False

    # ── Ключевые уровни (для вывода) ─────────────────────────────────────────
    key_levels: List[Tuple[float, str]] = field(default_factory=list)
    # [(price, label), ...]  e.g. [(42000, "PDH"), (41500, "Fib 0.618"), ...]


# ─────────────────────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────────────────────────

def _atr14(candles: list) -> float:
    """ATR(14) от списка CandleData."""
    if len(candles) < 15:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        pc = candles[i - 1].close
        tr = max(candles[i].high - candles[i].low,
                 abs(candles[i].high - pc),
                 abs(candles[i].low - pc))
        trs.append(tr)
    return sum(trs[-14:]) / 14


def _swing_high_low(candles: list, lookback: int = 20) -> Tuple[float, float]:
    """Swing high и low за последние lookback свечей."""
    if not candles:
        return 0.0, 0.0
    slc = candles[-lookback:]
    return max(c.high for c in slc), min(c.low for c in slc)


def _calc_fib(swing_high: float, swing_low: float, tf: str = "4h") -> FibLevels:
    """Рассчитывает уровни Fibonacci Retracement и Extension."""
    if swing_high <= swing_low or swing_high == 0:
        return FibLevels(tf=tf)
    rng = swing_high - swing_low
    return FibLevels(
        swing_high = swing_high,
        swing_low  = swing_low,
        r_0   = swing_high,
        r_236 = swing_high - rng * 0.236,
        r_382 = swing_high - rng * 0.382,
        r_50  = swing_high - rng * 0.500,
        r_618 = swing_high - rng * 0.618,
        r_786 = swing_high - rng * 0.786,
        r_100 = swing_low,
        # Extension вверх от swing_low
        e_1272 = swing_low + rng * 1.272,
        e_1618 = swing_low + rng * 1.618,
        e_2000 = swing_low + rng * 2.000,
        tf     = tf,
    )


def _calc_poc(candles: list, lookback: int = 20) -> float:
    """
    Упрощённый Point of Control — свеча с максимальным объёмом.
    Возвращает типичную цену (H+L+C)/3 этой свечи.
    """
    if not candles:
        return 0.0
    slc = candles[-lookback:]
    best = max(slc, key=lambda c: getattr(c, "quote_volume", 0))
    return (best.high + best.low + best.close) / 3


def _zone(price: float, swing_high: float, swing_low: float) -> str:
    """ICT Discount/Equilibrium/Premium."""
    if swing_high <= swing_low or price <= 0:
        return "neutral"
    mid = (swing_high + swing_low) / 2
    if price > mid * 1.005:
        return "premium"
    if price < mid * 0.995:
        return "discount"
    return "equilibrium"


def _detect_htf_structure(candles_4h: list) -> Tuple[str, str, float, float]:
    """
    Определяет рыночную структуру по 4H свечам.
    Returns: (structure, bias, swing_high, swing_low)
    """
    if len(candles_4h) < 12:
        return "unknown", "neutral", 0.0, 0.0

    # Разбиваем на два блока по 6 свечей
    a = candles_4h[-12:-6]
    b = candles_4h[-6:]

    high_a = max(c.high for c in a)
    low_a  = min(c.low  for c in a)
    high_b = max(c.high for c in b)
    low_b  = min(c.low  for c in b)

    swing_h = max(high_a, high_b)
    swing_l = min(low_a,  low_b)

    # HH + HL = бычья структура
    if high_b > high_a and low_b > low_a:
        return "bullish", "long", swing_h, swing_l
    # LH + LL = медвежья структура
    if high_b < high_a and low_b < low_a:
        return "bearish", "short", swing_h, swing_l
    return "ranging", "neutral", swing_h, swing_l


def _detect_fvg(candles: list, price: float, lookback: int = 15) -> Tuple[Optional[Tuple], Optional[Tuple], bool, bool]:
    """
    Fair Value Gap на заданном ТФ.
    Returns: (bearish_fvg, bullish_fvg, has_bearish, has_bullish)
    """
    bear_fvg = bull_fvg = None
    if len(candles) < 3:
        return None, None, False, False

    slc = candles[-lookback:]
    for i in range(len(slc) - 2):
        # Медвежий FVG: low[i] > high[i+2]  (разрыв между свечами 0 и 2)
        if slc[i].low > slc[i + 2].high:
            gap_low   = slc[i + 2].high
            gap_high  = slc[i].low
            # Проверяем что ещё не закрылся
            filled = any(slc[j].high >= gap_high for j in range(i + 1, len(slc)))
            if not filled and price <= gap_high * 1.01:  # цена рядом
                bear_fvg = (gap_low, gap_high)
        # Бычий FVG: high[i+2] < low[i]  (разрыв снизу)
        if slc[i + 2].low > slc[i].high:
            gap_low  = slc[i].high
            gap_high = slc[i + 2].low
            filled = any(slc[j].low <= gap_low * 0.99 for j in range(i + 1, len(slc)))
            if not filled and price >= gap_low * 0.99:
                bull_fvg = (gap_low, gap_high)

    return bear_fvg, bull_fvg, bear_fvg is not None, bull_fvg is not None


def _detect_ob(candles: list, price: float, lookback: int = 20) -> Tuple[Optional[Tuple], Optional[Tuple], bool, bool]:
    """
    Order Block на заданном ТФ.
    Медвежий OB: последняя бычья свеча перед сильным снижением.
    Бычий OB: последняя медвежья свеча перед сильным ростом.
    Returns: (bearish_ob, bullish_ob, has_bearish, has_bullish)
    """
    bear_ob = bull_ob = None
    if len(candles) < 5:
        return None, None, False, False

    slc = candles[-lookback:]
    for i in range(len(slc) - 2):
        c = slc[i]
        # Медвежий OB: бычья свеча → потом падение
        if c.close > c.open:  # бычья
            drop = (c.high - slc[i + 2].low) / c.high * 100 if c.high > 0 else 0
            if drop > 1.5:  # падение > 1.5%
                ob_low  = c.open
                ob_high = c.high
                if price <= ob_high * 1.01:  # цена рядом
                    bear_ob = (ob_low, ob_high)
        # Бычий OB: медвежья свеча → потом рост
        if c.close < c.open:  # медвежья
            rise = (slc[i + 2].high - c.low) / c.low * 100 if c.low > 0 else 0
            if rise > 1.5:
                ob_low  = c.low
                ob_high = c.open
                if price >= ob_low * 0.99:
                    bull_ob = (ob_low, ob_high)

    return bear_ob, bull_ob, bear_ob is not None, bull_ob is not None


# ─────────────────────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────────────────────

def compute_market_structure(
    price:      float,
    klines_30m: list,
    klines_1h:  list,
    klines_4h:  list,
    klines_1d:  list,
) -> MarketStructureResult:
    """
    Вычисляет полную рыночную структуру из свечей нескольких ТФ.
    Вызывается из get_complete_market_data() единожды за запрос символа.
    """
    r = MarketStructureResult()

    # ── Наличие данных ────────────────────────────────────────────────────────
    r.has_30m = bool(klines_30m and len(klines_30m) >= 10)
    r.has_4h  = bool(klines_4h  and len(klines_4h)  >= 6)
    r.has_1d  = bool(klines_1d  and len(klines_1d)  >= 3)

    # ── ATR 30m ───────────────────────────────────────────────────────────────
    if r.has_30m:
        atr_raw = _atr14(klines_30m)
        r.atr_30m_pct = round(atr_raw / price * 100, 3) if price > 0 else 0.0

    # ── PDH / PDL / PWH / PWL / PMH / PML / ATH / ATL ──────────────────────
    if r.has_1d:
        # Previous Day (индекс -2 = вчера, -1 = текущий день)
        if len(klines_1d) >= 2:
            prev_day = klines_1d[-2]
            r.pdh = prev_day.high
            r.pdl = prev_day.low
            r.pdc = prev_day.close

            # CRT Daily = prev day range
            r.crt_1d_high = prev_day.high
            r.crt_1d_low  = prev_day.low
            r.crt_1d_mid  = (prev_day.high + prev_day.low) / 2

            # Daily GAP: prev_close → current_open
            curr_day = klines_1d[-1]
            if prev_day.close > 0:
                gap = (curr_day.open - prev_day.close) / prev_day.close * 100
                r.daily_gap_pct = round(gap, 3)
                r.has_daily_gap = abs(gap) >= 0.3

        # Previous Week (последние 7 дней = индексы -8:-1)
        if len(klines_1d) >= 8:
            week = klines_1d[-8:-1]
            r.pwh = max(c.high for c in week)
            r.pwl = min(c.low  for c in week)

            # Weekly GAP: две недели
            if len(klines_1d) >= 15:
                prev_week = klines_1d[-15:-8]
                pw_close  = prev_week[-1].close
                cw_open   = week[0].open
                if pw_close > 0:
                    wgap = (cw_open - pw_close) / pw_close * 100
                    r.weekly_gap_pct = round(wgap, 3)
                    r.has_weekly_gap = abs(wgap) >= 0.5

        # Previous Month (последние 30 дней)
        month = klines_1d[-31:-1] if len(klines_1d) >= 31 else klines_1d[:-1]
        if month:
            r.pmh = max(c.high for c in month)
            r.pml = min(c.low  for c in month)

        # ATH/ATL (все доступные 1D данные)
        r.ath_1d = max(c.high for c in klines_1d)
        r.atl_1d = min(c.low  for c in klines_1d)

        # POC 1D (за последние 14 дней)
        r.poc_1d = _calc_poc(klines_1d, 14)

        # Fibonacci от недельного swing (14 дней)
        wh, wl = _swing_high_low(klines_1d, 14)
        if wh > wl:
            r.fib_1d = _calc_fib(wh, wl, "1d")
            r.equilibrium_1d = r.fib_1d.r_50
            r.zone_1d = _zone(price, wh, wl)

    # ── 4H анализ ─────────────────────────────────────────────────────────────
    if r.has_4h:
        # POC 4H
        r.poc_4h = _calc_poc(klines_4h, 20)

        # CRT 4H: предыдущая 4H свеча
        if len(klines_4h) >= 2:
            prev_4h = klines_4h[-2]
            r.crt_4h_high = prev_4h.high
            r.crt_4h_low  = prev_4h.low
            r.crt_4h_mid  = (prev_4h.high + prev_4h.low) / 2

        # HTF Structure
        r.htf_structure, r.htf_bias, r.htf_swing_h, r.htf_swing_l = \
            _detect_htf_structure(klines_4h)

        # Fibonacci 4H (20 свечей = ~3-4 дня)
        sh4, sl4 = _swing_high_low(klines_4h, 20)
        if sh4 > sl4:
            r.fib_4h = _calc_fib(sh4, sl4, "4h")
            r.equilibrium_4h = r.fib_4h.r_50
            r.zone_4h = _zone(price, sh4, sl4)

        # FVG 4H
        r.fvg_bearish_4h, r.fvg_bullish_4h, bear_fvg_b, bull_fvg_b = \
            _detect_fvg(klines_4h, price, 15)
        r.has_fvg_4h = bear_fvg_b or bull_fvg_b

        # OB 4H
        r.ob_bearish_4h, r.ob_bullish_4h, bear_ob_b, bull_ob_b = \
            _detect_ob(klines_4h, price, 20)
        r.has_ob_4h = bear_ob_b or bull_ob_b

    # ── Daily FVG/OB ─────────────────────────────────────────────────────────
    if r.has_1d and len(klines_1d) >= 5:
        r.fvg_bearish_1d, r.fvg_bullish_1d, bear_fvg_d, bull_fvg_d = \
            _detect_fvg(klines_1d, price, 10)
        r.has_fvg_1d = bear_fvg_d or bull_fvg_d

        r.ob_bearish_1d, r.ob_bullish_1d, bear_ob_d, bull_ob_d = \
            _detect_ob(klines_1d, price, 14)
        r.has_ob_1d = bear_ob_d or bull_ob_d

    # ── Ключевые уровни (для логов) ───────────────────────────────────────────
    levels: List[Tuple[float, str]] = []
    if r.pdh:  levels.append((r.pdh, "PDH"))
    if r.pdl:  levels.append((r.pdl, "PDL"))
    if r.pwh:  levels.append((r.pwh, "PWH"))
    if r.pwl:  levels.append((r.pwl, "PWL"))
    if r.poc_4h: levels.append((r.poc_4h, "POC 4H"))
    if r.poc_1d: levels.append((r.poc_1d, "POC 1D"))
    if r.fib_4h:
        levels += [
            (r.fib_4h.r_618, "Fib 0.618 (4H)"),
            (r.fib_4h.r_50,  "Fib 0.5 EQ (4H)"),
            (r.fib_4h.r_382, "Fib 0.382 (4H)"),
        ]
    if r.crt_4h_high: levels.append((r.crt_4h_high, "CRT 4H High"))
    if r.crt_4h_low:  levels.append((r.crt_4h_low,  "CRT 4H Low"))
    if r.ath_1d: levels.append((r.ath_1d, "ATH (1D data)"))
    if r.atl_1d: levels.append((r.atl_1d, "ATL (1D data)"))

    # Сортируем по близости к текущей цене (ближайшие сверху)
    if price > 0:
        levels.sort(key=lambda x: abs(x[0] - price))

    r.key_levels = levels[:10]  # топ-10 ближайших

    return r


def proximity_bonus(price: float, ms: MarketStructureResult, direction: str) -> Tuple[int, List[str]]:
    """
    Рассчитывает бонус к score на основе рыночной структуры.
    direction: "long" | "short"
    Returns: (bonus_points, reasons)
    """
    bonus = 0
    reasons = []
    if price <= 0:
        return 0, []

    def _near(level: float, pct: float = 0.5) -> bool:
        """Цена в pct% от уровня."""
        return level > 0 and abs(price - level) / price * 100 <= pct

    def _above(level: float) -> bool:
        return level > 0 and price > level

    def _below(level: float) -> bool:
        return level > 0 and price < level

    if direction == "long":
        # ── LONG бонусы ───────────────────────────────────────────────────────

        # Discount zone (цена в зоне скидки) = хорошо для лонга
        if ms.zone_4h == "discount":
            bonus += 8; reasons.append("4H Discount Zone +8")
        elif ms.zone_4h == "equilibrium":
            bonus += 4; reasons.append("4H Equilibrium +4")

        if ms.zone_1d == "discount":
            bonus += 6; reasons.append("1D Discount Zone +6")

        # HTF bullish structure
        if ms.htf_structure == "bullish":
            bonus += 10; reasons.append("HTF 4H Bullish Structure +10")
        elif ms.htf_structure == "ranging":
            bonus += 2; reasons.append("HTF 4H Ranging +2")

        # Цена у PDL (поддержка)
        if _near(ms.pdl, 0.8):
            bonus += 7; reasons.append(f"Near PDL {ms.pdl:.4f} +7")

        # Цена у Fib 0.618 (golden pocket) ±0.5%
        if ms.fib_4h and _near(ms.fib_4h.r_618, 0.5):
            bonus += 8; reasons.append("Near 4H Fib 0.618 (golden pocket) +8")
        if ms.fib_4h and _near(ms.fib_4h.r_50, 0.4):
            bonus += 5; reasons.append("At 4H Fib 0.5 EQ +5")
        if ms.fib_4h and _near(ms.fib_4h.r_786, 0.5):
            bonus += 6; reasons.append("Near 4H Fib 0.786 +6")

        # CRT 4H Low (поддержка)
        if _near(ms.crt_4h_low, 0.7):
            bonus += 5; reasons.append("Near CRT 4H Low +5")

        # POC как магнит снизу
        if ms.poc_4h > 0 and price < ms.poc_4h and _near(ms.poc_4h, 1.0):
            bonus += 4; reasons.append("Below POC 4H (magnetic) +4")

        # Bullish OB 4H / 1D
        if ms.has_ob_4h and ms.ob_bullish_4h:
            low, high = ms.ob_bullish_4h
            if low <= price <= high * 1.01:
                bonus += 10; reasons.append("In Bullish OB 4H +10")

        # Bullish FVG 4H
        if ms.has_fvg_4h and ms.fvg_bullish_4h:
            low, high = ms.fvg_bullish_4h
            if low <= price <= high:
                bonus += 8; reasons.append("In Bullish FVG 4H +8")

        # Gap fill potential (цена ниже дневного гэпа вверх)
        if ms.has_daily_gap and ms.daily_gap_pct > 0:
            bonus += 3; reasons.append("Daily Gap Up above (fill potential) +3")

        # Штрафы
        if ms.zone_4h == "premium":
            bonus -= 8; reasons.append("4H Premium Zone -8 (bad long entry)")
        if ms.htf_structure == "bearish":
            bonus -= 10; reasons.append("HTF 4H Bearish -10 (counter-trend long)")
        if _near(ms.pdh, 0.5):
            bonus -= 5; reasons.append("Near PDH (resistance) -5")

    else:  # direction == "short"
        # ── SHORT бонусы ──────────────────────────────────────────────────────

        # Premium zone = хорошо для шорта
        if ms.zone_4h == "premium":
            bonus += 8; reasons.append("4H Premium Zone +8")
        elif ms.zone_4h == "equilibrium":
            bonus += 3; reasons.append("4H Equilibrium +3")

        if ms.zone_1d == "premium":
            bonus += 6; reasons.append("1D Premium Zone +6")

        # HTF bearish structure
        if ms.htf_structure == "bearish":
            bonus += 10; reasons.append("HTF 4H Bearish Structure +10")

        # Цена у PDH (сопротивление)
        if _near(ms.pdh, 0.8):
            bonus += 7; reasons.append(f"Near PDH {ms.pdh:.4f} +7")

        # Fib 0.618 = golden pocket для шорта от пампа
        if ms.fib_4h and _near(ms.fib_4h.r_618, 0.5):
            bonus += 8; reasons.append("4H Fib 0.618 SHORT golden pocket +8")
        if ms.fib_4h and _near(ms.fib_4h.r_50, 0.4):
            bonus += 5; reasons.append("4H Fib 0.5 EQ SHORT +5")
        if ms.fib_4h and _near(ms.fib_4h.r_236, 0.5):
            bonus += 5; reasons.append("4H Fib 0.236 SHORT +5")

        # CRT 4H High (сопротивление)
        if _near(ms.crt_4h_high, 0.7):
            bonus += 5; reasons.append("Near CRT 4H High +5")

        # Bearish OB 4H
        if ms.has_ob_4h and ms.ob_bearish_4h:
            low, high = ms.ob_bearish_4h
            if low <= price <= high * 1.01:
                bonus += 10; reasons.append("In Bearish OB 4H +10")

        # Bearish FVG 4H
        if ms.has_fvg_4h and ms.fvg_bearish_4h:
            low, high = ms.fvg_bearish_4h
            if low <= price <= high:
                bonus += 8; reasons.append("In Bearish FVG 4H +8")

        # Daily gap down → шорт momentum
        if ms.has_daily_gap and ms.daily_gap_pct < 0:
            bonus += 4; reasons.append("Daily Gap Down +4 (short momentum)")

        # Штрафы
        if ms.zone_4h == "discount":
            bonus -= 8; reasons.append("4H Discount Zone -8 (bad short entry)")
        if ms.htf_structure == "bullish":
            bonus -= 10; reasons.append("HTF 4H Bullish -10 (counter-trend short)")
        if _near(ms.pdl, 0.5):
            bonus -= 5; reasons.append("Near PDL (support) -5")

    return bonus, reasons


def format_ms_summary(ms: MarketStructureResult) -> str:
    """Краткое текстовое резюме рыночной структуры для логов."""
    parts = []
    if ms.pdh:
        parts.append(f"PDH={ms.pdh:.4f} PDL={ms.pdl:.4f}")
    if ms.htf_structure != "unknown":
        parts.append(f"HTF={ms.htf_structure.upper()} ({ms.htf_bias})")
    if ms.zone_4h != "neutral":
        parts.append(f"Zone={ms.zone_4h.upper()}")
    if ms.fib_4h:
        parts.append(f"Fib0.618={ms.fib_4h.r_618:.4f} EQ={ms.fib_4h.r_50:.4f}")
    if ms.has_daily_gap:
        parts.append(f"DailyGap={ms.daily_gap_pct:+.2f}%")
    if ms.poc_4h:
        parts.append(f"POC4H={ms.poc_4h:.4f}")
    return " | ".join(parts) if parts else "no structure data"
