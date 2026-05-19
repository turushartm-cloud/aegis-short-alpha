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

    # ── CME / Weekend GAP ─────────────────────────────────────────────────────
    # Только для BTC и ETH. CME закрыт Пт 22:00 UTC → Вс 23:00 UTC
    cme_gap_pct:     float = 0.0    # % разрыв CME (Friday close → Monday open)
    cme_gap_low:     float = 0.0    # Нижняя граница CME gap
    cme_gap_high:    float = 0.0    # Верхняя граница CME gap
    has_cme_gap:     bool  = False  # Есть незакрытый CME gap (>0.5%)
    cme_gap_dir:     str   = "none" # "up" | "down" | "none"

    # ── Pivot Points (Floor Pivots) ───────────────────────────────────────────
    pivot_pp:  float = 0.0   # PP  = (H + L + C) / 3
    pivot_r1:  float = 0.0   # R1  = 2*PP - L
    pivot_r2:  float = 0.0   # R2  = PP + (H - L)
    pivot_r3:  float = 0.0   # R3  = H + 2*(PP - L)
    pivot_s1:  float = 0.0   # S1  = 2*PP - H
    pivot_s2:  float = 0.0   # S2  = PP - (H - L)
    pivot_s3:  float = 0.0   # S3  = L - 2*(H - PP)

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

    # ── Weekly (1W) SNR / OB / FVG / Zone ────────────────────────────────────
    has_1w:           bool  = False
    zone_weekly:      str   = "neutral"   # "discount" | "equilibrium" | "premium"
    equilibrium_1w:   float = 0.0
    fvg_bearish_1w:   Optional[Tuple[float, float]] = None
    fvg_bullish_1w:   Optional[Tuple[float, float]] = None
    has_fvg_1w:       bool  = False
    ob_bearish_1w:    Optional[Tuple[float, float]] = None
    ob_bullish_1w:    Optional[Tuple[float, float]] = None
    has_ob_1w:        bool  = False
    poc_1w:           float = 0.0
    fib_1w:           Optional[FibLevels] = None
    htf_structure_1w: str   = "unknown"
    htf_bias_1w:      str   = "neutral"

    # ── Monthly (1M) levels ───────────────────────────────────────────────────
    has_1M:           bool  = False
    zone_monthly:     str   = "neutral"
    poc_1M:           float = 0.0
    month_high:       float = 0.0   # high текущего / последнего месяца
    month_low:        float = 0.0

    # ── Confluence score ──────────────────────────────────────────────────────
    confluence_short: int   = 0   # кол-во совпавших медвежьих сигналов (OB+FVG+SNR+zone)
    confluence_long:  int   = 0   # кол-во совпавших бычьих сигналов

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


def _detect_fvg(
    candles: list,
    price: float,
    lookback: int = 15,
    prox_pct: float = 1.5,
) -> Tuple[Optional[Tuple], Optional[Tuple], bool, bool]:
    """
    Fair Value Gap на заданном ТФ.
    prox_pct: % близости цены к gap (4H=1.5, 1W=8.0, 1M=15.0)
    Returns: (bearish_fvg, bullish_fvg, has_bearish, has_bullish)
    """
    bear_fvg = bull_fvg = None
    if len(candles) < 3:
        return None, None, False, False

    slc = candles[-lookback:]
    _prox = 1 + prox_pct / 100
    _prox_inv = 1 - prox_pct / 100
    for i in range(len(slc) - 2):
        # Медвежий FVG: low[i] > high[i+2]
        if slc[i].low > slc[i + 2].high:
            gap_low   = slc[i + 2].high
            gap_high  = slc[i].low
            filled = any(slc[j].high >= gap_high for j in range(i + 1, len(slc)))
            if not filled and price <= gap_high * _prox:
                bear_fvg = (gap_low, gap_high)
        # Бычий FVG: high[i+2] < low[i]
        if slc[i + 2].low > slc[i].high:
            gap_low  = slc[i].high
            gap_high = slc[i + 2].low
            filled = any(slc[j].low <= gap_low * _prox_inv for j in range(i + 1, len(slc)))
            if not filled and price >= gap_low * _prox_inv:
                bull_fvg = (gap_low, gap_high)

    return bear_fvg, bull_fvg, bear_fvg is not None, bull_fvg is not None


def _detect_ob(
    candles: list,
    price: float,
    lookback: int = 20,
    drop_threshold: float = 1.5,
    prox_pct: float = 1.5,
) -> Tuple[Optional[Tuple], Optional[Tuple], bool, bool]:
    """
    Order Block на заданном ТФ.
    drop_threshold: минимальное движение после OB свечи (4H=1.5%, 1W=4.0%)
    prox_pct: % близости цены к OB (4H=1.5%, 1W=5.0%)
    Returns: (bearish_ob, bullish_ob, has_bearish, has_bullish)
    """
    bear_ob = bull_ob = None
    if len(candles) < 5:
        return None, None, False, False

    slc = candles[-lookback:]
    _prox_up  = 1 + prox_pct / 100
    _prox_dn  = 1 - prox_pct / 100
    for i in range(len(slc) - 2):
        c = slc[i]
        # Медвежий OB: бычья свеча → потом падение
        if c.close > c.open:
            drop = (c.high - slc[i + 2].low) / c.high * 100 if c.high > 0 else 0
            if drop > drop_threshold:
                ob_low  = c.open
                ob_high = c.high
                if price <= ob_high * _prox_up:
                    bear_ob = (ob_low, ob_high)
        # Бычий OB: медвежья свеча → потом рост
        if c.close < c.open:
            rise = (slc[i + 2].high - c.low) / c.low * 100 if c.low > 0 else 0
            if rise > drop_threshold:
                ob_low  = c.low
                ob_high = c.open
                if price >= ob_low * _prox_dn:
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
    klines_1w:  Optional[list] = None,
    klines_1M:  Optional[list] = None,
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

            # ✅ FIX: Если цена далеко выше PDH (>5%), монета в параболе
            # Помечаем как parabolic чтобы скорер мог блокировать SHORT
            if price > 0 and r.pdh > 0 and price > r.pdh * 1.05:
                # Пересчитываем PDH от последних 3 дней для актуальности
                recent_days = klines_1d[-3:]
                r.pdh = max(c.high for c in recent_days)
                r.pdl = min(c.low for c in recent_days)

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

    # ── Weekly (1W) SNR / OB / FVG / Zone ─────────────────────────────────────
    _kw = klines_1w or []
    r.has_1w = len(_kw) >= 4
    if r.has_1w:
        _w_lookback = min(20, len(_kw))
        sh_1w, sl_1w = _swing_high_low(_kw, _w_lookback)
        if sh_1w > sl_1w:
            r.fib_1w        = _calc_fib(sh_1w, sl_1w, "1w")
            r.equilibrium_1w = r.fib_1w.r_50
            r.zone_weekly   = _zone(price, sh_1w, sl_1w)
        r.poc_1w = _calc_poc(_kw, min(10, len(_kw)))
        if len(_kw) >= 12:
            r.htf_structure_1w, r.htf_bias_1w, _, _ = _detect_htf_structure(_kw)
        if len(_kw) >= 5:
            # Weekly: broader proximity (8%) and larger move thresholds (4%)
            r.fvg_bearish_1w, r.fvg_bullish_1w, _bfw, _bulfw = \
                _detect_fvg(_kw, price, min(10, len(_kw)), prox_pct=8.0)
            r.has_fvg_1w = _bfw or _bulfw
            r.ob_bearish_1w, r.ob_bullish_1w, _bow, _bulow = \
                _detect_ob(_kw, price, min(12, len(_kw)), drop_threshold=4.0, prox_pct=5.0)
            r.has_ob_1w = _bow or _bulow

    # ── Monthly (1M) levels ────────────────────────────────────────────────────
    _km = klines_1M or []
    r.has_1M = len(_km) >= 2
    if r.has_1M:
        sh_1M, sl_1M = _swing_high_low(_km, min(6, len(_km)))
        r.month_high  = sh_1M
        r.month_low   = sl_1M
        r.poc_1M      = _calc_poc(_km, min(6, len(_km)))
        if sh_1M > sl_1M:
            r.zone_monthly = _zone(price, sh_1M, sl_1M)

    # ── Confluence score ───────────────────────────────────────────────────────
    _cs = 0  # short
    _cl = 0  # long
    # 4H signals
    if r.has_ob_4h and r.ob_bearish_4h:
        lo, hi = r.ob_bearish_4h
        if lo <= price <= hi * 1.02: _cs += 1
    if r.has_fvg_4h and r.fvg_bearish_4h:
        lo, hi = r.fvg_bearish_4h
        if lo <= price <= hi * 1.01: _cs += 1
    if r.has_ob_4h and r.ob_bullish_4h:
        lo, hi = r.ob_bullish_4h
        if lo * 0.98 <= price <= hi: _cl += 1
    if r.has_fvg_4h and r.fvg_bullish_4h:
        lo, hi = r.fvg_bullish_4h
        if lo * 0.99 <= price <= hi: _cl += 1
    # Weekly signals
    if r.has_ob_1w and r.ob_bearish_1w:
        lo, hi = r.ob_bearish_1w
        if lo <= price <= hi * 1.02: _cs += 1
    if r.has_fvg_1w and r.fvg_bearish_1w:
        lo, hi = r.fvg_bearish_1w
        if lo <= price <= hi * 1.01: _cs += 1
    if r.has_ob_1w and r.ob_bullish_1w:
        lo, hi = r.ob_bullish_1w
        if lo * 0.98 <= price <= hi: _cl += 1
    if r.has_fvg_1w and r.fvg_bullish_1w:
        lo, hi = r.fvg_bullish_1w
        if lo * 0.99 <= price <= hi: _cl += 1
    # Zone signals
    if r.zone_4h == "premium":    _cs += 1
    if r.zone_weekly == "premium": _cs += 1
    if r.zone_4h == "discount":   _cl += 1
    if r.zone_weekly == "discount": _cl += 1
    r.confluence_short = _cs
    r.confluence_long  = _cl

    # ── Ключевые уровни (для логов) ───────────────────────────────────────────
    # ── Pivot Points (Floor Pivots от Previous Day) ────────────────────────────────
    if r.pdh > 0 and r.pdl > 0 and r.pdc > 0:
        H, L, C = r.pdh, r.pdl, r.pdc
        pp = (H + L + C) / 3
        r.pivot_pp = round(pp, 8)
        r.pivot_r1 = round(2 * pp - L, 8)
        r.pivot_r2 = round(pp + (H - L), 8)
        r.pivot_r3 = round(H + 2 * (pp - L), 8)
        r.pivot_s1 = round(2 * pp - H, 8)
        r.pivot_s2 = round(pp - (H - L), 8)
        r.pivot_s3 = round(L - 2 * (H - pp), 8)

    # ── CME / Session GAP (только Пт→Вс UTC, т.е. после Friday close) ───────
    # CME закрыт: пятница ~22:00 UTC → воскресенье ~23:00 UTC
    # Гэп образуется между закрытием пятницы и открытием понедельника.
    # Поэтому ищем только свечу с timestamp понедельника (weekday==0) или
    # пятница→субботу/воскресенье (weekday 4→5→6).
    if r.has_1d and len(klines_1d) >= 4:
        import datetime as _dt_module
        for _i in range(len(klines_1d) - 1, max(0, len(klines_1d) - 8), -1):
            _pc = klines_1d[_i - 1]
            _cc = klines_1d[_i]
            if _pc.close <= 0:
                continue
            _gap = (_cc.open - _pc.close) / _pc.close * 100
            if abs(_gap) < 0.3:
                continue
            # Weekday filter: гэп считается CME только если одна из сторон — Пт/Сб/Вс
            _is_cme = True  # дефолт: принимаем (для не-дневных или нет timestamp)
            _ts = getattr(_cc, "open_time", None) or getattr(_cc, "timestamp", None)
            if _ts:
                try:
                    if isinstance(_ts, (int, float)):
                        _dow = _dt_module.datetime.utcfromtimestamp(_ts / 1000 if _ts > 1e10 else _ts).weekday()
                    else:
                        _dow = _dt_module.datetime.fromisoformat(str(_ts)).weekday()
                    # 0=Пн, 4=Пт, 5=Сб, 6=Вс
                    # CME gap: открытие Пн (0) после выходных, или сама Пт/Сб/Вс
                    _is_cme = _dow in (0, 4, 5, 6)
                except Exception:
                    pass
            if _is_cme:
                r.cme_gap_pct = round(_gap, 3)
                r.has_cme_gap = True
                r.cme_gap_dir = "up" if _gap > 0 else "down"
                if _gap > 0:
                    r.cme_gap_low, r.cme_gap_high = _pc.close, _cc.open
                else:
                    r.cme_gap_low, r.cme_gap_high = _cc.open, _pc.close
                break

    levels: List[Tuple[float, str]] = []
    if r.pdh:      levels.append((r.pdh, "PDH"))
    if r.pdl:      levels.append((r.pdl, "PDL"))
    if r.pivot_pp: levels.append((r.pivot_pp, "PP (Pivot)"))
    if r.pivot_r1: levels.append((r.pivot_r1, "R1"))
    if r.pivot_s1: levels.append((r.pivot_s1, "S1"))
    if r.pivot_r2: levels.append((r.pivot_r2, "R2"))
    if r.pivot_s2: levels.append((r.pivot_s2, "S2"))
    if r.pwh:  levels.append((r.pwh, "PWH"))
    if r.pwl:  levels.append((r.pwl, "PWL"))
    if r.poc_4h: levels.append((r.poc_4h, "POC 4H"))
    if r.poc_1d: levels.append((r.poc_1d, "POC 1D"))
    if r.poc_1w: levels.append((r.poc_1w, "POC 1W"))
    if r.poc_1M: levels.append((r.poc_1M, "POC 1M"))
    if r.equilibrium_1w: levels.append((r.equilibrium_1w, "EQ Weekly"))
    if r.ob_bearish_1w:  levels.append((r.ob_bearish_1w[1], "OB Bear 1W"))
    if r.ob_bullish_1w:  levels.append((r.ob_bullish_1w[0], "OB Bull 1W"))
    if r.fvg_bearish_1w: levels.append((r.fvg_bearish_1w[0], "FVG Bear 1W"))
    if r.fvg_bullish_1w: levels.append((r.fvg_bullish_1w[1], "FVG Bull 1W"))
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

        # Pivot Points — S1/S2 как поддержки для LONG
        if _near(ms.pivot_s1, 0.5):
            bonus += 5; reasons.append(f"Near Pivot S1={ms.pivot_s1:.4f} +5")
        if _near(ms.pivot_pp, 0.4):
            bonus += 3; reasons.append(f"At Pivot PP={ms.pivot_pp:.4f} +3")
        # CME Gap fill potential
        if ms.has_cme_gap and ms.cme_gap_dir == "up" and price > ms.cme_gap_low and price < ms.cme_gap_high:
            bonus += 6; reasons.append(f"In CME Gap Up {ms.cme_gap_pct:+.2f}% +6")

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

        # POC 4H как магнит снизу
        if ms.poc_4h > 0 and price < ms.poc_4h and _near(ms.poc_4h, 1.0):
            bonus += 4; reasons.append("Below POC 4H (magnetic) +4")
        elif ms.poc_4h > 0 and price > ms.poc_4h * 1.005:
            bonus -= 3; reasons.append("Above POC 4H (extended, bad long entry) -3")

        # POC 1D — дневной уровень справедливой стоимости
        if ms.poc_1d > 0 and price < ms.poc_1d and _near(ms.poc_1d, 1.5):
            bonus += 5; reasons.append(f"Below POC 1D (daily value) +5")
        elif ms.poc_1d > 0 and price > ms.poc_1d * 1.01:
            bonus -= 2; reasons.append("Above POC 1D (premium, bad long) -2")

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

        # ── Weekly SNR / OB / FVG (КРИТИЧНО для лонга!) ───────────────────────
        if ms.zone_weekly == "discount":
            bonus += 12; reasons.append("Weekly DISCOUNT Zone +12 (ideal long zone)")
        elif ms.zone_weekly == "equilibrium":
            bonus += 4; reasons.append("Weekly EQ Zone +4")
        elif ms.zone_weekly == "premium":
            bonus -= 10; reasons.append("Weekly PREMIUM -10 (bad long entry)")

        if ms.has_ob_1w and ms.ob_bullish_1w:
            lo, hi = ms.ob_bullish_1w
            if lo * 0.99 <= price <= hi:
                bonus += 15; reasons.append(f"In Bullish OB Weekly {lo:.4f}–{hi:.4f} +15 🟢")
            elif _near(lo, 1.5):
                bonus += 8; reasons.append(f"Near Bullish OB Weekly {lo:.4f} +8")

        if ms.has_fvg_1w and ms.fvg_bullish_1w:
            lo, hi = ms.fvg_bullish_1w
            if lo * 0.99 <= price <= hi:
                bonus += 12; reasons.append(f"In Bullish FVG Weekly {lo:.4f}–{hi:.4f} +12 🟢")

        if ms.has_ob_1w and ms.ob_bearish_1w:
            lo, hi = ms.ob_bearish_1w
            if lo <= price <= hi:
                bonus -= 15; reasons.append(f"In Bearish OB Weekly {lo:.4f}–{hi:.4f} -15 (sell wall)")

        if ms.poc_1w > 0:
            if price < ms.poc_1w * 0.99:
                bonus += 6; reasons.append(f"Below POC 1W={ms.poc_1w:.4f} (weekly discount) +6")
            elif _near(ms.poc_1w, 0.8):
                bonus += 2; reasons.append(f"At POC 1W={ms.poc_1w:.4f} +2")
            elif price > ms.poc_1w * 1.01:
                bonus -= 5; reasons.append(f"Above POC 1W={ms.poc_1w:.4f} (extended, bad long) -5")

        if ms.htf_structure_1w == "bullish":
            bonus += 8; reasons.append("Weekly HTF Bullish Structure +8")
        elif ms.htf_structure_1w == "bearish":
            bonus -= 10; reasons.append("Weekly HTF Bearish Structure -10")

        if ms.zone_monthly == "discount":
            bonus += 5; reasons.append("Monthly DISCOUNT +5 (macro buy zone)")
        elif ms.zone_monthly == "premium":
            bonus -= 5; reasons.append("Monthly PREMIUM -5 (macro sell zone)")

        if ms.confluence_long >= 4:
            bonus += 15; reasons.append(f"Confluence LONG x{ms.confluence_long} +15 🎯")
        elif ms.confluence_long >= 3:
            bonus += 10; reasons.append(f"Confluence LONG x{ms.confluence_long} +10")
        elif ms.confluence_long >= 2:
            bonus += 5; reasons.append(f"Confluence LONG x{ms.confluence_long} +5")

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

        # Pivot Points — R1/R2 как сопротивление для SHORT
        if _near(ms.pivot_r1, 0.5):
            bonus += 5; reasons.append(f"Near Pivot R1={ms.pivot_r1:.4f} +5")
        if _near(ms.pivot_pp, 0.4):
            bonus += 3; reasons.append(f"At Pivot PP={ms.pivot_pp:.4f} +3")
        # CME Gap fill potential  
        if ms.has_cme_gap and ms.cme_gap_dir == "down" and price > ms.cme_gap_low and price < ms.cme_gap_high:
            bonus += 6; reasons.append(f"In CME Gap Down {ms.cme_gap_pct:+.2f}% +6")

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

        # POC 4H — цена выше POC = в зоне перекупленности (шорт из хаёв)
        if ms.poc_4h > 0 and price > ms.poc_4h * 1.005:
            bonus += 5; reasons.append(f"Above POC 4H (extended, short zone) +5")
        elif ms.poc_4h > 0 and _near(ms.poc_4h, 0.5):
            bonus += 2; reasons.append(f"At POC 4H (fair value) +2")
        elif ms.poc_4h > 0 and price < ms.poc_4h * 0.995:
            bonus -= 3; reasons.append("Below POC 4H (discount, risky short) -3")

        # POC 1D — цена выше дневного POC = хорошее место для шорта
        if ms.poc_1d > 0 and price > ms.poc_1d * 1.01:
            bonus += 4; reasons.append(f"Above POC 1D (daily premium) +4")
        elif ms.poc_1d > 0 and _near(ms.poc_1d, 1.0):
            bonus += 1; reasons.append(f"Near POC 1D +1")
        elif ms.poc_1d > 0 and price < ms.poc_1d * 0.99:
            bonus -= 2; reasons.append("Below POC 1D (discount, bad short) -2")

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

        # ── Weekly SNR / OB / FVG (КРИТИЧНО для шорта!) ───────────────────────
        # Weekly zone: PREMIUM = сильная зона для шорта, DISCOUNT = запрет
        if ms.zone_weekly == "premium":
            bonus += 12; reasons.append("Weekly PREMIUM Zone +12 (ideal short zone)")
        elif ms.zone_weekly == "equilibrium":
            bonus += 4; reasons.append("Weekly EQ Zone +4")
        elif ms.zone_weekly == "discount":
            bonus -= 20; reasons.append("Weekly DISCOUNT Zone -20 (institutional BUY zone — SHORT ЗАПРЕЩЁН)")

        # Bearish OB Weekly = сильнейшее сопротивление
        if ms.has_ob_1w and ms.ob_bearish_1w:
            lo, hi = ms.ob_bearish_1w
            if lo <= price <= hi * 1.01:
                bonus += 15; reasons.append(f"In Bearish OB Weekly {lo:.4f}–{hi:.4f} +15 🔴")
            elif _near(hi, 1.5):
                bonus += 8; reasons.append(f"Near Bearish OB Weekly top {hi:.4f} +8")

        # Bearish FVG Weekly = незакрытый медвежий дисбаланс на неделе
        if ms.has_fvg_1w and ms.fvg_bearish_1w:
            lo, hi = ms.fvg_bearish_1w
            if lo <= price <= hi:
                bonus += 12; reasons.append(f"In Bearish FVG Weekly {lo:.4f}–{hi:.4f} +12 🔴")

        # Bullish OB Weekly под ценой = стена покупок снизу (плохо для шорта)
        if ms.has_ob_1w and ms.ob_bullish_1w:
            lo, hi = ms.ob_bullish_1w
            if price > hi and _near(hi, 2.0):
                bonus -= 12; reasons.append(f"Bullish OB Weekly at {hi:.4f} below price -12 (buy wall)")
            elif lo <= price <= hi:
                bonus -= 20; reasons.append(f"In Bullish OB Weekly {lo:.4f}–{hi:.4f} -20 (институционалы покупают)")

        # POC Weekly — уровень наибольшего объёма за неделю
        if ms.poc_1w > 0:
            if price > ms.poc_1w * 1.01:
                bonus += 6; reasons.append(f"Above POC 1W={ms.poc_1w:.4f} (weekly premium) +6")
            elif _near(ms.poc_1w, 0.8):
                bonus += 2; reasons.append(f"At POC 1W={ms.poc_1w:.4f} +2")
            elif price < ms.poc_1w * 0.99:
                bonus -= 8; reasons.append(f"Below POC 1W={ms.poc_1w:.4f} (weekly discount, bad short) -8")

        # HTF Weekly bearish structure
        if ms.htf_structure_1w == "bearish":
            bonus += 8; reasons.append("Weekly HTF Bearish Structure +8")
        elif ms.htf_structure_1w == "bullish":
            bonus -= 10; reasons.append("Weekly HTF Bullish Structure -10 (counter-trend)")

        # Monthly zone penalty (если цена в месячном дискаунте — SHORT под угрозой)
        if ms.zone_monthly == "discount":
            bonus -= 8; reasons.append("Monthly DISCOUNT -8 (macro buy zone)")
        elif ms.zone_monthly == "premium":
            bonus += 5; reasons.append("Monthly PREMIUM +5")

        # Confluence multiplier: 3+ совпавших сигнала = максимальная уверенность
        if ms.confluence_short >= 4:
            bonus += 15; reasons.append(f"Confluence SHORT x{ms.confluence_short} +15 🎯")
        elif ms.confluence_short >= 3:
            bonus += 10; reasons.append(f"Confluence SHORT x{ms.confluence_short} +10")
        elif ms.confluence_short >= 2:
            bonus += 5; reasons.append(f"Confluence SHORT x{ms.confluence_short} +5")

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
        parts.append(f"Zone4H={ms.zone_4h.upper()}")
    if ms.zone_weekly != "neutral":
        parts.append(f"ZoneW={ms.zone_weekly.upper()}")
    if ms.zone_monthly != "neutral":
        parts.append(f"ZoneM={ms.zone_monthly.upper()}")
    if ms.fib_4h:
        parts.append(f"Fib0.618={ms.fib_4h.r_618:.4f} EQ={ms.fib_4h.r_50:.4f}")
    if ms.has_daily_gap:
        parts.append(f"DailyGap={ms.daily_gap_pct:+.2f}%")
    if ms.has_cme_gap:
        parts.append(f"CMEGap={ms.cme_gap_pct:+.2f}% ({ms.cme_gap_dir})")
    if ms.pivot_pp:
        parts.append(f"PP={ms.pivot_pp:.4f} R1={ms.pivot_r1:.4f} S1={ms.pivot_s1:.4f}")
    if ms.poc_4h:
        parts.append(f"POC4H={ms.poc_4h:.4f}")
    if ms.poc_1w:
        parts.append(f"POC1W={ms.poc_1w:.4f}")
    if ms.confluence_short > 1:
        parts.append(f"ConfShort={ms.confluence_short}")
    if ms.confluence_long > 1:
        parts.append(f"ConfLong={ms.confluence_long}")
    return " | ".join(parts) if parts else "no structure data"


# ─────────────────────────────────────────────────────────────────────────────
# CASCADE STRATEGY: 4H Fractal Raid → 1H SNR → 15M FVG
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CascadeSignal:
    """
    Каскадный сигнал: 4H Fractal Raid → 1H SNR → 15M FVG
    
    Логика:
    1. 4H Fractal Raid — цена пробивает ключевой фрактальный уровень на 4H
       (stop hunt: ликвидация стопов под swing low / над swing high)
    2. 1H SNR zone — формируется зона Support/Resistance после рейда на 1H
       (новый уровень = место откуда начался манипулятивный выход)
    3. 15M FVG — цена входит в Fair Value Gap на 15m внутри SNR-зоны
       (точный вход с минимальным риском)
    
    Daily FVG manipulation:
    - Daily FVG = дисбаланс на дневном графике (незакрытый gap)
    - Манипуляция = цена "заходит" в Daily FVG чтобы забрать ликвидность
    - После манипуляции → возврат → вход от 1H FVG внутри дневного диапазона
    """
    # Наличие сигнала
    has_signal:        bool  = False
    direction:         str   = "none"   # "long" | "short" | "none"
    confidence:        float = 0.0      # 0.0 - 1.0
    score_bonus:       int   = 0        # бонус к BASE_SCORER

    # 4H Fractal Raid
    fractal_4h_raided: bool  = False
    fractal_4h_level:  float = 0.0     # уровень пробитого фрактала
    fractal_4h_side:   str   = "none"  # "high" | "low"

    # 1H SNR Zone
    snr_zone_high:     float = 0.0
    snr_zone_low:      float = 0.0
    price_in_snr:      bool  = False

    # 15M FVG Entry
    fvg_15m_high:      float = 0.0
    fvg_15m_low:       float = 0.0
    price_in_fvg_15m:  bool  = False

    # Daily FVG manipulation
    daily_fvg_raid:    bool  = False   # цена вошла в Daily FVG
    daily_fvg_high:    float = 0.0
    daily_fvg_low:     float = 0.0

    description:       str   = ""


def detect_cascade_signal(
    price:       float,
    klines_15m:  list,
    klines_1h:   list,
    klines_4h:   list,
    klines_1d:   list,
    ms:          MarketStructureResult,
) -> CascadeSignal:
    """
    Детектирует каскадный сигнал типа 4H Fractal Raid → 1H SNR → 15M FVG.
    
    Вызывается из get_complete_market_data() если данные 4H и 15m доступны.
    Возвращает CascadeSignal с бонусом к скору если паттерн найден.
    """
    result = CascadeSignal()
    if not (klines_4h and klines_1h and klines_15m):
        return result
    if len(klines_4h) < 8 or len(klines_1h) < 12 or len(klines_15m) < 20:
        return result

    # ── 1. 4H Fractal Raid ────────────────────────────────────────────────────
    # Фрактал = локальный экстремум (5-свечной: свеча[i] выше/ниже двух соседей)
    def _fractal_highs(candles):
        highs = []
        for i in range(2, len(candles) - 2):
            if (candles[i].high > candles[i-1].high and
                candles[i].high > candles[i-2].high and
                candles[i].high > candles[i+1].high and
                candles[i].high > candles[i+2].high):
                highs.append((i, candles[i].high))
        return highs

    def _fractal_lows(candles):
        lows = []
        for i in range(2, len(candles) - 2):
            if (candles[i].low < candles[i-1].low and
                candles[i].low < candles[i-2].low and
                candles[i].low < candles[i+1].low and
                candles[i].low < candles[i+2].low):
                lows.append((i, candles[i].low))
        return lows

    # Ищем последний 4H фрактал который был пробит последними 2-3 свечами
    fractal_raid_short = False
    fractal_raid_long  = False
    fractal_level      = 0.0
    fractal_side       = "none"

    recent_4h = klines_4h[-12:]  # последние 12 свечей 4H = 2 дня
    frac_highs = _fractal_highs(recent_4h)
    frac_lows  = _fractal_lows(recent_4h)

    # BULLISH raid: цена пробила фрактальный лоу но последняя свеча закрылась выше
    if frac_lows:
        last_frac_low_idx, last_frac_low = frac_lows[-1]
        last_candle = recent_4h[-1]
        # Рейд = last candle low < fractal low, но close > fractal low (возврат)
        if last_candle.low < last_frac_low and last_candle.close > last_frac_low:
            fractal_raid_long = True
            fractal_level     = last_frac_low
            fractal_side      = "low"

    # BEARISH raid: пробой фрактального хая с возвратом вниз
    if frac_highs:
        last_frac_high_idx, last_frac_high = frac_highs[-1]
        last_candle = recent_4h[-1]
        if last_candle.high > last_frac_high and last_candle.close < last_frac_high:
            fractal_raid_short = True
            fractal_level      = last_frac_high
            fractal_side       = "high"

    result.fractal_4h_raided = fractal_raid_long or fractal_raid_short
    result.fractal_4h_level  = fractal_level
    result.fractal_4h_side   = fractal_side

    # Если нет фрактального рейда — каскада нет
    if not result.fractal_4h_raided:
        return result

    direction = "long" if fractal_raid_long else "short"

    # ── 2. 1H SNR Zone ────────────────────────────────────────────────────────
    # SNR формируется от уровня где начался рейд (origin candle на 1H)
    # Ищем 1H свечу которая "запустила" движение через фрактал
    snr_high = snr_low = 0.0
    recent_1h = klines_1h[-8:]  # последние 8 часов

    if direction == "long":
        # Ищем последнюю медвежью 1H свечу с большим телом (манипуляция вниз)
        for candle in reversed(recent_1h[:-1]):
            if candle.close < candle.open:  # медвежья
                body = abs(candle.open - candle.close)
                if body / candle.open > 0.005:  # тело > 0.5%
                    snr_high = candle.open
                    snr_low  = candle.close
                    break
    else:
        # BEARISH: последняя бычья 1H свеча (манипуляция вверх)
        for candle in reversed(recent_1h[:-1]):
            if candle.close > candle.open:  # бычья
                body = abs(candle.close - candle.open)
                if body / candle.open > 0.005:
                    snr_low  = candle.open
                    snr_high = candle.close
                    break

    result.snr_zone_high = snr_high
    result.snr_zone_low  = snr_low

    # Проверяем что цена сейчас в SNR zone (±1%)
    if snr_high > 0 and snr_low > 0:
        result.price_in_snr = (snr_low * 0.99 <= price <= snr_high * 1.01)

    # ── 3. 15M FVG внутри SNR ────────────────────────────────────────────────
    fvg_15m_high = fvg_15m_low = 0.0
    recent_15m = klines_15m[-20:]
    for i in range(len(recent_15m) - 2):
        if direction == "long":
            # Бычий FVG: high[i+2] < low[i] — разрыв снизу (цена упала быстро)
            if recent_15m[i + 2].low > recent_15m[i].high:
                g_low  = recent_15m[i].high
                g_high = recent_15m[i + 2].low
                # FVG должен быть в зоне SNR или ниже
                if g_high > 0 and (snr_low == 0 or g_low <= snr_high * 1.01):
                    fvg_15m_low  = g_low
                    fvg_15m_high = g_high
        else:
            # Медвежий FVG: low[i] > high[i+2]
            if recent_15m[i].low > recent_15m[i + 2].high:
                g_low  = recent_15m[i + 2].high
                g_high = recent_15m[i].low
                if g_high > 0 and (snr_high == 0 or g_high >= snr_low * 0.99):
                    fvg_15m_low  = g_low
                    fvg_15m_high = g_high

    result.fvg_15m_high   = fvg_15m_high
    result.fvg_15m_low    = fvg_15m_low
    result.price_in_fvg_15m = (
        fvg_15m_low > 0 and
        fvg_15m_low * 0.998 <= price <= fvg_15m_high * 1.002
    )

    # ── 4. Daily FVG Manipulation ─────────────────────────────────────────────
    # Проверяем что цена зашла в Daily FVG (Daily FVG из ms.fvg_bearish_1d / fvg_bullish_1d)
    daily_fvg_raid = False
    daily_fvg_high = daily_fvg_low = 0.0
    if direction == "long" and ms.fvg_bullish_1d:
        fl, fh = ms.fvg_bullish_1d
        if fl <= price <= fh:
            daily_fvg_raid = True
            daily_fvg_low  = fl
            daily_fvg_high = fh
    elif direction == "short" and ms.fvg_bearish_1d:
        fl, fh = ms.fvg_bearish_1d
        if fl <= price <= fh:
            daily_fvg_raid = True
            daily_fvg_low  = fl
            daily_fvg_high = fh

    result.daily_fvg_raid  = daily_fvg_raid
    result.daily_fvg_high  = daily_fvg_high
    result.daily_fvg_low   = daily_fvg_low

    # ── 5. Итоговый сигнал и бонус ────────────────────────────────────────────
    score = 0
    parts = []

    if result.fractal_4h_raided:
        score += 4
        parts.append(f"4H Fractal Raid {fractal_side.upper()} @ {fractal_level:.4f}")

    if result.price_in_snr:
        score += 4
        parts.append(f"In 1H SNR Zone {snr_low:.4f}–{snr_high:.4f}")
    elif snr_high > 0:
        score += 2
        parts.append(f"SNR formed {snr_low:.4f}–{snr_high:.4f}")

    if result.price_in_fvg_15m:
        score += 6
        parts.append(f"In 15M FVG {fvg_15m_low:.4f}–{fvg_15m_high:.4f} 🎯")
    elif fvg_15m_high > 0:
        score += 2
        parts.append(f"15M FVG nearby {fvg_15m_low:.4f}–{fvg_15m_high:.4f}")

    if daily_fvg_raid:
        score += 2
        parts.append(f"Daily FVG manipulation {daily_fvg_low:.4f}–{daily_fvg_high:.4f}")

    if score > 0:
        result.has_signal    = True
        result.direction     = direction
        result.confidence    = min(score / 16.0, 1.0)
        result.score_bonus   = score
        result.description   = " | ".join(parts)

    return result
