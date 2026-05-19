"""
SMC Detector v1.0 — BOS/CHoCH SL (#30) + SSL/BSL Liquidity SL (#32)

#30 BOS/CHoCH SL: SL за последним Break of Structure / Change of Character
    SHORT → SL за последним Lower High (LH) в даунтренде
    LONG  → SL за последним Higher Low (HL) в аптренде

#32 SSL/BSL Liquidity SL: SL за зонами ликвидности (Equal Highs / Equal Lows)
    SHORT → SL за кластером Equal Highs (BSL зона) выше цены
    LONG  → SL за кластером Equal Lows  (SSL зона) ниже цены

ENV:
  USE_BOS_CHOCH_SL      = true    включить BOS/CHoCH SL
  USE_SSL_BSL_SL        = true    включить SSL/BSL Liquidity SL
  SMC_PIVOT_BARS        = 3       баров с каждой стороны для pivot
  SMC_BUFFER_PCT        = 0.3     буфер за уровнем (%)
  SMC_SL_MIN_PCT        = 1.0     мин SL% от цены
  SMC_SL_MAX_PCT        = 8.0     макс SL% от цены
  SMC_EQUAL_TOLERANCE   = 0.4     допуск для Equal Highs/Lows (%)
  SMC_EQUAL_MIN_COUNT   = 2       мин количество касаний для зоны ликвидности
"""
import os
import logging
from typing import Optional, Tuple, List

logger = logging.getLogger(__name__)

_USE_BOS     = os.getenv("USE_BOS_CHOCH_SL", "true").lower() == "true"
_USE_SSL_BSL = os.getenv("USE_SSL_BSL_SL",   "true").lower() == "true"
_PIVOT_BARS  = int(float(os.getenv("SMC_PIVOT_BARS",       "3")))
_BUFFER_PCT  = float(os.getenv("SMC_BUFFER_PCT",           "0.3"))
_SL_MIN_PCT  = float(os.getenv("SMC_SL_MIN_PCT",           "1.0"))
_SL_MAX_PCT  = float(os.getenv("SMC_SL_MAX_PCT",           "8.0"))
_EQUAL_TOL   = float(os.getenv("SMC_EQUAL_TOLERANCE",      "0.4"))
_EQUAL_MIN   = int(float(os.getenv("SMC_EQUAL_MIN_COUNT",  "2")))


# ── Вспомогательные функции ───────────────────────────────────────────────────

def _find_pivot_highs(candles, pivot_bars: int) -> List[Tuple[int, float]]:
    """Подтверждённые swing highs: high[i] выше всех соседей на pivot_bars с каждой стороны."""
    result = []
    n = len(candles)
    confirmed = n - pivot_bars  # последние pivot_bars ещё не подтверждены
    for i in range(pivot_bars, confirmed):
        h = candles[i].high
        if (all(candles[j].high <= h for j in range(i - pivot_bars, i)) and
                all(candles[j].high <= h for j in range(i + 1, i + pivot_bars + 1))):
            result.append((i, h))
    return result


def _find_pivot_lows(candles, pivot_bars: int) -> List[Tuple[int, float]]:
    """Подтверждённые swing lows: low[i] ниже всех соседей на pivot_bars с каждой стороны."""
    result = []
    n = len(candles)
    confirmed = n - pivot_bars
    for i in range(pivot_bars, confirmed):
        l = candles[i].low
        if (all(candles[j].low >= l for j in range(i - pivot_bars, i)) and
                all(candles[j].low >= l for j in range(i + 1, i + pivot_bars + 1))):
            result.append((i, l))
    return result


def _classify_swing_highs(pivot_highs: List[Tuple[int, float]]) -> List[Tuple[int, float, str]]:
    """Классифицирует swing highs как HH (Higher High) или LH (Lower High)."""
    result = []
    for idx, (i, h) in enumerate(pivot_highs):
        if idx == 0:
            result.append((i, h, "SH"))
        else:
            prev_h = pivot_highs[idx - 1][1]
            result.append((i, h, "HH" if h > prev_h else "LH"))
    return result


def _classify_swing_lows(pivot_lows: List[Tuple[int, float]]) -> List[Tuple[int, float, str]]:
    """Классифицирует swing lows как HL (Higher Low) или LL (Lower Low)."""
    result = []
    for idx, (i, l) in enumerate(pivot_lows):
        if idx == 0:
            result.append((i, l, "SL"))
        else:
            prev_l = pivot_lows[idx - 1][1]
            result.append((i, l, "HL" if l > prev_l else "LL"))
    return result


def _cluster_prices(prices: List[float], tolerance_pct: float) -> List[Tuple[float, int]]:
    """
    Группирует цены в кластеры с допуском tolerance_pct%.
    Возвращает [(средняя_цена, количество)] отсортировано по цене.
    """
    if not prices:
        return []
    sorted_prices = sorted(set(prices))
    clusters: List[Tuple[float, int]] = []
    i = 0
    while i < len(sorted_prices):
        cluster = [sorted_prices[i]]
        j = i + 1
        while j < len(sorted_prices):
            if (sorted_prices[j] - cluster[0]) / cluster[0] * 100 <= tolerance_pct:
                cluster.append(sorted_prices[j])
                j += 1
            else:
                break
        clusters.append((sum(cluster) / len(cluster), len(cluster)))
        i = j
    return clusters


# ── #30 BOS/CHoCH SL ─────────────────────────────────────────────────────────

def calculate_bos_choch_sl(candles, price: float, direction: str) -> Tuple[Optional[float], str]:
    """
    #30 BOS/CHoCH Stop Loss.

    SHORT: SL за последним Lower High (LH) выше текущей цены.
           LH = swing high ниже предыдущего swing high → структура даунтренда.
           Рынок уже показал неспособность пробить этот уровень → идеальный стоп.

    LONG:  SL за последним Higher Low (HL) ниже текущей цены.
           HL = swing low выше предыдущего swing low → структура аптренда.

    Args:
        candles: список CandleData (old→new), минимум 20
        price:   текущая цена
        direction: "short" или "long"

    Returns:
        (sl_price, description) или (None, причина)
    """
    if not _USE_BOS:
        return None, "[BOS/CHoCH] disabled"
    if not candles or len(candles) < _PIVOT_BARS * 2 + 5:
        return None, "[BOS/CHoCH] недостаточно свечей"

    try:
        recent = candles[-60:]

        if direction == "short":
            pivot_highs = _find_pivot_highs(recent, _PIVOT_BARS)
            if len(pivot_highs) < 2:
                return None, "[BOS/CHoCH] нет подтверждённых swing highs"

            classified = _classify_swing_highs(pivot_highs)
            # Ищем ближайший LH выше текущей цены (или любой swing high как fallback)
            lh_above = [(i, h) for i, h, t in classified if t == "LH" and h > price]
            if lh_above:
                _, sl_level = min(lh_above, key=lambda x: x[1])
                label = "LH"
            else:
                # Fallback: ближайший swing high выше цены (любой тип)
                sh_above = [(i, h) for i, h, t in classified if h > price]
                if not sh_above:
                    return None, "[BOS/CHoCH] нет swing high выше цены"
                _, sl_level = min(sh_above, key=lambda x: x[1])
                label = "SwingH"

            sl_price = round(sl_level * (1 + _BUFFER_PCT / 100), 8)
            sl_pct   = (sl_price - price) / price * 100
            if not (_SL_MIN_PCT <= sl_pct <= _SL_MAX_PCT):
                return None, f"[BOS/CHoCH] SL {sl_pct:.1f}% вне [{_SL_MIN_PCT}%–{_SL_MAX_PCT}%]"
            return sl_price, f"🔷 [BOS/CHoCH] {label}={sl_level:.6f} → SL={sl_price:.6f} ({sl_pct:.2f}%)"

        else:  # long
            pivot_lows = _find_pivot_lows(recent, _PIVOT_BARS)
            if len(pivot_lows) < 2:
                return None, "[BOS/CHoCH] нет подтверждённых swing lows"

            classified = _classify_swing_lows(pivot_lows)
            hl_below = [(i, l) for i, l, t in classified if t == "HL" and l < price]
            if hl_below:
                _, sl_level = max(hl_below, key=lambda x: x[1])
                label = "HL"
            else:
                sl_below = [(i, l) for i, l, t in classified if l < price]
                if not sl_below:
                    return None, "[BOS/CHoCH] нет swing low ниже цены"
                _, sl_level = max(sl_below, key=lambda x: x[1])
                label = "SwingL"

            sl_price = round(sl_level * (1 - _BUFFER_PCT / 100), 8)
            sl_pct   = (price - sl_price) / price * 100
            if not (_SL_MIN_PCT <= sl_pct <= _SL_MAX_PCT):
                return None, f"[BOS/CHoCH] SL {sl_pct:.1f}% вне диапазона"
            return sl_price, f"🔷 [BOS/CHoCH] {label}={sl_level:.6f} → SL={sl_price:.6f} ({sl_pct:.2f}%)"

    except Exception as e:
        logger.debug(f"[BOS/CHoCH] error: {e}")
        return None, f"[BOS/CHoCH] error: {e}"


# ── #32 SSL/BSL Liquidity SL ──────────────────────────────────────────────────

def calculate_ssl_bsl_sl(candles, price: float, direction: str) -> Tuple[Optional[float], str]:
    """
    #32 SSL/BSL Liquidity Stop Loss.

    Equal Highs (BSL — Buy-Side Liquidity): кластер swing highs на одном уровне.
    Туда собраны стопы лонгистов → рынок часто уходит выше чтобы выбить их.
    → SHORT SL чуть выше ближайшей BSL зоны.

    Equal Lows (SSL — Sell-Side Liquidity): кластер swing lows на одном уровне.
    Туда собраны стопы шортистов → рынок уходит ниже чтобы снять их.
    → LONG SL чуть ниже ближайшей SSL зоны.

    Args:
        candles: список CandleData (old→new), минимум 15
        price:   текущая цена
        direction: "short" или "long"

    Returns:
        (sl_price, description) или (None, причина)
    """
    if not _USE_SSL_BSL:
        return None, "[SSL/BSL] disabled"
    if not candles or len(candles) < 15:
        return None, "[SSL/BSL] недостаточно свечей"

    try:
        recent = candles[-50:]

        if direction == "short":
            # Собираем все swing highs выше текущей цены
            pivot_highs = _find_pivot_highs(recent, _PIVOT_BARS)
            highs_above = [h for _, h in pivot_highs if h > price]
            if len(highs_above) < _EQUAL_MIN:
                return None, f"[SSL/BSL] нет достаточных swing highs выше цены ({len(highs_above)}<{_EQUAL_MIN})"

            clusters = _cluster_prices(highs_above, _EQUAL_TOL)
            # Только кластеры с >= _EQUAL_MIN касаний
            bsl_zones = [(lvl, cnt) for lvl, cnt in clusters if cnt >= _EQUAL_MIN and lvl > price]
            if not bsl_zones:
                return None, "[SSL/BSL] нет BSL зон ликвидности выше цены"

            nearest_bsl = min(bsl_zones, key=lambda x: x[0])
            sl_level = nearest_bsl[0]
            sl_price = round(sl_level * (1 + _BUFFER_PCT / 100), 8)
            sl_pct   = (sl_price - price) / price * 100
            if not (_SL_MIN_PCT <= sl_pct <= _SL_MAX_PCT):
                return None, f"[SSL/BSL] BSL SL {sl_pct:.1f}% вне диапазона"
            return (sl_price,
                    f"💧 [BSL] Equal Highs @ {sl_level:.6f} (×{nearest_bsl[1]}) → SL={sl_price:.6f} ({sl_pct:.2f}%)")

        else:  # long
            pivot_lows = _find_pivot_lows(recent, _PIVOT_BARS)
            lows_below = [l for _, l in pivot_lows if l < price]
            if len(lows_below) < _EQUAL_MIN:
                return None, f"[SSL/BSL] нет достаточных swing lows ниже цены ({len(lows_below)}<{_EQUAL_MIN})"

            clusters = _cluster_prices(lows_below, _EQUAL_TOL)
            ssl_zones = [(lvl, cnt) for lvl, cnt in clusters if cnt >= _EQUAL_MIN and lvl < price]
            if not ssl_zones:
                return None, "[SSL/BSL] нет SSL зон ликвидности ниже цены"

            nearest_ssl = max(ssl_zones, key=lambda x: x[0])
            sl_level = nearest_ssl[0]
            sl_price = round(sl_level * (1 - _BUFFER_PCT / 100), 8)
            sl_pct   = (price - sl_price) / price * 100
            if not (_SL_MIN_PCT <= sl_pct <= _SL_MAX_PCT):
                return None, f"[SSL/BSL] SSL SL {sl_pct:.1f}% вне диапазона"
            return (sl_price,
                    f"💧 [SSL] Equal Lows @ {sl_level:.6f} (×{nearest_ssl[1]}) → SL={sl_price:.6f} ({sl_pct:.2f}%)")

    except Exception as e:
        logger.debug(f"[SSL/BSL] error: {e}")
        return None, f"[SSL/BSL] error: {e}"
