"""
Swing High/Low Stop Loss v1.0
SL ставится за последним подтверждённым swing low (LONG) / swing high (SHORT) на 4H.

Алгоритм:
  1. Смотрим последние SWING_LOOKBACK свечей 4H
  2. Pivot low: low[i] < min(low[i-N:i]) AND low[i] < min(low[i+1:i+N+1])  (N=PIVOT_BARS)
  3. Берём самый свежий подтверждённый пивот (не последние N баров — ещё без подтверждения)
  4. SL = pivot_low  * (1 - BUFFER_PCT/100)  для LONG
         pivot_high * (1 + BUFFER_PCT/100)  для SHORT
  5. Зажим [SL_MIN_PCT%, SL_MAX_PCT%] от текущей цены

ENV:
  USE_SWING_SL        = true     включить swing SL (дефолт true)
  SWING_LOOKBACK      = 20       свечей назад для поиска пивота
  SWING_PIVOT_BARS    = 3        баров с каждой стороны для пивота
  SWING_BUFFER_PCT    = 0.2      буфер за пивотом (%)
  SWING_SL_MIN_PCT    = 1.0      мин SL% от цены
  SWING_SL_MAX_PCT    = 6.0      макс SL% от цены
"""
import os
import logging
from typing import Optional, Tuple, List

logger = logging.getLogger(__name__)

_ENABLE      = os.getenv("USE_SWING_SL", "true").lower() == "true"
_LOOKBACK    = int(float(os.getenv("SWING_LOOKBACK", "20")))
_PIVOT_BARS  = int(float(os.getenv("SWING_PIVOT_BARS", "3")))
_BUFFER_PCT  = float(os.getenv("SWING_BUFFER_PCT", "0.2"))
_SL_MIN_PCT  = float(os.getenv("SWING_SL_MIN_PCT", "1.0"))
_SL_MAX_PCT  = float(os.getenv("SWING_SL_MAX_PCT", "6.0"))


def _find_pivots(values: List[float], pivot_bars: int, find_min: bool) -> List[Tuple[int, float]]:
    """Находит pivot минимумы/максимумы. Возвращает [(индекс, значение)] от свежего к старому."""
    pivots = []
    n = len(values)
    for i in range(pivot_bars, n - pivot_bars):
        left  = values[max(0, i - pivot_bars):i]
        right = values[i + 1:i + pivot_bars + 1]
        window = left + right
        if not window:
            continue
        if find_min:
            if all(values[i] <= w for w in window):
                pivots.append((i, values[i]))
        else:
            if all(values[i] >= w for w in window):
                pivots.append((i, values[i]))
    return sorted(pivots, key=lambda x: x[0], reverse=True)


def calculate_swing_sl(candles, price: float, direction: str) -> Tuple[Optional[float], str]:
    """
    Рассчитывает SL по последнему подтверждённому swing low (LONG) / swing high (SHORT).

    Args:
        candles: список CandleData sorted old→new (.high, .low, .close)
        price:   текущая цена
        direction: "long" или "short"

    Returns:
        (sl_price, description)
        sl_price = None если swing не найден или вне диапазона → используй fallback
    """
    if not _ENABLE:
        return None, "[SwingSL] disabled"

    if not candles or len(candles) < _PIVOT_BARS * 2 + 3:
        return None, "[SwingSL] недостаточно свечей"

    try:
        lookback  = min(_LOOKBACK + _PIVOT_BARS, len(candles))
        recent    = candles[-lookback:]
        confirmed = len(recent) - _PIVOT_BARS  # только подтверждённые пивоты (без последних N баров)

        if direction == "long":
            lows   = [c.low for c in recent]
            pivots = _find_pivots(lows, _PIVOT_BARS, find_min=True)
            valid  = [(i, v) for i, v in pivots if i < confirmed]
            if not valid:
                return None, "[SwingSL] swing low не найден"

            _, swing_low = valid[0]  # самый свежий
            sl_price = swing_low * (1 - _BUFFER_PCT / 100)
            sl_pct   = (price - sl_price) / price * 100

            if sl_pct < _SL_MIN_PCT:
                return None, f"[SwingSL] слишком близко ({sl_pct:.2f}% < мин {_SL_MIN_PCT}%)"
            if sl_pct > _SL_MAX_PCT:
                return None, f"[SwingSL] слишком далеко ({sl_pct:.2f}% > макс {_SL_MAX_PCT}%)"

            return (
                round(sl_price, 8),
                f"📉 SwingSL: low={swing_low:.6f} → SL={sl_price:.6f} ({sl_pct:.2f}%)"
            )

        else:  # short
            highs  = [c.high for c in recent]
            pivots = _find_pivots(highs, _PIVOT_BARS, find_min=False)
            valid  = [(i, v) for i, v in pivots if i < confirmed]
            if not valid:
                return None, "[SwingSL] swing high не найден"

            _, swing_high = valid[0]
            sl_price = swing_high * (1 + _BUFFER_PCT / 100)
            sl_pct   = (sl_price - price) / price * 100

            if sl_pct < _SL_MIN_PCT:
                return None, f"[SwingSL] слишком близко ({sl_pct:.2f}% < мин {_SL_MIN_PCT}%)"
            if sl_pct > _SL_MAX_PCT:
                return None, f"[SwingSL] слишком далеко ({sl_pct:.2f}% > макс {_SL_MAX_PCT}%)"

            return (
                round(sl_price, 8),
                f"📈 SwingSL: high={swing_high:.6f} → SL={sl_price:.6f} ({sl_pct:.2f}%)"
            )

    except Exception as e:
        logger.debug(f"[SwingSL] error: {e}")
        return None, f"[SwingSL] error: {e}"
