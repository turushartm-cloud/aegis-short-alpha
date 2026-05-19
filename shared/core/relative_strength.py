"""
Relative Strength Calculator — сила токена относительно BTC.

RS = token_change% - btc_change%  (для 1h и 24h)

RS > 0 = токен сильнее BTC (если BTC -5%, а токен -1% → RS = +4%)
RS < 0 = токен слабее BTC

Использование:
    from core.relative_strength import score_rs
    bonus, reason = score_rs(
        token_1h=md.price_change_1h,
        token_24h=md.price_change_24h,
        btc_1h=cached_btc_1h,
        btc_24h=cached_btc_24h,
        direction="long",   # или "short"
    )
"""
import os
from typing import Tuple

_ENABLED   = os.getenv("ENABLE_RELATIVE_STRENGTH", "true").lower() == "true"
_BONUS_MAX = int(os.getenv("RS_BONUS_MAX", "12"))


def score_rs(
    token_1h:  float,
    token_24h: float,
    btc_1h:   float,
    btc_24h:  float,
    direction: str = "long",
) -> Tuple[int, str]:
    """
    Считает бонус/штраф за relative strength токена vs BTC.

    direction="long":  токен сильнее BTC (RS>0) = бонус, слабее = штраф
    direction="short": токен слабее BTC (RS<0) = бонус, сильнее = штраф

    Returns:
        (adj_points, reason_str)
        adj_points диапазон: -_BONUS_MAX .. +_BONUS_MAX
    """
    if not _ENABLED:
        return 0, ""
    if btc_1h == 0 and btc_24h == 0:
        return 0, ""

    rs_1h  = token_1h  - btc_1h
    rs_24h = token_24h - btc_24h

    _hi  = _BONUS_MAX
    _mid = max(1, int(_BONUS_MAX * 0.67))
    _lo  = max(1, int(_BONUS_MAX * 0.33))

    if direction == "long":
        if rs_24h >= 5.0:
            return _hi,  f"🔥 RS LONG +{rs_24h:.1f}%/24H vs BTC (дивергенция вверх)"
        if rs_24h >= 3.0:
            return _mid, f"RS↑ +{rs_24h:.1f}%/24H vs BTC"
        if rs_24h >= 1.5:
            return _lo,  f"RS +{rs_24h:.1f}%/24H vs BTC"
        if rs_24h <= -5.0:
            return -_mid, f"RS↓ {rs_24h:.1f}%/24H vs BTC (слабость токена)"
        if rs_24h <= -3.0:
            return -_lo,  f"RS {rs_24h:.1f}%/24H vs BTC"
        return 0, ""

    else:  # short
        if rs_24h <= -5.0:
            return _hi,  f"🔥 RS SHORT {rs_24h:.1f}%/24H vs BTC (токен слабее рынка)"
        if rs_24h <= -3.0:
            return _mid, f"RS↓ {rs_24h:.1f}%/24H vs BTC (слабость)"
        if rs_24h <= -1.5:
            return _lo,  f"RS {rs_24h:.1f}%/24H vs BTC"
        if rs_24h >= 5.0:
            return -_mid, f"RS↑ +{rs_24h:.1f}%/24H vs BTC (токен сильнее — плохо для SHORT)"
        if rs_24h >= 3.0:
            return -_lo,  f"RS +{rs_24h:.1f}%/24H vs BTC"
        return 0, ""
