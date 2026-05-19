"""
Token Divergence Scorer — индивидуальная дивергенция токена vs рынка.

Composite из 5 компонентов:
  1. Relative Strength vs BTC (30%) — токен сильнее/слабее рынка
  2. Volume Spike              (22%) — аномальный объём у токена
  3. OI Change Rate            (18%) — скорость изменения ОИ
  4. Funding direction         (18%) — кто платит: лонги или шорты
  5. CVD / Taker direction     (12%) — taker buy/sell давление

Возвращает (bonus: int, reasons: list[str])
Диапазон: -DIVERGENCE_BONUS_MAX .. +DIVERGENCE_BONUS_MAX (default ±20)

Использование:
    from core.token_divergence_scorer import score_divergence
    bonus, reasons = score_divergence(md, btc_1h, btc_24h, direction="long")
    effective_score = max(0, min(100, effective_score + bonus))
"""
import os
from typing import Tuple, List, Any

_ENABLED      = os.getenv("ENABLE_DIVERGENCE_SCORER", "true").lower() == "true"
_MAX_BONUS    = int(os.getenv("DIVERGENCE_BONUS_MAX", "20"))
_CVD_ENABLED  = os.getenv("ENABLE_CVD_COMPONENT", "true").lower() == "true"
# taker_buy_sell_ratio thresholds: >0.55 = buy pressure, <0.45 = sell pressure
_CVD_BUY_THR  = float(os.getenv("CVD_BUY_THRESHOLD",  "0.55"))
_CVD_SELL_THR = float(os.getenv("CVD_SELL_THRESHOLD", "0.45"))


def score_divergence(
    md:        Any,
    btc_1h:   float,
    btc_24h:  float,
    direction: str = "long",
) -> Tuple[int, List[str]]:
    """
    Рассчитывает composite дивергенцию токена vs рынка.

    direction="long":  позитивная дивергенция (токен сильнее) = +бонус
    direction="short": негативная дивергенция (токен слабее)  = +бонус

    Returns:
        (total_bonus: int, reasons: list[str])
    """
    if not _ENABLED:
        return 0, []

    from core.relative_strength import score_rs

    reasons: List[str] = []
    total = 0

    t_1h  = getattr(md, "price_change_1h",  0.0) or 0.0
    t_24h = getattr(md, "price_change_24h", 0.0) or 0.0

    # ── 1. Relative Strength vs BTC (вес 30%) ────────────────────────
    rs_bonus, rs_reason = score_rs(t_1h, t_24h, btc_1h, btc_24h, direction)
    rs_contrib = int(rs_bonus * 0.30)
    total += rs_contrib
    if rs_reason:
        reasons.append(rs_reason)

    # ── 2. Volume Spike (вес 22%) ─────────────────────────────────────
    vol_spike = getattr(md, "volume_spike_ratio", 1.0) or 1.0
    vol_chg   = getattr(md, "volume_change_24h",  0.0) or 0.0
    if vol_spike >= 3.0:
        vol_pts = 8
        reasons.append(f"Vol×{vol_spike:.1f} 🔥 аномальный объём")
    elif vol_spike >= 2.0:
        vol_pts = 5
        reasons.append(f"Vol×{vol_spike:.1f} spike объём")
    elif vol_spike >= 1.5:
        vol_pts = 2
    elif vol_chg > 200:
        vol_pts = 6
        reasons.append(f"vol_chg=+{vol_chg:.0f}% аномальный 24H объём")
    else:
        vol_pts = 0
    total += int(vol_pts * 0.22)

    # ── 3. OI Change Rate (вес 18%) ───────────────────────────────────
    oi_1h = getattr(md, "oi_change_1h", 0.0) or 0.0
    oi_4h = getattr(md, "oi_change_4h", 0.0) or 0.0

    if direction == "long":
        # OI падает = шорты закрываются = хорошо для лонга
        if oi_1h < -3.0:
            oi_pts = 8
            reasons.append(f"OI 1h {oi_1h:.1f}% ↓ шорты закрываются")
        elif oi_1h < -1.5:
            oi_pts = 4
        elif oi_4h < -5.0:
            oi_pts = 5
            reasons.append(f"OI 4h {oi_4h:.1f}% ↓")
        elif oi_1h > 5.0 and t_1h > 2.0:
            # OI растёт + цена растёт = новые лонги = тренд подтверждён
            oi_pts = 4
            reasons.append(f"OI 1h +{oi_1h:.1f}% + цена +{t_1h:.1f}% — momentum")
        else:
            oi_pts = 0
    else:  # short
        # OI растёт при падении цены = медведи открываются = хорошо для шорта
        if oi_1h > 3.0 and t_1h < 0:
            oi_pts = 8
            reasons.append(f"OI 1h +{oi_1h:.1f}% при падении цены")
        elif oi_4h > 5.0 and t_24h < 0:
            oi_pts = 5
            reasons.append(f"OI 4h +{oi_4h:.1f}% bear давление")
        elif oi_1h < -3.0:
            # OI падает при SHORT — медведи закрываются, плохо для шорта
            oi_pts = -4
            reasons.append(f"OI 1h {oi_1h:.1f}% ↓ (медведи закрываются)")
        else:
            oi_pts = 0
    total += int(oi_pts * 0.18)

    # ── 4. Funding direction (вес 18%) ────────────────────────────────
    funding = getattr(md, "funding_rate", 0.0) or 0.0

    if direction == "long":
        # Отрицательный funding = шорты переплачивают = хорошо для лонга
        if funding <= -0.05:
            f_pts = 8
            reasons.append(f"Funding {funding:.4f}% шорты переплачивают 🎯")
        elif funding <= -0.02:
            f_pts = 4
        elif funding >= 0.10:
            f_pts = -4
            reasons.append(f"Funding {funding:.4f}% лонги перегреты")
        else:
            f_pts = 0
    else:  # short
        # Положительный funding = лонги переплачивают = хорошо для шорта
        if funding >= 0.10:
            f_pts = 8
            reasons.append(f"Funding {funding:.4f}% лонги переплачивают 🎯")
        elif funding >= 0.05:
            f_pts = 4
        elif funding <= -0.05:
            f_pts = -4
            reasons.append(f"Funding {funding:.4f}% шорты переплачивают (плохо для SHORT)")
        else:
            f_pts = 0
    total += int(f_pts * 0.18)

    # ── 5. CVD / Taker direction (вес 12%) ────────────────────────────
    if _CVD_ENABLED:
        taker_ratio = getattr(md, "taker_buy_sell_ratio", None)
        if taker_ratio is not None:
            taker_ratio = float(taker_ratio)
            if direction == "long":
                if taker_ratio >= _CVD_BUY_THR + 0.10:
                    cvd_pts = 8
                    reasons.append(f"Taker buy {taker_ratio:.2f} 🟢 агрессивные покупатели")
                elif taker_ratio >= _CVD_BUY_THR:
                    cvd_pts = 4
                    reasons.append(f"Taker buy {taker_ratio:.2f} buy pressure")
                elif taker_ratio <= _CVD_SELL_THR - 0.10:
                    cvd_pts = -6
                    reasons.append(f"Taker sell {taker_ratio:.2f} 🔴 sell pressure (плохо для LONG)")
                elif taker_ratio <= _CVD_SELL_THR:
                    cvd_pts = -3
                else:
                    cvd_pts = 0
            else:  # short
                if taker_ratio <= _CVD_SELL_THR - 0.10:
                    cvd_pts = 8
                    reasons.append(f"Taker sell {taker_ratio:.2f} 🔴 агрессивные продавцы")
                elif taker_ratio <= _CVD_SELL_THR:
                    cvd_pts = 4
                    reasons.append(f"Taker sell {taker_ratio:.2f} sell pressure")
                elif taker_ratio >= _CVD_BUY_THR + 0.10:
                    cvd_pts = -6
                    reasons.append(f"Taker buy {taker_ratio:.2f} 🟢 buy pressure (плохо для SHORT)")
                elif taker_ratio >= _CVD_BUY_THR:
                    cvd_pts = -3
                else:
                    cvd_pts = 0
            total += int(cvd_pts * 0.12)

    total = max(-_MAX_BONUS, min(_MAX_BONUS, total))
    return total, reasons
