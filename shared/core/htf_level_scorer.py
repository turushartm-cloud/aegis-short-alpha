"""
M3: Weekly/Monthly HTF Level Scorer.

Использует уже вычисленные MarketStructureResult уровни PWH/PWL/PMH/PML
для бонусов когда цена приближается к ключевым недельным/месячным уровням.

Данные берутся из md.market_structure (уже загружен, no extra API calls).

Scoring:
  SHORT у PWH (≤2% ниже) → +8   (недельное сопротивление)
  SHORT у PMH (≤2% ниже) → +12  (месячное сопротивление, сильнее)
  LONG  у PWL (≤2% выше) → +8   (недельная поддержка)
  LONG  у PML (≤2% выше) → +12  (месячная поддержка, сильнее)

Если одновременно PMH ≈ PWH → confluence → суммируются (cap +14).

ENV:
  HTF_PROXIMITY_W  = 2.0   % близости к недельному уровню
  HTF_PROXIMITY_M  = 2.5   % близости к месячному уровню
  HTF_BONUS_W      = 8     бонус за недельный уровень
  HTF_BONUS_M      = 12    бонус за месячный уровень
"""
import os
import logging
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

_PROX_W  = float(os.getenv("HTF_PROXIMITY_W",  "2.0"))
_PROX_M  = float(os.getenv("HTF_PROXIMITY_M",  "2.5"))
_BONUS_W = int(os.getenv("HTF_BONUS_W",         "8"))
_BONUS_M = int(os.getenv("HTF_BONUS_M",         "12"))
_BONUS_CAP = 14


def _near_above(level: float, price: float, prox_pct: float) -> bool:
    """Цена ниже уровня, но в пределах prox_pct% (resistance overhead)."""
    if level <= 0 or price <= 0:
        return False
    return price < level <= price * (1 + prox_pct / 100)


def _near_below(level: float, price: float, prox_pct: float) -> bool:
    """Цена выше уровня, но в пределах prox_pct% (support underneath)."""
    if level <= 0 or price <= 0:
        return False
    return price * (1 - prox_pct / 100) <= level < price


def htf_level_score_bonus(
    price: float,
    direction: str,
    ms,  # MarketStructureResult
) -> Tuple[int, str]:
    """
    Вычисляет bonus за близость к ключевым HTF уровням (PWH/PWL/PMH/PML).

    Args:
        price:     текущая цена
        direction: "short" или "long"
        ms:        MarketStructureResult (из md.market_structure)

    Returns:
        (bonus, reason) или (0, "")
    """
    if ms is None or price <= 0:
        return 0, ""

    try:
        bonus = 0
        parts = []

        pwh = getattr(ms, "pwh", 0.0) or 0.0
        pwl = getattr(ms, "pwl", 0.0) or 0.0
        pmh = getattr(ms, "pmh", 0.0) or 0.0
        pml = getattr(ms, "pml", 0.0) or 0.0

        if direction == "short":
            # Недельное сопротивление выше
            if _near_above(pwh, price, _PROX_W):
                dist = (pwh - price) / price * 100
                bonus += _BONUS_W
                parts.append(f"PWH@{pwh:.5g}(+{dist:.1f}%)")

            # Месячное сопротивление выше
            if _near_above(pmh, price, _PROX_M):
                dist = (pmh - price) / price * 100
                # Если PMH уже засчитан через PWH (близко) — не суммировать
                already = any("PWH" in p for p in parts)
                if not already or abs(pmh - pwh) / price * 100 > 0.3:
                    bonus += _BONUS_M
                    parts.append(f"PMH@{pmh:.5g}(+{dist:.1f}%)")

        else:  # long
            # Недельная поддержка ниже
            if _near_below(pwl, price, _PROX_W):
                dist = (price - pwl) / price * 100
                bonus += _BONUS_W
                parts.append(f"PWL@{pwl:.5g}(-{dist:.1f}%)")

            # Месячная поддержка ниже
            if _near_below(pml, price, _PROX_M):
                dist = (price - pml) / price * 100
                already = any("PWL" in p for p in parts)
                if not already or abs(pml - pwl) / price * 100 > 0.3:
                    bonus += _BONUS_M
                    parts.append(f"PML@{pml:.5g}(-{dist:.1f}%)")

        if bonus == 0:
            return 0, ""

        bonus = min(bonus, _BONUS_CAP)
        arrow = "↑" if direction == "short" else "↓"
        reason = f"📅 [HTF WEEKLY/MONTHLY] {arrow} {' '.join(parts)} → +{bonus}"
        return bonus, reason

    except Exception as e:
        logger.debug(f"[HTFLevels] error: {e}")
        return 0, ""
