"""
KillZoneFilter — #19
ICT Kill Zones: высокоактивные временные зоны ликвидности.
Бонус к base_score в сессионные окна, штраф в мёртвые часы.

London:   02:00–05:00 UTC  → +KILL_ZONE_BONUS (default +10)
NY:       08:00–11:00 UTC  → +KILL_ZONE_BONUS (default +10)
Asia dead: 14:00–20:00 UTC → -KILL_ZONE_PENALTY (default -5)
"""
from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import Tuple

_BONUS   = int(os.getenv("KILL_ZONE_BONUS",   "10"))
_PENALTY = int(os.getenv("KILL_ZONE_PENALTY",  "5"))

# Kill zones (UTC hours, inclusive)
_LONDON_START, _LONDON_END = 2,  5
_NY_START,     _NY_END     = 8,  11
_ASIA_DEAD_START, _ASIA_DEAD_END = 14, 20


class KillZoneFilter:
    """
    Применяй в scan_symbol() после получения base_score:
        adj, reason = KillZoneFilter.get_adjustment()
        if adj != 0:
            base_score += adj
    """

    @staticmethod
    def get_adjustment(utc_dt: datetime | None = None) -> Tuple[int, str]:
        """
        Returns (score_delta, reason_str).
        score_delta > 0 — бонус (активная ликвидность).
        score_delta < 0 — штраф (мёртвое время, размытые движения).
        """
        if utc_dt is None:
            utc_dt = datetime.now(timezone.utc)
        h = utc_dt.hour

        if _LONDON_START <= h < _LONDON_END:
            return _BONUS, f"🇬🇧 London Kill Zone {h:02d}:xx UTC +{_BONUS}"
        if _NY_START <= h < _NY_END:
            return _BONUS, f"🗽 NY Kill Zone {h:02d}:xx UTC +{_BONUS}"
        if _ASIA_DEAD_START <= h < _ASIA_DEAD_END:
            return -_PENALTY, f"😴 Asia Dead Zone {h:02d}:xx UTC −{_PENALTY}"
        return 0, ""

    @staticmethod
    def is_active_session(utc_dt: datetime | None = None) -> bool:
        """True если сейчас активная торговая сессия (London или NY)."""
        delta, _ = KillZoneFilter.get_adjustment(utc_dt)
        return delta > 0
