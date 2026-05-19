"""
FTA (First Touch Area) Tracker.

Отслеживает сколько раз цена касалась каждого уровня (OB / FVG / SNR).
Используется для дифференцированного бонуса:

  Первое касание  (0 предыдущих) → × 1.5 бонус   (зона свежая, реакция сильнейшая)
  Второе касание  (1 предыдущее) → × 1.0 бонус   (зона ещё держится)
  Третье касание  (2 предыдущих) → × 0.3 бонус   (зона слабеет)
  4+ касания                     → штраф −30% base (зона истощена)

Redis-ключ: fta:{bot}:{symbol}:{level_bucket}:days  →  количество дней
            fta:{bot}:{symbol}:{level_bucket}:day:{date} → sentinel (25h TTL)

TTL счётчика: 14 дней (уровень живёт две недели, потом сбрасывается).

Использование в main.py:
    from utils.fta_tracker import FTATracker
    fta = FTATracker(state.redis.client, "short")   # или "long"
    bonus, reason = fta.score_level(symbol, level_price, base_bonus=10)
    fta.record(symbol, level_price)   # вызывать ПОСЛЕ открытия сделки
"""

import math
import logging
import datetime
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

_FTA_DAYS_TTL = 14 * 24 * 3600   # 14 дней
_FTA_DAY_TTL  = 25 * 3600        # 25 ч — sentinel дня


def _bucket(level: float) -> str:
    """
    Округляет цену до 3 значимых цифр для группировки близких касаний.
    BTC 83_412 → "83400" | DOGE 0.3456 → "0.346" | ETH 3_145 → "3150"
    """
    if level <= 0:
        return "0"
    try:
        mag = 10 ** (math.floor(math.log10(level)) - 2)
        return str(round(level / mag) * mag)
    except (ValueError, OverflowError):
        return str(round(level, 6))


class FTATracker:
    """First Touch Area tracker backed by Redis."""

    def __init__(self, redis_client=None, bot_prefix: str = "short"):
        self.redis  = redis_client
        self.prefix = bot_prefix

    # ── Приватные ключи ──────────────────────────────────────────────────────

    def _count_key(self, symbol: str, level: float) -> str:
        return f"fta:{self.prefix}:{symbol}:{_bucket(level)}:days"

    def _day_key(self, symbol: str, level: float) -> str:
        today = datetime.date.today().isoformat()
        return f"fta:{self.prefix}:{symbol}:{_bucket(level)}:day:{today}"

    # ── Публичное API ────────────────────────────────────────────────────────

    def get_touch_days(self, symbol: str, level: float) -> int:
        """Возвращает число дней, когда уровень касался цены (0 = ни разу)."""
        if not self.redis:
            return 0
        try:
            val = self.redis.get(self._count_key(symbol, level))
            return int(val) if val else 0
        except Exception:
            return 0

    def record(self, symbol: str, level: float) -> int:
        """
        Записывает одно касание уровня (сегодня).
        Если уже записано сегодня — игнорирует (один раз в сутки).
        Возвращает актуальный счётчик дней.
        """
        if not self.redis:
            return 0
        try:
            day_key   = self._day_key(symbol, level)
            count_key = self._count_key(symbol, level)

            # Sentinel: уже касались сегодня?
            if self.redis.get(day_key):
                val = self.redis.get(count_key)
                return int(val) if val else 1

            # Новый день — инкрементируем
            self.redis.setex(day_key, _FTA_DAY_TTL, "1")
            count = self.redis.incr(count_key)
            if count == 1:
                self.redis.expire(count_key, _FTA_DAYS_TTL)
            return count
        except Exception as e:
            logger.debug(f"[FTA] record error: {e}")
            return 0

    def score_level(
        self,
        symbol: str,
        level: float,
        base_bonus: int,
        record_touch: bool = True,
    ) -> Tuple[int, str]:
        """
        Возвращает (скорректированный_бонус, причина) с учётом истории касаний.

        Args:
            symbol:       символ (BTCUSDT)
            level:        цена уровня (OB mid, FVG mid, SNR)
            base_bonus:   бонус без FTA корректировки
            record_touch: если True — записывает касание сегодня

        Returns:
            (adj_bonus, reason_str)
        """
        if base_bonus == 0:
            return 0, ""

        days = self.get_touch_days(symbol, level)

        if days == 0:
            adj    = int(base_bonus * 1.5)
            reason = f"FTA 🎯 ПЕРВОЕ касание уровня {_bucket(level)} → +{adj} (×1.5)"
        elif days == 1:
            adj    = base_bonus
            reason = f"FTA 2-е касание уровня {_bucket(level)} → +{adj}"
        elif days == 2:
            adj    = max(1, int(base_bonus * 0.3))
            reason = f"FTA 3-е касание (слабеет) {_bucket(level)} → +{adj}"
        else:
            adj    = -max(1, int(base_bonus * 0.3))
            reason = f"FTA ИСТОЩЁН ({days} касаний) {_bucket(level)} → {adj}"

        if record_touch:
            self.record(symbol, level)

        return adj, reason

    def score_ob(
        self,
        symbol: str,
        ob: Optional[Tuple[float, float]],
        direction: str,
        base_bonus: int = 10,
    ) -> Tuple[int, str]:
        """
        Удобный метод для OB (использует середину зоны как level).
        direction: "bearish" или "bullish"
        """
        if not ob:
            return 0, ""
        lo, hi = ob
        mid = (lo + hi) / 2
        adj, reason = self.score_level(symbol, mid, base_bonus)
        label = "Bear OB" if direction == "bearish" else "Bull OB"
        return adj, f"[{label}] {reason}"

    def score_fvg(
        self,
        symbol: str,
        fvg: Optional[Tuple[float, float]],
        direction: str,
        base_bonus: int = 8,
    ) -> Tuple[int, str]:
        """
        Удобный метод для FVG.
        direction: "bearish" или "bullish"
        """
        if not fvg:
            return 0, ""
        lo, hi = fvg
        mid = (lo + hi) / 2
        adj, reason = self.score_level(symbol, mid, base_bonus)
        label = "Bear FVG" if direction == "bearish" else "Bull FVG"
        return adj, f"[{label}] {reason}"
