"""
Fear & Greed Index — alternative.me (free, no API key)
Кэш на 1 час — один запрос на весь скан-цикл, не на символ.

Шкала: 0-100
  0-24   Extreme Fear   → LONG setup (рынок в панике)
  25-44  Fear           → LONG bias
  45-55  Neutral        → без модификатора
  56-74  Greed          → SHORT bias
  75-100 Extreme Greed  → SHORT setup (рынок жадный)
"""
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

FG_URL = "https://api.alternative.me/fng/?limit=1"


class FearGreedCache:
    def __init__(self):
        self._value: Optional[int] = None
        self._label: str = "Unknown"
        self._last_fetch: Optional[datetime] = None
        self._ttl = timedelta(hours=1)
        self._lock = asyncio.Lock()
        self._fail_count = 0

    async def get(self) -> Optional[int]:
        """Возвращает F&G 0-100, или None если недоступно."""
        async with self._lock:
            if (self._value is not None and self._last_fetch and
                    datetime.utcnow() - self._last_fetch < self._ttl):
                return self._value
            await self._fetch()
            return self._value

    @property
    def label(self) -> str:
        return self._label

    @property
    def value(self) -> Optional[int]:
        return self._value

    def score_modifier(self, direction: str) -> int:
        """
        Возвращает бонус/штраф к total score на основе F&G.
        direction: 'long' или 'short'
        """
        v = self._value
        if v is None:
            return 0

        if direction == "long":
            if v <= 15:  return 10   # Extreme Fear — лонги исторически лучшие
            if v <= 24:  return 7
            if v <= 44:  return 4    # Fear — лонг-bias
            if v <= 55:  return 0    # Нейтраль
            if v <= 74:  return -2   # Greed — осторожнее с лонгами
            return -4                # Extreme Greed — не лонги

        else:  # short
            if v >= 85:  return 10   # Extreme Greed — шорты исторически лучшие
            if v >= 75:  return 7
            if v >= 56:  return 4    # Greed — шорт-bias
            if v >= 45:  return 0    # Нейтраль
            if v >= 25:  return -2   # Fear — осторожнее с шортами
            return -4                # Extreme Fear — не шорти

    async def _fetch(self):
        if self._fail_count >= 5:
            # После 5 неудач — молчим, не спамим логи
            if (self._last_fetch and
                    datetime.utcnow() - self._last_fetch < timedelta(hours=6)):
                return
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(FG_URL) as r:
                    if r.status == 200:
                        data = await r.json(content_type=None)
                        entry = data.get("data", [{}])[0]
                        self._value = int(entry.get("value", 50))
                        self._label = entry.get("value_classification", "Neutral")
                        self._last_fetch = datetime.utcnow()
                        self._fail_count = 0
                        logger.info(f"[FearGreed] {self._value} — {self._label}")
                    else:
                        self._fail_count += 1
        except Exception as e:
            self._fail_count += 1
            logger.debug(f"[FearGreed] fetch error: {e}")


_fg_cache: Optional[FearGreedCache] = None


def get_fear_greed() -> FearGreedCache:
    global _fg_cache
    if _fg_cache is None:
        _fg_cache = FearGreedCache()
    return _fg_cache
