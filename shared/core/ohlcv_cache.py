"""
OHLCVCache — A5
In-memory кеш OHLCV данных внутри одного скан-цикла.

Проблема: scan_symbol() вызывается параллельно для 200-1000 символов.
Каждый вызов делает 3 запроса get_klines (15m/30m/4h).
При 484 символах = 1452 запроса, BTC-канал загружается 484 раза.

Решение: кеш с TTL = SCAN_INTERVAL. В пределах одного цикла каждый
(symbol, interval, limit) запрашивается максимум 1 раз.

Использование:
    cache = OHLCVCache(ttl_sec=180)  # = SCAN_INTERVAL

    # В scan_symbol вместо прямого вызова:
    ohlcv = await cache.get(symbol, "15m", 100, fetch_fn)

    # В начале scan_market():
    cache.cycle_reset()  # опционально, но явно очищает старые данные
"""
from __future__ import annotations
import asyncio
import time
import os
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

_TTL = int(os.getenv("OHLCV_CACHE_TTL", "0"))  # 0 = авто (= SCAN_INTERVAL)


class OHLCVCache:

    def __init__(self, ttl_sec: int = 0):
        self._ttl = ttl_sec or int(os.getenv("SCAN_INTERVAL", "180"))
        self._cache: Dict[Tuple, Any] = {}
        self._ts:    Dict[Tuple, float] = {}
        self._locks: Dict[Tuple, asyncio.Lock] = {}

    def _key(self, symbol: str, interval: str, limit: int) -> Tuple:
        return (symbol, interval, limit)

    def cycle_reset(self):
        """Явный сброс — вызывать в начале каждого scan_market()."""
        self._cache.clear()
        self._ts.clear()
        self._locks.clear()

    def _is_fresh(self, key: Tuple) -> bool:
        ts = self._ts.get(key)
        return ts is not None and (time.monotonic() - ts) < self._ttl

    async def get(
        self,
        symbol: str,
        interval: str,
        limit: int,
        fetch_fn: Callable[..., Awaitable[Any]],
    ) -> Any:
        """
        Возвращает кешированные свечи или вызывает fetch_fn однократно.

        fetch_fn — корутина без аргументов: `lambda: client.get_klines(sym, iv, lim)`
        """
        key = self._key(symbol, interval, limit)

        if self._is_fresh(key):
            return self._cache[key]

        # Per-key lock — исключает дублирование запроса при параллельном доступе
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        async with self._locks[key]:
            if self._is_fresh(key):
                return self._cache[key]
            result = await fetch_fn()
            self._cache[key] = result
            self._ts[key]    = time.monotonic()
            return result

    def stats(self) -> Dict:
        fresh = sum(1 for k in self._cache if self._is_fresh(k))
        return {"total": len(self._cache), "fresh": fresh, "ttl": self._ttl}


# Singleton per bot process
_instance: Optional[OHLCVCache] = None


def get_ohlcv_cache(ttl_sec: int = 0) -> OHLCVCache:
    global _instance
    if _instance is None:
        _instance = OHLCVCache(ttl_sec)
    return _instance
