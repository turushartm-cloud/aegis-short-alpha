"""
Coinglass API v2 Client — Exchange Netflow & Liquidation Data
Требует COINGLASS_API_KEY (в render.yaml → Render Dashboard).
Docs: https://coinglass.com/api
"""
import os
import aiohttp
from typing import Optional, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://open-api.coinglass.com"
CACHE_TTL = 300  # 5 минут — netflow не меняется каждую секунду


class CoinglassClient:
    """
    Клиент для Coinglass API v2.
    Получает данные exchange netflow для определения накопления/распределения.
    """

    def __init__(self, api_key: str = None, timeout: int = 10):
        self.api_key = api_key or os.getenv("COINGLASS_API_KEY", "")
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: Dict[str, tuple] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={
                    "coinglassSecret": self.api_key,
                    "User-Agent": "AEGIS-Bot/1.0",
                },
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, path: str, params: dict = None) -> Optional[Any]:
        """Базовый GET запрос с обработкой ошибок."""
        if not self.api_key:
            logger.debug("[Coinglass] API key not set")
            return None
        session = await self._get_session()
        try:
            async with session.get(f"{BASE_URL}{path}", params=params) as resp:
                if resp.status == 500:
                    logger.debug(f"[Coinglass] HTTP 500 — API временно недоступен: {path}")
                    return None
                if resp.status != 200:
                    logger.debug(f"[Coinglass] HTTP {resp.status}: {path}")
                    return None
                body = await resp.json()
                if body.get("code") not in ("0", 0, "200", 200):
                    logger.debug(f"[Coinglass] API error code={body.get('code')}: {path}")
                    return None
                return body.get("data")
        except Exception as e:
            logger.debug(f"[Coinglass] Request error {path}: {e}")
            return None

    # ──────────────────────────────────────────────────────────────
    # Exchange Netflow
    # ──────────────────────────────────────────────────────────────

    async def get_exchange_netflow(self, symbol: str, period: str = "8h") -> Optional[Dict]:
        """
        Netflow монет на/с биржи за период.
        period: "1h" | "4h" | "8h" | "24h"

        Возвращает:
          {
            "total_netflow": float,   # отрицательный = outflow (BULLISH для LONG)
            "inflow":        float,
            "outflow":       float,
            "exchange_count": int,
          }
        """
        cache_key = f"netflow:{symbol}:{period}"
        if cache_key in self._cache:
            ts, cached = self._cache[cache_key]
            if (datetime.utcnow() - ts).total_seconds() < CACHE_TTL:
                return cached

        clean = symbol.replace("USDT", "").replace("1000", "")
        data = await self._get("/public/v2/indicator/exchange_netflow",
                               {"symbol": clean, "type": period})
        if not data:
            return None

        # data может быть списком по биржам или агрегатом
        if isinstance(data, list):
            total_in = total_out = 0.0
            count = 0
            for rec in data:
                if isinstance(rec, dict):
                    total_in  += float(rec.get("inflow",  0) or 0)
                    total_out += float(rec.get("outflow", 0) or 0)
                    count += 1
            if count == 0:
                return None
            result = {
                "total_netflow": total_in - total_out,
                "inflow":        total_in,
                "outflow":       total_out,
                "exchange_count": count,
            }
        elif isinstance(data, dict):
            total_in  = float(data.get("inflow",  0) or 0)
            total_out = float(data.get("outflow", 0) or 0)
            result = {
                "total_netflow": total_in - total_out,
                "inflow":        total_in,
                "outflow":       total_out,
                "exchange_count": 1,
            }
        else:
            return None

        self._cache[cache_key] = (datetime.utcnow(), result)
        return result


# Singleton
_coinglass_client: Optional[CoinglassClient] = None


def get_coinglass_client() -> CoinglassClient:
    global _coinglass_client
    if _coinglass_client is None:
        _coinglass_client = CoinglassClient()
    return _coinglass_client
