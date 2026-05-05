"""
OKX Public API Client — Open Interest & Funding Rate
Использует только публичные endpoints, API ключ НЕ нужен.

Docs: https://www.okx.com/docs-v5/en/#public-data-rest-api-get-open-interest
"""
import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://www.okx.com/api/v5"

@dataclass
class OKXOIData:
    symbol:        str     # BTCUSDT-style
    oi_usd:        float   # OI в USD на OKX
    oi_change_1h:  float   # % изменение за 1ч
    oi_change_4h:  float   # % изменение за 4ч
    oi_change_24h: float   # % изменение за 24ч
    funding_rate:  float   # текущий funding rate
    timestamp:     datetime


class OKXClient:
    """
    Клиент для публичного API OKX.
    Получает OI и funding для фьючерсов (SWAP).
    """

    _instance = None
    _session: Optional[aiohttp.ClientSession] = None

    def __init__(self, timeout: int = 8):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._cache: Dict[str, tuple] = {}    # symbol → (ts, OKXOIData)
        self._cache_ttl = 60                  # 60 сек кэш

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ─────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────

    def _to_okx_symbol(self, symbol: str) -> str:
        """BTCUSDT  →  BTC-USDT-SWAP"""
        base = symbol.replace("USDT", "").replace("1000", "")
        # Обработка 1000-символов (1000PEPE → PEPE)
        if symbol.startswith("1000"):
            base = symbol[4:].replace("USDT", "")
        return f"{base}-USDT-SWAP"

    async def _get(self, path: str, params: dict = None) -> Optional[dict]:
        session = await self._get_session()
        url = f"{BASE_URL}{path}"
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if data.get("code") != "0":
                    return None
                return data
        except Exception as e:
            logger.debug(f"OKX request error {path}: {e}")
            return None

    # ─────────────────────────────────────────────
    # Public methods
    # ─────────────────────────────────────────────

    async def get_liquidations(self, symbol: str, limit: int = 20) -> Optional[Dict]:
        """
        Ликвидации с OKX (/public/liquidation-orders).
        Fallback когда Binance/Coinglass недоступны.

        Возвращает: {
            "total_usd":      float,  # суммарный объём ликвидаций USD
            "long_usd":       float,  # ликвидации лонгов
            "short_usd":      float,  # ликвидации шортов
            "dominant_side":  str,    # "long" | "short" | "neutral"
            "count":          int,
        }
        """
        okx_sym = self._to_okx_symbol(symbol)
        data = await self._get(
            "/public/liquidation-orders",
            {"instType": "SWAP", "instId": okx_sym, "state": "filled"}
        )
        if not data or not data.get("data"):
            return None

        orders = data["data"]
        long_usd = short_usd = 0.0
        for order in orders[:limit]:
            # OKX: side="buy" = ликвидация шорта, side="sell" = ликвидация лонга
            try:
                sz    = float(order.get("sz", 0))
                px    = float(order.get("bkPx", 0))   # bankruptcy price
                usd   = sz * px
                side  = order.get("side", "")
                if side == "sell":    long_usd  += usd  # лонг ликвидирован
                elif side == "buy":   short_usd += usd  # шорт ликвидирован
            except (ValueError, TypeError):
                continue

        total_usd = long_usd + short_usd
        if total_usd == 0:
            return None

        dominant = (
            "long"    if long_usd  > short_usd * 1.5 else
            "short"   if short_usd > long_usd  * 1.5 else
            "neutral"
        )
        return {
            "total_usd":     round(total_usd, 2),
            "long_usd":      round(long_usd, 2),
            "short_usd":     round(short_usd, 2),
            "dominant_side": dominant,
            "count":         len(orders),
            "source":        "okx",
        }

    async def get_open_interest(self, symbol: str) -> Optional[OKXOIData]:
        """
        Получить OI с OKX для символа (BTCUSDT формат).
        Возвращает OKXOIData или None.
        """
        # Кэш
        if symbol in self._cache:
            ts, data = self._cache[symbol]
            if (datetime.utcnow() - ts).total_seconds() < self._cache_ttl:
                return data

        okx_sym = self._to_okx_symbol(symbol)

        # Текущий OI
        current = await self._get("/public/open-interest", {"instType": "SWAP", "instId": okx_sym})
        if not current or not current.get("data"):
            return None

        current_oi_usd = float(current["data"][0].get("oiUsd", 0))
        if current_oi_usd == 0:
            return None

        # История OI (для расчёта изменений)
        oi_1h, oi_4h, oi_24h = 0.0, 0.0, 0.0
        history = await self._get(
            "/public/open-interest-history",
            {
                "instType": "SWAP",
                "instId": okx_sym,
                "period": "1H",
                "limit": "25",   # 24 часа + запас
            }
        )
        if history and history.get("data"):
            rows = history["data"]  # [ts, oi, oiUsd, ...]
            # Строки в порядке убывания времени (новейшие первые)
            try:
                rows_sorted = sorted(rows, key=lambda r: int(r[0]), reverse=True)
                if len(rows_sorted) >= 2:
                    oi_1h = self._pct_change(
                        float(rows_sorted[0][2]),  # текущий
                        float(rows_sorted[1][2])   # 1ч назад
                    )
                if len(rows_sorted) >= 5:
                    oi_4h = self._pct_change(
                        float(rows_sorted[0][2]),
                        float(rows_sorted[4][2])
                    )
                if len(rows_sorted) >= 25:
                    oi_24h = self._pct_change(
                        float(rows_sorted[0][2]),
                        float(rows_sorted[24][2])
                    )
            except (IndexError, ValueError):
                pass

        # Текущий funding rate
        funding_rate = 0.0
        funding = await self._get("/public/funding-rate", {"instId": okx_sym})
        if funding and funding.get("data"):
            funding_rate = float(funding["data"][0].get("fundingRate", 0)) * 100  # в %

        result = OKXOIData(
            symbol=symbol,
            oi_usd=current_oi_usd,
            oi_change_1h=round(oi_1h, 2),
            oi_change_4h=round(oi_4h, 2),
            oi_change_24h=round(oi_24h, 2),
            funding_rate=round(funding_rate, 4),
            timestamp=datetime.utcnow(),
        )

        self._cache[symbol] = (datetime.utcnow(), result)
        return result

    @staticmethod
    def _pct_change(new: float, old: float) -> float:
        if old == 0:
            return 0.0
        return round((new - old) / old * 100, 2)


# Singleton
_okx_client: Optional[OKXClient] = None

def get_okx_client() -> OKXClient:
    global _okx_client
    if _okx_client is None:
        _okx_client = OKXClient()
    return _okx_client
