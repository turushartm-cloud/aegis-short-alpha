"""
Bybit Futures API Client
V5 API - актуальная версия
"""

import os
import json
import hmac
import hashlib
import time
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import aiohttp


@dataclass
class BybitMarketData:
    """Рыночные данные Bybit"""
    symbol: str
    price: float
    funding_rate: float
    funding_time: int
    open_interest: float
    oi_change_24h: float
    volume_24h: float
    volume_change_24h: float
    price_change_24h: float
    long_short_ratio: float  # long/short
    buy_ratio: float  # buy/sell ratio
    last_updated: datetime


class BybitClient:
    """
    Bybit V5 API Client
    Поддержка Unified Trading Account (UTA)
    """
    
    # Mainnet
    MAINNET_URL = "https://api.bybit.com"
    # Testnet
    TESTNET_URL = "https://api-testnet.bybit.com"
    
    def __init__(self,
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 testnet: bool = True):
        """
        Инициализация Bybit клиента
        
        Args:
            api_key: API ключ
            api_secret: API секрет
            testnet: True для тестовой сети
        """
        self.api_key = api_key or os.getenv("BYBIT_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BYBIT_API_SECRET", "")
        self.testnet = testnet
        
        self.base_url = self.TESTNET_URL if testnet else self.MAINNET_URL
        self.session: Optional[aiohttp.ClientSession] = None
        
        print(f"🚀 Bybit Client initialized ({'TESTNET' if testnet else 'MAINNET'})")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получить или создать сессию"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    def _generate_signature(self, payload: str) -> str:
        """Генерация подписи для приватных запросов"""
        timestamp = str(int(time.time() * 1000))
        sign_str = timestamp + self.api_key + payload
        
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            sign_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature, timestamp
    
    async def _make_request(self,
                           method: str,
                           endpoint: str,
                           params: Optional[Dict] = None,
                           signed: bool = False) -> Optional[Dict]:
        """
        Выполнить запрос к API
        
        Args:
            method: GET или POST
            endpoint: API endpoint
            params: Параметры
            signed: Требуется ли подпись
        """
        try:
            url = f"{self.base_url}{endpoint}"
            params = params or {}
            
            headers = {
                "Content-Type": "application/json"
            }
            
            if signed and self.api_key and self.api_secret:
                payload = json.dumps(params) if method == "POST" else ""
                signature, timestamp = self._generate_signature(payload)
                
                headers.update({
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-SIGN": signature,
                    "X-BAPI-RECV-WINDOW": "5000"
                })
            
            session = await self._get_session()
            
            if method == "GET":
                async with session.get(url, params=params, headers=headers, timeout=30) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        error = await resp.text()
                        print(f"Bybit Error {resp.status}: {error}")
                        return None
            
            elif method == "POST":
                async with session.post(url, json=params, headers=headers, timeout=30) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        error = await resp.text()
                        print(f"Bybit Error {resp.status}: {error}")
                        return None
        
        except Exception as e:
            print(f"Request error: {e}")
            return None
    
    async def close(self):
        """Закрыть сессию"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    # =========================================================================
    # PUBLIC ENDPOINTS (Маркет данные)
    # =========================================================================
    
    async def get_server_time(self) -> Optional[int]:
        """Получить серверное время"""
        result = await self._make_request("GET", "/v5/market/time")
        if result and result.get("retCode") == 0:
            return result["result"].get("timeSecond")
        return None
    
    async def get_symbols(self, category: str = "linear") -> List[str]:
        """
        Получить список торговых пар
        
        Args:
            category: 'linear' (USDT perp), 'inverse', 'spot'
        """
        result = await self._make_request(
            "GET",
            "/v5/market/instruments-info",
            params={"category": category}
        )
        
        symbols = []
        if result and result.get("retCode") == 0:
            for item in result["result"].get("list", []):
                symbol = item.get("symbol")
                # Только USDT перпетуал
                if symbol and symbol.endswith("USDT"):
                    symbols.append(symbol)
        
        return symbols
    
    async def get_price(self, symbol: str) -> Optional[float]:
        """Получить текущую цену (mark price)"""
        result = await self._make_request(
            "GET",
            "/v5/market/tickers",
            params={
                "category": "linear",
                "symbol": symbol
            }
        )
        
        if result and result.get("retCode") == 0:
            tickers = result["result"].get("list", [])
            if tickers:
                return float(tickers[0].get("lastPrice", 0))
        return None
    
    async def get_klines(self,
                        symbol: str,
                        interval: str = "15",
                        limit: int = 200) -> Optional[List]:
        """
        Получить свечи (OHLCV)
        
        Args:
            symbol: Торговая пара
            interval: 1, 3, 5, 15, 30, 60, 120, 240, D, W, M
            limit: Количество свечей (max 1000)
        """
        result = await self._make_request(
            "GET",
            "/v5/market/kline",
            params={
                "category": "linear",
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            }
        )
        
        if result and result.get("retCode") == 0:
            return result["result"].get("list", [])
        return None
    
    async def get_funding_rate(self, symbol: str) -> Optional[Dict]:
        """Получить текущий и предстоящий фандинг рейт"""
        result = await self._make_request(
            "GET",
            "/v5/market/funding/history",
            params={
                "category": "linear",
                "symbol": symbol,
                "limit": 1
            }
        )
        
        if result and result.get("retCode") == 0:
            funding_data = result["result"].get("list", [])
            if funding_data:
                return {
                    "rate": float(funding_data[0].get("fundingRate", 0)),
                    "timestamp": int(funding_data[0].get("fundingRateTimestamp", 0))
                }
        return None
    
    async def get_open_interest(self, symbol: str, interval: str = "15min") -> Optional[float]:
        """Получить Open Interest"""
        result = await self._make_request(
            "GET",
            "/v5/market/open-interest",
            params={
                "category": "linear",
                "symbol": symbol,
                "interval": interval,
                "limit": 1
            }
        )
        
        if result and result.get("retCode") == 0:
            oi_data = result["result"].get("list", [])
            if oi_data:
                return float(oi_data[0].get("openInterest", 0))
        return None
    
    async def get_long_short_ratio(self, symbol: str) -> Optional[Dict]:
        """Получить соотношение лонг/шорт"""
        result = await self._make_request(
            "GET",
            "/v5/market/account-ratio",
            params={
                "category": "linear",
                "symbol": symbol,
                "period": "15min",
                "limit": 1
            }
        )
        
        if result and result.get("retCode") == 0:
            ratio_data = result["result"].get("list", [])
            if ratio_data:
                return {
                    "long_ratio": float(ratio_data[0].get("longRatio", 0.5)),
                    "short_ratio": float(ratio_data[0].get("shortRatio", 0.5)),
                    "timestamp": int(ratio_data[0].get("timestamp", 0))
                }
        return None
    
    async def get_24h_ticker(self, symbol: str) -> Optional[Dict]:
        """Получить статистику за 24 часа"""
        result = await self._make_request(
            "GET",
            "/v5/market/tickers",
            params={
                "category": "linear",
                "symbol": symbol
            }
        )
        
        if result and result.get("retCode") == 0:
            tickers = result["result"].get("list", [])
            if tickers:
                return tickers[0]
        return None
    
    async def get_complete_market_data(self, symbol: str) -> Optional[BybitMarketData]:
        """
        Получить полные рыночные данные
        Агрегирует несколько endpoint'ов
        """
        try:
            # Получаем все данные параллельно
            price_task = self.get_price(symbol)
            funding_task = self.get_funding_rate(symbol)
            oi_task = self.get_open_interest(symbol)
            ratio_task = self.get_long_short_ratio(symbol)
            ticker_task = self.get_24h_ticker(symbol)
            
            price, funding, oi, ratio, ticker = await asyncio.gather(
                price_task, funding_task, oi_task, ratio_task, ticker_task
            )
            
            if not price:
                return None
            
            # Рассчитываем изменение OI (если есть данные)
            oi_change = 0.0
            if ticker:
                oi_change = float(ticker.get("oiChange", 0))
            
            return BybitMarketData(
                symbol=symbol,
                price=price,
                funding_rate=funding["rate"] * 100 if funding else 0.0,  # В процентах
                funding_time=funding["timestamp"] if funding else 0,
                open_interest=oi or 0.0,
                oi_change_24h=oi_change,
                volume_24h=float(ticker["volume24h"]) if ticker else 0.0,
                volume_change_24h=0.0,  # Нужно рассчитывать отдельно
                price_change_24h=float(ticker["price24hPcnt"]) * 100 if ticker else 0.0,
                long_short_ratio=ratio["long_ratio"] * 100 if ratio else 50.0,
                buy_ratio=50.0,  # Bybit не предоставляет напрямую
                last_updated=datetime.utcnow()
            )
        
        except Exception as e:
            print(f"Error getting Bybit data for {symbol}: {e}")
            return None
    
    # =========================================================================
    # PRIVATE ENDPOINTS (Торговля)
    # =========================================================================
    
    async def get_wallet_balance(self, account_type: str = "UNIFIED") -> Optional[Dict]:
        """Получить баланс кошелька"""
        result = await self._make_request(
            "GET",
            "/v5/account/wallet-balance",
            params={"accountType": account_type},
            signed=True
        )
        
        if result and result.get("retCode") == 0:
            return result["result"]
        return None
    
    async def get_positions(self, symbol: Optional[str] = None) -> Optional[List]:
        """Получить открытые позиции"""
        params = {
            "category": "linear",
            "settleCoin": "USDT"
        }
        if symbol:
            params["symbol"] = symbol
        
        result = await self._make_request(
            "GET",
            "/v5/position/list",
            params=params,
            signed=True
        )
        
        if result and result.get("retCode") == 0:
            return result["result"].get("list", [])
        return None
    
    async def place_order(self,
                         symbol: str,
                         side: str,  # Buy или Sell
                         order_type: str,  # Market, Limit
                         qty: float,
                         price: Optional[float] = None,
                         stop_loss: Optional[float] = None,
                         take_profit: Optional[float] = None,
                         time_in_force: str = "GTC") -> Optional[Dict]:
        """
        Разместить ордер
        
        Args:
            symbol: Торговая пара
            side: Buy или Sell
            order_type: Market, Limit
            qty: Количество
            price: Цена (для Limit)
            stop_loss: Стоп-лосс
            take_profit: Тейк-профит
            time_in_force: GTC, IOC, FOK
        """
        params = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": str(qty),
            "timeInForce": time_in_force
        }
        
        if price and order_type == "Limit":
            params["price"] = str(price)
        
        if stop_loss:
            params["stopLoss"] = str(stop_loss)
            params["slTriggerBy"] = "LastPrice"
        
        if take_profit:
            params["takeProfit"] = str(take_profit)
            params["tpTriggerBy"] = "LastPrice"
        
        result = await self._make_request(
            "POST",
            "/v5/order/create",
            params=params,
            signed=True
        )
        
        if result and result.get("retCode") == 0:
            return result["result"]
        else:
            print(f"Order error: {result}")
            return None
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Отменить ордер"""
        result = await self._make_request(
            "POST",
            "/v5/order/cancel",
            params={
                "category": "linear",
                "symbol": symbol,
                "orderId": order_id
            },
            signed=True
        )
        
        return result and result.get("retCode") == 0
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Установить плечо"""
        result = await self._make_request(
            "POST",
            "/v5/position/set-leverage",
            params={
                "category": "linear",
                "symbol": symbol,
                "buyLeverage": str(leverage),
                "sellLeverage": str(leverage)
            },
            signed=True
        )
        
        if result and result.get("retCode") == 0:
            print(f"✅ Leverage set: {symbol} {leverage}x")
            return True
        else:
            print(f"Leverage error: {result}")
            return False
    
    async def close_position(self, symbol: str) -> bool:
        """Закрыть позицию по рыночной цене"""
        # Получаем текущую позицию
        positions = await self.get_positions(symbol)
        if not positions:
            print(f"No position to close for {symbol}")
            return False
        
        position = positions[0]
        size = float(position.get("size", 0))
        side = position.get("side", "")
        
        if size == 0:
            return False
        
        # Определяем сторону для закрытия
        close_side = "Sell" if side == "Buy" else "Buy"
        
        result = await self.place_order(
            symbol=symbol,
            side=close_side,
            order_type="Market",
            qty=abs(size)
        )
        
        if result:
            print(f"✅ Position closed: {symbol}")
            return True
        return False
    
    async def test_connection(self) -> bool:
        """Тест соединения"""
        try:
            server_time = await self.get_server_time()
            if server_time:
                print(f"✅ Bybit connection OK ({'TESTNET' if self.testnet else 'MAINNET'})")
                return True
            return False
        except Exception as e:
            print(f"❌ Bybit connection failed: {e}")
            return False


# ============================================================================
# SINGLETON
# ============================================================================

_bybit_client = None

def get_bybit_client(testnet: bool = True) -> BybitClient:
    """Получить singleton Bybit клиент"""
    global _bybit_client
    if _bybit_client is None:
        _bybit_client = BybitClient(testnet=testnet)
    return _bybit_client


# ============================================================================
# EXAMPLE
# ============================================================================

async def test_bybit():
    """Тест Bybit API"""
    import os
    
    client = BybitClient(
        api_key=os.getenv("BYBIT_API_KEY"),
        api_secret=os.getenv("BYBIT_API_SECRET"),
        testnet=True
    )
    
    # Тест соединения
    connected = await client.test_connection()
    if not connected:
        return
    
    # Получаем символы
    symbols = await client.get_symbols()
    print(f"Available symbols: {len(symbols)}")
    print(f"Top 5: {symbols[:5]}")
    
    # Цена BTC
    if symbols:
        price = await client.get_price(symbols[0])
        print(f"{symbols[0]} price: ${price}")
    
    # Фандинг
    if symbols:
        funding = await client.get_funding_rate(symbols[0])
        print(f"Funding: {funding}")
    
    await client.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_bybit())
