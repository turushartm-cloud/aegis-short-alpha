"""
CoinMarketCap API Client
Бесплатный tier: 10,000 запросов/месяц
"""

import os
import asyncio
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from datetime import datetime
import aiohttp


@dataclass
class CMCMarketData:
    """Данные с CoinMarketCap"""
    symbol: str
    price: float
    market_cap: float
    volume_24h: float
    volume_change_24h: float
    price_change_1h: float
    price_change_24h: float
    price_change_7d: float
    price_change_30d: float
    circulating_supply: float
    total_supply: float
    max_supply: Optional[float]
    cmc_rank: int
    last_updated: datetime


class CoinMarketCapClient:
    """
    CoinMarketCap API Client
    https://coinmarketcap.com/api/documentation/v1/
    """
    
    BASE_URL = "https://pro-api.coinmarketcap.com"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Инициализация CMC клиента
        
        Args:
            api_key: API ключ (получить на https://coinmarketcap.com/api/)
        """
        self.api_key = api_key or os.getenv("COINMARKETCAP_API_KEY", "")
        
        if not self.api_key:
            print("⚠️ CMC API key not provided, using free tier limits")
        
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiting
        self.last_request_time = 0
        self.min_interval = 1.0  # 1 запрос в секунду (бесплатный tier)
        
        print("🚀 CoinMarketCap Client initialized")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получить или создать сессию"""
        if self.session is None or self.session.closed:
            headers = {
                "Accepts": "application/json",
                "X-CMC_PRO_API_KEY": self.api_key
            }
            self.session = aiohttp.ClientSession(headers=headers)
        return self.session
    
    async def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Выполнить запрос с rate limiting
        """
        import time
        
        # Rate limiting
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        
        try:
            url = f"{self.BASE_URL}{endpoint}"
            session = await self._get_session()
            
            async with session.get(url, params=params or {}, timeout=30) as response:
                self.last_request_time = time.time()
                
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    print("CMC Rate limit hit, waiting...")
                    await asyncio.sleep(60)
                    return await self._make_request(endpoint, params)
                else:
                    error_text = await response.text()
                    print(f"CMC Error {response.status}: {error_text}")
                    return None
        
        except Exception as e:
            print(f"CMC Request error: {e}")
            return None
    
    async def close(self):
        """Закрыть сессию"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    # =========================================================================
    # PUBLIC ENDPOINTS
    # =========================================================================
    
    async def get_cryptocurrency_map(self, limit: int = 100) -> Optional[List[Dict]]:
        """
        Получить маппинг ID → Symbol
        Полезно для конвертации символов в CMC ID
        """
        result = await self._make_request(
            "/v1/cryptocurrency/map",
            params={"limit": limit}
        )
        
        if result and result.get("status", {}).get("error_code") == 0:
            return result.get("data", [])
        return None
    
    async def get_quotes_latest(self, symbols: List[str]) -> Optional[Dict[str, CMCMarketData]]:
        """
        Получить актуальные котировки
        
        Args:
            symbols: Список символов (['BTC', 'ETH', 'SOL'])
        
        Returns:
            Словарь symbol → CMCMarketData
        """
        if not symbols:
            return {}
        
        # CMC принимает до 150 символов за раз
        if len(symbols) > 150:
            symbols = symbols[:150]
        
        result = await self._make_request(
            "/v1/cryptocurrency/quotes/latest",
            params={
                "symbol": ",".join(symbols),
                "convert": "USD"
            }
        )
        
        if not result or result.get("status", {}).get("error_code") != 0:
            return None
        
        data = result.get("data", {})
        quotes = {}
        
        for symbol, crypto_data in data.items():
            quote = crypto_data.get("quote", {}).get("USD", {})
            
            quotes[symbol] = CMCMarketData(
                symbol=symbol,
                price=quote.get("price", 0.0),
                market_cap=quote.get("market_cap", 0.0),
                volume_24h=quote.get("volume_24h", 0.0),
                volume_change_24h=quote.get("volume_change_24h", 0.0),
                price_change_1h=quote.get("percent_change_1h", 0.0),
                price_change_24h=quote.get("percent_change_24h", 0.0),
                price_change_7d=quote.get("percent_change_7d", 0.0),
                price_change_30d=quote.get("percent_change_30d", 0.0),
                circulating_supply=crypto_data.get("circulating_supply", 0.0),
                total_supply=crypto_data.get("total_supply", 0.0),
                max_supply=crypto_data.get("max_supply"),
                cmc_rank=crypto_data.get("cmc_rank", 0),
                last_updated=datetime.utcnow()
            )
        
        return quotes
    
    async def get_global_metrics(self) -> Optional[Dict]:
        """
        Получить глобальные метрики рынка
        Полезно для определения настроения рынка
        """
        result = await self._make_request("/v1/global-metrics/quotes/latest")
        
        if result and result.get("status", {}).get("error_code") == 0:
            data = result.get("data", {})
            quote = data.get("quote", {}).get("USD", {})
            
            return {
                "total_market_cap": quote.get("total_market_cap", 0),
                "total_volume_24h": quote.get("total_volume_24h", 0),
                "btc_dominance": data.get("btc_dominance", 0),
                "eth_dominance": data.get("eth_dominance", 0),
                "active_cryptocurrencies": data.get("active_cryptocurrencies", 0),
                "market_cap_change_24h": quote.get("total_market_cap_yesterday_percentage_change", 0)
            }
        return None
    
    async def get_trending_cryptocurrencies(self, limit: int = 10) -> Optional[List[Dict]]:
        """
        Получить трендовые криптовалюты
        Полезно для поиска "горячих" монет
        """
        result = await self._make_request(
            "/v1/cryptocurrency/trending/latest",
            params={"limit": limit}
        )
        
        if result and result.get("status", {}).get("error_code") == 0:
            return result.get("data", [])
        return None
    
    async def get_fear_greed_index(self) -> Optional[Dict]:
        """
        Получить Fear & Greed Index (если доступен)
        Альтернатива: alternative.me API
        """
        # CMC не предоставляет F&G напрямую
        # Вернём заглушку, можно добавить alternative.me позже
        return None
    
    async def get_futures_data(self, symbol: str) -> Optional[Dict]:
        """
        Получить данные о фьючерсах (если доступно на плане)
        На бесплатном tier может не работать
        """
        # Этот endpoint требует платный план
        print("⚠️ Futures data requires paid CMC plan")
        return None
    
    # =========================================================================
    # AGGREGATED DATA
    # =========================================================================
    
    async def get_market_sentiment(self) -> Dict:
        """
        Получить общее настроение рынка
        Агрегирует несколько метрик
        """
        sentiment = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall": "neutral",  # bullish, bearish, neutral
            "score": 50,  # 0-100
            "factors": {}
        }
        
        # Глобальные метрики
        global_metrics = await self.get_global_metrics()
        if global_metrics:
            # BTC доминирование
            btc_dom = global_metrics.get("btc_dominance", 50)
            sentiment["factors"]["btc_dominance"] = btc_dom
            
            # Изменение капитализации
            market_change = global_metrics.get("market_cap_change_24h", 0)
            sentiment["factors"]["market_cap_change_24h"] = market_change
            
            # Определяем настроение
            if market_change > 5:
                sentiment["overall"] = "very_bullish"
                sentiment["score"] = 75
            elif market_change > 2:
                sentiment["overall"] = "bullish"
                sentiment["score"] = 65
            elif market_change < -5:
                sentiment["overall"] = "very_bearish"
                sentiment["score"] = 25
            elif market_change < -2:
                sentiment["overall"] = "bearish"
                sentiment["score"] = 35
            else:
                sentiment["overall"] = "neutral"
                sentiment["score"] = 50
        
        return sentiment
    
    async def get_top_performers(self, limit: int = 20, period: str = "24h") -> Optional[List[Dict]]:
        """
        Получить топ performers (рост/падение)
        
        Args:
            limit: Количество
            period: Период (1h, 24h, 7d, 30d)
        """
        # Получаем топ 150 по капитализации
        result = await self._make_request(
            "/v1/cryptocurrency/listings/latest",
            params={
                "limit": 150,
                "convert": "USD",
                "sort": "market_cap"
            }
        )
        
        if not result or result.get("status", {}).get("error_code") != 0:
            return None
        
        data = result.get("data", [])
        performers = []
        
        for crypto in data:
            quote = crypto.get("quote", {}).get("USD", {})
            
            # Выбираем поле в зависимости от периода
            change_field = {
                "1h": "percent_change_1h",
                "24h": "percent_change_24h",
                "7d": "percent_change_7d",
                "30d": "percent_change_30d"
            }.get(period, "percent_change_24h")
            
            performers.append({
                "symbol": crypto.get("symbol"),
                "name": crypto.get("name"),
                "price": quote.get("price"),
                "change": quote.get(change_field, 0),
                "volume_24h": quote.get("volume_24h", 0),
                "market_cap": quote.get("market_cap", 0)
            })
        
        # Сортируем по изменению
        performers.sort(key=lambda x: x["change"], reverse=True)
        
        return performers[:limit]


# ============================================================================
# SINGLETON
# ============================================================================

_cmc_client = None

def get_coinmarketcap_client() -> CoinMarketCapClient:
    """Получить singleton CMC клиент"""
    global _cmc_client
    if _cmc_client is None:
        _cmc_client = CoinMarketCapClient()
    return _cmc_client


# ============================================================================
# EXAMPLE
# ============================================================================

async def test_cmc():
    """Тест CMC API"""
    import os
    
    client = CoinMarketCapClient(
        api_key=os.getenv("COINMARKETCAP_API_KEY")
    )
    
    # Проверяем ключ
    if not client.api_key:
        print("❌ CMC API key not set")
        print("Get free key at: https://coinmarketcap.com/api/")
        return
    
    # Глобальные метрики
    print("\n📊 Global Metrics:")
    global_metrics = await client.get_global_metrics()
    if global_metrics:
        print(f"  Total Market Cap: ${global_metrics['total_market_cap']:,.0f}")
        print(f"  BTC Dominance: {global_metrics['btc_dominance']:.1f}%")
        print(f"  24h Change: {global_metrics['market_cap_change_24h']:+.2f}%")
    
    # Котировки
    print("\n💰 Quotes:")
    quotes = await client.get_quotes_latest(["BTC", "ETH", "SOL"])
    if quotes:
        for symbol, data in quotes.items():
            print(f"  {symbol}: ${data.price:,.2f} ({data.price_change_24h:+.2f}%)")
    
    # Топ performers
    print("\n🔥 Top Performers (24h):")
    performers = await client.get_top_performers(limit=5)
    if performers:
        for p in performers[:3]:
            print(f"  {p['symbol']}: {p['change']:+.2f}%")
    
    # Настроение рынка
    print("\n🎭 Market Sentiment:")
    sentiment = await client.get_market_sentiment()
    print(f"  Overall: {sentiment['overall']}")
    print(f"  Score: {sentiment['score']}/100")
    
    await client.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_cmc())
