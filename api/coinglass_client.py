"""
Coinglass API Client
Отличные данные: ликвидации, funding heatmap, OI, L/S ratio
Нужен API ключ: https://coinglass.com/pricing
"""

import os
import asyncio
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import aiohttp


@dataclass
class LiquidationData:
    """Данные о ликвидациях"""
    symbol: str
    long_liquidations: float  # Объём ликвидаций лонгов
    short_liquidations: float  # Объём ликвидаций шортов
    total_liquidations: float
    long_liquidation_amount: float  # Сумма в USD
    short_liquidation_amount: float
    liquidation_density: str  # 'high', 'medium', 'low'
    timestamp: datetime


@dataclass
class FundingHeatmap:
    """Heatmap фандинга"""
    symbol: str
    current_funding: float
    avg_24h_funding: float
    extreme_positive: bool  # > 0.1%
    extreme_negative: bool  # < -0.1%
    heatmap_data: List[Dict]  # Исторические данные


@dataclass
class OpenInterestAnalysis:
    """Анализ OI"""
    symbol: str
    total_oi: float
    oi_change_24h: float
    oi_change_1h: float
    price_correlation: float  # Корреляция OI и цены
    signal: str  # 'increase_longs', 'increase_shorts', 'decrease', 'neutral'


class CoinglassClient:
    """
    Coinglass API Client
    https://coinglass.github.io/coinglass-api/
    """
    
    BASE_URL = "https://open-api.coinglass.com"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Инициализация Coinglass клиента
        
        Args:
            api_key: API ключ (получить на https://coinglass.com/pricing)
                   Бесплатный tier: 30 запросов/мин
        """
        self.api_key = api_key or os.getenv("COINGLASS_API_KEY", "")
        
        if not self.api_key:
            print("⚠️ Coinglass API key not provided")
            print("Get key at: https://coinglass.com/pricing")
            print("Free tier: 30 requests/min")
        
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiting
        self.last_request_time = 0
        self.min_interval = 2.0  # 2 секунды между запросами (30/min)
        
        print("🚀 Coinglass Client initialized")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получить или создать сессию"""
        if self.session is None or self.session.closed:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            headers["Content-Type"] = "application/json"
            
            self.session = aiohttp.ClientSession(headers=headers)
        return self.session
    
    async def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Выполнить запрос с rate limiting"""
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
                    print("Coinglass Rate limit hit, waiting 60s...")
                    await asyncio.sleep(60)
                    return await self._make_request(endpoint, params)
                else:
                    error_text = await response.text()
                    print(f"Coinglass Error {response.status}: {error_text}")
                    return None
        
        except Exception as e:
            print(f"Coinglass Request error: {e}")
            return None
    
    async def close(self):
        """Закрыть сессию"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    # =========================================================================
    # LIQUIDATION DATA (Ключевой endpoint!)
    # =========================================================================
    
    async def get_liquidation_data(self, 
                                  symbol: str,
                                  time_type: str = "1h",
                                  limit: int = 24) -> Optional[LiquidationData]:
        """
        Получить данные о ликвидациях
        
        Args:
            symbol: Торговая пара (BTC)
            time_type: 1h, 4h, 12h, 24h
            limit: Количество периодов
        
        Returns:
            LiquidationData с объёмами ликвидаций
        """
        if not self.api_key:
            return None
        
        result = await self._make_request(
            "/public/v2/liquidation",
            params={
                "symbol": symbol,
                "timeType": time_type,
                "limit": limit
            }
        )
        
        if not result or result.get("code") != "0":
            return None
        
        data = result.get("data", [])
        if not data:
            return None
        
        # Агрегируем данные
        total_long_liq = sum(item.get("longVolUsd", 0) for item in data)
        total_short_liq = sum(item.get("shortVolUsd", 0) for item in data)
        
        # Определяем плотность ликвидаций
        total = total_long_liq + total_short_liq
        if total > 100_000_000:  # > $100M
            density = "high"
        elif total > 50_000_000:  # > $50M
            density = "medium"
        else:
            density = "low"
        
        return LiquidationData(
            symbol=symbol,
            long_liquidations=sum(item.get("longVol", 0) for item in data),
            short_liquidations=sum(item.get("shortVol", 0) for item in data),
            total_liquidations=sum(item.get("vol", 0) for item in data),
            long_liquidation_amount=total_long_liq,
            short_liquidation_amount=total_short_liq,
            liquidation_density=density,
            timestamp=datetime.utcnow()
        )
    
    async def get_liquidation_heatmap(self, 
                                     symbol: str,
                                     range: str = "1d") -> Optional[Dict]:
        """
        Получить heatmap ликвидаций (где сосредоточены стопы)
        
        Args:
            symbol: Торговая пара
            range: 1d, 3d, 7d, 30d
        """
        if not self.api_key:
            return None
        
        result = await self._make_request(
            "/public/v2/liquidation_heatmap",
            params={
                "symbol": symbol,
                "range": range
            }
        )
        
        if result and result.get("code") == "0":
            return result.get("data", {})
        return None
    
    # =========================================================================
    # FUNDING RATE
    # =========================================================================
    
    async def get_funding_rate(self, symbol: str) -> Optional[FundingHeatmap]:
        """
        Получить детальный фандинг рейт
        """
        if not self.api_key:
            return None
        
        result = await self._make_request(
            "/public/v2/funding",
            params={"symbol": symbol}
        )
        
        if not result or result.get("code") != "0":
            return None
        
        data = result.get("data", [])
        if not data:
            return None
        
        # Текущий фандинг
        current = data[0].get("rate", 0) if data else 0
        
        # Средний за 24h
        avg_24h = sum(item.get("rate", 0) for item in data[:24]) / min(len(data), 24) if data else 0
        
        # Проверяем экстремальные значения
        extreme_pos = current > 0.001  # > 0.1%
        extreme_neg = current < -0.001  # < -0.1%
        
        return FundingHeatmap(
            symbol=symbol,
            current_funding=current * 100,  # В процентах
            avg_24h_funding=avg_24h * 100,
            extreme_positive=extreme_pos,
            extreme_negative=extreme_neg,
            heatmap_data=data[:24]  # Последние 24 точки
        )
    
    async def get_funding_heatmap_all(self) -> Optional[List[Dict]]:
        """
        Получить фандинг всех монет (для сканирования)
        """
        if not self.api_key:
            return None
        
        result = await self._make_request(
            "/public/v2/funding_ex",
            params={"type": "C"}  # C = Cryptocurrency
        )
        
        if result and result.get("code") == "0":
            return result.get("data", [])
        return None
    
    # =========================================================================
    # OPEN INTEREST
    # =========================================================================
    
    async def get_open_interest(self, symbol: str, interval: str = "h1") -> Optional[OpenInterestAnalysis]:
        """
        Получить анализ Open Interest
        """
        if not self.api_key:
            return None
        
        result = await self._make_request(
            "/public/v2/open_interest",
            params={
                "symbol": symbol,
                "interval": interval
            }
        )
        
        if not result or result.get("code") != "0":
            return None
        
        data = result.get("data", [])
        if not data:
            return None
        
        # Рассчитываем изменения
        current_oi = data[0].get("oi", 0) if data else 0
        oi_24h_ago = data[-1].get("oi", 0) if len(data) > 24 else data[-1].get("oi", 0)
        oi_change_24h = ((current_oi - oi_24h_ago) / oi_24h_ago * 100) if oi_24h_ago else 0
        
        # Определяем сигнал
        # Нужно сравнивать с изменением цены (нужен отдельный запрос)
        signal = "neutral"
        if oi_change_24h > 10:
            signal = "increase_positions"
        elif oi_change_24h < -10:
            signal = "decrease_positions"
        
        return OpenInterestAnalysis(
            symbol=symbol,
            total_oi=current_oi,
            oi_change_24h=oi_change_24h,
            oi_change_1h=0.0,  # Рассчитать если нужно
            price_correlation=0.0,  # Требует дополнительных данных
            signal=signal
        )
    
    async def get_oi_heatmap(self, symbol: str, range: str = "1d") -> Optional[Dict]:
        """OI heatmap"""
        if not self.api_key:
            return None
        
        result = await self._make_request(
            "/public/v2/open_interest_heatmap",
            params={
                "symbol": symbol,
                "range": range
            }
        )
        
        if result and result.get("code") == "0":
            return result.get("data", {})
        return None
    
    # =========================================================================
    # LONG/SHORT RATIO
    # =========================================================================
    
    async def get_long_short_ratio(self, symbol: str) -> Optional[Dict]:
        """
        Получить соотношение лонг/шорт
        """
        if not self.api_key:
            return None
        
        result = await self._make_request(
            "/public/v2/indicator/ls_ratio",
            params={"symbol": symbol}
        )
        
        if result and result.get("code") == "0":
            return result.get("data", {})
        return None
    
    async def get_long_short_accounts_ratio(self, symbol: str) -> Optional[Dict]:
        """
        Получить соотношение по аккаунтам (не объёмам)
        """
        if not self.api_key:
            return None
        
        result = await self._make_request(
            "/public/v2/indicator/ls_accounts_ratio",
            params={"symbol": symbol}
        )
        
        if result and result.get("code") == "0":
            return result.get("data", {})
        return None
    
    # =========================================================================
    # AGGREGATED ANALYSIS
    # =========================================================================
    
    async def get_liquidation_signal(self, symbol: str) -> Optional[Dict]:
        """
        Получить сигнал на основе ликвидаций
        
        Returns:
            {
                "signal": "short" | "long" | "neutral",
                "strength": 0-100,
                "reason": str,
                "data": LiquidationData
            }
        """
        data = await self.get_liquidation_data(symbol, time_type="1h", limit=4)
        
        if not data:
            return None
        
        # Анализируем ликвидации
        long_liq = data.long_liquidation_amount
        short_liq = data.short_liquidation_amount
        total = long_liq + short_liq
        
        if total < 1_000_000:  # Мало ликвидаций
            return {
                "signal": "neutral",
                "strength": 0,
                "reason": "Low liquidation activity",
                "data": data
            }
        
        # Если ликвидировано много лонгов → шорты выиграли, возможен отскок вверх
        if long_liq > short_liq * 2:  # В 2 раза больше ликвидаций лонгов
            return {
                "signal": "long",
                "strength": min(100, int((long_liq / short_liq) * 20)),
                "reason": f"Heavy long liquidations: ${long_liq:,.0f} vs ${short_liq:,.0f}",
                "data": data
            }
        
        # Если ликвидировано много шортов → лонги выиграли, возможна коррекция вниз
        if short_liq > long_liq * 2:
            return {
                "signal": "short",
                "strength": min(100, int((short_liq / long_liq) * 20)),
                "reason": f"Heavy short liquidations: ${short_liq:,.0f} vs ${long_liq:,.0f}",
                "data": data
            }
        
        return {
            "signal": "neutral",
            "strength": 0,
            "reason": "Balanced liquidations",
            "data": data
        }
    
    async def get_market_sentiment_detailed(self, symbol: str) -> Optional[Dict]:
        """
        Детальное настроение рынка из нескольких источников
        """
        sentiment = {
            "symbol": symbol,
            "timestamp": datetime.utcnow().isoformat(),
            "overall": "neutral",
            "score": 50,
            "factors": {}
        }
        
        # Ликвидации
        liq_signal = await self.get_liquidation_signal(symbol)
        if liq_signal:
            sentiment["factors"]["liquidations"] = {
                "signal": liq_signal["signal"],
                "strength": liq_signal["strength"],
                "reason": liq_signal["reason"]
            }
        
        # Фандинг
        funding = await self.get_funding_rate(symbol)
        if funding:
            sentiment["factors"]["funding"] = {
                "current": funding.current_funding,
                "extreme": funding.extreme_positive or funding.extreme_negative,
                "direction": "shorts_pay" if funding.current_funding > 0 else "longs_pay"
            }
        
        # L/S Ratio
        ls_ratio = await self.get_long_short_ratio(symbol)
        if ls_ratio:
            sentiment["factors"]["ls_ratio"] = ls_ratio
        
        # Рассчитываем общий скор
        score = 50
        
        if liq_signal:
            if liq_signal["signal"] == "long":
                score += liq_signal["strength"] * 0.3
            elif liq_signal["signal"] == "short":
                score -= liq_signal["strength"] * 0.3
        
        if funding:
            if funding.extreme_positive:
                score -= 10  # Перекупленность
            elif funding.extreme_negative:
                score += 10  # Перепроданность
        
        sentiment["score"] = max(0, min(100, int(score)))
        
        if sentiment["score"] > 60:
            sentiment["overall"] = "bullish"
        elif sentiment["score"] < 40:
            sentiment["overall"] = "bearish"
        
        return sentiment


# ============================================================================
# SINGLETON
# ============================================================================

_coinglass_client = None

def get_coinglass_client() -> CoinglassClient:
    """Получить singleton Coinglass клиент"""
    global _coinglass_client
    if _coinglass_client is None:
        _coinglass_client = CoinglassClient()
    return _coinglass_client


# ============================================================================
# EXAMPLE
# ============================================================================

async def test_coinglass():
    """Тест Coinglass API"""
    import os
    
    client = CoinglassClient(
        api_key=os.getenv("COINGLASS_API_KEY")
    )
    
    if not client.api_key:
        print("❌ Coinglass API key not set")
        print("Get key at: https://coinglass.com/pricing")
        return
    
    # Ликвидации
    print("\n💥 Liquidations (BTC):")
    liq = await client.get_liquidation_data("BTC", time_type="1h", limit=4)
    if liq:
        print(f"  Long liquidations: ${liq.long_liquidation_amount:,.0f}")
        print(f"  Short liquidations: ${liq.short_liquidation_amount:,.0f}")
        print(f"  Density: {liq.liquidation_density}")
    
    # Сигнал на основе ликвидаций
    print("\n📊 Liquidation Signal:")
    signal = await client.get_liquidation_signal("BTC")
    if signal:
        print(f"  Signal: {signal['signal']} (strength: {signal['strength']})")
        print(f"  Reason: {signal['reason']}")
    
    # Фандинг
    print("\n💰 Funding Rate:")
    funding = await client.get_funding_rate("BTC")
    if funding:
        print(f"  Current: {funding.current_funding:.4f}%")
        print(f"  24h Avg: {funding.avg_24h_funding:.4f}%")
        print(f"  Extreme: {funding.extreme_positive or funding.extreme_negative}")
    
    # Настроение
    print("\n🎭 Market Sentiment:")
    sentiment = await client.get_market_sentiment_detailed("BTC")
    if sentiment:
        print(f"  Overall: {sentiment['overall']}")
        print(f"  Score: {sentiment['score']}/100")
    
    await client.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_coinglass())
