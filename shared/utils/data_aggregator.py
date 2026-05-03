"""
Multi-Source Data Aggregator
Агрегирует данные с Binance, Bybit, CoinMarketCap, Coinglass
Для максимальной точности сигналов и 70%+ Win Rate
"""

import os
import asyncio
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from api.binance_client import get_binance_client, MarketData as BinanceData
from api.bybit_client import get_bybit_client, BybitMarketData
from api.coinmarketcap_client import get_coinmarketcap_client, CMCMarketData
from api.coinglass_client import get_coinglass_client, LiquidationData


@dataclass
class AggregatedMarketData:
    """Агрегированные рыночные данные из всех источников"""
    symbol: str
    timestamp: datetime
    
    # Цены (средняя из всех источников)
    price: float
    price_binance: float
    price_bybit: float
    price_spread: float  # Спред между источниками
    
    # Индикаторы (усреднённые/максимальные)
    funding_rate: float  # Лучшая оценка
    funding_sources: List[Tuple[str, float]]  # [(binance, 0.01), (bybit, 0.012)]
    
    open_interest: float
    oi_change_24h: float
    oi_confidence: str  # 'high', 'medium', 'low' - на основе согласия источников
    
    long_short_ratio: float  # Среднее взвешенное
    ls_ratio_sources: List[Tuple[str, float]]
    
    volume_24h: float
    volume_change_24h: float
    
    price_change_24h: float
    price_change_1h: float
    
    # CoinMarketCap данные
    market_cap: Optional[float] = None
    cmc_rank: Optional[int] = None
    price_change_7d: Optional[float] = None
    price_change_30d: Optional[float] = None
    
    # Coinglass данные
    liquidations_long: float = 0.0
    liquidations_short: float = 0.0
    liquidation_density: str = "low"  # high, medium, low
    liquidation_signal: str = "neutral"  # long, short, neutral
    
    funding_extreme: bool = False  # Экстремальный фандинг
    funding_direction: str = "neutral"  # shorts_pay, longs_pay, neutral
    
    # Оценка качества данных
    data_quality_score: int = 0  # 0-100
    sources_used: int = 0
    sources_total: int = 4
    
    # Рекомендации
    confidence: str = "low"  # high, medium, low
    data_age_seconds: float = 0.0


class DataAggregator:
    """
    Агрегатор данных из нескольких источников
    Обеспечивает максимальную точность за счёт кросс-валидации
    """
    
    def __init__(self):
        # Клиенты
        self.binance = get_binance_client()
        self.bybit = get_bybit_client(testnet=True)  # Только для данных, не торговли
        self.cmc = get_coinmarketcap_client()
        self.coinglass = get_coinglass_client()
        
        # Кэш
        self._cache = {}
        self._cache_ttl = 30  # 30 секунд кэш
        
        print("🔄 DataAggregator initialized (Binance + Bybit + CMC + Coinglass)")
    
    async def get_aggregated_data(self, 
                                  symbol: str,
                                  force_refresh: bool = False) -> Optional[AggregatedMarketData]:
        """
        Получить агрегированные данные для символа
        
        Args:
            symbol: Торговая пара (BTCUSDT)
            force_refresh: Принудительно обновить данные
        
        Returns:
            AggregatedMarketData или None
        """
        # Проверяем кэш
        cache_key = f"agg:{symbol}"
        if not force_refresh and cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            if (datetime.utcnow() - cached_time).total_seconds() < self._cache_ttl:
                return cached_data
        
        start_time = datetime.utcnow()
        
        # Нормализуем символ для разных API
        binance_symbol = symbol  # BTCUSDT
        bybit_symbol = symbol.replace('USDT', '')  # BTC
        cmc_symbol = symbol.replace('USDT', '')  # BTC
        coinglass_symbol = symbol.replace('USDT', '')  # BTC
        
        # Собираем данные параллельно
        tasks = [
            self._get_binance_data(binance_symbol),
            self._get_bybit_data(symbol),  # Bybit использует полный символ
            self._get_cmc_data(cmc_symbol),
            self._get_coinglass_data(coinglass_symbol)
        ]
        
        binance_data, bybit_data, cmc_data, coinglass_data = await asyncio.gather(
            *tasks, return_exceptions=True
        )
        
        # Обрабатываем ошибки
        if isinstance(binance_data, Exception):
            print(f"Binance error for {symbol}: {binance_data}")
            binance_data = None
        if isinstance(bybit_data, Exception):
            print(f"Bybit error for {symbol}: {bybit_data}")
            bybit_data = None
        if isinstance(cmc_data, Exception):
            print(f"CMC error for {symbol}: {cmc_data}")
            cmc_data = None
        if isinstance(coinglass_data, Exception):
            print(f"Coinglass error for {symbol}: {coinglass_data}")
            coinglass_data = None
        
        # Агрегируем данные
        aggregated = self._aggregate_data(
            symbol, binance_data, bybit_data, cmc_data, coinglass_data
        )
        
        # Сохраняем в кэш
        self._cache[cache_key] = (datetime.utcnow(), aggregated)
        
        # Рассчитываем время обработки
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        aggregated.data_age_seconds = elapsed
        
        return aggregated
    
    async def _get_binance_data(self, symbol: str) -> Optional[BinanceData]:
        """Получить данные с Binance"""
        try:
            return await self.binance.get_complete_market_data(symbol)
        except Exception as e:
            print(f"Binance data fetch error: {e}")
            return None
    
    async def _get_bybit_data(self, symbol: str) -> Optional[BybitMarketData]:
        """Получить данные с Bybit"""
        try:
            return await self.bybit.get_complete_market_data(symbol)
        except Exception as e:
            print(f"Bybit data fetch error: {e}")
            return None
    
    async def _get_cmc_data(self, symbol: str) -> Optional[CMCMarketData]:
        """Получить данные с CoinMarketCap"""
        try:
            quotes = await self.cmc.get_quotes_latest([symbol])
            if quotes and symbol in quotes:
                return quotes[symbol]
            return None
        except Exception as e:
            print(f"CMC data fetch error: {e}")
            return None
    
    async def _get_coinglass_data(self, symbol: str) -> Optional[Dict]:
        """Получить данные с Coinglass"""
        try:
            data = {}
            
            # Ликвидации
            liq = await self.coinglass.get_liquidation_data(symbol, time_type="1h", limit=4)
            if liq:
                data['liquidations'] = liq
            
            # Фандинг
            funding = await self.coinglass.get_funding_rate(symbol)
            if funding:
                data['funding'] = funding
            
            # Сигнал на основе ликвидаций
            signal = await self.coinglass.get_liquidation_signal(symbol)
            if signal:
                data['liquidation_signal'] = signal
            
            return data if data else None
        except Exception as e:
            print(f"Coinglass data fetch error: {e}")
            return None
    
    def _aggregate_data(self,
                       symbol: str,
                       binance_data: Optional[BinanceData],
                       bybit_data: Optional[BybitMarketData],
                       cmc_data: Optional[CMCMarketData],
                       coinglass_data: Optional[Dict]) -> AggregatedMarketData:
        """
        Агрегирует данные из всех источников
        """
        # Собираем цены из доступных источников
        prices = []
        if binance_data:
            prices.append(("binance", binance_data.price))
        if bybit_data:
            prices.append(("bybit", bybit_data.price))
        if cmc_data:
            prices.append(("cmc", cmc_data.price))
        
        # Рассчитываем среднюю цену и спред
        if prices:
            avg_price = sum(p[1] for p in prices) / len(prices)
            min_price = min(p[1] for p in prices)
            max_price = max(p[1] for p in prices)
            price_spread = ((max_price - min_price) / avg_price * 100) if avg_price > 0 else 0
        else:
            avg_price = 0.0
            price_spread = 0.0
        
        # Фандинг - берём усреднённое или максимальное
        funding_rates = []
        if binance_data:
            funding_rates.append(("binance", binance_data.funding_rate))
        if bybit_data:
            funding_rates.append(("bybit", bybit_data.funding_rate))
        
        if funding_rates:
            # Если источники сильно расходятся (>20%), берём консервативную оценку
            values = [fr[1] for fr in funding_rates]
            if max(values) - min(values) > 0.02:  # >2% разница
                funding_rate = sum(values) / len(values)  # Среднее
            else:
                funding_rate = max(values)  # Максимум для консервативности
        else:
            funding_rate = 0.0
        
        # L/S Ratio
        ls_ratios = []
        if binance_data:
            ls_ratios.append(("binance", binance_data.long_short_ratio))
        if bybit_data:
            ls_ratios.append(("bybit", bybit_data.long_short_ratio))
        
        if ls_ratios:
            avg_ls = sum(ls[1] for ls in ls_ratios) / len(ls_ratios)
        else:
            avg_ls = 50.0
        
        # OI
        oi_values = []
        if binance_data:
            oi_values.append(binance_data.oi_change_4d)
        if bybit_data:
            oi_values.append(bybit_data.oi_change_24h)
        
        if oi_values:
            avg_oi_change = sum(oi_values) / len(oi_values)
            # Оценка уверенности на основе согласия источников
            if max(oi_values) - min(oi_values) < 5:  # Меньше 5% разница
                oi_confidence = "high"
            elif max(oi_values) - min(oi_values) < 15:
                oi_confidence = "medium"
            else:
                oi_confidence = "low"
        else:
            avg_oi_change = 0.0
            oi_confidence = "low"
        
        # Объёмы (берём максимум)
        volumes = []
        if binance_data:
            volumes.append(binance_data.volume_24h)
        if bybit_data:
            volumes.append(bybit_data.volume_24h)
        
        max_volume = max(volumes) if volumes else 0.0
        
        # Изменение цены (среднее)
        price_changes = []
        if binance_data:
            price_changes.append(binance_data.price_change_24h)
        if bybit_data:
            price_changes.append(bybit_data.price_change_24h)
        if cmc_data:
            price_changes.append(cmc_data.price_change_24h)
        
        avg_price_change = sum(price_changes) / len(price_changes) if price_changes else 0.0
        
        # Coinglass данные
        liq_long = 0.0
        liq_short = 0.0
        liq_density = "low"
        liq_signal = "neutral"
        funding_extreme = False
        funding_dir = "neutral"
        
        if coinglass_data:
            # Ликвидации
            if 'liquidations' in coinglass_data:
                liq = coinglass_data['liquidations']
                liq_long = liq.long_liquidation_amount
                liq_short = liq.short_liquidation_amount
                liq_density = liq.liquidation_density
            
            # Сигнал на основе ликвидаций
            if 'liquidation_signal' in coinglass_data:
                liq_signal = coinglass_data['liquidation_signal']['signal']
            
            # Фандинг
            if 'funding' in coinglass_data:
                f = coinglass_data['funding']
                funding_extreme = f.extreme_positive or f.extreme_negative
                funding_dir = "shorts_pay" if f.current_funding > 0 else "longs_pay" if f.current_funding < 0 else "neutral"
        
        # Оценка качества данных
        sources_used = sum([1 for d in [binance_data, bybit_data, cmc_data, coinglass_data] if d])
        data_quality = int((sources_used / 4) * 100)
        
        # Уверенность на основе качества и согласия
        if sources_used >= 3 and price_spread < 0.5:
            confidence = "high"
        elif sources_used >= 2 and price_spread < 1.0:
            confidence = "medium"
        else:
            confidence = "low"
        
        return AggregatedMarketData(
            symbol=symbol,
            timestamp=datetime.utcnow(),
            price=avg_price,
            price_binance=binance_data.price if binance_data else 0.0,
            price_bybit=bybit_data.price if bybit_data else 0.0,
            price_spread=price_spread,
            funding_rate=funding_rate,
            funding_sources=funding_rates,
            open_interest=binance_data.open_interest if binance_data else (bybit_data.open_interest if bybit_data else 0.0),
            oi_change_24h=avg_oi_change,
            oi_confidence=oi_confidence,
            long_short_ratio=avg_ls,
            ls_ratio_sources=ls_ratios,
            volume_24h=max_volume,
            volume_change_24h=binance_data.volume_change_24h if binance_data else 0.0,
            price_change_24h=avg_price_change,
            price_change_1h=cmc_data.price_change_1h if cmc_data else 0.0,
            market_cap=cmc_data.market_cap if cmc_data else None,
            cmc_rank=cmc_data.cmc_rank if cmc_data else None,
            price_change_7d=cmc_data.price_change_7d if cmc_data else None,
            price_change_30d=cmc_data.price_change_30d if cmc_data else None,
            liquidations_long=liq_long,
            liquidations_short=liq_short,
            liquidation_density=liq_density,
            liquidation_signal=liq_signal,
            funding_extreme=funding_extreme,
            funding_direction=funding_dir,
            data_quality_score=data_quality,
            sources_used=sources_used,
            sources_total=4,
            confidence=confidence,
            data_age_seconds=0.0
        )
    
    async def scan_multiple_symbols(self, symbols: List[str]) -> List[AggregatedMarketData]:
        """
        Сканировать несколько символов параллельно
        
        Args:
            symbols: Список символов
        
        Returns:
            Список агрегированных данных
        """
        tasks = [self.get_aggregated_data(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем ошибки и None
        valid_results = []
        for result in results:
            if isinstance(result, AggregatedMarketData):
                valid_results.append(result)
            elif isinstance(result, Exception):
                print(f"Scan error: {result}")
        
        return valid_results
    
    async def close(self):
        """Закрыть все клиенты"""
        await self.binance.close()
        await self.bybit.close()
        await self.cmc.close()
        await self.coinglass.close()


# ============================================================================
# SINGLETON
# ============================================================================

_data_aggregator = None

def get_data_aggregator() -> DataAggregator:
    """Получить singleton агрегатор"""
    global _data_aggregator
    if _data_aggregator is None:
        _data_aggregator = DataAggregator()
    return _data_aggregator


# ============================================================================
# EXAMPLE
# ============================================================================

async def test_aggregator():
    """Тест агрегатора данных"""
    print("\n🔄 Testing Data Aggregator...")
    
    aggregator = DataAggregator()
    
    # Тест одного символа
    print("\n📊 Single symbol (BTCUSDT):")
    data = await aggregator.get_aggregated_data("BTCUSDT")
    
    if data:
        print(f"  Symbol: {data.symbol}")
        print(f"  Price: ${data.price:,.2f}")
        print(f"  Spread: {data.price_spread:.3f}%")
        print(f"  Sources: {data.sources_used}/4")
        print(f"  Quality: {data.data_quality_score}%")
        print(f"  Confidence: {data.confidence}")
        print(f"  Funding: {data.funding_rate:.4f}%")
        print(f"  L/S Ratio: {data.long_short_ratio:.1f}%")
        print(f"  OI Change: {data.oi_change_24h:+.2f}%")
        print(f"  Liquidations: L=${data.liquidations_long:,.0f} S=${data.liquidations_short:,.0f}")
        print(f"  Liquidation Signal: {data.liquidation_signal}")
        print(f"  Data age: {data.data_age_seconds:.2f}s")
    else:
        print("  ❌ Failed to get data")
    
    # Тест нескольких символов
    print("\n📊 Multiple symbols (BTC, ETH, SOL):")
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    results = await aggregator.scan_multiple_symbols(symbols)
    
    for result in results:
        print(f"  {result.symbol}: ${result.price:,.2f} ({result.confidence})")
    
    # Закрываем
    await aggregator.close()
    print("\n✅ Test complete!")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_aggregator())
