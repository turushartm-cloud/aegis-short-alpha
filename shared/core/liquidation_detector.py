"""
🆕 Liquidation Zone Detector — shared/core/liquidation_detector.py

Детектор магнитов ликвидации (Liquidation Clusters).
Находит зоны массовой ликвидации (магниты) которые притягивают цену.

Интеграция с:
- Coinglass API для данных ликвидаций
- Scorer для bonus к скору
- TradeManager для SL/TP оптимизации
"""

import numpy as np
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio


@dataclass
class LiquidationCluster:
    """Кластер ликвидаций = магнит для цены"""
    price_level: float
    volume: float  # Суммарный объём ликвидаций в USD
    side: str  # "long" | "short" — чьи позиции ликвидировались
    strength: float  # 0.0-1.0 относительная сила
    distance_pct: float  # Расстояние от текущей цены в %
    
    @property
    def is_above(self) -> bool:
        """Магнит выше текущей цены?"""
        return self.distance_pct > 0
    
    @property
    def is_below(self) -> bool:
        """Магнит ниже текущей цены?"""
        return self.distance_pct < 0


@dataclass
class LiquidationAnalysis:
    """Результат анализа ликвидаций для символа"""
    symbol: str
    current_price: float
    clusters: List[LiquidationCluster]
    
    # Ближайшие магниты
    nearest_above: Optional[LiquidationCluster]
    nearest_below: Optional[LiquidationCluster]
    
    # Самые сильные
    strongest_above: Optional[LiquidationCluster]
    strongest_below: Optional[LiquidationCluster]
    
    # Тренд ликвидаций
    long_liq_dominance: float  # 0.0-1.0 сколько ликвидаций было лонгов
    
    @property
    def has_targets(self) -> bool:
        """Есть ли цели для цены?"""
        return len(self.clusters) >= 1
    
    def get_recommended_tp(self, direction: str, default_tp: float) -> float:
        """Рекомендует TP на основе ближайшего магнита"""
        if direction == "long" and self.nearest_above:
            # TP у ближайшего магнита выше
            return self.nearest_above.price_level * 0.998  # Чуть ниже магнита
        elif direction == "short" and self.nearest_below:
            # TP у ближайшего магнита ниже
            return self.nearest_below.price_level * 1.002  # Чуть выше магнита
        return default_tp
    
    def get_recommended_sl(self, direction: str, default_sl: float) -> float:
        """Рекомендует SL защищённым от магнитов"""
        if direction == "long" and self.nearest_below:
            # SL за магнитом (с запасом)
            return self.nearest_below.price_level * 0.995  # Ниже магнита
        elif direction == "short" and self.nearest_above:
            # SL за магнитом
            return self.nearest_above.price_level * 1.005  # Выше магнита
        return default_sl
    
    def get_score_bonus(self, direction: str) -> int:
        """Вычисляет bonus к скору на основе магнитов"""
        bonus = 0
        
        if direction == "long":
            # Лонг хорош когда:
            # 1. Магнит выше (куда цена пойдёт)
            # 2. Нет сильного магнита ниже (не соберут стопы)
            
            if self.nearest_above:
                dist = abs(self.nearest_above.distance_pct)
                if 2 <= dist <= 8:  # Оптимальное расстояние
                    bonus += 15
                elif 8 < dist <= 15:
                    bonus += 10  # Далековато но есть цель
            
            # Штраф если сильный магнит близко снизу
            if self.nearest_below and abs(self.nearest_below.distance_pct) < 1.5:
                bonus -= 10  # Риск собрать стопы
                
        else:  # short
            if self.nearest_below:
                dist = abs(self.nearest_below.distance_pct)
                if 2 <= dist <= 8:
                    bonus += 15
                elif 8 < dist <= 15:
                    bonus += 10
            
            if self.nearest_above and abs(self.nearest_above.distance_pct) < 1.5:
                bonus -= 10
        
        return max(-20, min(20, bonus))  # Clamp -20..+20


class LiquidationZoneDetector:
    """
    🆕 Liquidation Zone Detector
    
    Находит магниты ликвидации используя алгоритм кластеризации.
    Работает с данными Coinglass или симуляцией для тестов.
    """
    
    # Параметры кластеризации
    PRICE_BUCKET_SIZE_PCT = 0.5  # Бакет цены = 0.5%
    MIN_CLUSTER_VOLUME_USD = 500_000  # Минимум $500K для значимого кластера
    
    def __init__(self, coinglass_client=None):
        self.coinglass = coinglass_client
        self._cache = {}  # symbol -> (timestamp, analysis)
        self._cache_ttl = timedelta(minutes=5)
    
    async def analyze_symbol(
        self,
        symbol: str,
        current_price: float,
        liquidations_data: Optional[List[Dict]] = None,
    ) -> LiquidationAnalysis:
        """
        Анализирует ликвидации для символа
        
        Args:
            symbol: Торговая пара (BTCUSDT)
            current_price: Текущая цена
            liquidations_data: Сырые данные ликвидаций (опционально)
        
        Returns:
            LiquidationAnalysis с кластерами
        """
        # Проверяем кеш
        cached = self._cache.get(symbol)
        if cached:
            timestamp, analysis = cached
            if datetime.utcnow() - timestamp < self._cache_ttl:
                return analysis
        
        # Получаем данные ликвидаций
        if liquidations_data is None and self.coinglass:
            liquidations_data = await self._fetch_coinglass_liq(symbol)
        
        if not liquidations_data:
            # Симуляция для тестов (в реальности использовать реальные данные)
            liquidations_data = self._simulate_liquidations(current_price)
        
        # Кластеризуем
        clusters = self._cluster_liquidations(
            liquidations_data, current_price
        )
        
        # Находим ближайшие и сильнейшие
        clusters_above = [c for c in clusters if c.is_above]
        clusters_below = [c for c in clusters if c.is_below]
        
        nearest_above = min(clusters_above, key=lambda x: abs(x.distance_pct)) if clusters_above else None
        nearest_below = min(clusters_below, key=lambda x: abs(x.distance_pct)) if clusters_below else None
        
        strongest_above = max(clusters_above, key=lambda x: x.strength) if clusters_above else None
        strongest_below = max(clusters_below, key=lambda x: x.strength) if clusters_below else None
        
        # Расчёт dominance
        long_liq_volume = sum(c.volume for c in clusters if c.side == "long")
        short_liq_volume = sum(c.volume for c in clusters if c.side == "short")
        total = long_liq_volume + short_liq_volume
        long_dominance = long_liq_volume / total if total > 0 else 0.5
        
        analysis = LiquidationAnalysis(
            symbol=symbol,
            current_price=current_price,
            clusters=clusters,
            nearest_above=nearest_above,
            nearest_below=nearest_below,
            strongest_above=strongest_above,
            strongest_below=strongest_below,
            long_liq_dominance=long_dominance,
        )
        
        # Кешируем
        self._cache[symbol] = (datetime.utcnow(), analysis)
        
        return analysis
    
    def _cluster_liquidations(
        self,
        liquidations: List[Dict],
        current_price: float,
    ) -> List[LiquidationCluster]:
        """Кластеризует ликвидации по ценовым уровням"""
        
        # Группируем по ценовым бакетам
        buckets: Dict[int, Dict] = {}
        
        for liq in liquidations:
            price = liq.get("price", current_price)
            volume = liq.get("volume", 0)
            side = liq.get("side", "long")
            
            # Определяем бакет
            pct_from_current = (price - current_price) / current_price * 100
            bucket_idx = int(pct_from_current / self.PRICE_BUCKET_SIZE_PCT)
            
            if bucket_idx not in buckets:
                buckets[bucket_idx] = {
                    "prices": [],
                    "volumes": [],
                    "sides": [],
                }
            
            buckets[bucket_idx]["prices"].append(price)
            buckets[bucket_idx]["volumes"].append(volume)
            buckets[bucket_idx]["sides"].append(side)
        
        # Создаём кластеры
        clusters = []
        for bucket_idx, data in buckets.items():
            total_volume = sum(data["volumes"])
            
            if total_volume < self.MIN_CLUSTER_VOLUME_USD:
                continue  # Слишком маленький кластер
            
            avg_price = np.mean(data["prices"])
            dominant_side = max(set(data["sides"]), key=data["sides"].count)
            distance_pct = (avg_price - current_price) / current_price * 100
            
            # Сила относительно максимума
            max_volume = max(sum(b["volumes"]) for b in buckets.values())
            strength = total_volume / max_volume if max_volume > 0 else 0
            
            clusters.append(LiquidationCluster(
                price_level=avg_price,
                volume=total_volume,
                side=dominant_side,
                strength=strength,
                distance_pct=distance_pct,
            ))
        
        # Сортируем по силе
        clusters.sort(key=lambda x: x.strength, reverse=True)
        
        return clusters
    
    async def _fetch_coinglass_liq(self, symbol: str) -> List[Dict]:
        """Получает данные ликвидаций с Coinglass"""
        if not self.coinglass:
            return []
        
        try:
            # Здесь должен быть вызов Coinglass API
            # Например: self.coinglass.get_liquidations(symbol)
            data = await self.coinglass.get_liquidation_data(symbol)
            return data if data else []
        except Exception as e:
            print(f"⚠️ Coinglass liq fetch failed for {symbol}: {e}")
            return []
    
    def _simulate_liquidations(self, current_price: float) -> List[Dict]:
        """Симуляция данных ликвидаций для тестирования"""
        # В реальности удалить эту функцию и использовать только реальные данные
        liquidations = []
        
        # Создаём фейковые ликвидации выше и ниже цены
        for i in range(10):
            # Ликвидации лонгов выше цены (для шортов)
            price_up = current_price * (1 + 0.02 + i * 0.01)
            liquidations.append({
                "price": price_up,
                "volume": 500_000 + i * 100_000,
                "side": "long",
            })
            
            # Ликвидации шортов ниже цены (для лонгов)
            price_down = current_price * (1 - 0.02 - i * 0.01)
            liquidations.append({
                "price": price_down,
                "volume": 600_000 + i * 80_000,
                "side": "short",
            })
        
        return liquidations
    
    def clear_cache(self):
        """Очищает кеш"""
        self._cache.clear()


# 🔧 Утилиты для интеграции

def format_liquidation_zones(analysis: LiquidationAnalysis) -> str:
    """Форматирует зоны для Telegram"""
    if not analysis.has_targets:
        return "🎯 Нет данных о ликвидациях"
    
    msg = "🧲 <b>Магниты ликвидации:</b>\n\n"
    
    if analysis.nearest_above:
        na = analysis.nearest_above
        msg += f"⬆️ Выше: ${na.price_level:,.2f} (+{na.distance_pct:.1f}%)\n"
        msg += f"   Объём: ${na.volume:,.0f} | Сила: {na.strength:.0%}\n\n"
    
    if analysis.nearest_below:
        nb = analysis.nearest_below
        msg += f"⬇️ Ниже: ${nb.price_level:,.2f} ({nb.distance_pct:.1f}%)\n"
        msg += f"   Объём: ${nb.volume:,.0f} | Сила: {nb.strength:.0%}\n\n"
    
    # Доминирование
    if analysis.long_liq_dominance > 0.6:
        msg += f"📊 Доминируют ликвидации <b>ЛОНГОВ</b> ({analysis.long_liq_dominance:.0%})\n"
        msg += "   💡 Цена тянется вверх за стопами\n"
    elif analysis.long_liq_dominance < 0.4:
        msg += f"📊 Доминируют ликвидации <b>ШОРТОВ</b> ({1-analysis.long_liq_dominance:.0%})\n"
        msg += "   💡 Цена тянется вниз за стопами\n"
    
    return msg


def get_liquidation_insight(analysis: LiquidationAnalysis, direction: str) -> str:
    """Генерирует инсайт для сигнала"""
    if direction == "long":
        if analysis.nearest_above and analysis.nearest_above.strength > 0.7:
            dist = abs(analysis.nearest_above.distance_pct)
            return f"🧲 Сильный магнит +{dist:.1f}% — цель для TP"
        elif analysis.nearest_below and abs(analysis.nearest_below.distance_pct) < 1.5:
            dist = abs(analysis.nearest_below.distance_pct)
            return f"⚠️ Магнит -{dist:.1f}% близко — риск стопа"
    else:  # short
        if analysis.nearest_below and analysis.nearest_below.strength > 0.7:
            dist = abs(analysis.nearest_below.distance_pct)
            return f"🧲 Сильный магнит -{dist:.1f}% — цель для TP"
        elif analysis.nearest_above and abs(analysis.nearest_above.distance_pct) < 1.5:
            dist = abs(analysis.nearest_above.distance_pct)
            return f"⚠️ Магнит +{dist:.1f}% близко — риск стопа"
    
    return "🎯 Магниты в нейтральной зоне"
