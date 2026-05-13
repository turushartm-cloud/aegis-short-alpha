"""
Volume Spike Detector
Анализ всплесков объема для раннего обнаружения сильных движений
"""

import numpy as np
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class VolumeSpike:
    """Данные о всплеске объема"""
    symbol: str
    current_volume: float
    avg_volume: float
    spike_ratio: float  # current / avg
    price_change_pct: float
    timestamp: int
    confidence: str  # "weak", "moderate", "strong", "extreme"


class VolumeAnalyzer:
    """
    Анализатор объема для обнаружения pump/dump
    
    Использует:
    - Скользящее среднее объема (20 периодов)
    - Отклонение текущего объема от среднего
    - Корреляция с ценовым движением
    """
    
    # Пороги для всплесков
    SPIKE_THRESHOLDS = {
        "weak": 1.5,      # 1.5x average
        "moderate": 2.5,  # 2.5x average
        "strong": 4.0,    # 4x average
        "extreme": 6.0    # 6x average
    }
    
    def __init__(self, lookback_periods: int = 20):
        self.lookback = lookback_periods
    
    def analyze_spike(self, 
                      symbol: str,
                      volumes: List[float],
                      prices: List[float],
                      timestamp: int) -> Optional[VolumeSpike]:
        """
        Анализировать всплеск объема
        
        Args:
            symbol: Торговая пара
            volumes: Список объемов (последний = текущий)
            prices: Список цен (close)
            timestamp: Текущее время
            
        Returns:
            VolumeSpike если всплеск обнаружен, иначе None
        """
        if len(volumes) < self.lookback + 1:
            return None
        
        current_volume = volumes[-1]
        avg_volume = np.mean(volumes[-self.lookback-1:-1])
        
        if avg_volume == 0:
            return None
        
        spike_ratio = current_volume / avg_volume
        
        # Проверяем порог
        if spike_ratio < self.SPIKE_THRESHOLDS["weak"]:
            return None
        
        # Рассчитываем изменение цены
        price_change_pct = ((prices[-1] - prices[-2]) / prices[-2]) * 100 if len(prices) >= 2 else 0
        
        # Определяем уровень уверенности
        confidence = "weak"
        for level, threshold in self.SPIKE_THRESHOLDS.items():
            if spike_ratio >= threshold:
                confidence = level
        
        return VolumeSpike(
            symbol=symbol,
            current_volume=current_volume,
            avg_volume=avg_volume,
            spike_ratio=spike_ratio,
            price_change_pct=price_change_pct,
            timestamp=timestamp,
            confidence=confidence
        )
    
    def calculate_volume_score(self, spike: VolumeSpike) -> int:
        """
        Рассчитать скор всплеска объема (0-100)
        
        Returns:
            int: Score от 0 до 100
        """
        base_score = {
            "weak": 25,
            "moderate": 50,
            "strong": 75,
            "extreme": 100
        }.get(spike.confidence, 0)
        
        # Бонус за сильное ценовое движение
        price_momentum = abs(spike.price_change_pct)
        if price_momentum > 5:
            base_score += 10
        if price_momentum > 10:
            base_score += 10
        
        return min(base_score, 100)


# Singleton для использования в ботах
_volume_analyzer = None

def get_volume_analyzer() -> VolumeAnalyzer:
    """Получить singleton VolumeAnalyzer"""
    global _volume_analyzer
    if _volume_analyzer is None:
        _volume_analyzer = VolumeAnalyzer(lookback_periods=20)
    return _volume_analyzer
