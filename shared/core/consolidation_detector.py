"""
Consolidation Detector v1.0
Находит зоны консолидации (range-bound markets) и блокирует входы в середине диапазона.

Принцип:
  1. Находим зоны накопления/распределения (flat price action)
  2. Блокируем сигналы если цена в середине диапазона (70% центра)
  3. Разрешаем входы только:
     - На пробое зоны (breakout)
     - На Spring (ложный пробой вниз для LONG)
     - На Upthrust (ложный пробой вверх для SHORT)

Использование:
    cd = ConsolidationDetector(lookback=20, max_range_pct=5.0)
    result = cd.detect(ohlcv, current_price)
    
    if result.is_consolidating and result.is_mid_range(current_price):
        # БЛОКИРУЕМ сигнал — цена в середине диапазона
        return None
    
    if result.has_spring and direction == "long":
        # Усиливаем сигнал — ложный пробой вниз
        score += 15
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple
import statistics


@dataclass
class ConsolidationResult:
    """Результат анализа консолидации."""
    is_consolidating: bool      # True = цена в боковике
    range_high: float          # Верхняя граница зоны
    range_low: float           # Нижняя граница зоны
    range_pct: float           # Ширина зоны в процентах
    position_in_range: float   # 0.0 = low, 0.5 = middle, 1.0 = high
    
    # Сигналы для входа
    has_spring: bool            # Ложный пробой вниз + возврат (для LONG)
    has_upthrust: bool         # Ложный пробой вверх + возврат (для SHORT)
    has_breakout_up: bool      # Пробой вверх
    has_breakout_down: bool    # Пробой вниз
    
    # Метаданные
    lookback_candles: int
    volatility_compression: bool  # ATR сжался = готовность к импульсу
    
    def is_mid_range(self, price: float, buffer: float = 0.15) -> bool:
        """
        Проверяет, находится ли цена в середине диапазона (40-60% = mid).
        buffer=0.15 значит блокируем 30% центра (35-65%)
        """
        if not self.is_consolidating:
            return False
        mid_low = 0.5 - buffer   # 0.35
        mid_high = 0.5 + buffer  # 0.65
        pos = self._price_position(price)
        return mid_low <= pos <= mid_high
    
    def is_near_support(self, price: float, threshold: float = 0.10) -> bool:
        """Цена близка к поддержке (нижняя граница)."""
        pos = self._price_position(price)
        return pos <= threshold
    
    def is_near_resistance(self, price: float, threshold: float = 0.10) -> bool:
        """Цена близка к сопротивлению (верхняя граница)."""
        pos = self._price_position(price)
        return pos >= (1.0 - threshold)
    
    def _price_position(self, price: float) -> float:
        """Позиция цены в диапазоне 0.0-1.0."""
        if self.range_high == self.range_low:
            return 0.5
        return (price - self.range_low) / (self.range_high - self.range_low)
    
    def entry_zone_status(self, price: float) -> str:
        """Возвращает статус для входа."""
        if not self.is_consolidating:
            return "trending"
        
        pos = self._price_position(price)
        
        if pos <= 0.15:
            return "near_support"  # Хорошо для LONG
        elif pos >= 0.85:
            return "near_resistance"  # Хорошо для SHORT
        elif 0.35 <= pos <= 0.65:
            return "mid_range"  # БЛОКИРОВАТЬ
        elif pos < 0.35:
            return "lower_half"  # Осторожно LONG
        else:
            return "upper_half"  # Осторожно SHORT


class ConsolidationDetector:
    """
    Детектор зон консолидации.
    Блокирует входы в середине диапазона — основная причина убыточных сделок.
    """
    
    def __init__(
        self,
        lookback: int = 20,           # Свечей для анализа
        max_range_pct: float = 6.0,   # Макс ширина зоны для консолидации
        min_candles: int = 10,        # Мин свечей для подтверждения
        atr_compression_factor: float = 0.6,  # ATR сжался на 40%
    ):
        self.lookback = lookback
        self.max_range_pct = max_range_pct
        self.min_candles = min_candles
        self.atr_compression_factor = atr_compression_factor
    
    def detect(self, candles: List, current_price: float) -> ConsolidationResult:
        """
        Анализирует свечи на предмет консолидации.
        
        Returns:
            ConsolidationResult с полной информацией о зоне
        """
        if len(candles) < self.min_candles:
            return self._empty_result(current_price)
        
        # Берём последние N свечей
        lookback = min(self.lookback, len(candles))
        recent = candles[-lookback:]
        
        # Находим границы зоны
        highs = [c.high for c in recent]
        lows = [c.low for c in recent]
        
        range_high = max(highs)
        range_low = min(lows)
        
        # Рассчитываем ширину зоны
        mid_price = (range_high + range_low) / 2
        range_pct = (range_high - range_low) / mid_price * 100 if mid_price > 0 else 100
        
        # Проверяем консолидацию
        is_consolidating = range_pct <= self.max_range_pct
        
        # Позиция текущей цены
        position_in_range = 0.5
        if range_high != range_low:
            position_in_range = (current_price - range_low) / (range_high - range_low)
        
        # Проверяем сжатие волатильности (ATR)
        volatility_compression = self._check_atr_compression(candles)
        
        # Ищем Spring/Upthrust
        has_spring = self._detect_spring(recent, range_low)
        has_upthrust = self._detect_upthrust(recent, range_high)
        
        # Проверяем пробои
        has_breakout_up = current_price > range_high * 1.005  # 0.5% пробой
        has_breakout_down = current_price < range_low * 0.995
        
        return ConsolidationResult(
            is_consolidating=is_consolidating,
            range_high=range_high,
            range_low=range_low,
            range_pct=range_pct,
            position_in_range=position_in_range,
            has_spring=has_spring,
            has_upthrust=has_upthrust,
            has_breakout_up=has_breakout_up,
            has_breakout_down=has_breakout_down,
            lookback_candles=lookback,
            volatility_compression=volatility_compression,
        )
    
    def _check_atr_compression(self, candles: List) -> bool:
        """Проверяет сжатие ATR (подготовка к импульсу)."""
        if len(candles) < 30:
            return False
        
        # ATR за первую половину vs вторую
        half = len(candles) // 2
        atr_first = self._calc_atr(candles[:half])
        atr_second = self._calc_atr(candles[half:])
        
        if atr_first <= 0:
            return False
        
        return atr_second / atr_first < self.atr_compression_factor
    
    def _calc_atr(self, candles: List, period: int = 14) -> float:
        """Расчёт ATR."""
        if len(candles) < 2:
            return 0.0
        
        trs = []
        for i in range(1, min(period + 1, len(candles))):
            c = candles[i]
            pc = candles[i-1].close
            tr = max(c.high - c.low, abs(c.high - pc), abs(c.low - pc))
            trs.append(tr)
        
        return sum(trs) / len(trs) if trs else 0.0
    
    def _detect_spring(self, candles: List, range_low: float) -> bool:
        """
        Spring: ложный пробой вниз с возвратом.
        Свеча уходит ниже range_low, но закрывается выше.
        """
        if len(candles) < 3:
            return False
        
        # Последние 3 свечи
        for c in candles[-3:]:
            # Пробой вниз
            if c.low < range_low * 0.997:
                # Но закрытие выше минимума зоны
                if c.close > range_low * 1.002:
                    return True
        return False
    
    def _detect_upthrust(self, candles: List, range_high: float) -> bool:
        """
        Upthrust: ложный пробой вверх с возвратом.
        Свеча уходит выше range_high, но закрывается ниже.
        """
        if len(candles) < 3:
            return False
        
        for c in candles[-3:]:
            # Пробой вверх
            if c.high > range_high * 1.003:
                # Но закрытие ниже максимума зоны
                if c.close < range_high * 0.998:
                    return True
        return False
    
    def _empty_result(self, current_price: float) -> ConsolidationResult:
        """Пустой результат при недостатке данных."""
        return ConsolidationResult(
            is_consolidating=False,
            range_high=current_price,
            range_low=current_price,
            range_pct=0.0,
            position_in_range=0.5,
            has_spring=False,
            has_upthrust=False,
            has_breakout_up=False,
            has_breakout_down=False,
            lookback_candles=0,
            volatility_compression=False,
        )


# ============================================================================
# FILTERS для использования в ботах
# ============================================================================

def filter_mid_range(
    consolidation: ConsolidationResult,
    price: float,
    direction: str,
    verbose: bool = False,
) -> Tuple[bool, str]:
    """
    Фильтр: блокировать входы в середине диапазона.
    
    Returns:
        (allow_signal: bool, reason: str)
    """
    if not consolidation.is_consolidating:
        return True, "trending market"
    
    status = consolidation.entry_zone_status(price)
    
    # В середине диапазона — БЛОКИРУЕМ
    if status == "mid_range":
        return False, f"MID_RANGE: цена в {consolidation.position_in_range:.0%} диапазона {consolidation.range_pct:.1f}%"
    
    # Для LONG — только нижняя половина или Spring
    if direction == "long":
        if consolidation.has_spring:
            return True, "SPRING detected — ложный пробой вниз"
        if consolidation.has_breakout_up:
            return True, "BREAKOUT UP — пробой консолидации"
        if status == "near_support":
            return True, "near support"
        if status in ("upper_half", "near_resistance"):
            return False, f"LONG в {status} — плохая зона для входа"
        return True, "lower half"
    
    # Для SHORT — только верхняя половина или Upthrust
    if direction == "short":
        if consolidation.has_upthrust:
            return True, "UPTHRUST detected — ложный пробой вверх"
        if consolidation.has_breakout_down:
            return True, "BREAKOUT DOWN — пробой консолидации"
        if status == "near_resistance":
            return True, "near resistance"
        if status in ("lower_half", "near_support"):
            return False, f"SHORT в {status} — плохая зона для входа"
        return True, "upper half"
    
    return True, "ok"
