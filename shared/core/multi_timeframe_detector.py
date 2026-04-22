"""
🆕 Multi-Timeframe Detector — shared/core/multi_timeframe_detector.py

Детектор сигналов на нескольких таймфреймах:
- 15m: Скальпинг, быстрые входы
- 45m: Свинг, среднесрочные
- 1h: Трендовые сигналы
- 2h: Сильные движения
- 4h: Крупные тренды

Интеграция в liquidity-bots-main
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from datetime import datetime
from enum import Enum


class Timeframe(Enum):
    M15 = "15m"
    M45 = "45m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"


@dataclass
class TimeframeConfig:
    """Конфигурация для каждого таймфрейма"""
    name: str
    interval: str  # Для API (например, "15" для 15m)
    oi_threshold: float
    price_threshold: float
    volume_threshold: float
    min_score: int
    lookback_periods: int
    weight: float  # Вес при агрегации (0.0-1.0)
    
    # Торговые параметры
    leverage_range: str
    tp_multiplier: float  # Множитель для TP расчёта
    sl_multiplier: float  # Множитель для SL расчёта


# 🎯 Конфигурации для каждого ТФ
TIMEFRAME_CONFIGS = {
    Timeframe.M15: TimeframeConfig(
        name="15m",
        interval="15",
        oi_threshold=3.0,      # 3% OI change
        price_threshold=0.5,   # 0.5% price change
        volume_threshold=30.0, # 30x volume spike
        min_score=50,
        lookback_periods=20,   # 5 часов истории
        weight=0.15,           # Меньший вес (шумный)
        leverage_range="10-20",
        tp_multiplier=1.5,     # Быстрые TP
        sl_multiplier=0.8,
    ),
    Timeframe.M45: TimeframeConfig(
        name="45m",
        interval="45",
        oi_threshold=4.0,
        price_threshold=1.0,
        volume_threshold=40.0,
        min_score=55,
        lookback_periods=16,   # 12 часов
        weight=0.20,
        leverage_range="8-15",
        tp_multiplier=2.0,
        sl_multiplier=1.0,
    ),
    Timeframe.H1: TimeframeConfig(
        name="1h",
        interval="60",
        oi_threshold=5.0,      # Как сейчас
        price_threshold=1.5,
        volume_threshold=50.0,
        min_score=60,
        lookback_periods=24,   # Сутки
        weight=0.25,
        leverage_range="5-10",
        tp_multiplier=2.5,
        sl_multiplier=1.2,
    ),
    Timeframe.H2: TimeframeConfig(
        name="2h",
        interval="120",
        oi_threshold=7.0,
        price_threshold=2.5,
        volume_threshold=70.0,
        min_score=65,
        lookback_periods=12,   # Сутки
        weight=0.20,
        leverage_range="3-8",
        tp_multiplier=3.0,
        sl_multiplier=1.5,
    ),
    Timeframe.H4: TimeframeConfig(
        name="4h",
        interval="240",
        oi_threshold=10.0,     # Сильные движения
        price_threshold=4.0,
        volume_threshold=100.0,
        min_score=70,
        lookback_periods=6,    # Сутки
        weight=0.20,
        leverage_range="3-5",
        tp_multiplier=4.0,     # Большие TP
        sl_multiplier=2.0,
    ),
}


@dataclass
class TimeframeSignal:
    """Сигнал с конкретного таймфрейма"""
    timeframe: Timeframe
    symbol: str
    direction: str  # "long" | "short"
    score: int
    confidence: float
    oi_change: float
    price_change: float
    volume_spike: float
    entry_price: float
    suggested_sl: float
    suggested_tp: float
    reasons: List[str]
    timestamp: datetime
    config: TimeframeConfig


@dataclass
class AggregatedSignal:
    """Агрегированный сигнал со всех ТФ"""
    symbol: str
    direction: str
    total_score: int
    weighted_score: float
    timeframes: List[TimeframeSignal]
    consensus: float  # Сколько ТФ согласны (0.0-1.0)
    strongest_tf: Timeframe
    entry_price: float
    recommended_leverage: int
    recommended_sl: float
    recommended_tp: float
    all_reasons: List[str]
    timestamp: datetime
    
    @property
    def is_multi_tf_confirmed(self) -> bool:
        """Сигнал подтверждён на 2+ таймфреймах"""
        return len(self.timeframes) >= 2
    
    @property
    def primary_timeframe(self) -> str:
        """Основной ТФ для входа"""
        if not self.timeframes:
            return "unknown"
        # Выбираем ТФ с лучшим соотношением score/риск
        best = max(self.timeframes, 
                   key=lambda x: x.score * x.config.weight)
        return best.timeframe.value


class MultiTimeframeDetector:
    """
    🆕 Multi-Timeframe Signal Detector
    
    Сканирует 5 таймфреймов (15m-4h) и агрегирует сигналы.
    """
    
    def __init__(self):
        self.configs = TIMEFRAME_CONFIGS
        self.active_timeframes = [
            Timeframe.M15,
            Timeframe.M45,
            Timeframe.H1,
            Timeframe.H2,
            Timeframe.H4,
        ]
    
    async def analyze_all_timeframes(
        self,
        symbol: str,
        market_data: Dict,
        ohlcv_fetcher,  # Функция для получения свечей
    ) -> AggregatedSignal:
        """
        Анализирует все ТФ и возвращает агрегированный сигнал
        """
        timeframe_signals = []
        
        for tf in self.active_timeframes:
            config = self.configs[tf]
            
            # Получаем данные для этого ТФ
            candles = await ohlcv_fetcher(symbol, config.interval, config.lookback_periods)
            if not candles or len(candles) < 10:
                continue
            
            # Анализируем
            signal = self._analyze_timeframe(
                tf, config, candles, market_data
            )
            
            if signal and signal.score >= config.min_score:
                timeframe_signals.append(signal)
        
        # Агрегируем результаты
        if not timeframe_signals:
            return None
        
        return self._aggregate_signals(symbol, timeframe_signals)
    
    def _analyze_timeframe(
        self,
        timeframe: Timeframe,
        config: TimeframeConfig,
        candles: List,
        market_data: Dict,
    ) -> Optional[TimeframeSignal]:
        """Анализирует один таймфрейм"""
        
        if len(candles) < 5:
            return None
        
        # Берём последние свечи для расчёта
        recent = candles[-5:]
        
        # Расчёт изменений
        oi_change = market_data.get("oi_change_1h", 0)  # Базовый
        if timeframe == Timeframe.M15:
            oi_change = market_data.get("oi_change_15m", oi_change * 0.25)
        elif timeframe == Timeframe.M45:
            oi_change = market_data.get("oi_change_45m", oi_change * 0.75)
        
        # Price change за период
        first_price = candles[0].open
        last_price = candles[-1].close
        price_change = abs((last_price - first_price) / first_price * 100)
        
        # Volume spike
        avg_vol = sum(c.volume for c in candles[:-1]) / max(1, len(candles) - 1)
        last_vol = candles[-1].volume
        volume_spike = (last_vol / avg_vol) if avg_vol > 0 else 1.0
        
        # Проверяем thresholds
        if oi_change < config.oi_threshold:
            return None
        if price_change < config.price_threshold:
            return None
        if volume_spike < config.volume_threshold / 10:  # Уменьшаем для сравнения
            return None
        
        # Определяем направление
        is_rising = last_price > first_price
        funding = market_data.get("funding_rate", 0)
        
        if funding > 0.05:  # Перегрет long
            direction = "short"
        elif funding < -0.05:  # Перегрет short
            direction = "long"
        else:
            direction = "short" if is_rising else "long"
        
        # Скоринг
        score = 50
        score += min(20, int(oi_change * 2))
        score += min(15, int(price_change * 5))
        score += min(15, int(volume_spike * 2))
        
        # Факторы
        reasons = [
            f"📊 {timeframe.value}: OI +{oi_change:.1f}%",
            f"💰 Price {price_change:+.1f}%",
            f"📈 Volume {volume_spike:.1f}x",
        ]
        
        # Расчёт SL/TP
        atr = self._calculate_atr(candles)
        entry = last_price
        
        if direction == "long":
            sl = entry - (atr * config.sl_multiplier)
            tp = entry + (atr * config.tp_multiplier)
        else:
            sl = entry + (atr * config.sl_multiplier)
            tp = entry - (atr * config.tp_multiplier)
        
        return TimeframeSignal(
            timeframe=timeframe,
            symbol=symbol,
            direction=direction,
            score=min(100, score),
            confidence=min(0.9, 0.5 + score/200),
            oi_change=oi_change,
            price_change=price_change,
            volume_spike=volume_spike,
            entry_price=entry,
            suggested_sl=sl,
            suggested_tp=tp,
            reasons=reasons,
            timestamp=datetime.utcnow(),
            config=config,
        )
    
    def _aggregate_signals(
        self,
        symbol: str,
        signals: List[TimeframeSignal],
    ) -> AggregatedSignal:
        """Агрегирует сигналы со всех ТФ"""
        
        # Проверяем консенсус направления
        long_count = sum(1 for s in signals if s.direction == "long")
        short_count = len(signals) - long_count
        
        if long_count > short_count:
            direction = "long"
            consensus = long_count / len(signals)
        else:
            direction = "short"
            consensus = short_count / len(signals)
        
        # Фильтруем сигналы по направлению большинства
        aligned_signals = [s for s in signals if s.direction == direction]
        
        # Взвешенный скор
        total_weight = sum(s.config.weight for s in aligned_signals)
        weighted_score = sum(
            s.score * s.config.weight for s in aligned_signals
        ) / total_weight if total_weight > 0 else 0
        
        # Самый сильный ТФ
        strongest = max(aligned_signals, key=lambda x: x.score)
        
        # Объединяем причины
        all_reasons = []
        for s in aligned_signals:
            all_reasons.extend(s.reasons)
        
        # Рекомендуемые параметры (от самого сильного ТФ)
        leverage = self._parse_leverage(strongest.config.leverage_range)
        
        return AggregatedSignal(
            symbol=symbol,
            direction=direction,
            total_score=int(weighted_score),
            weighted_score=weighted_score,
            timeframes=aligned_signals,
            consensus=consensus,
            strongest_tf=strongest.timeframe,
            entry_price=strongest.entry_price,
            recommended_leverage=leverage,
            recommended_sl=strongest.suggested_sl,
            recommended_tp=strongest.suggested_tp,
            all_reasons=all_reasons,
            timestamp=datetime.utcnow(),
        )
    
    def _calculate_atr(self, candles: List, period: int = 14) -> float:
        """Расчёт ATR"""
        if len(candles) < period:
            period = len(candles)
        
        tr_values = []
        for i in range(1, period):
            high = candles[i].high
            low = candles[i].low
            prev_close = candles[i-1].close
            
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            tr_values.append(max(tr1, tr2, tr3))
        
        return sum(tr_values) / len(tr_values) if tr_values else 0.0
    
    def _parse_leverage(self, range_str: str) -> int:
        """Парсит '5-10' в среднее значение"""
        try:
            parts = range_str.split("-")
            if len(parts) == 2:
                low, high = int(parts[0]), int(parts[1])
                return (low + high) // 2
            return int(range_str)
        except:
            return 5


# 🔧 Утилиты для интеграции

def get_timeframe_recommendation(signal: AggregatedSignal) -> str:
    """Генерирует рекомендацию по входу"""
    if not signal:
        return "Нет сигнала"
    
    tf_names = {
        Timeframe.M15: "15м (скальп)",
        Timeframe.M45: "45м (свинг)",
        Timeframe.H1: "1ч (тренд)",
        Timeframe.H2: "2ч (сильный тренд)",
        Timeframe.H4: "4ч (крупный тренд)",
    }
    
    primary = tf_names.get(signal.strongest_tf, signal.strongest_tf.value)
    
    if signal.is_multi_tf_confirmed:
        return f"✅ Мульти-ТФ подтверждение ({len(signal.timeframes)} ТФ). Основной: {primary}"
    else:
        return f"⚠️ Только {primary} (один ТФ)"


def format_multi_tf_message(signal: AggregatedSignal) -> str:
    """Форматирует сообщение для Telegram"""
    if not signal:
        return "Нет сигнала"
    
    emoji = "🟢" if signal.direction == "long" else "🔴"
    consensus_emoji = "✅" if signal.consensus >= 0.7 else "⚠️"
    
    msg = f"{emoji} <b>{signal.symbol} | {signal.direction.upper()}</b>\n"
    msg += f"📊 Score: <b>{signal.total_score}/100</b>\n"
    msg += f"{consensus_emoji} Консенсус: {signal.consensus:.0%} ({len(signal.timeframes)} ТФ)\n"
    msg += f"🎯 Вход: ${signal.entry_price:.6f}\n"
    msg += f"🛑 SL: ${signal.recommended_sl:.6f}\n"
    msg += f"💰 TP: ${signal.recommended_tp:.6f}\n"
    msg += f"⚡ Плечо: {signal.recommended_leverage}x\n\n"
    
    msg += "<b>Таймфреймы:</b>\n"
    for tf_signal in signal.timeframes:
        tf_emoji = "✓" if tf_signal.score >= tf_signal.config.min_score else "×"
        msg += f"  {tf_emoji} {tf_signal.timeframe.value}: {tf_signal.score} pts\n"
    
    return msg
