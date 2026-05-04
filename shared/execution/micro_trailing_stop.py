"""
Micro-Step Trailing Stop v1.0 — Phase 2 Implementation

Плавное движение Stop Loss микро-шагами для минимизации выбивания при ретестах.
Логика: TP1 → +0.3%, TP2 → +0.8%, TP3 → +1.5% (вместо агрессивного трейлинга)
"""

from typing import Dict, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class TrailingState:
    """Состояние микро-степ трейлинга для позиции."""
    symbol: str
    direction: str  # "long" или "short"
    entry_price: float
    initial_sl: float
    current_sl: float
    micro_levels: Dict[int, float] = field(default_factory=dict)  # TP index → SL level
    activated_at: Optional[str] = None
    last_update: Optional[str] = None
    be_triggered: bool = False  # Безубыток активирован
    be2_triggered: bool = False  # BE+0.2% активирован


class MicroTrailingStop:
    """
    Микро-ступенчатый трейлинг-стоп.
    
    Логика:
    - После TP1: SL → entry (0%)
    - После TP2: SL → entry + 0.2%
    - После TP3+: классический трейлинг с шагом 0.8%
    """
    
    # Микро-уровни для плавного трейлинга
    MICRO_STEP_1 = 0.003   # +0.3% — первый шаг после BE
    MICRO_STEP_2 = 0.008   # +0.8% — второй шаг
    MICRO_STEP_3 = 0.015   # +1.5% — третий шаг (начало классического трейла)
    
    # Безубыток
    BE_BUFFER_TP1 = 0.000  # SL = entry после TP1
    BE_BUFFER_TP2 = 0.002  # SL = entry + 0.2% после TP2
    
    # Трейлинг
    TRAIL_DISTANCE = 0.008  # 0.8% от текущей цены
    
    def __init__(self):
        self._states: Dict[str, TrailingState] = {}
    
    def get_state(self, symbol: str) -> Optional[TrailingState]:
        """Получить текущее состояние трейлинга для символа."""
        return self._states.get(symbol)
    
    def initialize(self, symbol: str, direction: str, entry_price: float, initial_sl: float):
        """Инициализация микро-трейлинга для новой позиции."""
        now = datetime.utcnow().isoformat()
        
        # Расчёт микро-уровней
        if direction == "long":
            micro_levels = {
                0: entry_price * (1 + self.BE_BUFFER_TP1),      # После TP1: BE
                1: entry_price * (1 + self.BE_BUFFER_TP2),      # После TP2: BE+0.2%
                2: entry_price * (1 + self.MICRO_STEP_2),       # После TP3: +0.8%
                3: entry_price * (1 + self.MICRO_STEP_3),       # После TP4: +1.5%
            }
        else:  # short
            micro_levels = {
                0: entry_price * (1 - self.BE_BUFFER_TP1),      # После TP1: BE
                1: entry_price * (1 - self.BE_BUFFER_TP2),      # После TP2: BE-0.2%
                2: entry_price * (1 - self.MICRO_STEP_2),       # После TP3: -0.8%
                3: entry_price * (1 - self.MICRO_STEP_3),       # После TP4: -1.5%
            }
        
        state = TrailingState(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            initial_sl=initial_sl,
            current_sl=initial_sl,
            micro_levels=micro_levels,
            activated_at=now,
            last_update=now,
            be_triggered=False,
            be2_triggered=False
        )
        
        self._states[symbol] = state
        return state
    
    def remove(self, symbol: str):
        """Удалить состояние трейлинга (при закрытии позиции)."""
        if symbol in self._states:
            del self._states[symbol]
    
    def calculate_micro_sl(self, symbol: str, tp_index: int, current_sl: float) -> Optional[float]:
        """
        Рассчитать новый SL на основе микро-уровня.
        
        Args:
            tp_index: Индекс только что взятого TP (0-based)
            current_sl: Текущий уровень SL
        
        Returns:
            Новый уровень SL или None если изменение не требуется
        """
        state = self._states.get(symbol)
        if not state:
            return None
        
        # TP1 (index 0) → BE
        if tp_index == 0 and not state.be_triggered:
            state.be_triggered = True
            new_sl = state.micro_levels.get(0, state.entry_price)
            if state.direction == "long":
                if new_sl > current_sl * 1.0001:
                    state.current_sl = new_sl
                    state.last_update = datetime.utcnow().isoformat()
                    return new_sl
            else:
                if new_sl < current_sl * 0.9999:
                    state.current_sl = new_sl
                    state.last_update = datetime.utcnow().isoformat()
                    return new_sl
        
        # TP2 (index 1) → BE + 0.2%
        if tp_index == 1 and not state.be2_triggered:
            state.be2_triggered = True
            new_sl = state.micro_levels.get(1, state.entry_price)
            if state.direction == "long":
                if new_sl > current_sl * 1.0001:
                    state.current_sl = new_sl
                    state.last_update = datetime.utcnow().isoformat()
                    return new_sl
            else:
                if new_sl < current_sl * 0.9999:
                    state.current_sl = new_sl
                    state.last_update = datetime.utcnow().isoformat()
                    return new_sl
        
        # TP3+ → микро-шаги
        if tp_index >= 2:
            micro_level = state.micro_levels.get(tp_index - 1)  # -1 т.к. TP3 → index 2
            if micro_level:
                if state.direction == "long":
                    if micro_level > current_sl * 1.0001:
                        state.current_sl = micro_level
                        state.last_update = datetime.utcnow().isoformat()
                        return micro_level
                else:
                    if micro_level < current_sl * 0.9999:
                        state.current_sl = micro_level
                        state.last_update = datetime.utcnow().isoformat()
                        return micro_level
        
        return None
    
    def calculate_trail_sl(self, symbol: str, current_price: float, current_sl: float) -> Optional[float]:
        """
        Рассчитать трейлинг SL на основе текущей цены.
        
        Args:
            current_price: Текущая рыночная цена
            current_sl: Текущий уровень SL
        
        Returns:
            Новый уровень SL или None если изменение не требуется
        """
        state = self._states.get(symbol)
        if not state:
            return None
        
        # Трейлинг только после BE2 (TP2 взят)
        if not state.be2_triggered:
            return None
        
        if state.direction == "long":
            # SL = цена - 0.8%
            new_sl = current_price * (1 - self.TRAIL_DISTANCE)
            if new_sl > current_sl * 1.003:  # Минимальный шаг 0.3%
                state.current_sl = new_sl
                state.last_update = datetime.utcnow().isoformat()
                return new_sl
        else:  # short
            # SL = цена + 0.8%
            new_sl = current_price * (1 + self.TRAIL_DISTANCE)
            if new_sl < current_sl * 0.997:  # Минимальный шаг 0.3%
                state.current_sl = new_sl
                state.last_update = datetime.utcnow().isoformat()
                return new_sl
        
        return None
    
    def get_trail_activation_level(self, symbol: str) -> Optional[float]:
        """Получить уровень активации трейлинга (после BE2)."""
        state = self._states.get(symbol)
        if not state:
            return None
        
        if state.direction == "long":
            return state.entry_price * (1 + self.BE_BUFFER_TP2)
        else:
            return state.entry_price * (1 - self.BE_BUFFER_TP2)
    
    def should_trail(self, symbol: str, current_price: float, profit_pct: float, 
                     trail_threshold: float) -> bool:
        """
        Проверить, следует ли активировать трейлинг.
        
        Args:
            current_price: Текущая цена
            profit_pct: Текущий профит в долях (0.03 = 3%)
            trail_threshold: Порог активации трейлинга (0.025 = 2.5%)
        
        Returns:
            True если трейлинг должен быть активирован
        """
        state = self._states.get(symbol)
        if not state:
            return False
        
        # Трейлинг только после BE2
        if not state.be2_triggered:
            return False
        
        return profit_pct > trail_threshold
    
    def get_all_states(self) -> Dict[str, TrailingState]:
        """Получить все активные состояния трейлинга."""
        return self._states.copy()
    
    def reset(self):
        """Сбросить все состояния (использовать с осторожностью)."""
        self._states.clear()


# Singleton instance
_micro_trailing_instance: Optional[MicroTrailingStop] = None


def get_micro_trailing() -> MicroTrailingStop:
    """Получить singleton экземпляр MicroTrailingStop."""
    global _micro_trailing_instance
    if _micro_trailing_instance is None:
        _micro_trailing_instance = MicroTrailingStop()
    return _micro_trailing_instance


def reset_micro_trailing():
    """Сбросить singleton (для тестов)."""
    global _micro_trailing_instance
    _micro_trailing_instance = None
