"""
Micro-Step Trailing Stop v1.0
==============================
Плавное движение SL микро-шагами при взятии каждого TP.
Решает проблему выбивания при ретестах (агрессивный трейлинг → преждевременный SL hit).

Логика:
  TP1 взят → SL подтягивается на +0.3% от входа (LONG) / -0.3% (SHORT)
  TP2 взят → SL подтягивается на +0.8% от входа
  TP3 взят → SL подтягивается на +1.5% от входа
  TP4+     → SL подтягивается на +2.0% от входа

Singleton: get_micro_trailing() возвращает один экземпляр на процесс.
"""

from typing import Dict, Optional

# Смещение SL от точки входа при взятии каждого TP-уровня (% от entry)
_TP_OFFSETS: Dict[int, float] = {
    1: 0.003,   # TP1 → +0.3%
    2: 0.008,   # TP2 → +0.8%
    3: 0.015,   # TP3 → +1.5%
    4: 0.020,   # TP4 → +2.0%
}
_DEFAULT_OFFSET = 0.025  # TP5+ → +2.5%


class MicroTrailingStop:
    """
    Управляет состоянием микро-трейлинга для каждого открытого символа.

    State per symbol:
        direction    : "long" | "short"
        entry_price  : float
        initial_sl   : float
        current_sl   : float
        steps_taken  : int  (кол-во микро-шагов)
        total_moved  : float (суммарное смещение SL в %)
    """

    def __init__(self):
        self._states: Dict[str, Dict] = {}

    # ------------------------------------------------------------------
    # PUBLIC API
    # ------------------------------------------------------------------

    def initialize(self, symbol: str, direction: str,
                   entry_price: float, initial_sl: float) -> None:
        """Инициализирует трейлинг для нового символа."""
        self._states[symbol] = {
            "direction":   direction,
            "entry_price": entry_price,
            "initial_sl":  initial_sl,
            "current_sl":  initial_sl,
            "steps_taken": 0,
            "total_moved": 0.0,   # % от entry суммарно
        }

    def get_state(self, symbol: str) -> Optional[Dict]:
        """Возвращает текущее состояние трейлинга или None если не инициализирован."""
        return self._states.get(symbol)

    def on_tp_taken(self, symbol: str, tp_level: int,
                    current_price: float) -> Optional[float]:
        """
        Вызывается при взятии TP. Вычисляет новый SL и обновляет состояние.

        Args:
            symbol      : торговый символ
            tp_level    : номер взятого TP (1, 2, 3, ...)
            current_price: текущая рыночная цена (для контекста, не используется в расчёте)

        Returns:
            Новый уровень SL (float) если SL нужно передвинуть, иначе None.
        """
        state = self._states.get(symbol)
        if state is None:
            return None

        direction   = state["direction"]
        entry       = state["entry_price"]
        current_sl  = state["current_sl"]

        if entry <= 0:
            return None

        offset = _TP_OFFSETS.get(tp_level, _DEFAULT_OFFSET)

        if direction == "long":
            new_sl = entry * (1 + offset)
            # SL движется только вверх
            if new_sl <= current_sl:
                return None
        else:  # short
            new_sl = entry * (1 - offset)
            # SL движется только вниз (к прибыли)
            if new_sl >= current_sl:
                return None

        # Обновляем состояние
        moved_pct = abs(new_sl - current_sl) / entry * 100
        state["current_sl"]  = new_sl
        state["steps_taken"] += 1
        state["total_moved"] += moved_pct

        print(f"[MicroTrail][{symbol}] TP{tp_level}: SL {current_sl:.6f} → {new_sl:.6f} "
              f"(+{offset*100:.1f}% от входа, шаг #{state['steps_taken']})")

        return round(new_sl, 8)

    def get_summary(self, symbol: str) -> Optional[Dict]:
        """
        Возвращает сводку для Telegram-уведомления.

        Returns dict с полями:
            steps_taken      : int
            total_moved_pct  : float  (суммарно % защиты)
            current_sl       : float
        """
        state = self._states.get(symbol)
        if state is None or state["steps_taken"] == 0:
            return None

        return {
            "steps_taken":     state["steps_taken"],
            "total_moved_pct": round(state["total_moved"], 3),
            "current_sl":      state["current_sl"],
            "direction":       state["direction"],
            "entry_price":     state["entry_price"],
        }

    def remove(self, symbol: str) -> None:
        """Очищает состояние трейлинга при закрытии позиции."""
        self._states.pop(symbol, None)

    def clear_all(self) -> None:
        """Полная очистка (при рестарте бота)."""
        self._states.clear()

    def active_symbols(self) -> list:
        """Список символов с активным трейлингом."""
        return list(self._states.keys())


# ─── Singleton ────────────────────────────────────────────────────────────────

_INSTANCE: Optional[MicroTrailingStop] = None


def get_micro_trailing() -> MicroTrailingStop:
    """
    Возвращает единственный экземпляр MicroTrailingStop.
    Вызывай в __init__ компонентов — все разделяют одно состояние.
    """
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = MicroTrailingStop()
    return _INSTANCE
