"""
BTCMomentumGuard — детектор V-shape восстановления и импульсного разворота BTC.

Проблема, которую решает:
  BTC дампит до -4%/1h → боты генерируют SHORT сигналы (бонус за падение).
  Затем BTC V-shape восстанавливается до -0.5%/1h за 1-2 цикла.
  Боты уже в позиции на локальных низах → все стопы рвутся.

Детектирует:
  1. V-Shape Recovery SHORT: был ≤ -DUMP_THR%, теперь ≥ -RECOVERY_THR%
     → блок SHORT (pos_multiplier=0.0) или тяжёлый штраф
  2. Rapid Recovery: BTC вырос на RAPID_BOUNCE_PCT% за последние N циклов
     → снижение SHORT риска
  3. Для LONG-бота (зеркально): V-shape dump = блок LONG

Вызывается в scan_market() ОДИН РАЗ для всего цикла:
    guard = BTCMomentumGuard()            # создаётся один раз в state
    guard.update(btc_1h_change)          # вызвать перед каждым scan_symbol циклом
    mult = guard.get_short_multiplier()  # 0.0 / 0.5 / 0.75 / 1.0

ENV:
  BTC_VSHAPE_DUMP_THR      = -2.0   % — если BTC был ниже этого → "dump зафиксирован"
  BTC_VSHAPE_RECOVERY_THR  = -0.5   % — если BTC теперь выше этого → "recovery активна"
  BTC_VSHAPE_HISTORY_SIZE  = 4      циклов скана — окно памяти
  BTC_RAPID_BOUNCE_PCT     = 1.5    % — рост BTC за N циклов = "bounce confirmed"
  ENABLE_BTC_MOMENTUM_GUARD = true
"""
from __future__ import annotations

import os
import logging
from collections import deque
from typing import Deque, Optional

logger = logging.getLogger("btc_momentum_guard")

_ENABLED        = os.getenv("ENABLE_BTC_MOMENTUM_GUARD", "true").lower() == "true"
_DUMP_THR       = float(os.getenv("BTC_VSHAPE_DUMP_THR",     "-2.0"))   # был ниже X%
_RECOVERY_THR   = float(os.getenv("BTC_VSHAPE_RECOVERY_THR", "-0.5"))   # теперь выше X%
_HISTORY_SIZE   = int(os.getenv("BTC_VSHAPE_HISTORY_SIZE",   "4"))      # циклов памяти
_RAPID_BOUNCE   = float(os.getenv("BTC_RAPID_BOUNCE_PCT",    "1.5"))    # % роста за N циклов


class BTCMomentumGuard:
    """
    Singleton-компонент state.btc_momentum_guard.
    Обновляется ОДИН раз за цикл скана, используется всеми scan_symbol.
    """

    def __init__(self):
        self._history: Deque[float] = deque(maxlen=_HISTORY_SIZE)
        self._vshape_active: bool   = False
        self._vshape_reason: str    = ""
        self._current_btc_1h: float = 0.0

    def update(self, btc_1h_change: float) -> None:
        """Вызывать один раз перед каждым scan-циклом с текущим btc_change_1h."""
        self._current_btc_1h = btc_1h_change
        self._history.append(btc_1h_change)
        self._evaluate()

    def _evaluate(self) -> None:
        if not _ENABLED or len(self._history) < 2:
            self._vshape_active = False
            return

        hist = list(self._history)
        current = hist[-1]

        # ── Детект V-shape recovery ───────────────────────────────────────
        # Был сильный дамп, теперь восстановление
        had_dump = any(v <= _DUMP_THR for v in hist[:-1])  # был dump в истории
        now_recovering = current >= _RECOVERY_THR           # сейчас не падает

        if had_dump and now_recovering:
            dump_val = min(v for v in hist[:-1] if v <= _DUMP_THR)
            recovery_delta = current - dump_val
            self._vshape_active = True
            self._vshape_reason = (
                f"V-Shape: BTC был {dump_val:+.1f}% → сейчас {current:+.1f}% "
                f"(∆={recovery_delta:+.1f}%)"
            )
            logger.warning(f"[BTCMomentumGuard] {self._vshape_reason}")
            return

        # ── Детект Rapid Bounce без предварительного dump ────────────────
        # BTC быстро вырос на RAPID_BOUNCE_PCT за N циклов
        if len(hist) >= 2:
            min_prev = min(hist[:-1])
            rapid_bounce = current - min_prev >= _RAPID_BOUNCE
            if rapid_bounce:
                self._vshape_active = True
                self._vshape_reason = (
                    f"Rapid Bounce: BTC {min_prev:+.1f}% → {current:+.1f}% "
                    f"(+{current - min_prev:.1f}% за {len(hist)} цикла)"
                )
                logger.info(f"[BTCMomentumGuard] {self._vshape_reason}")
                return

        self._vshape_active = False
        self._vshape_reason = ""

    def get_short_multiplier(self) -> float:
        """
        Возвращает pos_multiplier для SHORT позиций.
        0.0 = полный блок (V-shape активен)
        0.5 = сильный штраф (легкий bounce)
        1.0 = нормально
        """
        if not _ENABLED:
            return 1.0

        if not self._vshape_active:
            return 1.0

        current = self._current_btc_1h
        hist    = list(self._history)

        # Если текущий BTC > +2% — жёсткий блок
        if current >= 2.0:
            return 0.0

        # Если V-shape и BTC уже выше -0.5% — блок
        if current >= _RECOVERY_THR:
            return 0.0

        # Промежуточная зона — сильный штраф
        return 0.5

    def get_long_multiplier(self) -> float:
        """
        Зеркальный мультипликатор для LONG-бота.
        Активен при rapid dump: BTC быстро упал → V-shape dump — опасно открывать LONG.
        """
        if not _ENABLED:
            return 1.0

        hist    = list(self._history)
        current = self._current_btc_1h

        if len(hist) < 2:
            return 1.0

        max_prev = max(hist[:-1])
        rapid_dump = max_prev - current >= _RAPID_BOUNCE

        if rapid_dump and current <= _DUMP_THR:
            return 0.0
        if rapid_dump:
            return 0.5

        return 1.0

    @property
    def is_vshape_active(self) -> bool:
        return _ENABLED and self._vshape_active

    @property
    def reason(self) -> str:
        return self._vshape_reason
