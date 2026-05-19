"""
Aegis SystemicCrashGuard — A2
Блокирует LONG позиции при системном краше рынка:
  - BTC падает ≤ -SYSTEMIC_CRASH_BTC_PCT%/час (default -5%)
  - 80%+ символов с отрицательной ценой за 1ч (alts breadth)

Outlier bypass (is_crash_for_token):
  Если конкретный токен price_24h > CRASH_OUTLIER_PRICE_PCT% (def 15%)
  И volume_spike > CRASH_OUTLIER_VOL_SPIKE (def 2.0x) — токен divergирует
  от краша, блок снимается индивидуально.
"""
from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("aegis.systemic_crash_guard")

_CRASH_BTC_PCT     = float(os.getenv("SYSTEMIC_CRASH_BTC_PCT",        "-5.0"))
_CRASH_ALTS_RATIO  = float(os.getenv("SYSTEMIC_CRASH_ALTS_RATIO",      "0.80"))
_CRASH_COOLDOWN_M  = int(os.getenv("SYSTEMIC_CRASH_COOLDOWN_MIN",       "30"))
_POST_CRASH_CD_M   = int(os.getenv("CRASH_POST_COOLDOWN_MIN",           "30"))  # доп. cooldown ПОСЛЕ восстановления
_OUTLIER_PRICE_PCT = float(os.getenv("CRASH_OUTLIER_PRICE_PCT",        "15.0"))
_OUTLIER_VOL_SPIKE = float(os.getenv("CRASH_OUTLIER_VOL_SPIKE",         "2.0"))

# Risk multiplier thresholds (BTC 1h change)
_RISK_MULT_HARD_BLOCK = float(os.getenv("CRASH_RISK_MULT_BLOCK_PCT",  "-8.0"))   # 0.0x — hard block
_RISK_MULT_HALF       = float(os.getenv("CRASH_RISK_MULT_HALF_PCT",   "-5.0"))   # 0.5x
_RISK_MULT_REDUCE     = float(os.getenv("CRASH_RISK_MULT_REDUCE_PCT", "-3.0"))   # 0.75x


class SystemicCrashGuard:
    """
    Обновляется каждым scan_symbol:
      update(symbol, price_change_1h) → накапливает статистику
    В начале каждого скан-цикла вызывать reset_cycle().
    is_crash() → True если рынок в системном сливе.
    """

    def __init__(self):
        self._btc_change_1h:    float   = 0.0
        self._cycle_neg:        int     = 0  # символов с отриц. 1h в цикле
        self._cycle_total:      int     = 0  # всего символов в цикле
        self._crash_until:      datetime | None = None  # до когда crash-режим
        self._post_crash_until: datetime | None = None  # дополнительный cooldown ПОСЛЕ краша
        self._newly_detected:   bool    = False  # флаг: краш обнаружен в этом цикле впервые
        self._last_reason:      str     = ""

    def reset_cycle(self):
        """Сбросить счётчики альтов перед новым циклом скана."""
        self._cycle_neg   = 0
        self._cycle_total = 0

    def update_btc(self, btc_change_1h: float):
        self._btc_change_1h = btc_change_1h

    def update_symbol(self, price_change_1h: float):
        """Вызывать для каждого символа в scan loop."""
        self._cycle_total += 1
        if price_change_1h < 0:
            self._cycle_neg += 1

    def evaluate(self):
        """Вычислить crash-режим по накопленным данным цикла. Вызывать после скана всего вотчлиста."""
        btc_crash = self._btc_change_1h <= _CRASH_BTC_PCT
        alts_ratio = self._cycle_neg / self._cycle_total if self._cycle_total > 0 else 0.0
        alts_crash = alts_ratio >= _CRASH_ALTS_RATIO

        if btc_crash or alts_crash:
            _was_already_crashed = self._crash_until is not None and datetime.utcnow() < self._crash_until
            self._crash_until = datetime.utcnow() + timedelta(minutes=_CRASH_COOLDOWN_M)
            self._last_reason = (
                f"BTC {self._btc_change_1h:+.1f}%/1H"
                if btc_crash else
                f"Alts {alts_ratio:.0%} negative"
            )
            if not _was_already_crashed:
                # Первичное обнаружение краша в этом цикле — сигнализируем
                self._newly_detected = True
            logger.warning(
                f"[SYSTEMIC CRASH] Обнаружен — {self._last_reason}. "
                f"LONG заблокирован до {self._crash_until.strftime('%H:%M')} UTC"
            )

    def is_crash(self) -> bool:
        if self._crash_until is None:
            return False
        if datetime.utcnow() < self._crash_until:
            return True
        # Краш только что закончился → запускаем post-crash cooldown
        if self._post_crash_until is None or datetime.utcnow() >= self._post_crash_until:
            self._post_crash_until = datetime.utcnow() + timedelta(minutes=_POST_CRASH_CD_M)
            logger.warning(
                f"[SYSTEMIC CRASH] Режим краша снят, но post-crash cooldown активен ещё {_POST_CRASH_CD_M} мин "
                f"до {self._post_crash_until.strftime('%H:%M')} UTC — лонги блокированы"
            )
        self._crash_until = None
        return False

    def is_post_crash_cooldown(self) -> bool:
        """True в течение CRASH_POST_COOLDOWN_MIN после окончания crash-режима."""
        if self._post_crash_until is None:
            return False
        if datetime.utcnow() < self._post_crash_until:
            return True
        self._post_crash_until = None
        return False

    def was_newly_detected(self) -> bool:
        """Возвращает True и сбрасывает флаг — краш обнаружен впервые в этом цикле.
        Используется для однократного запуска экстренных действий (SL tighten)."""
        if self._newly_detected:
            self._newly_detected = False
            return True
        return False

    def is_crash_for_token(self, price_24h: float, vol_spike: float) -> bool:
        """
        Crash-блок с outlier bypass для конкретного токена.
        Если токен price_24h > порог И volume_spike > порог —
        он divergирует от рынка, блок снимается.
        """
        if not self.is_crash():
            return False
        if price_24h >= _OUTLIER_PRICE_PCT and vol_spike >= _OUTLIER_VOL_SPIKE:
            logger.info(
                f"[CRASH_GUARD] Outlier bypass: price_24h={price_24h:+.1f}% "
                f"vol×{vol_spike:.1f} — токен divergирует, LONG разрешён"
            )
            return False
        return True

    def get_position_multiplier(self) -> float:
        """
        Risk size multiplier based on current BTC 1h momentum.
        Returns 0.0 (hard block), 0.5, 0.75, or 1.0.
        Independent of crash cooldown — reflects live market intensity.
        """
        btc = self._btc_change_1h
        if btc <= _RISK_MULT_HARD_BLOCK:
            return 0.0
        if btc <= _RISK_MULT_HALF:
            return 0.5
        if btc <= _RISK_MULT_REDUCE:
            return 0.75
        return 1.0

    @property
    def reason(self) -> str:
        return self._last_reason
