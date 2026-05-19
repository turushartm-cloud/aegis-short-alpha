"""
Aegis SystemicPumpGuard — D
Блокирует SHORT позиции при системном памп рынка:
  - BTC растёт >= SYSTEMIC_PUMP_BTC_PCT%/час (default +3%)
  - >50% символов с price_trend=up (alts breadth)
Блокирует только SHORT на символах с HTF=BULLISH (локально, попарно).

Outlier bypass (is_pump_for_token):
  Если конкретный токен падает price_24h < -PUMP_OUTLIER_PRICE_PCT% (def -15%)
  И volume_spike > PUMP_OUTLIER_VOL_SPIKE (def 2.0x) — токен divergирует
  против памп рынка, SHORT блок снимается индивидуально.
"""
from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("aegis.systemic_pump_guard")

_PUMP_BTC_PCT         = float(os.getenv("SYSTEMIC_PUMP_BTC_PCT",    "3.0"))
_PUMP_ALTS_RATIO      = float(os.getenv("SYSTEMIC_PUMP_ALTS_RATIO", "0.50"))
_PUMP_COOLDOWN_M      = int(os.getenv("SYSTEMIC_PUMP_COOLDOWN_MIN", "30"))
_OUTLIER_PRICE_BEAR   = float(os.getenv("PUMP_OUTLIER_PRICE_PCT",   "15.0"))  # падение ≥ этого %
_OUTLIER_VOL_SPIKE    = float(os.getenv("PUMP_OUTLIER_VOL_SPIKE",    "2.0"))

# Risk multiplier thresholds (BTC 1h change) for short-bot
_RISK_MULT_HARD_BLOCK = float(os.getenv("PUMP_RISK_MULT_BLOCK_PCT",  "8.0"))   # 0.0x — hard block
_RISK_MULT_HALF       = float(os.getenv("PUMP_RISK_MULT_HALF_PCT",   "5.0"))   # 0.5x
_RISK_MULT_REDUCE     = float(os.getenv("PUMP_RISK_MULT_REDUCE_PCT", "3.0"))   # 0.75x


class SystemicPumpGuard:
    """
    Обновляется каждым scan_symbol через update_symbol(price_trend).
    В начале скан-цикла вызывать reset_cycle().
    is_pump() -> True если зафиксирован системный памп рынка.
    Блокирует SHORT только на символах с HTF=BULLISH.
    """

    def __init__(self):
        self._btc_change_1h: float           = 0.0
        self._cycle_up:      int             = 0
        self._cycle_total:   int             = 0
        self._pump_until:    datetime | None = None
        self._last_reason:   str             = ""

    def reset_cycle(self):
        self._cycle_up    = 0
        self._cycle_total = 0

    def update_btc(self, btc_change_1h: float):
        self._btc_change_1h = btc_change_1h

    def update_symbol(self, price_trend: str):
        self._cycle_total += 1
        if price_trend == "up":
            self._cycle_up += 1

    def evaluate(self):
        btc_pump   = self._btc_change_1h >= _PUMP_BTC_PCT
        alts_ratio = self._cycle_up / self._cycle_total if self._cycle_total > 0 else 0.0
        alts_pump  = alts_ratio >= _PUMP_ALTS_RATIO

        if btc_pump and alts_pump:
            self._pump_until = datetime.utcnow() + timedelta(minutes=_PUMP_COOLDOWN_M)
            self._last_reason = (
                f"BTC {self._btc_change_1h:+.1f}%/1H + {alts_ratio:.0%} альтов растут"
            )
            logger.warning(
                f"[SYSTEMIC PUMP] {self._last_reason}. "
                f"SHORT на HTF=BULLISH заблокирован до {self._pump_until.strftime('%H:%M')} UTC"
            )

    def is_pump(self) -> bool:
        if self._pump_until is None:
            return False
        if datetime.utcnow() < self._pump_until:
            return True
        self._pump_until = None
        return False

    def is_pump_for_token(self, price_24h: float, vol_spike: float) -> bool:
        """
        Pump-блок с outlier bypass для конкретного токена.
        Если токен падает price_24h < -порог И volume_spike > порог —
        токен divergирует против памп рынка, SHORT разрешён.
        """
        if not self.is_pump():
            return False
        if price_24h <= -_OUTLIER_PRICE_BEAR and vol_spike >= _OUTLIER_VOL_SPIKE:
            logger.info(
                f"[PUMP_GUARD] Outlier bypass: price_24h={price_24h:+.1f}% "
                f"vol×{vol_spike:.1f} — токен bearish divergence, SHORT разрешён"
            )
            return False
        return True

    def get_position_multiplier(self) -> float:
        """
        Risk size multiplier based on current BTC 1h momentum (for shorts).
        Strong pump = reduce SHORT size. Returns 0.0, 0.5, 0.75, or 1.0.
        """
        btc = self._btc_change_1h
        if btc >= _RISK_MULT_HARD_BLOCK:
            return 0.0
        if btc >= _RISK_MULT_HALF:
            return 0.5
        if btc >= _RISK_MULT_REDUCE:
            return 0.75
        return 1.0

    @property
    def reason(self) -> str:
        return self._last_reason
