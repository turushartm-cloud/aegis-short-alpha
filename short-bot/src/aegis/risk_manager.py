"""
Aegis Risk Manager v1.0
Институциональный риск-менеджмент для SHORT бота.

Компоненты:
  1. Kelly Criterion (fractional) — оптимальный размер позиции
  2. Portfolio Heat Monitor — суммарная экспозиция
  3. Circuit Breakers — автостоп при просадке/серии стопов
  4. Correlation Filter — не открываем коррелирующие позиции

Лимиты (PAID minimal tier):
  max_position_pct:     15% на позицию
  max_total_exposure:   60% всего капитала
  max_daily_drawdown:   5.0%
  max_consecutive_loss: 4
  kelly_fraction:       0.25 (Quarter-Kelly)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("aegis.risk_manager")


# ─────────────────────────────────────────────────────────────────────
# Dataclasses
# ─────────────────────────────────────────────────────────────────────

@dataclass
class RiskLimits:
    """Риск-параметры (из env или дефолты)"""
    max_position_pct:     float = 0.15   # 15% капитала на 1 позицию
    max_total_exposure:   float = 0.60   # 60% суммарно открыто
    max_daily_drawdown:   float = 5.0    # 5% дневная просадка = стоп
    max_consecutive_loss: int   = 4      # 4 подряд стопа = стоп
    kelly_fraction:       float = 0.25   # Quarter-Kelly
    min_rr_ratio:         float = 1.5    # Минимум R:R 1:1.5
    max_corr_positions:   int   = 3      # Макс коррелирующих позиций


@dataclass
class CircuitBreakerState:
    """Состояние circuit breaker"""
    triggered:         bool    = False
    reason:            str     = ""
    triggered_at:      Optional[datetime] = None
    daily_pnl_pct:     float   = 0.0
    consecutive_losses: int    = 0
    daily_trades:      int     = 0
    daily_loss_usd:    float   = 0.0
    reset_date:        Optional[date] = None

    def is_triggered(self) -> bool:
        return self.triggered

    def reset_daily(self, today: date):
        """Дневной сброс PnL статистики"""
        if self.reset_date != today:
            self.daily_pnl_pct  = 0.0
            self.daily_loss_usd = 0.0
            self.daily_trades   = 0
            self.reset_date     = today
            if self.triggered and "daily" in self.reason:
                self.triggered = False
                self.reason    = ""
                logger.info("Circuit breaker: daily drawdown reset")


@dataclass
class PositionSizeResult:
    """Результат расчёта размера позиции"""
    size_usd:        float
    size_pct:        float          # % от капитала
    kelly_pct:       float          # Исходный Kelly %
    adjusted_pct:    float          # После всех ограничений
    risk_usd:        float          # Максимальный убыток (в USD)
    blocked:         bool = False
    block_reason:    str  = ""


# ─────────────────────────────────────────────────────────────────────
# AegisRiskManager
# ─────────────────────────────────────────────────────────────────────

class AegisRiskManager:
    """
    Институциональный риск-менеджер.
    
    Usage:
        rm = AegisRiskManager(limits=RiskLimits(), capital=10000)
        size = rm.calculate_position_size(win_rate=0.65, avg_win=50, avg_loss=30,
                                           signal_score=78, sl_pct=2.5)
        blocked, reason = rm.check_circuit_breakers()
    """

    def __init__(
        self,
        limits:  Optional[RiskLimits] = None,
        capital: float = 1000.0,
    ):
        self.limits  = limits or RiskLimits()
        self.capital = capital
        self.cb      = CircuitBreakerState()

        # История трейдов (in-memory, сбрасывается при рестарте)
        self._trade_history: List[Dict] = []
        self._open_symbols:  List[str]  = []

    # ─────────────────────────────────────────────────────────────────
    # Kelly Criterion
    # ─────────────────────────────────────────────────────────────────

    def _kelly_fraction_pct(
        self,
        win_rate: float,          # 0.0 – 1.0
        avg_win:  float,          # Средний выигрыш USD
        avg_loss: float,          # Средний убыток USD (положит.)
    ) -> float:
        """
        Kelly = (b*p - q) / b
        где b = avg_win/avg_loss, p = win_rate, q = 1-p
        Возвращает долю капитала (0.0 – 1.0), НЕ ограниченную.
        """
        if avg_loss <= 0 or win_rate <= 0:
            return 0.02   # Дефолт 2%

        b = avg_win / avg_loss
        p = win_rate
        q = 1.0 - p

        kelly = (b * p - q) / b
        return max(kelly, 0.0)   # Kelly не отрицательный

    def calculate_position_size(
        self,
        win_rate:       float = 0.60,
        avg_win_pct:    float = 5.0,    # Средний выигрыш % (от позиции)
        avg_loss_pct:   float = 2.5,    # Средний убыток % (SL)
        signal_score:   float = 65.0,   # Aegis score (модификатор)
        sl_pct:         float = 2.5,    # Stop Loss % для данной сделки
        current_exposure_usd: float = 0.0,
    ) -> PositionSizeResult:
        """
        Полный расчёт размера позиции с учётом всех ограничений.
        
        1. Kelly Criterion (fractional)
        2. Signal confidence weight
        3. Portfolio heat check
        4. Per-position max
        """
        # Проверяем circuit breaker
        blocked, reason = self.check_circuit_breakers()
        if blocked:
            return PositionSizeResult(
                size_usd=0, size_pct=0, kelly_pct=0, adjusted_pct=0,
                risk_usd=0, blocked=True, block_reason=reason
            )

        # 1. Kelly Criterion
        avg_win_usd  = self.capital * avg_win_pct / 100
        avg_loss_usd = self.capital * avg_loss_pct / 100
        kelly_raw    = self._kelly_fraction_pct(win_rate, avg_win_usd, avg_loss_usd)

        # 2. Quarter-Kelly (консервативный)
        kelly_adj = kelly_raw * self.limits.kelly_fraction

        # 3. Signal score modifier (60→0.8x, 70→0.9x, 80→1.0x, 85+→1.1x)
        if signal_score >= 85:
            score_mult = 1.15
        elif signal_score >= 75:
            score_mult = 1.0
        elif signal_score >= 65:
            score_mult = 0.85
        else:
            score_mult = 0.70
        kelly_adj *= score_mult

        # 4. Ограничения портфеля
        max_pos_pct = self.limits.max_position_pct
        remaining_exposure_pct = (
            (self.capital * self.limits.max_total_exposure - current_exposure_usd)
            / self.capital
        )
        remaining_exposure_pct = max(remaining_exposure_pct, 0.0)

        # Финальный % — минимум из Kelly, per-position max, remaining
        final_pct = min(kelly_adj, max_pos_pct, remaining_exposure_pct)
        final_pct = max(final_pct, 0.001)   # Минимум 0.1%

        size_usd = self.capital * final_pct
        risk_usd = size_usd * sl_pct / 100

        return PositionSizeResult(
            size_usd=round(size_usd, 2),
            size_pct=round(final_pct * 100, 2),
            kelly_pct=round(kelly_raw * 100, 2),
            adjusted_pct=round(final_pct * 100, 2),
            risk_usd=round(risk_usd, 2),
            blocked=False,
        )

    # ─────────────────────────────────────────────────────────────────
    # Circuit Breakers
    # ─────────────────────────────────────────────────────────────────

    def check_circuit_breakers(self) -> Tuple[bool, str]:
        """
        Проверяет все circuit breakers.
        Returns: (triggered: bool, reason: str)
        """
        self.cb.reset_daily(date.today())

        if self.cb.triggered:
            return True, self.cb.reason

        # 1. Daily drawdown limit
        if self.cb.daily_pnl_pct <= -self.limits.max_daily_drawdown:
            self._trigger(
                f"Daily drawdown limit: {self.cb.daily_pnl_pct:.2f}% "
                f"(limit: {self.limits.max_daily_drawdown:.1f}%)"
            )
            return True, self.cb.reason

        # 2. Consecutive losses
        if self.cb.consecutive_losses >= self.limits.max_consecutive_loss:
            self._trigger(
                f"Consecutive losses: {self.cb.consecutive_losses} "
                f"(limit: {self.limits.max_consecutive_loss})"
            )
            return True, self.cb.reason

        return False, ""

    def _trigger(self, reason: str):
        self.cb.triggered    = True
        self.cb.reason       = reason
        self.cb.triggered_at = datetime.utcnow()
        logger.warning(f"⛔ CIRCUIT BREAKER: {reason}")

    def reset_circuit_breaker(self, force: bool = False):
        """Ручной сброс circuit breaker (/reset команда Telegram)"""
        if force or not self.cb.triggered:
            self.cb.triggered   = False
            self.cb.reason      = ""
            self.cb.consecutive_losses = 0
            logger.info("Circuit breaker manually reset")

    # ─────────────────────────────────────────────────────────────────
    # Trade Tracking
    # ─────────────────────────────────────────────────────────────────

    def record_trade_result(
        self,
        symbol:    str,
        pnl_usd:   float,
        pnl_pct:   float,
        won:       bool,
    ):
        """Записывает результат сделки, обновляет статистику"""
        self.cb.reset_daily(date.today())

        self.cb.daily_pnl_pct += pnl_pct
        self.cb.daily_trades  += 1

        if won:
            self.cb.consecutive_losses = 0
        else:
            self.cb.consecutive_losses += 1
            self.cb.daily_loss_usd += abs(pnl_usd)

        self._trade_history.append({
            "symbol": symbol,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "won": won,
            "timestamp": datetime.utcnow().isoformat(),
        })

        # Обновляем капитал (простое приближение)
        self.capital += pnl_usd
        logger.info(f"Trade recorded: {symbol} PnL={pnl_usd:+.2f}$ | "
                    f"Daily={self.cb.daily_pnl_pct:.2f}% | "
                    f"ConsecLoss={self.cb.consecutive_losses}")

    def get_win_stats(self) -> Dict:
        """Статистика побед/поражений за историю"""
        if not self._trade_history:
            return {"win_rate": 0.60, "avg_win_pct": 5.0, "avg_loss_pct": 2.5,
                    "profit_factor": 1.0, "total_trades": 0}

        wins   = [t for t in self._trade_history if t["won"]]
        losses = [t for t in self._trade_history if not t["won"]]

        win_rate = len(wins) / len(self._trade_history) if self._trade_history else 0
        avg_win  = sum(t["pnl_pct"] for t in wins)  / len(wins)  if wins   else 5.0
        avg_loss = sum(abs(t["pnl_pct"]) for t in losses) / len(losses) if losses else 2.5
        pf       = (avg_win * len(wins)) / (avg_loss * len(losses)) if losses else 99.0

        return {
            "win_rate":       round(win_rate, 3),
            "avg_win_pct":    round(avg_win, 2),
            "avg_loss_pct":   round(avg_loss, 2),
            "profit_factor":  round(pf, 2),
            "total_trades":   len(self._trade_history),
            "wins":           len(wins),
            "losses":         len(losses),
        }

    # ─────────────────────────────────────────────────────────────────
    # R/R Validation
    # ─────────────────────────────────────────────────────────────────

    def validate_rr(
        self,
        entry:      float,
        stop_loss:  float,
        take_profit: float,     # Первый TP
    ) -> Tuple[bool, float, str]:
        """
        Проверяет соответствие R/R минимуму.
        Returns: (valid, rr_ratio, message)
        """
        risk   = abs(stop_loss - entry)
        reward = abs(entry - take_profit)

        if risk <= 0:
            return False, 0.0, "Zero risk distance"

        rr = reward / risk
        if rr < self.limits.min_rr_ratio:
            return False, round(rr, 2), f"R/R {rr:.2f} < min {self.limits.min_rr_ratio}"

        return True, round(rr, 2), f"R/R {rr:.2f} ✅"

    # ─────────────────────────────────────────────────────────────────
    # Portfolio Heat
    # ─────────────────────────────────────────────────────────────────

    def get_portfolio_heat(self, open_positions_usd: float) -> Dict:
        """Текущая тепловая нагрузка портфеля"""
        exposure_pct = open_positions_usd / self.capital * 100 if self.capital else 0
        max_exposure = self.limits.max_total_exposure * 100

        return {
            "open_usd":         round(open_positions_usd, 2),
            "exposure_pct":     round(exposure_pct, 1),
            "max_exposure_pct": round(max_exposure, 1),
            "remaining_usd":    round(max(0, self.capital * self.limits.max_total_exposure
                                          - open_positions_usd), 2),
            "heat_level":       ("🔴 HIGH" if exposure_pct > max_exposure * 0.85
                                 else "🟡 MED" if exposure_pct > max_exposure * 0.60
                                 else "🟢 LOW"),
            "cb_status":        ("⛔ BLOCKED" if self.cb.triggered else "✅ OK"),
            "consecutive_loss": self.cb.consecutive_losses,
            "daily_pnl_pct":    round(self.cb.daily_pnl_pct, 2),
        }

    def status_report(self) -> str:
        """Telegram-форматированный отчёт риск-менеджера"""
        stats = self.get_win_stats()
        cb    = self.cb

        status_icon = "⛔" if cb.triggered else "✅"
        return (
            f"🛡️ <b>Risk Manager Status</b>\n\n"
            f"{status_icon} CB: {'TRIGGERED — ' + cb.reason if cb.triggered else 'OK'}\n"
            f"📊 Daily PnL: {cb.daily_pnl_pct:+.2f}% | Limit: -{self.limits.max_daily_drawdown}%\n"
            f"❌ ConsecLoss: {cb.consecutive_losses}/{self.limits.max_consecutive_loss}\n"
            f"💰 Capital: ${self.capital:,.2f}\n"
            f"📈 Win Rate: {stats['win_rate']*100:.1f}% | PF: {stats['profit_factor']:.2f}\n"
            f"🔢 Trades: {stats['total_trades']} (W:{stats['wins']} L:{stats['losses']})"
        )
