"""
Aegis Performance Tracker v1.0
Real-time аналитика P&L, статистика сигналов, отчёты для Telegram.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger("aegis.performance_tracker")


@dataclass
class TradeRecord:
    symbol:      str
    direction:   str = "short"
    entry_price: float = 0.0
    exit_price:  float = 0.0
    entry_time:  str   = ""
    exit_time:   str   = ""
    pnl_usd:     float = 0.0
    pnl_pct:     float = 0.0
    size_usd:    float = 0.0
    won:         bool  = False
    exit_reason: str   = ""   # "TP1", "SL", "TP4", "manual"
    score:       float = 0.0
    strength:    str   = ""   # "ULTRA", "STRONG", etc.


@dataclass
class DailyStats:
    date:          str   = ""
    trades:        int   = 0
    wins:          int   = 0
    losses:        int   = 0
    pnl_usd:       float = 0.0
    pnl_pct:       float = 0.0
    best_trade:    float = 0.0
    worst_trade:   float = 0.0
    signals_sent:  int   = 0
    avg_score:     float = 0.0


class PerformanceTracker:
    """
    Отслеживает все сигналы и сделки.
    Хранит в памяти (Redis sync для персистентности).
    """

    def __init__(self, redis_client=None):
        self.redis = redis_client
        self._trades:   List[TradeRecord] = []
        self._signals:  List[Dict]        = []
        self._daily:    Dict[str, DailyStats] = {}
        self._start_time = datetime.utcnow()

    def record_signal(self, symbol: str, score: float, strength: str, direction: str = "short"):
        today = date.today().isoformat()
        if today not in self._daily:
            self._daily[today] = DailyStats(date=today)
        self._daily[today].signals_sent += 1

        # Обновляем средний скор
        d = self._daily[today]
        prev_total = d.avg_score * (d.signals_sent - 1)
        d.avg_score = round((prev_total + score) / d.signals_sent, 1)

        self._signals.append({
            "symbol": symbol, "score": score, "strength": strength,
            "direction": direction, "timestamp": datetime.utcnow().isoformat(),
        })

    def record_trade(self, trade: TradeRecord):
        self._trades.append(trade)
        today = date.today().isoformat()
        if today not in self._daily:
            self._daily[today] = DailyStats(date=today)

        d = self._daily[today]
        d.trades  += 1
        d.pnl_usd += trade.pnl_usd
        d.pnl_pct += trade.pnl_pct
        if trade.won:
            d.wins += 1
        else:
            d.losses += 1
        d.best_trade  = max(d.best_trade, trade.pnl_usd)
        d.worst_trade = min(d.worst_trade, trade.pnl_usd)

    def get_stats(self, days: int = 7) -> Dict:
        """Сводная статистика за N дней"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        recent = [t for t in self._trades
                  if t.entry_time and datetime.fromisoformat(t.entry_time) >= cutoff]

        if not recent:
            return {
                "period_days":    days,
                "total_trades":   0,
                "win_rate":       0.0,
                "profit_factor":  0.0,
                "total_pnl_usd":  0.0,
                "max_drawdown":   0.0,
                "sharpe":         0.0,
                "avg_score":      0.0,
                "signals_total":  len(self._signals),
            }

        wins   = [t for t in recent if t.won]
        losses = [t for t in recent if not t.won]

        win_rate   = len(wins) / len(recent) if recent else 0
        avg_win    = sum(t.pnl_pct for t in wins)  / len(wins)   if wins   else 0
        avg_loss   = sum(abs(t.pnl_pct) for t in losses) / len(losses) if losses else 1
        pf         = (avg_win * len(wins)) / (avg_loss * len(losses)) if losses else 99.0
        total_pnl  = sum(t.pnl_usd for t in recent)

        # Max Drawdown (упрощённый)
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in recent:
            cumulative += t.pnl_usd
            peak = max(peak, cumulative)
            dd = (peak - cumulative) / peak * 100 if peak > 0 else 0
            max_dd = max(max_dd, dd)

        # Sharpe (упрощённый)
        returns = [t.pnl_pct for t in recent]
        if len(returns) > 1:
            import statistics
            avg_r = statistics.mean(returns)
            std_r = statistics.stdev(returns)
            sharpe = avg_r / std_r * (252 ** 0.5) if std_r > 0 else 0
        else:
            sharpe = 0.0

        avg_score = (sum(t.score for t in recent) / len(recent)) if recent else 0

        return {
            "period_days":   days,
            "total_trades":  len(recent),
            "wins":          len(wins),
            "losses":        len(losses),
            "win_rate":      round(win_rate, 3),
            "avg_win_pct":   round(avg_win, 2),
            "avg_loss_pct":  round(avg_loss, 2),
            "profit_factor": round(pf, 2),
            "total_pnl_usd": round(total_pnl, 2),
            "max_drawdown":  round(max_dd, 2),
            "sharpe":        round(sharpe, 2),
            "avg_score":     round(avg_score, 1),
            "signals_total": len(self._signals),
        }

    def daily_report(self) -> str:
        """Telegram ежедневный отчёт"""
        today = date.today().isoformat()
        d     = self._daily.get(today, DailyStats(date=today))
        s7    = self.get_stats(7)

        wr = f"{d.wins}/{d.trades}" if d.trades else "0/0"
        uptime = datetime.utcnow() - self._start_time
        uptime_str = f"{int(uptime.total_seconds()//3600)}h {int((uptime.total_seconds()%3600)//60)}m"

        return (
            f"📊 <b>Aegis Daily Report</b> — {today}\n\n"
            f"<b>Сегодня:</b>\n"
            f"  Сигналов: {d.signals_sent} | Avg Score: {d.avg_score:.1f}%\n"
            f"  Сделок: {d.trades} | W/L: {wr}\n"
            f"  PnL: {d.pnl_usd:+.2f}$ ({d.pnl_pct:+.2f}%)\n"
            f"  Лучшая: +{d.best_trade:.2f}$ | Худшая: {d.worst_trade:.2f}$\n\n"
            f"<b>7 дней:</b>\n"
            f"  Сделок: {s7['total_trades']} | WR: {s7['win_rate']*100:.1f}%\n"
            f"  PF: {s7['profit_factor']:.2f} | Sharpe: {s7['sharpe']:.2f}\n"
            f"  PnL: {s7['total_pnl_usd']:+.2f}$ | MaxDD: {s7['max_drawdown']:.1f}%\n\n"
            f"⏱️ Uptime: {uptime_str}"
        )
