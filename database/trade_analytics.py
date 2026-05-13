"""
🆕 Trade Analytics — shared/database/trade_analytics.py

Детальное отслеживание TP1-6, SL, BE и P&L по уровням.
Интеграция с отчётами в Telegram.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class TradeResult:
    """Результат сделки с детализацией TP"""
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    pnl_percent: float
    pnl_usd: float
    tp_level: int  # 1-6 для TP, 0 для SL, -1 для BE
    tp_hit: bool
    sl_hit: bool
    be_hit: bool
    timeframe: str
    closed_at: datetime


class TradeAnalytics:
    """
    🆕 Аналитика сделок с разбивкой по TP уровням.
    
    Хранит в Redis:
    - trade_history:list — история всех сделок
    - tp_stats:{date}:{level} — статистика по TP уровням за день
    - daily_tp_report:{date} — готовый отчёт по TP
    """
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.TP_LEVELS = ["SL", "BE", "TP1", "TP2", "TP3", "TP4", "TP5", "TP6"]
    
    def record_trade(self, trade: TradeResult):
        """Записывает сделку с указанием TP уровня"""
        trade_data = {
            "symbol": trade.symbol,
            "direction": trade.direction,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "pnl_percent": trade.pnl_percent,
            "pnl_usd": trade.pnl_usd,
            "tp_level": trade.tp_level,
            "tp_hit": trade.tp_hit,
            "sl_hit": trade.sl_hit,
            "be_hit": trade.be_hit,
            "timeframe": trade.timeframe,
            "closed_at": trade.closed_at.isoformat(),
        }
        
        # Сохраняем в историю
        self.redis.lpush("trade_history", json.dumps(trade_data))
        self.redis.ltrim("trade_history", 0, 9999)  # Храним последние 10k
        
        # Обновляем статистику по TP уровню
        date_key = trade.closed_at.strftime("%Y-%m-%d")
        level_name = self._get_level_name(trade.tp_level)
        
        # Счётчик
        self.redis.hincrby(f"tp_stats:{date_key}", f"{level_name}:count", 1)
        
        # P&L суммарный
        self.redis.hincrbyfloat(f"tp_stats:{date_key}", f"{level_name}:pnl_pct", trade.pnl_percent)
        self.redis.hincrbyfloat(f"tp_stats:{date_key}", f"{level_name}:pnl_usd", trade.pnl_usd)
        
        # По timeframe
        self.redis.hincrby(f"tp_stats:{date_key}:by_tf", f"{trade.timeframe}:{level_name}", 1)
    
    def _get_level_name(self, level: int) -> str:
        """Конвертирует число в название уровня"""
        if level == 0:
            return "SL"
        elif level == -1:
            return "BE"
        elif 1 <= level <= 6:
            return f"TP{level}"
        return "UNKNOWN"
    
    def get_daily_tp_stats(self, date: str) -> Dict:
        """Получает статистику по TP уровням за день"""
        stats = self.redis.hgetall(f"tp_stats:{date}")
        
        result = {}
        for level in self.TP_LEVELS:
            count = int(stats.get(f"{level}:count", 0))
            pnl_pct = float(stats.get(f"{level}:pnl_pct", 0))
            pnl_usd = float(stats.get(f"{level}:pnl_usd", 0))
            avg_pnl = pnl_pct / count if count > 0 else 0
            
            result[level] = {
                "count": count,
                "pnl_percent": round(pnl_pct, 2),
                "pnl_usd": round(pnl_usd, 2),
                "avg_pnl": round(avg_pnl, 2),
            }
        
        return result
    
    def get_period_tp_report(self, start_date: str, end_date: str) -> Dict:
        """Отчёт по TP уровням за период (день/неделя/месяц)"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Агрегируем данные
        aggregated = defaultdict(lambda: {"count": 0, "pnl_percent": 0, "pnl_usd": 0})
        
        current = start
        while current <= end:
            date_key = current.strftime("%Y-%m-%d")
            daily_stats = self.get_daily_tp_stats(date_key)
            
            for level, data in daily_stats.items():
                aggregated[level]["count"] += data["count"]
                aggregated[level]["pnl_percent"] += data["pnl_percent"]
                aggregated[level]["pnl_usd"] += data["pnl_usd"]
            
            current += timedelta(days=1)
        
        # Добавляем средние значения
        for level in aggregated:
            count = aggregated[level]["count"]
            if count > 0:
                aggregated[level]["avg_pnl"] = round(
                    aggregated[level]["pnl_percent"] / count, 2
                )
        
        # Считаем итоги
        total_trades = sum(aggregated[level]["count"] for level in self.TP_LEVELS)
        winning_trades = sum(aggregated[f"TP{i}"]["count"] for i in range(1, 7))
        be_trades = aggregated["BE"]["count"]
        losing_trades = aggregated["SL"]["count"]
        
        total_pnl = sum(aggregated[level]["pnl_percent"] for level in self.TP_LEVELS)
        
        return {
            "period": f"{start_date} — {end_date}",
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "be_trades": be_trades,
            "losing_trades": losing_trades,
            "win_rate": round(winning_trades / total_trades * 100, 1) if total_trades else 0,
            "total_pnl_percent": round(total_pnl, 2),
            "total_pnl_usd": round(sum(aggregated[level]["pnl_usd"] for level in self.TP_LEVELS), 2),
            "by_level": dict(aggregated),
        }
    
    def format_tp_report(self, report: Dict) -> str:
        """Форматирует отчёт по TP для Telegram"""
        lines = [
            "📊 <b>ДЕТАЛЬНАЯ СТАТИСТИКА ПО УРОВНЯМ</b>",
            f"Период: {report['period']}",
            "",
            f"📈 <b>Всего сделок:</b> {report['total_trades']}",
            f"   ✅ TP (прибыль): {report['winning_trades']}",
            f"   ⚖️ BE (безубыток): {report['be_trades']}",
            f"   ❌ SL (убыток): {report['losing_trades']}",
            f"   🎯 Win Rate: {report['win_rate']}%",
            "",
            "💰 <b>ФИНАНСОВЫЙ РЕЗУЛЬТАТ:</b>",
            f"   Общий P&L: {report['total_pnl_percent']:+.2f}%",
            f"   P&L ($): ${report['total_pnl_usd']:,.2f}",
            "",
            "🎯 <b>РАСПРЕДЕЛЕНИЕ ПО TP УРОВНЯМ:</b>",
        ]
        
        # TP уровни
        for level in ["TP1", "TP2", "TP3", "TP4", "TP5", "TP6"]:
            data = report["by_level"].get(level, {"count": 0, "pnl_percent": 0, "avg_pnl": 0})
            if data["count"] > 0:
                lines.append(
                    f"   {level}: {data['count']:>3} сделок | "
                    f"P&L: {data['pnl_percent']:+7.2f}% | "
                    f"Средн: {data['avg_pnl']:+5.2f}%"
                )
        
        # BE
        be_data = report["by_level"].get("BE", {"count": 0})
        if be_data["count"] > 0:
            lines.append(f"   BE:  {be_data['count']:>3} сделок | P&L: 0.00%")
        
        # SL
        sl_data = report["by_level"].get("SL", {"count": 0, "pnl_percent": 0})
        if sl_data["count"] > 0:
            lines.append(
                f"   SL:  {sl_data['count']:>3} сделок | "
                f"P&L: {sl_data['pnl_percent']:+7.2f}% | "
                f"Средн: {sl_data['pnl_percent']/sl_data['count']:+5.2f}%"
            )
        
        return "\n".join(lines)


# Вспомогательная функция для интеграции
def record_trade_with_tp(
    redis_client,
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float,
    pnl_percent: float,
    pnl_usd: float,
    tp_level: int,  # 1-6 или 0 (SL) или -1 (BE)
    timeframe: str,
    closed_at: datetime = None,
):
    """
    🆕 Упрощённая функция для записи сделки с TP уровнем.
    
    Args:
        tp_level: 1-6 (TP1-6), 0 (SL), -1 (BE)
    """
    if closed_at is None:
        closed_at = datetime.utcnow()
    
    analytics = TradeAnalytics(redis_client)
    
    trade = TradeResult(
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        exit_price=exit_price,
        pnl_percent=pnl_percent,
        pnl_usd=pnl_usd,
        tp_level=tp_level,
        tp_hit=tp_level > 0,
        sl_hit=tp_level == 0,
        be_hit=tp_level == -1,
        timeframe=timeframe,
        closed_at=closed_at,
    )
    
    analytics.record_trade(trade)
