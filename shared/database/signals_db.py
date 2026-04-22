"""
🆕 SQLite Signals Database — shared/database/signals_db.py

Сохранение истории сигналов и P&L для аналитики.
Альтернатива Redis для долгосрочного хранения.
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from contextlib import contextmanager


@dataclass
class SignalRecord:
    """Запись о сигнале в БД"""
    id: Optional[int]
    timestamp: datetime
    symbol: str
    direction: str  # "long" | "short"
    timeframe: str  # "15m", "45m", "1h", "2h", "4h"
    score: int
    confidence: float
    entry_price: float
    oi_change: float
    price_change: float
    volume_spike: float
    recommended_sl: float
    recommended_tp: float
    leverage: int
    pattern_name: str
    bot_type: str  # "long" | "short"
    
    # Исполнение
    executed: bool = False
    execution_price: Optional[float] = None
    execution_time: Optional[datetime] = None
    
    # Результат
    closed: bool = False
    close_price: Optional[float] = None
    close_time: Optional[datetime] = None
    pnl_percent: Optional[float] = None
    pnl_usd: Optional[float] = None
    status: str = "pending"  # pending, active, closed, cancelled


class SignalsDatabase:
    """
    🆕 SQLite database для хранения сигналов и результатов.
    
    Features:
    - История всех сигналов
    - P&L tracking
    - Аналитика по timeframes, patterns
    - Статистика win rate
    """
    
    def __init__(self, db_path: str = "signals_history.db"):
        self.db_path = Path(db_path)
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        """Контекстный менеджер для соединений"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_db(self):
        """Создаёт таблицы если не существуют"""
        with self._get_connection() as conn:
            # Основная таблица сигналов
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    score INTEGER NOT NULL,
                    confidence REAL,
                    entry_price REAL,
                    oi_change REAL,
                    price_change REAL,
                    volume_spike REAL,
                    recommended_sl REAL,
                    recommended_tp REAL,
                    leverage INTEGER,
                    pattern_name TEXT,
                    bot_type TEXT,
                    
                    -- Execution
                    executed BOOLEAN DEFAULT 0,
                    execution_price REAL,
                    execution_time TEXT,
                    
                    -- Result
                    closed BOOLEAN DEFAULT 0,
                    close_price REAL,
                    close_time TEXT,
                    pnl_percent REAL,
                    pnl_usd REAL,
                    status TEXT DEFAULT 'pending',
                    
                    -- Meta
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Индексы для быстрых запросов
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_timeframe ON signals(timeframe)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_bot ON signals(bot_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)")
            
            # Таблица для multi-timeframe агрегации
            conn.execute("""
                CREATE TABLE IF NOT EXISTS multi_tf_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_signal_id INTEGER,
                    timeframe TEXT NOT NULL,
                    score INTEGER,
                    FOREIGN KEY (parent_signal_id) REFERENCES signals(id)
                )
            """)
            
            # Таблица для ежедневной статистики
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    date TEXT PRIMARY KEY,
                    bot_type TEXT NOT NULL,
                    total_signals INTEGER DEFAULT 0,
                    executed_signals INTEGER DEFAULT 0,
                    closed_trades INTEGER DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    losing_trades INTEGER DEFAULT 0,
                    total_pnl_percent REAL,
                    total_pnl_usd REAL,
                    avg_score REAL,
                    best_trade_symbol TEXT,
                    best_trade_pnl REAL,
                    worst_trade_symbol TEXT,
                    worst_trade_pnl REAL
                )
            """)
            
            conn.commit()
    
    def save_signal(self, signal: SignalRecord) -> int:
        """Сохраняет сигнал и возвращает ID"""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO signals (
                    timestamp, symbol, direction, timeframe, score, confidence,
                    entry_price, oi_change, price_change, volume_spike,
                    recommended_sl, recommended_tp, leverage, pattern_name, bot_type,
                    executed, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.timestamp.isoformat(),
                signal.symbol,
                signal.direction,
                signal.timeframe,
                signal.score,
                signal.confidence,
                signal.entry_price,
                signal.oi_change,
                signal.price_change,
                signal.volume_spike,
                signal.recommended_sl,
                signal.recommended_tp,
                signal.leverage,
                signal.pattern_name,
                signal.bot_type,
                signal.executed,
                signal.status
            ))
            conn.commit()
            return cursor.lastrowid
    
    def mark_executed(self, signal_id: int, execution_price: float):
        """Отмечает сигнал как исполненный"""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE signals 
                SET executed = 1, 
                    execution_price = ?,
                    execution_time = ?,
                    status = 'active',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (execution_price, datetime.utcnow().isoformat(), signal_id))
            conn.commit()
    
    def close_signal(self, signal_id: int, close_price: float, 
                     pnl_percent: float, pnl_usd: float = None):
        """Закрывает сигнал и сохраняет P&L"""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE signals 
                SET closed = 1,
                    close_price = ?,
                    close_time = ?,
                    pnl_percent = ?,
                    pnl_usd = ?,
                    status = 'closed',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (close_price, datetime.utcnow().isoformat(), 
                  pnl_percent, pnl_usd, signal_id))
            conn.commit()
    
    def get_signal_by_id(self, signal_id: int) -> Optional[SignalRecord]:
        """Получает сигнал по ID"""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM signals WHERE id = ?", (signal_id,)
            ).fetchone()
            
            if row:
                return self._row_to_signal(row)
            return None
    
    def get_signals_by_symbol(self, symbol: str, 
                             days: int = 7) -> List[SignalRecord]:
        """Получает сигналы по символу за последние N дней"""
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        with self._get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM signals 
                WHERE symbol = ? AND timestamp > ?
                ORDER BY timestamp DESC
            """, (symbol, since)).fetchall()
            
            return [self._row_to_signal(row) for row in rows]
    
    def get_stats_by_timeframe(self, timeframe: str, 
                               days: int = 30,
                               bot_type: str = None) -> Dict[str, Any]:
        """Статистика по конкретному таймфрейму"""
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN executed = 1 THEN 1 ELSE 0 END) as executed,
                SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as winners,
                SUM(CASE WHEN pnl_percent < 0 THEN 1 ELSE 0 END) as losers,
                AVG(pnl_percent) as avg_pnl,
                AVG(score) as avg_score
            FROM signals 
            WHERE timeframe = ? AND timestamp > ?
        """
        params = [timeframe, since]
        
        if bot_type:
            query += " AND bot_type = ?"
            params.append(bot_type)
        
        with self._get_connection() as conn:
            row = conn.execute(query, params).fetchone()
            
            return {
                "timeframe": timeframe,
                "total_signals": row["total"] or 0,
                "executed": row["executed"] or 0,
                "closed": row["closed"] or 0,
                "winners": row["winners"] or 0,
                "losers": row["losers"] or 0,
                "win_rate": (row["winners"] / row["closed"] * 100) if row["closed"] else 0,
                "avg_pnl": row["avg_pnl"] or 0,
                "avg_score": row["avg_score"] or 0,
            }
    
    def get_pattern_performance(self, pattern_name: str, days: int = 30) -> Dict:
        """Анализ эффективности паттерна"""
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        with self._get_connection() as conn:
            row = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    AVG(pnl_percent) as avg_pnl,
                    SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as winners,
                    SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed
                FROM signals 
                WHERE pattern_name = ? AND timestamp > ? AND closed = 1
            """, (pattern_name, since)).fetchone()
            
            return {
                "pattern": pattern_name,
                "total_signals": row["total"] or 0,
                "avg_pnl": row["avg_pnl"] or 0,
                "winners": row["winners"] or 0,
                "win_rate": (row["winners"] / row["closed"] * 100) if row["closed"] else 0,
            }
    
    def get_best_timeframe(self, bot_type: str = None, days: int = 30) -> List[Dict]:
        """Возвращает рейтинг таймфреймов по P&L"""
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        query = """
            SELECT 
                timeframe,
                COUNT(*) as total,
                AVG(pnl_percent) as avg_pnl,
                SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as winners,
                SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed
            FROM signals 
            WHERE timestamp > ? AND closed = 1
        """
        params = [since]
        
        if bot_type:
            query += " AND bot_type = ?"
            params.append(bot_type)
        
        query += " GROUP BY timeframe ORDER BY avg_pnl DESC"
        
        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            
            return [
                {
                    "timeframe": row["timeframe"],
                    "total_signals": row["total"],
                    "avg_pnl": row["avg_pnl"] or 0,
                    "winners": row["winners"] or 0,
                    "win_rate": (row["winners"] / row["closed"] * 100) if row["closed"] else 0,
                }
                for row in rows
            ]
    
    def _row_to_signal(self, row: sqlite3.Row) -> SignalRecord:
        """Конвертирует строку БД в SignalRecord"""
        return SignalRecord(
            id=row["id"],
            timestamp=datetime.fromisoformat(row["timestamp"]),
            symbol=row["symbol"],
            direction=row["direction"],
            timeframe=row["timeframe"],
            score=row["score"],
            confidence=row["confidence"],
            entry_price=row["entry_price"],
            oi_change=row["oi_change"],
            price_change=row["price_change"],
            volume_spike=row["volume_spike"],
            recommended_sl=row["recommended_sl"],
            recommended_tp=row["recommended_tp"],
            leverage=row["leverage"],
            pattern_name=row["pattern_name"],
            bot_type=row["bot_type"],
            executed=bool(row["executed"]),
            execution_price=row["execution_price"],
            execution_time=datetime.fromisoformat(row["execution_time"]) if row["execution_time"] else None,
            closed=bool(row["closed"]),
            close_price=row["close_price"],
            close_time=datetime.fromisoformat(row["close_time"]) if row["close_time"] else None,
            pnl_percent=row["pnl_percent"],
            pnl_usd=row["pnl_usd"],
            status=row["status"],
        )
    
    def cleanup_old_signals(self, days: int = 90):
        """Очищает старые сигналы (кроме закрытых с P&L)"""
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        with self._get_connection() as conn:
            conn.execute("""
                DELETE FROM signals 
                WHERE timestamp < ? 
                AND (closed = 0 OR pnl_percent IS NULL)
            """, (since,))
            conn.commit()


# Singleton instance
_signals_db = None

def get_signals_db(db_path: str = "signals_history.db") -> SignalsDatabase:
    """Возвращает singleton instance базы"""
    global _signals_db
    if _signals_db is None:
        _signals_db = SignalsDatabase(db_path)
    return _signals_db
