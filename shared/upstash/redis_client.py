"""
Upstash Redis Client для Dual Bot System
Бесплатный tier: 10,000 запросов/день, 256MB
"""

import os
import json
import redis
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from dataclasses import asdict


class UpstashRedisClient:
    """Клиент для Upstash Redis (бесплатный tier)"""
    
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL")
        if not self.redis_url:
            raise ValueError("REDIS_URL not provided and not in environment")
        
        # ✅ FIX: redis 5.x handles SSL automatically via rediss:// scheme.
        # Do NOT pass ssl_cert_reqs or ssl manually — it causes
        # "AbstractConnection.__init__() got an unexpected keyword argument 'ssl'"
        self.client = redis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5
        )
        
        # TTL для разных типов данных (в секундах)
        self.TTL = {
            "signal": 86400,        # 24 часа для сигналов
            "position": 604800,     # 7 дней для подтвержденных позиций
            "position_unconfirmed": 1800,  # ✅ FIX: 30 мин (было 60s — вызывало авто-экспири и "1m" сделки)
            "state": 3600,          # 1 час для состояния
            "stats": 2592000,       # 30 дней для статистики
            "cache": 300            # 5 минут для кэша API
        }
    
    def health_check(self) -> bool:
        """Проверка соединения с Redis"""
        try:
            return self.client.ping()
        except Exception as e:
            print(f"Redis health check failed: {e}")
            return False
    
    # =========================================================================
    # SIGNALS
    # =========================================================================
    
    def save_signal(self, bot_type: str, symbol: str, signal_data: Dict) -> bool:
        try:
            key = f"{bot_type}:signals:{symbol}"
            if "timestamp" not in signal_data:
                signal_data["timestamp"] = datetime.utcnow().isoformat()
            
            # 🆕 Динамический TTL: 60 сек для неподтвержденных, 7 дней для подтвержденных
            confirmed = signal_data.get("confirmed", False)
            if confirmed:
                ttl = self.TTL["position"]  # 7 дней
            else:
                ttl = self.TTL["position_unconfirmed"]  # 60 сек
            
            self.client.lpush(key, json.dumps(signal_data))
            self.client.expire(key, ttl)
            self.client.ltrim(key, 0, 49)
            
            # 🆕 Логирование TTL для дебага
            if not confirmed:
                print(f"⏱️ [Redis] {symbol}: TTL=30min (unconfirmed), will auto-expire if not confirmed")
            
            return True
        except Exception as e:
            print(f"Error saving signal: {e}")
            return False
    
    def get_signals(self, bot_type: str, symbol: str, limit: int = 10) -> List[Dict]:
        try:
            key = f"{bot_type}:signals:{symbol}"
            signals = self.client.lrange(key, 0, limit - 1)
            return [json.loads(s) for s in signals]
        except Exception as e:
            print(f"Error getting signals: {e}")
            return []
    
    def get_active_signals(self, bot_type: str) -> List[Dict]:
        try:
            pattern = f"{bot_type}:signals:*"
            keys = self.client.keys(pattern)
            active_signals = []
            for key in keys:
                signals = self.client.lrange(key, 0, 0)
                if signals:
                    signal = json.loads(signals[0])
                    if signal.get("status") == "active":
                        signal["symbol"] = key.split(":")[-1]
                        active_signals.append(signal)
            return active_signals
        except Exception as e:
            print(f"Error getting active signals: {e}")
            return []
    
    def update_signal_status(self, bot_type: str, symbol: str,
                             timestamp: str, new_status: str) -> bool:
        try:
            key = f"{bot_type}:signals:{symbol}"
            signals = self.client.lrange(key, 0, -1)
            for i, signal_json in enumerate(signals):
                signal = json.loads(signal_json)
                if signal.get("timestamp") == timestamp:
                    signal["status"] = new_status
                    signal["updated_at"] = datetime.utcnow().isoformat()
                    self.client.lset(key, i, json.dumps(signal))
                    return True
            return False
        except Exception as e:
            print(f"Error updating signal status: {e}")
            return False
    
    # =========================================================================
    # POSITIONS
    # =========================================================================
    
    def save_position(self, bot_type: str, symbol: str, position_data: Dict) -> bool:
        try:
            key = f"{bot_type}:positions:{symbol}"
            self.client.setex(key, self.TTL["position"], json.dumps(position_data))
            return True
        except Exception as e:
            print(f"Error saving position: {e}")
            return False
    
    def get_position(self, bot_type: str, symbol: str) -> Optional[Dict]:
        try:
            key = f"{bot_type}:positions:{symbol}"
            data = self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            print(f"Error getting position: {e}")
            return None
    
    def get_all_positions(self, bot_type: str) -> List[Dict]:
        try:
            pattern = f"{bot_type}:positions:*"
            keys = self.client.keys(pattern)
            positions = []
            for key in keys:
                data = self.client.get(key)
                if data:
                    pos = json.loads(data)
                    pos["symbol"] = key.split(":")[-1]
                    positions.append(pos)
            return positions
        except Exception as e:
            print(f"Error getting positions: {e}")
            return []
    
    def close_position(self, bot_type: str, symbol: str,
                       pnl: float, close_price: float, tp_level: str = "SL") -> bool:
        try:
            key = f"{bot_type}:positions:{symbol}"
            data = self.client.get(key)
            if data:
                position = json.loads(data)
                position["status"] = "closed"
                position["close_price"] = close_price
                position["pnl"] = pnl
                position["tp_level"] = tp_level  # ✅ FIX: Сохраняем tp_level
                position["closed_at"] = datetime.utcnow().isoformat()
                history_key = f"{bot_type}:history:{symbol}"
                self.client.lpush(history_key, json.dumps(position))
                self.client.ltrim(history_key, 0, 99)
                # 🆕 Также пишем в all_trades для статистики
                all_key = f"{bot_type}:all_trades"
                self.client.lpush(all_key, json.dumps(position))
                self.client.ltrim(all_key, 0, 9999)
                self.client.delete(key)
                return True
            return False
        except Exception as e:
            print(f"Error closing position: {e}")
            return False
    
    # =========================================================================
    # BOT STATE
    # =========================================================================
    
    def update_bot_state(self, bot_type: str, state_data: Dict) -> bool:
        try:
            key = f"{bot_type}:state"
            self.client.setex(key, self.TTL["state"], json.dumps(state_data))
            return True
        except Exception as e:
            print(f"Error updating bot state: {e}")
            return False
    
    def get_bot_state(self, bot_type: str) -> Optional[Dict]:
        try:
            key = f"{bot_type}:state"
            data = self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            print(f"Error getting bot state: {e}")
            return None
    
    # =========================================================================
    # STATISTICS
    # =========================================================================
    
    def update_daily_stats(self, bot_type: str, date: str, stats: Dict) -> bool:
        try:
            key = f"{bot_type}:stats:daily:{date}"
            self.client.setex(key, self.TTL["stats"], json.dumps(stats))
            return True
        except Exception as e:
            print(f"Error updating stats: {e}")
            return False
    
    def get_daily_stats(self, bot_type: str, date: str) -> Optional[Dict]:
        try:
            key = f"{bot_type}:stats:daily:{date}"
            data = self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            print(f"Error getting stats: {e}")
            return None
    
    def get_stats_range(self, bot_type: str, days: int = 30) -> List[Dict]:
        try:
            stats = []
            for i in range(days):
                date = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
                day_stats = self.get_daily_stats(bot_type, date)
                if day_stats:
                    day_stats["date"] = date
                    stats.append(day_stats)
            return stats
        except Exception as e:
            print(f"Error getting stats range: {e}")
            return []
    
    # =========================================================================
    # CACHE
    # =========================================================================
    
    def cache_set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        try:
            ttl = ttl or self.TTL["cache"]
            self.client.setex(key, ttl, json.dumps(value))
            return True
        except Exception as e:
            print(f"Error setting cache: {e}")
            return False
    
    def cache_get(self, key: str) -> Optional[Any]:
        try:
            data = self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            print(f"Error getting cache: {e}")
            return None
    
    # =========================================================================
    # CROSS-BOT SYNC
    # =========================================================================
    
    def check_opposite_signal(self, symbol: str, bot_type: str) -> Optional[Dict]:
        try:
            opposite = "long" if bot_type == "short" else "short"
            return self.get_active_signals(opposite)
        except Exception as e:
            print(f"Error checking opposite signal: {e}")
            return None
    
    def get_shared_market_data(self, symbol: str) -> Optional[Dict]:
        try:
            key = f"shared:market:{symbol}"
            data = self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            print(f"Error getting shared market data: {e}")
            return None
    
    def set_shared_market_data(self, symbol: str, data: Dict) -> bool:
        try:
            key = f"shared:market:{symbol}"
            self.client.setex(key, 60, json.dumps(data))
            return True
        except Exception as e:
            print(f"Error setting shared market data: {e}")
            return False
    
    # =========================================================================
    # RATE LIMITING
    # =========================================================================
    
    def check_rate_limit(self, action: str, max_requests: int = 10,
                         window: int = 60) -> bool:
        try:
            key = f"ratelimit:{action}:{datetime.utcnow().strftime('%Y%m%d%H%M')}"
            current = self.client.incr(key)
            if current == 1:
                self.client.expire(key, window)
            return current <= max_requests
        except Exception as e:
            print(f"Error checking rate limit: {e}")
            return True
    
    # =========================================================================
    # INFO & METRICS
    # =========================================================================
    
    def get_info(self) -> Dict:
        try:
            info = self.client.info()
            return {
                "used_memory": info.get("used_memory_human", "unknown"),
                "connected_clients": info.get("connected_clients", 0),
                "uptime": info.get("uptime_in_seconds", 0),
                "version": info.get("redis_version", "unknown")
            }
        except Exception as e:
            print(f"Error getting Redis info: {e}")
            return {}
    
    # =========================================================================
    # SIGNAL LOG — постоянный лог ВСЕХ сигналов (исполненных + пропущенных)
    # =========================================================================

    def save_signal_log(self, bot_type: str, signal_data: Dict) -> bool:
        """
        Сохраняет КАЖДЫЙ сигнал, который прошёл MIN_SCORE и попал в Telegram.
        Включает флаг executed=True/False и skip_reason.
        Используется для статистики сигналов и виртуальных трейдов.
        """
        try:
            key = f"{bot_type}:signal_log"
            entry = dict(signal_data)
            entry["log_ts"] = datetime.utcnow().isoformat()
            self.client.lpush(key, json.dumps(entry))
            self.client.ltrim(key, 0, 4999)  # Хранить последние 5000 сигналов
            return True
        except Exception as e:
            print(f"Error saving signal log: {e}")
            return False

    def get_signal_log(self, bot_type: str, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Получение лога сигналов для дашборда"""
        try:
            key = f"{bot_type}:signal_log"
            items = self.client.lrange(key, offset, offset + limit - 1)
            return [json.loads(i) for i in items]
        except Exception as e:
            print(f"Error getting signal log: {e}")
            return []

    # =========================================================================
    # VIRTUAL POSITIONS — мониторинг TP/SL для TG-only сигналов
    # =========================================================================

    def save_virtual_position(self, bot_type: str, symbol: str, signal_data: Dict) -> bool:
        """
        Сохраняет виртуальную позицию для мониторинга TP/SL.
        Используется для сигналов, не открытых на бирже (exchange_full, paused и т.д.)
        Структура: HASH {bot}:virtual_positions, field = {symbol}:{unix_ts}

        ✅ FIX: Дедупликация — не создаём новую виртуальную позицию если по этому символу
        уже есть открытая (outcome=None). Без этого бот каждый скан (~3 мин) добавлял
        дубликат CHIPUSDT/MEGAUSDT → в дашборде одна монета появлялась 3-4 раза.
        """
        try:
            key = f"{bot_type}:virtual_positions"

            # ✅ Проверяем: нет ли уже открытой виртуальной позиции по этому символу
            existing = self.client.hgetall(key)
            if existing:
                for field_key, val in existing.items():
                    try:
                        pos = json.loads(val)
                        pos_symbol = pos.get("symbol", "")
                        pos_outcome = pos.get("outcome")
                        # Совпадение символа + нет исхода (позиция ещё открыта)
                        if pos_symbol == symbol and pos_outcome is None:
                            print(f"[VIRT-DEDUP] {bot_type}: {symbol} уже в virtual_positions — пропускаем дубликат")
                            return False
                    except Exception:
                        continue

            ts = int(datetime.utcnow().timestamp())
            field = f"{symbol}:{ts}"
            entry = {
                **signal_data,
                "virtual_key": field,
                "virtual_opened_at": datetime.utcnow().isoformat(),
                "outcome": None,
                "bot_type": bot_type,
            }
            self.client.hset(key, field, json.dumps(entry))
            self.client.expire(key, 604800)  # 7 дней TTL для всего hash
            return True
        except Exception as e:
            print(f"Error saving virtual position: {e}")
            return False

    def get_virtual_positions(self, bot_type: str) -> Dict[str, Dict]:
        """Возвращает все активные виртуальные позиции (без исхода)"""
        try:
            key = f"{bot_type}:virtual_positions"
            data = self.client.hgetall(key)
            if not data:
                return {}
            return {field: json.loads(val) for field, val in data.items()}
        except Exception as e:
            print(f"Error getting virtual positions: {e}")
            return {}

    def close_virtual_position(self, bot_type: str, field: str, outcome: str,
                               outcome_price: float, pnl_pct: float) -> bool:
        """
        Закрывает виртуальную позицию с результатом.
        outcome: "tp" | "sl" | "expired"
        Запись уходит в {bot}:virtual_trades LIST (lpush, ltrim 5000).
        """
        try:
            key = f"{bot_type}:virtual_positions"
            raw = self.client.hget(key, field)
            if not raw:
                return False
            pos = json.loads(raw)
            pos["outcome"] = outcome
            pos["outcome_price"] = outcome_price
            pos["pnl_pct"] = pnl_pct
            pos["closed_at"] = datetime.utcnow().isoformat()
            # Записываем в историю
            hist_key = f"{bot_type}:virtual_trades"
            self.client.lpush(hist_key, json.dumps(pos))
            self.client.ltrim(hist_key, 0, 4999)
            # Удаляем из активных
            self.client.hdel(key, field)
            return True
        except Exception as e:
            print(f"Error closing virtual position: {e}")
            return False

    def get_virtual_trades(self, bot_type: str, limit: int = 100) -> List[Dict]:
        """Получение закрытых виртуальных сделок"""
        try:
            key = f"{bot_type}:virtual_trades"
            items = self.client.lrange(key, 0, limit - 1)
            return [json.loads(i) for i in items]
        except Exception as e:
            print(f"Error getting virtual trades: {e}")
            return []

    def get_memory_usage(self) -> Dict:
        try:
            info = self.client.info("memory")
            used = info.get("used_memory", 0)
            peak = info.get("used_memory_peak", 0)
            limit = 256 * 1024 * 1024
            return {
                "used_bytes": used,
                "used_mb": round(used / 1024 / 1024, 2),
                "peak_bytes": peak,
                "peak_mb": round(peak / 1024 / 1024, 2),
                "limit_mb": 256,
                "usage_percent": round(used / limit * 100, 2)
            }
        except Exception as e:
            print(f"Error getting memory usage: {e}")
            return {}


# ============================================================================
    # =========================================================================
    # PROXY METHODS — совместимость с MicroTrailingStop (redis-py interface)
    # =========================================================================

    def set(self, key: str, value: str, ex=None) -> bool:
        try:
            if ex:
                return bool(self.client.set(key, value, ex=ex))
            return bool(self.client.set(key, value))
        except Exception as e:
            print(f'[Redis] set error: {e}')
            return False

    def get(self, key: str):
        try:
            return self.client.get(key)
        except Exception as e:
            print(f'[Redis] get error: {e}')
            return None

    def keys(self, pattern: str = '*'):
        try:
            return self.client.keys(pattern)
        except Exception as e:
            print(f'[Redis] keys error: {e}')
            return []

    def delete(self, *keys: str) -> int:
        try:
            return self.client.delete(*keys)
        except Exception as e:
            print(f'[Redis] delete error: {e}')
            return 0

    def remove_position(self, bot_type: str, symbol: str) -> bool:
        """Удаляет ключ positions:{symbol} из Redis (вызывается при закрытии позиции)."""
        try:
            key = f"{bot_type}:positions:{symbol}"
            self.client.delete(key)
            print(f"[Redis] 🗑️ positions key deleted: {key}")
            return True
        except Exception as e:
            print(f"[Redis] remove_position error: {e}")
            return False

# SINGLETON INSTANCE
# ============================================================================

_redis_client = None

def get_redis_client() -> UpstashRedisClient:
    global _redis_client
    if _redis_client is None:
        _redis_client = UpstashRedisClient()
    return _redis_client
