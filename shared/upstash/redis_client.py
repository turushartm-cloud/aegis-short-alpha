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
            "position": 604800,     # 7 дней для позиций
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
            self.client.lpush(key, json.dumps(signal_data))
            self.client.expire(key, self.TTL["signal"])
            self.client.ltrim(key, 0, 49)
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
                       pnl: float, close_price: float) -> bool:
        try:
            key = f"{bot_type}:positions:{symbol}"
            data = self.client.get(key)
            if data:
                position = json.loads(data)
                position["status"] = "closed"
                position["close_price"] = close_price
                position["pnl"] = pnl
                position["closed_at"] = datetime.utcnow().isoformat()
                history_key = f"{bot_type}:history:{symbol}"
                self.client.lpush(history_key, json.dumps(position))
                self.client.ltrim(history_key, 0, 99)
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
# SINGLETON INSTANCE
# ============================================================================

_redis_client = None

def get_redis_client() -> UpstashRedisClient:
    global _redis_client
    if _redis_client is None:
        _redis_client = UpstashRedisClient()
    return _redis_client
