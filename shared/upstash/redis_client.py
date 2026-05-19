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
        # ✅ POSITION_TTL_DAYS можно задать в ENV (дефолт: 1 день)
        _pos_days = int(os.getenv("POSITION_TTL_DAYS", "1"))
        self.TTL = {
            "signal": 86400,
            "position": _pos_days * 86400,
            "position_unconfirmed": 1800,
            "state": 3600,
            "stats": 2592000,
            "cache": 300
        }
        # ✅ OPT v19: In-memory cache — снижает кол-во Redis reads с 22k до ~4k/час
        # Кэш активных сигналов (8s TTL): position_tracker вызывает каждые 30-60s,
        # несколько параллельных вызовов за одну итерацию → одно реальное обращение к Redis
        import time as _time_module
        self._signals_cache: dict = {}   # {bot_type: (timestamp, signals_list)}
        self._signals_cache_ttl = 8.0    # секунд
    
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
    
    def get_active_signals(self, bot_type: str, use_cache: bool = True) -> List[Dict]:
        """
        ✅ OPT v19: Возвращает активные сигналы/позиции.
        
        Оптимизации:
        1. In-memory cache (8s) — избегает повторных Redis запросов
        2. Индекс {bot}:active_index (Redis SET) — O(1) lookup вместо KEYS scan O(N)
        3. Pipeline для batch-чтения нескольких позиций
        """
        import time as _t
        now = _t.time()
        # 1. In-memory cache check
        if use_cache and bot_type in self._signals_cache:
            ts, cached = self._signals_cache[bot_type]
            if now - ts < self._signals_cache_ttl:
                return cached

        active_signals = []
        try:
            # 2. Попытка использовать активный индекс (если создан)
            index_key = f"{bot_type}:active_index"
            active_symbols = self.client.smembers(index_key)

            if active_symbols:
                # Pipeline: читаем все позиции за 1 round-trip
                pipe = self.client.pipeline()
                sym_list = [s.decode() if isinstance(s, bytes) else s for s in active_symbols]
                for sym in sym_list:
                    pipe.get(f"{bot_type}:positions:{sym}")
                results = pipe.execute()
                for sym, raw in zip(sym_list, results):
                    if raw:
                        try:
                            sig = json.loads(raw)
                            if sig.get("status") in ("active", None):
                                sig["symbol"] = sym
                                active_signals.append(sig)
                        except Exception:
                            pass
            else:
                # Fallback: KEYS scan (медленный, но надёжный)
                pattern = f"{bot_type}:positions:*"
                keys = self.client.keys(pattern)
                if keys:
                    pipe = self.client.pipeline()
                    for k in keys:
                        pipe.get(k)
                    vals = pipe.execute()
                    for k, raw in zip(keys, vals):
                        if raw:
                            try:
                                sig = json.loads(raw)
                                sym = k.decode().split(":")[-1] if isinstance(k, bytes) else k.split(":")[-1]
                                sig["symbol"] = sym
                                active_signals.append(sig)
                            except Exception:
                                pass

        except Exception as e:
            print(f"Error getting active signals: {e}")

        # Update cache
        self._signals_cache[bot_type] = (now, active_signals)
        return active_signals

    def _index_add(self, bot_type: str, symbol: str) -> None:
        """Добавляет символ в активный индекс при открытии позиции."""
        try:
            self.client.sadd(f"{bot_type}:active_index", symbol)
            self.client.expire(f"{bot_type}:active_index", 7 * 86400)
        except Exception:
            pass

    def _index_remove(self, bot_type: str, symbol: str) -> None:
        """Убирает символ из активного индекса при закрытии позиции."""
        try:
            self.client.srem(f"{bot_type}:active_index", symbol)
        except Exception:
            pass

    def _index_invalidate_cache(self, bot_type: str) -> None:
        """Сбрасывает in-memory кэш при записи позиции."""
        self._signals_cache.pop(bot_type, None)
    
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
            self._index_add(bot_type, symbol)       # ✅ OPT: индекс активных
            self._index_invalidate_cache(bot_type)  # ✅ OPT: сброс in-mem кэша
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
                position["tp_level"] = tp_level  # ✅ FIX: Сохраняем tp_level
                position["closed_at"] = datetime.utcnow().isoformat()

                # ✅ B8-FIX #2: Восстанавливаем реальный close_price и pnl
                # auto_trader.close_position() передаёт 0.0 — используем last_price из позиции
                _entry = float(position.get("entry_price", 0) or 0)
                _direction = position.get("direction", "long")
                _actual_close = close_price if close_price > 0 else float(
                    position.get("last_price", 0) or position.get("entry_price", 0) or 0
                )
                position["close_price"] = _actual_close

                if pnl == 0.0 and _entry > 0 and _actual_close > 0:
                    if _direction == "short":
                        pnl = round((_entry - _actual_close) / _entry * 100, 4)
                    else:
                        pnl = round((_actual_close - _entry) / _entry * 100, 4)

                position["pnl"]     = pnl
                position["pnl_pct"] = pnl  # PatternML читает оба поля

                history_key = f"{bot_type}:history:{symbol}"
                self.client.lpush(history_key, json.dumps(position))
                self.client.ltrim(history_key, 0, 99)
                # 🆕 Также пишем в all_trades для PatternML статистики
                all_key = f"{bot_type}:all_trades"
                self.client.lpush(all_key, json.dumps(position))
                self.client.ltrim(all_key, 0, 9999)
                self.client.expire(all_key, 7776000)  # 90 дней
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
    # INFO & METRICS
    # =========================================================================

    def get_info(self) -> Dict:
        """Информация о Redis (статистика сервера)."""
        try:
            info = self.client.info()
            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_human": info.get("used_memory_human", "N/A"),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0),
            }
        except Exception as e:
            print(f"Error getting Redis info: {e}")
            return {}

    def get_memory_usage(self) -> Dict:
        """Использование памяти Redis."""
        try:
            info = self.client.info("memory")
            return {
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "N/A"),
                "used_memory_peak_human": info.get("used_memory_peak_human", "N/A"),
                "maxmemory_human": info.get("maxmemory_human", "N/A"),
            }
        except Exception as e:
            print(f"Error getting memory usage: {e}")
            return {}

    def check_rate_limit(self, key: str, max_calls: int, window_seconds: int) -> bool:
        """Rate limiting через Redis INCR+EXPIRE. True = разрешено, False = превышен лимит."""
        try:
            rl_key = f"rl:{key}"
            current = self.client.incr(rl_key)
            if current == 1:
                self.client.expire(rl_key, window_seconds)
            return current <= max_calls
        except Exception as e:
            print(f"Error checking rate limit: {e}")
            return True

    def check_opposite_signal(self, bot_type: str, symbol: str) -> bool:
        """Проверяет наличие активной позиции от противоположного бота (long↔short)."""
        try:
            other = "long" if bot_type == "short" else "short"
            key = f"{other}:positions:{symbol}"
            return bool(self.client.exists(key))
        except Exception as e:
            print(f"Error checking opposite signal: {e}")
            return False

    def get_shared_market_data(self, symbol: str) -> Optional[Dict]:
        """Получение общих рыночных данных — кэш между ботами (TTL 5 мин)."""
        try:
            key = f"shared:market:{symbol}"
            data = self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            print(f"Error getting shared market data: {e}")
            return None

    def set_shared_market_data(self, symbol: str, data: Dict, ttl: int = 300) -> bool:
        """Сохранение общих рыночных данных — кэш между ботами (TTL 5 мин)."""
        try:
            key = f"shared:market:{symbol}"
            self.client.setex(key, ttl, json.dumps(data))
            return True
        except Exception as e:
            print(f"Error setting shared market data: {e}")
            return False

    def get_signal_log(self, bot_type: str, limit: int = 100) -> List[Dict]:
        """Получение лога всех сигналов (выполненных + пропущенных)."""
        try:
            key = f"{bot_type}:signal_log"
            items = self.client.lrange(key, 0, limit - 1)
            return [json.loads(i) for i in items]
        except Exception as e:
            print(f"Error getting signal log: {e}")
            return []

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
            self._index_remove(bot_type, symbol)      # ✅ OPT
            self._index_invalidate_cache(bot_type)  # ✅ OPT
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
