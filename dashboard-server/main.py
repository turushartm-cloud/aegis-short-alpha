"""
Aegis Dashboard Server v3.3
✅ 30-minute caching — reduces Redis load
✅ Health-check before every request — detects disconnections
✅ Auto-reconnect with exponential backoff
✅ Connection pool for better performance

Fixes based on actual Redis debug data:
  - position keys: both SYMBOL and SYM-BOL (BingX dash format)
  - today_stats null → calculate from all_trades filtered by today
  - mode from state["status"] field
  - virtual signals from state["active_signals"] or signals without order_id
"""
import os
import json
import time
import threading
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

try:
    import redis as redis_lib
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# ── Config ────────────────────────────────────────────────────────────────
LONG_REDIS_URL = os.getenv("LONG_REDIS_URL", "")
SHORT_REDIS_URL = os.getenv("SHORT_REDIS_URL", "")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL", "1800"))  # 30 min default
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_DELAY_BASE = 2  # seconds

# ── Connection Pool with Health Check & Auto-Reconnect ─────────────────────
class RedisConnectionPool:
    """
    Connection pool with:
    - Lazy connection (connect on first use)
    - Health check before every operation
    - Auto-reconnect with exponential backoff
    - Thread-safe operations
    """

    def __init__(self, url: str, name: str):
        self.url = url
        self.name = name
        self._conn: Optional[object] = None
        self._lock = threading.Lock()
        self._last_error: Optional[str] = None
        self._reconnect_attempts = 0
        self._last_successful_ping = 0.0

    def _create_connection(self) -> Optional[object]:
        """Create new Redis connection with connection pool settings."""
        if not self.url or not REDIS_AVAILABLE:
            return None
        try:
            # Use connection pool for better performance
            conn = redis_lib.from_url(
                self.url,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                max_connections=10,  # Pool size
                health_check_interval=30,  # Auto health-check every 30s
            )
            conn.ping()
            self._reconnect_attempts = 0
            self._last_successful_ping = time.time()
            print(f"✅ [{self.name}] Redis connected")
            return conn
        except Exception as e:
            self._last_error = str(e)
            print(f"❌ [{self.name}] Redis connect error: {e}")
            return None

    def _should_reconnect(self) -> bool:
        """Check if we should attempt reconnect (exponential backoff)."""
        if self._reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
            return False
        # Exponential backoff: 2s, 4s, 8s, 16s, 32s
        delay = RECONNECT_DELAY_BASE * (2 ** self._reconnect_attempts)
        time_since_error = time.time() - (self._last_successful_ping or 0)
        return time_since_error > delay

    def get_connection(self) -> Optional[object]:
        """Get healthy connection (auto-reconnect if needed)."""
        with self._lock:
            # Test existing connection
            if self._conn is not None:
                try:
                    self._conn.ping()
                    self._last_successful_ping = time.time()
                    return self._conn
                except Exception as e:
                    print(f"⚠️ [{self.name}] Redis ping failed: {e}")
                    self._conn = None

            # Try to reconnect if eligible
            if self._conn is None:
                if self._reconnect_attempts == 0 or self._should_reconnect():
                    self._reconnect_attempts += 1
                    self._conn = self._create_connection()

            return self._conn

    def health_check(self) -> Tuple[bool, str]:
        """Explicit health check for monitoring."""
        conn = self.get_connection()
        if conn is None:
            return False, self._last_error or "Not connected"
        try:
            conn.ping()
            info = conn.info("server") if hasattr(conn, "info") else {}
            version = info.get("redis_version", "unknown")
            return True, f"OK (Redis v{version})"
        except Exception as e:
            return False, str(e)


# Global connection pools
_pools: Dict[str, RedisConnectionPool] = {
    "long": RedisConnectionPool(LONG_REDIS_URL, "LONG"),
    "short": RedisConnectionPool(SHORT_REDIS_URL, "SHORT"),
}


def rc(bot: str) -> Optional[object]:
    """Get healthy Redis connection (with auto-reconnect)."""
    pool = _pools.get(bot)
    if not pool:
        return None
    return pool.get_connection()


def health_check(bot: str) -> Tuple[bool, str]:
    """Check Redis health for specific bot."""
    pool = _pools.get(bot)
    if not pool:
        return False, "Pool not found"
    return pool.health_check()


# ── Caching Layer ───────────────────────────────────────────────────────────
class DataCache:
    """Simple in-memory cache with TTL."""

    def __init__(self, ttl_seconds: int = CACHE_TTL_SECONDS):
        self.ttl = ttl_seconds
        self._cache: Dict[str, Tuple[any, float]] = {}  # key -> (value, timestamp)
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[any]:
        """Get cached value if not expired."""
        with self._lock:
            if key not in self._cache:
                return None
            value, timestamp = self._cache[key]
            if time.time() - timestamp > self.ttl:
                # Expired
                del self._cache[key]
                return None
            return value

    def set(self, key: str, value: any):
        """Set cache value."""
        with self._lock:
            self._cache[key] = (value, time.time())

    def invalidate(self, key: str = None):
        """Invalidate cache (specific key or all)."""
        with self._lock:
            if key:
                self._cache.pop(key, None)
            else:
                self._cache.clear()

    def stats(self) -> Dict:
        """Cache statistics."""
        with self._lock:
            now = time.time()
            valid = sum(1 for _, ts in self._cache.values() if now - ts <= self.ttl)
            expired = len(self._cache) - valid
            return {
                "total_keys": len(self._cache),
                "valid": valid,
                "expired": expired,
                "ttl_seconds": self.ttl,
            }


# Global cache instance
cache = DataCache()


def jl(s):
    try:
        return json.loads(s) if s else None
    except:
        return None


def _normalize_symbol(key: str) -> str:
    """AIXBT-USDT → AIXBTUSDT"""
    return key.replace("-", "")

# ── Positions ──────────────────────────────────────────────────────────────
# ✅ OPT v19: Dashboard in-memory cache — 1 request per 15s max
_dash_cache: dict = {}
_dash_cache_ts: float = 0.0
_DASH_CACHE_TTL = 15  # seconds

def _get_positions(c, bot: str) -> List[Dict]:
    """Read all active positions, handles both SYMBOL and SYM-BOL key formats."""
    if not c: return []
    try:
        keys = c.keys(f"{bot}:positions:*")
        seen = set()
        out  = []
        for k in keys:
            raw = c.get(k)
            if not raw: continue
            d = jl(raw)
            if not d: continue
            # Skip closed positions
            if d.get("status") == "closed": continue
            # Extract symbol from key
            sym_raw = k[len(f"{bot}:positions:"):]
            sym     = _normalize_symbol(sym_raw)
            if sym in seen: continue  # dedup (AIXBT-USDT and AIXBTUSDT same)
            seen.add(sym)
            d["bot"]    = bot
            d["symbol"] = sym
            d["is_real"] = True
            out.append(d)
        return out
    except Exception as e:
        print(f"_get_positions({bot}): {e}")
        return []

# ── Active signals (virtual = no order_id) ────────────────────────────────
def _get_signals(c, bot: str) -> List[Dict]:
    if not c: return []
    try:
        # state["active_signals"] — это INT (count), не список сигналов.
        # Всегда используем scan по ключам для получения реальных данных.
        state_raw = c.get(f"{bot}:state")
        _ = jl(state_raw) or {}  # читаем для кэша, но active_signals там int

        # Scan signals:* keys
        keys = c.keys(f"{bot}:signals:*")
        out  = []
        for k in keys[:300]:
            try:
                raw = c.lrange(k, 0, 0)
            except Exception:
                raw = []
            if not raw: continue
            d = jl(raw[0])
            if not d: continue
            if d.get("status") != "active": continue
            d["bot"] = bot
            if "symbol" not in d:
                d["symbol"] = _normalize_symbol(k.split(":")[-1])
            out.append(d)
        return out
    except Exception as e:
        print(f"_get_signals({bot}): {e}")
        return []

# ── Virtual Positions (HASH: {bot}:virtual_positions) ────────────────────
def _get_virtual_positions_count(c, bot: str) -> int:
    """✅ FIX: Читаем реальный HASH virtual_positions, не signals без order_id.
    Возвращает количество активных виртуальных позиций (outcome=None)."""
    if not c: return 0
    try:
        data = c.hgetall(f"{bot}:virtual_positions")
        if not data: return 0
        count = 0
        for val in data.values():
            try:
                pos = json.loads(val)
                if pos.get("outcome") is None:  # только открытые
                    count += 1
            except Exception:
                continue
        return count
    except Exception as e:
        print(f"_get_virtual_positions_count({bot}): {e}")
        return 0

# ── History ────────────────────────────────────────────────────────────────
def _get_history(c, bot: str, limit: int = 9999) -> List[Dict]:
    if not c: return []
    try:
        items = c.lrange(f"{bot}:all_trades", 0, limit - 1)
        out   = []
        for raw in items:
            d = jl(raw)
            if d:
                d["bot"] = bot
                out.append(d)
        return out
    except Exception as e:
        print(f"_get_history({bot}): {e}")
        return []

# ── State ──────────────────────────────────────────────────────────────────
def _get_state(c, bot: str) -> Dict:
    if not c: return {}
    try:
        raw = c.get(f"{bot}:state")
        return jl(raw) or {}
    except: return {}

def _parse_mode(state: Dict) -> str:
    """Extract mode from state. Actual key is 'status'."""
    # state_keys: ["status","last_scan","daily_signals","active_signals","version"]
    status = state.get("status", "")
    if isinstance(status, str):
        s = status.upper()
        if "AUTO" in s:   return "AUTO"
        if "MANUAL" in s: return "MANUAL"
        if "DEMO" in s:   return "DEMO"
        if "REAL" in s:   return "REAL"
        if status:        return status
    if state.get("auto_trading_enabled"): return "AUTO"
    return "ACTIVE"

# ── Daily stats — calculated from history since today_stats key is null ───
def _calc_daily_stats(history: List[Dict]) -> Dict:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    today_trades = [
        h for h in history
        if (h.get("closed_at") or h.get("close_time") or h.get("timestamp") or "")[:10] == today
    ]
    wins   = sum(1 for h in today_trades if (h.get("pnl") or 0) > 0)
    losses = sum(1 for h in today_trades if (h.get("pnl") or 0) <= 0)
    sl     = sum(1 for h in today_trades
                 if h.get("close_type") == "sl" or h.get("tp_level") == "SL")
    pnl    = round(sum(h.get("pnl", 0) for h in today_trades), 4)
    return {"trades": len(today_trades), "wins": wins, "losses": losses,
            "sl": sl, "pnl": pnl}

# ── Build performance ──────────────────────────────────────────────────────
def _build_perf(bot: str) -> Dict:
    c       = rc(bot)
    history = _get_history(c, bot)
    wins    = sum(1 for h in history if (h.get("pnl") or 0) > 0)
    losses  = sum(1 for h in history if (h.get("pnl") or 0) <= 0)
    total_pnl = round(sum(h.get("pnl", 0) for h in history), 4)

    ds    = _calc_daily_stats(history)
    state = _get_state(c, bot)

    sigs     = _get_signals(c, bot)
    real_pos = _get_positions(c, bot)
    real_sigs = [s for s in sigs if s.get("order_id")]
    virt_sigs = [s for s in sigs if not s.get("order_id")]

    # Dedup real: positions table is more accurate
    pos_syms = {p["symbol"] for p in real_pos}
    real_sigs_deduped = [s for s in real_sigs
                         if _normalize_symbol(s.get("symbol","")) not in pos_syms]
    total_real = len(real_pos) + len(real_sigs_deduped)

    # ✅ FIX: Используем тот же источник что и вкладка Virtual Signals
    # virt_sigs = signals без order_id (не HASH который пустой)
    virtual_count = len(virt_sigs)

    # Also check state["daily_signals"] for today's signal count
    daily_sigs = state.get("daily_signals", {})
    today = datetime.utcnow().strftime("%Y-%m-%d")
    today_sig_count = daily_sigs.get(today, 0) if isinstance(daily_sigs, dict) else 0

    return {
        "bot_type":      bot,
        "wins":          wins,
        "losses":        losses,
        "total_pnl":     total_pnl,
        "daily_pnl":     ds["pnl"],
        "sl_today":      ds["sl"],
        "wins_today":    ds["wins"],
        "losses_today":  ds["losses"],
        "trades_today":  ds["trades"],
        "open_exchange": total_real,
        "open_virtual":  virtual_count,  # ✅ FIX: из {bot}:virtual_positions HASH
        "total_trades":  len(history),
        "mode":          _parse_mode(state),
        "min_score":     state.get("min_score", 60),
        "last_scan":     state.get("last_scan"),
        "connected":     c is not None,
        "signals_today": today_sig_count,
        "version":       state.get("version", "?"),
    }

# ── Cached Data Fetchers ───────────────────────────────────────────────────
def _get_cached_overview():
    """Get overview with caching."""
    cache_key = "overview"
    cached = cache.get(cache_key)
    if cached:
        return cached

    lp = _build_perf("long")
    sp = _build_perf("short")
    result = {
        "long":  lp, "short": sp,
        "cached_at": datetime.utcnow().isoformat(),
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "combined": {
            "total_pnl":    round(lp["total_pnl"] + sp["total_pnl"], 4),
            "daily_pnl":    round(lp["daily_pnl"]  + sp["daily_pnl"], 4),
            "wins":         lp["wins"]  + sp["wins"],
            "losses":       lp["losses"]+ sp["losses"],
            "sl_today":     lp["sl_today"]   + sp["sl_today"],
            "wins_today":   lp["wins_today"] + sp["wins_today"],
            "losses_today": lp["losses_today"]+ sp["losses_today"],
            "trades_today": lp["trades_today"]+ sp["trades_today"],
            "open_exchange":lp["open_exchange"]+ sp["open_exchange"],
            "open_virtual": lp["open_virtual"] + sp["open_virtual"],
            "total_trades": lp["total_trades"] + sp["total_trades"],
        }
    }
    cache.set(cache_key, result)
    return result


def _get_cached_positions():
    """Get positions with caching."""
    cache_key = "positions"
    cached = cache.get(cache_key)
    if cached:
        return cached

    lc, sc = rc("long"), rc("short")
    lreal  = _get_positions(lc, "long")
    sreal  = _get_positions(sc, "short")
    lsigs  = _get_signals(lc, "long")
    ssigs  = _get_signals(sc, "short")

    lpos_syms = {p["symbol"] for p in lreal}
    spos_syms = {p["symbol"] for p in sreal}

    lreal_extra = [s for s in lsigs if s.get("order_id")
                   and _normalize_symbol(s.get("symbol","")) not in lpos_syms]
    sreal_extra = [s for s in ssigs if s.get("order_id")
                   and _normalize_symbol(s.get("symbol","")) not in spos_syms]

    real = lreal + lreal_extra + sreal + sreal_extra
    virt = ([s for s in lsigs if not s.get("order_id")] +
            [s for s in ssigs if not s.get("order_id")])

    result = {
        "real": sorted(real, key=lambda x: x.get("timestamp",""), reverse=True),
        "virtual": sorted(virt, key=lambda x: x.get("timestamp",""), reverse=True),
        "cached_at": datetime.utcnow().isoformat(),
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
    }
    cache.set(cache_key, result)
    return result


def _get_cached_history(limit: int = 40):
    """Get history with caching."""
    cache_key = f"history:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    lc, sc = rc("long"), rc("short")
    lh = _get_history(lc, "long",  max(limit, 200))
    sh = _get_history(sc, "short", max(limit, 200))
    combined = sorted(lh + sh,
        key=lambda x: x.get("closed_at") or x.get("close_time") or x.get("timestamp") or "",
        reverse=True)

    result = {
        "trades": combined[:limit],
        "total": len(combined),
        "cached_at": datetime.utcnow().isoformat(),
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
    }
    cache.set(cache_key, result)
    return result


# ── FastAPI ────────────────────────────────────────────────────────────────
app = FastAPI(title="Aegis Dashboard API", version="3.3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
@app.head("/health")
async def health():
    """Enhanced health check with Redis status and cache stats.
    ✅ HEAD поддерживается для UptimeRobot и других мониторингов.
    """
    long_ok, long_msg = health_check("long")
    short_ok, short_msg = health_check("short")

    # Force reconnect check
    lc = rc("long")
    sc = rc("short")

    return {
        "status": "ok" if (long_ok or short_ok) else "degraded",
        "version": "3.3.0",
        "time": datetime.utcnow().isoformat(),
        "redis": {
            "long": {"connected": long_ok, "message": long_msg},
            "short": {"connected": short_ok, "message": short_msg},
        },
        "cache": cache.stats(),
    }

@app.get("/api/overview")
@app.head("/api/overview")
async def overview():
    """Overview with 30-minute caching.
    ✅ HEAD поддерживается для UptimeRobot.
    """
    return _get_cached_overview()

@app.get("/api/positions")
async def positions():
    """Positions with 30-minute caching."""
    return _get_cached_positions()

@app.get("/api/history")
async def history(limit: int = 40):
    """History with 30-minute caching."""
    return _get_cached_history(limit)

@app.api_route("/api/cache/invalidate", methods=["GET", "POST"])
async def invalidate_cache():
    """Manually invalidate cache (for force refresh)."""
    cache.invalidate()
    return {"status": "ok", "message": "Cache invalidated", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/cache/stats")
async def cache_stats():
    """Cache statistics."""
    return cache.stats()

@app.get("/api/debug/{bot}")
async def debug(bot: str):
    c = rc(bot)
    if not c:
        return {"error": "Redis not connected"}
    try:
        sig_keys  = c.keys(f"{bot}:signals:*")[:10]
        pos_keys  = c.keys(f"{bot}:positions:*")[:10]
        all_count = c.llen(f"{bot}:all_trades")
        state_raw = c.get(f"{bot}:state")
        state     = jl(state_raw) or {}
        today     = datetime.utcnow().strftime("%Y-%m-%d")
        stats_raw = c.get(f"{bot}:stats:daily:{today}")
        # Sample first history entry
        hist_sample = c.lrange(f"{bot}:all_trades", 0, 0)
        hist_first  = jl(hist_sample[0]) if hist_sample else None
        return {
            "signal_keys":      sig_keys,
            "position_keys":    pos_keys,
            "all_trades_count": all_count,
            "state_keys":       list(state.keys()),
            "state_status":     state.get("status"),
            "today_stats_key":  stats_raw,
            "history_first_keys": list(hist_first.keys())[:10] if hist_first else [],
            "history_first_date": (hist_first or {}).get("closed_at") or
                                   (hist_first or {}).get("close_time") or
                                   (hist_first or {}).get("timestamp"),
            "history_first_pnl":  (hist_first or {}).get("pnl"),
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(html_path):
        with open(html_path) as f:
            return f.read()
    return "<h1>index.html not found</h1>"

@app.head("/")
async def serve_dashboard_head():
    """✅ HEAD для UptimeRobot — возвращает 200 без тела."""
    from fastapi.responses import Response
    return Response(status_code=200)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0",
                port=int(os.getenv("PORT", "10000")), reload=False)
