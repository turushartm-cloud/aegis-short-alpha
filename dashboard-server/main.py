"""
Aegis Dashboard Server v3.1 — FIXED Redis key patterns
Fixes:
  - signals: {bot}:signals:{symbol} (NOT signal:)
  - positions: {bot}:positions:{symbol} (NOT position:)
  - SL today via {bot}:stats:daily:{date} (accurate)
  - mode from {bot}:state correctly
  - real vs virtual: positions have order_id
"""
import os, json
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

try:
    import redis as redis_lib
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

LONG_REDIS_URL  = os.getenv("LONG_REDIS_URL",  "")
SHORT_REDIS_URL = os.getenv("SHORT_REDIS_URL", "")

_rc: Dict[str, Optional[object]] = {"long": None, "short": None}

def _conn(url: str):
    if not url or not REDIS_AVAILABLE:
        return None
    try:
        c = redis_lib.from_url(url, decode_responses=True,
                               socket_timeout=5, socket_connect_timeout=5)
        c.ping()
        return c
    except Exception as e:
        print(f"Redis connect error: {e}")
        return None

def rc(bot: str):
    if _rc[bot] is None:
        url = LONG_REDIS_URL if bot == "long" else SHORT_REDIS_URL
        _rc[bot] = _conn(url)
    return _rc[bot]

def jl(s):
    try:   return json.loads(s)
    except: return None

# ── Key patterns (matching actual redis_client.py) ────────────────────────
# signals:  {bot}:signals:{SYMBOL}   → lpush list, [0] = latest
# positions:{bot}:positions:{SYMBOL} → setex string (active real positions)
# state:    {bot}:state              → setex string
# stats:    {bot}:stats:daily:{date} → setex string
# history:  {bot}:all_trades         → lpush list (all closed trades)

def _get_signals(c, bot: str) -> List[Dict]:
    """Active signals: both real (with order_id) and virtual (without)."""
    if not c: return []
    try:
        keys = c.keys(f"{bot}:signals:*")
        out = []
        for k in keys[:200]:
            raw = c.lrange(k, 0, 0)  # latest only
            if not raw: continue
            d = jl(raw[0])
            if not d: continue
            if d.get("status") != "active": continue
            d["bot"] = bot
            if "symbol" not in d:
                d["symbol"] = k.split(":")[-1]
            out.append(d)
        return out
    except Exception as e:
        print(f"_get_signals({bot}): {e}")
        return []

def _get_positions(c, bot: str) -> List[Dict]:
    """Real positions saved by auto_trader (have order_id)."""
    if not c: return []
    try:
        keys = c.keys(f"{bot}:positions:*")
        out = []
        for k in keys[:100]:
            raw = c.get(k)
            if not raw: continue
            d = jl(raw)
            if not d: continue
            if d.get("status") not in ("active", None, ""): continue
            d["bot"] = bot
            if "symbol" not in d:
                d["symbol"] = k.split(":")[-1]
            out.append(d)
        return out
    except Exception as e:
        print(f"_get_positions({bot}): {e}")
        return []

def _get_history(c, bot: str, limit: int = 40) -> List[Dict]:
    if not c: return []
    try:
        items = c.lrange(f"{bot}:all_trades", 0, limit - 1)
        out = []
        for raw in items:
            d = jl(raw)
            if d:
                d["bot"] = bot
                out.append(d)
        return out
    except Exception as e:
        print(f"_get_history({bot}): {e}")
        return []

def _get_state(c, bot: str) -> Dict:
    if not c: return {}
    try:
        raw = c.get(f"{bot}:state")
        return jl(raw) or {}
    except: return {}

def _get_daily_stats(c, bot: str) -> Dict:
    """Accurate today stats from {bot}:stats:daily:{date}."""
    if not c: return {"trades":0,"wins":0,"losses":0,"pnl":0.0,"sl":0}
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        raw = c.get(f"{bot}:stats:daily:{today}")
        d = jl(raw) or {}
        # Also check bot_state.daily_trades[today]
        if not d:
            state = _get_state(c, bot)
            dt = state.get("daily_trades", {})
            d = dt.get(today, {})
        return {
            "trades":  d.get("trades",  0),
            "wins":    d.get("wins",    0),
            "losses":  d.get("losses",  0),
            "pnl":     round(d.get("pnl", 0.0), 4),
            "sl":      d.get("sl", d.get("losses", 0)),  # sl = losses if no separate field
        }
    except Exception as e:
        print(f"_get_daily_stats({bot}): {e}")
        return {"trades":0,"wins":0,"losses":0,"pnl":0.0,"sl":0}

def _build_perf(bot: str) -> Dict:
    c = rc(bot)
    history = _get_history(c, bot, limit=9999)
    wins   = sum(1 for h in history if (h.get("pnl") or 0) > 0)
    losses = sum(1 for h in history if (h.get("pnl") or 0) <= 0)
    total_pnl = round(sum(h.get("pnl", 0) for h in history), 4)

    # Use accurate daily stats from Redis stats key
    ds = _get_daily_stats(c, bot)

    state = _get_state(c, bot)

    sigs  = _get_signals(c, bot)
    real_pos = _get_positions(c, bot)

    # Virtual = active signals WITHOUT order_id
    virt = [s for s in sigs if not s.get("order_id")]

    # Real open = positions table + signals with order_id
    real_from_sigs = [s for s in sigs if s.get("order_id")]
    total_real_open = len(real_pos) + len(real_from_sigs)

    mode = state.get("mode") or state.get("auto_trading_mode") or \
           ("AUTO" if state.get("auto_trading_enabled") else "MANUAL") or "?"

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
        "open_exchange": total_real_open,
        "open_virtual":  len(virt),
        "total_trades":  len(history),
        "mode":          mode,
        "min_score":     state.get("min_score") or state.get("aegis_min_score", 60),
        "last_scan":     state.get("last_scan"),
        "connected":     c is not None,
    }

# ── App ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Aegis Dashboard API", version="3.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health():
    lc, sc = rc("long"), rc("short")
    return {
        "status": "ok",
        "long_redis":  lc is not None,
        "short_redis": sc is not None,
        "time": datetime.utcnow().isoformat(),
        "version": "3.1.0",
    }

@app.get("/api/overview")
async def overview():
    lp = _build_perf("long")
    sp = _build_perf("short")
    tw = lp["wins"] + sp["wins"]
    tl = lp["losses"] + sp["losses"]
    return {
        "long":  lp,
        "short": sp,
        "combined": {
            "total_pnl":     round(lp["total_pnl"] + sp["total_pnl"], 4),
            "daily_pnl":     round(lp["daily_pnl"]  + sp["daily_pnl"],  4),
            "wins":          tw,
            "losses":        tl,
            "sl_today":      lp["sl_today"]    + sp["sl_today"],
            "wins_today":    lp["wins_today"]  + sp["wins_today"],
            "losses_today":  lp["losses_today"]+ sp["losses_today"],
            "trades_today":  lp["trades_today"]+ sp["trades_today"],
            "open_exchange": lp["open_exchange"]+ sp["open_exchange"],
            "open_virtual":  lp["open_virtual"] + sp["open_virtual"],
            "total_trades":  lp["total_trades"]+ sp["total_trades"],
        }
    }

@app.get("/api/positions")
async def positions():
    lc, sc = rc("long"), rc("short")

    # Real positions from positions table
    lreal = _get_positions(lc, "long")
    sreal = _get_positions(sc, "short")

    # Real from signals (have order_id)
    lsigs = _get_signals(lc, "long")
    ssigs = _get_signals(sc, "short")
    lreal_sigs = [s for s in lsigs if s.get("order_id")]
    sreal_sigs = [s for s in ssigs if s.get("order_id")]

    real = lreal + lreal_sigs + sreal + sreal_sigs

    # Virtual = active signals without order_id
    virt = ([s for s in lsigs if not s.get("order_id")] +
            [s for s in ssigs if not s.get("order_id")])

    return {
        "real":    sorted(real, key=lambda x: x.get("timestamp",""), reverse=True),
        "virtual": sorted(virt, key=lambda x: x.get("timestamp",""), reverse=True),
    }

@app.get("/api/history")
async def history(limit: int = 40):
    lc, sc = rc("long"), rc("short")
    lh = _get_history(lc, "long",  max(limit, 100))
    sh = _get_history(sc, "short", max(limit, 100))
    combined = sorted(lh + sh,
        key=lambda x: x.get("closed_at") or x.get("close_time") or x.get("timestamp") or "",
        reverse=True)
    return {"trades": combined[:limit], "total": len(combined)}

@app.get("/api/debug/{bot}")
async def debug(bot: str):
    """Debug endpoint - shows raw Redis keys."""
    c = rc(bot)
    if not c:
        return {"error": "Redis not connected"}
    try:
        sig_keys  = c.keys(f"{bot}:signals:*")[:10]
        pos_keys  = c.keys(f"{bot}:positions:*")[:10]
        all_count = c.llen(f"{bot}:all_trades")
        state_raw = c.get(f"{bot}:state")
        today = datetime.utcnow().strftime("%Y-%m-%d")
        stats_raw = c.get(f"{bot}:stats:daily:{today}")
        return {
            "signal_keys":   sig_keys,
            "position_keys": pos_keys,
            "all_trades_count": all_count,
            "state_keys": list(jl(state_raw).keys()) if state_raw else [],
            "today_stats": jl(stats_raw),
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

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0",
                port=int(os.getenv("PORT", "10000")), reload=False)
