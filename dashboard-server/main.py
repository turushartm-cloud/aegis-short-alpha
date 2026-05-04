"""
Aegis Dashboard Server v3.2
Fixes based on actual Redis debug data:
  - position keys: both SYMBOL and SYM-BOL (BingX dash format)
  - today_stats null → calculate from all_trades filtered by today
  - mode from state["status"] field
  - virtual signals from state["active_signals"] or signals without order_id
"""
import os, json
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
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
    try:   return json.loads(s) if s else None
    except: return None

def _normalize_symbol(key: str) -> str:
    """AIXBT-USDT → AIXBTUSDT"""
    return key.replace("-", "")

# ── Positions ──────────────────────────────────────────────────────────────
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
        # First try state["active_signals"] — fast path
        state_raw = c.get(f"{bot}:state")
        state     = jl(state_raw) or {}
        active_from_state = state.get("active_signals", [])
        if active_from_state and isinstance(active_from_state, list):
            for s in active_from_state:
                s["bot"] = bot
            return active_from_state

        # Fallback: scan signals:* keys
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
        "open_virtual":  len(virt_sigs),
        "total_trades":  len(history),
        "mode":          _parse_mode(state),
        "min_score":     state.get("min_score", 60),
        "last_scan":     state.get("last_scan"),
        "connected":     c is not None,
        "signals_today": today_sig_count,
        "version":       state.get("version", "?"),
    }

# ── FastAPI ────────────────────────────────────────────────────────────────
app = FastAPI(title="Aegis Dashboard API", version="3.2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health():
    lc, sc = rc("long"), rc("short")
    return {"status": "ok", "long_redis": lc is not None,
            "short_redis": sc is not None,
            "time": datetime.utcnow().isoformat(), "version": "3.2.0"}

@app.get("/api/overview")
async def overview():
    lp = _build_perf("long")
    sp = _build_perf("short")
    return {
        "long":  lp, "short": sp,
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

@app.get("/api/positions")
async def positions():
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

    return {
        "real": sorted(real, key=lambda x: x.get("timestamp",""), reverse=True),
        "virtual": sorted(virt, key=lambda x: x.get("timestamp",""), reverse=True),
    }

@app.get("/api/history")
async def history(limit: int = 40):
    lc, sc = rc("long"), rc("short")
    lh = _get_history(lc, "long",  max(limit, 200))
    sh = _get_history(sc, "short", max(limit, 200))
    combined = sorted(lh + sh,
        key=lambda x: x.get("closed_at") or x.get("close_time") or x.get("timestamp") or "",
        reverse=True)
    return {"trades": combined[:limit], "total": len(combined)}

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

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0",
                port=int(os.getenv("PORT", "10000")), reload=False)
