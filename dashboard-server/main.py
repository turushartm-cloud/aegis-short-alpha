"""
Aegis Dashboard Server v3.0
Standalone FastAPI service — reads directly from both Redis instances.
Deploy on Render free tier (separate service).
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ── Redis ──────────────────────────────────────────────────────────────────
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

LONG_REDIS_URL  = os.getenv("LONG_REDIS_URL",  "")
SHORT_REDIS_URL = os.getenv("SHORT_REDIS_URL", "")

_long_redis  = None
_short_redis = None

def _get_client(url: str):
    if not url or not REDIS_AVAILABLE:
        return None
    try:
        c = redis.from_url(url, decode_responses=True, socket_timeout=5,
                           socket_connect_timeout=5)
        c.ping()
        return c
    except Exception as e:
        print(f"Redis connect error: {e}")
        return None

def long_redis():
    global _long_redis
    if _long_redis is None:
        _long_redis = _get_client(LONG_REDIS_URL)
    return _long_redis

def short_redis():
    global _short_redis
    if _short_redis is None:
        _short_redis = _get_client(SHORT_REDIS_URL)
    return _short_redis


# ── App ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Aegis Dashboard API", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Redis helpers ──────────────────────────────────────────────────────────
def _safe_json(s: str) -> Optional[Dict]:
    try:
        return json.loads(s)
    except Exception:
        return None

def _get_active_signals(rc, bot_type: str) -> List[Dict]:
    if not rc:
        return []
    try:
        pattern = f"{bot_type}:signal:*"
        keys    = rc.keys(pattern)
        out     = []
        for k in keys[:100]:
            raw = rc.get(k)
            if raw:
                d = _safe_json(raw)
                if d and d.get("status") == "active":
                    d["bot"] = bot_type
                    out.append(d)
        return out
    except Exception as e:
        print(f"_get_active_signals: {e}")
        return []

def _get_history(rc, bot_type: str, limit: int = 40) -> List[Dict]:
    if not rc:
        return []
    try:
        items = rc.lrange(f"{bot_type}:all_trades", 0, limit - 1)
        out   = []
        for raw in items:
            d = _safe_json(raw)
            if d:
                d["bot"] = bot_type
                out.append(d)
        return out
    except Exception as e:
        print(f"_get_history: {e}")
        return []

def _get_bot_state(rc, bot_type: str) -> Dict:
    if not rc:
        return {}
    try:
        raw = rc.get(f"{bot_type}:state")
        return _safe_json(raw) or {}
    except Exception:
        return {}

def _get_daily_stats(rc, bot_type: str) -> Dict:
    if not rc:
        return {}
    try:
        state   = _get_bot_state(rc, bot_type)
        daily   = state.get("daily_trades", {})
        today   = datetime.utcnow().strftime("%Y-%m-%d")
        return daily.get(today, {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0})
    except Exception:
        return {}

def _build_perf(rc, bot_type: str) -> Dict:
    """Aggregate performance from Redis history."""
    history = _get_history(rc, bot_type, limit=9999)
    wins    = sum(1 for h in history if (h.get("pnl") or 0) > 0)
    losses  = sum(1 for h in history if (h.get("pnl") or 0) <= 0)
    total_pnl = round(sum(h.get("pnl", 0) for h in history), 4)

    today   = datetime.utcnow().strftime("%Y-%m-%d")
    history_today = [h for h in history if (h.get("closed_at") or "")[:10] == today]
    daily_pnl     = round(sum(h.get("pnl", 0) for h in history_today), 4)
    sl_today      = sum(1 for h in history_today if h.get("close_type") == "sl")
    wins_today    = sum(1 for h in history_today if (h.get("pnl") or 0) > 0)

    sigs      = _get_active_signals(rc, bot_type)
    open_real = [s for s in sigs if s.get("order_id")]
    open_virt = [s for s in sigs if not s.get("order_id")]

    state_data = _get_bot_state(rc, bot_type)

    return {
        "bot_type":      bot_type,
        "wins":          wins,
        "losses":        losses,
        "total_pnl":     total_pnl,
        "daily_pnl":     daily_pnl,
        "sl_today":      sl_today,
        "wins_today":    wins_today,
        "open_exchange": len(open_real),
        "open_virtual":  len(open_virt),
        "total_trades":  len(history),
        "mode":          state_data.get("mode", "?"),
        "min_score":     state_data.get("min_score", 60),
        "last_scan":     state_data.get("last_scan"),
        "version":       state_data.get("version", "?"),
    }


# ── Routes ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status":      "ok",
        "long_redis":  long_redis() is not None,
        "short_redis": short_redis() is not None,
        "time":        datetime.utcnow().isoformat(),
    }

@app.get("/api/overview")
async def overview():
    lr, sr = long_redis(), short_redis()
    lp     = _build_perf(lr, "long")
    sp     = _build_perf(sr, "short")
    return {
        "long":  lp,
        "short": sp,
        "combined": {
            "total_pnl":     round(lp["total_pnl"] + sp["total_pnl"], 4),
            "daily_pnl":     round(lp["daily_pnl"]  + sp["daily_pnl"],  4),
            "wins":          lp["wins"]   + sp["wins"],
            "losses":        lp["losses"] + sp["losses"],
            "sl_today":      lp["sl_today"]  + sp["sl_today"],
            "wins_today":    lp["wins_today"] + sp["wins_today"],
            "open_exchange": lp["open_exchange"] + sp["open_exchange"],
            "open_virtual":  lp["open_virtual"]  + sp["open_virtual"],
            "total_trades":  lp["total_trades"] + sp["total_trades"],
        }
    }

@app.get("/api/positions")
async def positions():
    lr, sr = long_redis(), short_redis()
    lsigs  = _get_active_signals(lr, "long")
    ssigs  = _get_active_signals(sr, "short")
    real   = [s for s in lsigs + ssigs if s.get("order_id")]
    virt   = [s for s in lsigs + ssigs if not s.get("order_id")]
    return {
        "real":    sorted(real, key=lambda x: x.get("timestamp",""), reverse=True),
        "virtual": sorted(virt, key=lambda x: x.get("timestamp",""), reverse=True),
    }

@app.get("/api/history")
async def history(limit: int = 40):
    lr, sr  = long_redis(), short_redis()
    lhist   = _get_history(lr, "long",  limit)
    shist   = _get_history(sr, "short", limit)
    combined = sorted(lhist + shist,
                      key=lambda x: x.get("closed_at", x.get("timestamp", "")),
                      reverse=True)
    return {"trades": combined[:limit], "total": len(combined)}

@app.get("/api/signals/{bot_type}")
async def signals_by_bot(bot_type: str):
    rc = long_redis() if bot_type == "long" else short_redis()
    return {"signals": _get_active_signals(rc, bot_type)}

@app.get("/api/stats/daily")
async def daily_stats():
    lr, sr = long_redis(), short_redis()
    return {
        "long":  _get_daily_stats(lr, "long"),
        "short": _get_daily_stats(sr, "short"),
    }

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(html_path):
        with open(html_path) as f:
            return f.read()
    return "<h1>Dashboard HTML not found</h1>"


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")),
                reload=False)
