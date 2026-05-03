"""
🟢 AEGIS LONG ALPHA v1.0 — Institutional Long Trading Bot
FastAPI Application

УЛУЧШЕНИЯ vs long-bot v2.3:
  ✅ DumpExhaustionDetector — Z-Score < -2.5σ (oversold climax)
  ✅ WyckoffAccumulationDetector — Spring/SOS/LPS
  ✅ BSLScanner — Buy Side Liquidity магниты выше
  ✅ OIAnalyzerLong — Negative Funding + Short Squeeze
  ✅ AegisLongSignalEngine — 5-компонентный скорер
  ✅ SmartDCALongEngine — DCA НИЖЕ входа (ATR-based)
  ✅ AegisRiskManager — Kelly + Circuit Breakers
  ✅ PerformanceTracker — daily P&L report
  ✅ BTC Positive Correlation Filter
  ✅ ETH/BTC Ratio signal
  ✅ Paid tier: 150 пар, 240s scan, 15 позиций
"""

import os
import asyncio
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn


# ============================================================================
# PATH SETUP
# ============================================================================

def _find_shared() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "shared"),
        os.path.join(here, "..", "shared"),
        os.path.join(here, "..", "..", "shared"),
        os.path.join(here, "..", "..", "..", "shared"),
        "/opt/render/project/src/shared",
    ]
    for c in candidates:
        c = os.path.normpath(c)
        if os.path.isdir(c):
            return c
    return os.path.join(here, "..", "..", "shared")


_SHARED = _find_shared()
_SRC    = os.path.dirname(os.path.abspath(__file__))
for _p in [_SHARED, os.path.dirname(_SHARED), _SRC]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

print(f"📁 shared: {_SHARED}")

# ── Shared modules ──
from upstash.redis_client import get_redis_client
from utils.binance_client import get_binance_client
from core.scorer import get_long_scorer
from core.pattern_detector import LongPatternDetector
from core.position_tracker import PositionTracker
from core.realtime_scorer import get_realtime_scorer
from core.consolidation_detector import ConsolidationDetector, filter_mid_range
from bot.telegram import TelegramBot, TelegramCommandHandler

# ── Aegis Long modules ──
from aegis.signal_engine_long import AegisLongSignalEngine, SignalStrengthLong
from aegis.smart_dca_long import SmartDCALongEngine, GridConfigLong, GridTypeLong
from aegis.risk_manager import AegisRiskManager, RiskLimits
from aegis.performance_tracker import PerformanceTracker, TradeRecord
from detectors.dump_detector import DumpExhaustionDetector, DumpDetectorConfig
from detectors.wyckoff_detector import WyckoffAccumulationDetector
from detectors.bsl_scanner import BSLScanner
from detectors.oi_analyzer_long import OIAnalyzerLong, FundingConfigLong


# ============================================================================
# CONFIGURATION — PAID MINIMAL TIER
# ============================================================================

class Config:
    BOT_NAME    = "Aegis-Long-Alpha"
    BOT_VERSION = "1.0.0"
    BOT_TYPE    = "long"

    # Paid tier
    MAX_PAIRS     = int(os.getenv("MAX_PAIRS", "150"))
    SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "240"))    # Long = медленнее
    MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "15"))

    MIN_SCORE     = int(os.getenv("MIN_LONG_SCORE", "58"))     # Чуть мягче Short (60)
    SL_BUFFER     = float(os.getenv("LONG_SL_BUFFER", "3.0")) # Long = больше SL
    LEVERAGE      = os.getenv("LONG_LEVERAGE", "5-20")

    # LONG TP: меньше фиксируем рано (ждём движения)
    TP_LEVELS  = [3.0, 5.0, 8.0, 12.0, 18.0, 25.0]
    TP_WEIGHTS = [15,  20,  20,  15,   15,   15]

    # Risk management
    RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE", "0.001"))
    MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.15"))
    MAX_EXPOSURE_PCT = float(os.getenv("MAX_EXPOSURE_PCT", "0.60"))
    DAILY_DD_LIMIT   = float(os.getenv("DAILY_DRAWDOWN_LIMIT", "5.0"))
    MAX_CONSEC_LOSS  = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "4"))
    KELLY_FRACTION   = float(os.getenv("KELLY_FRACTION", "0.25"))

    # Smart DCA
    DCA_LEVELS   = int(os.getenv("DCA_LEVELS", "4"))
    DCA_ATR_MULT = float(os.getenv("DCA_ATR_MULT", "1.5"))
    DCA_SIZE_MULT = float(os.getenv("DCA_SIZE_MULT", "1.5"))

    # Feature flags
    ENABLE_DUMP_DETECTOR   = os.getenv("ENABLE_DUMP_DETECTOR", "true").lower() == "true"
    ENABLE_WYCKOFF         = os.getenv("ENABLE_WYCKOFF", "true").lower() == "true"
    ENABLE_BSL_SCANNER     = os.getenv("ENABLE_BSL_SCANNER", "true").lower() == "true"
    ENABLE_OI_ANALYZER     = os.getenv("ENABLE_OI_ANALYZER", "true").lower() == "true"
    ENABLE_AEGIS_ENGINE    = os.getenv("ENABLE_AEGIS_ENGINE", "true").lower() == "true"
    ENABLE_SMART_DCA       = os.getenv("ENABLE_SMART_DCA", "true").lower() == "true"
    ENABLE_SMC             = os.getenv("USE_SMC", "true").lower() == "true"
    ENABLE_BTC_FILTER      = os.getenv("ENABLE_BTC_CORRELATION", "true").lower() == "true"

    # Auto trading
    AUTO_TRADING = os.getenv("AUTO_TRADING_ENABLED", "false").lower() == "true"
    BINGX_DEMO   = os.getenv("BINGX_DEMO_MODE", "true").lower() == "true"

    # Watchlist
    MIN_VOLUME_USDT = int(os.getenv("MIN_VOLUME_USDT", "300000"))
    MAX_WATCHLIST   = int(os.getenv("MAX_WATCHLIST", "150"))

    SIGNAL_TTL_HOURS  = 24
    TRAIL_ACTIVATION  = float(os.getenv("LONG_TRAIL_ACTIVATION", "0.015"))  # +1.5%

    # BTC correlation thresholds (Long = позитивная корреляция)
    BTC_BLOCK_THRESHOLD  = float(os.getenv("BTC_BLOCK_THRESHOLD", "-3.0"))  # Блок LONG при BTC -3%/h


# ============================================================================
# GLOBAL STATE
# ============================================================================

class BotState:
    def __init__(self):
        self.is_running       = False
        self.is_paused        = False
        self.last_scan        = None
        self.active_signals   = 0
        self.daily_signals    = 0
        self.watchlist: List[str] = []
        self.redis            = None
        self.binance          = None
        self.telegram         = None
        self.cmd_handler      = None
        self.auto_trader      = None
        self.tracker: Optional[PositionTracker] = None
        self.start_time       = None
        self._min_score       = Config.MIN_SCORE

        # Existing detectors
        self.scorer           = None
        self.pattern_detector = None
        self.consolidation_detector: Optional[ConsolidationDetector] = None  # 🆕

        # Aegis Long modules
        self.signal_engine:       Optional[AegisLongSignalEngine]  = None
        self.dca_engine:          Optional[SmartDCALongEngine]      = None
        self.risk_manager:        Optional[AegisRiskManager]        = None
        self.performance_tracker: Optional[PerformanceTracker]      = None
        self.dump_detector:       Optional[DumpExhaustionDetector]  = None
        self.wyckoff_detector:    Optional[WyckoffAccumulationDetector] = None
        self.bsl_scanner:         Optional[BSLScanner]              = None
        self.oi_analyzer:         Optional[OIAnalyzerLong]          = None


state = BotState()


# ============================================================================
# WATCHLIST
# ============================================================================

async def _build_combined_watchlist(binance_client, min_vol: float, max_count: int) -> List[str]:
    from utils.binance_client import FALLBACK_WATCHLIST
    bybit_syms, binance_syms = set(), set()

    try:
        await binance_client._init_source()
    except Exception as e:
        print(f"⚠️ _init_source: {e}")

    EXCLUDE = ("UP", "DOWN", "BULL", "BEAR", "3L", "3S")

    try:
        result = await binance_client._bybit("/v5/market/tickers", {"category": "linear"})
        if result and result.get("list"):
            for t in result.get("list", []):
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"): continue
                if any(sym.endswith(s) for s in EXCLUDE): continue
                if float(t.get("turnover24h", 0)) >= min_vol:
                    bybit_syms.add(sym)
        print(f"✅ Bybit: {len(bybit_syms)} symbols")
    except Exception as e:
        print(f"⚠️ Bybit: {e}")

    try:
        tickers = await binance_client._binance("/fapi/v1/ticker/24hr")
        if tickers:
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"): continue
                if any(sym.endswith(s) for s in EXCLUDE): continue
                if float(t.get("quoteVolume", 0)) >= min_vol:
                    binance_syms.add(sym)
        print(f"✅ Binance: {len(binance_syms)} symbols")
    except Exception as e:
        print(f"⚠️ Binance: {e}")

    if not bybit_syms and not binance_syms:
        return FALLBACK_WATCHLIST[:max_count]

    both    = list(bybit_syms & binance_syms)
    only_one = [s for s in (bybit_syms | binance_syms) if s not in set(both)]
    result  = (both + only_one)[:max_count]
    print(f"📊 Watchlist: {len(result)} symbols")
    return result


# ============================================================================
# LIFESPAN
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"🚀 Starting {Config.BOT_NAME} v{Config.BOT_VERSION}...")
    state.start_time = datetime.utcnow()

    state.redis   = get_redis_client()
    redis_ok      = state.redis.health_check()
    print(f"{'✅' if redis_ok else '❌'} Redis")

    state.binance = get_binance_client()
    await state.binance._init_source()

    state.scorer           = get_long_scorer(Config.MIN_SCORE)
    state.pattern_detector = LongPatternDetector()
    
    # 🆕 Consolidation Detector — блокировка входов в середине диапазона
    state.consolidation_detector = ConsolidationDetector(
        lookback=20, max_range_pct=5.0, min_candles=10
    )

    # ── Aegis Long Detectors ──
    print("🔧 Initializing Aegis Long detectors...")

    state.dump_detector = DumpExhaustionDetector(DumpDetectorConfig(
        threshold=2.5, volume_spike=2.0, rsi_oversold=28, lookback=20
    )) if Config.ENABLE_DUMP_DETECTOR else None

    state.wyckoff_detector = WyckoffAccumulationDetector(
        lookback=50
    ) if Config.ENABLE_WYCKOFF else None

    state.bsl_scanner = BSLScanner(
        lookback=50, equal_high_tolerance=0.003
    ) if Config.ENABLE_BSL_SCANNER else None

    state.oi_analyzer = OIAnalyzerLong(
        FundingConfigLong(
            lookback_hours=24,
            funding_threshold=-0.03,
            funding_spike=-0.08,
            funding_extreme=-0.15,
        ),
        binance_client=state.binance,
    ) if Config.ENABLE_OI_ANALYZER else None

    state.signal_engine = AegisLongSignalEngine(
        dump_detector=state.dump_detector,
        oi_analyzer=state.oi_analyzer,
        bsl_scanner=state.bsl_scanner,
        wyckoff_detector=state.wyckoff_detector,
        delta_analyzer=None,
        min_score=Config.MIN_SCORE,
    ) if Config.ENABLE_AEGIS_ENGINE else None

    state.dca_engine = SmartDCALongEngine(GridConfigLong(
        grid_type=GridTypeLong.ATR_BASED,
        dca_levels=Config.DCA_LEVELS,
        atr_multiplier=Config.DCA_ATR_MULT,
        size_multiplier=Config.DCA_SIZE_MULT,
        max_exposure_pct=Config.MAX_EXPOSURE_PCT,
        trail_activation_pct=1.5,
    )) if Config.ENABLE_SMART_DCA else None

    account_capital = float(os.getenv("ACCOUNT_CAPITAL_USD", "1000"))
    state.risk_manager = AegisRiskManager(
        limits=RiskLimits(
            max_position_pct=Config.MAX_POSITION_PCT,
            max_total_exposure=Config.MAX_EXPOSURE_PCT,
            max_daily_drawdown=Config.DAILY_DD_LIMIT,
            max_consecutive_loss=Config.MAX_CONSEC_LOSS,
            kelly_fraction=Config.KELLY_FRACTION,
        ),
        capital=account_capital,
    )
    state.performance_tracker = PerformanceTracker(redis_client=state.redis)

    print(f"✅ Aegis Long Engine: {'ON' if state.signal_engine else 'OFF'} | "
          f"Wyckoff: {'ON' if state.wyckoff_detector else 'OFF'} | "
          f"BSL: {'ON' if state.bsl_scanner else 'OFF'}")

    # ── Telegram ──
    state.telegram = TelegramBot(
        bot_token=os.getenv("LONG_TELEGRAM_BOT_TOKEN") or os.getenv("TG_BOT_TOKEN"),
        chat_id=os.getenv("LONG_TELEGRAM_CHAT_ID")    or os.getenv("TG_CHAT_ID"),
        topic_id=os.getenv("LONG_TELEGRAM_TOPIC_ID"),
    )
    telegram_ok = await state.telegram.send_test_message()
    print(f"{'✅' if telegram_ok else '❌'} Telegram")

    state.cmd_handler = TelegramCommandHandler(
        bot=state.telegram, redis_client=state.redis,
        bot_state=state, bot_type=Config.BOT_TYPE,
        scan_callback=scan_market, config=Config,
    )

    render_url = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
    if render_url:
        ok = await state.telegram.setup_webhook(f"{render_url}/webhook")
        print(f"{'✅' if ok else '⚠️'} Webhook")

    # ── BingX AutoTrader ──
    if Config.AUTO_TRADING:
        try:
            from api.bingx_client import BingXClient
            from execution.auto_trader import AutoTrader, TradeConfig
            bingx = BingXClient(
                api_key=os.getenv("BINGX_API_KEY"),
                api_secret=os.getenv("BINGX_API_SECRET"),
                demo=Config.BINGX_DEMO,
            )
            if await bingx.test_connection():
                trade_cfg = TradeConfig(
                    enabled=True, demo_mode=Config.BINGX_DEMO,
                    max_positions=Config.MAX_POSITIONS,
                    risk_per_trade=Config.RISK_PER_TRADE,
                    min_score_for_trade=Config.MIN_SCORE,
                )
                state.auto_trader = AutoTrader(
                    bingx_client=bingx, config=trade_cfg, telegram=state.telegram
                )
                print(f"✅ BingX {'DEMO' if Config.BINGX_DEMO else 'REAL'} AutoTrader")
            else:
                print("❌ BingX connection failed")
        except Exception as e:
            import traceback
            print(f"❌ AutoTrader init: {e}")
            print(f"📋 Full traceback:\n{traceback.format_exc()}")

    # ── Watchlist ──
    try:
        state.watchlist = await _build_combined_watchlist(
            state.binance, Config.MIN_VOLUME_USDT, Config.MAX_WATCHLIST
        )
    except Exception as e:
        print(f"⚠️ Watchlist failed: {e}")
        state.watchlist = []

    state.is_running = True
    state.last_scan  = datetime.utcnow()

    state.tracker = PositionTracker(
        bot_type=Config.BOT_TYPE, telegram=state.telegram,
        redis_client=state.redis, binance_client=state.binance,
        config=Config, auto_trader=state.auto_trader,
    )

    mode_str = "DEMO" if Config.BINGX_DEMO else "REAL"
    at_str   = f"✅ {mode_str}" if state.auto_trader else "❌ disabled"
    await state.telegram.send_message(
        f"🟢 <b>{Config.BOT_NAME} v{Config.BOT_VERSION} запущен</b>\n\n"
        f"📊 Watchlist: {len(state.watchlist)} монет\n"
        f"🛑 SL: {Config.SL_BUFFER}% | Score≥{Config.MIN_SCORE} | Scan: {Config.SCAN_INTERVAL}s\n"
        f"🤖 AutoTrader: {at_str}\n"
        f"💎 Aegis Engine: {'✅' if state.signal_engine else '❌'}\n"
        f"📐 Wyckoff: {'✅' if state.wyckoff_detector else '❌'} | BSL: {'✅' if state.bsl_scanner else '❌'}\n"
        f"🛡️ Risk: Kelly×{Config.KELLY_FRACTION} | DD≤{Config.DAILY_DD_LIMIT}% | CB: ✅\n"
        f"📈 TP: {Config.TP_LEVELS[:4]} | Trail: +{Config.TRAIL_ACTIVATION*100:.1f}%"
    )

    asyncio.create_task(background_scanner())
    asyncio.create_task(state.tracker.run())
    asyncio.create_task(_daily_report_task())

    yield

    state.is_running = False
    if state.binance: await state.binance.close()
    if state.auto_trader: await state.auto_trader.bingx.close()
    print("👋 Aegis Long stopped")


app = FastAPI(lifespan=lifespan, title=f"{Config.BOT_NAME} v{Config.BOT_VERSION}")


# ============================================================================
# SCAN LOGIC
# ============================================================================

def _is_fresh(existing: List[Dict]) -> bool:
    if not existing or existing[0].get("status") != "active":
        return False
    try:
        age_h = (datetime.utcnow() -
                 datetime.fromisoformat(existing[0].get("timestamp", ""))
                 ).total_seconds() / 3600
        return age_h < Config.SIGNAL_TTL_HOURS
    except Exception:
        return True


def _ohlcv(candles) -> List:
    return [[c.open, c.high, c.low, c.close, c.volume] for c in candles]


async def _count_long_positions() -> int:
    if state.auto_trader:
        try:
            pos = await state.auto_trader.bingx.get_positions()
            long_pos = [p for p in pos if (
                getattr(p, "position_side", "").upper() == "LONG" or
                getattr(p, "side", "").upper() == "LONG"
            )]
            return len(long_pos)
        except Exception as e:
            print(f"[LONG] count error: {e}")
    try:
        cutoff     = datetime.utcnow() - timedelta(hours=Config.SIGNAL_TTL_HOURS)
        all_active = state.redis.get_active_signals(Config.BOT_TYPE)
        return sum(1 for s in all_active
                   if datetime.fromisoformat(s.get("timestamp", "2000-01-01")) > cutoff)
    except Exception:
        return 0


async def _get_btc_change() -> Optional[float]:
    """BTC 1h изменение — ключевой фильтр для Long"""
    try:
        btc = await state.binance.get_complete_market_data("BTCUSDT")
        return getattr(btc, "price_change_1h", 0) if btc else None
    except Exception:
        return None


async def _get_eth_btc_ratio() -> float:
    """ETH/BTC ratio — индикатор альт-сезона"""
    try:
        eth = await state.binance.get_complete_market_data("ETHUSDT")
        btc = await state.binance.get_complete_market_data("BTCUSDT")
        if eth and btc and btc.price > 0:
            return eth.price / btc.price
        return 0.0
    except Exception:
        return 0.0


async def scan_symbol(symbol: str, cached_btc_1h: Optional[float] = None, verbose: bool = True) -> Optional[Dict]:
    """
    Aegis Long scan_symbol v1.0:
    - SL НИЖЕ входа (Long)
    - TP ВЫШЕ входа
    - DumpExhaustion + Wyckoff + BSL + OI Negative Funding + SMC Bullish
    - BTC positive correlation filter
    - VERBOSE LOGGING: показывает каждый этап скоринга
    """
    log_prefix = f"🟢 [{symbol}]"
    try:
        md = await state.binance.get_complete_market_data(symbol)
        if not md:
            if verbose:
                print(f"{log_prefix} ❌ Нет market data от Binance")
            return None

        # ── BTC фильтр для LONG (критичный) ─────────────────────────
        btc_1h = cached_btc_1h
        if btc_1h is not None and Config.ENABLE_BTC_FILTER:
            if btc_1h <= Config.BTC_BLOCK_THRESHOLD:
                # BTC падает > 3%/час — блокируем все Long
                if verbose:
                    print(f"{log_prefix} ❌ [BTC_FILTER] BTC {btc_1h:.1f}% <= {Config.BTC_BLOCK_THRESHOLD}% — блокировка LONG")
                return None
            if verbose:
                print(f"{log_prefix} 📊 [BTC_FILTER] BTC {btc_1h:+.1f}% — OK для LONG")

        # ── Загружаем OHLCV параллельно ──────────────────────────────
        ohlcv_15m, ohlcv_30m, ohlcv_4h = await asyncio.gather(
            state.binance.get_klines(symbol, "15m", 100),
            state.binance.get_klines(symbol, "30m", 50),
            state.binance.get_klines(symbol, "4h", 20),
            return_exceptions=True,
        )
        if isinstance(ohlcv_15m, Exception) or not ohlcv_15m or len(ohlcv_15m) < 20:
            if verbose:
                print(f"{log_prefix} ❌ Недостаточно OHLCV данных (нужно 20, есть {len(ohlcv_15m) if ohlcv_15m else 0})")
            return None
        if isinstance(ohlcv_30m, Exception): ohlcv_30m = []
        if isinstance(ohlcv_4h, Exception):  ohlcv_4h  = []

        # ── Базовый scorer (backward compat) ─────────────────────────
        hourly_deltas = await state.binance.get_hourly_volume_profile(symbol, 7)
        price_trend   = state.pattern_detector._get_price_trend(ohlcv_15m)
        patterns      = state.pattern_detector.detect_all(ohlcv_15m, hourly_deltas, md)

        p4d = 0.0
        try:
            klines = await state.binance.get_klines(symbol, "1d", 6)
            if klines and len(klines) >= 5:
                p4d = round((klines[-1].close - klines[-5].close) / klines[-5].close * 100, 2)
        except Exception:
            p4d = md.price_change_24h * 4

        base_result = state.scorer.calculate_score(
            rsi_1h=md.rsi_1h or 50,
            funding_current=md.funding_rate / 100,
            funding_accumulated=md.funding_accumulated / 100,
            long_ratio=md.long_short_ratio,
            oi_change_4d=md.oi_change_4d,
            price_change_4d=p4d,
            hourly_deltas=hourly_deltas,
            price_trend=price_trend,
            patterns=patterns,
            volume_spike_ratio=getattr(md, "volume_spike_ratio", 1.0),
            atr_14_pct=getattr(md, "atr_14_pct", 0.5),
        )
        if not base_result.is_valid:
            if verbose:
                print(f"{log_prefix} ❌ [BASE_SCORER] is_valid=False — базовый скоринг отклонил")
            return None

        price      = md.price
        base_score = base_result.total_score
        
        # 🆕 Консолидация фильтр — блокировка входов в середине диапазона
        if state.consolidation_detector and ohlcv_15m:
            cons = state.consolidation_detector.detect(ohlcv_15m, price)
            allow, reason = filter_mid_range(cons, price, "long", verbose=False)
            
            if cons.is_consolidating and not allow:
                if verbose:
                    print(f"{log_prefix} ❌ [CONSOLIDATION] {reason}")
                return None
            
            if cons.has_spring and cons.is_consolidating:
                base_score += 12  # Бонус за Spring
                if verbose:
                    print(f"{log_prefix} ✅ [SPRING] +12 — ложный пробой вниз")
            
            if cons.has_breakout_up and cons.is_consolidating:
                base_score += 8  # Бонус за пробой
                if verbose:
                    print(f"{log_prefix} ✅ [BREAKOUT] +8 — пробой консолидации")
        if verbose:
            print(f"{log_prefix} 📊 [BASE_SCORER] score={base_score:.1f} | reasons: {list(base_result.reasons)[:3]}")

        # ── RealtimeScorer ────────────────────────────────────────────
        rt        = get_realtime_scorer()
        rt_result = await rt.score(
            direction="long", market_data=md,
            base_score=base_score, hourly_deltas=hourly_deltas,
        )
        if rt_result.early_only:
            if verbose:
                print(f"{log_prefix} ❌ [REALTIME] early_only=True — ранний выход, сигнал слабый")
            return None
        
        if verbose:
            print(f"{log_prefix} 📊 [REALTIME] base={rt_result.base_score:.1f} bonus={rt_result.bonus:+.1f} final={rt_result.final_score:.1f}")
        
        base_score = rt_result.final_score

        # ── SL НИЖЕ входа (Long) ──────────────────────────────────────
        stop_loss   = price * (1 - Config.SL_BUFFER / 100)
        entry_price = price

        # SMC Bullish refinement
        smc_data = {}
        if Config.ENABLE_SMC:
            try:
                from core.smc_ict_detector import get_smc_result
                smc = get_smc_result(_ohlcv(ohlcv_15m), "long",
                                     base_sl_pct=Config.SL_BUFFER, base_entry=price)
                if smc.score_bonus > 0:
                    base_score += smc.score_bonus
                    if verbose:
                        print(f"{log_prefix} ✅ [SMC] бонус +{smc.score_bonus:.1f} | has_ob={smc.has_ob}, has_fvg={smc.has_fvg}")
                if smc.refined_sl and smc.refined_sl < price:
                    stop_loss = smc.refined_sl
                    if verbose:
                        print(f"{log_prefix} 🎯 [SMC] SL refined: {stop_loss:.4f}")
                if smc.ob_entry:
                    entry_price = smc.ob_entry
                smc_data = {"has_ob": smc.has_ob, "has_fvg": smc.has_fvg, "bonus": smc.score_bonus}
            except Exception as e:
                if verbose:
                    print(f"{log_prefix} ⚠️ [SMC] error: {e}")

        sl_pct = round((price - stop_loss) / price * 100, 2)
        if sl_pct < Config.SL_BUFFER:
            stop_loss = price * (1 - Config.SL_BUFFER / 100)
            sl_pct    = Config.SL_BUFFER

        # ── Dynamic TP (выше входа для Long) ─────────────────────────
        take_profits = []
        if state.dca_engine:
            atr_val = state.dca_engine.calculate_atr(ohlcv_15m)
            tps     = state.dca_engine.calculate_tp_levels(
                entry_price=entry_price, sl_price=stop_loss,
                num_tps=4, funding_rate=md.funding_rate, atr=atr_val,
            )
            take_profits = tps
        else:
            for i, tp_pct in enumerate(Config.TP_LEVELS[:4]):
                tp_price = price * (1 + tp_pct / 100)
                take_profits.append((round(tp_price, 8), Config.TP_WEIGHTS[i]))

        # ── AEGIS LONG ENGINE ─────────────────────────────────────────
        aegis_signal = None
        aegis_components = {}
        if state.signal_engine and Config.ENABLE_AEGIS_ENGINE:
            try:
                aegis_signal = await state.signal_engine.generate_signal(
                    symbol=symbol,
                    market_data=md,
                    ohlcv_15m=ohlcv_15m,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    sl_pct=sl_pct,
                    take_profits=take_profits,
                    base_score=base_score,
                )
                if aegis_signal:
                    final_score      = aegis_signal.total_score
                    aegis_components = {k: round(v.raw_score, 1)
                                        for k, v in aegis_signal.components.items()}
                    if verbose:
                        print(f"{log_prefix} ✅ [AEGIS] score={final_score:.1f} | components: {aegis_components}")
                else:
                    if verbose:
                        print(f"{log_prefix} ❌ [AEGIS] signal=None — Aegis engine отклонил")
                    return None
            except Exception as e:
                if verbose:
                    print(f"{log_prefix} ⚠️ [AEGIS] error: {e}")
                final_score = base_score
        else:
            final_score = base_score

        if final_score < Config.MIN_SCORE:
            if verbose:
                print(f"{log_prefix} ❌ [FINAL_FILTER] score={final_score:.1f} < MIN={Config.MIN_SCORE} — сигнал отклонён")
            return None

        # ── Smart DCA Grid (ниже входа для Long) ─────────────────────
        dca_grid_info = {}
        if state.dca_engine and Config.ENABLE_SMART_DCA:
            try:
                atr_val = state.dca_engine.calculate_atr(ohlcv_15m)
                grid    = state.dca_engine.calculate_grid(
                    symbol=symbol, entry_price=entry_price,
                    capital=state.risk_manager.capital,
                    initial_risk_pct=Config.RISK_PER_TRADE,
                    atr=atr_val, sl_price=stop_loss,
                )
                dca_grid_info = {
                    "levels": [(l.price, l.size_usd, l.distance_pct) for l in grid.levels],
                    "weighted_avg": grid.weighted_avg,
                    "total_exposure": grid.total_exposure,
                    "atr": round(atr_val, 8),
                }
            except Exception as e:
                print(f"DCA grid error {symbol}: {e}")

        # ── Risk sizing ───────────────────────────────────────────────
        risk_result = None
        if state.risk_manager:
            try:
                risk_result = state.risk_manager.calculate_position_size(
                    signal_score=final_score, sl_pct=sl_pct,
                )
                if verbose and risk_result:
                    print(f"{log_prefix} 💰 [RISK] Kelly pos={risk_result.size_usd:.2f}$ | kelly={risk_result.kelly_pct:.2f}%")
            except Exception as e:
                if verbose:
                    print(f"{log_prefix} ⚠️ [RISK] error: {e}")

        # ── Performance tracking ──────────────────────────────────────
        if state.performance_tracker:
            strength = aegis_signal.strength.value if aegis_signal else "N/A"
            state.performance_tracker.record_signal(symbol, final_score, strength, "long")

        reasons = list(base_result.reasons)
        reasons.extend(rt_result.factors)
        if aegis_signal:
            reasons.extend(aegis_signal.reasons[:6])

        return {
            "symbol":       symbol,
            "direction":    "long",
            "score":        round(final_score, 1),
            "grade":        aegis_signal.grade if aegis_signal else base_result.grade,
            "strength":     aegis_signal.strength.value if aegis_signal else "N/A",
            "confidence":   base_result.confidence.value,
            "price":        price,
            "entry_price":  entry_price,
            "stop_loss":    round(stop_loss, 8),
            "sl_pct":       sl_pct,
            "take_profits": take_profits,
            "patterns":     [p.name for p in patterns],
            "best_pattern": patterns[0].name if patterns else None,
            "indicators": {
                "RSI":      f"{md.rsi_1h:.1f}" if md.rsi_1h else "N/A",
                "Funding":  f"{md.funding_rate:+.3f}%",
                "L/S":      f"{md.long_short_ratio:.0f}% longs",
                "OI 4d":    f"{md.oi_change_4d:+.1f}%",
                "Price 4d": f"{p4d:+.1f}%",
            },
            "aegis_components": aegis_components,
            "dca_grid":     dca_grid_info,
            "risk": {
                "size_usd": risk_result.size_usd if risk_result else None,
                "kelly_pct": risk_result.kelly_pct if risk_result else None,
            },
            "smc": smc_data,
            "reasons": reasons[:14],
            # Compatibility
            "rsi_1h":           round(md.rsi_1h or 0, 1),
            "funding_rate":     round(md.funding_rate, 4),
            "oi_change":        round(md.oi_change_4d, 2),
            "long_short_ratio": round(md.long_short_ratio, 1),
            "volume_spike_ratio": round(getattr(md, "volume_spike_ratio", 1.0), 2),
            "atr_14_pct":       round(getattr(md, "atr_14_pct", 0.5), 3),
            "pattern":          patterns[0].name if patterns else "",
            "timestamp":        datetime.utcnow().isoformat(),
            "status":           "active",
            "taken_tps":        [],
        }
        
        if verbose:
            print(f"🟢 [SIGNAL-LONG] {symbol}: score={final_score:.1f} grade={signal['grade']} — сигнал создан и отправлен в Telegram!")
        return signal

    except Exception as e:
        print(f"Error scanning {symbol}: {e}")
        return None


async def scan_market():
    if state.is_paused:
        return

    print(f"\n🔍 {Config.BOT_NAME} scan at {datetime.utcnow().strftime('%H:%M:%S UTC')}")
    print(f"📊 {len(state.watchlist)} symbols | Score≥{Config.MIN_SCORE}")

    # Circuit breaker
    if state.risk_manager:
        blocked, reason = state.risk_manager.check_circuit_breakers()
        if blocked:
            print(f"⛔ CB: {reason}")
            await state.telegram.send_message(
                f"⛔ <b>CIRCUIT BREAKER</b>\n{reason}\n\nИспользуйте /reset"
            )
            return

    # BTC data (главный фильтр для Long)
    _btc_cache_1h: Optional[float] = await _get_btc_change()

    # BTC correlation score adjustment (Long — ПОЗИТИВНАЯ корреляция)
    btc_adj = 0
    btc_label = "BTC N/A"
    if _btc_cache_1h is not None:
        if _btc_cache_1h > 2.0:   btc_adj = +5;  btc_label = f"BTC +{_btc_cache_1h:.1f}% 🚀"
        elif _btc_cache_1h > 0.5: btc_adj = +2;  btc_label = f"BTC +{_btc_cache_1h:.1f}% ↗"
        elif _btc_cache_1h < -2.0: btc_adj = -5; btc_label = f"BTC {_btc_cache_1h:.1f}% 🔴"
        elif _btc_cache_1h < -0.5: btc_adj = -2; btc_label = f"BTC {_btc_cache_1h:.1f}% ↘"
        else: btc_label = f"BTC {_btc_cache_1h:.1f}% ↔"
    print(f"📡 {btc_label} (score adj {btc_adj:+d})")

    active_count  = await _count_long_positions()
    exchange_full = active_count >= Config.MAX_POSITIONS
    if exchange_full:
        print(f"📊 Exchange: {active_count}/{Config.MAX_POSITIONS} LONG slots — TG-only mode")

    new_signals = tg_only_count = 0

    for symbol in state.watchlist:
        try:
            if _is_fresh(state.redis.get_signals(Config.BOT_TYPE, symbol, limit=1)):
                continue

            signal = await scan_symbol(symbol, _btc_cache_1h)
            if not signal:
                continue

            # BTC correlation adj
            signal["score"] = round(signal["score"] + btc_adj, 1)
            if signal["score"] < Config.MIN_SCORE:
                continue

            # Telegram сигнал
            tg_msg_id = await state.telegram.send_signal(
                direction="long", symbol=signal["symbol"],
                score=signal["score"], price=signal["price"],
                pattern=signal.get("strength", signal.get("best_pattern") or "N/A"),
                indicators=signal["indicators"],
                entry=signal["entry_price"],
                stop_loss=signal["stop_loss"],
                take_profits=signal["take_profits"],
                leverage=Config.LEVERAGE, risk="Kelly-sized",
            )
            signal["tg_msg_id"] = tg_msg_id

            # Aegis компоненты в TG
            if signal.get("aegis_components"):
                comp_str = " | ".join(
                    f"{k[:4]}: {v:.0f}" for k, v in signal["aegis_components"].items()
                )
                await state.telegram.send_message(
                    f"📊 <b>Aegis Components</b> — {signal['symbol']}\n"
                    f"<code>{comp_str}</code>\n"
                    f"Grade: {signal.get('grade','?')} | {signal.get('strength','?')}"
                )

            state.redis.save_signal(Config.BOT_TYPE, symbol, signal)

            if not exchange_full and Config.AUTO_TRADING and not state.is_paused:
                if state.auto_trader:
                    try:
                        await state.auto_trader.execute_signal(signal)
                        active_count += 1
                        exchange_full = active_count >= Config.MAX_POSITIONS
                    except Exception as e:
                        print(f"AutoTrader error {symbol}: {e}")
                new_signals += 1
            else:
                tg_only_count += 1

            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"Error {symbol}: {e}")

    state.daily_signals += new_signals + tg_only_count
    state.last_scan      = datetime.utcnow()
    state.active_signals = len(state.redis.get_active_signals(Config.BOT_TYPE))
    state.redis.update_bot_state(Config.BOT_TYPE, {
        "status":        "paused" if state.is_paused else "running",
        "last_scan":     state.last_scan.isoformat(),
        "daily_signals": state.daily_signals,
        "version":       Config.BOT_VERSION,
    })
    print(f"✅ Scan done. New: {new_signals} | TG-only: {tg_only_count} | "
          f"Exchange: {active_count}/{Config.MAX_POSITIONS}")


async def background_scanner():
    while state.is_running:
        if not state.is_paused:
            try:
                await scan_market()
            except Exception as e:
                print(f"Scanner error: {e}")
        await asyncio.sleep(Config.SCAN_INTERVAL)


async def _daily_report_task():
    while state.is_running:
        now  = datetime.utcnow()
        next_report = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= next_report:
            next_report += timedelta(days=1)
        await asyncio.sleep((next_report - now).total_seconds())
        if state.performance_tracker and state.is_running:
            try:
                await state.telegram.send_message(
                    state.performance_tracker.daily_report()
                )
            except Exception as e:
                print(f"Daily report error: {e}")


# ============================================================================
# ROUTES
# ============================================================================

@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return JSONResponse({
        "status": "ok", "bot": Config.BOT_NAME, "version": Config.BOT_VERSION,
        "watchlist": len(state.watchlist), "active": state.active_signals,
        "aegis_engine": state.signal_engine is not None,
        "wyckoff": state.wyckoff_detector is not None,
    })

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return JSONResponse({
        "bot": Config.BOT_NAME, "version": Config.BOT_VERSION,
        "status": "running" if state.is_running else "stopped",
    })

@app.get("/status")
async def status():
    cb = state.risk_manager.get_portfolio_heat(0) if state.risk_manager else {}
    return {
        "bot": Config.BOT_NAME, "version": Config.BOT_VERSION,
        "is_running": state.is_running, "is_paused": state.is_paused,
        "watchlist_count": len(state.watchlist),
        "active_signals":  state.active_signals,
        "last_scan":       state.last_scan.isoformat() if state.last_scan else None,
        "risk_manager":    cb,
        "config": {
            "min_score": Config.MIN_SCORE, "sl_buffer": Config.SL_BUFFER,
            "scan_interval": Config.SCAN_INTERVAL, "max_pairs": Config.MAX_PAIRS,
            "btc_block_at": Config.BTC_BLOCK_THRESHOLD,
            "auto_trading": Config.AUTO_TRADING,
        },
    }

@app.post("/api/scan")
async def trigger_scan(bg: BackgroundTasks):
    if not state.is_running: raise HTTPException(503, "Not running")
    if state.is_paused:      raise HTTPException(503, "Paused")
    bg.add_task(scan_market)
    return {"message": "Scan triggered"}

@app.get("/api/signals")
async def get_signals():
    signals = state.redis.get_active_signals(Config.BOT_TYPE)
    return {"count": len(signals), "signals": signals}

@app.get("/api/performance")
async def get_performance():
    return state.performance_tracker.get_stats(7) if state.performance_tracker else {}

@app.get("/api/risk")
async def get_risk():
    if state.risk_manager:
        return {
            "status": state.risk_manager.status_report(),
            "stats":  state.risk_manager.get_win_stats(),
            "heat":   state.risk_manager.get_portfolio_heat(0),
        }
    return {"error": "Not initialized"}

@app.get("/api/dca/{symbol}")
async def get_dca_grid(symbol: str):
    if not state.dca_engine:
        return {"error": "DCA disabled"}
    grid = state.dca_engine.calculate_grid(
        symbol=symbol.upper(), entry_price=1.0,
        capital=state.risk_manager.capital if state.risk_manager else 1000,
        initial_risk_pct=Config.RISK_PER_TRADE,
    )
    return {
        "symbol": grid.symbol, "entry": grid.entry_price,
        "levels": [(l.price, l.size_usd, l.distance_pct) for l in grid.levels],
        "weighted_avg": grid.weighted_avg, "total_usd": grid.total_exposure,
    }

@app.get("/api/positions")
async def get_positions():
    if state.auto_trader:
        pos = await state.auto_trader.bingx.get_positions()
        return {"count": len(pos), "positions": [
            {"symbol": p.symbol, "side": p.side, "upnl": p.unrealized_pnl}
            for p in pos
        ]}
    return {"count": 0, "positions": []}

@app.post("/api/circuit-breaker/reset")
async def reset_cb():
    if state.risk_manager:
        state.risk_manager.reset_circuit_breaker(force=True)
        return {"message": "Reset OK"}
    return {"error": "Not initialized"}

@app.post("/webhook")
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
        if state.cmd_handler:
            await state.cmd_handler.handle_update(update)
        return {"ok": True}
    except Exception as e:
        print(f"Webhook error: {e}"); return {"ok": False}

@app.get("/webhook/setup")
@app.get("/webhook/reset")
async def setup_webhook():
    render_url = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url: return {"error": "RENDER_EXTERNAL_URL not set"}
    wh_url = f"{render_url}/webhook"
    await state.telegram.delete_webhook()
    await asyncio.sleep(1)
    ok = await state.telegram.setup_webhook(wh_url)
    return {"ok": ok, "url": wh_url}

@app.get("/webhook/info")
async def webhook_info():
    return {"webhook": await state.telegram.get_webhook_info()} if state.telegram else {}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0",
                port=int(os.getenv("PORT", 8001)), reload=False)
