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
import re
import asyncio
import sys
import logging
import gc
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from contextlib import asynccontextmanager

# Настройка логирования — однократная конфигурация с dedup-фильтром
import time as _time

class _DedupLogFilter(logging.Filter):
    """Подавляет одинаковые log-строки в пределах 5-секундного окна."""
    def __init__(self, capacity: int = 300):
        super().__init__()
        self._seen: dict = {}
        self._cap = capacity

    def filter(self, record: logging.LogRecord) -> bool:
        key = f"{record.levelno}:{record.name}:{record.getMessage()[:120]}"
        now = _time.monotonic()
        last = self._seen.get(key, 0.0)
        if now - last < 5.0:
            return False
        self._seen[key] = now
        if len(self._seen) > self._cap:
            oldest = sorted(self._seen, key=self._seen.get)[:60]
            for k in oldest:
                del self._seen[k]
        return True

if not logging.root.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
_dedup_filter = _DedupLogFilter()
logging.getLogger("aegis.signal_engine_long").addFilter(_dedup_filter)
logging.getLogger("aegis").addFilter(_dedup_filter)
# B6 FIX: расширяем dedup на PatternML, orderbook и root (подавляем ×12 при hot-reload)
logging.getLogger("aegis.pattern_ml").addFilter(_dedup_filter)
logging.getLogger("core.orderbook_scorer").addFilter(_dedup_filter)
logging.root.addFilter(_dedup_filter)  # ← ловит всё, включая не-aegis логгеры

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
# ВАЖНО: _SHARED должен быть в sys.path РАНЬШЕ _SRC, иначе пустой
# long-bot/src/execution/ затенит shared/execution/ (package shadowing bug)
for _p in [_SHARED, os.path.dirname(_SHARED)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# _SRC добавляем в конец — локальные модули (aegis/, detectors/) не конфликтуют с shared
if _SRC not in sys.path:
    sys.path.append(_SRC)

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
from utils.okx_liquidation_ws import OKXLiquidationFeed

# ── Aegis Long modules ──
from aegis.signal_engine_long import AegisLongSignalEngine, SignalStrengthLong
from aegis.systemic_crash_guard import SystemicCrashGuard
from aegis.smart_dca_long import SmartDCALongEngine, GridConfigLong, GridTypeLong
from aegis.risk_manager import AegisRiskManager, RiskLimits
from aegis.performance_tracker import PerformanceTracker, TradeRecord
from detectors.dump_detector import DumpExhaustionDetector, DumpDetectorConfig
from detectors.wyckoff_detector import WyckoffAccumulationDetector
from detectors.bsl_scanner import BSLScanner
from detectors.oi_analyzer_long import OIAnalyzerLong, FundingConfigLong
from detectors.liquidation_mapper_long import LiquidationMapperLong
from detectors.delta_analyzer_long import DeltaAnalyzerLong
from detectors.netflow_analyzer import NetflowAnalyzerLong
from core.kill_zone_filter import KillZoneFilter  # #19
from core.btc_momentum_guard import BTCMomentumGuard


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
    MAX_POSITIONS = int(os.getenv("MAX_LONG_POSITIONS", os.getenv("MAX_POSITIONS", "12")))

    MIN_SCORE     = int(os.getenv("MIN_LONG_SCORE", "72"))   # FIX: default выровнен с render.yaml (было 52 — расхождение 20 пунктов)
    SL_BUFFER     = float(os.getenv("LONG_SL_BUFFER", "2.5"))  # ✅ FIX v17: 3.0→2.5% (TP1 теперь выше)
    LEVERAGE      = os.getenv("LONG_LEVERAGE", "5-20")

    # LONG TP: 4 уровня дефолт. TP5–TP6 только при EXTENDED_TP_LONG=true + трендовый паттерн
    # ENV: EXTENDED_TP_LONG=true → разрешить 6 TP для BREAKOUT/WYCKOFF/SWEEP
    TP_LEVELS  = [3.0, 5.0, 8.0, 12.0, 18.0, 25.0]  # TP5=18%, TP6=25% — только extended
    TP_WEIGHTS_4 = [25, 30, 25, 20]                   # дефолт 4 TP, сумма=100%
    TP_WEIGHTS_6 = [15, 20, 20, 15, 15, 15]           # extended 6 TP, сумма=100%
    # Паттерны, при которых оправдан Extended TP (трендовые, не контртрендовые)
    EXTENDED_TP_PATTERNS = {
        "BREAKOUT_LONG", "WYCKOFF_SPRING", "LIQUIDITY_SWEEP_LONG",
        "MOMENTUM_LONG", "BREAKOUT_LONG_4H", "WYCKOFF_SPRING_4H",
        "LIQUIDITY_SWEEP_LONG_4H", "MOMENTUM_LONG_4H",
    }

    # Risk management
    RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE", "0.0004"))
    MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.15"))
    MAX_EXPOSURE_PCT = float(os.getenv("MAX_EXPOSURE_PCT", "0.60"))
    DAILY_DD_LIMIT   = float(os.getenv("DAILY_DRAWDOWN_LIMIT", "3.0"))
    MAX_CONSEC_LOSS  = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))
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
    # DEMO / REAL режим: BINGX_DEMO_MODE=true (демо, по умолч.) | BINGX_DEMO_MODE=false (реальные деньги ⚠️)
    BINGX_DEMO   = os.getenv("BINGX_DEMO_MODE", "true").strip().lower() not in ("false", "0", "no", "real")

    # Watchlist
    MIN_VOLUME_USDT     = int(os.getenv("MIN_VOLUME_USDT", "200000"))   # ✅ v2.1: 300K→200K (ловим больше монет)
    MAX_WATCHLIST       = int(os.getenv("MAX_WATCHLIST", "200"))         # ✅ v2.1: 150→200
    WATCHLIST_REFRESH_H = float(os.getenv("WATCHLIST_REFRESH_H", "2.0")) # ✅ v2.1: обновление каждые 2ч

    # ATR-dynamic SL (M1)
    USE_ATR_SL  = os.getenv("USE_ATR_SL", "true").lower() == "true"
    ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "1.5"))   # SL = entry - ATR × 1.5
    ATR_SL_MIN  = float(os.getenv("ATR_SL_MIN_PCT", "1.0")) # мин SL не меньше 1%
    ATR_SL_MAX  = float(os.getenv("ATR_SL_MAX_PCT", "4.0")) # макс SL не больше 4%

    # Momentum LONG (M2)
    ENABLE_MOMENTUM_LONG     = os.getenv("ENABLE_MOMENTUM_LONG", "true").lower() == "true"
    MOMENTUM_RSI_MIN         = float(os.getenv("MOMENTUM_RSI_MIN", "58"))
    MOMENTUM_VOL_MIN         = float(os.getenv("MOMENTUM_VOL_MIN", "1.8"))
    # ✅ FIX: MOMENTUM PATH — отдельный порог для momentum сделок (bypass BASE_SCORER gate)
    MOMENTUM_SCORE_THRESHOLD = int(os.getenv("MOMENTUM_SCORE_THRESHOLD", "58"))

    # ✅ FIX: AEGIS_LONG_MIN_SCORE — реальный порог Aegis engine (был мёртвым ENV, теперь работает)
    AEGIS_MIN_SCORE    = int(os.getenv("AEGIS_LONG_MIN_SCORE", "65"))  # FIX: default выровнен с render.yaml (было 52)

    # ✅ FIX: Adaptive threshold ceiling — max +N от MIN_LONG_BASE_SCORE (было хардкод 78)
    ADAPTIVE_MAX_BOOST = int(os.getenv("ADAPTIVE_MAX_BOOST", "3"))

    # ✅ Постоянный блэклист — символы которые всегда пропускаем
    # Формат ENV: SYMBOL_BLACKLIST=GIGAUSDT,LUNAUSDT,我踏马来了USDT
    SYMBOL_BLACKLIST: set = set(
        s.strip().upper() for s in os.getenv("SYMBOL_BLACKLIST", "").split(",") if s.strip()
    )

    MAX_DAILY_TRADES  = int(os.getenv("MAX_DAILY_TRADES_LONG", "10"))  # v3.0
    SIGNAL_TTL_HOURS  = 24
    TRAIL_ACTIVATION  = float(os.getenv("LONG_TRAIL_ACTIVATION", "0.015"))  # +1.5%

    # BTC correlation thresholds (Long = позитивная корреляция)
    # BTC фильтр: блокируем LONG только при РЕЗКОМ падении BTC.
    # НЕ блокируем при небольших движениях — альты могут идти независимо.
    # Рекомендация: 1H -3.0%, 4H -3.0% — только экстремальные движения.
    BTC_BLOCK_THRESHOLD  = float(os.getenv("BTC_BLOCK_THRESHOLD", "-3.0"))  # 1H: блок при -3%/h (резкий дамп)
    BTC_4H_BLOCK         = float(os.getenv("BTC_4H_BLOCK_THRESHOLD", "-3.0"))  # 4H: блок при -3% за 4ч

    # P4: Funding extreme thresholds (informational — scorer reads ENV directly)
    FUNDING_EXTREME_LONG  = float(os.getenv("FUNDING_EXTREME_LONG",  "-0.05"))
    FUNDING_EXTREME_SHORT = float(os.getenv("FUNDING_EXTREME_SHORT",  "0.05"))

    # P1: Order book
    ENABLE_ORDERBOOK = os.getenv("ENABLE_ORDERBOOK_SCORER", "true").lower() == "true"


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
        self.okx_ws_feed:         Optional[OKXLiquidationFeed]       = None
        self.oi_analyzer:         Optional[OIAnalyzerLong]          = None
        self.liq_mapper:          Optional[LiquidationMapperLong]   = None
        self.delta_analyzer:      Optional[DeltaAnalyzerLong]       = None
        self.coinglass            = None
        self.netflow_analyzer:    Optional[NetflowAnalyzerLong] = None
        self.liq_detector:        Optional[Any] = None
        self.fear_greed_index: Optional[int] = None   # 🆕 0-100
        self.btc_change_1h:    Optional[float] = None  # ✅ FIX #5: кешируем BTC 1h для delta scorer
        # A2: SystemicCrashGuard
        self.crash_guard:      SystemicCrashGuard      = SystemicCrashGuard()
        self.btc_momentum_guard: BTCMomentumGuard      = BTCMomentumGuard()
        # signals_db + trade_analytics
        self.signals_db        = None
        self._signal_db_map: dict = {}
        self.trade_analytics   = None
        # Signal Queue + Trade Manager
        self.signal_queue      = None
        self.trade_manager     = None
        self.ohlcv_cache       = None  # A5


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
    combined = (both + only_one)[:max_count]
    # ✅ FIX: Reject garbage symbols (Chinese chars, non-ASCII, malformed)
    _VALID_SYM = re.compile(r'^[A-Z0-9]{2,20}USDT$')
    result = [s for s in combined if _VALID_SYM.match(s)]
    # ✅ FIX БАГ 3: Дедупликация — сохраняет порядок, убирает дубликаты
    result = list(dict.fromkeys(result))
    if len(result) < len(combined):
        print(f"⚠️ Filtered {len(combined) - len(result)} invalid/duplicate symbols from watchlist")
    # ✅ FIX: ENV-блэклист (SYMBOL_BLACKLIST=GIGAUSDT,LUNAUSDT,...)
    if Config.SYMBOL_BLACKLIST:
        before = len(result)
        result = [s for s in result if s not in Config.SYMBOL_BLACKLIST]
        print(f"🚫 ENV blacklist filtered {before - len(result)} symbols")
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
    try:
        await state.binance._init_source()
    except Exception as e:
        print(f"⚠️ Binance init failed (continuing with Bybit fallback): {e}")

    # ── OKX WebSocket Liquidation Feed ──────────────────────────────────
    state.binance.set_redis(state.redis)
    state.okx_ws_feed = OKXLiquidationFeed(redis_client=state.redis)
    await state.okx_ws_feed.start()
    print("✅ OKX WS liquidation feed started (Redis cache mode)")

    # П2 FIX: BASE_SCORER получает мягкий порог — строгий порог только у AEGIS
    _long_base_min = int(os.getenv("MIN_LONG_BASE_SCORE", "58"))  # ✅ OPT v19: 50→58 убрана dead zone 50-57  # Снижено с 55: лонги в нейтральном рынке нужно ловить раньше
    state.scorer           = get_long_scorer(_long_base_min)
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

    state.liq_mapper = LiquidationMapperLong()
    state.delta_analyzer = DeltaAnalyzerLong()
    # Coinglass — exchange netflow (институциональное накопление/распределение)
    _cg_key = os.getenv("COINGLASS_API_KEY", "")
    if _cg_key:
        from api.coinglass_client import CoinglassClient
        state.coinglass = CoinglassClient(api_key=_cg_key)
        state.netflow_analyzer = NetflowAnalyzerLong(coinglass_client=state.coinglass)
        print("✅ NetflowAnalyzerLong включён (Coinglass API key найден)")
    else:
        state.coinglass = None
        state.netflow_analyzer = None
        print("⚠️ NetflowAnalyzerLong отключён (COINGLASS_API_KEY не задан)")
    state.liq_detector = None  # LiquidationZoneDetector требует отдельного восстановления

    from core.pre_pump_detector import get_pre_pump_detector
    state.signal_engine = AegisLongSignalEngine(
        dump_detector=state.dump_detector,
        oi_analyzer=state.oi_analyzer,
        bsl_scanner=state.bsl_scanner,
        wyckoff_detector=state.wyckoff_detector,
        delta_analyzer=state.delta_analyzer,
        liq_mapper=state.liq_mapper,
        netflow_analyzer=state.netflow_analyzer,
        pre_pump_detector=get_pre_pump_detector(),
        min_score=Config.AEGIS_MIN_SCORE,
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

    # ── Signals DB + Trade Analytics (подключение PLAN2 файлов) ──
    try:
        from database.signals_db import get_signals_db
        from database.trade_analytics import TradeAnalytics
        _db_path = os.getenv("SIGNALS_DB_PATH", "/opt/render/project/signals_long.db")
        state.signals_db      = get_signals_db(db_path=_db_path)
        state.trade_analytics = TradeAnalytics(redis_client=state.redis, bot_type="long")
        print("✅ SignalsDB + TradeAnalytics: подключены")
    except Exception as _e:
        print(f"⚠️ SignalsDB init error: {_e}")

    try:
        from core.signal_queue import get_signal_queue
        from execution.trade_manager import get_trade_manager
        from core.ohlcv_cache import get_ohlcv_cache  # A5
        state.signal_queue  = get_signal_queue()
        state.trade_manager = get_trade_manager()
        state.ohlcv_cache   = get_ohlcv_cache(Config.SCAN_INTERVAL)
        print("✅ SignalQueue + TradeManager + OHLCVCache: инициализированы")
    except Exception as _e:
        print(f"⚠️ SignalQueue/TradeManager init: {_e}")

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
                    max_daily_risk=Config.DAILY_DD_LIMIT,
                    max_daily_trades=Config.MAX_DAILY_TRADES,
                )
                state.auto_trader = AutoTrader(
                    bingx_client=bingx, config=trade_cfg, telegram=state.telegram
                )
                print(f"✅ BingX {'DEMO' if Config.BINGX_DEMO else 'REAL'} AutoTrader")
                if Config.BINGX_DEMO:
                    print("⚠️  BINGX_DEMO_MODE=true — ордера идут на ДЕМО-счёт!")
                    print("⚠️  Для реальной торговли: BINGX_DEMO_MODE=false")
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

    def _on_trade_closed(record: dict):
        _sym      = record.get("symbol", "")
        _pnl_pct  = float(record.get("pnl_pct", 0))
        _capital  = float(os.getenv("ACCOUNT_CAPITAL_USD", "1000"))
        _pnl_usd  = _pnl_pct * _capital / 100

        # performance_tracker
        if state.performance_tracker:
            try:
                from aegis.performance_tracker import TradeRecord
                state.performance_tracker.record_trade(TradeRecord(
                    symbol=_sym,
                    direction=record.get("direction", "long"),
                    entry_price=float(record.get("entry_price", 0)),
                    exit_price=float(record.get("close_price", 0)),
                    entry_time=record.get("opened_at", ""),
                    exit_time=record.get("closed_at", ""),
                    pnl_pct=_pnl_pct, pnl_usd=_pnl_usd,
                    won=_pnl_pct > 0,
                    exit_reason=record.get("close_type", ""),
                    score=float(record.get("score", 0)),
                    strength=record.get("strength", ""),
                ))
            except Exception:
                pass

        # signals_db: закрываем запись сигнала с P&L
        if state.signals_db:
            try:
                _sid = state._signal_db_map.pop(_sym, None)
                if _sid:
                    state.signals_db.close_signal(_sid, float(record.get("close_price", 0)), _pnl_pct, _pnl_usd)
            except Exception:
                pass

        # trade_analytics: TP-уровень детализация
        if state.trade_analytics:
            try:
                from database.trade_analytics import record_trade_with_tp
                _ct = record.get("close_type", "SL")
                if _ct == "SL":        _tp_lvl = 0
                elif _ct == "BE":      _tp_lvl = -1
                elif _ct.startswith("TP"):
                    _tp_lvl = int(_ct[2:]) if _ct[2:].isdigit() else 1
                else:                  _tp_lvl = 1 if _pnl_pct > 0 else 0
                record_trade_with_tp(
                    redis_client=state.redis,
                    symbol=_sym,
                    direction=record.get("direction", "long"),
                    entry_price=float(record.get("entry_price", 0)),
                    exit_price=float(record.get("close_price", 0)),
                    pnl_percent=_pnl_pct, pnl_usd=_pnl_usd,
                    tp_level=_tp_lvl, timeframe="15m",
                    bot_type="long",  # ✅ B8-FIX #3: изолируем long:trade_history
                )
            except Exception:
                pass

        # trade_manager: закрытие позиции → обновление TP/Win статистики
        if state.trade_manager:
            try:
                _ct = record.get("close_type", "SL")
                _close_price = float(record.get("close_price", 0))
                _reason = "SL_HIT" if _ct == "SL" else "TRAIL_STOP" if _ct == "TRAIL" else "CLOSED"
                _open_pos = state.trade_manager.get_open_positions()
                for _tm_pos in _open_pos:
                    if _tm_pos.symbol == _sym:
                        state.trade_manager._close_position(_tm_pos.trade_id, _reason, _close_price)
                        break
            except Exception:
                pass

    state.tracker = PositionTracker(
        bot_type=Config.BOT_TYPE, telegram=state.telegram,
        redis_client=state.redis, binance_client=state.binance,
        config=Config, auto_trader=state.auto_trader,
        on_trade_closed=_on_trade_closed,
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
    asyncio.create_task(_fear_greed_task())       # 🆕 F&G polling
    asyncio.create_task(_startup_sl_sync())       # 🚨 FIX: SL=0 bug sync
    asyncio.create_task(_watchlist_refresh_task()) # ✅ C3: watchlist auto-refresh

    # SignalQueue: retry processor для failed execute_signal
    if state.signal_queue:
        async def _execute_queued_signal(sq_sig) -> bool:
            if not state.auto_trader:
                return False
            try:
                sig_dict = sq_sig.indicators if isinstance(sq_sig.indicators, dict) else {}
                if not sig_dict or not sig_dict.get("entry_price"):
                    return False
                result = await state.auto_trader.execute_signal(sig_dict)
                if result and state.trade_manager:
                    try:
                        _lev = int(str(Config.LEVERAGE).split("-")[0])
                        state.trade_manager.create_position(
                            symbol=sq_sig.symbol, direction="LONG",
                            entry_price=sig_dict.get("entry_price", 0),
                            qty=float(result.get("qty", 0)) if isinstance(result, dict) else 0.0,
                            stop_loss=sig_dict.get("stop_loss", 0), leverage=_lev,
                        )
                    except Exception:
                        pass
                return result is not None
            except Exception as _e:
                print(f"[SignalQueue] retry failed {sq_sig.symbol}: {_e}")
                return False
        await state.signal_queue.start_processing(_execute_queued_signal)

    yield

    state.is_running = False
    if state.okx_ws_feed: await state.okx_ws_feed.stop()
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
                getattr(p, "position_side", "").upper() == "LONG"
                or getattr(p, "positionSide", "").upper() == "LONG"
                or getattr(p, "side", "").upper() == "LONG"
                or getattr(p, "direction", "").upper() == "BUY"
            ) and getattr(p, "size", 0) != 0]
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


async def _get_btc_change_4h() -> Optional[float]:
    """BTC 4H изменение — трендовый фильтр для Long (не открываем лонги в даунтренде)
    Берём последние 4 часовые свечи из get_ohlcv и считаем изменение закрытия.
    """
    try:
        ohlcv = await state.binance.get_klines("BTCUSDT", interval="1h", limit=5)
        if ohlcv and len(ohlcv) >= 5:
            open_price  = ohlcv[-5].close  # 4 часа назад
            close_price = ohlcv[-1].close  # сейчас
            return round((close_price - open_price) / open_price * 100, 2)
        return None
    except Exception:
        return None


async def _get_btc_change_24h() -> float:
    """BTC 24h изменение — для Relative Strength расчёта."""
    try:
        btc = await state.binance.get_complete_market_data("BTCUSDT")
        return getattr(btc, "price_change_24h", 0.0) if btc else 0.0
    except Exception:
        return 0.0



async def scan_symbol(symbol: str, cached_btc_1h: Optional[float] = None, verbose: bool = True, cached_btc_24h: float = 0.0) -> Optional[Dict]:
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
        # ✅ OPT: in-memory sets (загружены в scan_market) — нет Redis-вызовов
        _bl_set = getattr(state, '_blacklist_set', None)
        _sk_set = getattr(state, '_skip_nodata_set', None)
        if _bl_set is not None:
            if symbol in _bl_set: return None
        elif state.redis and state.redis.client.exists(f"blacklist:{symbol}"):
            return None
        if _sk_set is not None:
            if symbol in _sk_set: return None
        elif state.redis and state.redis.client.exists(f"skip:nodata:{symbol}"):
            return None

        md = await state.binance.get_complete_market_data(symbol)
        if not md:
            if verbose:
                print(f"{log_prefix} ❌ Нет market data от Binance")
            # ✅ FIX: Счётчик промахов → вечный бан после 3 раз
            try:
                if state.redis:
                    count_key = f"nodata_count:{symbol}"
                    count = state.redis.client.incr(count_key)
                    state.redis.client.expire(count_key, 86400 * 30)
                    if count >= 3:
                        state.redis.client.set(f"blacklist:{symbol}", f"nodata:{count}")
                        state.redis.client.delete(count_key)
                        print(f"🚫 [{symbol}] Добавлен в постоянный блэклист ({count} промахов)")
                        if hasattr(state, '_blacklist_set') and state._blacklist_set is not None:
                            state._blacklist_set.add(symbol)
                    else:
                        state.redis.client.setex(f"skip:nodata:{symbol}", 86400, "1")
                        if hasattr(state, '_skip_nodata_set') and state._skip_nodata_set is not None:
                            state._skip_nodata_set.add(symbol)
            except Exception:
                pass
            return None

        # A2: CrashGuard — регистрируем символ для alts breadth (FIX: было пропущено)
        state.crash_guard.update_symbol(getattr(md, "price_change_1h", 0.0) or 0.0)

        # A2: SystemicCrashGuard — системный краш блокирует новые LONG
        # Outlier bypass: если токен price_24h > 15% И vol_spike > 2x — divergence, разрешаем
        _cg_price_24h = getattr(md, "price_change_24h", 0.0) or 0.0
        _cg_vol_spike = getattr(md, "volume_spike_ratio", 1.0) or 1.0
        if state.crash_guard.is_crash_for_token(_cg_price_24h, _cg_vol_spike):
            if verbose:
                print(f"{log_prefix} 🆘 [SYSTEMIC_CRASH] {state.crash_guard.reason} — LONG заблокирован")
            return None

        # Post-crash cooldown: рынок восстановился, но слишком рано — блок на N мин
        if state.crash_guard.is_post_crash_cooldown():
            if verbose:
                print(f"{log_prefix} ⏳ [POST_CRASH_COOLDOWN] Рынок восстановился, но ещё cooldown — LONG заблокирован")
            return None

        # ── BTCMomentumGuard: Rapid Dump блокирует LONG ───────────────
        _btc_mg_long_mult = state.btc_momentum_guard.get_long_multiplier()
        if _btc_mg_long_mult <= 0.0:
            if verbose:
                print(f"{log_prefix} 🚫 [BTC_RAPID_DUMP] {state.btc_momentum_guard.reason} — LONG заблокирован")
            return None
        if _btc_mg_long_mult < 1.0:
            if verbose:
                print(f"{log_prefix} ⚠️ [BTC_RAPID_DUMP] ×{_btc_mg_long_mult} — штраф ({state.btc_momentum_guard.reason})")

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

        # A5: Загружаем OHLCV через кеш (один запрос на (symbol, interval) за скан)
        _cache = state.ohlcv_cache
        if _cache:
            ohlcv_15m, ohlcv_30m, ohlcv_4h = await asyncio.gather(
                _cache.get(symbol, "15m", 100, lambda: state.binance.get_klines(symbol, "15m", 100)),
                _cache.get(symbol, "30m", 50,  lambda: state.binance.get_klines(symbol, "30m", 50)),
                _cache.get(symbol, "4h",  20,  lambda: state.binance.get_klines(symbol, "4h",  20)),
                return_exceptions=True,
            )
        else:
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

        # П10: Multi-Timeframe RSI bonus (LONG: oversold = бонус, momentum = нейтральный)
        _mtf_bonus = 0
        try:
            def _calc_rsi_l(candles, period=14):
                if not candles or len(candles) < period + 1: return None
                closes = [c.close for c in candles]
                gains, losses = [], []
                for i in range(1, len(closes)):
                    d = closes[i] - closes[i-1]
                    gains.append(max(d, 0)); losses.append(max(-d, 0))
                avg_g = sum(gains[:period]) / period
                avg_l = sum(losses[:period]) / period
                for i in range(period, len(gains)):
                    avg_g = (avg_g*(period-1)+gains[i])/period
                    avg_l = (avg_l*(period-1)+losses[i])/period
                rs = avg_g / avg_l if avg_l > 0 else 100
                return round(100 - 100/(1+rs), 1)

            rsi_30m = _calc_rsi_l(ohlcv_30m) if ohlcv_30m else None
            rsi_4h  = _calc_rsi_l(ohlcv_4h)  if ohlcv_4h  else None
            rsi_1h  = md.rsi_1h or 50
            _price_1h_chg = getattr(md, "price_change_1h", 0) or 0
            _price_4h_chg = getattr(md, "price_change_4h", 0) or 0
            _is_momentum_up = _price_1h_chg > 3.0 or _price_4h_chg > 8.0

            if rsi_4h and rsi_30m:
                if rsi_4h < 30 and rsi_1h < 35 and rsi_30m < 35:
                    _mtf_bonus = 15
                    if verbose: print(f"{log_prefix} 📉 [MTF] RSI 4H={rsi_4h} 1H={rsi_1h:.0f} 30M={rsi_30m} — всё перепродано +15 LONG")
                elif rsi_4h < 35 and rsi_1h < 40:
                    _mtf_bonus = 8
                    if verbose: print(f"{log_prefix} 📉 [MTF] RSI 4H={rsi_4h} 1H={rsi_1h:.0f} — перепродан +8 LONG")
                elif rsi_4h > 70 and rsi_1h > 70 and _is_momentum_up:
                    # MOMENTUM LONG: RSI высокий, но цена реально растёт — не штрафуем
                    _mtf_bonus = 5
                    if verbose: print(f"{log_prefix} 🚀 [MTF] RSI {rsi_1h:.0f} высокий но MOMENTUM +{_price_1h_chg:.1f}%/1H — нейтральный +5")
                elif rsi_4h > 70 and rsi_1h > 70:
                    _mtf_bonus = -5  # Перегрет без ценового подтверждения — лёгкий штраф
                    if verbose: print(f"{log_prefix} ⚠️ [MTF] RSI 4H={rsi_4h} перегрет без тренда {_mtf_bonus}")
        except Exception as _mtf_e:
            if verbose: print(f"{log_prefix} ⚠️ [MTF] error: {_mtf_e}")

        # ── Базовый scorer (backward compat) ─────────────────────────
        # ✅ OPT: hourly_deltas уже загружены в get_complete_market_data → md.hourly_deltas
        hourly_deltas = getattr(md, "hourly_deltas", None) or []
        price_trend   = state.pattern_detector._get_price_trend(ohlcv_15m)
        patterns      = state.pattern_detector.detect_all(ohlcv_15m, hourly_deltas, md)

        # 🆕 Паттерны на 30M — уже загружены, просто запускаем detect_all
        # Вес 1.15x (между 15m и 4H) — более структурные чем 15m
        if ohlcv_30m and len(ohlcv_30m) >= 20:
            try:
                _pat_30m = state.pattern_detector.detect_all(ohlcv_30m, None, md)
                for _p in _pat_30m:
                    _p.score_bonus = int(_p.score_bonus * 1.15)
                    _p.name = f"{_p.name}_30M"
                patterns = patterns + _pat_30m
            except Exception:
                pass

        # ✅ v18: HTF паттерны на 4H и 1D (более значимые сигналы)
        # Паттерн на 4H весит больше т.к. структурно значим
        _ms_data = getattr(md, "market_structure", None)
        _klines_4h = _ms_data and getattr(_ms_data, "has_4h", False)
        _klines_1d = _ms_data and getattr(_ms_data, "has_1d", False)
        if ohlcv_4h and len(ohlcv_4h) >= 20:
            try:
                _pat_4h = state.pattern_detector.detect_all(ohlcv_4h, None, md)
                for _p in _pat_4h:
                    # Увеличиваем вес HTF паттернов — они надёжнее
                    _p.score_bonus = int(_p.score_bonus * 1.3)
                    _p.name = f"{_p.name}_4H"
                patterns = patterns + _pat_4h
            except Exception:
                pass
        if _ms_data and _ms_data.has_1d:
            # ✅ OPT v19: Используем уже загруженные 1D klines из market_structure (no extra API call)
            try:
                # Пробуем получить из кэша через batch-запрос binance (уже загружены)
                _kl_1d = getattr(state.binance, "_last_1d_klines", {}).get(symbol)
                if not _kl_1d:
                    # Fallback: но 1D klines уже загружены в get_complete_market_data
                    # Просто пропускаем отдельный запрос
                    pass
                if _kl_1d and len(_kl_1d) >= 10:
                    _pat_1d = state.pattern_detector.detect_all(_kl_1d, None, md)
                    for _p in _pat_1d:
                        _p.score_bonus = int(_p.score_bonus * 1.6)
                        _p.name = f"{_p.name}_1D"
                    patterns = patterns + _pat_1d
            except Exception:
                pass
        # Дедупликация: убираем паттерны одного типа если уже есть более HTF версия
        _seen_base = set()
        _dedup = []
        for _p in sorted(patterns, key=lambda x: x.score_bonus, reverse=True):
            _base = _p.name.replace("_4H","").replace("_1D","").replace("_30M","")
            if _base not in _seen_base:
                _seen_base.add(_base)
                _dedup.append(_p)
        patterns = _dedup[:8]  # топ-8 паттернов

        # ✅ FIX P5: CASCADE → synthetic PatternResult
        # Когда CASCADE обнаружен (Fractal Raid + SNR + FVG) но patterns=0 → добавляем синтетический паттерн
        # Это исправляет RONINUSDT/MANTAUSDT где CASCADE обнаружен но Patterns=0
        _cas_p5 = getattr(md, "cascade_signal", None)
        if _cas_p5 is not None and _cas_p5.has_signal and _cas_p5.direction == "long":
            try:
                from core.pattern_detector import PatternResult as _PR
                # CASCADE = аналог LIQUIDITY_SWEEP_LONG по силе
                _cas_bonus = min(int(_cas_p5.score_bonus * 0.8), 20)
                _already_has_pattern = any(
                    p.name in ("LIQUIDITY_SWEEP_LONG", "BREAKOUT_LONG", "CASCADE_LONG") for p in patterns
                )
                if not _already_has_pattern:
                    patterns.append(_PR(
                        name="CASCADE_LONG",
                        score_bonus=_cas_bonus,
                        confidence=0.75,
                        direction="long",
                        reasons=[f"CASCADE: {_cas_p5.description[:60]}"],
                    ))
                    if verbose:
                        print(f"{log_prefix} 🎯 [CASCADE→PATTERN] CASCADE_LONG score={_cas_bonus} (было Patterns=0)")
            except Exception as _cas_p5_e:
                if verbose:
                    print(f"{log_prefix} ⚠️ [CASCADE→PATTERN] {_cas_p5_e}")

        # ✅ FIX #6: Wyckoff результат конвертируется в PatternResult для scorer
        # Ранее Wyckoff давал бонус только внутри signal_engine, но не попадал в
        # LongScorer.calculate_pattern_component() → ACCUMULATION всегда = 0
        if state.wyckoff_detector and ohlcv_15m:
            try:
                from core.pattern_detector import PatternResult
                wy_result = await state.wyckoff_detector.analyze(symbol, ohlcv_15m, md)
                wy_score  = wy_result.get("score", 0) if wy_result else 0
                wy_phase  = wy_result.get("phase", "unknown") if wy_result else "unknown"
                wy_event  = wy_result.get("event", "") if wy_result else ""
                if wy_score >= 40 and wy_phase in ("C", "D"):
                    # Фаза C (Spring) или D (SOS/LPS) → WYCKOFF_SPRING или ACCUMULATION
                    pat_name = "WYCKOFF_SPRING" if wy_event in ("SPRING", "LPS") else "ACCUMULATION"
                    bonus    = min(int(wy_score * 0.3), 22)  # макс 22 = ACCUMULATION strength
                    patterns.append(PatternResult(
                        name=pat_name,
                        score_bonus=bonus,
                        confidence=min(wy_score / 100, 0.85),
                        direction="long",
                        reasons=[f"Wyckoff phase={wy_phase} event={wy_event} score={wy_score:.0f}"],
                    ))
                    if verbose:
                        print(f"{log_prefix} ✅ [WYCKOFF→PATTERN] {pat_name} score={bonus} phase={wy_phase} event={wy_event}")
            except Exception as _wy_e:
                if verbose:
                    print(f"{log_prefix} ⚠️ [WYCKOFF→PATTERN] {_wy_e}")

        # ── P2: Flag / Pennant Detector ───────────────────────────────────────
        _fp_result = None
        try:
            if ohlcv_4h and len(ohlcv_4h) >= 12:
                from core.flag_pennant_detector import detect_flag_pennant
                from core.pattern_detector import PatternResult as _PR2
                _fp_result = detect_flag_pennant(ohlcv_4h, md.price, "long")
                if _fp_result and _fp_result.has_signal:
                    _fp_bonus = min(_fp_result.score_bonus, 20)
                    patterns.append(_PR2(
                        name=_fp_result.pattern_type,
                        score_bonus=_fp_bonus,
                        direction="long",
                        confidence=0.78 if _fp_result.is_breakout else 0.60,
                        reasons=[_fp_result.description],
                    ))
                    if verbose:
                        print(f"{log_prefix} 🚩 [FLAG/PENNANT] {_fp_result.description}")
        except Exception as _fp_err:
            pass  # не критично

        # OKX Liquidations fallback (WebSocket→Redis, реальные данные)
        if md.recent_liquidations_usd is None or md.liq_side is None:
            try:
                from utils.okx_liquidation_ws import get_okx_liq_from_redis
                okx_liq = get_okx_liq_from_redis(state.redis, symbol)
                if okx_liq:
                    md.recent_liquidations_usd = okx_liq["total_usd"]
                    md.liq_side = okx_liq.get("dominant_side")  # "LONG" | "SHORT" — совместимо со скорером
                    if verbose:
                        print(f"{log_prefix} 🔄 [OKX_LIQ] WS cache: {okx_liq.get('dominant_side')} ${okx_liq['total_usd']:.0f}")
            except Exception:
                pass

        # OKX OI cross-exchange — заполняет пробелы если Binance/Bybit вернули 0
        try:
            from api.okx_client import get_okx_client
            _okx_oi = await get_okx_client().get_open_interest(symbol)
            if _okx_oi:
                if not md.oi_change_1h:
                    md.oi_change_1h = _okx_oi.oi_change_1h
                if not getattr(md, 'oi_change_4h', 0.0):
                    md.oi_change_4h = _okx_oi.oi_change_4h
                if not md.funding_rate:
                    md.funding_rate = _okx_oi.funding_rate
        except Exception:
            pass

        # Multi-TF RSI и OI для scorer
        _rsi_15m = _calc_rsi_l(ohlcv_15m) if ohlcv_15m else None
        _oi_15m  = getattr(md, 'oi_change_15m', 0.0) or 0.0
        _oi_30m  = getattr(md, 'oi_change_30m', 0.0) or 0.0
        _oi_1h   = getattr(md, 'oi_change_1h', 0.0) or 0.0
        _oi_4h   = getattr(md, 'oi_change_4h', 0.0) or 0.0
        _p1h     = getattr(md, 'price_change_1h', 0.0) or 0.0
        _vol_sp  = getattr(md, 'volume_spike_ratio', 1.0) or 1.0
        # HTF structure и zone из market_structure
        _ms_s        = getattr(md, 'market_structure', None)
        _htf_str     = getattr(_ms_s, 'htf_structure', '') or ''
        _zone        = getattr(_ms_s, 'zone_4h', '') or ''
        _zone_weekly = getattr(_ms_s, 'zone_weekly',  '') or ''
        # 30M delta — вычисляем из уже загруженных 30m свечей (без доп. API вызова)
        _delta_30m = []
        if ohlcv_30m:
            for _c in ohlcv_30m[-14:]:
                _pdp = (_c.close - _c.open) / _c.open if _c.open > 0 else 0
                _delta_30m.append(_c.quote_volume * (1 if _pdp >= 0 else -1))
        # ── P1: Order Book Score ──────────────────────────────────────────────
        _ob_score = 0
        try:
            if Config.ENABLE_ORDERBOOK and state.auto_trader and hasattr(state.auto_trader, 'bingx') and state.auto_trader.bingx:
                # Fix: 109429 cooldown — пропускаем depth если BingX в rate limit
                _depth_cooldown = False
                if state.redis:
                    try:
                        _depth_cooldown = bool(state.redis.client.get('bingx:depth_cooldown'))
                    except Exception:
                        pass
                if not _depth_cooldown:
                    try:
                        # ✅ OPT: timeout 5s — BingX иногда висит 30s без ответа
                        _ob_data = await asyncio.wait_for(
                            state.auto_trader.bingx.get_order_book(symbol), timeout=5.0
                        )
                    except asyncio.TimeoutError:
                        _ob_data = None
                    if _ob_data:
                        from core.orderbook_scorer import calculate_orderbook_score
                        _ob_score, _ob_desc, _ = calculate_orderbook_score(_ob_data, md.price, "long")
                        if verbose and _ob_desc:
                            print(f"{log_prefix} {_ob_desc}")
                    else:
                        _bingx_err = getattr(state.auto_trader.bingx, 'last_error_code', None)
                        if _bingx_err == 109429:
                            # Rate limit — отключаем depth на 15 мин
                            if state.redis:
                                try:
                                    state.redis.client.setex('bingx:depth_cooldown', 900, '1')
                                except Exception:
                                    pass
                            print(f"⏸ [BingX] 109429 rate limit → depth отключён на 15 мин")
                        elif _bingx_err in (109418, 109425):
                            # Fix #1: BingX 109418 (offline) → постоянный блэклист
                            #         BingX 109425 (not exist) → skip 7 дней
                            try:
                                if state.redis:
                                    if _bingx_err == 109418:
                                        state.redis.client.set(f"blacklist:{symbol}", f"bingx:offline:{_bingx_err}")
                                        if hasattr(state, '_blacklist_set') and state._blacklist_set is not None:
                                            state._blacklist_set.add(symbol)
                                        print(f"🚫 [{symbol}] BingX 109418 offline → постоянный блэклист")
                                    else:
                                        state.redis.client.setex(f"skip:nodata:{symbol}", 86400 * 7, f"bingx:notexist:{_bingx_err}")
                                        if hasattr(state, '_skip_nodata_set') and state._skip_nodata_set is not None:
                                            state._skip_nodata_set.add(symbol)
                                        print(f"⏭ [{symbol}] BingX 109425 not exist → skip 7д")
                            except Exception:
                                pass
        except Exception as _ob_err:
            pass  # не критично

        # ── P3+P35: OnChain CoinGecko — параллельный запрос (было sequential → +8s/symbol) ──
        _onchain_bonus = 0
        _onchain_desc = ""
        _addr_bonus = 0
        _addr_desc = ""
        try:
            if os.getenv("ENABLE_ONCHAIN", "true").lower() == "true":
                from core.onchain_client import (get_volume_z_score, onchain_score_bonus,
                                                  get_active_addr_proxy, addr_proxy_score_bonus)
                _redis_cli = state.redis.client if state.redis else None
                # ✅ OPT: обе функции запускаем параллельно (раньше sequential: 2×8s → теперь max(8s))
                _oc_results = await asyncio.gather(
                    asyncio.wait_for(get_volume_z_score(symbol, _redis_cli), timeout=8.0),
                    asyncio.wait_for(get_active_addr_proxy(symbol, _redis_cli), timeout=8.0),
                    return_exceptions=True,
                )
                _oc_z, _oc_addr = _oc_results
                if not isinstance(_oc_z, Exception):
                    _z, _zdesc = _oc_z
                    _onchain_bonus, _onchain_desc = onchain_score_bonus(_z, "long")
                    if verbose and _zdesc:
                        print(f"{log_prefix} {_zdesc}")
                if not isinstance(_oc_addr, Exception):
                    _addr_pct, _addr_raw_desc = _oc_addr
                    _addr_bonus, _addr_desc = addr_proxy_score_bonus(_addr_pct, "long")
                    # Блок 2: при аномальном объёме (vol_chg > 200%) ADDR penalty отменяется —
                    # объём сам по себе перевешивает сигнал адресной активности
                    if _addr_bonus < 0:
                        _vol_chg_24h = getattr(md, "volume_change_24h", 0.0) or 0.0
                        if _vol_chg_24h > 200.0:
                            _addr_bonus = 0
                            _addr_desc  = f"⚡ [ADDR penalty отменён: vol_chg=+{_vol_chg_24h:.0f}%]"
                    if verbose and _addr_desc:
                        print(f"{log_prefix} 📊 [ADDR] {_addr_desc}")
        except Exception:
            pass  # не критично

        # Momentum Mode: RSI в зоне разгона + цена растёт + OI подтверждает + объём
        _momentum_mode = (
            55 <= (md.rsi_1h or 50) <= 72 and
            _p1h > 2.5 and
            _oi_1h > 1.0 and
            _vol_sp > 1.5
        )
        if _momentum_mode and verbose:
            print(f"{log_prefix} 🚀 [MOMENTUM MODE] RSI={md.rsi_1h:.0f} price1h=+{_p1h:.1f}% OI1h=+{_oi_1h:.1f}% vol={_vol_sp:.1f}x")

        # S10: Liquidation Zone магниты (Coinglass)
        _liq_analysis = None
        if state.liq_detector:
            try:
                _liq_analysis = await state.liq_detector.analyze_symbol(symbol, md.price)
            except Exception:
                pass

        base_result = state.scorer.calculate_score(
            rsi_1h=md.rsi_1h or 50,
            funding_current=md.funding_rate,
            funding_accumulated=md.funding_accumulated,
            long_ratio=md.long_short_ratio,
            price_change_24h=md.price_change_24h,
            hourly_deltas=hourly_deltas,
            price_trend=price_trend,
            patterns=patterns,
            volume_spike_ratio=getattr(md, "volume_spike_ratio", 1.0),
            atr_14_pct=getattr(md, "atr_14_pct", 0.5),
            price_change_1h=getattr(md, "price_change_1h", 0.0),
            top_trader_ratio=getattr(md, "top_trader_long_short_ratio", None),
            taker_ratio=getattr(md, "taker_buy_sell_ratio", None),
            btc_change_1h=state.btc_change_1h or 0.0,
            oi_15m=_oi_15m,
            oi_30m=_oi_30m,
            oi_1h=_oi_1h,
            oi_4h=_oi_4h,
            rsi_15m=_rsi_15m,
            rsi_30m=rsi_30m,
            rsi_4h=rsi_4h,
            htf_structure=_htf_str,
            zone=_zone,
            momentum_mode=_momentum_mode,
            delta_30m=_delta_30m,
            orderbook_score=_ob_score,
            liq_analysis=_liq_analysis,
        )

        # Fear & Greed макро-модификатор — применяем ДО проверки is_valid
        fg = state.fear_greed_index
        fg_modifier = 0
        fg_reason = ""
        if fg is not None:
            if fg < 20:
                fg_modifier, fg_reason = 2,  f"🧠 [F&G] {fg} Экстремальный страх → LONG +2"
            elif fg < 35:
                fg_modifier, fg_reason = 1,  f"🧠 [F&G] {fg} Страх → LONG +1"
            elif fg > 80:
                fg_modifier, fg_reason = -6, f"🧠 [F&G] {fg} Жадность → LONG -6"
            elif fg > 65:
                fg_modifier, fg_reason = -3, f"🧠 [F&G] {fg} Умеренная жадность → LONG -3"

        raw_score       = base_result.total_score
        # ✅ FIX P1: CASCADE учитывается ДО gate-проверки (раньше применялся после → RONIN/MANTA отсеивались)
        _cas_pre = getattr(md, "cascade_signal", None)
        _cas_pre_bonus = 0
        if _cas_pre is not None and _cas_pre.has_signal and _cas_pre.direction == "long":
            # Конвертируем CASCADE bonus → BASE_SCORER очки (60% от оригинала)
            if _cas_pre.score_bonus >= 14:   _cas_pre_bonus = 10
            elif _cas_pre.score_bonus >= 10: _cas_pre_bonus = 7
            elif _cas_pre.score_bonus >= 8:  _cas_pre_bonus = 5
            elif _cas_pre.score_bonus >= 6:  _cas_pre_bonus = 3
        effective_score = max(min(raw_score + fg_modifier + _mtf_bonus + _cas_pre_bonus + _onchain_bonus + _addr_bonus, 100), 0)
        min_score       = state.scorer.min_score

        if verbose:
            fg_str   = f" F&G={fg_modifier:+d}" if fg_modifier != 0 else ""
            mtf_str  = f" MTF={_mtf_bonus:+d}"  if _mtf_bonus  != 0 else ""
            cas_str  = f" CASCADE={_cas_pre_bonus:+d}" if _cas_pre_bonus > 0 else ""
            oc_str   = f" ONCHAIN={_onchain_bonus:+d}" if _onchain_bonus != 0 else ""
            addr_str = f" ADDR={_addr_bonus:+d}"       if _addr_bonus    != 0 else ""
            print(f"{log_prefix} 📊 [BASE_SCORER] score={raw_score}{fg_str}{mtf_str}{cas_str}{oc_str}{addr_str} → {effective_score} (min={min_score})"
                  f" | components: {[(c.name, c.score) for c in base_result.components]}")
            if _cas_pre_bonus > 0:
                print(f"{log_prefix} 🎯 [CASCADE PRE-GATE] +{_cas_pre_bonus}pts (bonus={_cas_pre.score_bonus}) → помогает пройти gate")
            if _onchain_desc:
                print(f"{log_prefix} 📊 [ONCHAIN] {_onchain_desc}")
            if base_result.funding_info:
                print(f"{log_prefix} 💰 {base_result.funding_info}")

        # ── Token Divergence Scorer: RS vs BTC + Volume + OI + Funding ─────────────
        try:
            from core.token_divergence_scorer import score_divergence as _score_div
            _div_bonus, _div_reasons = _score_div(md, cached_btc_1h or 0.0, cached_btc_24h, "long")
            if _div_bonus != 0:
                effective_score = max(0, min(100, effective_score + _div_bonus))
                if verbose:
                    _dsign = "+" if _div_bonus >= 0 else ""
                    _dreason = " | ".join(_div_reasons) if _div_reasons else ""
                    print(f"{log_prefix} 🧩 [DIVERGENCE] {_dsign}{_div_bonus} → {effective_score}"
                          + (f" | {_dreason}" if _dreason else ""))
        except Exception:
            pass

        if effective_score < min_score:
            # ✅ FIX P2: MOMENTUM PATH bypass — отдельный порог для momentum сделок
            # Если RSI растущий + volume spike + тренд → разрешаем даже при слабом mean-rev score
            _byp_rsi = getattr(md, "rsi_1h", 50) or 50
            _byp_vol = getattr(md, "volume_spike_ratio", 1.0) or 1.0
            _byp_p1h = getattr(md, "price_change_1h", 0) or 0
            _byp_p4h = getattr(md, "price_change_4h", 0) or 0
            _momentum_bypass = (
                Config.ENABLE_MOMENTUM_LONG
                and effective_score >= Config.MOMENTUM_SCORE_THRESHOLD
                and _byp_rsi >= Config.MOMENTUM_RSI_MIN
                and _byp_vol >= Config.MOMENTUM_VOL_MIN
                and (_byp_p1h > 0.3 or _byp_p4h > 1.0)
            )
            if _momentum_bypass:
                if verbose:
                    print(f"{log_prefix} 🚀 [MOMENTUM PATH] score={effective_score} >= threshold={Config.MOMENTUM_SCORE_THRESHOLD}"
                          f" | RSI={_byp_rsi:.0f} Vol×{_byp_vol:.1f} 1H={_byp_p1h:+.1f}% — bypass mean-rev gate")
            else:
                if verbose:
                    print(f"{log_prefix} ❌ [BASE_SCORER] is_valid=False — базовый скоринг отклонил")
                    if fg_reason: print(f"{log_prefix} {fg_reason}")
                return None

        if verbose and fg_reason:
            print(f"{log_prefix} {fg_reason}")

        price      = md.price
        base_score = effective_score

        # ══════════════════════════════════════════════════════════════════════
        # ANTI-CATASTROPHE HARD BLOCKS (LONG)
        # Не входить в LONG в недельных зонах продаж — так же как SHORT не
        # входит в weekly DISCOUNT. Это защита от покупки у институциональных
        # уровней сопротивления на PREMIUM неделях.
        # ══════════════════════════════════════════════════════════════════════
        _long_require_discount = os.getenv("LONG_REQUIRE_DISCOUNT", "true").lower() == "true"
        if _long_require_discount and _ms_s:
            # Блок 1: Weekly PREMIUM — цена выше недельного POC, умные деньги продают
            if "premium" in _zone_weekly.lower():
                _long_weekly_prem_bypass = float(os.getenv("LONG_WEEKLY_PREMIUM_BYPASS_SCORE", "88"))
                if effective_score < _long_weekly_prem_bypass:
                    if verbose:
                        print(
                            f"{log_prefix} 🚫 [WEEKLY PREMIUM BLOCK] zone_weekly={_zone_weekly!r} — "
                            f"цена в WEEKLY PREMIUM, умные деньги продают, LONG заблокирован"
                        )
                    return None

            # Блок 2: Цена внутри Weekly Bearish OB — мощная стена продаж сверху
            _long_block_bear_ob_1w = os.getenv("LONG_BLOCK_WEEKLY_BEAR_OB", "true").lower() == "true"
            if _long_block_bear_ob_1w:
                _ob_bear_1w = getattr(_ms_s, 'ob_bearish_1w', None)
                if _ob_bear_1w:
                    _ob_lo, _ob_hi = _ob_bear_1w
                    if _ob_lo > 0 and _ob_lo <= price <= _ob_hi * 1.02:
                        if verbose:
                            print(
                                f"{log_prefix} 🚫 [WEEKLY BEAR OB BLOCK] цена {price:.4f} внутри "
                                f"Bearish OB Weekly [{_ob_lo:.4f}–{_ob_hi:.4f}] — LONG заблокирован"
                            )
                        return None

        # ── Market Structure Bonus (HTF) ─────────────────────────────────────
        # PDH/PDL, Fib 0.618, OB/FVG 4H/1W, CRT, HTF structure, confluence
        _ms = getattr(md, "market_structure", None)
        if _ms is not None:
            try:
                from utils.market_structure import proximity_bonus
                _ms_bonus, _ms_reasons = proximity_bonus(price, _ms, "long")
                if _ms_bonus != 0:
                    base_score = max(0, min(100, base_score + _ms_bonus))
                    if verbose and _ms_reasons:
                        print(f"{log_prefix} 🏗 [MS] {' | '.join(_ms_reasons[:4])}")
            except Exception as _ms_e:
                pass  # MS bonus не критичен

        # ── FTA (First Touch Area) Bonus ─────────────────────────────────────
        # Первое касание Bullish OB/FVG (4H и Weekly) — самая сильная реакция.
        if _ms is not None and state.redis:
            try:
                from utils.fta_tracker import FTATracker
                _fta = FTATracker(state.redis.client, "long")
                _fta_total = 0
                _fta_parts = []

                # Bullish OB 4H
                if _ms.has_ob_4h and _ms.ob_bullish_4h:
                    _lo, _hi = _ms.ob_bullish_4h
                    if _lo * 0.98 <= price <= _hi:
                        _adj, _rsn = _fta.score_ob(symbol, _ms.ob_bullish_4h, "bullish", 10)
                        _fta_total += _adj; _fta_parts.append(_rsn)

                # Bullish FVG 4H
                if _ms.has_fvg_4h and _ms.fvg_bullish_4h:
                    _lo, _hi = _ms.fvg_bullish_4h
                    if _lo * 0.98 <= price <= _hi:
                        _adj, _rsn = _fta.score_fvg(symbol, _ms.fvg_bullish_4h, "bullish", 8)
                        _fta_total += _adj; _fta_parts.append(_rsn)

                # Bullish OB Weekly (критично для лонга!)
                if _ms.has_ob_1w and _ms.ob_bullish_1w:
                    _lo, _hi = _ms.ob_bullish_1w
                    if _lo * 0.95 <= price <= _hi:
                        _adj, _rsn = _fta.score_ob(symbol, _ms.ob_bullish_1w, "bullish", 15)
                        _fta_total += _adj; _fta_parts.append(_rsn)

                # Bullish FVG Weekly
                if _ms.has_fvg_1w and _ms.fvg_bullish_1w:
                    _lo, _hi = _ms.fvg_bullish_1w
                    if _lo * 0.95 <= price <= _hi:
                        _adj, _rsn = _fta.score_fvg(symbol, _ms.fvg_bullish_1w, "bullish", 12)
                        _fta_total += _adj; _fta_parts.append(_rsn)

                if _fta_total != 0:
                    base_score = max(0, min(100, base_score + _fta_total))
                    if verbose and _fta_parts:
                        print(f"{log_prefix} 🎯 [FTA] {_fta_total:+d} | {' | '.join(_fta_parts[:2])}")
            except Exception as _fta_e:
                logger.debug(f"[FTA LONG] {_fta_e}")

        # ── CASCADE SIGNAL Bonus (4H Fractal Raid → 1H SNR → 15M FVG) ──────
        _cas = getattr(md, "cascade_signal", None)
        if _cas is not None and _cas.has_signal and _cas.direction == "long":
            base_score = max(0, min(100, base_score + _cas.score_bonus))
            if verbose:
                print(f"{log_prefix} 🎯 [CASCADE LONG] +{_cas.score_bonus}: {_cas.description[:80]}")
        # 🆕 Консолидация фильтр — блокировка входов в середине диапазона
        # Управляется ENV CONSOLIDATION_FILTER_ENABLED (по умолч. true)
        _cons_filter_on = os.getenv("CONSOLIDATION_FILTER_ENABLED", "true").lower() == "true"
        if _cons_filter_on and state.consolidation_detector and ohlcv_15m:
            cons = state.consolidation_detector.detect(ohlcv_15m, price)
            rsi_1h_val = getattr(md, "rsi_1h", 50.0) or 50.0
            _htf_is_bullish = "bull" in _htf_str.lower() or "bullish" in _htf_str.lower()
            _htf_is_bearish = "bear" in _htf_str.lower() or "bearish" in _htf_str.lower()
            allow, reason = filter_mid_range(cons, price, "long", verbose=False, rsi_1h=rsi_1h_val,
                                             htf_bullish=_htf_is_bullish, htf_bearish=_htf_is_bearish)

            if cons.is_consolidating and not allow:
                # ✅ FIX P4: CONSOLIDATION softening — сильный сигнал + breakout override
                _cons_bypass = (
                    base_score >= 75
                    and (cons.has_breakout_up or cons.has_spring)
                )
                if _cons_bypass:
                    if verbose:
                        print(f"{log_prefix} 🟡 [CONSOLIDATION BYPASS] score={base_score:.0f}≥75 + breakout/spring → override {reason}")
                else:
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

            # A1: Multi-touch бонус за подтверждённые уровни S/R
            _touch_bonus = cons.get_touch_bonus("long")
            if _touch_bonus > 0:
                base_score += _touch_bonus
                if verbose:
                    print(f"{log_prefix} ✅ [MULTI-TOUCH] +{_touch_bonus} — support touches={cons.support_touches}")
        if verbose and (base_score != effective_score):
            print(f"{log_prefix} 📊 [POST_FILTERS] score={base_score:.1f} | reasons: {list(base_result.reasons)[:3]}")

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

        # ── PatternML: исторический win-rate бонус/штраф ─────────────────────
        if state.redis and patterns:
            try:
                from core.pattern_ml_scorer import get_pattern_ml_scorer
                _ml = get_pattern_ml_scorer(state.redis, "long")
                _ml_bonus, _ml_reason = _ml.get_bonus([p.name for p in patterns])
                if _ml_bonus != 0:
                    base_score = max(0, min(100, base_score + _ml_bonus))
                    if verbose:
                        print(f"{log_prefix} 🤖 [PatternML] {_ml_reason}")
            except Exception as _ml_e:
                pass  # ML scorer не критичен

        # ── ATR-dynamic SL (M1) ──────────────────────────────────────────
        # ✅ v2.1: ATR-based SL вместо фиксированного %
        # SL порядок приоритетов: BOS/CHoCH → SSL/BSL → Swing → VP POC → ATR → Fixed %
        entry_price = price

        # ── #30 BOS/CHoCH SL (рыночная структура, приоритет 1) ──────────────
        _swing_sl_used = False
        try:
            from core.smc_detector import calculate_bos_choch_sl
            if ohlcv_4h and len(ohlcv_4h) >= 20:
                _bos_sl, _bos_desc = calculate_bos_choch_sl(ohlcv_4h, price, "long")
                if _bos_sl is not None:
                    stop_loss = _bos_sl
                    _swing_sl_used = True
                    if verbose:
                        print(f"{log_prefix} 🎯 [BOS/CHoCH SL] {_bos_desc}")
        except Exception:
            pass

        # ── #32 SSL/BSL Liquidity SL (приоритет 2) ───────────────────────────
        if not _swing_sl_used:
            try:
                from core.smc_detector import calculate_ssl_bsl_sl
                if ohlcv_4h and len(ohlcv_4h) >= 15:
                    _ssl_sl, _ssl_desc = calculate_ssl_bsl_sl(ohlcv_4h, price, "long")
                    if _ssl_sl is not None:
                        stop_loss = _ssl_sl
                        _swing_sl_used = True
                        if verbose:
                            print(f"{log_prefix} 🎯 [SSL/BSL SL] {_ssl_desc}")
            except Exception:
                pass

        # ── #29 Swing High/Low SL (приоритет 3) ──────────────────────────────
        if not _swing_sl_used:
            try:
                from core.swing_sl import calculate_swing_sl
                if ohlcv_4h and len(ohlcv_4h) >= 10:
                    _sw_sl, _sw_desc = calculate_swing_sl(ohlcv_4h, price, "long")
                    if _sw_sl is not None:
                        stop_loss = _sw_sl
                        _swing_sl_used = True
                        if verbose:
                            print(f"{log_prefix} 🎯 [SWING SL] {_sw_desc}")
            except Exception:
                pass

        # ── #31 Volume Profile POC SL (приоритет 4) ──────────────────────────
        if not _swing_sl_used:
            try:
                from core.volume_profile import calculate_poc_sl
                if ohlcv_4h and len(ohlcv_4h) >= 20:
                    _vp_sl, _vp_desc = calculate_poc_sl(ohlcv_4h, price, "long")
                    if _vp_sl is not None:
                        stop_loss = _vp_sl
                        _swing_sl_used = True
                        if verbose:
                            print(f"{log_prefix} 🎯 {_vp_desc}")
            except Exception:
                pass

        # ── ATR SL (приоритет 2, только если Swing SL не нашёл уровень) ──────
        _atr_sl_used = False
        if not _swing_sl_used and Config.USE_ATR_SL and ohlcv_4h and len(ohlcv_4h) >= 14:
            try:
                # Вычисляем ATR(14) по 4H свечам
                _highs  = [c.high  for c in ohlcv_4h[-15:]]
                _lows   = [c.low   for c in ohlcv_4h[-15:]]
                _closes = [c.close for c in ohlcv_4h[-15:]]
                _trs = []
                for _i in range(1, len(_highs)):
                    _tr = max(_highs[_i] - _lows[_i],
                              abs(_highs[_i] - _closes[_i-1]),
                              abs(_lows[_i]  - _closes[_i-1]))
                    _trs.append(_tr)
                _atr = sum(_trs[-14:]) / 14 if len(_trs) >= 14 else sum(_trs) / len(_trs)
                _atr_sl = price - _atr * Config.ATR_SL_MULT
                _atr_sl_pct = (price - _atr_sl) / price * 100

                # Применяем только если в допустимом диапазоне [MIN%, MAX%]
                if Config.ATR_SL_MIN <= _atr_sl_pct <= Config.ATR_SL_MAX:
                    stop_loss = _atr_sl
                    _atr_sl_used = True
                    if verbose:
                        print(f"{log_prefix} 📐 [ATR SL] ATR={_atr:.6f} × {Config.ATR_SL_MULT} → SL={stop_loss:.6f} ({_atr_sl_pct:.2f}%)")
                else:
                    # ✅ FIX C7: была двойная запись — строка ниже перезаписывала ATR-clamped SL
                    _clamped_pct = max(Config.ATR_SL_MIN, min(Config.ATR_SL_MAX, _atr_sl_pct))
                    stop_loss = price * (1 - _clamped_pct / 100)
                    if verbose:
                        print(f"{log_prefix} 📐 [ATR SL] clamped: ATR-SL={_atr_sl_pct:.2f}% → clamped to {_clamped_pct:.2f}%")
            except Exception as _e:
                if verbose:
                    print(f"{log_prefix} ⚠️ [ATR SL] error: {_e} → fallback fixed %")
                stop_loss = price * (1 - Config.SL_BUFFER / 100)
        elif not _swing_sl_used:
            # Fallback fixed % только если ни Swing, ни ATR SL не сработали
            stop_loss = price * (1 - Config.SL_BUFFER / 100)

        # ── Momentum LONG detection (M2) ─────────────────────────────────
        # ✅ v2.1: если RSI растущий + volume spike + тренд вверх → MOMENTUM bonus
        _is_momentum = False
        if Config.ENABLE_MOMENTUM_LONG:
            _m_rsi   = getattr(md, "rsi_1h", 50)             or 50
            _m_vol   = getattr(md, "volume_spike_ratio", 1.0) or 1.0
            _m_p1h   = getattr(md, "price_change_1h", 0)      or 0
            _m_p4h   = getattr(md, "price_change_4h", 0)      or 0
            if (_m_rsi >= Config.MOMENTUM_RSI_MIN
                    and _m_vol >= Config.MOMENTUM_VOL_MIN
                    and (_m_p1h > 0.3 or _m_p4h > 1.0)):
                _is_momentum = True
                _mom_bonus = min(10, int(_m_vol * 3))
                base_score = min(100, base_score + _mom_bonus)
                if verbose:
                    print(f"{log_prefix} 🚀 [MOMENTUM] RSI={_m_rsi:.0f} Vol×{_m_vol:.1f} "
                          f"1H={_m_p1h:+.1f}% 4H={_m_p4h:+.1f}% → +{_mom_bonus} bonus")

        # ── #33/#34 Trend Following detector (bonus + counter-trend penalty) ──
        _trend_result = None
        try:
            from core.trend_detector import detect_trend
            _trend_result = detect_trend(
                candles_4h=ohlcv_4h,
                price_change_1h=getattr(md, "price_change_1h", 0.0) or 0.0,
                price_change_4h=getattr(md, "price_change_4h", 0.0) or 0.0,
                price_change_1d=getattr(md, "price_change_24h", 0.0) or 0.0,
                volume_spike_ratio=getattr(md, "volume_spike_ratio", 1.0) or 1.0,
                direction="long",
            )
            if _trend_result and _trend_result.has_trend:
                base_score = max(0, min(100, base_score + _trend_result.score_bonus))
                if verbose:
                    print(f"{log_prefix} {_trend_result.description}")
        except Exception as _tr_e:
            logger.debug(f"[TrendDetector] long: {_tr_e}")
            pass

        # #19: KillZoneFilter — бонус/штраф по времени сессии
        _kz_delta, _kz_reason = KillZoneFilter.get_adjustment()
        if _kz_delta != 0:
            base_score = max(0, min(100, base_score + _kz_delta))
            if verbose:
                print(f"{log_prefix} 🕐 [KILLZONE] {_kz_reason} → base_score={base_score:.1f}")

        # #21: Delta Divergence — бычья дивергенция = +18 к base_score для LONG
        if state.delta_analyzer and ohlcv_15m:
            try:
                _div = state.delta_analyzer.detect_divergence(ohlcv_15m, lookback=20)
                if _div["bullish"] and _div["score_bonus"] > 0:
                    base_score = max(0, min(100, base_score + _div["score_bonus"]))
                    if verbose:
                        print(f"{log_prefix} {_div['reason']}")
            except Exception as _div_e:
                logger.debug(f"[DeltaDiv] long: {_div_e}")

        # M2: Volume Profile HVN/LVN scorer
        _vpa_poc = None
        if ohlcv_4h and len(ohlcv_4h) >= 20:
            try:
                from core.volume_profile import VolumeProfileAnalyzer
                _vpa = VolumeProfileAnalyzer(ohlcv_4h)
                _vpa_poc = _vpa.poc
                _vp_bonus, _vp_reason = _vpa.score_bonus(price, "long")
                if _vp_bonus > 0:
                    base_score = max(0, min(100, base_score + _vp_bonus))
                    if verbose:
                        print(f"{log_prefix} {_vp_reason}")
            except Exception as _vpa_e:
                logger.debug(f"[VPA] long: {_vpa_e}")

        # M4: Confluence Scoring — cross-TF S/R подтверждение
        try:
            from core.confluence_scorer import build_confluence_scorer
            _cs = build_confluence_scorer(
                price=price,
                ohlcv_15m=ohlcv_15m,
                ohlcv_1h=ohlcv_30m,   # 30m как средний ТФ
                ohlcv_4h=ohlcv_4h,
                poc_4h=_vpa_poc,
            )
            _cf_bonus, _cf_reason = _cs.score_bonus(price, "long")
            if _cf_bonus > 0:
                base_score = max(0, min(100, base_score + _cf_bonus))
                if verbose:
                    print(f"{log_prefix} {_cf_reason}")
        except Exception as _cf_e:
            logger.debug(f"[Confluence] long: {_cf_e}")

        # M1 + M5/M7: S/R кластеризация (общий инстанс для M1, M5, M7)
        _src_shared = None
        if ohlcv_4h and len(ohlcv_4h) >= 15:
            try:
                from core.sr_cluster import SRCluster
                _src_shared = SRCluster(ohlcv_4h)
                _src_bonus, _src_reason = _src_shared.score_bonus(price, "long")
                if _src_bonus > 0:
                    base_score = max(0, min(100, base_score + _src_bonus))
                    if verbose:
                        print(f"{log_prefix} {_src_reason}")
            except Exception as _src_e:
                logger.debug(f"[SRCluster] long: {_src_e}")

        # M3: Weekly/Monthly HTF level scorer
        _ms_htf = getattr(md, "market_structure", None)
        if _ms_htf and getattr(_ms_htf, "has_1d", False):
            try:
                from core.htf_level_scorer import htf_level_score_bonus
                _htf_bonus, _htf_reason = htf_level_score_bonus(price, "long", _ms_htf)
                if _htf_bonus > 0:
                    base_score = max(0, min(100, base_score + _htf_bonus))
                    if verbose:
                        print(f"{log_prefix} {_htf_reason}")
            except Exception as _htf_e:
                logger.debug(f"[HTFLevels] long: {_htf_e}")

        # M5/M7: False Breakout + Absorption (переиспользуем _src_shared из M1)
        if ohlcv_15m and len(ohlcv_15m) >= 3:
            try:
                from core.false_breakout_detector import detect_false_breakout_from_sr
                _fb_bonus, _fb_reason = detect_false_breakout_from_sr(
                    ohlcv_15m, price, "long", sr_cluster=_src_shared
                )
                if _fb_bonus > 0:
                    base_score = max(0, min(100, base_score + _fb_bonus))
                    if verbose:
                        print(f"{log_prefix} {_fb_reason}")

                from core.absorption_detector import detect_absorption_from_sr
                _ab_bonus, _ab_reason = detect_absorption_from_sr(
                    ohlcv_15m, price, "long", sr_cluster=_src_shared
                )
                if _ab_bonus > 0:
                    base_score = max(0, min(100, base_score + _ab_bonus))
                    if verbose:
                        print(f"{log_prefix} {_ab_reason}")

            except Exception as _m57_e:
                logger.debug(f"[M5/M7] long: {_m57_e}")

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
                if smc.has_ob and smc.refined_sl and smc.refined_sl < price:
                    stop_loss = smc.refined_sl
                    if verbose:
                        print(f"{log_prefix} 🎯 [SMC] SL refined: {stop_loss:.4f}")
                if smc.ob_entry:
                    entry_price = smc.ob_entry
                smc_data = {"has_ob": smc.has_ob, "has_fvg": smc.has_fvg, "bonus": smc.score_bonus}
            except Exception as e:
                if verbose:
                    print(f"{log_prefix} ⚠️ [SMC] error: {e}")

        # Hard minimum SL — абсолютный минимум от ВХОДА (entry_price)
        # Предотвращает случаи когда Swing/SMC ставит SL слишком близко (как XNYUSDT 0.65%)
        _hard_sl_min_pct = float(os.getenv("LONG_SL_HARD_MIN_PCT", "2.0"))
        _sl_from_entry = (entry_price - stop_loss) / entry_price * 100 if entry_price > 0 else 0
        if _sl_from_entry < _hard_sl_min_pct:
            stop_loss = entry_price * (1 - _hard_sl_min_pct / 100)
            if verbose:
                print(f"{log_prefix} 🛡 [SL MIN] SL слишком близко ({_sl_from_entry:.2f}% < {_hard_sl_min_pct}%) → принудительно {_hard_sl_min_pct}%")

        sl_pct = round((price - stop_loss) / price * 100, 2)
        if sl_pct < Config.SL_BUFFER:
            stop_loss = price * (1 - Config.SL_BUFFER / 100)
            sl_pct    = Config.SL_BUFFER

        # ── Dynamic TP (выше входа для Long) ─────────────────────────
        # Extended TP: 6 уровней для трендовых паттернов при EXTENDED_TP_LONG=true
        import os as _os
        _env_ext_long   = _os.getenv("EXTENDED_TP_LONG", "false").lower() == "true"
        _pat_name_base  = (patterns[0].name if patterns else "").replace("_30M","").replace("_1D","")
        _pat_is_trend   = _pat_name_base in Config.EXTENDED_TP_PATTERNS
        # P2: Flag/Pennant → extended TP
        _fp_extend_tp   = _fp_result is not None and getattr(_fp_result, 'extend_tp', False)
        # #33 Trend Following → extended TP
        _trend_ext_tp   = _trend_result is not None and getattr(_trend_result, 'extend_tp', False)
        _use_ext_tp     = (_env_ext_long and _pat_is_trend) or _fp_extend_tp or _trend_ext_tp
        _tp_count       = 6 if _use_ext_tp else 4
        _tp_weights     = Config.TP_WEIGHTS_6 if _use_ext_tp else Config.TP_WEIGHTS_4
        if _use_ext_tp and verbose:
            print(f"{log_prefix} 🎯 [EXTENDED TP] 6 уровней для {_pat_name_base}")

        take_profits = []
        if state.dca_engine:
            atr_val = state.dca_engine.calculate_atr(ohlcv_15m)
            tps     = state.dca_engine.calculate_tp_levels(
                entry_price=entry_price, sl_price=stop_loss,
                num_tps=_tp_count, funding_rate=md.funding_rate, atr=atr_val,
            )
            take_profits = tps
        else:
            for i, tp_pct in enumerate(Config.TP_LEVELS[:_tp_count]):
                tp_price = price * (1 + tp_pct / 100)
                take_profits.append((round(tp_price, 8), _tp_weights[i]))

        # ── SL COOLDOWN CHECK: блок повторного входа после стопа ─────
        _sl_cd_h = float(os.getenv("SL_COOLDOWN_HOURS", "1.0"))
        _sl_cd_key = f"sl_cooldown:long:{symbol.replace('-', '')}"
        try:
            if state.redis and state.redis._client.exists(_sl_cd_key):
                if verbose:
                    print(f"{log_prefix} 🚫 [SL_COOLDOWN] {symbol}: стоп был недавно — ждём {_sl_cd_h}ч")
                return None
        except Exception:
            pass

        # ── BTCMomentumGuard штраф к base_score при Rapid Dump ──────
        if _btc_mg_long_mult < 1.0:
            base_score = max(0, int(base_score * _btc_mg_long_mult))
            if verbose:
                print(f"{log_prefix} ⚠️ [BTC_RAPID_DUMP] base_score ×{_btc_mg_long_mult} → {base_score}")

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
                    btc_change_1h=float(cached_btc_1h or 0.0),
                )
                if aegis_signal:
                    final_score      = aegis_signal.total_score
                    aegis_components = {k: round(v.raw_score, 1)
                                        for k, v in aegis_signal.components.items()}
                    if verbose:
                        print(f"{log_prefix} ✅ [AEGIS] score={final_score:.1f} | components: {aegis_components}")
                else:
                    if verbose:
                        print(f"{log_prefix} ❌ [AEGIS] сигнал отклонён (pre_aegis_score={base_score:.1f})"
                              f" — z_gate мог быть bypassed, но AEGIS internal score ниже порога")
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
                # FIX: real win_rate from risk_manager history (was default 0.60)
                _rm_stats = state.risk_manager.get_win_stats()
                _real_wr  = _rm_stats.get("win_rate", 0.0) if _rm_stats.get("total_trades", 0) >= 10 else 0.55
                _real_awp = _rm_stats.get("avg_win_pct", 5.0) or 5.0
                risk_result = state.risk_manager.calculate_position_size(
                    win_rate=_real_wr, avg_win_pct=_real_awp,
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

        # ── TradeManager: оптимизация SL/TP по liquidation magnets ──
        _liq_opt_reasons = []
        if state.trade_manager and _liq_analysis:
            try:
                _tp1_price = take_profits[0][0] if take_profits else entry_price * 1.03
                stop_loss, _tp1_price, _liq_opt_reasons = state.trade_manager.optimize_levels_with_liquidation(
                    direction="LONG", entry_price=entry_price,
                    default_sl=stop_loss, default_tp=_tp1_price, liq_analysis=_liq_analysis,
                )
                if _liq_opt_reasons:
                    print(f"{log_prefix} 🧲 [LIQ-OPT] {' | '.join(_liq_opt_reasons)}")
            except Exception as _loe:
                print(f"[TradeManager] liq_opt error {symbol}: {_loe}")

        reasons = list(base_result.reasons)
        reasons.extend(rt_result.factors)
        if aegis_signal:
            reasons.extend(aegis_signal.reasons[:6])
        reasons.extend(_liq_opt_reasons)

        ret_dict = {
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
                "OI 15m":    f"{getattr(md,'oi_change_15m',0.0):+.1f}%",
                "OI 1h":    f"{getattr(md,'oi_change_1h',0.0):+.1f}%",
                "OI 4h":    f"{getattr(md,'oi_change_4h',0.0):+.1f}%",
                "Price 24h": f"{md.price_change_24h:+.1f}%",
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
            "oi_change":        round(getattr(md, 'oi_change_1h', 0.0), 2),
            "long_short_ratio": round(md.long_short_ratio, 1),
            "volume_spike_ratio": round(getattr(md, "volume_spike_ratio", 1.0), 2),
            "atr_14_pct":       round(getattr(md, "atr_14_pct", 0.5), 3),
            "pattern":          patterns[0].name if patterns else "",
            "timestamp":        datetime.utcnow().isoformat(),
            "status":           "active",
            "taken_tps":        [],
            # ATR-based leverage cap: высокая волатильность → меньше плечо
            # atr_14_pct < 2%: норм | 2-4%: cap 15x | 4-6%: cap 10x | >6%: cap 5x
            "leverage": (
                lambda _atr, _base: (
                    f"{int(str(_base).split('-')[0])}-5"  if _atr >= 6.0 else
                    f"{int(str(_base).split('-')[0])}-10" if _atr >= 4.0 else
                    f"{int(str(_base).split('-')[0])}-15" if _atr >= 2.0 else
                    str(_base)
                )
            )(round(getattr(md, "atr_14_pct", 0.5), 3), Config.LEVERAGE),
            # MS-данные для дашборда (pivot, PDH/PDL, CME gap)
            "ms_pivot_pp":  round(getattr(_ms_data, "pivot_pp",  0) or 0, 8) if _ms_data else 0,
            "ms_pivot_r1":  round(getattr(_ms_data, "pivot_r1",  0) or 0, 8) if _ms_data else 0,
            "ms_pivot_s1":  round(getattr(_ms_data, "pivot_s1",  0) or 0, 8) if _ms_data else 0,
            "ms_pdh":       round(getattr(_ms_data, "pdh",        0) or 0, 8) if _ms_data else 0,
            "ms_pdl":       round(getattr(_ms_data, "pdl",        0) or 0, 8) if _ms_data else 0,
            "ms_cme_gap_pct":   round(getattr(_ms_data, "cme_gap_pct",  0) or 0, 3) if _ms_data else 0,
            "ms_cme_gap_dir":   getattr(_ms_data, "cme_gap_dir", "none") if _ms_data else "none",
            "ms_cme_gap_low":   round(getattr(_ms_data, "cme_gap_low",  0) or 0, 8) if _ms_data else 0,
            "ms_cme_gap_high":  round(getattr(_ms_data, "cme_gap_high", 0) or 0, 8) if _ms_data else 0,
            "ms_has_cme_gap":   bool(getattr(_ms_data, "has_cme_gap",   False)) if _ms_data else False,
            "ms_zone_4h":       getattr(_ms_data, "zone_4h", "neutral") if _ms_data else "neutral",
            "ms_htf_structure": getattr(_ms_data, "htf_structure", "unknown") if _ms_data else "unknown",
            # Block 5: risk size multiplier based on BTC market context
            "pos_multiplier":   state.crash_guard.get_position_multiplier() if hasattr(state, "crash_guard") else 1.0,
        }

        if verbose:
            print(f"🟢 [SIGNAL-LONG] {symbol}: score={final_score:.1f} — сигнал создан!")

        # signals_db: сохраняем сигнал для аналитики
        if state.signals_db:
            try:
                from database.signals_db import SignalRecord
                _dt = __import__("datetime").datetime
                _sid = state.signals_db.save_signal(SignalRecord(
                    id=None,
                    timestamp=_dt.utcnow(),
                    symbol=symbol,
                    direction="long",
                    timeframe="15m",
                    score=int(final_score),
                    confidence=final_score / 100,
                    entry_price=ret_dict.get("entry_price", 0),
                    stop_loss=ret_dict.get("stop_loss", 0),
                    take_profit=ret_dict.get("take_profit_1", 0),
                    reasons=", ".join(reasons[:8]),
                    extra={},
                ))
                state._signal_db_map[symbol] = _sid
            except Exception as _sde:
                print(f"[signals_db] save error {symbol}: {_sde}")

        return ret_dict

    except Exception as e:
        print(f"Error scanning {symbol}: {e}")
        return None


async def scan_market():
    if state.is_paused:
        return

    # ✅ FIX: Prevent concurrent/duplicate scans (process-level guard)
    if getattr(state, '_scan_running', False):
        print(f"⏳ [{Config.BOT_TYPE}] Scan already running. Skipping duplicate.")
        return
    state._scan_running = True

    # ✅ FIX: Redis distributed lock (cross-restart protection)
    lock_key = f"scan_lock:{Config.BOT_TYPE}"
    _lock_held = False
    try:
        _lock_held = bool(state.redis.client.set(lock_key, 1, nx=True, ex=300))
        if not _lock_held:
            print(f"⏳ [{Config.BOT_TYPE}] Redis scan lock held. Skipping.")
            state._scan_running = False
            return
    except Exception as e:
        print(f"⚠️ Redis lock error: {e} — proceeding without distributed lock")

    # 🧠 MEM-MONITOR: показываем RAM в начале каждого цикла
    try:
        import resource
        _mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        print(f"\n🔍 {Config.BOT_NAME} scan at {datetime.utcnow().strftime('%H:%M:%S UTC')} | 🧠 RAM: {_mem_mb:.0f}MB")
    except Exception:
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
            # ✅ FIX: Release locks on early return
            state._scan_running = False
            if _lock_held:
                try: state.redis.client.delete(lock_key)
                except Exception: pass
            return

    # BTC data (главный фильтр для Long)
    _btc_cache_1h: Optional[float] = await _get_btc_change()
    _btc_cache_4h: Optional[float] = await _get_btc_change_4h()
    _btc_cache_24h: float           = await _get_btc_change_24h()
    # ✅ BTC 4H тренд-блок: не открываем лонги если BTC в даунтренде на 4H
    if _btc_cache_4h is not None and Config.ENABLE_BTC_FILTER:
        if _btc_cache_4h <= Config.BTC_4H_BLOCK:
            print(f"🔴 [BTC_4H_FILTER] BTC 4H {_btc_cache_4h:.1f}% <= {Config.BTC_4H_BLOCK}% — блокируем ВСЕ LONG на этот скан")
            _btc_cache_1h = Config.BTC_BLOCK_THRESHOLD - 1  # Force block 1H filter too
        else:
            print(f"📊 [BTC_4H_FILTER] BTC 4H {_btc_cache_4h:+.1f}% — OK для LONG")
    # ✅ FIX #5: сохраняем в state для delta scorer
    state.btc_change_1h = _btc_cache_1h
    # BTCMomentumGuard: обновляем один раз за цикл
    state.btc_momentum_guard.update(_btc_cache_1h or 0.0)
    if state.btc_momentum_guard.is_vshape_active:
        print(f"⚠️ [BTC_MOMENTUM] Rapid Dump активен: {state.btc_momentum_guard.reason}")

    # ✅ OPT: Batch-загрузка blacklist + skip:nodata в Python sets (2 Redis команды вместо ~1000)
    try:
        if state.redis:
            _bl_keys = state.redis.client.keys("blacklist:*")
            state._blacklist_set = {k.replace("blacklist:", "") for k in _bl_keys}
            _sk_keys = state.redis.client.keys("skip:nodata:*")
            state._skip_nodata_set = {k.replace("skip:nodata:", "") for k in _sk_keys}
    except Exception:
        state._blacklist_set = None
        state._skip_nodata_set = None

    # ✅ OPT v18: Batch-загрузка тикеров
    try:
        await state.binance._fetch_ticker_batch()
    except Exception:
        pass

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

    # ── ADAPTIVE MIN_SCORE ───────────────────────────────────────────────────
    # ✅ FIX P3: Adaptive base = MIN_LONG_BASE_SCORE (было Config.MIN_SCORE=52 → игнорировало ENV)
    # Adaptive ceiling = base + ADAPTIVE_MAX_BOOST (было хардкод 78, теперь base+3 макс)
    _base_aegis = int(os.getenv("MIN_LONG_BASE_SCORE", "58"))  # читаем напрямую, не через Config
    _adaptive_score = _base_aegis
    _fg = state.fear_greed_index
    if _fg is not None:
        if _fg < 20:   _adaptive_score -= 5  # экстремальный страх → покупаем
        elif _fg < 35: _adaptive_score -= 2
        elif _fg > 75: _adaptive_score += Config.ADAPTIVE_MAX_BOOST   # жадность → осторожно с лонгами
        elif _fg > 65: _adaptive_score += max(1, Config.ADAPTIVE_MAX_BOOST - 2)
    if _btc_cache_1h is not None:
        if _btc_cache_1h < -3.0: _adaptive_score += 2
        elif _btc_cache_1h > 2.0: _adaptive_score -= 2
    # Clamp: не выше base+ADAPTIVE_MAX_BOOST, не ниже base-5
    _adaptive_score = max(_base_aegis - 5, min(_base_aegis + Config.ADAPTIVE_MAX_BOOST, _adaptive_score))
    if _adaptive_score != _base_aegis:
        print(f"🎯 [ADAPTIVE] LONG min: {_base_aegis} → {_adaptive_score} (F&G={_fg}, BTC={_btc_cache_1h})")
    state._adaptive_min_score = _adaptive_score

    active_count  = await _count_long_positions()
    exchange_full = active_count >= Config.MAX_POSITIONS
    if exchange_full:
        print(f"📊 Exchange: {active_count}/{Config.MAX_POSITIONS} LONG slots — TG-only mode")

    # A2: SystemicCrashGuard — сбрасываем счётчики перед сканом, BTC обновляем
    state.crash_guard.reset_cycle()
    state.crash_guard.update_btc(_btc_cache_1h or 0.0)
    # NOTE: evaluate() вызывается ПОСЛЕ prefetch — чтобы собрать update_symbol() по всем символам

    # A5: сбрасываем OHLCV кеш перед новым циклом скана
    if state.ohlcv_cache:
        state.ohlcv_cache.cycle_reset()

    new_signals = tg_only_count = 0

    # ✅ FIX БАГ 5: Параллельный pre-fetch
    _SCAN_SEM = asyncio.Semaphore(int(os.getenv("SCAN_CONCURRENCY", "12")))  # ✅ FIX v17: 8→12
    _FRESH = object()

    async def _prefetch(sym: str):
        async with _SCAN_SEM:
            if hasattr(state, "_adaptive_min_score"): state.scorer.min_score = state._adaptive_min_score
            try:
                if _is_fresh(state.redis.get_signals(Config.BOT_TYPE, sym, limit=1)):
                    return sym, _FRESH
                sig = await scan_symbol(sym, _btc_cache_1h, cached_btc_24h=_btc_cache_24h)
                return sym, sig
            except Exception as _pfe:
                print(f"⚠️ Prefetch {sym}: {_pfe}")
                return sym, None

    _t0 = datetime.utcnow()
    _prefetch_results = await asyncio.gather(*[_prefetch(s) for s in state.watchlist])
    _dt = (datetime.utcnow() - _t0).total_seconds()
    print(f"⚡ Parallel fetch: {len(state.watchlist)} symbols in {_dt:.1f}s")
    _prefetch_map = dict(_prefetch_results)

    # A2: FIX — evaluate() ПОСЛЕ prefetch: теперь _cycle_neg/_cycle_total заполнены
    state.crash_guard.evaluate()
    if state.crash_guard.is_crash():
        print(f"🆘 [SYSTEMIC_CRASH] {state.crash_guard.reason} — LONG сигналы заблокированы до восстановления")

    # ── EMERGENCY SL TIGHTEN: при первичном обнаружении краша → BE для открытых LONG ──
    if state.crash_guard.was_newly_detected():
        asyncio.create_task(_emergency_crash_tighten_sl())

    for symbol in state.watchlist:
        try:
            _fetched = _prefetch_map.get(symbol)
            if _fetched is _FRESH:
                continue
            signal = _fetched
            if not signal:
                continue

            # BTC correlation adj
            signal["score"] = round(signal["score"] + btc_adj, 1)
            # ✅ FIX: Cap score at 100.0 (prevents overflow display like 110.5)
            signal["score"] = min(signal["score"], 100.0)
            if signal["score"] < Config.MIN_SCORE:
                continue

            # ✅ FIX: RR pre-check BEFORE Telegram — don't alert on signals we won't trade
            _MIN_RR = float(os.getenv("MIN_RR_RATIO", "1.0"))  # ✅ FIX: 1.2 было слишком жёстким → 1.0  # ✅ FIX: was hardcoded 1.0 → ENV MIN_RR_RATIO=1.2
            _tp_list = signal.get("take_profits", [])
            if _tp_list:
                _tp1_raw = _tp_list[0]
                try:
                    if isinstance(_tp1_raw, (list, tuple)):   _tp1 = float(_tp1_raw[0])
                    elif isinstance(_tp1_raw, dict):          _tp1 = float(_tp1_raw.get("price", 0))
                    else:                                      _tp1 = float(_tp1_raw)
                    _sl_dist  = abs(signal["entry_price"] - signal["stop_loss"])
                    _tp1_dist = abs(_tp1 - signal["entry_price"])
                    _rr = _tp1_dist / _sl_dist if _sl_dist > 0 else 0
                    if _rr < _MIN_RR:
                        print(f"⏸ [{signal['symbol']}][LONG] RR={_rr:.2f} < {_MIN_RR} — pre-filtered before Telegram")
                        continue
                except Exception as _rr_err:
                    print(f"⚠️ RR pre-check error {signal['symbol']}: {_rr_err}")

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

            # Aegis компоненты в TG — человекочитаемый формат
            if signal.get("aegis_components"):
                comps = signal["aegis_components"]
                grade    = signal.get("grade", "N/A")
                strength = signal.get("strength", "N/A")
                grade_emoji = {"A+": "💎", "A": "🥇", "B": "🥈", "C": "🥉", "D": "⚠️"}.get(grade, "📊")
                strength_ru = {
                    "ULTRA": "🚀 ЭКСТРЕМАЛЬНЫЙ", "STRONG": "🟢 СИЛЬНЫЙ",
                    "MODERATE": "🟡 УМЕРЕННЫЙ", "WATCH": "👀 СЛАБЫЙ", "NOISE": "🔕 ШУМ"
                }.get(str(strength), str(strength))

                def bar(v): return "▓" * int(v/10) + "░" * (10 - int(v/10))

                comp_names = {
                    "z_volume":     ("📊 Объём/Z-скор", "дамп ниже VWAP → отскок вверх"),
                    "oi_change":    ("📈 OI + L/S",     "шорты закрываются → сквиз"),
                    "funding_rate": ("💸 Фандинг",      "шорты переплачивают → лонг-сквиз"),
                    "smc_structure":("🏗 Структура",    "Spring / OB / CHoCH по SMC"),
                    "delta_flow":   ("⚡ Дельта",       "агрессивные покупки"),
                    "rsi_aux":      ("📉 RSI aux",      "RSI вспомогательный"),
                }
                lines = ""
                for k, v in comps.items():
                    name, desc = comp_names.get(k, (k, ""))
                    score_val = int(v)
                    lines += f"  {name}: <b>{score_val}</b>/100  {bar(score_val)}\n"
                    lines += f"    <i>{desc}</i>\n"

                demo_flag = " [DEMO]" if Config.BINGX_DEMO else " [REAL]"
                if not Config.AUTO_TRADING:
                    auto_status = "📋 Только уведомление"
                elif exchange_full:
                    auto_status = f"📊 TG-уведомление (биржа заполнена)"
                elif state.is_paused:
                    auto_status = "⏸ Бот на паузе"
                else:
                    auto_status = f"⏳ Открываем на BingX{demo_flag}..."

                await state.telegram.send_message(
                    f"{grade_emoji} <b>Aegis-анализ LONG — {signal['symbol']}</b>\n"
                    f"Оценка: <b>{grade}</b> | Сила: {strength_ru}\n\n"
                    f"{lines}\n"
                    f"<i>ℹ️ Aegis-движок ищет капитуляцию продавцов + отрицательный фандинг + OI-сквиз.\n"
                    f"Высокий компонент = сильное совпадение с условиями лонг-сквиза.</i>\n"
                    f"🔄 Статус: {auto_status}"
                )

            state.redis.save_signal(Config.BOT_TYPE, symbol, signal)

            trade_result = None
            if not exchange_full and Config.AUTO_TRADING and not state.is_paused:
                if state.auto_trader:
                    try:
                        trade_result = await state.auto_trader.execute_signal(signal)
                        if trade_result:
                            active_count += 1
                            exchange_full = active_count >= Config.MAX_POSITIONS
                            # signals_db: mark signal as executed
                            if state.signals_db:
                                try:
                                    _sid = state._signal_db_map.get(symbol)
                                    _ep = float(trade_result.get("entry_price", 0)) if isinstance(trade_result, dict) else 0.0
                                    if _sid and _ep:
                                        state.signals_db.mark_executed(_sid, _ep)
                                except Exception:
                                    pass
                            # TradeManager: запись открытой позиции для TP-статистики
                            if state.trade_manager:
                                try:
                                    _lev = int(str(Config.LEVERAGE).split("-")[0])
                                    _qty = float(trade_result.get("qty", 0)) if isinstance(trade_result, dict) else 0.0
                                    state.trade_manager.create_position(
                                        symbol=symbol, direction="LONG",
                                        entry_price=signal.get("entry_price", signal.get("price", 0)),
                                        qty=_qty, stop_loss=signal.get("stop_loss", 0), leverage=_lev,
                                    )
                                except Exception as _tme:
                                    print(f"[TradeManager] create_position {symbol}: {_tme}")
                    except Exception as e:
                        print(f"AutoTrader error {symbol}: {e}")
                        # SignalQueue: повторная попытка с exponential backoff (max 3)
                        if state.signal_queue:
                            try:
                                _tps = signal.get("take_profits", [])
                                state.signal_queue.add_from_detection(
                                    symbol=symbol, direction="LONG",
                                    score=int(signal.get("score", 0)),
                                    price=float(signal.get("price", 0)),
                                    pattern=signal.get("pattern", ""),
                                    indicators=signal,
                                    entry=signal.get("entry_price", 0),
                                    stop_loss=signal.get("stop_loss", 0),
                                    take_profits=_tps if isinstance(_tps, list) else [],
                                    leverage=str(Config.LEVERAGE), risk="Kelly-sized",
                                )
                                print(f"[SignalQueue] {symbol} LONG → очередь retry")
                            except Exception as _qe:
                                print(f"[SignalQueue] queue error {symbol}: {_qe}")
                new_signals += 1
            else:
                tg_only_count += 1

            # ── Итоговый статус после попытки открытия ────────────────────────
            if signal.get("aegis_components"):
                demo_flag = " [DEMO]" if Config.BINGX_DEMO else " [REAL]"
                if exchange_full and not trade_result:
                    final_status = f"📊 Виртуальная (биржа заполнена {active_count}/{Config.MAX_POSITIONS})"
                    if state.redis:
                        saved = state.redis.save_virtual_position(Config.BOT_TYPE, symbol, signal)
                        if saved:
                            print(f"✅ Virtual position saved (exchange full): {symbol}")
                elif trade_result:
                    exchange_label = "BingX DEMO" if Config.BINGX_DEMO else "BingX REAL"
                    final_status = f"✅ Открыта на {exchange_label}"
                elif not Config.AUTO_TRADING or state.is_paused or state.auto_trader is None:
                    final_status = f"✅ Позиция открыта (ВИРТУАЛЬНО) без исполнения на бирже BingX{demo_flag}"
                    if state.redis:
                        saved = state.redis.save_virtual_position(Config.BOT_TYPE, symbol, signal)
                        if saved:
                            print(f"✅ Virtual position saved: {symbol}")
                else:
                    final_status = "⚠️ Виртуальная сделка (ошибка биржи или RR)"
                await state.telegram.send_message(
                    f"🔄 <b>#{symbol}</b> — итог: {final_status}"
                )

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

    # ✅ FIX: Release distributed lock
    state._scan_running = False
    if _lock_held:
        try:
            state.redis.client.delete(lock_key)
        except Exception:
            pass

    # 🧹 MEM-FIX: явный GC после каждого цикла — освобождаем OHLCV/паттерны/asyncio объекты
    _gc_collected = gc.collect()
    print(f"🧹 [GC] Собрано {_gc_collected} объектов после скан-цикла")


async def background_scanner():
    while state.is_running:
        if not state.is_paused:
            try:
                await scan_market()
            except Exception as e:
                print(f"Scanner error: {e}")
        await asyncio.sleep(Config.SCAN_INTERVAL)


async def _watchlist_refresh_task():
    """
    ✅ C3: Обновляем вотчлист каждые WATCHLIST_REFRESH_H часов.
    Важно для ловли монет, которые начали двигаться ПОСЛЕ запуска бота.
    По умолчанию: каждые 2 часа.
    """
    refresh_interval = int(Config.WATCHLIST_REFRESH_H * 3600)
    print(f"[WATCHLIST] Авто-обновление каждые {Config.WATCHLIST_REFRESH_H:.1f}ч")
    while state.is_running:
        await asyncio.sleep(refresh_interval)
        try:
            old_count = len(state.watchlist)
            new_wl = await _build_combined_watchlist(
                state.binance, Config.MIN_VOLUME_USDT, Config.MAX_WATCHLIST
            )
            if new_wl:
                old_set = set(state.watchlist or [])
                added   = len(set(new_wl) - old_set)
                removed = len(old_set - set(new_wl))
                state.watchlist = new_wl
                print(f"[WATCHLIST] ✅ Обновлён: {old_count} → {len(new_wl)} монет "
                      f"(+{added} новых, -{removed} убрано)")
            else:
                print("[WATCHLIST] ⚠️ Обновление вернуло пустой список — оставляем старый")
        except Exception as e:
            print(f"[WATCHLIST] ❌ Ошибка обновления: {e}")


async def _emergency_crash_tighten_sl():
    """
    🆘 SYSTEMIC_CRASH: при первичном обнаружении краша — подтягиваем SL открытых LONG
    к break-even (entry_price * 0.999) если позиция в убытке или хуже текущего SL.
    Цель: ограничить потери на уже открытых позициях, которые краш-детектор не блокировал.
    """
    if not state.auto_trader or not state.auto_trader.bingx:
        return

    print("[CRASH-SL] 🆘 SYSTEMIC_CRASH обнаружен — подтягиваем SL открытых LONG к BE...")
    try:
        positions = await state.auto_trader.bingx.get_positions()
        if not positions:
            print("[CRASH-SL] Нет открытых позиций")
            return

        tightened = 0
        for p in positions:
            if p.position_side != "LONG":
                continue
            sym      = p.symbol
            entry    = p.entry_price or 0.0
            cur_sl   = p.stop_loss   or 0.0
            if entry <= 0:
                continue

            # BE-SL: чуть ниже entry чтобы не сработал от спреда (0.1% ниже)
            be_sl = round(entry * 0.999, 8)

            # Тянем SL только если новый SL ВЫШЕ текущего (тянем вверх, не вниз!)
            if cur_sl >= be_sl:
                print(f"[CRASH-SL] {sym}: текущий SL={cur_sl:.6f} уже ≥ BE={be_sl:.6f} — пропускаем")
                continue

            ok = await state.auto_trader.bingx.update_stop_loss(sym, "LONG", be_sl, "long")
            if ok:
                tightened += 1
                print(f"[CRASH-SL] ✅ {sym}: SL {cur_sl:.6f} → BE {be_sl:.6f}")
                # Обновляем Redis
                try:
                    _sig = state.redis.get_position(Config.BOT_TYPE, sym.replace("-", ""))
                    if _sig:
                        _sig["stop_loss"] = be_sl
                        state.redis.save_position(Config.BOT_TYPE, sym.replace("-", ""), _sig)
                except Exception:
                    pass
            else:
                print(f"[CRASH-SL] ⚠️ {sym}: не удалось обновить SL")

        print(f"[CRASH-SL] Итого SL подтянут для {tightened} позиций")
    except Exception as e:
        print(f"[CRASH-SL] ❌ Ошибка: {e}")


async def _startup_sl_sync():
    """
    🚨 FIX: SL=0.000000 bug — синхронизация SL при старте.

    LONG бот обрабатывает ТОЛЬКО LONG позиции.
    SHORT позиции — ответственность short-bot.
    Защита от дублирования через Redis-ключ (TTL 10 мин).
    """
    await asyncio.sleep(10)

    if not state.auto_trader or not state.auto_trader.bingx:
        print("[SL-SYNC] AutoTrader не инициализирован — пропускаем")
        return

    print("[SL-SYNC] 🔍 Проверка SL на LONG BingX позициях...")
    try:
        positions = await state.auto_trader.bingx.get_positions()
        if not positions:
            print("[SL-SYNC] Нет открытых позиций")
            return

        fixed = 0
        skipped = 0
        for p in positions:
            # ✅ FIX: LONG бот трогает ТОЛЬКО LONG позиции
            if p.position_side != "LONG":
                continue

            sym = p.symbol
            direction = "long"
            entry = p.entry_price

            # ✅ Защита от дублирования: проверяем Redis-ключ
            _dedup_key = f"sl_sync_done:long:{sym}"
            try:
                if state.redis._client.exists(_dedup_key):
                    print(f"[SL-SYNC] {sym} LONG: уже синкован недавно — пропускаем")
                    continue
            except Exception:
                pass

            if p.stop_loss and p.stop_loss > 0:
                # SL есть на бирже — только синкуем Redis если пустой
                _redis_sig = state.redis.get_position(Config.BOT_TYPE, sym.replace("-", ""))
                if _redis_sig:
                    _changed = False
                    try:
                        _rs_sl = float(_redis_sig.get("stop_loss", 0))
                    except Exception:
                        _rs_sl = 0.0
                    if _rs_sl <= 0:
                        _redis_sig["stop_loss"] = p.stop_loss
                        _changed = True
                        print(f"[SL-SYNC] {sym} LONG: Redis SL обновлён с биржи → {p.stop_loss:.6f}")
                    # ✅ Enrichment: восстанавливаем take_profits из открытых TP ордеров если пусто
                    _rs_tps = _redis_sig.get("take_profits")
                    if not _rs_tps:
                        try:
                            _tp_result = await state.auto_trader.bingx._make_request(
                                "GET", "/openApi/swap/v2/trade/openOrders", params={"symbol": sym}
                            )
                            if _tp_result and _tp_result.get("code") == 0:
                                _tp_orders = [
                                    o for o in (_tp_result.get("data", {}).get("orders", []))
                                    if o.get("type") in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT", "LIMIT")
                                    and o.get("positionSide") == "LONG"
                                    and o.get("side") == "SELL"
                                ]
                                if _tp_orders:
                                    _tp_prices = sorted(
                                        float(o.get("stopPrice") or o.get("price", 0))
                                        for o in _tp_orders if float(o.get("stopPrice") or o.get("price", 0)) > 0
                                    )
                                    _w = round(100 / len(_tp_prices)) if _tp_prices else 25
                                    _redis_sig["take_profits"] = [[_pr, _w] for _pr in _tp_prices]
                                    _changed = True
                                    print(f"[SL-SYNC] {sym} LONG: take_profits восстановлены из {len(_tp_prices)} TP ордеров")
                        except Exception as _tp_e:
                            print(f"[SL-SYNC] {sym}: TP enrichment error: {_tp_e}")
                    if _changed:
                        state.redis.save_position(Config.BOT_TYPE, sym.replace("-", ""), _redis_sig)
                continue

            if entry <= 0:
                skipped += 1
                continue

            # ✅ FIX: BingX может вернуть stop_loss=0 пока реальный STOP_MARKET ордер уже есть.
            # Проверяем открытые ордера явно, чтобы не создавать дублирующий SL.
            _existing_sl_orders = []
            try:
                _oo_result = await state.auto_trader.bingx._make_request(
                    "GET", "/openApi/swap/v2/trade/openOrders",
                    params={"symbol": sym}
                )
                if _oo_result and _oo_result.get("code") == 0:
                    _all_orders = _oo_result.get("data", {}).get("orders", [])
                    _existing_sl_orders = [
                        o for o in _all_orders
                        if o.get("type") in ("STOP_MARKET", "STOP")
                        and o.get("positionSide") == "LONG"
                    ]
            except Exception as _oe:
                print(f"[SL-SYNC] ⚠️ {sym}: ошибка запроса ордеров: {_oe}")

            if _existing_sl_orders:
                _sl_price = float(_existing_sl_orders[0].get("stopPrice", 0))
                print(f"[SL-SYNC] {sym} LONG: уже есть SL ордер {_sl_price:.6f} — пропускаем (BingX вернул stop_loss=0)")
                _redis_sig = state.redis.get_position(Config.BOT_TYPE, sym.replace("-", ""))
                try:
                    _rs_sl = float(_redis_sig.get("stop_loss", 0)) if _redis_sig else 0.0
                except Exception:
                    _rs_sl = 0.0
                if _redis_sig and _rs_sl <= 0 and _sl_price > 0:
                    _redis_sig["stop_loss"] = _sl_price
                    state.redis.save_position(Config.BOT_TYPE, sym.replace("-", ""), _redis_sig)
                continue

            sl_pct = Config.SL_BUFFER
            sl = entry * (1 - sl_pct / 100)  # LONG: SL НИЖЕ entry

            pos_side = "LONG"
            # ── RETRY LOOP: до 3 попыток, 15-25с между попытками при code=109400 ──
            _max_attempts = 3
            _sl_ok = False
            for _att in range(_max_attempts):
                if _att > 0:
                    _wait = 15 + _att * 5
                    print(f"[SL-SYNC] ⏳ {sym} LONG: retry {_att}/{_max_attempts-1} через {_wait}s (API может быть временно недоступен)...")
                    await asyncio.sleep(_wait)
                _sl_ok = await state.auto_trader.bingx.update_stop_loss(sym, pos_side, sl, direction)
                if _sl_ok:
                    break

            if _sl_ok:
                fixed += 1
                print(f"[SL-SYNC] ✅ {sym} LONG: SL={sl:.6f} выставлен")
                _redis_sig = state.redis.get_position(Config.BOT_TYPE, sym.replace("-", ""))
                if _redis_sig:
                    _redis_sig["stop_loss"] = sl
                    state.redis.save_position(Config.BOT_TYPE, sym.replace("-", ""), _redis_sig)
                try:
                    state.redis._client.setex(_dedup_key, 600, "1")
                except Exception:
                    pass
                await state.telegram.send_message(
                    f"🚨 <b>SL SYNC</b> — аварийный стоп выставлен\n\n"
                    f"🟢 <code>#{sym}</code> LONG\n"
                    f"📍 Вход: <b>{entry:.6f}</b>\n"
                    f"🛑 Новый SL: <b>{sl:.6f}</b> ({sl_pct}%)\n"
                    f"<i>⚠️ Позиция не имела SL — исправлено при старте</i>"
                )
            else:
                skipped += 1
                print(f"[SL-SYNC] ❌ {sym} LONG: SL НЕ ВЫСТАВЛЕН после {_max_attempts} попыток — позиция БЕЗ ЗАЩИТЫ!")
                try:
                    await state.telegram.send_message(
                        f"🚨🚨 <b>КРИТИЧНО: SL НЕ ВЫСТАВЛЕН</b>\n\n"
                        f"🟢 <code>#{sym}</code> LONG\n"
                        f"📍 Вход: <b>{entry:.6f}</b>\n"
                        f"🛑 Пробовали SL: <b>{sl:.6f}</b>\n"
                        f"❌ <b>{_max_attempts} попытки провалились — позиция без стоп-лосса!</b>\n"
                        f"⚠️ Проверь и выставь SL вручную на BingX!"
                    )
                except Exception:
                    pass

        print(f"[SL-SYNC] Итого: {fixed} исправлено, {skipped} пропущено из {len(positions)}")

    except Exception as e:
        import traceback
        print(f"[SL-SYNC] ❌ Ошибка: {e}\n{traceback.format_exc()}")


async def _fear_greed_task():
    """Fear & Greed Index polling — обновляем каждые 30 мин (alternative.me, без ключа)."""
    from core.fear_greed import get_fear_greed
    fg_cache = get_fear_greed()
    while state.is_running:
        try:
            value = await fg_cache.get()
            if value is not None:
                state.fear_greed_index = value
                print(f"🧠 Fear & Greed: {value} ({fg_cache.label})")
        except Exception:
            pass
        await asyncio.sleep(1800)


async def _daily_report_task():
    while state.is_running:
        now  = datetime.utcnow()
        next_report = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= next_report:
            next_report += timedelta(days=1)
        await asyncio.sleep((next_report - now).total_seconds())
        if state.is_running and state.telegram:
            try:
                # Redis-история — не обнуляется при рестарте (как /daily_rep)
                # ✅ FIX: cmd_daily_report на TelegramCommandHandler, не TelegramBot
                if hasattr(state.telegram, "cmd_daily_report"):
                    await state.telegram.cmd_daily_report("", state.telegram.chat_id)
                elif hasattr(state.telegram, "_send_daily_report"):
                    await state.telegram._send_daily_report()
                print("✅ Daily report sent (Redis-based)")
                # PerformanceTracker: Sharpe / PF / MaxDD (in-RAM stats)
                if state.performance_tracker:
                    try:
                        pt_msg = state.performance_tracker.daily_report()
                        await state.telegram.send_message(pt_msg)
                    except Exception:
                        pass
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
    # ✅ FIX: не запускаем если скан уже идёт (процессный флаг)
    if getattr(state, '_scan_running', False):
        return {"message": "Scan already running", "skipped": True}
    bg.add_task(scan_market)
    return {"message": "Scan triggered"}

@app.get("/api/blacklist")
async def get_blacklist():
    """Просмотр постоянного блэклиста символов без данных."""
    try:
        keys = state.redis.client.keys("blacklist:*")
        bl = {k.replace("blacklist:", ""): state.redis.client.get(k) for k in keys}
        skip_keys = state.redis.client.keys("skip:nodata:*")
        skip = [k.replace("skip:nodata:", "") for k in skip_keys]
        return {"permanent_blacklist": bl, "count": len(bl),
                "temp_skip_24h": skip, "temp_count": len(skip),
                "env_blacklist": list(Config.SYMBOL_BLACKLIST)}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/blacklist/{symbol}")
async def remove_from_blacklist(symbol: str):
    """Убрать символ из постоянного блэклиста (если данные появились)."""
    symbol = symbol.upper()
    try:
        state.redis.client.delete(f"blacklist:{symbol}")
        state.redis.client.delete(f"skip:nodata:{symbol}")
        state.redis.client.delete(f"nodata_count:{symbol}")
        return {"message": f"{symbol} удалён из блэклиста"}
    except Exception as e:
        return {"error": str(e)}

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
