"""
Market Data Client v3.5 — Bybit (основной) + Binance через прокси

ИЗМЕНЕНИЯ v2.1:
  ✅ get_all_symbols: default min_volume снижен 5M → 300K (было 50 монет, стало 150-200)
  ✅ MarketData: добавлены поля для breakout/momentum анализа:
       volume_spike_ratio  — объём последней 15м свечи / средний объём (20 свечей)
       price_change_1h     — изменение цены за последний час %
       atr_14_pct          — ATR(14) на 15м как % от цены (волатильность)
       candle_body_pct     — тело последней свечи как % от ATR (сила свечи)
       volume_15m_candles  — последние 20 объёмов на 15м (для паттерн-детектора)

ИЗМЕНЕНИЯ v3.2:
  ✅ Whitelist cache: /fapi/v1/exchangeInfo → только существующие символы
  ✅ Dead symbol cache: повторяющиеся 404 → пропускаем Binance
  ✅ Explicit fallback logging при 404/400

ИЗМЕНЕНИЯ v3.3:
  ✅ Добавлен `period` в get_liquidations: поддержка 30m, 4h, 1d
  ✅ OKX fallback для ликвидаций

ИЗМЕНЕНИЯ v3.4:
  ✅ Детальное логирование OKX fallback
  ✅ Логирование ошибок парсинга OKX
  ✅ INFO уровень для всех fallback сообщений (видимость в логах)
  ✅ FIX: OKX instId формат для perpetual swaps (добавлен -SWAP)

ИЗМЕНЕНИЯ v3.5:
  ✅ FIX: OKX liquidation endpoint — /public/liquidation-orders (public API)
  ✅ FIX: Убран OKX Taker fallback — /api/v5/rubik/stat/taker-volume приватный
  ✅ Taker ratio: только Binance (с whitelist), fallback отключён
  ✅ FIX: Telegram HTML escaping для сообщений
"""

import os
import asyncio
import aiohttp
import logging
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime

# ── Market Structure Analysis ─────────────────────────────────────────────
try:
    from .market_structure import (
        MarketStructureResult, compute_market_structure,
        CascadeSignal, detect_cascade_signal
    )
    _MS_AVAILABLE = True
except ImportError:
    _MS_AVAILABLE = False
    MarketStructureResult = None
import time
import statistics

# ✅ FIX #1: logger был не определён → NameError на всех Binance API вызовах
logger = logging.getLogger("aegis.binance")


@dataclass
class CandleData:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float


@dataclass
class MarketData:
    # ── Базовые поля ─────────────────────────────────────────────────────────
    symbol:              str
    price:               float
    rsi_1h:              Optional[float]
    funding_rate:        float
    funding_accumulated: float
    open_interest:       float
    oi_change_4d:        float
    long_short_ratio:    float
    volume_24h:          float
    volume_change_24h:   float
    price_change_24h:    float
    hourly_deltas:       List[float]
    last_updated:        datetime

    # ── Новые поля для breakout / momentum / структуры ───────────────────────
    # Соотношение объёма последней 15м свечи к среднему за 20 свечей
    # >2.0 = spike, >3.0 = сильный spike, >5.0 = экстремальный
    volume_spike_ratio:   float = 1.0

    # Изменение цены за последний час (для определения импульса)
    price_change_1h:      float = 0.0

    # ATR(14) на 15м в % от цены — мера волатильности пары
    # Низкий (<0.5%) = консолидация, Высокий (>1.5%) = волатильно
    atr_14_pct:           float = 0.5

    # Тело последней 15м свечи как доля ATR (0-1+)
    # >0.8 = сильная направленная свеча (momentum)
    candle_body_pct:      float = 0.5

    # Сырые объёмы последних 20 свечей 15м (для паттерн-детектора)
    volume_15m_candles:   List[float] = field(default_factory=list)

    # 24h хай и лоу — для breakout уровней
    high_24h:             float = 0.0
    low_24h:              float = 0.0

    # Расстояние до 24h high в % (0.5% = почти у хая → breakout риск)
    pct_from_high_24h:    float = 5.0

    # Расстояние до 24h low в %
    pct_from_low_24h:     float = 5.0

    # ── Краткосрочные OI изменения (v4.0) ────────────────────────────────────
    # Источник: get_oi_short_term() — цепочка Binance→Bybit→OKX
    # oi_15m = позиции открываются/закрываются ПРЯМО СЕЙЧАС (самый быстрый сигнал)
    oi_change_15m:        float = 0.0  # 🆕 15m OI — самый быстрый
    oi_change_30m:        float = 0.0
    oi_change_1h:         float = 0.0
    oi_change_4h:         float = 0.0
    # oi_change_4d используется в OIAnalyzerLong.analyze() (oi_4d секция)
    # >0.6 = агрессивные покупки (бычье давление), <0.4 = агрессивные продажи
    taker_buy_sell_ratio: Optional[float] = None

    # Ликвидации в USD за последний период (15m/1h)
    recent_liquidations_usd: Optional[float] = None

    # Сторона доминирующих ликвидаций: "LONG" | "SHORT" | None
    liq_side: Optional[str] = None

    # Top trader L/S ratio - "умные деньги" позиции
    # >1.5 = топы в лонгах, <0.8 = топы в шортах
    top_trader_long_short_ratio: Optional[float] = None

    # ── Market Structure (v4.0) ───────────────────────────────────────────────
    # Полный HTF анализ: PDH/PDL, Fib, POC, CRT, GAP, OB/FVG 4H+1D
    market_structure: Optional[object] = None  # MarketStructureResult
    # Каскадный сигнал: 4H Fractal Raid → 1H SNR → 15M FVG
    cascade_signal: Optional[object] = None    # CascadeSignal

    # ── 30m ATR (отдельный ТФ) ───────────────────────────────────────────────
    atr_30m_pct: float = 0.0


@dataclass
class BreakoutData:
    """Быстрый датакласс только для breakout/momentum проверки."""
    symbol:              str
    price:               float
    volume_spike_ratio:  float    # объём / средний объём (15м)
    price_change_1h:     float    # % за час
    price_change_15m:    float    # % за 15 мин
    atr_14_pct:          float    # ATR как % от цены
    candle_body_pct:     float    # тело свечи / ATR
    is_near_high_24h:    bool     # цена в 1% от 24h хая
    is_near_low_24h:     bool     # цена в 1% от 24h лоя
    rsi_1h:              float    # RSI часовой
    funding_rate:        float    # фандинг
    volume_15m_candles:  List[float]


FALLBACK_WATCHLIST = [
    # Tier 1 — топ по капитализации
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "TRXUSDT", "TONUSDT",
    "LINKUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT", "UNIUSDT",
    "ATOMUSDT", "ETCUSDT", "XLMUSDT", "BCHUSDT", "FILUSDT",
    # Tier 2 — DeFi / Layer2
    "AAVEUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT",
    "INJUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT", "WLDUSDT",
    "ORDIUSDT", "LDOUSDT", "STXUSDT", "RUNEUSDT", "MKRUSDT",
    "SNXUSDT", "GALAUSDT", "SANDUSDT", "MANAUSDT", "AXSUSDT",
    "APEUSDT", "GMXUSDT", "DYDXUSDT", "FTMUSDT", "ALGOUSDT",
    "FLOWUSDT", "HBARUSDT", "QNTUSDT", "EGLDUSDT", "THETAUSDT",
    # Tier 3 — mid-cap
    "BLURUSDT", "OCEANUSDT", "FETUSDT", "AGIXUSDT", "RNDRUSDT",
    "IMXUSDT", "GRTUSDT", "1INCHUSDT", "COMPUSDT", "YFIUSDT",
    "CRVUSDT", "CVXUSDT", "BALUSDT", "SUSHIUSDT", "PEPEUSDT",
    "FLOKIUSDT", "WOOUSDT", "ILVUSDT", "LRCUSDT", "SKLUSDT",
    "CELOUSDT", "ENJUSDT", "CHZUSDT", "BATUSDT", "ZILUSDT",
    "RVNUSDT", "ICPUSDT", "VETUSDT", "XTZUSDT", "NEOUSDT",
    "ONTUSDT", "IOTAUSDT", "KASUSDT", "ARUSDT", "CFXUSDT",
    "PENDLEUSDT", "JUPUSDT", "PYTHUSDT", "WIFUSDT", "BONKUSDT",
    "MEMEUSDT", "SATSUSDT", "ACEUSDT", "NFPUSDT", "AIUSDT",
    "XAIUSDT", "MANTAUSDT", "ALTUSDT", "ZETAUSDT", "RONINUSDT",
    # Tier 4 — small-cap с объёмом
    "STRKUSDT", "DYMUSDT", "PIXELUSDT", "PORTALUSDT", "AEVOUSDT",
    "EIGENUSDT", "SOLVUSDT", "SCRUSDT", "GRASSUSDT", "MORPHOUSDT",
    "MOVEUSDT", "MEUSDT", "VIRTUALUSDT", "SPXUSDT", "GRIFFAINUSDT",
    "PNUTUSDT", "ACTUSDT", "GOATUSDT", "CHILLGUYUSDT", "COWUSDT",
    "HYPEUSDT", "RENDERUSDT", "TAIKOUSDT", "ZKUSDT", "ETHFIUSDT",
    "SAFEUSDT", "KMNOUSDT", "REZUSDT", "BOMEUSDT", "SLEEPLESSUSDT",
    "XVSUSDT", "POLYXUSDT", "BIGTIMEUSDT", "RDNTUSDT", "HOOKUSDT",
    "HIGHUSDT", "AMBUSDT", "FORTHUSDT", "IDUSDT", "TYPEUSDT",
    "LPTUSDT", "USTCUSDT", "MXUSDT", "TRUUSDT", "GLMRUSDT",
    "TUSDT", "COREUSDT", "OMUSDT", "STPTUSDT", "MYRIAUSDT",
]


class BinanceFuturesClient:
    """
    Клиент рыночных данных v3.1.
    Цепочка источников: Binance(proxy) → Bybit → OKX(direct)
    USE_BINANCE=false (default) → Bybit first, OKX fallback
    USE_BINANCE=true            → Binance proxy first, Bybit, OKX fallback
    """

    BYBIT_URL   = "https://api.bybit.com"
    BINANCE_URL = "https://fapi.binance.com"
    OKX_URL     = "https://www.okx.com"

    # Dead symbol cache: {symbol: timestamp_when_first_failed}
    # Symbols that fail 3+ consecutive fetches are skipped for DEAD_SYMBOL_TTL seconds
    _dead_symbols:    dict = {}
    DEAD_SYMBOL_TTL:  int  = 3600   # 1 час — потом перепроверяем
    DEAD_SYMBOL_HITS: int  = 3      # сколько подряд фейлов → мёртвый

    # ✅ NEW: Binance whitelist cache — предотвращает 404 для несуществующих пар
    _binance_symbols_cache:     Optional[set] = None
    _binance_symbols_last_update: float = 0.0
    BINANCE_SYMBOLS_TTL:        int   = 3600  # Обновляем whitelist каждый час

    # Прокси-ротация: {proxy: failed_at_timestamp}
    _dead_proxy_times: dict = {}
    DEAD_PROXY_TTL: int = 300  # 5 мин cooldown на мёртвый прокси

    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key or os.getenv("BINANCE_API_KEY", "")
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_request_time = 0.0
        self.min_request_interval = 0.05

        use_binance_env  = os.getenv("USE_BINANCE", "false").lower()
        self._try_binance = use_binance_env == "true"
        self._use_binance = False
        self._okx_redis   = None   # Устанавливается ботом: client.set_redis(redis)

        proxy_env = os.getenv("PROXY_LIST", "")
        raw_proxies = [p.strip() for p in proxy_env.split(",") if p.strip()]
        # Дедупликация прокси (в списке могут быть повторы)
        seen = set()
        self._proxies = []
        for p in raw_proxies:
            if p not in seen:
                seen.add(p)
                self._proxies.append(p)
        self._proxy_idx   = 0
        self._active_proxy: Optional[str] = None

        print(f"🔧 Market client v3.1: {'Binance+proxy' if self._try_binance else 'Bybit'} → OKX fallback"
              + (f" ({len(self._proxies)} proxies)" if self._proxies else ""))

    def _next_proxy(self) -> Optional[str]:
        if not self._proxies:
            return None
        p = self._proxies[self._proxy_idx % len(self._proxies)]
        self._proxy_idx += 1
        return p

    def _is_proxy_dead(self, proxy: str) -> bool:
        failed_at = BinanceFuturesClient._dead_proxy_times.get(proxy, 0)
        return time.time() - failed_at < self.DEAD_PROXY_TTL

    def _mark_proxy_dead(self, proxy: str):
        BinanceFuturesClient._dead_proxy_times[proxy] = time.time()
        host = proxy.split('@')[-1] if '@' in proxy else proxy
        logger.warning(f"[Proxy] ❌ {host} → недоступен, cooldown {self.DEAD_PROXY_TTL}s")
        if self._active_proxy == proxy:
            self._active_proxy = None

    def _get_live_proxies(self) -> list:
        """Живые прокси — активный первым, остальные в порядке списка."""
        live = [p for p in self._proxies if not self._is_proxy_dead(p)]
        if not live:
            # Все мёртвые — сбрасываем cooldown и пробуем заново
            BinanceFuturesClient._dead_proxy_times.clear()
            logger.warning("[Proxy] Все прокси в cooldown — сбрасываем и пробуем заново")
            live = self._proxies.copy()
        # Активный первым (быстрый путь)
        if self._active_proxy and self._active_proxy in live:
            live.remove(self._active_proxy)
            live.insert(0, self._active_proxy)
        return live

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(ssl=False)
            self.session = aiohttp.ClientSession(connector=connector)
        return self.session

    async def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def set_redis(self, redis_client):
        """Привязываем Redis для чтения OKX WS кеша ликвидаций."""
        self._okx_redis = redis_client

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def _init_source(self):
        # v3.0: Re-check source every 30 min (don't cache dead proxy forever)
        now = time.time()
        if hasattr(self, '_source_ready'):
            if now - getattr(self, '_source_checked_at', 0) < 1800:
                return
        self._source_ready = True
        self._source_checked_at = now

        if not self._try_binance:
            self._use_binance = False
            print("✅ Data source: Bybit (default)")
            return

        for proxy in self._proxies:  # пробуем все прокси, не только первые 3
            if self._is_proxy_dead(proxy):
                continue
            try:
                session = await self._get_session()
                async with session.get(
                    f"{self.BINANCE_URL}/fapi/v1/time",
                    proxy=proxy,
                    timeout=aiohttp.ClientTimeout(total=6),
                    ssl=False
                ) as resp:
                    if resp.status == 200:
                        self._use_binance = True
                        self._active_proxy = proxy
                        host = proxy.split('@')[-1] if '@' in proxy else proxy
                        print(f"✅ Data source: Binance via proxy ({host})")
                        return
                    # HTTP ошибка — прокси работает но Binance отвечает плохо
                    break
            except Exception:
                self._mark_proxy_dead(proxy)
                continue

        self._use_binance = False
        print("⚠️ Binance unavailable. Falling back to Bybit.")

    # =========================================================================
    # BYBIT / BINANCE REQUESTS
    # =========================================================================

    _bybit_blocked: bool = False       # class-level flag: Bybit geo-blocked
    _ticker_cache:  dict = {}           # {symbol: ticker_dict} — batch cache
    _ticker_cache_ts: float = 0.0       # timestamp of last batch fetch
    _bybit_blocked_at: float = 0.0     # timestamp when blocked (for retry)
    BYBIT_RETRY_INTERVAL: int = 300    # ✅ FIX v17: retry Bybit every 5 min (было 30 мин)

    async def _bybit(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        await self._rate_limit()
        # v3.0 FIX: retry Bybit every 30 min (was: blocked forever)
        if BinanceFuturesClient._bybit_blocked:
            elapsed = time.time() - BinanceFuturesClient._bybit_blocked_at
            if elapsed < BinanceFuturesClient.BYBIT_RETRY_INTERVAL:
                return None
            # Reset and retry
            BinanceFuturesClient._bybit_blocked = False
            print(f"🔄 Bybit retry after {elapsed/60:.0f}m block — testing connection...")
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.BYBIT_URL}{endpoint}",
                params=params or {},
                timeout=aiohttp.ClientTimeout(total=15)  # ✅ FIX v17: 10→15s
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        return data.get("result")
                    return None
                if resp.status == 403:
                    # Geo-block: Render IP banned. Try Binance via proxy.
                    if not BinanceFuturesClient._bybit_blocked:
                        BinanceFuturesClient._bybit_blocked = True
                        BinanceFuturesClient._bybit_blocked_at = time.time()
                        print("⛔ Bybit geo-blocked (403). Falling back to Binance proxy.")
                    return None
                # Suppress 404 for liquidation endpoint (normal when no recent liq activity)
                if resp.status == 404 and "liquidation" in endpoint:
                    logger.debug(f"[Bybit] 404 {endpoint} | {params.get('symbol','')} — нет активных ликвидаций")
                else:
                    logger.warning(f"[Bybit] HTTP {resp.status} | {endpoint} | params={params}")
                return None
        except asyncio.TimeoutError:
            logger.warning(f"[Bybit] TIMEOUT | {endpoint}")
            return None
        except Exception as e:
            logger.debug(f"[Bybit] ERROR | {endpoint} | {type(e).__name__}: {e}")
            return None

    # Log deduplication: {(endpoint, symbol_key): last_logged_ts}
    # Suppresses repeated 404/400 for same endpoint+symbol within LOG_SUPPRESS_TTL seconds
    _log_suppress:     dict = {}
    LOG_SUPPRESS_TTL:  int  = 300   # suppress identical error for 5 min

    def _should_log_error(self, endpoint: str, params: dict) -> bool:
        """Returns True if this error should be logged (not suppressed)."""
        symbol = params.get("symbol", "") if params else ""
        key = (endpoint, symbol)
        now = time.time()
        last = BinanceFuturesClient._log_suppress.get(key, 0)
        if now - last < self.LOG_SUPPRESS_TTL:
            return False
        BinanceFuturesClient._log_suppress[key] = now
        return True

    async def _binance(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        await self._rate_limit()
        proxies = self._get_live_proxies()
        if not proxies:
            return None

        for proxy in proxies:
            try:
                session = await self._get_session()
                async with session.get(
                    f"{self.BINANCE_URL}{endpoint}",
                    params=params or {},
                    proxy=proxy,
                    timeout=aiohttp.ClientTimeout(total=10),
                    ssl=False
                ) as resp:
                    if resp.status == 200:
                        # Фиксируем рабочий прокси если сменился
                        if proxy != self._active_proxy:
                            host = proxy.split('@')[-1] if '@' in proxy else proxy
                            logger.info(f"[Proxy] ✅ Активный прокси → {host}")
                            self._active_proxy = proxy
                        return await resp.json()
                    # HTTP-ошибка (404, 400 и т.д.) — не вина прокси, выходим
                    if self._should_log_error(endpoint, params):
                        logger.warning(
                            f"[Binance] HTTP {resp.status} | {endpoint} | proxy={proxy.split('@')[-1] if '@' in proxy else proxy}"
                        )
                    return None
            except asyncio.TimeoutError:
                logger.warning(f"[Binance] TIMEOUT | proxy={proxy.split('@')[-1] if '@' in proxy else proxy} | пробуем следующий")
                self._mark_proxy_dead(proxy)
                continue
            except Exception as e:
                err = str(e)
                if any(kw in err for kw in ("Cannot connect", "Connection", "proxy", "tunnel")):
                    self._mark_proxy_dead(proxy)
                    continue
                logger.warning(f"[Binance] ERROR | {endpoint} | {type(e).__name__}: {e}")
                return None

        logger.warning(f"[Binance] Все прокси недоступны для {endpoint}")
        return None

    # =========================================================================
    # OKX DIRECT (no proxy needed — globally accessible)
    # =========================================================================

    @staticmethod
    def _to_okx_instid(symbol: str) -> str:
        """BTCUSDT → BTC-USDT-SWAP  |  1000BONKUSDT → 1000BONK-USDT-SWAP
        ✅ FIX: was returning BTC-USDT (code=51001) — rubik endpoints need -SWAP suffix
        """
        if symbol.endswith("USDT"):
            base = symbol[:-4]   # strip USDT
            return f"{base}-USDT-SWAP"
        return symbol

    async def _okx(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        """Прямой запрос OKX без прокси. Возвращает data[] или None.
        ✅ FIX: timeout 8s → 3s. Rubik stats = fallback, не должны тормозить скан.
        """
        await self._rate_limit()
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.OKX_URL}{endpoint}",
                params=params or {},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    body = await resp.json()
                    if body.get("code") == "0":
                        return body.get("data")
                    logger.debug(f"[OKX] {endpoint} | code={body.get('code')} msg={body.get('msg')}")
                else:
                    logger.debug(f"[OKX] HTTP {resp.status} | {endpoint}")
        except asyncio.TimeoutError:
            logger.debug(f"[OKX] TIMEOUT | {endpoint}")
        except Exception as e:
            logger.debug(f"[OKX] ERROR | {endpoint} | {type(e).__name__}: {e}")
        return None

    # =========================================================================
    # DEAD SYMBOL CACHE
    # =========================================================================

    def _is_dead_symbol(self, symbol: str) -> bool:
        entry = BinanceFuturesClient._dead_symbols.get(symbol)
        if not entry:
            return False
        hits, ts = entry
        if time.time() - ts > self.DEAD_SYMBOL_TTL:
            # TTL истёк — перепроверяем
            del BinanceFuturesClient._dead_symbols[symbol]
            return False
        return hits >= self.DEAD_SYMBOL_HITS

    def _mark_symbol_fail(self, symbol: str):
        entry = BinanceFuturesClient._dead_symbols.get(symbol)
        if entry:
            hits, ts = entry
            BinanceFuturesClient._dead_symbols[symbol] = (hits + 1, ts)
        else:
            BinanceFuturesClient._dead_symbols[symbol] = (1, time.time())

    def _mark_symbol_ok(self, symbol: str):
        BinanceFuturesClient._dead_symbols.pop(symbol, None)

    # =========================================================================
    # ✅ NEW: BINANCE WHITELIST — предотвращает 404 для несуществующих пар
    # =========================================================================

    async def _load_binance_symbols(self) -> set:
        """Загрузить список всех активных USDT фьючерсов с Binance."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.BINANCE_URL}/fapi/v1/exchangeInfo",
                timeout=aiohttp.ClientTimeout(total=10),
                ssl=False
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    symbols = set()
                    for s in data.get("symbols", []):
                        # Только активные USDT фьючерсы
                        if (s.get("status") == "TRADING" and 
                            s.get("contractType") == "PERPETUAL" and
                            s.get("quoteAsset") == "USDT"):
                            symbols.add(s.get("symbol", ""))
                    BinanceFuturesClient._binance_symbols_cache = symbols
                    BinanceFuturesClient._binance_symbols_last_update = time.time()
                    logger.debug(f"[Binance] Whitelist loaded: {len(symbols)} symbols")
                    return symbols
        except Exception as e:
            logger.warning(f"[Binance] Failed to load whitelist: {e}")
        return set()

    async def _is_binance_symbol_available(self, symbol: str) -> bool:
        """Проверить существует ли пара на Binance фьючерсах."""
        # Если кэш устарел или пуст — обновляем
        now = time.time()
        if (BinanceFuturesClient._binance_symbols_cache is None or 
            now - BinanceFuturesClient._binance_symbols_last_update > self.BINANCE_SYMBOLS_TTL):
            await self._load_binance_symbols()
        
        # ✅ FIX: Если кэш всё ещё None (ошибка загрузки) — ЗАПРЕЩАЕМ запросы к Binance
        # Возвращаем False чтобы сразу перейти к Bybit/OKX fallback
        if BinanceFuturesClient._binance_symbols_cache is None:
            logger.warning(f"[Whitelist] Кэш пустой — {symbol} не будет запрошен у Binance")
            return False
            
        return symbol in BinanceFuturesClient._binance_symbols_cache

    async def _req(self, binance_ep: str, bybit_ep: str,
                   binance_params: Dict = None,
                   bybit_params: Dict = None) -> Optional[Any]:
        await self._init_source()
        if self._use_binance:
            result = await self._binance(binance_ep, binance_params)
            if result is not None:
                return result
            # Binance failed → fallback to Bybit
            return await self._bybit(bybit_ep, bybit_params)

        # v3.0: Bybit first. If geo-blocked AND proxies available → try Binance
        result = await self._bybit(bybit_ep, bybit_params)
        if result is None and BinanceFuturesClient._bybit_blocked and self._proxies:
            # Auto-switch to Binance via proxy when Bybit is geo-blocked
            if not self._active_proxy:
                self._active_proxy = self._next_proxy()
            result = await self._binance(binance_ep, binance_params)
            if result is not None and not self._use_binance:
                self._use_binance = True
                print(f"✅ Auto-switched to Binance proxy: {self._active_proxy}")
        return result

    # =========================================================================
    # SYMBOLS
    # =========================================================================

    async def get_all_symbols(self,
                               min_volume_usdt: float = 300_000) -> List[str]:
        """
        Получить отсортированный по объёму список символов USDT фьючерсов.

        ✅ v2.1: default снижен 5_000_000 → 300_000
          - При 5M: ~50 символов (слишком мало, пропускали EPIC, OGN и др.)
          - При 1M: ~80-100 символов
          - При 300K: ~150-200 символов (рекомендуется)
          - Реальный порог задаётся через ENV MIN_VOLUME_USDT

        Символы возвращаются отсортированные по убыванию объёма.
        """
        await self._init_source()

        if self._use_binance:
            syms = await self._symbols_binance(min_volume_usdt)
            if syms:
                return syms

        syms = await self._symbols_bybit(min_volume_usdt)
        return syms if syms else FALLBACK_WATCHLIST

    async def _symbols_bybit(self, min_vol: float) -> List[str]:
        max_wl = int(os.getenv("MAX_WATCHLIST", "200"))
        try:
            result = await self._bybit("/v5/market/tickers", {"category": "linear"})
            if not result:
                return FALLBACK_WATCHLIST

            tickers = result.get("list", [])

            # Фильтр: USDT, объём >= min_vol, исключаем стейблы и леверидж-токены
            EXCLUDE = {"USDC", "BUSD", "TUSD", "DAI", "FDUSD", "USDP"}
            EXCLUDE_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR", "3L", "3S")

            syms = []
            for t in tickers:
                sym = t.get("symbol", "")
                base = sym.replace("USDT", "")
                if not sym.endswith("USDT"):
                    continue
                if base in EXCLUDE:
                    continue
                if any(sym.endswith(s) for s in EXCLUDE_SUFFIXES):
                    continue
                vol = float(t.get("turnover24h", 0))
                if vol >= min_vol:
                    syms.append((sym, vol))

            # Сортируем по объёму — самые ликвидные первые
            syms.sort(key=lambda x: x[1], reverse=True)
            result_list = [s[0] for s in syms[:max_wl]]

            print(f"✅ Bybit watchlist: {len(result_list)} symbols "
                  f"(min_vol=${min_vol/1e6:.1f}M, max={max_wl})")
            return result_list if result_list else FALLBACK_WATCHLIST

        except Exception as e:
            print(f"Bybit symbols error: {e}")
            return FALLBACK_WATCHLIST

    async def _symbols_binance(self, min_vol: float) -> List[str]:
        max_wl = int(os.getenv("MAX_WATCHLIST", "200"))
        try:
            # Получаем все 24h tickers за один запрос
            tickers = await self._binance("/fapi/v1/ticker/24hr")
            if not tickers:
                return []

            EXCLUDE_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR", "3L", "3S")
            syms = []
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                if any(sym.endswith(s) for s in EXCLUDE_SUFFIXES):
                    continue
                vol = float(t.get("quoteVolume", 0))
                if vol >= min_vol:
                    syms.append((sym, vol))

            syms.sort(key=lambda x: x[1], reverse=True)
            result = [s[0] for s in syms[:max_wl]]
            print(f"✅ Binance watchlist: {len(result)} symbols")
            return result if result else []
        except Exception:
            return []

    # =========================================================================
    # PRICE
    # =========================================================================

    async def get_price(self, symbol: str) -> Optional[float]:
        await self._init_source()
        if self._use_binance:
            d = await self._binance("/fapi/v1/ticker/price", {"symbol": symbol})
            if d:
                return float(d["price"])
        result = await self._bybit("/v5/market/tickers",
                                   {"category": "linear", "symbol": symbol})
        if result:
            items = result.get("list", [])
            if items:
                p = items[0].get("lastPrice")
                return float(p) if p else None
        return None

    # =========================================================================
    # KLINES
    # =========================================================================

    async def get_klines(self, symbol: str, interval: str = "1h",
                         limit: int = 100) -> List[CandleData]:
        await self._init_source()
        if self._use_binance:
            return await self._klines_binance(symbol, interval, limit)
        return await self._klines_bybit(symbol, interval, limit)

    async def _klines_binance(self, symbol, interval, limit) -> List[CandleData]:
        data = await self._binance("/fapi/v1/klines",
                                   {"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            return []
        return [CandleData(int(c[0]), float(c[1]), float(c[2]),
                           float(c[3]), float(c[4]), float(c[5]), float(c[7]))
                for c in data]

    async def _klines_bybit(self, symbol, interval, limit) -> List[CandleData]:
        imap = {"1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
                "1h": "60", "2h": "120", "4h": "240", "1d": "D"}
        result = await self._bybit("/v5/market/kline",
                                   {"category": "linear", "symbol": symbol,
                                    "interval": imap.get(interval, "60"),
                                    "limit": limit})
        if not result:
            return []
        candles = [
            CandleData(int(c[0]), float(c[1]), float(c[2]),
                       float(c[3]), float(c[4]), float(c[5]),
                       float(c[6]) if len(c) > 6 else 0.0)
            for c in result.get("list", [])
        ]
        candles.reverse()
        return candles

    async def get_24h_ticker(self, symbol: Optional[str] = None) -> Optional[Dict]:
        await self._init_source()
        # ✅ OPT v18: использовать batch-кэш вместо отдельного запроса на символ
        if symbol and BinanceFuturesClient._ticker_cache:
            cached = BinanceFuturesClient._ticker_cache.get(symbol)
            if cached:
                return cached
        # Fallback: одиночный запрос (для первого скана до инициализации кэша)
        if self._use_binance:
            params = {"symbol": symbol} if symbol else {}
            return await self._binance("/fapi/v1/ticker/24hr", params)
        if symbol:
            result = await self._bybit("/v5/market/tickers",
                                       {"category": "linear", "symbol": symbol})
            if result:
                items = result.get("list", [])
                if items:
                    t = items[0]
                    pct = float(t.get("price24hPcnt", 0))
                    return {
                        "quoteVolume":        t.get("turnover24h", 0),
                        "priceChangePercent": pct * 100,
                        "highPrice":          t.get("highPrice24h", 0),
                        "lowPrice":           t.get("lowPrice24h", 0),
                    }
        return None

    async def _fetch_ticker_batch(self) -> None:
        """
        Batch-загрузка всех USDT-linear тикеров за один запрос.
        Кэш живёт 60 секунд. Используется в get_24h_ticker() вместо
        отдельного запроса на каждый символ (~341 запрос → 1 запрос).
        Сокращает время скана примерно на 25-35 секунд.
        """
        now = time.time()
        if now - BinanceFuturesClient._ticker_cache_ts < 60:
            return  # кэш актуален
        try:
            if self._use_binance:
                data = await self._binance("/fapi/v1/ticker/24hr", {})
                if data and isinstance(data, list):
                    BinanceFuturesClient._ticker_cache = {
                        d["symbol"]: {
                            "quoteVolume":        d.get("quoteVolume", 0),
                            "priceChangePercent": float(d.get("priceChangePercent", 0)),
                            "highPrice":          d.get("highPrice", 0),
                            "lowPrice":           d.get("lowPrice", 0),
                        }
                        for d in data if "symbol" in d
                    }
                    BinanceFuturesClient._ticker_cache_ts = time.time()
                    logger.debug(f"[TickerBatch] Binance: {len(BinanceFuturesClient._ticker_cache)} symbols cached")
                    return
            # Bybit batch
            result = await self._bybit("/v5/market/tickers", {"category": "linear"})
            if result:
                items = result.get("list", [])
                cache = {}
                for t in items:
                    sym = t.get("symbol", "")
                    if not sym.endswith("USDT"):
                        continue
                    try:
                        pct = float(t.get("price24hPcnt", 0)) * 100
                        cache[sym] = {
                            "quoteVolume":        t.get("turnover24h", 0),
                            "priceChangePercent": round(pct, 4),
                            "highPrice":          t.get("highPrice24h", 0),
                            "lowPrice":           t.get("lowPrice24h", 0),
                            "fundingRate":        t.get("fundingRate", None),
                            "markPrice":          t.get("markPrice", None),
                            "lastPrice":          t.get("lastPrice", None),
                        }
                    except Exception:
                        pass
                BinanceFuturesClient._ticker_cache = cache
                BinanceFuturesClient._ticker_cache_ts = time.time()
                logger.info(f"[TickerBatch] Bybit: {len(cache)} symbols cached (1 request)")
        except Exception as e:
            logger.debug(f"[TickerBatch] failed: {e}")

    # =========================================================================
    # FUNDING
    # =========================================================================

    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        await self._init_source()
        if self._use_binance:
            d = await self._binance("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1})
            if d and len(d) > 0:
                return float(d[0].get("fundingRate", 0))
        result = await self._bybit("/v5/market/tickers",
                                   {"category": "linear", "symbol": symbol})
        if result:
            items = result.get("list", [])
            if items:
                fr = items[0].get("fundingRate")
                return float(fr) if fr else None
        return None

    async def get_funding_history(self, symbol: str, limit: int = 12) -> List[Dict]:
        await self._init_source()
        if self._use_binance:
            d = await self._binance("/fapi/v1/fundingRate",
                                    {"symbol": symbol, "limit": limit})
            return d or []
        result = await self._bybit("/v5/market/funding/history",
                                   {"category": "linear", "symbol": symbol, "limit": limit})
        if result:
            return [{"fundingRate": item.get("fundingRate", 0)}
                    for item in result.get("list", [])]
        return []

    async def get_accumulated_funding(self, symbol: str, days: int = 4) -> float:
        history = await self.get_funding_history(symbol, days * 3)
        if not history:
            return 0.0
        return round(sum(float(h.get("fundingRate", 0)) for h in history) * 100, 4)

    # =========================================================================
    # OPEN INTEREST
    # =========================================================================

    async def get_open_interest(self, symbol: str) -> Optional[float]:
        """
        Текущий Open Interest: Binance(proxy) → Bybit.
        
        ✅ v3.2: Whitelist check — пропускаем Binance если символа нет
        """
        await self._init_source()
        
        # ── 1. Binance (с whitelist проверкой) ───────────────────────────────
        if self._use_binance:
            is_available = await self._is_binance_symbol_available(symbol)
            if is_available:
                d = await self._binance("/fapi/v1/openInterest", {"symbol": symbol})
                if d:
                    return float(d.get("openInterest", 0))
            else:
                logger.debug(f"[OI current] {symbol}: не на Binance → сразу к Bybit")
        result = await self._bybit("/v5/market/tickers",
                                   {"category": "linear", "symbol": symbol})
        if result:
            items = result.get("list", [])
            if items:
                oi = items[0].get("openInterest")
                return float(oi) if oi else None
        return None

    async def get_open_interest_history(self, symbol: str,
                                         period: str = "1h", limit: int = 5) -> List[Dict]:
        """
        OI история: Binance(proxy) → Bybit → OKX(direct).
        Возвращает [{sumOpenInterest: float}, ...] — старый формат сохранён.
        Поддерживаемые period: 5m, 15m, 30m, 1h, 4h, 1d
        
        ✅ v3.2: Whitelist check — пропускаем Binance если символа нет на бирже
        """
        await self._init_source()

        # ── 1. Binance via proxy (с whitelist проверкой) ──────────────────────
        if self._use_binance:
            is_available = await self._is_binance_symbol_available(symbol)
            if is_available:
                d = await self._binance("/fapi/v1/openInterestHist",
                                        {"symbol": symbol, "period": period, "limit": limit})
                if d and len(d) >= 2:
                    return d
                logger.info(f"[OI] {symbol}: Binance вернул пусто/404 → fallback к Bybit/OKX")
            else:
                logger.info(f"[OI] {symbol}: не на Binance → сразу к Bybit/OKX")

        # ── 2. Bybit fallback ──────────────────────────────────────────────────
        imap_bybit = {"5m": "5min", "15m": "15min", "30m": "30min",
                      "1h": "1h", "4h": "4h", "1d": "1d"}
        result = await self._bybit("/v5/market/open-interest",
                                   {"category": "linear", "symbol": symbol,
                                    "intervalTime": imap_bybit.get(period, "1h"),
                                    "limit": limit})
        if result:
            items = result.get("list", [])
            if len(items) >= 2:
                logger.debug(f"[OI] {symbol}: Bybit fallback ✅")
                # ✅ FIX S9: Bybit возвращает newest-first (как и свечи).
                # Разворачиваем → oldest-first, чтобы _analyze_oi_trend и get_oi_change
                # корректно считали change_pct = (newest - oldest) / oldest.
                return list(reversed([
                    {"sumOpenInterest": float(item.get("openInterest", 0))}
                    for item in items
                ]))

        # ── 3. OKX direct fallback ────────────────────────────────────────────
        imap_okx = {"5m": "5m", "15m": "5m", "30m": "5m",   # OKX rubik min=5m
                    "1h": "1H", "4h": "4H", "1d": "1D"}
        okx_period = imap_okx.get(period, "1H")
        okx_limit  = max(limit, 5)
        inst_id = self._to_okx_instid(symbol)
        data = await self._okx(
            "/api/v5/rubik/stat/contracts/open-interest-volume",
            {"instId": inst_id, "period": okx_period, "limit": okx_limit}  # instId = BTC-USDT-SWAP
        )
        if data and len(data) >= 2:
            # data format: [[ts, oi, vol], ...]  newest first → reverse
            rows = list(reversed(data))
            logger.info(f"[OI] {symbol}: OKX fallback ")
            return [{"sumOpenInterest": float(row[1])} for row in rows if len(row) >= 2]

        logger.info(f"[OI] {symbol}: все источники пустые")
        return []

    async def get_oi_change(self, symbol: str, days: int = 4) -> float:
        """OI изменение за N дней. Цепочка: 1d → 4h → 1h fallback."""
        history = await self.get_open_interest_history(symbol, "1d", days + 1)
        if not history or len(history) < 2:
            logger.debug(f"[OI] {symbol}: '1d' пустой → fallback '4h'")
            history = await self.get_open_interest_history(symbol, "4h", (days + 1) * 6)
        if not history or len(history) < 2:
            logger.debug(f"[OI] {symbol}: '4h' пустой → fallback '1h'")
            history = await self.get_open_interest_history(symbol, "1h", 24)
        if not history or len(history) < 2:
            return 0.0
        old = float(history[0].get("sumOpenInterest", 0))
        new = float(history[-1].get("sumOpenInterest", 0))
        return round((new - old) / old * 100, 2) if old else 0.0

    async def get_oi_short_term(self, symbol: str) -> Dict[str, float]:
        """
        Краткосрочные OI-изменения для Aegis-скора.
        Возвращает {'oi_15m', 'oi_30m', 'oi_1h', 'oi_4h'} в %.
        Порядок от быстрого к медленному: 15m → 30m → 1h → 4h.
        """
        results = {"oi_15m": 0.0, "oi_30m": 0.0, "oi_1h": 0.0, "oi_4h": 0.0}
        try:
            # 15m — самый быстрый: 3 свечи (15m × 3 = 45 мин окно, берём первую и последнюю)
            h15 = await self.get_open_interest_history(symbol, "15m", 4)
            if h15 and len(h15) >= 2:
                old = float(h15[0].get("sumOpenInterest", 0))
                new = float(h15[-1].get("sumOpenInterest", 0))
                results["oi_15m"] = round((new - old) / old * 100, 2) if old else 0.0

            # 30m — 2 свечи
            h30 = await self.get_open_interest_history(symbol, "30m", 4)
            if h30 and len(h30) >= 2:
                old = float(h30[0].get("sumOpenInterest", 0))
                new = float(h30[-1].get("sumOpenInterest", 0))
                results["oi_30m"] = round((new - old) / old * 100, 2) if old else 0.0

            # 1h
            h1 = await self.get_open_interest_history(symbol, "1h", 6)
            if h1 and len(h1) >= 2:
                old = float(h1[0].get("sumOpenInterest", 0))
                new = float(h1[-1].get("sumOpenInterest", 0))
                results["oi_1h"] = round((new - old) / old * 100, 2) if old else 0.0

            # 4h
            h4 = await self.get_open_interest_history(symbol, "4h", 6)
            if h4 and len(h4) >= 2:
                old = float(h4[0].get("sumOpenInterest", 0))
                new = float(h4[-1].get("sumOpenInterest", 0))
                results["oi_4h"] = round((new - old) / old * 100, 2) if old else 0.0
        except Exception as e:
            logger.debug(f"[OI short-term] {symbol}: {e}")
        return results

    # =========================================================================
    # LONG/SHORT RATIO
    # =========================================================================

    async def get_long_short_ratio(self, symbol: str, period: str = "1h") -> Optional[float]:
        """
        L/S ratio (% лонгов, 0-100): Binance(proxy) → Bybit → OKX.
        Санитарный диапазон 10-90%, вне — возврат 50.0 (дефолт).
        
        ✅ v3.2: Whitelist check — пропускаем Binance если символа нет
        """
        await self._init_source()

        # ── 1. Binance (с whitelist проверкой) ────────────────────────────────
        if self._use_binance:
            is_available = await self._is_binance_symbol_available(symbol)
            if is_available:
                d = await self._binance("/futures/data/topLongShortAccountRatio",
                                        {"symbol": symbol, "period": period, "limit": 1})
                if d and len(d) > 0:
                    raw = d[0].get("longAccount", 0)
                    val = float(raw) * 100 if float(raw) <= 1 else float(raw)
                    if 10 <= val <= 90:
                        logger.debug(f"[LS] {symbol}: Binance → {val:.1f}%")
                        return round(val, 1)
                logger.info(f"[LS] {symbol}: Binance пусто/404 → fallback к Bybit/OKX")
            else:
                logger.info(f"[LS] {symbol}: не на Binance → сразу к Bybit/OKX")

        # ── 2. Bybit fallback ──────────────────────────────────────────────────
        imap = {"5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h", "4h": "4h"}
        result = await self._bybit("/v5/market/account-ratio",
                                   {"category": "linear", "symbol": symbol,
                                    "period": imap.get(period, "1h"), "limit": 1})
        if result:
            items = result.get("list", [])
            if items:
                buy = items[0].get("buyRatio")
                if buy:
                    val = float(buy) * 100
                    if 10 <= val <= 90:
                        logger.debug(f"[LS] {symbol}: Bybit → {val:.1f}%")
                        return round(val, 1)

        # ── 3. OKX direct ─────────────────────────────────────────────────────
        okx_period = {"5m": "5m", "15m": "15m", "30m": "30m",
                      "1h": "1H", "4h": "4H"}.get(period, "1H")
        inst_id = self._to_okx_instid(symbol)
        data = await self._okx(
            "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract",
            {"instId": inst_id, "period": okx_period, "limit": 1}
        )
        if data and len(data) > 0:
            row = data[0]   # [ts, ratio_str]  ratio = longs/shorts
            if len(row) >= 2:
                try:
                    ratio = float(row[1])
                    pct = ratio / (1.0 + ratio) * 100
                    if 10 <= pct <= 90:
                        logger.debug(f"[LS] {symbol}: OKX → {pct:.1f}%")
                        return round(pct, 1)
                except (ValueError, ZeroDivisionError):
                    pass

        logger.debug(f"[LS] {symbol}: все источники None → default 50.0")
        return 50.0

    async def get_taker_buy_sell_ratio(self, symbol: str,
                                        period: str = "15m") -> Optional[float]:
        """
        Taker Buy/Sell ratio (0=продавцы, 1=покупатели).
        Цепочка: Binance(proxy) → Bybit account-ratio (прокси) → None.

        ✅ FIX: Добавлен Bybit fallback через account-ratio.
        Bybit не публикует taker volume публично, но account-ratio
        (доля покупателей по счетам) коррелирует с taker direction.
        Нормализован в тот же диапазон 0–1.
        """
        await self._init_source()

        # ── 1. Binance (с whitelist проверкой) ────────────────────────────────
        if self._use_binance:
            is_available = await self._is_binance_symbol_available(symbol)
            if is_available:
                d = await self._binance("/futures/data/takerBuySellVolRatio",
                                        {"symbol": symbol, "period": period, "limit": 1})
                if d and len(d) > 0:
                    buy_vol  = float(d[0].get("buyVol",  0))
                    sell_vol = float(d[0].get("sellVol", 0))
                    total = buy_vol + sell_vol
                    if total > 0:
                        ratio = buy_vol / total
                        logger.debug(f"[Taker] {symbol}: Binance buy={buy_vol:.0f} sell={sell_vol:.0f} → {ratio:.3f}")
                        return ratio
                logger.info(f"[Taker] {symbol}: Binance пусто/404 → Bybit fallback")

        # ── 2. Bybit account-ratio как прокси для taker direction ─────────────
        # buyRatio = доля аккаунтов в лонге. Коррелирует с taker buy pressure.
        imap = {"5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h", "4h": "4h"}
        result = await self._bybit("/v5/market/account-ratio",
                                   {"category": "linear", "symbol": symbol,
                                    "period": imap.get(period, "15min"), "limit": 1})
        if result:
            items = result.get("list", [])
            if items:
                buy = items[0].get("buyRatio")
                if buy is not None:
                    val = float(buy)  # уже 0–1
                    if 0.0 < val < 1.0:
                        logger.debug(f"[Taker] {symbol}: Bybit account-ratio proxy → {val:.3f}")
                        return round(val, 3)

        logger.info(f"[Taker] {symbol}: все источники пустые → None")
        return None

    async def get_liquidations(self, symbol: str,
                                limit: int = 100,
                                period: str = "1h") -> Optional[Dict]:
        """
        Ликвидации: Binance(proxy) → Bybit → OKX.
        Поддерживаемые period: 30m, 1h, 4h, 1d

        ✅ FIX: Добавлен Bybit как шаг 2 (/v5/market/liquidation — публичный)
        ✅ v3.2: Whitelist check — пропускаем Binance если символа нет
        ✅ v3.3: Добавлены ТФ 30m, 4h, 1d
        ✅ v3.4: Детальное логирование OKX + видимые ошибки OKX
        """
        await self._init_source()

        # Маппинг period → секунды
        period_seconds = {"30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}
        seconds = period_seconds.get(period, 3600)
        start_time_ms = int((time.time() - seconds) * 1000)

        # ── 1. Binance (с whitelist проверкой) ───────────────────────────────
        if self._use_binance:
            is_available = await self._is_binance_symbol_available(symbol)
            if not is_available:
                logger.info(f"[Liq] {symbol}: не на Binance → сразу к Bybit")
            else:
                try:
                    d = await self._binance("/fapi/v1/allForceOrders",
                                            {"symbol": symbol,
                                             "startTime": start_time_ms,
                                             "limit": limit})
                    if d:
                        long_liq = short_liq = 0.0
                        for order in d:
                            qty   = float(order.get("origQty",  0))
                            price = float(order.get("avgPrice", 0))
                            side  = order.get("side", "").upper()
                            usd   = qty * price
                            if side == "SELL":   long_liq  += usd
                            else:                short_liq += usd
                        total = long_liq + short_liq
                        if total > 0:
                            logger.info(f"[Liq] {symbol}: Binance ✅ total=${total:,.0f}")
                            return {
                                "total_usd":   total,
                                "long_liq_usd":  long_liq,
                                "short_liq_usd": short_liq,
                                "dominant_side": "LONG" if long_liq > short_liq
                                                 else "SHORT" if short_liq > long_liq else None
                            }
                        logger.info(f"[Liq] {symbol}: Binance пусто/404 → fallback к OKX")
                except Exception as e:
                    logger.info(f"[Liq] {symbol}: Binance error — {e} → fallback к OKX")

        # ── 2. Bybit liquidation (публичный endpoint, не требует auth) ─────────
        # GET /v5/market/liquidation?category=linear&symbol=BTCUSDT&limit=200
        # side=Buy  → ликвидация лонга (лонгист получил маржин-колл)
        # side=Sell → ликвидация шорта
        try:
            # ✅ FIX БАГ 1: Добавлен startTime — без него Bybit возвращает 404
            # когда нет ТЕКУЩИХ форсированных ликвидаций в эту секунду
            _start_ms = str(int((time.time() - seconds) * 1000))
            bybit_result = await self._bybit("/v5/market/liquidation",
                                             {"category": "linear", "symbol": symbol,
                                              "limit": "200", "startTime": _start_ms})
            if bybit_result:
                items = bybit_result.get("list", [])
                if items:
                    long_liq = short_liq = 0.0
                    cutoff_ms = int((time.time() - seconds) * 1000)
                    for item in items:
                        try:
                            ts  = int(item.get("updatedTime", 0))
                            if ts < cutoff_ms:
                                continue
                            sz    = float(item.get("size",  0))
                            price = float(item.get("price", 0))
                            side  = item.get("side", "")
                            usd   = sz * price
                            if side == "Buy":    long_liq  += usd   # Buy закрывает SHORT позицию (ликв. шорта)
                            elif side == "Sell": short_liq += usd   # Sell закрывает LONG позицию (ликв. лонга)
                        except (ValueError, TypeError):
                            continue
                    total = long_liq + short_liq
                    if total > 0:
                        logger.info(f"[Liq] {symbol}: Bybit ✅ total=${total:,.0f} (long=${long_liq:,.0f}, short=${short_liq:,.0f})")
                        return {
                            "total_usd":     total,
                            "long_liq_usd":  long_liq,
                            "short_liq_usd": short_liq,
                            "dominant_side": "LONG" if long_liq > short_liq
                                             else "SHORT" if short_liq > long_liq else None
                        }
                    logger.info(f"[Liq] {symbol}: Bybit вернул данные но ликвидации=0 в период {period} → OKX")
        except Exception as e:
            logger.info(f"[Liq] {symbol}: Bybit error — {e} → OKX")

        # ── 3. OKX WebSocket кеш (Redis) ─────────────────────────────────
        # REST /api/v5/public/liquidation-orders удалён OKX в 2023 → 400.
        # Вместо REST используем WebSocket feed → данные в Redis okx:liq:{symbol}
        # OKXLiquidationFeed пишет данные при каждой реальной ликвидации.
        # Если WS ещё не подключён или для этой пары нет данных → пропускаем.
        try:
            from utils.okx_liquidation_ws import get_okx_liq_from_redis
            # redis_client нужен из внешнего state — передаём через self._okx_redis если задан
            _redis = getattr(self, "_okx_redis", None)
            if _redis:
                okx_cached = get_okx_liq_from_redis(_redis, symbol)
                if okx_cached:
                    total = okx_cached["total_usd"]
                    logger.info(f"[Liq] {symbol}: OKX WS cache ✅ total=${total:,.0f}")
                    return okx_cached
        except Exception as e:
            logger.debug(f"[Liq] {symbol}: OKX cache error — {e}")

        logger.debug(f"[Liq] {symbol}: все источники пустые")
        return None

    async def get_top_trader_position_ratio(self, symbol: str,
                                             period: str = "15m") -> Optional[float]:
        """
        Топ-трейдеры Long/Short Position Ratio: Binance(proxy) → Bybit → OKX.
        >1.5 = топы в лонгах, <0.8 = топы в шортах.
        """
        await self._init_source()

        # ── 1. Binance ────────────────────────────────────────────────────────
        if self._use_binance:
            d = await self._binance("/futures/data/topLongShortPositionRatio",
                                    {"symbol": symbol, "period": period, "limit": 1})
            if d and len(d) > 0:
                long_pos  = float(d[0].get("longPosition",  0))
                short_pos = float(d[0].get("shortPosition", 0))
                if short_pos > 0:
                    ratio = long_pos / short_pos
                    logger.debug(f"[TopTrader] {symbol}: Binance → {ratio:.3f}")
                    return round(ratio, 3)

        # ── 2. Bybit (account-ratio как прокси) ──────────────────────────────
        imap = {"5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h", "4h": "4h"}
        result = await self._bybit("/v5/market/account-ratio",
                                   {"category": "linear", "symbol": symbol,
                                    "period": imap.get(period, "1h"), "limit": 1})
        if result:
            items = result.get("list", [])
            if items:
                buy = items[0].get("buyRatio")
                if buy:
                    buy_f = float(buy)
                    if 0 < buy_f < 1:
                        ratio = buy_f / (1.0 - buy_f)
                        logger.debug(f"[TopTrader] {symbol}: Bybit → {ratio:.3f}")
                        return round(ratio, 3)

        # ── 3. OKX direct — elite account ratio ───────────────────────────────
        okx_period = {"5m": "5m", "15m": "5m", "30m": "5m",
                      "1h": "1H", "4h": "4H"}.get(period, "5m")
        inst_id = self._to_okx_instid(symbol)
        data = await self._okx(
            "/api/v5/rubik/stat/contracts/top-long-short-account-ratio",
            {"instId": inst_id, "period": okx_period, "limit": 1}
        )
        if data and len(data) > 0:
            row = data[0]   # [ts, ratio_str]
            if len(row) >= 2:
                try:
                    ratio = float(row[1])
                    if ratio > 0:
                        logger.debug(f"[TopTrader] {symbol}: OKX elite → {ratio:.3f}")
                        return round(ratio, 3)
                except (ValueError, IndexError):
                    pass

        logger.debug(f"[TopTrader] {symbol}: все источники None")
        return None

    # =========================================================================
    # VOLUME PROFILE
    # =========================================================================

    async def get_hourly_volume_profile(self, symbol: str, hours: int = 7) -> List[float]:
        """
        ✅ FIX #3: Возвращает NET DELTA (buy_vol - sell_vol) для часовых свечей.
        SHORT scorer ищет отрицательные значения (продавцы доминируют).
        Если taker данные недоступны — используем price_action как прокси:
          - цена выросла за час → delta > 0 (покупатели)
          - цена упала за час   → delta < 0 (продавцы)
        """
        try:
            candles = await self.get_klines(symbol, "1h", hours + 2)
            if candles and len(candles) >= hours:
                result = []
                for c in candles[-hours:]:
                    # Price-action proxy: бычья свеча = позитивная дельта
                    price_delta_pct = (c.close - c.open) / c.open if c.open > 0 else 0
                    # Масштабируем объём знаком price direction
                    net_delta = c.quote_volume * (1 if price_delta_pct >= 0 else -1)
                    result.append(net_delta)
                return result
        except Exception:
            pass
        return [0.0] * hours

    # =========================================================================
    # BREAKOUT / MOMENTUM ДАННЫЕ  ← NEW v2.1
    # =========================================================================

    @staticmethod
    def _calc_atr(candles: List[CandleData], period: int = 14) -> float:
        """ATR(14) — Average True Range."""
        if len(candles) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(candles)):
            prev_close = candles[i-1].close
            tr = max(
                candles[i].high - candles[i].low,
                abs(candles[i].high - prev_close),
                abs(candles[i].low  - prev_close),
            )
            trs.append(tr)
        return sum(trs[-period:]) / period

    @staticmethod
    def _calc_volume_spike(candles: List[CandleData], lookback: int = 20) -> float:
        """
        Отношение объёма последней свечи к среднему за lookback свечей.
        >2.0 = volume spike, >5.0 = экстремальный spike.
        """
        if len(candles) < lookback + 1:
            return 1.0
        vols = [c.quote_volume for c in candles[-(lookback+1):-1]]
        if not vols:
            return 1.0
        avg_vol = sum(vols) / len(vols)
        if avg_vol <= 0:
            return 1.0
        return round(candles[-1].quote_volume / avg_vol, 2)

    async def get_breakout_data(self, symbol: str) -> Optional[BreakoutData]:
        """
        Быстро получить данные для breakout/momentum проверки.
        Использует 15м свечи и 1h RSI.
        Вызывается из scan_symbol ПЕРЕД основным анализом (быстрая фильтрация).
        """
        try:
            # Параллельно: 15м свечи + 1h свечи + 24h тикер + фандинг
            results = await asyncio.gather(
                self.get_klines(symbol, "15m", 50),
                self.get_klines(symbol, "1h", 50),
                self.get_24h_ticker(symbol),
                self.get_funding_rate(symbol),
                return_exceptions=True
            )
            candles_15m, candles_1h, ticker, funding = results

            if isinstance(candles_15m, Exception) or not candles_15m or len(candles_15m) < 25:
                return None
            if isinstance(candles_1h, Exception) or not candles_1h or len(candles_1h) < 15:
                return None

            price = candles_15m[-1].close

            # Volume spike на 15м
            vol_spike = self._calc_volume_spike(candles_15m, lookback=20)

            # Изменение цены: 15м и 1ч
            price_15m_ago  = candles_15m[-2].close  if len(candles_15m) >= 2  else price
            price_1h_ago   = candles_15m[-5].close  if len(candles_15m) >= 5  else price
            price_chg_15m  = (price - price_15m_ago) / price_15m_ago * 100 if price_15m_ago else 0
            price_chg_1h   = (price - price_1h_ago) / price_1h_ago   * 100 if price_1h_ago  else 0

            # ATR 15м как % от цены
            atr = self._calc_atr(candles_15m, 14)
            atr_pct = (atr / price * 100) if price else 0.5

            # Тело последней 15м свечи как доля ATR
            last = candles_15m[-1]
            body = abs(last.close - last.open)
            body_pct = (body / atr) if atr > 0 else 0.5

            # RSI 1h
            rsi = self._calculate_rsi([c.close for c in candles_1h])

            # 24h хай/лоу
            high_24h = low_24h = 0.0
            if isinstance(ticker, dict):
                high_24h = float(ticker.get("highPrice", 0))
                low_24h  = float(ticker.get("lowPrice", 0))

            # Расстояние до экстремумов
            near_high = high_24h > 0 and (high_24h - price) / high_24h * 100 < 1.0
            near_low  = low_24h  > 0 and (price - low_24h) / low_24h  * 100 < 1.0

            vol_15m = [c.quote_volume for c in candles_15m[-20:]]

            return BreakoutData(
                symbol=symbol,
                price=price,
                volume_spike_ratio=vol_spike,
                price_change_1h=round(price_chg_1h, 3),
                price_change_15m=round(price_chg_15m, 3),
                atr_14_pct=round(atr_pct, 3),
                candle_body_pct=round(body_pct, 3),
                is_near_high_24h=near_high,
                is_near_low_24h=near_low,
                rsi_1h=rsi or 50.0,
                funding_rate=float(funding) * 100 if isinstance(funding, float) else 0.0,
                volume_15m_candles=vol_15m,
            )
        except Exception as e:
            print(f"[BreakoutData] {symbol}: {e}")
            return None

    async def get_volume_spike_ratio(self, symbol: str,
                                      interval: str = "15m",
                                      lookback: int = 20) -> float:
        """
        Быстро получить volume spike ratio для символа.
        Используй если нужна только эта метрика.
        """
        try:
            candles = await self.get_klines(symbol, interval, lookback + 2)
            if not candles or len(candles) < lookback + 1:
                return 1.0
            return self._calc_volume_spike(candles, lookback)
        except Exception:
            return 1.0

    # =========================================================================
    # COMPLETE MARKET DATA — расширен breakout метриками
    # =========================================================================

    async def get_complete_market_data(self, symbol: str) -> Optional[MarketData]:
        """
        Полные рыночные данные v3.1.
        Добавлено: dead symbol cache, OI short-term, тройной fallback.
        """
        try:
            # ── Dead symbol cache ─────────────────────────────────────────────
            if self._is_dead_symbol(symbol):
                logger.debug(f"[Market] {symbol}: пропуск (dead symbol cache)")
                return None

            await self._init_source()

            results = await asyncio.gather(
                self.get_price(symbol),
                self.get_funding_rate(symbol),
                self.get_open_interest(symbol),
                self.get_long_short_ratio(symbol),
                self.get_24h_ticker(symbol),
                self.get_klines(symbol, "1h", 100),
                self.get_klines(symbol, "15m", 75),   # 75 свечей = 18.75ч
                self.get_klines(symbol, "30m", 75),   # 30m + ATR
                self.get_klines(symbol, "4h",  50),   # 4H HTF structure
                self.get_klines(symbol, "1d",  35),   # 35 дней PDH/PDL/ATH
                self.get_klines(symbol, "1w",  20),   # 20 недель — Weekly SNR/OB/FVG
                self.get_klines(symbol, "1M",   6),   # 6 месяцев — Monthly levels
                return_exceptions=True
            )

            price, funding, oi, ratio, ticker, klines_1h, klines_15m, \
                klines_30m, klines_4h, klines_1d, klines_1w, klines_1M = results

            if isinstance(price, Exception) or not price:
                self._mark_symbol_fail(symbol)
                return None
            if isinstance(klines_1h, Exception) or not klines_1h or len(klines_1h) < 20:
                self._mark_symbol_fail(symbol)
                return None

            self._mark_symbol_ok(symbol)   # сбрасываем счётчик фейлов

            funding   = None if isinstance(funding,   Exception) else funding
            oi        = None if isinstance(oi,        Exception) else oi
            ratio     = None if isinstance(ratio,     Exception) else ratio
            ticker    = None if isinstance(ticker,    Exception) else ticker
            klines_15m = [] if isinstance(klines_15m, Exception) else (klines_15m or [])
            klines_30m = [] if isinstance(klines_30m, Exception) else (klines_30m or [])
            klines_4h  = [] if isinstance(klines_4h,  Exception) else (klines_4h  or [])
            klines_1d  = [] if isinstance(klines_1d,  Exception) else (klines_1d  or [])
            klines_1w  = [] if isinstance(klines_1w,  Exception) else (klines_1w  or [])
            klines_1M  = [] if isinstance(klines_1M,  Exception) else (klines_1M  or [])

            rsi = self._calculate_rsi([c.close for c in klines_1h])

            funding_acc = await self.get_accumulated_funding(symbol, 4)
            oi_change   = await self.get_oi_change(symbol, 4)
            hourly_vols = await self.get_hourly_volume_profile(symbol, 7)

            # ── Realtime метрики (параллельно для скорости) ───────────────────
            realtime_results = await asyncio.gather(
                self.get_taker_buy_sell_ratio(symbol, "15m"),
                self.get_liquidations(symbol, 100),
                self.get_top_trader_position_ratio(symbol, "15m"),
                self.get_oi_short_term(symbol),      # ← NEW: краткосрочный OI
                return_exceptions=True
            )
            taker_ratio, liq_data, top_trader_ls, oi_short = realtime_results

            # Обработка realtime метрик
            taker_buy_sell_ratio = None if isinstance(taker_ratio, Exception) else taker_ratio
            recent_liquidations_usd = None
            liq_side = None
            if not isinstance(liq_data, Exception) and liq_data:
                recent_liquidations_usd = liq_data.get("total_usd")
                liq_side = liq_data.get("dominant_side")
            top_trader_long_short_ratio = None if isinstance(top_trader_ls, Exception) else top_trader_ls


            # ── Breakout метрики из 15м ───────────────────────────────────────
            vol_spike      = 1.0
            price_chg_1h   = 0.0
            atr_pct        = 0.5
            body_pct       = 0.5
            vol_15m_list   = []
            high_24h       = 0.0
            low_24h        = 0.0
            pct_from_high  = 5.0
            pct_from_low   = 5.0

            if klines_15m and len(klines_15m) >= 20:
                vol_spike    = self._calc_volume_spike(klines_15m, 20)
                atr_raw      = self._calc_atr(klines_15m, 14)
                atr_pct      = (atr_raw / float(price) * 100) if float(price) > 0 else 0.5

                last = klines_15m[-1]
                body = abs(last.close - last.open)
                body_pct = (body / atr_raw) if atr_raw > 0 else 0.5

                price_1h_ago = klines_15m[-5].close if len(klines_15m) >= 5 else float(price)
                price_chg_1h = (float(price) - price_1h_ago) / price_1h_ago * 100 if price_1h_ago else 0
                vol_15m_list = [c.quote_volume for c in klines_15m[-20:]]

            if isinstance(ticker, dict):
                high_24h = float(ticker.get("highPrice", 0))
                low_24h  = float(ticker.get("lowPrice",  0))
                if high_24h > 0:
                    pct_from_high = (high_24h - float(price)) / high_24h * 100
                if low_24h > 0:
                    pct_from_low  = (float(price) - low_24h) / low_24h  * 100

            # ── Обработка oi_short ────────────────────────────────────────────
            oi_st = {} if isinstance(oi_short, Exception) or not oi_short else oi_short

            # ── Market Structure Analysis (HTF) ──────────────────────────────
            ms_result = None
            if _MS_AVAILABLE:
                try:
                    ms_result = compute_market_structure(
                        price=float(price),
                        klines_30m=klines_30m,
                        klines_1h=klines_1h,
                        klines_4h=klines_4h,
                        klines_1d=klines_1d,
                        klines_1w=klines_1w or None,
                        klines_1M=klines_1M or None,
                    )
                    if ms_result.key_levels:
                        nearest = ms_result.key_levels[:3]
                        lvl_str = ", ".join(f"{lbl}={px:.4f}" for px, lbl in nearest)
                        logger.debug(f"[MS] {symbol}: {lvl_str} | HTF={ms_result.htf_structure} | 4H={ms_result.zone_4h} | 1W={ms_result.zone_weekly} | conf_s={ms_result.confluence_short}")
                except Exception as _e:
                    logger.debug(f"[MS] {symbol} error: {_e}")

            # ── Cascade Signal (4H Fractal Raid → 1H SNR → 15M FVG) ─────────
            cascade_result = None
            if _MS_AVAILABLE and ms_result:
                try:
                    cascade_result = detect_cascade_signal(
                        price=float(price),
                        klines_15m=klines_15m,
                        klines_1h=klines_1h,
                        klines_4h=klines_4h,
                        klines_1d=klines_1d,
                        ms=ms_result,
                    )
                    if cascade_result and cascade_result.has_signal:
                        logger.info(f"[CASCADE] {symbol}: {cascade_result.description} (bonus=+{cascade_result.score_bonus})")
                except Exception as _ce:
                    pass

            return MarketData(
                symbol=symbol,
                price=float(price),
                rsi_1h=rsi,
                funding_rate=round(float(funding) * 100, 4) if funding else 0.0,
                funding_accumulated=funding_acc,
                open_interest=float(oi) if oi else 0.0,
                oi_change_4d=oi_change,
                # ✅ FIX L/S: санитарная проверка — если ratio вне 25-75% → 50 (API баг)
                long_short_ratio=float(ratio) if ratio and 10 <= float(ratio) <= 90 else 50.0,
                volume_24h=float(ticker.get("quoteVolume", 0)) if isinstance(ticker, dict) else 0.0,
                volume_change_24h=float(ticker.get("priceChangePercent", 0)) if isinstance(ticker, dict) else 0.0,
                price_change_24h=float(ticker.get("priceChangePercent", 0)) if isinstance(ticker, dict) else 0.0,
                hourly_deltas=hourly_vols,
                last_updated=datetime.utcnow(),
                # ── Breakout поля ────────────────────────────────────────────
                volume_spike_ratio=round(vol_spike, 2),
                price_change_1h=round(price_chg_1h, 3),
                atr_14_pct=round(atr_pct, 3),
                candle_body_pct=round(body_pct, 3),
                volume_15m_candles=vol_15m_list,
                high_24h=high_24h,
                low_24h=low_24h,
                pct_from_high_24h=round(pct_from_high, 3),
                pct_from_low_24h=round(pct_from_low, 3),
                # ── Realtime метрики ───────────────────────────────────────
                taker_buy_sell_ratio=taker_buy_sell_ratio,
                recent_liquidations_usd=recent_liquidations_usd,
                liq_side=liq_side,
                top_trader_long_short_ratio=top_trader_long_short_ratio,
                oi_change_15m=oi_st.get("oi_15m", 0.0),
                oi_change_30m=oi_st.get("oi_30m", 0.0),
                oi_change_1h=oi_st.get("oi_1h", 0.0),
                oi_change_4h=oi_st.get("oi_4h", 0.0),
                # ── Market Structure ─────────────────────────────────────────
                market_structure=ms_result,
                atr_30m_pct=ms_result.atr_30m_pct if ms_result else 0.0,
                cascade_signal=cascade_result,
            )
        except Exception as e:
            print(f"Market data error {symbol}: {e}")
            return None

    # =========================================================================
    # RSI
    # =========================================================================

    def _calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        if len(prices) < period + 1:
            return None
        deltas    = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        recent    = deltas[-period:]
        avg_gain  = sum(d for d in recent if d > 0) / period
        avg_loss  = sum(-d for d in recent if d < 0) / period
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)

    # =========================================================================
    # TREND — для мультитаймфрейм анализа
    # =========================================================================

    async def get_multi_tf_trend(self, symbol: str) -> Dict[str, str]:
        """
        Определить тренд на нескольких таймфреймах.
        Возвращает: {"15m": "up"/"down"/"flat", "1h": ..., "4h": ..., "1d": ...}
        Используется в scan_symbol для подтверждения направления.
        """
        result = {}
        for tf, limit in [("15m", 50), ("1h", 50), ("4h", 30), ("1d", 20)]:
            try:
                candles = await self.get_klines(symbol, tf, limit)
                if not candles or len(candles) < 20:
                    result[tf] = "flat"
                    continue
                # EMA 20 vs цена
                closes   = [c.close for c in candles]
                ema20    = self._calc_ema(closes, 20)
                price    = closes[-1]
                ema20_v  = ema20[-1] if ema20 else price
                slope    = (ema20[-1] - ema20[-5]) / ema20[-5] * 100 if len(ema20) >= 5 else 0
                if price > ema20_v and slope > 0.1:
                    result[tf] = "up"
                elif price < ema20_v and slope < -0.1:
                    result[tf] = "down"
                else:
                    result[tf] = "flat"
            except Exception:
                result[tf] = "flat"
        return result

    @staticmethod
    def _calc_ema(prices: List[float], period: int) -> List[float]:
        if len(prices) < period:
            return prices
        k = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        for p in prices[period:]:
            ema.append(p * k + ema[-1] * (1 - k))
        return ema

    async def get_price_trend(self, symbol: str, tf: str = "15m",
                               lookback: int = 20) -> str:
        """Быстрый тренд для одного TF: 'up' / 'down' / 'flat'."""
        try:
            candles = await self.get_klines(symbol, tf, lookback + 5)
            if not candles or len(candles) < lookback:
                return "flat"
            closes = [c.close for c in candles]
            ema    = self._calc_ema(closes, lookback)
            if len(ema) < 2:
                return "flat"
            slope = (ema[-1] - ema[-3]) / ema[-3] * 100 if len(ema) >= 3 else 0
            if closes[-1] > ema[-1] and slope > 0.05:
                return "up"
            elif closes[-1] < ema[-1] and slope < -0.05:
                return "down"
            return "flat"
        except Exception:
            return "flat"


# ============================================================================
# SINGLETON
# ============================================================================

_client = None

def get_binance_client() -> BinanceFuturesClient:
    global _client
    if _client is None:
        _client = BinanceFuturesClient()
    return _client
