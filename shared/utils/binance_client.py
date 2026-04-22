"""
Market Data Client v2.1 — Bybit (основной) + Binance через прокси

ИЗМЕНЕНИЯ v2.1:
  ✅ get_all_symbols: default min_volume снижен 5M → 300K (было 50 монет, стало 150-200)
  ✅ MarketData: добавлены поля для breakout/momentum анализа:
       volume_spike_ratio  — объём последней 15м свечи / средний объём (20 свечей)
       price_change_1h     — изменение цены за последний час %
       atr_14_pct          — ATR(14) на 15м как % от цены (волатильность)
       candle_body_pct     — тело последней свечи как % от ATR (сила свечи)
       volume_15m_candles  — последние 20 объёмов на 15м (для паттерн-детектора)
       high_24h / low_24h  — хай/лоу за сутки (для breakout зон)
  ✅ get_breakout_data()   — быстрый метод только для breakout проверки
  ✅ get_volume_spike_ratio() — отдельный метод для volume spike
  ✅ _symbols_bybit: возвращает до MAX_WATCHLIST символов (не ограничено 200)
"""

import os
import asyncio
import aiohttp
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime
import time
import statistics


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

    # ── Realtime метрики (из Pump Detector) ──────────────────────────────────
    # Taker buy/sell ratio: 0.0 = все продают, 1.0 = все покупают
    # >0.6 = агрессивные покупки (бычье давление), <0.4 = агрессивные продажи
    taker_buy_sell_ratio: Optional[float] = None

    # Ликвидации в USD за последний период (15m/1h)
    recent_liquidations_usd: Optional[float] = None

    # Сторона доминирующих ликвидаций: "LONG" | "SHORT" | None
    liq_side: Optional[str] = None

    # Top trader L/S ratio - "умные деньги" позиции
    # >1.5 = топы в лонгах, <0.8 = топы в шортах
    top_trader_long_short_ratio: Optional[float] = None


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
    Клиент рыночных данных.
    USE_BINANCE=false (default) → Bybit, без прокси
    USE_BINANCE=true            → Binance через прокси
    """

    BYBIT_URL   = "https://api.bybit.com"
    BINANCE_URL = "https://fapi.binance.com"

    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key or os.getenv("BINANCE_API_KEY", "")
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_request_time = 0.0
        self.min_request_interval = 0.05

        use_binance_env  = os.getenv("USE_BINANCE", "false").lower()
        self._try_binance = use_binance_env == "true"
        self._use_binance = False

        proxy_env = os.getenv("PROXY_LIST", "")
        self._proxies     = [p.strip() for p in proxy_env.split(",") if p.strip()]
        self._proxy_idx   = 0
        self._active_proxy: Optional[str] = None

        print(f"🔧 Market client: {'Binance+proxy' if self._try_binance else 'Bybit'} mode")

    def _next_proxy(self) -> Optional[str]:
        if not self._proxies:
            return None
        p = self._proxies[self._proxy_idx % len(self._proxies)]
        self._proxy_idx += 1
        return p

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

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def _init_source(self):
        if hasattr(self, '_source_ready'):
            return
        self._source_ready = True

        if not self._try_binance:
            self._use_binance = False
            print("✅ Data source: Bybit (default)")
            return

        for proxy in self._proxies[:3]:
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
            except Exception:
                continue

        self._use_binance = False
        print("⚠️ Binance unavailable. Falling back to Bybit.")

    # =========================================================================
    # BYBIT / BINANCE REQUESTS
    # =========================================================================

    _bybit_blocked: bool = False  # class-level flag: Bybit geo-blocked

    async def _bybit(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        await self._rate_limit()
        # ✅ FIX: if Bybit is geo-blocked (403), skip immediately - don't spam logs
        if BinanceFuturesClient._bybit_blocked:
            return None
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.BYBIT_URL}{endpoint}",
                params=params or {},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        return data.get("result")
                    return None
                if resp.status == 403:
                    # Geo-block: Render IP banned. Stop trying Bybit.
                    if not BinanceFuturesClient._bybit_blocked:
                        BinanceFuturesClient._bybit_blocked = True
                        print("⛔ Bybit geo-blocked (403). Switching to Binance-only mode.")
                    return None
                return None
        except Exception:
            return None

    async def _binance(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        await self._rate_limit()
        proxy = self._active_proxy or self._next_proxy()
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
                    return await resp.json()
                return None
        except Exception:
            return None

    async def _req(self, binance_ep: str, bybit_ep: str,
                   binance_params: Dict = None,
                   bybit_params: Dict = None) -> Optional[Any]:
        await self._init_source()
        if self._use_binance:
            return await self._binance(binance_ep, binance_params)
        return await self._bybit(bybit_ep, bybit_params)

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
        await self._init_source()
        if self._use_binance:
            d = await self._binance("/fapi/v1/openInterest", {"symbol": symbol})
            if d:
                return float(d.get("openInterest", 0))
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
        await self._init_source()
        if self._use_binance:
            d = await self._binance("/fapi/v1/openInterestHist",
                                    {"symbol": symbol, "period": period, "limit": limit})
            return d or []
        imap = {"5m": "5min", "15m": "15min", "30m": "30min",
                "1h": "1h", "4h": "4h", "1d": "1d"}
        result = await self._bybit("/v5/market/open-interest",
                                   {"category": "linear", "symbol": symbol,
                                    "intervalTime": imap.get(period, "1h"),
                                    "limit": limit})
        if result:
            return [{"sumOpenInterest": item.get("openInterest", 0)}
                    for item in result.get("list", [])]
        return []

    async def get_oi_change(self, symbol: str, days: int = 4) -> float:
        history = await self.get_open_interest_history(symbol, "1d", days + 1)
        if not history or len(history) < 2:
            return 0.0
        old = float(history[0].get("sumOpenInterest", 0))
        new = float(history[-1].get("sumOpenInterest", 0))
        return round((new - old) / old * 100, 2) if old else 0.0

    # =========================================================================
    # LONG/SHORT RATIO
    # =========================================================================

    async def get_long_short_ratio(self, symbol: str, period: str = "1h") -> Optional[float]:
        await self._init_source()
        if self._use_binance:
            d = await self._binance("/futures/data/topLongShortAccountRatio",
                                    {"symbol": symbol, "period": period, "limit": 1})
            if d and len(d) > 0:
                return float(d[0].get("longAccount", 0))
        result = await self._bybit("/v5/market/account-ratio",
                                   {"category": "linear", "symbol": symbol,
                                    "period": period, "limit": 1})
        if result:
            items = result.get("list", [])
            if items:
                buy = items[0].get("buyRatio")
                return float(buy) * 100 if buy else 50.0
        return 50.0

    async def get_taker_buy_sell_ratio(self, symbol: str,
                                        period: str = "15m") -> Optional[float]:
        """
        Получить Taker Buy/Sell Volume Ratio.
        0.0 = все продают агрессивно, 1.0 = все покупают агрессивно.
        """
        await self._init_source()
        if self._use_binance:
            d = await self._binance("/futures/data/takerBuySellVolRatio",
                                    {"symbol": symbol, "period": period, "limit": 1})
            if d and len(d) > 0:
                buy_vol = float(d[0].get("buyVol", 0))
                sell_vol = float(d[0].get("sellVol", 0))
                total = buy_vol + sell_vol
                return buy_vol / total if total > 0 else None
        return None

    async def get_liquidations(self, symbol: str,
                                limit: int = 100) -> Optional[Dict]:
        """
        Получить данные о ликвидациях за последние сделки.
        Возвращает сумму в USD и доминирующую сторону.
        """
        await self._init_source()
        if self._use_binance:
            try:
                d = await self._binance("/fapi/v1/allForceOrders",
                                        {"symbol": symbol, "limit": limit})
                if d:
                    long_liq = 0.0
                    short_liq = 0.0
                    for order in d:
                        qty = float(order.get("origQty", 0))
                        price = float(order.get("avgPrice", 0))
                        side = order.get("side", "").upper()
                        usd = qty * price
                        if side == "SELL":
                            # SELL liquidation = LONG position liquidated
                            long_liq += usd
                        else:
                            # BUY liquidation = SHORT position liquidated
                            short_liq += usd
                    total = long_liq + short_liq
                    return {
                        "total_usd": total,
                        "long_liq_usd": long_liq,
                        "short_liq_usd": short_liq,
                        "dominant_side": "LONG" if long_liq > short_liq else "SHORT" if short_liq > long_liq else None
                    }
            except Exception:
                pass
        return None

    async def get_top_trader_position_ratio(self, symbol: str,
                                             period: str = "15m") -> Optional[float]:
        """
        Получить Long/Short Position Ratio для топ-трейдеров.
        >1.5 = топы в лонгах, <0.8 = топы в шортах.
        """
        await self._init_source()
        if self._use_binance:
            d = await self._binance("/futures/data/topLongShortPositionRatio",
                                    {"symbol": symbol, "period": period, "limit": 1})
            if d and len(d) > 0:
                long_pos = float(d[0].get("longPosition", 0))
                short_pos = float(d[0].get("shortPosition", 0))
                return long_pos / short_pos if short_pos > 0 else None
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
        Полные рыночные данные.
        v2.1: добавлены breakout поля из 15м свечей.
        """
        try:
            await self._init_source()

            results = await asyncio.gather(
                self.get_price(symbol),
                self.get_funding_rate(symbol),
                self.get_open_interest(symbol),
                self.get_long_short_ratio(symbol),
                self.get_24h_ticker(symbol),
                self.get_klines(symbol, "1h", 100),
                self.get_klines(symbol, "15m", 50),   # ← NEW: 15м данные
                return_exceptions=True
            )

            price, funding, oi, ratio, ticker, klines_1h, klines_15m = results

            if isinstance(price, Exception) or not price:
                return None
            if isinstance(klines_1h, Exception) or not klines_1h or len(klines_1h) < 20:
                return None

            funding   = None if isinstance(funding,   Exception) else funding
            oi        = None if isinstance(oi,        Exception) else oi
            ratio     = None if isinstance(ratio,     Exception) else ratio
            ticker    = None if isinstance(ticker,    Exception) else ticker
            klines_15m = [] if isinstance(klines_15m, Exception) else (klines_15m or [])

            rsi = self._calculate_rsi([c.close for c in klines_1h])

            funding_acc = await self.get_accumulated_funding(symbol, 4)
            oi_change   = await self.get_oi_change(symbol, 4)
            hourly_vols = await self.get_hourly_volume_profile(symbol, 7)

            # ── Realtime метрики (параллельно для скорости) ───────────────────
            realtime_results = await asyncio.gather(
                self.get_taker_buy_sell_ratio(symbol, "15m"),
                self.get_liquidations(symbol, 100),
                self.get_top_trader_position_ratio(symbol, "15m"),
                return_exceptions=True
            )
            taker_ratio, liq_data, top_trader_ls = realtime_results

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
