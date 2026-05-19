"""
CoinGecko On-Chain Client v2.0  (#35 Active Addresses proxy added)
Реальные on-chain метрики через бесплатный CoinGecko API.

Данные: total_volumes за 14 дней
→ z-score vs rolling_avg_14d
→ z_score > +2.0 → аномальный ПРИТОК → давление продажи → SHORT сигнал / блок LONG
→ z_score < -1.5 → аномальный ОТТОК  → накопление       → LONG сигнал

Active Addresses proxy (#35):
→ Сравниваем avg объём последних 7 дней vs предыдущих 7 дней
→ Рост >20% = рост активности сети → накопление (LONG +5)
→ Падение >20% = снижение активности → распределение (SHORT +5 / блок LONG)
CoinGecko не даёт active_addresses в free tier — используем volume как proxy
(корреляция ~0.72 с активными адресами по Glassnode данным)

Rate limits: 10000 req/day (бесплатно)
Redis TTL: 1 час (данные медленно меняются)
"""
import os
import json
import math
import logging
import asyncio
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)

_ENABLE_ONCHAIN   = os.getenv("ENABLE_ONCHAIN", "true").lower() == "true"
_CG_BASE_URL      = "https://api.coingecko.com/api/v3"
_CG_API_KEY       = os.getenv("COINGECKO_API_KEY", "")            # demo key → x-cg-demo-api-key header
_REDIS_TTL        = int(os.getenv("ONCHAIN_REDIS_TTL", "3600"))   # 1 час
_FALLBACK_TTL     = int(os.getenv("ONCHAIN_FALLBACK_TTL", "10800"))  # 3ч — последнее известное z при сбое API
_Z_INFLOW_HIGH    = float(os.getenv("ONCHAIN_Z_INFLOW", "2.0"))   # z > этого = аномальный приток
_Z_OUTFLOW_LOW    = float(os.getenv("ONCHAIN_Z_OUTFLOW", "-1.5")) # z < этого = аномальный отток
_ADDR_ANOMALY_PCT = float(os.getenv("ONCHAIN_ADDR_ANOMALY_PCT", "20.0"))  # % для #35
_CMC_API_KEY      = os.getenv("COINMARKETCAP_API_KEY", "")        # CMC fallback
_CMC_BASE_URL     = "https://pro-api.coinmarketcap.com"
_CC_API_KEY       = os.getenv("CRYPTOCOMPARE_API_KEY", "")        # CryptoCompare fallback (3-й)
_CC_BASE_URL      = "https://min-api.cryptocompare.com"
_CG_ID_CACHE_TTL  = 86400                                          # 24ч — маппинг меняется редко

# ── B7 FIX: Rate limiting + exponential backoff для CoinGecko ─────────────────
# asyncio.Semaphore нельзя создавать при импорте (нет event loop) — lazy init
_CG_SEM: Optional[asyncio.Semaphore] = None


def _get_cg_sem() -> asyncio.Semaphore:
    global _CG_SEM
    if _CG_SEM is None:
        _CG_SEM = asyncio.Semaphore(2)   # max 2 параллельных запроса к CoinGecko
    return _CG_SEM


async def _cg_fetch(url: str, params: dict) -> Tuple[int, Any]:
    """
    B7 FIX: CoinGecko GET с semaphore + 0.5s throttle + exponential backoff на 429.

    Semaphore(2):  max 2 параллельных запроса (~4 req/s burst, CG demo limit: 500/min)
    Backoff:       429 → sleep 1s / 2s / 4s, до 3 попыток
    Returns:       (http_status, json_data) или (status, None) при ошибке
    """
    import aiohttp
    sem = _get_cg_sem()
    timeout = aiohttp.ClientTimeout(total=10)

    async with sem:
        await asyncio.sleep(0.5)    # inter-request throttle внутри semaphore
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(
                    timeout=timeout, headers=_cg_headers()
                ) as sess:
                    async with sess.get(url, params=params) as resp:
                        if resp.status == 200:
                            return 200, await resp.json()
                        if resp.status == 429:
                            wait = 2.0 ** attempt          # 1s → 2s → 4s
                            logger.warning(
                                f"[OnChain] 429 Too Many Requests — "
                                f"backoff {wait:.0f}s (попытка {attempt + 1}/3)"
                            )
                            await asyncio.sleep(wait)
                            continue                        # retry
                        return resp.status, None           # другой не-200 — не ретраим
            except asyncio.TimeoutError:
                logger.debug(f"[OnChain] _cg_fetch timeout: {url}")
                return 408, None
            except Exception as e:
                logger.debug(f"[OnChain] _cg_fetch error: {e}")
                return 0, None
        # Все 3 попытки на 429 исчерпаны
        logger.warning(f"[OnChain] 429 — все 3 попытки исчерпаны: {url}")
        return 429, None


def _cg_headers() -> dict:
    """Заголовки для CoinGecko API. Demo key снимает 429-лимит (30 req/min → 500/min)."""
    if _CG_API_KEY:
        return {"x-cg-demo-api-key": _CG_API_KEY}
    return {}


def _extract_base(symbol: str) -> str:
    """PLAYSOUTUSDT → PLAYSOUT, 1000SHIBUSDT → SHIB, 1000000XECUSDT → XEC"""
    return symbol.replace("USDT", "").replace("BUSD", "").replace("1000000", "").replace("1000", "").upper()


async def _search_coingecko_dynamic(base: str) -> Optional[str]:
    """Динамический поиск CoinGecko ID по тикеру через /api/v3/search."""
    try:
        url = f"{_CG_BASE_URL}/search"
        params = {"query": base}
        status, data = await _cg_fetch(url, params)
        if status != 200 or data is None:
            return None
        for coin in data.get("coins", []):
            if coin.get("symbol", "").upper() == base.upper():
                return coin["id"]
        return None
    except Exception as e:
        logger.debug(f"[OnChain] CG search {base}: {e}")
        return None


async def _get_cmc_volume(base: str) -> Optional[tuple]:
    """CMC fallback: возвращает (volume_24h, pct_change_24h) или None."""
    if not _CMC_API_KEY:
        return None
    try:
        import aiohttp
        url = f"{_CMC_BASE_URL}/v1/cryptocurrency/quotes/latest"
        headers = {"X-CMC_PRO_API_KEY": _CMC_API_KEY, "Accept": "application/json"}
        params = {"symbol": base, "convert": "USD"}
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(url, headers=headers, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        coins = data.get("data", {})
        if isinstance(coins, dict):
            entry = coins.get(base.upper())
            if entry:
                if isinstance(entry, list):
                    entry = entry[0]
                quote = entry.get("quote", {}).get("USD", {})
                vol    = float(quote.get("volume_24h", 0) or 0)
                pct    = float(quote.get("volume_change_24h", 0) or 0)
                return (vol, pct)
        return None
    except Exception as e:
        logger.debug(f"[OnChain] CMC {base}: {e}")
        return None


def _cmc_pct_to_z(pct_change_24h: float) -> float:
    """% изменение объёма CMC → приближённый z-score для совместимости."""
    if pct_change_24h >= 100:  return 2.5
    if pct_change_24h >= 50:   return 1.5
    if pct_change_24h >= 20:   return 0.8
    if pct_change_24h >= -20:  return 0.0
    if pct_change_24h >= -40:  return -1.0
    return -2.0


async def _get_cc_volumes(base: str) -> Optional[list]:
    """CryptoCompare fallback: возвращает список дневных объёмов (USD) за 14 дней или None."""
    if not _CC_API_KEY:
        return None
    try:
        import aiohttp
        url = f"{_CC_BASE_URL}/data/v2/histoday"
        headers = {"Authorization": f"Apikey {_CC_API_KEY}"}
        params = {"fsym": base, "tsym": "USD", "limit": 14}
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(url, headers=headers, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        if data.get("Response") != "Success":
            return None
        days = data.get("Data", {}).get("Data", [])
        if not days or len(days) < 5:
            return None
        # volumeto = объём в USD (покупки + продажи)
        return [float(d.get("volumeto", 0)) for d in days if d.get("volumeto", 0) > 0]
    except Exception as e:
        logger.debug(f"[OnChain] CryptoCompare {base}: {e}")
        return None


async def _resolve_dynamic_id(symbol: str, redis_client=None) -> Optional[str]:
    """
    Резолвит CoinGecko ID для неизвестного символа.
    Redis cache (24ч) → CoinGecko search API.
    Возвращает cg_id или None.
    """
    id_key = f"onchain:cg_id:{symbol}"
    if redis_client:
        try:
            cached = redis_client.get(id_key)
            if cached:
                val = cached if isinstance(cached, str) else cached.decode()
                return None if val == "NOT_FOUND" else val
        except Exception:
            pass
    base = _extract_base(symbol)
    cg_id = await _search_coingecko_dynamic(base)
    if redis_client:
        try:
            redis_client.setex(id_key, _CG_ID_CACHE_TTL, cg_id or "NOT_FOUND")
        except Exception:
            pass
    if cg_id:
        logger.info(f"[OnChain] {symbol}: CoinGecko search → {cg_id}")
    return cg_id

# Маппинг Binance symbol → CoinGecko id
_SYMBOL_MAP: Dict[str, str] = {
    # ── Топ L1/L2 ────────────────────────────────────────────────────────────
    "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "BNBUSDT": "binancecoin",
    "SOLUSDT": "solana", "XRPUSDT": "ripple", "DOGEUSDT": "dogecoin",
    "ADAUSDT": "cardano", "AVAXUSDT": "avalanche-2", "DOTUSDT": "polkadot",
    "MATICUSDT": "matic-network", "LINKUSDT": "chainlink", "LTCUSDT": "litecoin",
    "ATOMUSDT": "cosmos", "UNIUSDT": "uniswap", "NEARUSDT": "near",
    "APTUSDT": "aptos", "ARBUSDT": "arbitrum", "OPUSDT": "optimism",
    "INJUSDT": "injective-protocol", "SUIUSDT": "sui",
    "AAVEUSDT": "aave", "MKRUSDT": "maker", "LDOUSDT": "lido-dao",
    "FTMUSDT": "fantom", "SANDUSDT": "the-sandbox", "MANAUSDT": "decentraland",
    "AXSUSDT": "axie-infinity", "GALAUSDT": "gala", "IMXUSDT": "immutable-x",
    "GRTUSDT": "the-graph", "COMPUSDT": "compound-governance-token",
    "RUNEUSDT": "thorchain", "ORDIUSDT": "ordinals", "STXUSDT": "blockstack",
    "WLDUSDT": "worldcoin-wld", "TIAUSDT": "celestia", "SEIUSDT": "sei-network",
    # ── Дополнительные L1/L2 ─────────────────────────────────────────────────
    "TRXUSDT": "tron", "XLMUSDT": "stellar", "XMRUSDT": "monero",
    "ETCUSDT": "ethereum-classic", "VETUSDT": "vechain", "FILUSDT": "filecoin",
    "ICPUSDT": "internet-computer", "HBARUSDT": "hedera-hashgraph",
    "THETAUSDT": "theta-token", "EOSUSDT": "eos", "XTZUSDT": "tezos",
    "FLOWUSDT": "flow", "ALGOUSDT": "algorand", "ZILUSDT": "zilliqa",
    "WAVESUSDT": "waves", "ZRXUSDT": "0x", "KAVAUSDT": "kava",
    "ONTUSDT": "ontology", "BANDUSDT": "band-protocol",
    "KLAYUSDT": "klay-token", "ONEUSDT": "harmony", "DGBUSDT": "digibyte",
    "MINAUSDT": "mina-protocol", "EGLDUSDT": "elrond-erd-2",
    "CFXUSDT": "conflux-token", "METISUSDT": "metis-token",
    "GLMRUSDT": "moonbeam", "MOVRUSDT": "moonriver",
    # ── DeFi ─────────────────────────────────────────────────────────────────
    "CRVUSDT": "curve-dao-token", "SNXUSDT": "havven", "YFIUSDT": "yearn-finance",
    "BALUSDT": "balancer", "SUSHIUSDT": "sushi", "PERPUSDT": "perpetual-protocol",
    "DYDXUSDT": "dydx", "GMXUSDT": "gmx", "PENDLEUSDT": "pendle",
    "STGUSDT": "stargate-finance", "UMAUSDT": "uma", "LRCUSDT": "loopring",
    "WOOUSDT": "woo-network", "RAYDIUMUSDT": "raydium", "ORCAUSDT": "orca",
    "JUPUSDT": "jupiter-exchange-solana", "CAKEUSDT": "pancakeswap-token",
    "COTIUSDT": "coti", "COWUSDT": "cow-protocol",
    # ── AI / инфраструктура ───────────────────────────────────────────────────
    "FETUSDT": "fetch-ai", "AGIXUSDT": "singularitynet", "RENDERUSDT": "render-token",
    "AIOZUSDT": "aioz-network", "ARUSDT": "arweave", "STORJUSDT": "storj",
    "ENSUSDT": "ethereum-name-service", "QNTUSDT": "quant-network",
    "EIGENUSDT": "eigenlayer", "PYTHUSDT": "pyth-network",
    # ── Gaming / NFT / Метавёрс ───────────────────────────────────────────────
    "CHZUSDT": "chiliz", "AUDIOUSDT": "audius", "ALICEUSDT": "my-neighbor-alice",
    "VOXELUSDT": "voxies", "TLMUSDT": "alien-worlds", "HOOKUSDT": "hooked-protocol",
    "GALUSDT": "project-galaxy", "CYBERUSDT": "cyberconnect",
    # ── Layer2 / Bridge / Staking ─────────────────────────────────────────────
    "NTRNUSDT": "neutron-3", "MNTUSDT": "mantle", "ALTUSDT": "altlayer",
    "ZETAUSDT": "zetachain", "SKLUSDT": "skale", "ANKRUSDT": "ankr",
    "APEUSDT": "apecoin", "BATUSDT": "basic-attention-token",
    "BLURUSDT": "blur", "MASKUSDT": "mask-network",
    "ENAUSDT": "ethena", "ONDOUSDT": "ondo-finance",
    "TONUSDT": "the-open-network", "NOTUSDT": "notcoin",
    # ── Meme ─────────────────────────────────────────────────────────────────
    "1000SHIBUSDT": "shiba-inu", "1000PEPEUSDT": "pepe", "1000BONKUSDT": "bonk",
    "1000TURBOUSDT": "turbo",
    # ── Exchange tokens / CEX ─────────────────────────────────────────────────
    "CROUSDT": "crypto-com-chain",
    # ── Misc top-300 ─────────────────────────────────────────────────────────
    "FLRUSDT": "flare-networks", "BLASTUSDT": "blast",
    "SCUSDT": "siacoin", "CTCUSDT": "creditcoin-2",
    "ASTRUSDT": "astar", "JASMYUSDT": "jasmy", "IOTXUSDT": "iotex",
    "HOTUSDT": "holotoken", "TRBUSDT": "tellor", "NMRUSDT": "numeraire",
    "PEOPLEUSDT": "constitutiondao", "RSRUSDT": "reserve-rights-token",
    "LPTUSDT": "livepeer", "IDUSDT": "space-id", "SPELLUSDT": "spell-token",
    "IOSTUSDT": "iostoken", "DASHUSDT": "dash", "NKNUSDT": "nkn",
    "ARKMUSDT": "arkham", "ARPAUSDT": "arpa",
}


def _calc_z_score(values: list) -> float:
    """Вычисляет z-score последнего значения относительно всего ряда."""
    if not values or len(values) < 3:
        return 0.0
    try:
        n = len(values)
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / n
        std = math.sqrt(variance)
        if std == 0:
            return 0.0
        return (values[-1] - mean) / std
    except Exception:
        return 0.0


def _z_fallback_key(symbol: str) -> str:
    return f"onchain:vol_z_fallback:{symbol}"


def _save_fallback_z(redis_client, symbol: str, z: float, desc: str) -> None:
    """Сохраняем последнее успешное z_score как fallback (TTL=3ч)."""
    if not redis_client or z == 0.0:
        return
    try:
        redis_client.setex(
            _z_fallback_key(symbol),
            _FALLBACK_TTL,
            json.dumps({"z": z, "desc": desc}),
        )
    except Exception:
        pass


def _read_fallback_z(redis_client, symbol: str) -> Optional[Tuple[float, str]]:
    """Читаем последнее известное z_score при сбое API (до 3ч стабильности)."""
    if not redis_client:
        return None
    try:
        cached = redis_client.get(_z_fallback_key(symbol))
        if cached:
            data = json.loads(cached)
            z, desc = data["z"], data["desc"]
            # Помечаем как устаревшее, чтобы было видно в логах
            stale_desc = desc if "[↩кэш]" in desc else f"{desc} [↩кэш]"
            return z, stale_desc
    except Exception:
        pass
    return None


async def get_volume_z_score(
    symbol: str,
    redis_client=None,
    http_session=None,
) -> Tuple[float, str]:
    """
    Получает z-score объёма для символа через CoinGecko.

    Returns:
        (z_score: float, description: str)
        z_score > 2.0  → аномальный приток (SHORT сигнал / блок LONG)
        z_score < -1.5 → аномальный отток (LONG сигнал)
        0.0 = нет данных или нейтрально
    """
    if not _ENABLE_ONCHAIN:
        return 0.0, ""

    cache_key = f"onchain:vol_z:{symbol}"

    # Проверяем Redis кеш (до резолвинга ID — работает для всех источников)
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                data = json.loads(cached)
                return data["z"], data["desc"]
        except Exception:
            pass

    # Резолвим CoinGecko ID: статический маппинг → динамический поиск
    cg_id = _SYMBOL_MAP.get(symbol)
    if not cg_id:
        cg_id = await _resolve_dynamic_id(symbol, redis_client)

    # CMC fallback если CoinGecko не нашёл
    if not cg_id:
        base = _extract_base(symbol)
        cmc = await _get_cmc_volume(base)
        if cmc:
            _vol, _pct = cmc
            z = _cmc_pct_to_z(_pct)
            desc = f"OnChain(CMC): z≈{z:.1f} vol_chg={_pct:+.0f}%"
            logger.info(f"[OnChain] {symbol}: CMC fallback z≈{z:.1f} vol_chg={_pct:+.0f}%")
            if redis_client:
                try:
                    redis_client.setex(cache_key, _REDIS_TTL, json.dumps({"z": z, "desc": desc}))
                except Exception:
                    pass
            _save_fallback_z(redis_client, symbol, z, desc)
            return z, desc
        # CryptoCompare fallback (3-й) — даёт настоящий z-score из 14-дневной истории
        cc_vols = await _get_cc_volumes(base)
        if cc_vols:
            z = round(_calc_z_score(cc_vols), 2)
            if z > _Z_INFLOW_HIGH:
                desc = f"📥 OnChain(CC): z={z:.1f} аномальный ПРИТОК → давление продаж"
            elif z < _Z_OUTFLOW_LOW:
                desc = f"📤 OnChain(CC): z={z:.1f} аномальный ОТТОК → накопление"
            else:
                desc = f"OnChain(CC): z={z:.1f} нейтрально"
            logger.info(f"[OnChain] {symbol}: CryptoCompare fallback z={z:.2f}")
            if redis_client:
                try:
                    redis_client.setex(cache_key, _REDIS_TTL, json.dumps({"z": z, "desc": desc}))
                except Exception:
                    pass
            _save_fallback_z(redis_client, symbol, z, desc)
            return z, desc
        _fb = _read_fallback_z(redis_client, symbol)
        if _fb:
            logger.info(f"[OnChain] {symbol}: нет маппинга → fallback z={_fb[0]:.1f}")
            return _fb
        return 0.0, f"[OnChain] {symbol}: нет маппинга CoinGecko/CMC/CC"

    # Запрашиваем CoinGecko (B7 FIX: через _cg_fetch с semaphore + retry)
    try:
        url = f"{_CG_BASE_URL}/coins/{cg_id}/market_chart"
        params = {"vs_currency": "usd", "days": "14", "interval": "daily"}

        status, raw = await _cg_fetch(url, params)
        if status != 200 or raw is None:
            _fb = _read_fallback_z(redis_client, symbol)
            if _fb:
                logger.info(f"[OnChain] {symbol}: HTTP {status} → fallback z={_fb[0]:.1f}")
                return _fb
            return 0.0, f"[OnChain] HTTP {status}"

        volumes = raw.get("total_volumes", [])
        if not volumes or len(volumes) < 5:
            _fb = _read_fallback_z(redis_client, symbol)
            if _fb:
                logger.info(f"[OnChain] {symbol}: мало данных → fallback z={_fb[0]:.1f}")
                return _fb
            return 0.0, "[OnChain] Мало данных"

        vol_values = [v[1] for v in volumes]
        z = round(_calc_z_score(vol_values), 2)

        if z > _Z_INFLOW_HIGH:
            desc = f"📥 OnChain: z={z:.1f} аномальный ПРИТОК → давление продаж"
        elif z < _Z_OUTFLOW_LOW:
            desc = f"📤 OnChain: z={z:.1f} аномальный ОТТОК → накопление"
        else:
            desc = f"OnChain: z={z:.1f} нейтрально"

        # Кешируем в Redis + сохраняем fallback для восстановления при сбоях
        if redis_client:
            try:
                redis_client.setex(cache_key, _REDIS_TTL, json.dumps({"z": z, "desc": desc}))
            except Exception:
                pass
        _save_fallback_z(redis_client, symbol, z, desc)

        logger.info(f"[OnChain] {symbol}: z={z:.2f} | {desc}")
        return z, desc

    except asyncio.TimeoutError:
        _fb = _read_fallback_z(redis_client, symbol)
        if _fb:
            logger.info(f"[OnChain] {symbol}: timeout → fallback z={_fb[0]:.1f}")
            return _fb
        return 0.0, "[OnChain] Timeout"
    except Exception as e:
        logger.debug(f"[OnChain] {symbol}: {e}")
        _fb = _read_fallback_z(redis_client, symbol)
        if _fb:
            return _fb
        return 0.0, ""


def onchain_score_bonus(z_score: float, direction: str) -> Tuple[int, str]:
    """
    Конвертирует z-score в bonus очки.

    SHORT: высокий z (приток) = +8 (давление продаж подтверждает шорт)
    LONG:  низкий z  (отток)  = +8 (накопление подтверждает лонг)
    """
    if direction == "short":
        if z_score >= _Z_INFLOW_HIGH:
            bonus = 8 if z_score >= 3.0 else 5
            return bonus, f"📥 OnChain приток z={z_score:.1f} → SHORT подтверждён"
        elif z_score <= _Z_OUTFLOW_LOW:
            return -3, f"📤 OnChain отток z={z_score:.1f} → против SHORT"
    else:  # long
        if z_score <= _Z_OUTFLOW_LOW:
            bonus = 8 if z_score <= -2.5 else 5
            return bonus, f"📤 OnChain отток z={z_score:.1f} → LONG подтверждён"
        elif z_score >= _Z_INFLOW_HIGH:
            return -3, f"📥 OnChain приток z={z_score:.1f} → против LONG"
    return 0, ""


async def get_active_addr_proxy(
    symbol: str,
    redis_client=None,
) -> Tuple[float, str]:
    """
    #35 Active Addresses proxy через CoinGecko volume (free tier).

    Сравнивает средний объём последних 7 дней vs предыдущих 7 дней.
    Рост >ONCHAIN_ADDR_ANOMALY_PCT% = аномальный рост активности.

    Returns:
        (change_pct: float, description: str)
        change_pct > +threshold → рост активности (накопление, LONG)
        change_pct < -threshold → спад активности (распределение, SHORT)
        0.0 = нет данных или нейтрально
    """
    if not _ENABLE_ONCHAIN:
        return 0.0, ""

    cache_key = f"onchain:addr_proxy:{symbol}"

    # Redis кеш (до резолвинга)
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                data = json.loads(cached)
                return data["pct"], data["desc"]
        except Exception:
            pass

    # Резолвим CoinGecko ID
    cg_id = _SYMBOL_MAP.get(symbol)
    if not cg_id:
        cg_id = await _resolve_dynamic_id(symbol, redis_client)

    # CMC fallback
    if not cg_id:
        base = _extract_base(symbol)
        cmc = await _get_cmc_volume(base)
        if cmc:
            _vol, _pct = cmc
            # vol_change_24h → грубый proxy для 7-дневного изменения активности
            change_pct = round(max(-80.0, min(80.0, _pct * 0.5)), 1)
            if change_pct >= _ADDR_ANOMALY_PCT:
                desc = f"🟢 AddrProxy(CMC): объём +{_pct:.0f}% → рост активности"
            elif change_pct <= -_ADDR_ANOMALY_PCT:
                desc = f"🔴 AddrProxy(CMC): объём {_pct:.0f}% → спад активности"
            else:
                desc = f"AddrProxy(CMC): {_pct:+.0f}% нейтрально"
            logger.info(f"[AddrProxy] {symbol}: CMC fallback {_pct:+.0f}%")
            if redis_client:
                try:
                    redis_client.setex(cache_key, _REDIS_TTL, json.dumps({"pct": change_pct, "desc": desc}))
                except Exception:
                    pass
            return change_pct, desc
        # CryptoCompare fallback (3-й) — 14 дней, считаем 7d vs 7d как в CoinGecko
        cc_vols = await _get_cc_volumes(base)
        if cc_vols and len(cc_vols) >= 14:
            recent_7  = cc_vols[-7:]
            prev_7    = cc_vols[-14:-7]
            avg_recent = sum(recent_7) / len(recent_7)
            avg_prev   = sum(prev_7) / len(prev_7)
            if avg_prev > 0:
                change_pct = round((avg_recent - avg_prev) / avg_prev * 100, 1)
                if change_pct >= _ADDR_ANOMALY_PCT:
                    desc = f"🟢 AddrProxy(CC): объём +{change_pct:.0f}% за 7д → рост активности"
                elif change_pct <= -_ADDR_ANOMALY_PCT:
                    desc = f"🔴 AddrProxy(CC): объём {change_pct:.0f}% за 7д → спад активности"
                else:
                    desc = f"AddrProxy(CC): {change_pct:+.0f}% нейтрально"
                logger.info(f"[AddrProxy] {symbol}: CryptoCompare fallback {change_pct:+.1f}%")
                if redis_client:
                    try:
                        redis_client.setex(cache_key, _REDIS_TTL, json.dumps({"pct": change_pct, "desc": desc}))
                    except Exception:
                        pass
                return change_pct, desc
        return 0.0, f"[AddrProxy] {symbol}: нет маппинга CoinGecko/CMC/CC"

    try:
        url = f"{_CG_BASE_URL}/coins/{cg_id}/market_chart"
        params = {"vs_currency": "usd", "days": "14", "interval": "daily"}

        # B7 FIX: через _cg_fetch с semaphore + retry на 429
        status, raw = await _cg_fetch(url, params)
        if status != 200 or raw is None:
            return 0.0, f"[AddrProxy] HTTP {status}"

        volumes = raw.get("total_volumes", [])
        if not volumes or len(volumes) < 14:
            return 0.0, "[AddrProxy] Мало данных"

        vol_values = [v[1] for v in volumes]
        # Последние 7 дней vs предыдущие 7 дней
        recent_7  = vol_values[-7:]
        prev_7    = vol_values[-14:-7]
        avg_recent = sum(recent_7) / len(recent_7)
        avg_prev   = sum(prev_7)   / len(prev_7)

        if avg_prev <= 0:
            return 0.0, "[AddrProxy] avg_prev=0"

        change_pct = round((avg_recent - avg_prev) / avg_prev * 100, 1)

        if change_pct >= _ADDR_ANOMALY_PCT:
            desc = f"🟢 AddrProxy: объём +{change_pct:.0f}% за 7д → рост активности (LONG)"
        elif change_pct <= -_ADDR_ANOMALY_PCT:
            desc = f"🔴 AddrProxy: объём {change_pct:.0f}% за 7д → спад активности (SHORT)"
        else:
            desc = f"AddrProxy: {change_pct:+.0f}% нейтрально"

        if redis_client:
            try:
                redis_client.setex(cache_key, _REDIS_TTL, json.dumps({"pct": change_pct, "desc": desc}))
            except Exception:
                pass

        logger.info(f"[AddrProxy] {symbol}: {change_pct:+.1f}%")
        return change_pct, desc

    except asyncio.TimeoutError:
        return 0.0, "[AddrProxy] Timeout"
    except Exception as e:
        logger.debug(f"[AddrProxy] {symbol}: {e}")
        return 0.0, ""


def addr_proxy_score_bonus(change_pct: float, direction: str) -> Tuple[int, str]:
    """
    Конвертирует change_pct из get_active_addr_proxy в bonus очки.

    LONG:  рост активности  → +5
    SHORT: спад активности  → +5
    Против сигнала → -3
    """
    threshold = _ADDR_ANOMALY_PCT
    if direction == "long":
        if change_pct >= threshold:
            return 5, f"🟢 AddrProxy: активность ↑{change_pct:.0f}% → LONG подтверждён"
        elif change_pct <= -threshold:
            return -3, f"🔴 AddrProxy: активность ↓{abs(change_pct):.0f}% → против LONG"
    else:  # short
        if change_pct <= -threshold:
            return 5, f"🔴 AddrProxy: активность ↓{abs(change_pct):.0f}% → SHORT подтверждён"
        elif change_pct >= threshold:
            return -3, f"🟢 AddrProxy: активность ↑{change_pct:.0f}% → против SHORT"
    return 0, ""
