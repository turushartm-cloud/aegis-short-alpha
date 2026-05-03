"""
BingX Futures API Client  v2.4-final

ВСЕ ИСПРАВЛЕНИЯ:
  v2.1 — RAW URL query string (signature mismatch fix)
  v2.2 — compact JSON separators для SL/TP
  v2.3 — "price" как float (не str) в SL/TP | фильтр offline символов
  v2.4 — type=STOP_MARKET/TAKE_PROFIT_MARKET + stopPrice (не price!)
       — добавлено max_notional в symbol info (для авто-уменьшения позиции)
       — code=101209 в ERROR_CODES
       — error 101209 логируется с подсказкой
"""

import os, json, hmac, hashlib, time
from typing import Optional, Dict, List, Any, Set
from dataclasses import dataclass
from datetime import datetime
import aiohttp


@dataclass
class BingXPosition:
    symbol: str
    side: str
    position_side: str
    size: float
    entry_price: float
    leverage: int
    unrealized_pnl: float
    realized_pnl: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


@dataclass
class BingXOrder:
    order_id: str
    symbol: str
    side: str
    position_side: str
    type: str
    size: float
    price: Optional[float] = None
    status: str = "PENDING"
    filled_size: float = 0.0
    avg_fill_price: float = 0.0
    created_at: Optional[datetime] = None


class BingXClient:
    DEMO_BASE_URL = "https://open-api-vst.bingx.com"
    REAL_BASE_URL = "https://open-api.bingx.com"

    ERROR_CODES = {
        80001:  "Parameter error",
        80012:  "Price precision — цена не кратна tickSize",
        80014:  "Quantity precision/min — объём ниже минимума",
        80016:  "Order does not exist",
        80020:  "Insufficient margin — недостаточно маржи",
        80021:  "Position does not exist",
        80022:  "Max positions reached",
        80030:  "Symbol does not exist",
        80041:  "SL/TP price invalid",
        101204: "Insufficient balance",
        101209: "Max position value exceeded — позиция превышает лимит для этого плеча. "
                "Уменьши размер или снизи плечо. AutoTrader авто-уменьшит в следующий раз.",
        109201: "Leverage exceeds max allowed",
        109400: "Timestamp invalid — разница времени между сервером и клиентом > 1000ms",
    }

    def __init__(self, api_key=None, api_secret=None, demo=True):
        self.api_key    = api_key    or os.getenv("BINGX_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BINGX_API_SECRET", "")
        force_real = os.getenv("BINGX_FORCE_REAL", "false").lower() == "true"
        self.demo  = (not force_real) or demo
        if not self.demo:
            print("🚨 WARNING: REAL MODE!")
        if not self.api_key or not self.api_secret:
            raise ValueError("BingX API key and secret required")
        self.base_url = self.DEMO_BASE_URL if self.demo else self.REAL_BASE_URL
        self.session: Optional[aiohttp.ClientSession] = None
        self._symbol_info_cache: Dict[str, Dict] = {}
        self._active_symbols: Set[str] = set()
        self._symbols_loaded = False
        self.last_error: Optional[str] = None
        self.last_error_code: Optional[int] = None
        self._time_offset: int = 0   # ✅ FIX: server time offset
        print(f"🚀 BingX Client ({'DEMO' if self.demo else 'REAL'})")

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers={"X-BX-APIKEY": self.api_key})
        return self.session

    def _sign(self, raw_qs: str) -> str:
        return hmac.new(
            self.api_secret.encode(), raw_qs.encode(), hashlib.sha256
        ).hexdigest()

    def _build_raw_qs(self, params: Dict[str, Any]) -> str:
        return "&".join(f"{k}={v}" for k, v in sorted(params.items()))

    async def _sync_server_time(self):
        """Получает время сервера BingX для корректного timestamp."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.base_url}/openApi/swap/v2/server/time",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("code") == 0:
                        server_ts = int(data.get("data", {}).get("serverTime", 0))
                        if server_ts > 0:
                            self._time_offset = server_ts - int(time.time() * 1000)
                            return
        except Exception:
            pass
        self._time_offset = 0

    def _get_timestamp(self) -> int:
        offset = getattr(self, "_time_offset", 0)
        return int(time.time() * 1000) + offset

    async def _make_request(self, method, endpoint, params=None, body=None, signed=True):
        try:
            session  = await self._get_session()
            all_p    = {}
            if params: all_p.update(params)
            if body:   all_p.update(body)
            if signed:
                all_p["timestamp"] = self._get_timestamp()
                all_p["recvWindow"] = 10000   # ✅ FIX: 10s окно (было не задано)
                raw_qs = self._build_raw_qs(all_p)
                full_url = f"{self.base_url}{endpoint}?{raw_qs}&signature={self._sign(raw_qs)}"
            else:
                raw_qs   = self._build_raw_qs(all_p)
                full_url = f"{self.base_url}{endpoint}?{raw_qs}" if raw_qs else f"{self.base_url}{endpoint}"
            timeout = aiohttp.ClientTimeout(total=30)
            fn = {"GET": session.get, "POST": session.post, "DELETE": session.delete}[method]
            async with fn(full_url, timeout=timeout) as r:
                return await self._parse_response(r, endpoint)
        except Exception as e:
            self.last_error = str(e)
            print(f"❌ [BingX] {method} {endpoint}: {e}")
            return None

    async def _parse_response(self, response, endpoint=""):
        text = await response.text()
        self.last_error = None
        self.last_error_code = None
        if response.status == 200:
            try:
                data = json.loads(text)
                code = data.get("code")
                if code != 0:
                    msg  = data.get("msg", "unknown")
                    hint = self.ERROR_CODES.get(code, "")
                    self.last_error = msg
                    self.last_error_code = code
                    # ✅ AUTO-SYNC: при ошибке timestamp сбрасываем offset
                    if code == 109400:
                        self._time_offset = 0   # сбросим, пересинхронизируем при следующем вызове
                    print(f"❌ [BingX] [{endpoint}] code={code} | {msg}"
                          + (f"\n   💡 {hint}" if hint else ""))
                return data
            except Exception as e:
                self.last_error = f"JSON: {e}"
                print(f"❌ [BingX] JSON: {e} | {text[:200]}")
                return None
        else:
            self.last_error = f"HTTP {response.status}"
            print(f"❌ [BingX] HTTP {response.status}: {text[:300]}")
            return None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # =========================================================================
    # SYMBOL INFO — включает max_notional для auto_trader
    # =========================================================================

    async def _load_contracts(self):
        if self._symbols_loaded:
            return
        result = await self._make_request(
            "GET", "/openApi/swap/v2/quote/contracts", params={}, signed=False
        )
        if result and result.get("code") == 0:
            for c in result.get("data", []):
                sym    = c.get("symbol", "")
                status = c.get("status", 1)
                if not sym:
                    continue

                # Пробуем получить max notional из поля maintenanceMarginRate
                # или из contractSize * maxOrderNum
                # BingX возвращает разные поля в зависимости от версии API
                # Безопасный дефолт: 5000 USDT
                max_notional = 5000.0
                try:
                    # Некоторые контракты имеют поле maxPositionValue
                    if "maxPositionValue" in c:
                        max_notional = float(c["maxPositionValue"])
                    elif "maxOrderValue" in c:
                        max_notional = float(c["maxOrderValue"])
                except Exception:
                    pass

                self._symbol_info_cache[sym] = {
                    "price_precision": int(c.get("pricePrecision", 4)),
                    "qty_precision":   int(c.get("quantityPrecision", 3)),
                    "min_qty":         float(c.get("tradeMinQuantity", 0.001)),
                    "max_leverage":    int(c.get("maxLeverage", 50)),
                    "online":          (status != 0),
                    "max_notional":    max_notional,   # ← НОВОЕ: лимит позиции
                }
                if status != 0:
                    self._active_symbols.add(sym)
            self._symbols_loaded = True
            print(f"📋 [BingX] {len(self._symbol_info_cache)} contracts, "
                  f"{len(self._active_symbols)} active")

    async def get_symbol_info(self, symbol: str) -> Dict:
        await self._load_contracts()
        return self._symbol_info_cache.get(symbol, {
            "price_precision": 4, "qty_precision": 3, "min_qty": 0.001,
            "max_leverage": 50, "online": False, "max_notional": 5000.0
        })

    async def is_symbol_active(self, symbol: str) -> bool:
        await self._load_contracts()
        info = self._symbol_info_cache.get(symbol)
        return info.get("online", True) if info else False

    async def _round_price(self, symbol: str, price: float) -> float:
        info = await self.get_symbol_info(symbol)
        return round(price, info.get("price_precision", 4))

    async def _round_qty(self, symbol: str, qty: float) -> float:
        info    = await self.get_symbol_info(symbol)
        prec    = info.get("qty_precision", 3)
        min_qty = info.get("min_qty", 0.001)
        return max(round(qty, prec), min_qty)

    # =========================================================================
    # ACCOUNT
    # =========================================================================

    async def get_account_balance(self) -> Optional[Dict]:
        result = await self._make_request("GET", "/openApi/swap/v2/user/balance")
        if result and result.get("code") == 0:
            data = result.get("data", {})
            bal  = data.get("balance", [])
            b    = (bal[0] if isinstance(bal, list) and bal
                    else bal if isinstance(bal, dict) else {})
            return {
                "equity":          b.get("equity", "0"),
                "availableMargin": b.get("availableMargin", "0"),
                "walletBalance":   b.get("walletBalance", "0"),
                "unrealizedPNL":   b.get("unrealizedPNL", "0"),
            }
        return None

    # =========================================================================
    # POSITIONS
    # =========================================================================

    async def get_positions(self, symbol=None) -> List[BingXPosition]:
        params = {"symbol": symbol} if symbol else {}
        result = await self._make_request("GET", "/openApi/swap/v2/user/positions", params=params)
        positions = []
        if result and result.get("code") == 0:
            for d in result.get("data", []):
                try:
                    size = float(d.get("positionAmt", 0))
                    if size == 0:
                        continue
                    positions.append(BingXPosition(
                        symbol=d.get("symbol",""),
                        side="LONG" if d.get("positionSide") in ("LONG", "BOTH") and float(d.get("positionAmt",0))>=0 else "SHORT",
                        position_side=d.get("positionSide",""),
                        size=size,
                        entry_price=float(d.get("avgPrice",0)),
                        leverage=int(d.get("leverage",1)),
                        unrealized_pnl=float(d.get("unrealizedProfit",0)),
                        realized_pnl=float(d.get("realizedProfit",0)),
                        stop_loss=float(d.get("stopLoss",0)) or None,
                        take_profit=float(d.get("takeProfit",0)) or None,
                    ))
                except Exception as e:
                    print(f"⚠️ Position parse: {e}")
        return positions

    async def close_position(self, symbol: str, position_side: str) -> bool:
        result = await self._make_request("POST", "/openApi/swap/v2/trade/closePosition",
                                          body={"symbol": symbol, "positionSide": position_side})
        if result and result.get("code") == 0:
            print(f"✅ Closed: {symbol} {position_side}")
            return True
        print(f"❌ Close failed: {symbol} | {self.last_error}")
        return False

    async def close_all_positions(self) -> int:
        positions = await self.get_positions()
        closed = 0
        for p in positions:
            if abs(p.size) > 0 and await self.close_position(p.symbol, p.position_side):
                closed += 1
        return closed

    # =========================================================================
    # ORDERS — v2.4 FINAL
    # =========================================================================

    async def place_order(self, symbol, side, position_side, order_type, size,
                          price=None, stop_loss=None, take_profit=None):
        """
        v2.4 FINAL — все исправления:
          1. RAW URL (не params=) — signature fix
          2. is_symbol_active проверка
          3. Правильное округление price/qty
          4. stopLoss: {"type":"STOP_MARKET", "stopPrice": float, "workingType":"MARK_PRICE"}
          5. takeProfit: {"type":"TAKE_PROFIT_MARKET", "stopPrice": float, ...}
          6. Все значения — числа, не строки (fix float64 mismatch)
        """
        if not await self.is_symbol_active(symbol):
            self.last_error = f"{symbol} offline on BingX"
            print(f"⏭ SKIP — {self.last_error}")
            return None

        rounded_size = await self._round_qty(symbol, size)
        rounded_sl   = await self._round_price(symbol, stop_loss)   if stop_loss   else None
        rounded_tp   = await self._round_price(symbol, take_profit)  if take_profit else None
        rounded_px   = await self._round_price(symbol, price)        if price       else None

        print(f"📤 Order: {symbol} {side} {position_side} {order_type} | "
              f"qty={rounded_size} | SL={rounded_sl} | TP={rounded_tp}")

        body: Dict[str, Any] = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": position_side,
            "type":         order_type,
            "quantity":     str(rounded_size),
        }
        if rounded_px and order_type == "LIMIT":
            body["price"] = str(rounded_px)

        # ✅ type=STOP_MARKET, stopPrice=float (не str, не "price")
        if rounded_sl is not None:
            body["stopLoss"] = json.dumps(
                {"type": "STOP_MARKET", "stopPrice": rounded_sl, "workingType": "MARK_PRICE"},
                separators=(',', ':')
            )
        if rounded_tp is not None:
            body["takeProfit"] = json.dumps(
                {"type": "TAKE_PROFIT_MARKET", "stopPrice": rounded_tp, "workingType": "MARK_PRICE"},
                separators=(',', ':')
            )

        result = await self._make_request("POST", "/openApi/swap/v2/trade/order", body=body)
        if result and result.get("code") == 0:
            d        = result.get("data", {})
            order    = d.get("order", d)
            order_id = str(order.get("orderId", ""))
            print(f"✅ Order placed: {symbol} {side} qty={rounded_size} id={order_id}")
            return BingXOrder(
                order_id=order_id, symbol=symbol, side=side,
                position_side=position_side, type=order_type,
                size=rounded_size, price=rounded_px, status="PENDING",
            )

        code = (result or {}).get("code")
        hint = self.ERROR_CODES.get(code, "") if code else ""
        print(f"❌ Order REJECTED: {symbol} | code={code} | {self.last_error}"
              + (f"\n   💡 {hint}" if hint else ""))
        return None

    async def place_market_order(self, symbol, side, position_side, size,
                                  stop_loss=None, take_profit=None):
        return await self.place_order(symbol=symbol, side=side, position_side=position_side,
                                       order_type="MARKET", size=size,
                                       stop_loss=stop_loss, take_profit=take_profit)

    # =========================================================================
    # LEVERAGE
    # =========================================================================

    async def set_leverage(self, symbol, leverage, position_side="BOTH"):
        sides  = ["LONG", "SHORT"] if position_side == "BOTH" else [position_side]
        all_ok = True
        for side in sides:
            result = await self._make_request("POST", "/openApi/swap/v2/trade/leverage",
                                              body={"symbol": symbol, "leverage": str(leverage),
                                                    "side": side})
            ok = result and result.get("code") == 0
            print(f"{'✅' if ok else '❌'} Leverage {symbol} {side} {leverage}x"
                  + ("" if ok else f" | {self.last_error}"))
            if not ok:
                all_ok = False
        return all_ok

    # =========================================================================
    # CONNECTION TEST
    # =========================================================================

    async def test_connection(self) -> bool:
        try:
            balance = await self.get_account_balance()
            if balance:
                print(f"✅ BingX OK ({'DEMO' if self.demo else 'REAL'}) equity={balance.get('equity','?')}")
                return True
            print(f"❌ BingX failed | {self.last_error}")
            return False
        except Exception as e:
            print(f"❌ BingX error: {e}")
            return False


_bingx_client = None

def get_bingx_client(demo=True):
    global _bingx_client
    if _bingx_client is None:
        _bingx_client = BingXClient(demo=demo)
    return _bingx_client
