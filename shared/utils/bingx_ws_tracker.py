"""
BingX WebSocket Position Tracker v1.0
======================================
Заменяет polling каждые 30 секунд на push-уведомления от BingX.

Решает КРИТИЧЕСКУЮ проблему: при резком spike (например LABUSDT +6.7% за <30s)
polling не успевает среагировать на SL, позиция закрывается по худшей цене.

Архитектура:
1. Получаем listenKey через REST /openApi/user/auth/userDataStream
2. Подключаемся к WSS wss://open-api-swap.bingx.com/swap-market
3. Подписываемся на ORDER_TRADE_UPDATE события
4. При получении ORDER_TRADE_UPDATE со статусом FILLED/CANCELED:
   - Если это SL/TP → немедленно обновляем Redis позицию
   - Если это SL (STOP_MARKET FILLED) → закрываем позицию в Redis
5. listenKey обновляем каждые 30 мин (BingX expires в 60 мин)

Параллельно с polling (polling остаётся как fallback каждые 30s).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
from typing import Optional, Callable, Awaitable

logger = logging.getLogger("aegis.bingx_ws")

# BingX WSS endpoint для бессрочных фьючерсов
BINGX_WS_URL  = "wss://open-api-swap.bingx.com/swap-market"
BINGX_REST    = "https://open-api.bingx.com"
LISTEN_KEY_REFRESH_INTERVAL = 1800  # 30 мин


class BingXWSTracker:
    """
    WebSocket-трекер позиций BingX.
    
    Использование:
        tracker = BingXWSTracker(api_key, api_secret, on_sl_hit, on_tp_hit)
        await tracker.start()
        # ... работает в фоне
        await tracker.stop()
    """

    def __init__(
        self,
        api_key:    str,
        api_secret: str,
        on_sl_hit:  Optional[Callable[[str, float], Awaitable[None]]] = None,
        on_tp_hit:  Optional[Callable[[str, int, float], Awaitable[None]]] = None,
        demo:       bool = True,
    ):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.on_sl_hit  = on_sl_hit   # async (symbol, price)
        self.on_tp_hit  = on_tp_hit   # async (symbol, tp_num, price)
        self.demo       = demo

        self._listen_key:   Optional[str] = None
        self._listen_key_ts: float = 0.0
        self._running = False
        self._ws_task: Optional[asyncio.Task] = None
        self._refresh_task: Optional[asyncio.Task] = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _sign(self, params: dict) -> str:
        qs = urllib.parse.urlencode(sorted(params.items()))
        return hmac.new(self.api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

    async def _get_listen_key(self) -> Optional[str]:
        """Получить или обновить listenKey от BingX REST API."""
        try:
            import aiohttp
            ts = int(time.time() * 1000)
            params = {"timestamp": ts}
            params["signature"] = self._sign(params)
            headers = {"X-BX-APIKEY": self.api_key}
            url = f"{BINGX_REST}/openApi/user/auth/userDataStream"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, params=params, headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
                    # BingX DEMO возвращает {listenKey: ...} без поля code
                    # BingX REAL возвращает {code: 0, data: {listenKey: ...}}
                    key = (
                        data.get("listenKey")
                        or data.get("data", {}).get("listenKey")
                        or (data.get("code") == 0 and data.get("data", {}).get("listenKey"))
                    ) or None
                    if key:
                        logger.info(f"[BingX WS] Got listenKey: {str(key)[:16]}...")
                        return key
                    if data.get("code") not in (0, None, "0"):
                        logger.warning(f"[BingX WS] listenKey API error: code={data.get('code')} msg={data.get('msg', data)}")
                    else:
                        logger.warning(f"[BingX WS] listenKey missing in response: {data}")
        except Exception as e:
            logger.warning(f"[BingX WS] listenKey fetch failed: {e}")
        return None

    async def _refresh_listen_key_loop(self) -> None:
        """Обновляем listenKey каждые 30 мин (BingX expire = 60 мин)."""
        while self._running:
            await asyncio.sleep(LISTEN_KEY_REFRESH_INTERVAL)
            if not self._running:
                break
            try:
                import aiohttp
                ts = int(time.time() * 1000)
                params = {"listenKey": self._listen_key, "timestamp": ts}
                params["signature"] = self._sign(params)
                url = f"{BINGX_REST}/openApi/user/auth/userDataStream"
                async with aiohttp.ClientSession() as s:
                    async with s.put(url, params=params,
                                     headers={"X-BX-APIKEY": self.api_key},
                                     timeout=aiohttp.ClientTimeout(total=10)) as r:
                        data = await r.json()
                        if data.get("code") == 0:
                            logger.info("[BingX WS] listenKey refreshed")
                        else:
                            logger.warning(f"[BingX WS] listenKey refresh failed: {data}")
                            # Получаем новый ключ
                            self._listen_key = await self._get_listen_key()
            except Exception as e:
                logger.warning(f"[BingX WS] refresh error: {e}")

    # ── Message handler ───────────────────────────────────────────────────────

    async def _keepalive_loop(self, ws) -> None:
        """
        ✅ FIX v3.0: BingX требует ping frame каждые 20-30с иначе закрывает соединение.
        Стандартный websockets ping_interval не работает с BingX (нет pong).
        Решение: вручную шлём Ping frame + раз в 25 мин обновляем listenKey.
        """
        ping_count = 0
        while self._running:
            await asyncio.sleep(20)
            try:
                await ws.ping()
                ping_count += 1
                # Каждые 25 мин (75 пингов × 20с) обновляем listenKey
                if ping_count % 75 == 0:
                    await self._do_refresh_listen_key()
            except Exception:
                break  # WS закрылся — выходим, внешний цикл переподключится

    async def _do_refresh_listen_key(self) -> None:
        """Разовое обновление listenKey."""
        if not self._listen_key:
            return
        try:
            import aiohttp
            ts = int(time.time() * 1000)
            params = {"listenKey": self._listen_key, "timestamp": ts}
            params["signature"] = self._sign(params)
            url = f"{BINGX_REST}/openApi/user/auth/userDataStream"
            async with aiohttp.ClientSession() as s:
                async with s.put(url, params=params,
                                 headers={"X-BX-APIKEY": self.api_key},
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
                    data = await r.json()
                    if data.get("code") == 0:
                        logger.info("[BingX WS] listenKey refreshed")
                    else:
                        logger.warning(f"[BingX WS] listenKey refresh failed: {data}")
                        self._listen_key = await self._get_listen_key()
        except Exception as e:
            logger.warning(f"[BingX WS] refresh error: {e}")

    async def _handle_message(self, raw: str) -> None:
        """Обрабатывает входящее WS сообщение от BingX."""
        try:
            msg = json.loads(raw)
        except Exception:
            return

        # BingX отправляет ORDER_TRADE_UPDATE при изменении ордера
        event = msg.get("e") or msg.get("event") or msg.get("dataType", "")

        if "ORDER_TRADE" not in str(event).upper() and "order" not in str(msg).lower():
            return

        # Извлекаем данные ордера
        order = msg.get("o") or msg.get("data") or msg
        if not isinstance(order, dict):
            return

        symbol_raw  = order.get("s") or order.get("symbol", "")
        symbol      = symbol_raw.replace("-USDT", "USDT").replace("-", "")
        order_type  = (order.get("ot") or order.get("o") or order.get("orderType", "")).upper()
        status      = (order.get("X") or order.get("orderStatus") or order.get("status", "")).upper()
        price       = float(order.get("ap") or order.get("avgPrice") or order.get("price") or 0)
        side        = (order.get("ps") or order.get("positionSide", "")).upper()

        if not symbol or status != "FILLED":
            return

        logger.info(f"[BingX WS] ORDER FILLED: {symbol} {order_type} side={side} price={price}")

        # SL hit: STOP_MARKET ордер исполнен
        if order_type in ("STOP_MARKET", "STOP") and price > 0:
            logger.warning(f"[BingX WS] 🛑 SL HIT via WS: {symbol} @ {price}")
            if self.on_sl_hit:
                await self.on_sl_hit(symbol, price)

        # TP hit: TAKE_PROFIT_MARKET ордер исполнен
        elif order_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT") and price > 0:
            # Определяем номер TP из clientOrderId если есть
            client_id = order.get("c") or order.get("clientOrderId", "")
            tp_num = 1
            for part in str(client_id).split("_"):
                if part.startswith("TP") and part[2:].isdigit():
                    tp_num = int(part[2:])
            logger.info(f"[BingX WS] ✅ TP{tp_num} HIT via WS: {symbol} @ {price}")
            if self.on_tp_hit:
                await self.on_tp_hit(symbol, tp_num, price)

    # ── Main WS loop ──────────────────────────────────────────────────────────

    async def _ws_loop(self) -> None:
        """Основной WebSocket цикл с автоматическим reconnect."""
        backoff = 1
        while self._running:
            if not self._listen_key:
                self._listen_key = await self._get_listen_key()
                if not self._listen_key:
                    logger.warning("[BingX WS] No listenKey, retry in 30s")
                    await asyncio.sleep(30)
                    continue

            ws_url = f"{BINGX_WS_URL}?listenKey={self._listen_key}"
            try:
                import websockets
                logger.info(f"[BingX WS] Connecting to {BINGX_WS_URL}...")
                async with websockets.connect(
                    ws_url,
                    ping_interval=None,   # ✅ FIX: BingX не поддерживает стандартный WS ping
                    close_timeout=5,
                ) as ws:
                    backoff = 1
                    logger.info("[BingX WS] ✅ Connected — listening for order updates")
                    # ✅ FIX: keepalive задача — каждые 25 мин обновляем listenKey + шлём ping
                    keepalive_task = asyncio.create_task(
                        self._keepalive_loop(ws)
                    )
                    try:
                        async for raw_msg in ws:
                            if not self._running:
                                break
                            if isinstance(raw_msg, bytes):
                                # BingX может присылать gzip
                                try:
                                    import gzip
                                    raw_msg = gzip.decompress(raw_msg).decode("utf-8")
                                except Exception:
                                    raw_msg = raw_msg.decode("utf-8", errors="ignore")
                            await self._handle_message(raw_msg)
                    finally:
                        keepalive_task.cancel()
                        try:
                            await keepalive_task
                        except asyncio.CancelledError:
                            pass

            except Exception as e:
                if not self._running:
                    break
                logger.warning(f"[BingX WS] Connection error: {e}. Reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        logger.info("[BingX WS] WS loop stopped")

    # ── Public API ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Запустить WS tracker в фоне."""
        if self._running:
            return
        if not self.api_key or not self.api_secret:
            logger.warning("[BingX WS] No API credentials — WS tracker disabled")
            return
        # ✅ FIX: BingX DEMO/paper trading не поддерживает userDataStream WS.
        # listenKey endpoint работает только для реальных аккаунтов.
        # В DEMO режиме WS tracker не нужен — ордера отслеживаются через polling.
        if self.demo:
            logger.info("[BingX WS] DEMO mode — WS tracker отключён (paper trading не поддерживает userDataStream)")
            return
        self._running = True
        self._ws_task      = asyncio.create_task(self._ws_loop())
        self._refresh_task = asyncio.create_task(self._refresh_listen_key_loop())
        logger.info("[BingX WS] Position tracker started (WS mode)")

    async def stop(self) -> None:
        """Остановить WS tracker."""
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
        if self._refresh_task:
            self._refresh_task.cancel()
        logger.info("[BingX WS] Position tracker stopped")

    @property
    def is_connected(self) -> bool:
        return self._running and self._ws_task is not None and not self._ws_task.done()
