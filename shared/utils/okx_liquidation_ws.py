"""
OKX Liquidation WebSocket Feed v1.0
=====================================
REST /api/v5/public/liquidation-orders был удалён OKX в 2023 → всегда HTTP 400.
Официальное решение: публичный WebSocket канал liquidation-orders.

Архитектура:
  OKX WS (wss://ws.okx.com:8443) ──push──► этот модуль
           ► Redis: okx:liq:{BTCUSDT} TTL=300s (скользящие 1h суммы)
           ► get_liquidations() читает из Redis вместо мёртвого REST

Подписка:
  {"channel": "liquidation-orders", "instType": "SWAP"}
  {"channel": "liquidation-orders", "instType": "FUTURES"}

Формат Redis value (JSON):
  {"long_usd": 12345, "short_usd": 6789, "total_usd": 19134, "ts": 1234567890000}
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional, Set

logger = logging.getLogger("aegis.okx_ws")

# Exponential backoff delays (секунды)
RECONNECT_DELAYS = [1, 2, 4, 8, 15, 30, 60]
PING_INTERVAL    = 25    # OKX требует ping каждые 25s
PONG_TIMEOUT     = 10    # Если нет ответа через 10s → reconnect
LIQ_TTL_SECONDS  = 300   # Redis TTL для накопленных ликвидаций (5 мин)
LIQ_WINDOW_MS    = 3_600_000  # Окно накопления: 1 час


class OKXLiquidationFeed:
    """
    Публичный WebSocket стрим ликвидаций OKX.
    Пишет агрегированные данные в Redis.
    Запускается как asyncio background task.
    """

    WS_URL     = "wss://ws.okx.com:8443/ws/v5/public"
    WS_URL_ALT = "wss://wspap.okx.com:8443/ws/v5/public"  # demo fallback

    SUBSCRIBE_ARGS = [
        {"channel": "liquidation-orders", "instType": "SWAP"},
        {"channel": "liquidation-orders", "instType": "FUTURES"},
    ]

    def __init__(self, redis_client, demo: bool = False):
        self.redis   = redis_client
        self.url     = self.WS_URL_ALT if demo else self.WS_URL
        self.running = False
        self._task:    Optional[asyncio.Task] = None
        self._connect_attempt  = 0
        self._seen:   Set[str] = set()   # dedup кеш (ts:instId:bkPx)
        self._connected = False

    # ── Public API ────────────────────────────────────────────────────────

    async def start(self):
        """Запустить WS feed как фоновую задачу."""
        self.running = True
        self._task   = asyncio.create_task(self._run(), name="okx_liq_ws")
        logger.info("[OKX WS] Liquidation feed started")

    async def stop(self):
        """Graceful shutdown."""
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[OKX WS] Liquidation feed stopped")

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Main loop ─────────────────────────────────────────────────────────

    async def _run(self):
        while self.running:
            self._connected = False
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[OKX WS] Connection error: {e}")

            if not self.running:
                break

            delay = RECONNECT_DELAYS[min(self._connect_attempt, len(RECONNECT_DELAYS) - 1)]
            logger.info(f"[OKX WS] Reconnecting in {delay}s (attempt {self._connect_attempt+1})")
            await asyncio.sleep(delay)
            self._connect_attempt += 1

    async def _connect_and_listen(self):
        try:
            import websockets
        except ImportError:
            logger.error("[OKX WS] 'websockets' library not installed! Run: pip install websockets")
            await asyncio.sleep(60)
            return

        async with websockets.connect(
            self.url,
            ping_interval=None,   # Manage ping manually (OKX sends string "pong")
            close_timeout=5,
            open_timeout=15,
        ) as ws:
            self._connect_attempt = 0
            self._connected = True
            logger.info(f"[OKX WS] Connected to {self.url}")

            # Subscribe
            await ws.send(json.dumps({"op": "subscribe", "args": self.SUBSCRIBE_ARGS}))

            # Wait for subscribe ACK
            try:
                ack_raw = await asyncio.wait_for(ws.recv(), timeout=10)
                ack = json.loads(ack_raw) if ack_raw != "pong" else {}
                if ack.get("event") == "subscribe":
                    logger.info(f"[OKX WS] Subscribed: {[a['instType'] for a in self.SUBSCRIBE_ARGS]}")
                else:
                    logger.info(f"[OKX WS] Subscribe response: {str(ack)[:120]}")
            except asyncio.TimeoutError:
                logger.warning("[OKX WS] No subscribe ACK received (continuing anyway)")

            # Start pinger + message loop concurrently
            ping_task = asyncio.create_task(self._pinger(ws))
            try:
                async for raw in ws:
                    if not self.running:
                        break
                    await self._on_message(raw)
            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    async def _pinger(self, ws):
        """OKX требует ping каждые 25s, иначе соединение рвётся."""
        while True:
            await asyncio.sleep(PING_INTERVAL)
            try:
                await ws.send("ping")
            except Exception as e:
                logger.debug(f"[OKX WS] Ping failed: {e}")
                break

    # ── Message handler ───────────────────────────────────────────────────

    async def _on_message(self, raw: str):
        # OKX pong response
        if raw == "pong":
            return

        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return

        # Service messages (subscribe ACK, error, etc.)
        if "event" in msg:
            if msg.get("event") == "error":
                logger.warning(f"[OKX WS] Server error: {msg}")
            return

        arg = msg.get("arg", {})
        if arg.get("channel") != "liquidation-orders":
            return

        for item in msg.get("data", []):
            await self._process_item(item)

    async def _process_item(self, item: dict):
        """Парсим один элемент данных и пишем в Redis."""
        inst_id = item.get("instId", "")     # BTC-USDT-SWAP
        if not inst_id:
            return

        # Конвертируем instId → symbol (BTCUSDT)
        symbol = self._to_symbol(inst_id)
        if not symbol:
            return

        now_ms   = int(time.time() * 1000)
        cutoff   = now_ms - LIQ_WINDOW_MS
        long_usd = 0.0
        short_usd = 0.0

        for detail in item.get("details", []):
            ts = int(detail.get("ts", 0))
            if ts < cutoff:
                continue  # слишком старая ликвидация

            # Deduplication
            bk_px  = detail.get("bkPx", "0")
            sz     = detail.get("sz", "0")
            dup_key = f"{ts}:{inst_id}:{bk_px}:{sz}"
            if dup_key in self._seen:
                continue
            self._seen.add(dup_key)

            # Очищаем кеш дедупликации (ограничиваем память)
            if len(self._seen) > 50_000:
                self._seen.clear()

            try:
                bk_px_f  = float(bk_px) if bk_px else 0.0
                sz_f     = float(sz) if sz else 0.0
                usd      = bk_px_f * sz_f
                if usd <= 0:
                    continue

                pos_side = detail.get("posSide", "")
                side     = detail.get("side", "")

                # side=sell = ликвидация LONG позиции (лонгист получил маржин-колл)
                # side=buy  = ликвидация SHORT позиции
                if side == "sell" or pos_side == "long":
                    long_usd += usd
                else:
                    short_usd += usd
            except (ValueError, TypeError):
                continue

        total = long_usd + short_usd
        if total < 10:  # dust threshold
            return

        # Читаем текущее накопленное значение из Redis и добавляем
        key = f"okx:liq:{symbol}"
        try:
            existing_raw = self.redis.client.get(key)
            if existing_raw:
                prev = json.loads(existing_raw)
                long_usd  += float(prev.get("long_usd",  0))
                short_usd += float(prev.get("short_usd", 0))

            payload = {
                "long_usd":  round(long_usd),
                "short_usd": round(short_usd),
                "total_usd": round(long_usd + short_usd),
                "ts":        now_ms,
            }
            self.redis.client.setex(key, LIQ_TTL_SECONDS, json.dumps(payload))
            logger.debug(
                f"[OKX WS] {symbol}: liq long=${long_usd:,.0f} "
                f"short=${short_usd:,.0f} total=${long_usd+short_usd:,.0f}"
            )
        except Exception as e:
            logger.debug(f"[OKX WS] Redis write error {symbol}: {e}")

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _to_symbol(inst_id: str) -> str:
        """
        Конвертируем OKX instId в USDT-символ Binance/BingX.
        BTC-USDT-SWAP     → BTCUSDT
        BTC-USDT-FUTURES  → BTCUSDT
        ETH-USDC-SWAP     → пропускаем (не USDT)
        """
        if "-USDT-" not in inst_id:
            return ""
        base = inst_id.split("-USDT-")[0]
        return f"{base}USDT"


def get_okx_liq_from_redis(redis_client, symbol: str) -> Optional[dict]:
    """
    Читаем OKX ликвидации из Redis кеша.
    Возвращает dict или None если данных нет.

    Пример возврата:
    {"long_usd": 123456, "short_usd": 78901, "total_usd": 202357, "dominant_side": "LONG"}
    """
    try:
        key = f"okx:liq:{symbol}"
        raw = redis_client.client.get(key)
        if not raw:
            return None
        data = json.loads(raw)
        total = data.get("total_usd", 0)
        if total <= 0:
            return None
        long_usd  = data.get("long_usd", 0)
        short_usd = data.get("short_usd", 0)
        return {
            "total_usd":     total,
            "long_liq_usd":  long_usd,
            "short_liq_usd": short_usd,
            "dominant_side": "LONG" if long_usd > short_usd
                             else "SHORT" if short_usd > long_usd else None,
        }
    except Exception:
        return None
