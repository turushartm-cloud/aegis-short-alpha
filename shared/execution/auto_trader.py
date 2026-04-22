"""
Auto Trader v2.6

ИСПРАВЛЕНИЯ v2.6:
  ✅ code=101209 RETRY: парсим фактический лимит из текста ошибки → retry
     Было: cap=$5000, но если лимит=5000 → reject (edge case точное совпадение)
     Стало: cap = parsed_max * 0.92 (8% запас) → retry 1 раз
     Пример: "max is 5000 USDT" → cap = 4600, retry → успех
  ✅ daily_pnl: единицы в % (5.0 = 5%), не дробях (было 0.05)
  ✅ cooldown 30с между открытиями (защита от дублей)
  ✅ TP Hedge Mode: нет reduceOnly
"""

import os
import asyncio
import re
import time
from typing import Optional, Dict, List
from dataclasses import dataclass, field
from datetime import datetime

import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api.bingx_client import BingXClient
from upstash.redis_client import get_redis_client


def _parse_max_notional_from_error(error_msg: str) -> Optional[float]:
    """
    Из строки "The maximum position value for this leverage is 5000 USDT"
    или "The maximum position value for this leverage is 10,000 USDT"
    извлекаем число.
    """
    if not error_msg:
        return None
    # Ищем число (возможно с запятыми) перед "USDT"
    m = re.search(r'is\s+([\d,]+(?:\.\d+)?)\s+USDT', error_msg, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except Exception:
            pass
    return None


@dataclass
class TradeConfig:
    enabled:             bool  = True
    demo_mode:           bool  = True
    max_positions:       int   = 20
    risk_per_trade:      float = 0.0005     # 0.05%
    max_daily_risk:      float = 5.0        # 5% (в %, не дробях!)
    default_leverage:    int   = 20
    min_leverage:        int   = 5
    max_leverage:        int   = 50
    min_score_for_trade: int   = 65
    max_position_usdt:   float = 5000.0    # глобальный потолок
    notional_safety_pct: float = 0.92      # 92% от лимита (запас 8%)
    open_cooldown_sec:   float = 30.0      # антидубль


class AutoTrader:

    def __init__(self, bingx_client=None, config=None, telegram=None):
        self.config   = config or TradeConfig()
        self.bingx    = bingx_client or BingXClient(demo=self.config.demo_mode)
        self.redis    = get_redis_client()
        self.telegram = telegram

        self.daily_pnl    = 0.0   # в % (напр. -2.5 = убыток 2.5%)
        self.daily_trades = 0
        self.total_pnl    = 0.0
        self.win_count    = 0
        self.loss_count   = 0
        self.last_reset   = datetime.utcnow().date()
        self._last_open_ts = 0.0

        mode = "DEMO" if self.config.demo_mode else "REAL"
        print(f"🤖 AutoTrader initialized ({mode})")
        print(f"   Risk/trade: {self.config.risk_per_trade*100:.3f}% | "
              f"Max pos: {self.config.max_positions} | "
              f"Min score: {self.config.min_score_for_trade} | "
              f"Max notional: ${self.config.max_position_usdt:,.0f} | "
              f"Daily risk limit: {self.config.max_daily_risk}%")

    async def _tg(self, msg: str):
        if self.telegram:
            try:
                await self.telegram.send_message(msg)
            except Exception as e:
                print(f"⚠️ Telegram: {e}")

    async def _tg_reply(self, msg: str, msg_id: Optional[int]):
        if not self.telegram:
            return
        try:
            if msg_id:
                await self.telegram.send_reply(msg, reply_to_message_id=msg_id)
            else:
                await self.telegram.send_message(msg)
        except Exception:
            try:
                await self.telegram.send_message(msg)
            except Exception:
                pass

    async def execute_signal(self, signal: Dict) -> Optional[Dict]:
        symbol = signal.get("symbol", "?")
        score  = signal.get("score", 0)
        print(f"\n🚀 [AutoTrader] {symbol} | score={score:.1f}")
        try:
            return await self.open_position(
                symbol       = symbol,
                direction    = signal["direction"],
                entry_price  = signal["entry_price"],
                stop_loss    = signal["stop_loss"],
                take_profits = signal["take_profits"],
                signal_score = signal["score"],
                smc_data     = signal.get("smc"),
                tg_msg_id    = signal.get("tg_msg_id"),
            )
        except KeyError as e:
            print(f"❌ [AutoTrader] {symbol}: missing field {e}")
            return None
        except Exception as e:
            import traceback
            print(f"❌ [AutoTrader] {symbol}: {e}\n{traceback.format_exc()}")
            return None

    async def open_position(self, symbol, direction, entry_price, stop_loss,
                            take_profits, signal_score, smc_data=None,
                            tg_msg_id=None) -> Optional[Dict]:
        mode = "DEMO" if self.config.demo_mode else "REAL"
        pfx  = f"[AT][{symbol}][{direction.upper()}]"

        # ── 0. Cooldown ───────────────────────────────────────────────────────
        since_last = time.time() - self._last_open_ts
        if since_last < self.config.open_cooldown_sec:
            print(f"{pfx} ⏸ SKIP — cooldown ({since_last:.0f}s)")
            return None

        if not self.config.enabled:
            return None

        if signal_score < self.config.min_score_for_trade:
            print(f"{pfx} ⏸ SKIP — score {signal_score:.1f} < {self.config.min_score_for_trade}")
            return None

        # ── 1. Daily risk ─────────────────────────────────────────────────────
        self._check_daily_reset()
        if self.daily_pnl <= -self.config.max_daily_risk:
            print(f"{pfx} ⏸ SKIP — daily risk limit {self.daily_pnl:.2f}% <= -{self.config.max_daily_risk}%")
            await self._tg(
                f"⏸ <b>[{mode}]</b> <code>#{symbol}</code>: "
                f"дневной лимит ({self.daily_pnl:.2f}% ≤ -{self.config.max_daily_risk}%)"
            )
            return None

        # ── 2. Positions ──────────────────────────────────────────────────────
        print(f"{pfx} 🔍 Checking open positions...")
        try:
            current_positions = await self.bingx.get_positions()
        except Exception as e:
            print(f"{pfx} ❌ get_positions failed: {e}")
            return None

        n_pos    = len(current_positions)
        pos_list = " | ".join(f"{p.symbol}({p.side})" for p in current_positions)
        print(f"{pfx} 📊 Open: {n_pos}/{self.config.max_positions}")
        if pos_list:
            print(f"{pfx} 📋 {pos_list}")

        if n_pos >= self.config.max_positions:
            print(f"{pfx} ⏸ SKIP — max positions")
            await self._tg_reply(
                f"⏸ <b>Биржа заполнена</b> ({n_pos}/{self.config.max_positions})\n"
                f"<b>#{symbol}</b> — сигнал пропущен", tg_msg_id
            )
            return None

        # ── 3. Duplicate ──────────────────────────────────────────────────────
        bingx_symbol = self._to_bingx_symbol(symbol)
        existing = [p for p in current_positions
                    if p.symbol.replace("-", "") == symbol.replace("-", "")]
        if existing:
            print(f"{pfx} ⏸ SKIP — already open ({existing[0].side})")
            await self._tg_reply(
                f"ℹ️ <b>Позиция уже открыта</b>\n"
                f"<b>#{symbol}</b> — {existing[0].side} уже активен", tg_msg_id
            )
            return None

        # ── 4. Symbol online? ─────────────────────────────────────────────────
        if not await self.bingx.is_symbol_active(bingx_symbol):
            print(f"{pfx} ⏭ SKIP — {bingx_symbol} offline/delisted")
            return None

        # ── 5. Balance ────────────────────────────────────────────────────────
        print(f"{pfx} 💰 Getting balance...")
        bal = await self.bingx.get_account_balance()
        if not bal:
            print(f"{pfx} ❌ SKIP — balance error: {self.bingx.last_error}")
            return None

        available = float(bal.get("availableMargin", 0))
        equity    = float(bal.get("equity", 0))
        print(f"{pfx} 💰 Equity={equity:.2f} | Available={available:.2f}")

        if available <= 0:
            print(f"{pfx} ❌ SKIP — no margin")
            return None

        # ── 6. Sizing ─────────────────────────────────────────────────────────
        risk_mult   = 1.5 if signal_score >= 85 else (1.2 if signal_score >= 75 else 1.0)
        actual_risk = self.config.risk_per_trade * risk_mult
        risk_amount = available * actual_risk
        sl_distance = abs(entry_price - stop_loss) / entry_price

        print(f"{pfx} 📐 entry={entry_price} | SL={stop_loss} | sl_dist={sl_distance:.4%}")

        if sl_distance < 0.001:
            print(f"{pfx} ❌ SKIP — SL too small ({sl_distance:.4%})")
            return None

        position_value = risk_amount / sl_distance
        leverage       = self._calc_leverage(signal_score)
        size           = position_value / entry_price

        # ── 7. Max notional cap (fix code=101209 edge case) ───────────────────
        # Используем 92% от лимита — 8% запас против edge-case на точной границе
        sym_info     = await self.bingx.get_symbol_info(bingx_symbol)
        raw_max      = sym_info.get("max_notional", self.config.max_position_usdt)
        # Берём минимум из глобального и символьного лимита, с запасом 8%
        effective_max = min(self.config.max_position_usdt, raw_max) * self.config.notional_safety_pct
        notional      = size * entry_price

        if notional > effective_max:
            old_size = size
            size     = effective_max / entry_price
            notional = size * entry_price
            actual_risk = (notional * sl_distance) / available if available else actual_risk
            print(f"{pfx} ⚠️ Notional capped: ${old_size*entry_price:,.0f} → ${notional:,.0f} "
                  f"(effective_max=${effective_max:,.0f})")

        print(f"{pfx} 📐 risk={actual_risk*100:.3f}% | notional=${notional:,.0f} | "
              f"size={size:.6f} | leverage={leverage}x")

        # ── 8. BingX params ───────────────────────────────────────────────────
        side          = "BUY"  if direction == "long"  else "SELL"
        position_side = "LONG" if direction == "long"  else "SHORT"

        tp1_price = None
        if take_profits:
            tp_item = take_profits[0]
            if isinstance(tp_item, (list, tuple)):
                tp1_price = float(tp_item[0])
            elif isinstance(tp_item, dict):
                tp1_price = float(tp_item.get("price", 0)) or None

        # ── 9. Main order с RETRY при 101209 ─────────────────────────────────
        print(f"{pfx} 📤 Sending order to BingX [{mode}]...")
        self._last_open_ts = time.time()

        # ✅ FIX: НЕ передаём take_profit в основной ордер!
        # takeProfit на основном ордере BingX закрывает ВСЮ позицию при срабатывании TP1.
        # Все TP размещаются отдельно через _place_tp_orders_hedge с правильными размерами.
        order = await self.bingx.place_market_order(
            symbol=bingx_symbol, side=side, position_side=position_side,
            size=size, stop_loss=stop_loss, take_profit=None,
        )

        # ── RETRY логика для code=101209 ──────────────────────────────────────
        if order is None and self.bingx.last_error_code == 101209:
            err_msg = self.bingx.last_error or ""
            parsed_max = _parse_max_notional_from_error(err_msg)

            if parsed_max and parsed_max > 0:
                # Применяем реальный лимит с запасом
                retry_max  = parsed_max * self.config.notional_safety_pct
                retry_size = retry_max / entry_price
                retry_size = max(retry_size, 0.001)

                print(f"{pfx} 🔄 RETRY 101209: parsed_max=${parsed_max:,.0f} → "
                      f"retry_max=${retry_max:,.0f} | retry_size={retry_size:.6f}")

                order = await self.bingx.place_market_order(
                    symbol=bingx_symbol, side=side, position_side=position_side,
                    size=retry_size, stop_loss=stop_loss, take_profit=None,  # ✅ FIX
                )
                if order:
                    size        = retry_size
                    notional    = retry_size * entry_price
                    actual_risk = (notional * sl_distance) / available if available else actual_risk
                    print(f"{pfx} ✅ RETRY succeeded! qty={retry_size:.4f} notional=${notional:,.0f}")
            else:
                print(f"{pfx} ⚠️ RETRY skipped: can't parse max from '{err_msg[:80]}'")

        if order is None:
            self._last_open_ts = 0.0   # сбрасываем cooldown при финальной ошибке
            err  = self.bingx.last_error or "unknown"
            code = self.bingx.last_error_code
            hint = ""
            try:
                from api.bingx_client import BingXClient as _BX
                hint = _BX.ERROR_CODES.get(code, "") if code else ""
            except Exception:
                pass
            print(f"{pfx} ❌ ORDER FAILED — code={code} | {err}")
            await self._tg(
                f"❌ <b>AutoTrader [{mode}] — ОРДЕР ОТКЛОНЁН</b>\n\n"
                f"<code>#{symbol}</code> {direction.upper()}\n"
                f"Score: {signal_score:.0f}% | SL: {stop_loss}\n\n"
                f"🔴 code={code}: <code>{err}</code>"
                + (f"\n💡 {hint}" if hint else "")
            )
            return None

        # ── 10. TP1-TP6 — ALL как отдельные ордера с правильным qty ──────────
        # ✅ FIX: передаём ВСЕ TPs включая TP1 (раньше TP1 шёл в основной ордер)
        if take_profits:
            asyncio.create_task(
                self._place_tp_orders_hedge(
                    bingx_symbol  = bingx_symbol,
                    position_side = position_side,
                    total_size    = order.size,
                    take_profits  = take_profits,   # ✅ все TPs, не [1:]
                    direction     = direction,
                    start_num     = 1,              # ✅ начинаем с TP1
                )
            )

        # ── 11. Save ──────────────────────────────────────────────────────────
        position_data = {
            "symbol":       symbol,
            "direction":    direction,
            "entry_price":  entry_price,
            "size":         order.size,
            "leverage":     leverage,
            "stop_loss":    stop_loss,
            "take_profits": take_profits,
            "signal_score": signal_score,
            "smc_data":     smc_data,
            "order_id":     order.order_id,
            "opened_at":    datetime.utcnow().isoformat(),
            "timestamp":    datetime.utcnow().isoformat(),
            "status":       "active",
            "risk_pct":     round(actual_risk * 100, 4),
            "tg_msg_id":    tg_msg_id,
            "taken_tps":    [],
            "be_done":      False,
        }
        bot_type = "long" if direction == "long" else "short"
        self.redis.save_signal(bot_type, symbol, position_data)
        self.redis.save_position(bot_type, symbol, position_data)
        self.daily_trades += 1

        print(f"✅ {pfx} Position opened [{mode}]! id={order.order_id}")

        d_emoji    = "🟢" if direction == "long" else "🔴"
        notify_msg = (
            f"🤖 <b>AUTO-TRADE [{mode}]</b>\n\n"
            f"{d_emoji} <code>#{symbol}</code> {direction.upper()}\n"
            f"📍 Entry: <b>{entry_price}</b>\n"
            f"🛑 SL: <b>{stop_loss}</b>\n"
            f"📊 Size: {order.size} | {leverage}x | {actual_risk*100:.3f}% risk\n"
            f"🎯 Score: {signal_score:.0f}%\n"
            f"🆔 OrderID: {order.order_id}"
        )
        await self._tg_reply(notify_msg, tg_msg_id)
        return position_data

    # =========================================================================
    # TP HEDGE MODE — без reduceOnly
    # =========================================================================

    async def _place_tp_orders_hedge(self, bingx_symbol, position_side,
                                      total_size, take_profits, direction,
                                      start_num: int = 1):   # ✅ FIX: TP1 starts at 1
        await asyncio.sleep(1.5)
        close_side = "SELL" if direction == "long" else "BUY"
        success = 0
        fails   = 0

        for i, tp_raw in enumerate(take_profits):
            try:
                if isinstance(tp_raw, (list, tuple)):
                    tp_price  = float(tp_raw[0])
                    tp_weight = float(tp_raw[1]) / 100 if len(tp_raw) > 1 else 0.2
                elif isinstance(tp_raw, dict):
                    tp_price  = float(tp_raw.get("price", 0))
                    tp_weight = float(tp_raw.get("weight", 20)) / 100
                else:
                    continue

                if tp_price <= 0 or tp_weight <= 0:
                    continue

                tp_size       = total_size * tp_weight
                tp_num        = i + start_num  # ✅ FIX: используем start_num
                rounded_price = await self.bingx._round_price(bingx_symbol, tp_price)
                rounded_size  = await self.bingx._round_qty(bingx_symbol, tp_size)

                if rounded_size <= 0:
                    continue

                print(f"📤 TP{tp_num}: {bingx_symbol} {close_side} {position_side} "
                      f"qty={rounded_size} stopPrice={rounded_price}")

                # ✅ НЕТ reduceOnly — Hedge Mode!
                body = {
                    "symbol":       bingx_symbol,
                    "side":         close_side,
                    "positionSide": position_side,
                    "type":         "TAKE_PROFIT_MARKET",
                    "quantity":     str(rounded_size),
                    "stopPrice":    str(rounded_price),
                    "workingType":  "MARK_PRICE",
                }

                result = await self.bingx._make_request(
                    "POST", "/openApi/swap/v2/trade/order", body=body
                )

                if result and result.get("code") == 0:
                    d        = result.get("data", {})
                    ord_d    = d.get("order", d)
                    order_id = ord_d.get("orderId", "?")
                    print(f"✅ TP{tp_num}: {bingx_symbol} id={order_id}")
                    success += 1
                else:
                    err = (result or {}).get("msg") or self.bingx.last_error or "unknown"
                    print(f"⚠️ TP{tp_num} failed: {bingx_symbol} | {err}")
                    fails += 1

                await asyncio.sleep(0.4)

            except Exception as e:
                print(f"⚠️ TP{i+2} exception: {bingx_symbol} | {e}")
                fails += 1

        status = "✅" if success > 0 else "⚠️"
        print(f"{status} TP orders {bingx_symbol}: {success} placed, {fails} failed")

    # =========================================================================
    # CLOSE
    # =========================================================================

    async def close_position(self, symbol: str, position_side: str) -> bool:
        bingx_symbol = self._to_bingx_symbol(symbol)
        ok = await self.bingx.close_position(bingx_symbol, position_side)
        if ok:
            bot_type = "long" if position_side == "LONG" else "short"
            self.redis.close_position(bot_type, symbol, 0.0, 0.0)
        return ok

    async def close_all_positions(self) -> int:
        positions = await self.bingx.get_positions()
        closed = 0
        for p in positions:
            if abs(p.size) > 0 and await self.close_position(p.symbol, p.position_side):
                closed += 1
        return closed

    async def get_account_summary(self) -> Dict:
        try:
            balance    = await self.bingx.get_account_balance() or {}
            positions  = await self.bingx.get_positions()
            unrealized = sum(p.unrealized_pnl for p in positions)
            return {
                "balance":        balance,
                "open_positions": len(positions),
                "unrealized_pnl": unrealized,
                "daily_trades":   self.daily_trades,
                "daily_pnl_pct":  round(self.daily_pnl, 4),
                "total_pnl":      self.total_pnl,
                "win_count":      self.win_count,
                "loss_count":     self.loss_count,
                "mode":           "DEMO" if self.config.demo_mode else "REAL",
            }
        except Exception as e:
            print(f"❌ get_account_summary: {e}")
            return {}

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _to_bingx_symbol(self, symbol: str) -> str:
        if "-" not in symbol and symbol.endswith("USDT"):
            return symbol[:-4] + "-USDT"
        return symbol

    def _calc_leverage(self, score: float) -> int:
        base = self.config.default_leverage
        if score >= 85: return min(self.config.max_leverage, base + 2)
        if score >= 75: return min(self.config.max_leverage, base + 1)
        return base

    def _check_daily_reset(self):
        today = datetime.utcnow().date()
        if today != self.last_reset:
            self.daily_pnl    = 0.0
            self.daily_trades = 0
            self.last_reset   = today
            print("📅 Daily stats reset")

    def record_trade_result(self, pnl_pct: float):
        """pnl_pct в % (напр. 1.5 = +1.5%). daily_pnl та же единица."""
        self.total_pnl    += pnl_pct
        self.daily_pnl    += pnl_pct
        self.daily_trades += 1
        if pnl_pct > 0: self.win_count  += 1
        else:           self.loss_count += 1
        print(f"📊 Trade: {pnl_pct:+.2f}% | Day: {self.daily_pnl:+.2f}% | "
              f"Total trades: {self.daily_trades}")
