import os
"""
Auto Trader v3.2 — HARDENED ENTRY CONDITIONS

ИЗМЕНЕНИЯ v3.0:
  ✅ max_daily_risk: 5.0% → 3.0% (жёстче дневной лимит)
  ✅ max_daily_trades: 12 (новый лимит сделок в день)
  ✅ min_score_for_trade: 65 → 70 (меньше входов, выше качество)
  ✅ risk_per_trade: 0.0005 → 0.0004 (-20% риска на сделку)
  ✅ risk_mult убран — нет 1.5x на позиции (агрессия удалена)
  ✅ RR Filter: TP1 / SL >= 1.5 — отклонение плохих RR
  ✅ max_positions: 20 → 12 (меньше одновременных позиций)
  ✅ Cooldown 30с между открытиями (антидубль)
  ✅ code=101209 RETRY: парсим фактический лимит → retry

ИЗМЕНЕНИЯ v3.1:
  ✅ min_rr_ratio: 1.5 → 1.0 (более гибкие входы для стратегий с близким TP1)
  ✅ FIX: Telegram HTML parse error — убраны <br>, используем \n

ИЗМЕНЕНИЯ v3.2:
  ✅ FIX: Telegram HTML escaping — экранируем &, <, > для избежания parse errors
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
    if not error_msg:
        return None
    m = re.search(r'is\s+([\d,]+(?:\.\d+)?)\s+USDT', error_msg, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except Exception:
            pass
    return None


def _escape_value(value: str) -> str:
    """
    Экранировать HTML спецсимволы в пользовательских значениях.
    Использовать для symbol, error_msg и других внешних данных.
    """
    if not value or not isinstance(value, str):
        return value
    return (value
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


@dataclass
class TradeConfig:
    enabled:             bool  = True
    demo_mode:           bool  = True
    max_positions:       int   = 12          # ✅ v3.0: было 20 → меньше одновременных
    risk_per_trade:      float = 0.0004      # ✅ v3.0: было 0.0005 → -20%
    max_daily_risk:      float = 3.0         # ✅ v3.0: было 5.0% → жёстче
    max_daily_trades:    int   = 12          # ✅ v3.0: НОВЫЙ лимит сделок
    min_rr_ratio:        float = 1.0         # ✅ v3.1: снижено с 1.5 → 1.0 для более гибких входов
    default_leverage:    int   = 20
    min_leverage:        int   = 5
    max_leverage:        int   = 50
    min_score_for_trade: int   = 70          # ✅ v3.0: было 65 → +5 пунктов
    max_position_usdt:   float = 5000.0
    notional_safety_pct: float = 0.92
    open_cooldown_sec:   float = 30.0


class AutoTrader:

    def __init__(self, bingx_client=None, config=None, telegram=None):
        self.config   = config or TradeConfig()
        self.bingx    = bingx_client or BingXClient(demo=self.config.demo_mode)
        self.redis    = get_redis_client()
        self.telegram = telegram

        self.daily_pnl    = 0.0
        self.daily_trades = 0
        self.total_pnl    = 0.0
        self.win_count    = 0
        self.loss_count   = 0
        self.last_reset   = datetime.utcnow().date()
        # ✅ FIX v3.0: per-symbol cooldown (было глобальное — блокировало все символы на 30с)
        self._last_open_ts: dict = {}  # symbol → timestamp

        mode = "DEMO" if self.config.demo_mode else "REAL"
        print(f"🤖 AutoTrader v3.0 initialized ({mode})")
        print(f"   Risk/trade: {self.config.risk_per_trade*100:.3f}% | "
              f"Max pos: {self.config.max_positions} | "
              f"Min score: {self.config.min_score_for_trade} | "
              f"Max daily risk: {self.config.max_daily_risk}% | "
              f"Max daily trades: {self.config.max_daily_trades} | "
              f"Min RR: {self.config.min_rr_ratio}")

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
        # FIX: extract max leverage from signal (e.g. "5-30".split("-")[1]=30 or int "20")
        _sig_lev_raw = signal.get("leverage", "")
        try:
            if isinstance(_sig_lev_raw, str) and "-" in str(_sig_lev_raw):
                _sig_lev = int(str(_sig_lev_raw).split("-")[1])
            else:
                _sig_lev = int(_sig_lev_raw) if _sig_lev_raw else 0
        except (ValueError, IndexError):
            _sig_lev = 0
        try:
            return await self.open_position(
                symbol           = symbol,
                direction        = signal["direction"],
                entry_price      = signal["entry_price"],
                stop_loss        = signal["stop_loss"],
                take_profits     = signal["take_profits"],
                signal_score     = signal["score"],
                signal_leverage  = _sig_lev,
                smc_data         = signal.get("smc"),
                tg_msg_id        = signal.get("tg_msg_id"),
                ms_context       = {k: signal[k] for k in signal if k.startswith("ms_")},
                pos_multiplier   = float(signal.get("pos_multiplier", 1.0)),
            )
        except KeyError as e:
            print(f"❌ [AutoTrader] {symbol}: missing field {e}")
            return None
        except Exception as e:
            import traceback
            print(f"❌ [AutoTrader] {symbol}: {e}\n{traceback.format_exc()}")
            return None

    async def open_position(self, symbol, direction, entry_price, stop_loss,
                            take_profits, signal_score, signal_leverage=0,
                            smc_data=None, tg_msg_id=None, ms_context=None,
                            pos_multiplier: float = 1.0) -> Optional[Dict]:
        mode = "DEMO" if self.config.demo_mode else "REAL"
        pfx  = f"[AT][{symbol}][{direction.upper()}]"

        # ── 0. Cooldown (per-symbol) ─────────────────────────────────────────
        since_last = time.time() - self._last_open_ts.get(symbol, 0)
        if since_last < self.config.open_cooldown_sec:
            print(f"{pfx} ⏸ SKIP — cooldown ({since_last:.0f}s)")
            return None

        if not self.config.enabled:
            return None

        # ── 1a. Score filter ──────────────────────────────────────────────────
        if signal_score < self.config.min_score_for_trade:
            print(f"{pfx} ⏸ SKIP — score {signal_score:.1f} < {self.config.min_score_for_trade}")
            return None

        # ── 1b. Daily risk limit ──────────────────────────────────────────────
        self._check_daily_reset()
        if self.daily_pnl <= -self.config.max_daily_risk:
            print(f"{pfx} ⏸ SKIP — daily risk limit {self.daily_pnl:.2f}% <= -{self.config.max_daily_risk}%")
            safe_symbol = _escape_value(symbol)
            await self._tg(
                f"⏸ <b>[{mode}]</b> <code>#{safe_symbol}</code>: "
                f"дневной лимит ({self.daily_pnl:.2f}% ≤ -{self.config.max_daily_risk}%)"
            )
            return None

        # ── 1c. Daily trade count limit ───────────────────────────────────────
        if self.daily_trades >= self.config.max_daily_trades:
            print(f"{pfx} ⏸ SKIP — daily trade limit ({self.daily_trades}/{self.config.max_daily_trades})")
            safe_symbol = _escape_value(symbol)
            await self._tg(
                f"⏸ <b>[{mode}]</b> <code>#{safe_symbol}</code>: "
                f"лимит сделок за день ({self.daily_trades}/{self.config.max_daily_trades})"
            )
            return None

        # ── 1d. RR Filter — TP1 / SL >= min_rr_ratio ─────────────────────────
        if take_profits and self.config.min_rr_ratio > 0:
            tp1_raw = take_profits[0]
            try:
                if isinstance(tp1_raw, (list, tuple)):
                    tp1_price = float(tp1_raw[0])
                elif isinstance(tp1_raw, dict):
                    tp1_price = float(tp1_raw.get("price", 0))
                else:
                    tp1_price = float(tp1_raw)

                sl_dist  = abs(entry_price - stop_loss)
                tp1_dist = abs(tp1_price - entry_price)
                rr = tp1_dist / sl_dist if sl_dist > 0 else 0
                self._last_rr = round(rr, 2)  # ← FIX: was never assigned

                if rr < self.config.min_rr_ratio:
                    print(f"{pfx} ⏸ SKIP — RR too low ({rr:.2f} < {self.config.min_rr_ratio})")
                    safe_symbol = _escape_value(symbol)
                    await self._tg(
                        f"⏸ <b>[{mode}]</b> <code>#{safe_symbol}</code>\n"
                        f"RR слишком низкий: <b>{rr:.2f}</b> < {self.config.min_rr_ratio}\n"
                        f"Entry={entry_price} | TP1={tp1_price} | SL={stop_loss}"
                    )
                    return None
                print(f"{pfx} ✅ RR={rr:.2f} >= {self.config.min_rr_ratio}")
            except Exception as e:
                print(f"{pfx} ⚠️ RR check failed: {e}")

        # ── 2. Positions ──────────────────────────────────────────────────────
        print(f"{pfx} 🔍 Checking open positions...")
        try:
            current_positions = await self.bingx.get_positions()
        except Exception as e:
            print(f"{pfx} ❌ get_positions failed: {e}")
            return None

        # Считаем только позиции ЭТОГО направления (LONG-бот → только LONG, SHORT-бот → только SHORT)
        _dir_side = "LONG" if direction == "long" else "SHORT"
        dir_positions = [
            p for p in current_positions
            if (
                getattr(p, "position_side", "").upper() == _dir_side
                or getattr(p, "positionSide", "").upper() == _dir_side
                or getattr(p, "side", "").upper() == _dir_side
                or (direction == "long"  and getattr(p, "direction", "").upper() == "BUY")
                or (direction == "short" and getattr(p, "direction", "").upper() == "SELL")
            ) and abs(getattr(p, "size", 0) or 0) > 0
        ]
        n_pos    = len(dir_positions)
        n_total  = len(current_positions)
        pos_list = " | ".join(f"{p.symbol}({p.side})" for p in current_positions)
        print(f"{pfx} 📊 Open: {n_pos}/{self.config.max_positions} {_dir_side} ({n_total} total)")
        if pos_list:
            print(f"{pfx} 📋 {pos_list}")

        if n_pos >= self.config.max_positions:
            print(f"{pfx} ⏸ SKIP — max {_dir_side} positions")
            safe_symbol = _escape_value(symbol)
            await self._tg_reply(
                f"⏸ <b>Лимит {_dir_side} позиций достигнут</b> ({n_pos}/{self.config.max_positions})\n"
                f"<b>#{safe_symbol}</b> — сигнал пропущен", tg_msg_id
            )
            return None

        # ── 3. Duplicate ──────────────────────────────────────────────────────
        bingx_symbol = self._to_bingx_symbol(symbol)
        existing = [p for p in current_positions
                    if p.symbol.replace("-", "") == symbol.replace("-", "")]
        if existing:
            print(f"{pfx} ⏸ SKIP — already open ({existing[0].side})")
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

        # ── 6. Sizing — FLAT risk × context multiplier ───────────────────────
        actual_risk = self.config.risk_per_trade
        if pos_multiplier <= 0.0:
            print(f"{pfx} 🛑 SKIP — pos_multiplier=0.0 (hard block by context)")
            return None
        if pos_multiplier < 1.0:
            actual_risk *= pos_multiplier
            print(f"{pfx} ⚠️ Risk reduced ×{pos_multiplier} → {actual_risk:.6f} (market context)")
        risk_amount = available * actual_risk
        sl_distance = abs(entry_price - stop_loss) / entry_price

        print(f"{pfx} 📐 entry={entry_price} | SL={stop_loss} | sl_dist={sl_distance:.4%}")

        if sl_distance < 0.001:
            print(f"{pfx} ❌ SKIP — SL too small ({sl_distance:.4%})")
            return None

        # 🚨 FIX: Никогда не открываем позицию без валидного SL
        if not stop_loss or stop_loss <= 0:
            print(f"{pfx} ❌ SKIP — stop_loss=0 (critical: no SL defined)")
            return None

        position_value = risk_amount / sl_distance
        leverage       = self._calc_leverage(signal_score, signal_leverage)
        size           = position_value / entry_price

        # ── 7. Max notional cap ───────────────────────────────────────────────
        sym_info      = await self.bingx.get_symbol_info(bingx_symbol)
        raw_max       = sym_info.get("max_notional", self.config.max_position_usdt)
        effective_max = min(self.config.max_position_usdt, raw_max) * self.config.notional_safety_pct
        notional      = size * entry_price

        if notional > effective_max:
            old_size = size
            size     = effective_max / entry_price
            notional = size * entry_price
            actual_risk = (notional * sl_distance) / available if available else actual_risk
            print(f"{pfx} ⚠️ Notional capped: ${old_size*entry_price:,.0f} → ${notional:,.0f}")

        print(f"{pfx} 📐 risk={actual_risk*100:.3f}% | notional=${notional:,.0f} | "
              f"size={size:.6f} | leverage={leverage}x")

        # ── # ✅ FIX v17: BingX notional cap before order
        _bingx_max = float(os.environ.get("BINGX_MAX_NOTIONAL", "5000"))
        if notional > _bingx_max:
            size = _bingx_max / entry_price
            notional = size * entry_price
            print(f"{pfx} CAP notional -> ${_bingx_max:,.0f}")
        # 8. BingX params
        side          = "BUY"  if direction == "long"  else "SELL"
        position_side = "LONG" if direction == "long"  else "SHORT"

        # ── 9. Main order с RETRY при 101209 ─────────────────────────────────
        print(f"{pfx} 📤 Sending order to BingX [{mode}]...")
        self._last_open_ts[symbol] = time.time()

        order = await self.bingx.place_market_order(
            symbol=bingx_symbol, side=side, position_side=position_side,
            size=size, stop_loss=stop_loss, take_profit=None,
        )

        if order is None and self.bingx.last_error_code == 101209:
            err_msg    = self.bingx.last_error or ""
            parsed_max = _parse_max_notional_from_error(err_msg)
            if parsed_max and parsed_max > 0:
                retry_max  = parsed_max * self.config.notional_safety_pct
                retry_size = max(retry_max / entry_price, 0.001)
                print(f"{pfx} 🔄 RETRY 101209: parsed_max=${parsed_max:,.0f} → retry_size={retry_size:.6f}")
                order = await self.bingx.place_market_order(
                    symbol=bingx_symbol, side=side, position_side=position_side,
                    size=retry_size, stop_loss=stop_loss, take_profit=None,
                )
                if order:
                    size = retry_size
                    notional = retry_size * entry_price

        if order is None:
            self._last_open_ts.pop(symbol, None)
            err  = self.bingx.last_error or "unknown"
            code = self.bingx.last_error_code
            print(f"{pfx} ❌ ORDER FAILED — code={code} | {err}")
            safe_symbol = _escape_value(symbol)
            safe_err = _escape_value(err)
            await self._tg(
                f"❌ <b>AutoTrader [{mode}] — ОРДЕР ОТКЛОНЁН</b>\n\n"
                f"<code>#{safe_symbol}</code> {direction.upper()}\n"
                f"Score: {signal_score:.0f} | SL: {stop_loss}\n\n"
                f"<pre>{safe_err}</pre>"
            )
            return None

        # ── 10. TP1-TP6 — ALL как отдельные ордера ───────────────────────────
        if take_profits:
            asyncio.create_task(
                self._place_tp_orders_hedge(
                    bingx_symbol=bingx_symbol, position_side=position_side,
                    total_size=order.size, take_profits=take_profits,
                    direction=direction, start_num=1,
                )
            )

        # ── 10.5. LOG-1: Проверяем что SL выставлен на бирже ────────────────
        asyncio.create_task(
            self._ensure_sl(bingx_symbol, position_side, stop_loss, direction, order.size, tg_msg_id)
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
            "be2_done":     False,
            **(ms_context or {}),  # MS-данные: pivot_pp, pdh, pdl, cme_gap, zone_4h, htf_structure
        }
        bot_type = "long" if direction == "long" else "short"
        self.redis.save_signal(bot_type, symbol, position_data)
        self.redis.save_position(bot_type, symbol, position_data)
        self.daily_trades += 1

        print(f"✅ {pfx} Position opened [{mode}]! id={order.order_id}")

        exchange_label = "BingX DEMO" if self.config.demo_mode else "BingX REAL"
        d_emoji    = "🟢" if direction == "long" else "🔴"
        safe_symbol = _escape_value(symbol)
        notify_msg = (
            f"🤖 <b>AUTO-TRADE [{mode}]</b>\n\n"
            f"{d_emoji} <code>#{safe_symbol}</code> {direction.upper()}\n"
            f"📍 Entry: <b>{entry_price}</b>\n"
            f"🛑 SL: <b>{stop_loss}</b>\n"
            f"📊 Size: {order.size} | {leverage}x | {actual_risk*100:.3f}% risk\n"
            f"🎯 Score: {signal_score:.0f} | RR: {self._last_rr:.2f}\n"
            f"🆔 OrderID: {order.order_id}\n"
            f"✅ Позиция открыта на бирже {exchange_label}"
        )
        await self._tg_reply(notify_msg, tg_msg_id)
        return position_data

    async def _place_tp_orders_hedge(self, bingx_symbol, position_side,
                                      total_size, take_profits, direction,
                                      start_num: int = 1):
        await asyncio.sleep(1.5)

        # B2 FIX (Variant A): чистим осиротевшие TP ордера перед выставлением новых
        await self._cancel_orphaned_tp_orders()

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
                tp_num        = i + start_num
                rounded_price = await self.bingx._round_price(bingx_symbol, tp_price)
                rounded_size  = await self.bingx._round_qty(bingx_symbol, tp_size)

                if rounded_size <= 0:
                    continue

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
                    err_code = (result or {}).get("code")
                    err      = (result or {}).get("msg") or self.bingx.last_error or "unknown"
                    if err_code == 110206:
                        # B2 FIX (Variant B): Too many orders — пауза 2s и 1 retry
                        print(f"[B2 RETRY] TP{tp_num} {bingx_symbol}: 110206 too many orders — retry через 2s")
                        await asyncio.sleep(2.0)
                        retry_result = await self.bingx._make_request(
                            "POST", "/openApi/swap/v2/trade/order", body=body
                        )
                        if retry_result and retry_result.get("code") == 0:
                            rd       = retry_result.get("data", {})
                            ord_rd   = rd.get("order", rd)
                            order_id = ord_rd.get("orderId", "?")
                            print(f"✅ TP{tp_num} (retry OK): {bingx_symbol} id={order_id}")
                            success += 1
                        else:
                            retry_err = (retry_result or {}).get("msg") or "unknown"
                            print(f"⚠️ TP{tp_num} retry failed: {bingx_symbol} | {retry_err} — прерываем")
                            fails += 1
                            break  # лимит не освободился — дальше ставить бессмысленно
                    else:
                        print(f"⚠️ TP{tp_num} failed: {bingx_symbol} | {err}")
                        fails += 1

                await asyncio.sleep(0.4)
            except Exception as e:
                print(f"⚠️ TP{i+2} exception: {bingx_symbol} | {e}")
                fails += 1

        status = "✅" if success > 0 else "⚠️"
        print(f"{status} TP orders {bingx_symbol}: {success} placed, {fails} failed")

    async def _cancel_orphaned_tp_orders(self) -> int:
        """
        B2 FIX (Variant A): Отменяем осиротевшие TAKE_PROFIT_MARKET ордера
        (от уже закрытых позиций) перед выставлением новых TP.
        Освобождает лимит ордеров на аккаунте BingX (~20-50 слотов).
        """
        try:
            # 1. Все открытые ордера по аккаунту (без фильтра по символу)
            all_result = await self.bingx._make_request(
                "GET", "/openApi/swap/v2/trade/openOrders"
            )
            if not all_result or all_result.get("code") != 0:
                print("[B2 ORPHAN] Не удалось получить все открытые ордера")
                return 0

            all_orders = all_result.get("data", {}).get("orders", [])
            tp_orders = [o for o in all_orders if o.get("type") == "TAKE_PROFIT_MARKET"]
            if not tp_orders:
                return 0

            # 2. Активные позиции — символы с ненулевым qty
            active_positions = await self.bingx.get_positions()
            active_symbols = {
                p.symbol.replace("-", "")
                for p in active_positions
                if abs(getattr(p, "size", 0) or 0) > 0
            }

            # 3. Отменяем TP ордера без соответствующей позиции
            cancelled = 0
            for order in tp_orders:
                o_symbol = order.get("symbol", "").replace("-", "")
                o_id     = order.get("orderId", "")
                if o_symbol and o_id and o_symbol not in active_symbols:
                    cancel_res = await self.bingx._make_request(
                        "DELETE", "/openApi/swap/v2/trade/order",
                        body={"symbol": o_symbol, "orderId": str(o_id)}
                    )
                    if cancel_res and cancel_res.get("code") == 0:
                        cancelled += 1
                        print(f"[B2 ORPHAN] Отменён осиротевший TP id={o_id} ({o_symbol})")
                    else:
                        print(f"[B2 ORPHAN] Не удалось отменить TP id={o_id} ({o_symbol}): "
                              f"{(cancel_res or {}).get('msg', self.bingx.last_error)}")
                    await asyncio.sleep(0.2)  # rate limit

            if cancelled:
                print(f"[B2 ORPHAN] Итого отменено осиротевших TP: {cancelled}/{len(tp_orders)}")
            return cancelled
        except Exception as e:
            print(f"[B2 ORPHAN] Exception: {e}")
            return 0

    async def _ensure_sl(self, bingx_symbol: str, position_side: str,
                          stop_loss: float, direction: str,
                          qty: float, tg_msg_id: Optional[int]):
        """LOG-1: После открытия позиции проверяем что SL выставлен на бирже.
        3 попытки: 5s → 15s → 30s. При неудаче — критический алерт в Telegram."""
        delays = [5, 15, 30]
        for attempt, delay in enumerate(delays, 1):
            await asyncio.sleep(delay)
            try:
                orders_result = await self.bingx._make_request(
                    "GET", "/openApi/swap/v2/trade/openOrders",
                    params={"symbol": bingx_symbol}
                )
                open_orders = []
                if orders_result and orders_result.get("code") == 0:
                    open_orders = orders_result.get("data", {}).get("orders", [])

                sl_exists = any(
                    o.get("type") in ("STOP_MARKET", "STOP")
                    and o.get("positionSide") == position_side
                    for o in open_orders
                )

                if sl_exists:
                    print(f"✅ [SL_CHECK] {bingx_symbol}/{position_side}: SL подтверждён (попытка {attempt})")
                    return

                print(f"⚠️ [SL_CHECK] {bingx_symbol}/{position_side}: SL не найден (попытка {attempt}), выставляем...")
                ok = await self.bingx.update_stop_loss(bingx_symbol, position_side, stop_loss, direction)
                if ok:
                    print(f"✅ [SL_CHECK] {bingx_symbol}/{position_side}: SL выставлен (попытка {attempt})")
                    return

                code = getattr(self.bingx, 'last_error_code', None)
                print(f"⚠️ [SL_CHECK] {bingx_symbol}/{position_side}: SL отклонён (попытка {attempt}), code={code}")

            except Exception as e:
                print(f"⚠️ [SL_CHECK] {bingx_symbol}/{position_side}: Exception (попытка {attempt}): {e}")

        safe_sym = _escape_value(bingx_symbol)
        await self._tg_reply(
            f"🚨 <b>КРИТИЧНО: SL не выставлен!</b>\n"
            f"<code>{safe_sym}</code> {position_side} qty={qty}\n"
            f"SL={stop_loss} — все {len(delays)} попытки провалились\n"
            f"⚠️ Проверьте и выставьте SL вручную!",
            tg_msg_id
        )

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

    _last_rr: float = 0.0

    def _to_bingx_symbol(self, symbol: str) -> str:
        if "-" not in symbol and symbol.endswith("USDT"):
            return symbol[:-4] + "-USDT"
        return symbol

    def _calc_leverage(self, score: float, signal_leverage: int = 0) -> int:
        # FIX: score-based dynamic leverage within configured min/max range
        # signal_leverage > 0 → use as ceiling (from signal config e.g. "5-30")
        max_lev = signal_leverage if signal_leverage > 0 else self.config.max_leverage
        max_lev = min(max_lev, self.config.max_leverage)
        min_lev = self.config.min_leverage
        # Score 65→min_lev, Score 100→max_lev, linear interpolation
        normalized = max(0.0, min(1.0, (score - 65.0) / 35.0))
        lev = int(min_lev + normalized * (max_lev - min_lev))
        return max(min_lev, min(max_lev, lev))

    def _check_daily_reset(self):
        today = datetime.utcnow().date()
        if today != self.last_reset:
            self.daily_pnl    = 0.0
            self.daily_trades = 0
            self.last_reset   = today
            print("📅 Daily stats reset")

    def record_trade_result(self, pnl_pct: float):
        self.total_pnl    += pnl_pct
        self.daily_pnl    += pnl_pct
        self.daily_trades += 1
        if pnl_pct > 0: self.win_count  += 1
        else:           self.loss_count += 1
        print(f"📊 Trade: {pnl_pct:+.2f}% | Day: {self.daily_pnl:+.2f}% | "
              f"Total trades: {self.daily_trades}")
