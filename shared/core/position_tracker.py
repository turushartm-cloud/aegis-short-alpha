"""
Position Tracker v3.0

ИЗМЕНЕНИЯ v3.0:
  ✅ BE после TP1 → SL точно на entry (было entry+0.15%)
  ✅ LOCK после TP2 → SL на entry +0.2% (LONG) / entry -0.2% (SHORT)
  ✅ Trailing активируется только ПОСЛЕ LOCK (TP2), не сразу после BE
  ✅ TRAIL_DISTANCE: 1.2% → 1.0% (плотнее трейлинг)
  ✅ be2_done флаг для двухступенчатого BE
  ✅ Notify: указывает TP1/TP2 при переносе стопа
  ✅ _record_pnl: tp_level в историю сделок
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple


class PositionTracker:
    """
    Каждые CHECK_INTERVAL секунд:
      1. Берёт active сигналы из Redis
      2. Получает текущую цену через Binance
      3. TP1 hit → SL на entry (BE)
      4. TP2 hit → SL на entry ±0.2% (LOCK)
      5. После LOCK → trailing с шагом 1.0%
    """

    CHECK_INTERVAL = 30

    # ── Trailing (активируется ПОСЛЕ LOCK, т.е. после TP2) ──────────────────
    TRAIL_DISTANCE   = 0.010    # 1.0% от текущей цены (плотнее, было 1.2%)

    # ── TP1 hit → SL точно на entry (нулевой BE) ─────────────────────────────
    BREAKEVEN_AFTER_TP   = 1
    BREAKEVEN_BUFFER     = 0.000   # ровно entry — гарантированный 0% убыток

    # ── TP2 hit → SL на entry ± 0.2% (LOCK с небольшой прибылью) ────────────
    LOCK_AFTER_TP        = 2
    LOCK_BUFFER          = 0.002   # +0.2% для LONG, -0.2% для SHORT

    def __init__(self, *, bot_type, telegram, redis_client,
                 binance_client, config, auto_trader=None):
        self.bot_type    = bot_type
        self.tg          = telegram
        self.redis       = redis_client
        self.binance     = binance_client
        self.config      = config
        self.auto_trader = auto_trader
        self._running    = False

    async def run(self):
        self._running = True
        print(f"📍 PositionTracker v3.0 started (interval={self.CHECK_INTERVAL}s)")
        while self._running:
            try:
                await self._scan_all()
            except Exception as e:
                print(f"[PositionTracker] loop error: {e}")
            await asyncio.sleep(self.CHECK_INTERVAL)

    def stop(self):
        self._running = False

    async def _scan_all(self):
        try:
            signals = self.redis.get_active_signals(self.bot_type)
        except Exception as e:
            print(f"[PositionTracker] redis error: {e}")
            return
        if not signals:
            return
        for sig in signals:
            if sig.get("status") != "active":
                continue
            try:
                await self._check_one(sig)
            except Exception as e:
                print(f"[PositionTracker] {sig.get('symbol')} error: {e}")
            await asyncio.sleep(0.3)

    async def _check_one(self, signal: Dict):
        symbol    = signal.get("symbol", "")
        entry     = _f(signal.get("entry_price", 0))
        sl        = _f(signal.get("stop_loss", 0))
        direction = signal.get("direction", "long")
        opened_at = signal.get("timestamp", "")
        tps_raw   = signal.get("take_profits", [])
        taken     = list(signal.get("taken_tps", []))

        if not symbol or not entry:
            return

        # Экспирация 24ч
        if opened_at:
            try:
                age = datetime.utcnow() - datetime.fromisoformat(opened_at)
                if age > timedelta(hours=24):
                    await self._expire(signal)
                    return
            except Exception:
                pass

        md = await self.binance.get_complete_market_data(symbol)
        if not md:
            return
        price = _f(md.price)

        # Проверяем BE / LOCK / трейлинг
        await self._check_trailing(signal, price)

        # Обновляем SL после возможного перемещения
        sl = _f(signal.get("stop_loss", 0))

        # SL hit
        if sl and _sl_hit(direction, price, sl):
            await self._close_sl(signal, price)
            return

        # TP hit
        for i, tp_raw in enumerate(tps_raw):
            if i in taken:
                continue
            tp_price, tp_weight = _parse_tp(tp_raw)
            if tp_price <= 0:
                continue
            if _tp_hit(direction, price, tp_price):
                is_last = (len(taken) + 1 >= len(tps_raw))
                await self._close_tp(signal, i, tp_price, tp_weight, price, is_last)
                break

    # =========================================================================
    # TRAILING — двухступенчатый BE
    # =========================================================================

    async def _check_trailing(self, signal: Dict, price: float):
        symbol          = signal.get("symbol", "")
        entry           = _f(signal.get("entry_price", 0))
        direction       = signal.get("direction", "long")
        current_sl      = _f(signal.get("stop_loss", 0))
        be_done         = signal.get("be_done", False)    # TP1 → entry
        be2_done        = signal.get("be2_done", False)   # TP2 → entry±0.2%
        trailing_active = signal.get("trailing_active", False)
        taken_tps       = signal.get("taken_tps", [])

        if not entry or not current_sl:
            return

        taken_count = len(taken_tps)

        if direction == "long":
            profit_pct = (price - entry) / entry

            # ── Шаг 1: TP1 → SL на entry (BE) ────────────────────────────────
            if not be_done and taken_count >= self.BREAKEVEN_AFTER_TP:
                new_sl = entry * (1 + self.BREAKEVEN_BUFFER)  # точно entry
                if new_sl > current_sl * 1.0003:
                    await self._move_sl(signal, current_sl, new_sl, f"BE после TP{self.BREAKEVEN_AFTER_TP} → вход")
                    signal["be_done"] = True
                    signal["trailing_active"] = False  # трейл ещё не включаем
                    return
                else:
                    # SL уже выше entry — просто помечаем
                    signal["be_done"] = True
                    self._save(symbol, signal)
                    return

            # ── Шаг 2: TP2 → SL на entry + 0.2% (LOCK) ──────────────────────
            if be_done and not be2_done and taken_count >= self.LOCK_AFTER_TP:
                new_sl = entry * (1 + self.LOCK_BUFFER)
                if new_sl > current_sl * 1.0003:
                    await self._move_sl(signal, current_sl, new_sl, f"LOCK после TP{self.LOCK_AFTER_TP} → +{self.LOCK_BUFFER*100:.1f}%")
                    signal["be2_done"] = True
                    signal["trailing_active"] = True  # теперь включаем трейлинг
                    return
                else:
                    signal["be2_done"] = True
                    signal["trailing_active"] = True
                    self._save(symbol, signal)
                    return

            # ── Трейлинг: только после LOCK (be2_done) ───────────────────────
            if trailing_active and be2_done and profit_pct > 0.015:
                new_sl = price * (1 - self.TRAIL_DISTANCE)
                if new_sl > current_sl * 1.003:
                    await self._move_sl(signal, current_sl, new_sl, "трейлинг")

        else:  # SHORT
            profit_pct = (entry - price) / entry

            # ── Шаг 1: TP1 → SL на entry (BE) ────────────────────────────────
            if not be_done and taken_count >= self.BREAKEVEN_AFTER_TP:
                new_sl = entry * (1 - self.BREAKEVEN_BUFFER)
                if new_sl < current_sl * 0.9997:
                    await self._move_sl(signal, current_sl, new_sl, f"BE после TP{self.BREAKEVEN_AFTER_TP} → вход")
                    signal["be_done"] = True
                    signal["trailing_active"] = False
                    return
                else:
                    signal["be_done"] = True
                    self._save(symbol, signal)
                    return

            # ── Шаг 2: TP2 → SL на entry - 0.2% (LOCK) ──────────────────────
            if be_done and not be2_done and taken_count >= self.LOCK_AFTER_TP:
                new_sl = entry * (1 - self.LOCK_BUFFER)
                if new_sl < current_sl * 0.9997:
                    await self._move_sl(signal, current_sl, new_sl, f"LOCK после TP{self.LOCK_AFTER_TP} → -{self.LOCK_BUFFER*100:.1f}%")
                    signal["be2_done"] = True
                    signal["trailing_active"] = True
                    return
                else:
                    signal["be2_done"] = True
                    signal["trailing_active"] = True
                    self._save(symbol, signal)
                    return

            # ── Трейлинг: только после LOCK ──────────────────────────────────
            if trailing_active and be2_done and profit_pct > 0.015:
                new_sl = price * (1 + self.TRAIL_DISTANCE)
                if new_sl < current_sl * 0.997:
                    await self._move_sl(signal, current_sl, new_sl, "трейлинг")

    async def _move_sl(self, signal: Dict, old_sl: float, new_sl: float, move_type: str):
        symbol    = signal["symbol"]
        direction = signal["direction"]
        entry     = _f(signal["entry_price"])

        signal["stop_loss"] = round(new_sl, 8)
        self._save(symbol, signal)

        d_emoji = "🟢" if direction == "long" else "🔴"
        icon    = "🔒" if "BE" in move_type or "LOCK" in move_type else "🔄"
        sl_pnl  = _pnl(direction, entry, new_sl)
        old_pnl = _pnl(direction, entry, old_sl)
        taken   = len(signal.get("taken_tps", []))

        lines = [
            f"{icon} <b>Стоп передвинут — {move_type.upper()}</b>",
            "",
            f"{d_emoji} <b>#{symbol}</b>  {direction.upper()}",
            f"📍 Вход:      <b>${entry:,.6f}</b>",
            f"🛑 Было SL:   <b>${old_sl:,.6f}</b>  ({old_pnl:+.2f}%)",
            f"✅ Теперь SL: <b>${new_sl:,.6f}</b>  ({sl_pnl:+.2f}%)",
            f"📊 TP взято: {taken}",
            f"🕐 {datetime.utcnow().strftime('%H:%M UTC')}",
        ]
        if "BE" in move_type:
            lines.append(f"\n<i>TP1 взят — стоп на вход. Риск = 0.</i>")
        elif "LOCK" in move_type:
            lines.append(f"\n<i>TP2 взят — стоп зафиксирован с прибылью +{self.LOCK_BUFFER*100:.1f}%.</i>")

        await self._notify(signal, "\n".join(lines))
        print(f"[PositionTracker] SL {move_type}: {symbol} {old_sl:.6f} → {new_sl:.6f}")

    # =========================================================================
    # CLOSE TP
    # =========================================================================

    async def _close_tp(self, signal: Dict, tp_idx: int,
                        tp_price: float, tp_weight: float,
                        current_price: float, is_last: bool):
        direction = signal["direction"]
        entry     = _f(signal["entry_price"])
        symbol    = signal["symbol"]
        total     = len(signal.get("take_profits", []))
        tp_num    = tp_idx + 1
        tp_label  = f"TP{tp_num}"

        pnl_pct  = _pnl(direction, entry, tp_price)
        time_str = _time_in_trade(signal)

        taken = list(signal.get("taken_tps", []))
        taken.append(tp_idx)
        signal["taken_tps"] = taken

        if is_last:
            total_pnl = _calc_weighted_pnl(direction, entry, signal.get("take_profits", []), taken)
            signal["status"]      = "closed_tp"
            signal["close_price"] = current_price
            signal["close_time"]  = datetime.utcnow().isoformat()
            signal["pnl_pct"]     = round(total_pnl, 4)
            signal["tp_level"]    = tp_label

        self._save(symbol, signal)

        d_emoji = "🔴" if direction == "short" else "🟢"
        icon    = "🏆" if is_last else "🎯"

        lines = [
            f"{icon} <b>{tp_label}/{total} взят!</b>",
            "",
            f"{d_emoji} <b>#{symbol}</b>  {direction.upper()}",
            f"📍 Вход:       <b>${entry:,.6f}</b>",
            f"🎯 {tp_label}:      <b>${tp_price:,.6f}</b>  ({tp_weight:.0f}% позиции)",
            f"📊 P&L:        <b>+{pnl_pct:.2f}%</b>",
            f"⏱ В сделке:   {time_str}",
        ]

        if is_last:
            total_pnl_w = signal.get("pnl_pct", pnl_pct)
            lines += ["", "🏆 <b>Все тейки взяты!</b>",
                      f"💰 Итоговый P&L: <b>+{total_pnl_w:.2f}%</b>"]
            await self._record_pnl(signal, total_pnl_w, "tp", tp_label)
        else:
            remaining = total - len(taken)
            lines.append(f"⏳ До следующего TP: {remaining} шт.")
            if tp_num == self.BREAKEVEN_AFTER_TP:
                lines.append(f"\n🔒 <i>SL переносится на вход (BE) — риск = 0</i>")
            elif tp_num == self.LOCK_AFTER_TP:
                lines.append(f"\n🔐 <i>SL переносится на вход +{self.LOCK_BUFFER*100:.1f}% (LOCK)</i>")

        await self._notify(signal, "\n".join(lines))

    # =========================================================================
    # CLOSE SL
    # =========================================================================

    async def _close_sl(self, signal: Dict, current_price: float):
        direction    = signal["direction"]
        entry        = _f(signal["entry_price"])
        sl_price     = _f(signal["stop_loss"])
        symbol       = signal["symbol"]
        was_trailing = signal.get("trailing_active", False)
        be_done      = signal.get("be_done", False)
        be2_done     = signal.get("be2_done", False)
        taken        = list(signal.get("taken_tps", []))
        tps_raw      = signal.get("take_profits", [])

        tp_pnl = _calc_weighted_pnl(direction, entry, tps_raw, taken) if taken else 0.0
        taken_weight = sum(_parse_tp(tps_raw[i])[1] for i in taken if i < len(tps_raw))
        remaining_w  = max(0.0, 100.0 - taken_weight) / 100.0
        raw_sl_pnl   = _pnl(direction, entry, current_price)
        total_pnl    = round(tp_pnl + raw_sl_pnl * remaining_w, 4)

        time_str = _time_in_trade(signal)
        signal["status"]      = "closed_sl"
        signal["close_price"] = current_price
        signal["close_time"]  = datetime.utcnow().isoformat()
        signal["pnl_pct"]     = total_pnl
        signal["tp_level"]    = "SL"
        self._save(symbol, signal)

        d_emoji  = "🔴" if direction == "short" else "🟢"
        if was_trailing and be2_done:
            sl_type = "трейлинг-стоп"
        elif be2_done:
            sl_type = "lock+0.2%"
        elif be_done:
            sl_type = "безубыток"
        else:
            sl_type = "стоп-лосс"

        pnl_sign = "+" if total_pnl >= 0 else ""

        lines = [
            f"🛑 <b>Стоп выбит</b>  ({sl_type})",
            "",
            f"{d_emoji} <b>#{symbol}</b>  {direction.upper()}",
            f"📍 Вход:      <b>${entry:,.6f}</b>",
            f"🛑 Стоп:      <b>${sl_price:,.6f}</b>",
            f"💰 Закрыто:   <b>${current_price:,.6f}</b>",
        ]
        if taken:
            lines.append(f"🎯 TP взято:  {len(taken)} шт.  (вклад {tp_pnl:+.2f}%)")
        lines += [
            f"📊 Итог P&L:  <b>{pnl_sign}{total_pnl:.2f}%</b>",
            f"⏱ В сделке:  {time_str}",
            f"🕐 {datetime.utcnow().strftime('%H:%M UTC')}",
        ]
        if be_done and total_pnl >= -0.1:
            lines.append("\n<i>Закрыто в безубытке или в плюсе. Риск = 0.</i>")

        await self._notify(signal, "\n".join(lines))
        await self._record_pnl(signal, total_pnl, "sl", "SL")

    async def _expire(self, signal: Dict):
        symbol   = signal.get("symbol", "?")
        entry    = signal.get("entry_price", 0)
        time_str = _time_in_trade(signal)
        signal["status"]     = "expired"
        signal["close_time"] = datetime.utcnow().isoformat()
        self._save(symbol, signal)
        d_emoji = "🔴" if signal.get("direction") == "short" else "🟢"
        await self._send(
            f"⏰ <b>Сигнал истёк (24ч)</b>\n"
            f"{d_emoji} <b>#{symbol}</b>\n"
            f"📍 Вход: <b>${entry:,.6f}</b>  |  ⏱ {time_str}"
        )

    # =========================================================================
    # STATS
    # =========================================================================

    async def _record_pnl(self, signal: Dict, pnl_pct: float,
                          close_type: str, tp_level: str = ""):
        try:
            today  = datetime.utcnow().strftime("%Y-%m-%d")
            symbol = signal.get("symbol", "?")

            try:
                state_data = self.redis.get_bot_state(self.bot_type) or {}
                daily = state_data.get("daily_trades", {})
                day   = daily.get(today, {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0})
                day["trades"] += 1
                day["pnl"]     = round(day["pnl"] + pnl_pct, 4)
                if pnl_pct > 0: day["wins"]   += 1
                else:           day["losses"] += 1
                daily[today] = day
                if len(daily) > 30:
                    del daily[sorted(daily.keys())[0]]
                state_data["daily_trades"] = daily
                self.redis.update_bot_state(self.bot_type, state_data)
            except Exception as e:
                print(f"[PT] bot_state stats: {e}")

            try:
                day2 = self.redis.get_daily_stats(self.bot_type, today) or \
                       {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
                day2["trades"] += 1
                day2["pnl"]     = round(day2.get("pnl", 0.0) + pnl_pct, 4)
                if pnl_pct > 0: day2["wins"]   = day2.get("wins", 0) + 1
                else:           day2["losses"] = day2.get("losses", 0) + 1
                self.redis.update_daily_stats(self.bot_type, today, day2)
            except Exception as e:
                print(f"[PT] daily_stats: {e}")

            try:
                opened_at = signal.get("timestamp", "")
                closed_at = signal.get("close_time", datetime.utcnow().isoformat())
                hold_secs = 0
                try:
                    t0 = datetime.fromisoformat(opened_at)
                    t1 = datetime.fromisoformat(closed_at)
                    hold_secs = int((t1 - t0).total_seconds())
                except Exception:
                    pass

                entry   = signal.get("entry_price", 0)
                close_p = signal.get("close_price", 0)
                sl_price = signal.get("stop_loss", 0)
                tps  = signal.get("take_profits", [])
                taken = signal.get("taken_tps", [])

                record = {
                    "symbol":       symbol,
                    "direction":    signal.get("direction", "?"),
                    "entry_price":  entry,
                    "close_price":  close_p,
                    "stop_loss":    sl_price,
                    "pnl":          round(pnl_pct, 4),
                    "tp_level":     tp_level,
                    "close_type":   close_type,
                    "opened_at":    opened_at,
                    "closed_at":    closed_at,
                    "hold_minutes": hold_secs // 60,
                    "score":        signal.get("score", 0),
                    "pattern":      signal.get("pattern", ""),
                    "leverage":     signal.get("leverage", "?"),
                    "risk":         signal.get("risk", "?"),
                    "rsi_1h":       signal.get("rsi_1h", 0),
                    "funding_rate": signal.get("funding_rate", 0),
                    "oi_change":    signal.get("oi_change", 0),
                    "long_short_ratio": signal.get("long_short_ratio", 0),
                    "volume_spike": signal.get("volume_spike_ratio", 0),
                    "atr_pct":      signal.get("atr_14_pct", 0),
                    "smc_ob":       signal.get("smc_data", {}).get("has_ob", False),
                    "smc_fvg":      signal.get("smc_data", {}).get("has_fvg", False),
                    "smc_bonus":    signal.get("smc_data", {}).get("score_bonus", 0),
                    "tp_count":     len(tps),
                    "tp_taken":     len(taken),
                    "tp_prices":    [t[0] if isinstance(t, (list, tuple)) else t.get("price", 0) for t in tps[:6]],
                    "reasons":      signal.get("reasons", [])[:8],
                    "realtime_factors": signal.get("realtime_factors", [])[:5],
                    # v3.0: добавляем флаги BE
                    "be_done":      signal.get("be_done", False),
                    "be2_done":     signal.get("be2_done", False),
                }
                hkey = f"{self.bot_type}:history:{symbol}"
                self.redis.client.lpush(hkey, json.dumps(record))
                self.redis.client.ltrim(hkey, 0, 199)
                self.redis.client.expire(hkey, 2592000)
                all_key = f"{self.bot_type}:all_trades"
                self.redis.client.lpush(all_key, json.dumps(record))
                self.redis.client.ltrim(all_key, 0, 9999)
                self.redis.client.expire(all_key, 7776000)
            except Exception as e:
                print(f"[PT] history: {e}")

            if self.auto_trader:
                self.auto_trader.record_trade_result(pnl_pct)

        except Exception as e:
            print(f"[PT] _record_pnl: {e}")

    # =========================================================================
    # HELPERS
    # =========================================================================

    async def _notify(self, signal: Dict, text: str):
        tg_msg_id = signal.get("tg_msg_id")
        if tg_msg_id:
            try:
                await self.tg.send_reply(text, reply_to_message_id=tg_msg_id)
                return
            except Exception as e:
                print(f"[PT] send_reply failed: {e}")
        await self._send(text)

    def _save(self, symbol: str, signal: Dict):
        try:
            self.redis.save_signal(self.bot_type, symbol, signal)
        except Exception as e:
            print(f"[PT] redis save: {e}")

    async def _send(self, text: str):
        try:
            await self.tg.send_message(text)
        except Exception as e:
            print(f"[PT] telegram: {e}")


# ============================================================================
# PURE HELPERS
# ============================================================================

def _f(v) -> float:
    try:   return float(v)
    except: return 0.0

def _sl_hit(direction: str, price: float, sl: float) -> bool:
    return price >= sl if direction == "short" else price <= sl

def _tp_hit(direction: str, price: float, tp: float) -> bool:
    return price <= tp if direction == "short" else price >= tp

def _pnl(direction: str, entry: float, close: float) -> float:
    if entry == 0: return 0.0
    return (entry - close) / entry * 100 if direction == "short" else (close - entry) / entry * 100

def _parse_tp(raw) -> Tuple[float, float]:
    try:
        if isinstance(raw, (list, tuple)):
            return _f(raw[0]), _f(raw[1]) if len(raw) > 1 else 20.0
        if isinstance(raw, dict):
            return _f(raw.get("price", 0)), _f(raw.get("weight", 20))
    except Exception:
        pass
    return 0.0, 0.0

def _calc_weighted_pnl(direction: str, entry: float, tps_raw: list, taken: list) -> float:
    total = 0.0
    for i in taken:
        if i < len(tps_raw):
            tp_price, tp_weight = _parse_tp(tps_raw[i])
            if tp_price > 0:
                total += _pnl(direction, entry, tp_price) * tp_weight / 100
    return round(total, 4)

def _time_in_trade(signal: Dict) -> str:
    try:
        opened = datetime.fromisoformat(signal["timestamp"])
        delta  = datetime.utcnow() - opened
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m = rem // 60
        return f"{h}ч {m}м" if h else f"{m}м"
    except Exception:
        return "N/A"
