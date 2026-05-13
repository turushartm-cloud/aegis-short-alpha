from typing import Optional
"""
Position Tracker v2.9 — Phase 2: Micro-Step Trailing Stop

ИЗМЕНЕНИЯ v2.9:
  🎢 Micro-Step Trailing Stop — плавное движение SL микро-шагами
     TP1 → +0.3%, TP2 → +0.8%, TP3 → +1.5% (вместо агрессивного трейлинга)
     Решает проблему выбивания при ретестах (как PUMPBTCUSDT)

ИЗМЕНЕНИЯ v2.7:
  ✅ Стоп в безубыток ПОСЛЕ TP2 (было: при +1.5% прибыли)
     Логика: после закрытия 2-го тейка (40% позиции зафиксировано)
     SL переносится на entry+0.1%. Это лучше чем немедленный BE,
     потому что даёт позиции "дышать" и не срабатывает преждевременно.
     Математика: TP2 = +3%, 40% зафиксировано → даже если SL hit → 0% потерь.
  ✅ _notify: правильный порядок аргументов send_reply
  ✅ _record_pnl: пишет в stats:daily:{date} для /stats команды
  ✅ tp_level в историю сделок для /leaderswr и отчётов
"""

import asyncio
import json
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# 🎢 Phase 2: Micro-Step Trailing Stop
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from execution.micro_trailing_stop import get_micro_trailing


class PositionTracker:
    """
    Каждые CHECK_INTERVAL секунд:
      1. Берёт active сигналы из Redis
      2. Получает текущую цену через Binance
      3. Проверяет TP / SL / трейлинг
      4. Перемещает SL в безубыток ПОСЛЕ TP2
      5. Thread reply через tg_msg_id
      6. Записывает P&L в stats:daily:{date}
    """

    CHECK_INTERVAL = 60  # ✅ OPT v19: 30→60s (BingX WS handles instant SL/TP)

    # ── Трейлинг (активируется ПОСЛЕ BE, не сразу) ───────────────────────────
    TRAIL_DISTANCE  = 0.008   # 0.8% ниже текущей цены (для LONG)

    # ── Двухступенчатый безубыток ─────────────────────────────────────────────
    # Шаг 1: После TP1 → SL в точку входа (breakeven = 0%)
    # Шаг 2: После TP2 → SL в entry + 0.2% (гарантированная небольшая прибыль)
    BREAKEVEN_AFTER_TP   = 1      # ✅ FIX: After which TP to move SL to BE (was missing → AttributeError)
    BREAKEVEN_BUFFER_TP1 = 0.000  # SL = entry (ровно точка входа)
    BREAKEVEN_BUFFER_TP2 = 0.002  # SL = entry + 0.2% после TP2

    def __init__(self, *, bot_type, telegram, redis_client,
                 binance_client, config, auto_trader=None):
        self.bot_type    = bot_type
        self.tg          = telegram
        self.redis       = redis_client
        self.binance     = binance_client
        self.config      = config
        self.auto_trader = auto_trader
        
        # Trail activation thresholds из config (env vars)
        # LONG: 2.5% по умолчанию, SHORT: 3% по умолчанию
        self.long_trail_threshold  = getattr(config, 'LONG_TRAIL_ACTIVATION', 0.025)
        self.short_trail_threshold = getattr(config, 'SHORT_TRAIL_ACTIVATION', 0.030)
        
        # Конвертируем строку в float если нужно
        if isinstance(self.long_trail_threshold, str):
            self.long_trail_threshold = float(self.long_trail_threshold)
        if isinstance(self.short_trail_threshold, str):
            self.short_trail_threshold = float(self.short_trail_threshold)
        
        # 🎢 Phase 2: Micro-Step Trailing Stop
        self.micro_trailing = get_micro_trailing()
        
        self._running    = False
        self._scan_lock  = None   # asyncio.Lock — set on first use (event loop needed)
        self._bingx_ws: Optional[object] = None  # BingXWSTracker для мгновенного SL/TP

    async def run(self):
        self._running = True
        print(f"📍 PositionTracker started (interval={self.CHECK_INTERVAL}s)")
        # ✅ v18: BingX WS tracker for instant SL/TP detection
        import asyncio as _aio
        _aio.create_task(self._start_bingx_ws())
        while self._running:
            try:
                await self._scan_all()
            except Exception as e:
                print(f"[PositionTracker] loop error: {e}")
            await asyncio.sleep(self.CHECK_INTERVAL)

    def stop(self):
        self._running = False

    def _log(self, symbol: str, direction: str, message: str):
        """Логирование для Render."""
        d_str = "LONG" if direction == "long" else "SHORT"
        print(f"[PT][{d_str}][{symbol}] {message}")

    async def _start_bingx_ws(self):
        """Запускает BingX WS tracker — мгновенное обнаружение SL/TP без 30s задержки."""
        try:
            import os
            from utils.bingx_ws_tracker import BingXWSTracker
            api_key    = os.getenv("BINGX_API_KEY", "")
            api_secret = os.getenv("BINGX_API_SECRET", "")
            if not api_key:
                print("[BingX WS] No API key — WS disabled, polling only")
                return

            async def _on_sl(symbol, price):
                print(f"[BingX WS] 🛑 INSTANT SL: {symbol} @ {price}")
                for bt in ("short", "long"):
                    for sig in self.redis.get_active_signals(bt):
                        if sig.get("symbol","").upper() == symbol.upper():
                            sig.update({"status":"sl_hit_ws","close_price":price})
                            self.redis.save_position(bt, symbol, sig)

            async def _on_tp(symbol, tp_num, price):
                print(f"[BingX WS] ✅ INSTANT TP{tp_num}: {symbol} @ {price}")
                for bt in ("short", "long"):
                    for sig in self.redis.get_active_signals(bt):
                        if sig.get("symbol","").upper() == symbol.upper():
                            taken = sig.get("taken_tps", [])
                            if tp_num not in taken:
                                taken.append(tp_num)
                                sig["taken_tps"] = taken
                                self.redis.save_position(bt, symbol, sig)

            self._bingx_ws = BingXWSTracker(api_key=api_key, api_secret=api_secret,
                                             on_sl_hit=_on_sl, on_tp_hit=_on_tp)
            await self._bingx_ws.start()
            print("[BingX WS] ✅ Instant position tracker active")
        except ImportError:
            print("[BingX WS] websockets not installed — pip install websockets")
        except Exception as e:
            print(f"[BingX WS] startup error: {e}")

    async def _scan_all(self):
        # ✅ FIX v3.0: guard covers ENTIRE processing, not just signal fetch
        if getattr(self, '_scan_running', False):
            return
        self._scan_running = True
        try:
            signals = self.redis.get_active_signals(self.bot_type)
        except Exception as e:
            print(f"[PositionTracker] redis error: {e}")
            self._scan_running = False
            return

        if not signals:
            self._scan_running = False
            return

        # ✅ v4.0: Zombie cleanup — раз в 10 итераций чистим «мёртвые» Redis позиции
        if not hasattr(self, '_scan_count'):
            self._scan_count = 0
        self._scan_count += 1
        if self._scan_count % 10 == 0:
            await self._cleanup_zombie_positions(signals)

        for sig in signals:
            if sig.get("status") != "active":
                continue
            try:
                await self._check_one(sig)
            except Exception as e:
                print(f"[PositionTracker] {sig.get('symbol')} error: {e}")
            await asyncio.sleep(0.3)

        # ✅ VIRTUAL: Отслеживаем TG-only сигналы для статистики
        await self._scan_virtual()
        self._scan_running = False

    async def _scan_virtual(self):
        """
        Отслеживает виртуальные позиции (TG-only, не открытые на бирже).
        Полный мониторинг TP/SL + статистика, без BingX API.
        """
        try:
            virtual_positions = self.redis.get_virtual_positions(self.bot_type)
        except Exception as e:
            print(f"[VirtualTracker] redis error: {e}")
            return

        if not virtual_positions:
            return

        for field, pos in virtual_positions.items():
            if pos.get("outcome") is not None:
                continue  # уже закрыта
            try:
                await self._check_one_virtual(field, pos)
            except Exception as e:
                print(f"[VirtualTracker] {pos.get('symbol', '?')} error: {e}")
            await asyncio.sleep(0.2)

    async def _check_one_virtual(self, field: str, signal: Dict):
        """Проверяет виртуальную позицию по текущей цене. Без BingX API."""
        import json as _json

        symbol    = signal.get("symbol", "")
        entry     = _f(signal.get("entry_price", 0))
        sl        = _f(signal.get("stop_loss", 0))
        direction = signal.get("direction", "long")
        tps_raw   = signal.get("take_profits", [])
        taken     = list(signal.get("taken_tps", []))
        bot_type  = signal.get("bot_type", self.bot_type)

        if not symbol or not entry:
            return

        # Экспирация 48ч
        opened_at = signal.get("virtual_opened_at", signal.get("timestamp", ""))
        if opened_at:
            try:
                age = datetime.utcnow() - datetime.fromisoformat(opened_at)
                if age > timedelta(hours=48):
                    self.redis.close_virtual_position(bot_type, field, "expired", entry, 0.0)
                    print(f"[VT] {symbol}: истёк срок 48ч, закрываем")
                    return
            except Exception:
                pass

        md = await self.binance.get_complete_market_data(symbol)
        if not md:
            return
        price = _f(md.price)

        d_str   = "LONG" if direction == "long" else "SHORT"
        d_emoji = "🟢" if direction == "long" else "🔴"
        total   = len(tps_raw)

        print(f"[VT][{d_str}][{symbol}] цена={price:.6f} вход={entry:.6f} "
              f"SL={sl:.6f} TP={len(taken)}/{total} [ВИРТУАЛ]")

        # ── SL hit ──────────────────────────────────────────────────────────────
        if sl and _sl_hit(direction, price, sl):
            sl_loss     = _pnl(direction, entry, sl)
            taken_pnl   = 0.0
            rem_weight  = 1.0
            for idx in taken:
                if idx < len(tps_raw):
                    tp_p, tp_w = _parse_tp(tps_raw[idx])
                    w = tp_w / 100
                    taken_pnl  += _pnl(direction, entry, tp_p) * w
                    rem_weight -= w
            total_pnl = taken_pnl + sl_loss * max(0, rem_weight)

            signal["taken_tps"]  = taken
            signal["close_price"] = price
            signal["close_time"]  = datetime.utcnow().isoformat()
            signal["pnl_pct"]     = round(total_pnl, 4)

            await self._record_pnl(signal, total_pnl, "sl", "SL")
            self.redis.close_virtual_position(bot_type, field, "sl", price, total_pnl)

            taken_str = f" (TP взято: {len(taken)})" if taken else ""
            await self._notify(signal, (
                f"🛑 <b>[ВИРТУАЛ] SL сработал{taken_str}</b>\n\n"
                f"{d_emoji} <b>#{symbol}</b>  {direction.upper()}\n"
                f"📍 Вход:   <b>${entry:,.6f}</b>\n"
                f"🛑 SL:     <b>${sl:,.6f}</b>\n"
                f"📊 P&L:    <b>{total_pnl:+.2f}%</b>\n"
                f"<i>📋 Виртуал — не открыта на бирже</i>"
            ))
            return

        # ── TP hit ──────────────────────────────────────────────────────────────
        for i, tp_raw in enumerate(tps_raw):
            if i in taken:
                continue
            tp_price, tp_weight = _parse_tp(tp_raw)
            if tp_price <= 0:
                continue
            if _tp_hit(direction, price, tp_price):
                pnl_pct = _pnl(direction, entry, tp_price)
                taken.append(i)
                signal["taken_tps"] = taken
                is_last   = (len(taken) >= len(tps_raw))
                tp_label  = f"TP{i+1}"

                if is_last:
                    total_pnl = _calc_weighted_pnl(direction, entry, tps_raw, taken)
                    signal["close_price"] = tp_price
                    signal["close_time"]  = datetime.utcnow().isoformat()
                    signal["pnl_pct"]     = round(total_pnl, 4)

                    await self._record_pnl(signal, total_pnl, "tp", tp_label)
                    self.redis.close_virtual_position(bot_type, field, "tp", tp_price, total_pnl)

                    await self._notify(signal, (
                        f"🏆 <b>[ВИРТУАЛ] Все TP взяты!</b>\n\n"
                        f"{d_emoji} <b>#{symbol}</b>  {direction.upper()}\n"
                        f"📍 Вход:         <b>${entry:,.6f}</b>\n"
                        f"🎯 {tp_label}:        <b>${tp_price:,.6f}</b>\n"
                        f"💰 Итоговый P&L: <b>+{total_pnl:.2f}%</b>\n"
                        f"<i>📋 Виртуал — не открыта на бирже</i>"
                    ))
                else:
                    # ✅ BE Stop для виртуальных позиций
                    # После TP1 → SL в точку входа (breakeven)
                    # После TP2 → SL в entry + 0.2% (small profit locked)
                    old_sl = signal.get("stop_loss", sl)
                    new_sl_virtual = None
                    be_msg = ""

                    if i == 0:  # TP1 взят → BE
                        if direction == "long":
                            new_sl_virtual = entry  # SL = точка входа
                        else:
                            new_sl_virtual = entry  # SL = точка входа
                        be_msg = f"🔒 SL → ТВХ (BE)"

                    elif i == 1:  # TP2 взят → BE + 0.2%
                        if direction == "long":
                            new_sl_virtual = entry * 1.002
                        else:
                            new_sl_virtual = entry * 0.998
                        be_msg = f"🔒 SL → ТВХ+0.2% (гарантия прибыли)"

                    if new_sl_virtual:
                        signal["stop_loss"] = new_sl_virtual
                        sl = new_sl_virtual  # обновляем локальную переменную
                        print(f"[VT] {symbol}: {be_msg} | SL: {old_sl:.6f} → {new_sl_virtual:.6f}")

                    # Обновляем taken_tps + stop_loss в Redis hash
                    try:
                        vkey = f"{bot_type}:virtual_positions"
                        self.redis.client.hset(vkey, field, _json.dumps(signal))
                    except Exception as e:
                        print(f"[VT] update taken_tps error: {e}")

                    remaining = total - len(taken)
                    be_line = f"\n{be_msg}" if be_msg else ""
                    await self._notify(signal, (
                        f"🎯 <b>[ВИРТУАЛ] {tp_label}/{total} взят!</b>\n\n"
                        f"{d_emoji} <b>#{symbol}</b>  {direction.upper()}\n"
                        f"📍 Вход:        <b>${entry:,.6f}</b>\n"
                        f"🎯 {tp_label}:  <b>${tp_price:,.6f}</b>  ({tp_weight:.0f}% позиции)\n"
                        f"📊 P&L:         <b>+{pnl_pct:.2f}%</b>\n"
                        f"⏳ Осталось TP: {remaining}"
                        + be_line +
                        f"\n<i>📋 Виртуал — не открыта на бирже</i>"
                    ))
                break

    async def _cleanup_zombie_positions(self, signals: list):
        """
        ✅ v4.0: Удаляет из Redis позиции которых нет на бирже.
        Zombie = сигнал в Redis со status=active, но BingX не знает о позиции.
        Без этого — SL никогда не срабатывает → бесконечный убыток.
        """
        if not self.auto_trader or not hasattr(self.auto_trader, 'bingx'):
            return
        
        bingx = self.auto_trader.bingx
        if not bingx:
            return

        for sig in signals:
            symbol = sig.get('symbol', '')
            direction = sig.get('direction', 'long')
            if not symbol:
                continue
            try:
                # 🆕 NEW: Проверяем только подтвержденные позиции (confirmed=True)
                confirmed = sig.get('confirmed', False)
                if not confirmed:
                    # Позиция не подтверждена — проверяем возраст.
                    # Если > 30 мин → мёртвая запись, удаляем из Redis.
                    _ts = sig.get('timestamp', '')
                    _age_min = 9999
                    if _ts:
                        try:
                            _dt = datetime.fromisoformat(_ts.replace('Z', '+00:00'))
                            _age_min = (datetime.utcnow() - _dt.replace(tzinfo=None)).total_seconds() / 60
                        except Exception:
                            pass
                    if _age_min > 30:
                        print(f"[ZOMBIE-AUTO-CLEAN] {symbol}: confirmed=False, возраст={_age_min:.0f}м > 30м → удаляем")
                        self.redis.remove_position(self.bot_type, symbol)
                        self.micro_trailing.remove(symbol)
                    else:
                        print(f"[ZOMBIE-SKIP] {symbol}: confirmed=False, возраст={_age_min:.0f}м < 30м — ждём подтверждения")
                    continue

                pos_side = 'LONG' if direction == 'long' else 'SHORT'
                positions = await bingx.get_positions(symbol)
                has_real_position = any(
                    abs(p.size) > 0 and p.position_side == pos_side
                    for p in positions
                )
                if not has_real_position:
                    # Позиция есть в Redis, нет на бирже — это zombie
                    entry = sig.get('entry_price', 0)
                    redis_price = sig.get('last_price', entry)
                    opened_at = sig.get('timestamp', '')
                    
                    # 🆕 NEW: Получаем АКТУАЛЬНУЮ рыночную цену для точного P&L
                    market_price = None
                    price_source = "redis"
                    try:
                        # Пробуем получить текущую цену с биржи
                        ticker = await bingx.get_ticker(symbol)
                        if ticker and 'lastPrice' in ticker:
                            market_price = float(ticker['lastPrice'])
                            price_source = "market"
                        elif ticker and 'price' in ticker:
                            market_price = float(ticker['price'])
                            price_source = "market"
                    except Exception as e:
                        print(f"⚠️ [ZOMBIE] Could not fetch market price for {symbol}: {e}")
                    
                    # Используем лучшую доступную цену
                    current_price = market_price if market_price else redis_price
                    
                    # Вычисляем P&L на основе актуальной цены
                    if entry and entry > 0:
                        # ✅ FIX v5: _pnl уже определена в этом модуле — убрал circular import
                        pnl = _pnl(direction, float(entry), float(current_price))
                    else:
                        pnl = 0.0
                    
                    # Расчёт времени удержания
                    duration_str = ""
                    if opened_at:
                        try:
                            opened_dt = datetime.fromisoformat(opened_at.replace('Z', '+00:00'))
                            duration = datetime.utcnow() - opened_dt.replace(tzinfo=None)
                            hours = int(duration.total_seconds() / 3600)
                            mins = int((duration.total_seconds() % 3600) / 60)
                            duration_str = f"{hours}ч {mins}м" if hours > 0 else f"{mins}м"
                        except:
                            pass
                    
                    # Разница между ценой Redis и рынком
                    price_diff = ""
                    if market_price and redis_price and redis_price > 0:
                        diff_pct = ((market_price - redis_price) / redis_price) * 100
                        if abs(diff_pct) > 0.1:  # Показываем если разница > 0.1%
                            price_diff = f"📊 Цена Redis→Рынок: {diff_pct:+.2f}%\n"
                    
                    # 🆕 Улучшенное сохранение с деталями
                    sig['status'] = 'closed_zombie'
                    sig['close_price'] = current_price
                    sig['close_time'] = datetime.utcnow().isoformat()
                    sig['pnl_pct'] = round(pnl, 4)
                    sig['tp_level'] = 'ZOMBIE'
                    sig['price_source'] = price_source  # market или redis
                    if market_price:
                        sig['market_price_at_close'] = market_price
                        sig['redis_price'] = redis_price
                    if duration_str:
                        sig['holding_duration'] = duration_str
                    self._save(symbol, sig)
                    self.micro_trailing.remove(symbol)
                    
                    # 🆕 Улучшенное логирование с деталями
                    price_info = f"(рынок: {market_price:,.6f})" if market_price else f"(Redis: {redis_price:,.6f})"
                    print(f"🧟 [ZOMBIE-CLEANUP] {symbol} {direction}: закрываем. "
                          f"Вход: {entry:,.6f}, Выход: {current_price:,.6f} {price_info}, "
                          f"P&L: {pnl:+.2f}%, Держали: {duration_str or 'N/A'}")
                    
                    # 🆕 Улучшенное уведомление с подробностями
                    d_emoji = '🟢' if direction == 'long' else '🔴'
                    price_type = "📈 Рыночная" if market_price else "📋 Из Redis"
                    await self._notify(sig, (
                        f"🧟 <b>Zombie позиция закрыта</b>\n\n"
                        f"{d_emoji} <b>#{symbol}</b> {direction.upper()}\n"
                        f"📍 Вход: <b>${float(entry):,.6f}</b>\n"
                        f"📍 Выход: <b>${float(current_price):,.6f}</b> ({price_type})\n"
                        f"📊 P&L: <b>{pnl:+.2f}%</b>\n"
                        f"⏱ Держали: <b>{duration_str or 'N/A'}</b>\n"
                        f"{price_diff}"
                        f"<i>⚠️ Позиция не найдена на бирже</i>"
                    ))
            except Exception as e:
                print(f"⚠️ [ZOMBIE-CLEANUP] {symbol}: {e}")

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

        # 🎢 Phase 2: Инициализация Micro-Step Trailing при первом обнаружении позиции
        trailing_state = self.micro_trailing.get_state(symbol)
        if trailing_state is None and len(taken) == 0:
            # Новая позиция — инициализируем трейлинг
            self.micro_trailing.initialize(
                symbol=symbol,
                direction=direction,
                entry_price=entry,
                initial_sl=sl
            )
            print(f"🎢 [MicroTrail][{symbol}] Initialized: entry={entry:.6f}, SL={sl:.6f}")

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

        # 🆕 Обновляем unrealized P&L в Redis position для дашборда
        try:
            unrealized_pnl = _pnl(direction, entry, price)
            self._update_position_pnl(symbol, price, unrealized_pnl)
        except Exception as e:
            print(f"[PT][{symbol}] unrealized_pnl update error: {e}")

        # ── ДЕТАЛЬНЫЙ ЛОГ RENDER ─────────────────────────────────────────────
        be_done      = signal.get("be_done", False)
        trail_active = signal.get("trailing_active", False)
        max_tp_log   = signal.get("max_tp_reached", "")
        mode_str     = "TRAIL" if trail_active else ("BE" if be_done else "ACTIVE")
        d_str        = "LONG" if direction == "long" else "SHORT"
        sl_pct_now   = abs((price - sl) / entry * 100) if entry else 0
        print(f"[PT][{d_str}][{symbol}] 🔍 цена={price:.6f} вход={entry:.6f} "
              f"SL={sl:.6f}({sl_pct_now:.2f}%) режим={mode_str} "
              f"TP={len(taken)}/{len(tps_raw)}"
              + (f" макс={max_tp_log}" if max_tp_log else ""))
        # ─────────────────────────────────────────────────────────────────────

        # Трейлинг (только если уже в безубытке)
        await self._check_trailing(signal, price)

        # Обновляем SL после трейлинга
        sl = _f(signal.get("stop_loss", 0))

        # SL hit
        if sl and _sl_hit(direction, price, sl):
            print(f"[PT][{d_str}][{symbol}] 🛑 SL HIT! "
                  f"цена={price:.6f} sl={sl:.6f} "
                  f"TP взято={len(taken)} макс={max_tp_log or '—'}")
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
                pnl_now = _pnl(direction, entry, tp_price)
                print(f"[PT][{d_str}][{symbol}] 🎯 TP{i+1} HIT! "
                      f"цена={price:.6f} tp={tp_price:.6f} "
                      f"P&L={pnl_now:+.2f}% вес={tp_weight:.0f}% is_last={is_last}")
                await self._close_tp(signal, i, tp_price, tp_weight, price, is_last)
                break

    # =========================================================================
    # =========================================================================
    # BE + MICROTRAIL — MicroTrail является ЕДИНСТВЕННЫМ механизмом трейлинга
    # Price-based trailing ОТКЛЮЧЁН — конфликтовал с MicroTrail
    # =========================================================================

    async def _check_trailing(self, signal: Dict, price: float):
        symbol          = signal.get("symbol", "")
        entry           = _f(signal.get("entry_price", 0))
        direction       = signal.get("direction", "long")
        current_sl      = _f(signal.get("stop_loss", 0))
        be_done         = signal.get("be_done", False)
        be2_done        = signal.get("be2_done", False)
        taken_tps       = signal.get("taken_tps", [])
        taken_count     = len(taken_tps)

        if not entry or not current_sl:
            return

        if direction == "long":
            profit_pct = (price - entry) / entry

            # ── Шаг 1: TP1 взят → SL в точку входа (breakeven) ─────────────
            if not be_done and taken_count >= 1:
                new_sl = entry * (1 + self.BREAKEVEN_BUFFER_TP1)  # = entry
                if new_sl > current_sl * 1.0001:
                    print(f"[PT][LONG][{symbol}] 🔒 BE-TP1 → entry | "
                          f"SL: {current_sl:.6f} → {new_sl:.6f} | profit={profit_pct*100:+.2f}%")
                    await self._move_sl(signal, current_sl, new_sl, "BE после TP1")
                    signal["be_done"]         = True
                    signal["trailing_active"] = True
                    return
                else:
                    signal["be_done"] = True
                    signal["trailing_active"] = True
                    self._save(symbol, signal)
                    print(f"[PT][LONG][{symbol}] 🔒 BE-TP1 помечен (SL уже на уровне entry)")

            # ── Шаг 2: TP2 взят → SL в entry + 0.2% ────────────────────────
            if be_done and not be2_done and taken_count >= 2:
                new_sl = entry * (1 + self.BREAKEVEN_BUFFER_TP2)
                if new_sl > current_sl * 1.0001:
                    print(f"[PT][LONG][{symbol}] 🔒 BE2-TP2 → entry+0.2% | "
                          f"SL: {current_sl:.6f} → {new_sl:.6f} | profit={profit_pct*100:+.2f}%")
                    await self._move_sl(signal, current_sl, new_sl, "BE+0.2% после TP2")
                    signal["be2_done"] = True
                    return
                else:
                    signal["be2_done"] = True
                    self._save(symbol, signal)
                    print(f"[PT][LONG][{symbol}] 🔒 BE2-TP2 помечен (SL уже выше entry+0.2%)")

            # ── Price-based trailing ОТКЛЮЧЁН ────────────────────────────────
            # MicroTrail (on_tp_taken) — единственный механизм движения SL после TP
            # Логируем текущее состояние для мониторинга
            mode = "BE✅" if be_done else "ACTIVE"
            micro = self.micro_trailing.get_summary(symbol)
            micro_str = f"MicroStep#{micro['steps_taken']}" if micro else "MicroTrail-"
            print(f"[PT][LONG][{symbol}] 📊 {mode} | {micro_str} | "
                  f"SL={current_sl:.6f} | цена={price:.6f} | profit={profit_pct*100:+.2f}%")

        else:  # SHORT
            profit_pct = (entry - price) / entry

            # ── Шаг 1: TP1 взят → SL в точку входа (breakeven) ─────────────
            if not be_done and taken_count >= 1:
                new_sl = entry * (1 - self.BREAKEVEN_BUFFER_TP1)  # = entry
                if new_sl < current_sl * 0.9999:
                    print(f"[PT][SHORT][{symbol}] 🔒 BE-TP1 → entry | "
                          f"SL: {current_sl:.6f} → {new_sl:.6f} | profit={profit_pct*100:+.2f}%")
                    await self._move_sl(signal, current_sl, new_sl, "BE после TP1")
                    signal["be_done"]         = True
                    signal["trailing_active"] = True
                    return
                else:
                    signal["be_done"] = True
                    signal["trailing_active"] = True
                    self._save(symbol, signal)
                    print(f"[PT][SHORT][{symbol}] 🔒 BE-TP1 помечен (SL уже на уровне entry)")

            # ── Шаг 2: TP2 взят → SL в entry - 0.2% ────────────────────────
            if be_done and not be2_done and taken_count >= 2:
                new_sl = entry * (1 - self.BREAKEVEN_BUFFER_TP2)
                if new_sl < current_sl * 0.9999:
                    print(f"[PT][SHORT][{symbol}] 🔒 BE2-TP2 → entry-0.2% | "
                          f"SL: {current_sl:.6f} → {new_sl:.6f} | profit={profit_pct*100:+.2f}%")
                    await self._move_sl(signal, current_sl, new_sl, "BE-0.2% после TP2")
                    signal["be2_done"] = True
                    return
                else:
                    signal["be2_done"] = True
                    self._save(symbol, signal)
                    print(f"[PT][SHORT][{symbol}] 🔒 BE2-TP2 помечен (SL уже ниже entry-0.2%)")

            # ── Price-based trailing ОТКЛЮЧЁН ────────────────────────────────
            # MicroTrail (on_tp_taken) — единственный механизм движения SL после TP
            mode = "BE✅" if be_done else "ACTIVE"
            micro = self.micro_trailing.get_summary(symbol)
            micro_str = f"MicroStep#{micro['steps_taken']}" if micro else "MicroTrail-"
            print(f"[PT][SHORT][{symbol}] 📊 {mode} | {micro_str} | "
                  f"SL={current_sl:.6f} | цена={price:.6f} | profit={profit_pct*100:+.2f}%")

    async def _move_sl(self, signal: Dict, old_sl: float, new_sl: float, move_type: str):
        """
        ✅ v2.5 FIX КРИТИЧЕСКИЙ:
        Было: только Redis + Telegram — биржа не знала о новом SL!
        Стало: 1) Обновляем SL на BingX (cancel old → place new STOP_MARKET)
               2) Обновляем Redis
               3) Уведомляем в Telegram
        """
        symbol    = signal["symbol"]
        direction = signal["direction"]
        entry     = _f(signal["entry_price"])
        position_side = "LONG" if direction == "long" else "SHORT"

        # ✅ ШАГ 0: Проверяем что позиция ещё существует на бирже
        position_exists = False
        if self.auto_trader and self.auto_trader.bingx:
            try:
                all_positions = await self.auto_trader.bingx.get_positions()
                clean_symbol = symbol.replace("-", "").replace("_", "").upper()
                for pos in all_positions:
                    pos_clean = pos.symbol.replace("-", "").replace("_", "").upper()
                    if pos_clean == clean_symbol:
                        position_exists = True
                        break
                if not position_exists:
                    # ✅ FIX Bug2: Логируем что вернул BingX для дебага
                    returned_syms = [p.symbol for p in all_positions[:10]]
                    print(f"⚠️  [PT] _move_sl: позиция {symbol} не найдена на бирже. "
                          f"BingX вернул {len(all_positions)} позиций: {returned_syms}")
                    # ✅ FIX: НЕ удаляем Redis — позиция могла быть временно не видна (DEMO lag)
                    # Продолжаем обновление Redis и Telegram (только биржу пропускаем)
                    print(f"⚠️  [PT] _move_sl: Обновляем только Redis + Telegram для {symbol}")
            except Exception as e:
                print(f"⚠️  [PT] _move_sl: ошибка проверки позиции {symbol}: {e}")

        # ✅ ШАГ 1: Обновляем SL на бирже (если auto_trader доступен)
        exchange_updated = False
        if self.auto_trader and self.auto_trader.bingx:
            try:
                bingx_symbol = symbol + "-USDT" if "-USDT" not in symbol else symbol
                print(f"🔍 [PT] _move_sl: symbol={symbol}, bingx_symbol={bingx_symbol}, position_side={position_side}")
                # ✅ RETRY: 3 попытки с паузой 1 секунда (v2.7)
                for attempt in range(3):
                    print(f"🔍 [PT] Attempt {attempt + 1}/3")
                    for sym_fmt in [bingx_symbol, symbol.replace("USDT", "-USDT"), symbol]:
                        print(f"🔍 [PT] Trying sym_fmt={sym_fmt}")
                        ok = await self.auto_trader.bingx.update_stop_loss(
                            sym_fmt, position_side, new_sl, direction
                        )
                        print(f"🔍 [PT] update_stop_loss returned: {ok}")
                        if ok:
                            exchange_updated = True
                            print(f"✅ [PT] SL updated successfully with sym_fmt={sym_fmt}")
                            break
                    if exchange_updated:
                        break
                    if attempt < 2:  # Пауза между попытками (не после последней)
                        print(f"🔍 [PT] Waiting 1s before next attempt...")
                        await asyncio.sleep(1)
                if not exchange_updated:
                    print(f"⚠️  [PT] SL на бирже не обновлён для {symbol} после 3 попыток — только Redis")
            except Exception as e:
                print(f"⚠️  [PT] update_stop_loss error {symbol}: {e}")
                import traceback
                traceback.print_exc()

        # ✅ ШАГ 2: Обновляем Redis (всегда)
        signal["stop_loss"] = round(new_sl, 8)
        self._save(symbol, signal)

        d_emoji = "🟢" if direction == "long" else "🔴"
        icon    = "🔒" if move_type == "безубыток" else "🔄"
        # ✅ FIX v5: Запись SL cooldown при срабатывании стопа
        try:
            sl_cd_hours = float(os.getenv("SL_COOLDOWN_HOURS", "2.0"))
            sl_cd_key = f"sl_cooldown:{self.bot_type}:{symbol}"
            self.redis.set(sl_cd_key, "1", ex=int(sl_cd_hours * 3600))
        except Exception:
            pass
        sl_pnl  = _pnl(direction, entry, new_sl)
        old_pnl = _pnl(direction, entry, old_sl)
        taken   = len(signal.get("taken_tps", []))
        ex_icon = "✅ Биржа" if exchange_updated else "⚠️ Только Redis"

        lines = [
            f"{icon} <b>Стоп передвинут — {move_type.upper()}</b>",
            "",
            f"{d_emoji} <b>#{symbol}</b>  {direction.upper()}",
            f"📍 Вход:      <b>${entry:,.6f}</b>",
            f"🛑 Было SL:   <b>${old_sl:,.6f}</b>  ({old_pnl:+.2f}%)",
            f"✅ Теперь SL: <b>${new_sl:,.6f}</b>  ({sl_pnl:+.2f}%)",
            f"📊 TP взято: {taken}",
            f"🔄 Обновление: {ex_icon}",
            f"🕐 {datetime.utcnow().strftime('%H:%M UTC')}",
        ]
        if move_type == "безубыток":
            lines.append(f"\n<i>Сработало после TP{self.BREAKEVEN_AFTER_TP} — позиция в безубытке.</i>")

        await self._notify(signal, "\n".join(lines))
        print(f"[PositionTracker] SL {move_type}: {symbol} {old_sl:.6f} → {new_sl:.6f} | Биржа: {exchange_updated}")

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
        # ✅ FIX: define d_emoji early (was used before definition → crash)
        d_emoji   = "🟢" if direction == "long" else "🔴"

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
            signal["pnl"]         = round(total_pnl, 4)  # ✅ FIX v6: dashboard compatibility
            signal["tp_level"]    = tp_label
            
            # 🎢 Phase 2: Очистка Micro-Step Trailing при закрытии всех TP
            self.micro_trailing.remove(symbol)
            # ✅ FIX: удаляем positions:{symbol} — иначе дашборд показывает закрытую позицию вечно
            self.redis.remove_position(self.bot_type, symbol)

        # ✅ v2.5: Трекаем максимальный взятый TP уровень
        signal["max_tp_reached"] = tp_label
        signal["tp_taken_count"] = len(taken)
        
        # 🎢 Phase 2: Micro-Step Trailing — обновляем SL после TP
        if not is_last:  # Не обновляем если последний TP (позиция закрывается)
            new_sl_micro = self.micro_trailing.on_tp_taken(
                symbol=symbol,
                tp_level=tp_num,
                current_price=current_price
            )
            if new_sl_micro:
                # Обновляем SL в сигнале и на бирже
                old_sl = signal.get("stop_loss", 0)
                signal["stop_loss"] = new_sl_micro
                signal["trailing_active"] = True
                # ✅ FIX v3.0: Ставим be_done=True здесь, чтобы _check_trailing()
                # не сделал второй _move_sl() в том же цикле → дубли Telegram
                if tp_num == 1:
                    signal["be_done"] = True
                elif tp_num == 2:
                    signal["be_done"] = True
                    signal["be2_done"] = True
                self._save(symbol, signal)
                
                # Перемещаем SL на бирже
                await self._move_sl(signal, old_sl, new_sl_micro, "трейлинг")
                
                # 🎢 Phase 3: Красивое уведомление о Micro-Step Trailing
                summary = self.micro_trailing.get_summary(symbol)
                if summary:
                    trail_lines = [
                        f"🎢 <b>Стоп передвинут — Micro-Step #{summary['steps_taken']}</b>",
                        "",
                        f"{d_emoji} <b>#{symbol}</b>  {direction.upper()}",
                        f"📍 Вход:       <b>${entry:,.6f}</b>",
                        f"🛑 Было SL:    <b>${old_sl:,.6f}</b>",
                        f"✅ Теперь SL:  <b>${new_sl_micro:,.6f}</b>",
                        f"📊 TP взято:   {len(taken)}/{total}",
                        f"🎯 Защита:    <b>+{summary['total_moved_pct']:.2f}%</b> от входа",
                    ]
                    await self._notify(signal, "\n".join(trail_lines))
        
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
            # Уведомление о безубытке который будет после TP2
            if tp_num == self.BREAKEVEN_AFTER_TP:
                lines.append(f"\n🔒 <i>SL переносится в безубыток после TP{self.BREAKEVEN_AFTER_TP}</i>")

        await self._notify(signal, "\n".join(lines))

    # =========================================================================
    # CLOSE SL
    # =========================================================================

    async def _close_sl(self, signal: Dict, current_price: float):
        """
        ✅ v2.4 FIX: Итоговый P&L учитывает уже взятые TP.
        Было: pnl = _pnl(entry, price) = всегда -1.5% (игнорировал TP1..5).
        Стало: tp_profit + sl_loss × remaining_weight.
        Пример: TP1=+5% (25%) взят → SL=-1.5%(75%) → net=+0.125% (WIN!)
        """
        direction    = signal["direction"]
        entry        = _f(signal["entry_price"])
        sl_price     = _f(signal["stop_loss"])
        symbol       = signal["symbol"]
        was_trailing = signal.get("trailing_active", False)
        be_done      = signal.get("be_done", False)
        taken        = list(signal.get("taken_tps", []))
        tps_raw      = signal.get("take_profits", [])

        # P&L от уже взятых TP
        tp_pnl = _calc_weighted_pnl(direction, entry, tps_raw, taken) if taken else 0.0

        # Вес оставшейся позиции
        taken_weight = sum(_parse_tp(tps_raw[i])[1] for i in taken if i < len(tps_raw))
        remaining_w  = max(0.0, 100.0 - taken_weight) / 100.0

        # Итоговый P&L = прибыль TP + убыток по стопу на остаток
        raw_sl_pnl = _pnl(direction, entry, current_price)
        total_pnl  = round(tp_pnl + raw_sl_pnl * remaining_w, 4)

        time_str = _time_in_trade(signal)

        # ✅ Определяем тип закрытия для статистики
        if was_trailing:
            tp_level_label = "SL-TRAIL"
        elif be_done and total_pnl >= -0.1:
            tp_level_label = "BE"
        else:
            tp_level_label = "SL"

        # 🎢 Phase 3: Информация о Micro-Step при закрытии
        trail_summary = self.micro_trailing.get_summary(symbol)
        micro_info = ""
        if trail_summary and trail_summary['steps_taken'] > 0:
            micro_info = (f"\n🎢 Micro-Step: {trail_summary['steps_taken']} шагов, "
                         f"защита +{trail_summary['total_moved_pct']:.2f}%")
        
        # Очистка Micro-Step Trailing
        self.micro_trailing.remove(symbol)
        
        signal["status"]      = "closed_sl"
        signal["close_price"] = current_price
        signal["close_time"]  = datetime.utcnow().isoformat()
        signal["pnl_pct"]     = total_pnl
        signal["pnl"]         = total_pnl  # ✅ FIX v6: dashboard compat
        # ✅ v2.5: Показываем "SL(после TP1)" если был взят TP
        max_tp_hit = signal.get("max_tp_reached", "")
        if max_tp_hit:
            sl_type_label = f"SL(после {max_tp_hit})"
        elif be_done:
            sl_type_label = "BE"
        elif was_trailing:
            sl_type_label = "SL-TRAIL"
        else:
            sl_type_label = "SL"
        signal["tp_level"]    = sl_type_label
        self._save(symbol, signal)
        # ✅ FIX: удаляем positions:{symbol} после SL — дашборд не должен показывать закрытые позиции
        self.redis.remove_position(self.bot_type, symbol)

        d_emoji  = "🔴" if direction == "short" else "🟢"
        sl_type  = ("трейлинг-стоп" if was_trailing else
                    "безубыток"     if be_done      else "стоп-лосс")
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
            lines.append("\n<i>Закрыто в безубытке. Риск = 0.</i>")

        await self._notify(signal, "\n".join(lines))
        # ✅ FIX: используем sl_type_label вместо tp_level_label (учитывает max_tp_hit)
        await self._record_pnl(signal, total_pnl, "sl", sl_type_label)

    async def _expire(self, signal: Dict):
        symbol   = signal.get("symbol", "?")
        entry    = signal.get("entry_price", 0)
        time_str = _time_in_trade(signal)
        signal["status"]     = "expired"
        signal["close_time"] = datetime.utcnow().isoformat()
        self._save(symbol, signal)
        # ✅ FIX: удаляем positions:{symbol} при истечении сигнала
        self.redis.remove_position(self.bot_type, symbol)
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

            # Bot state (backward compat)
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

            # stats:daily:{date} (для /stats команды)
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

            # 🆕 ПОЛНАЯ история для /alltradestat
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

                entry = signal.get("entry_price", 0)
                close_p = signal.get("close_price", 0)
                sl_price = signal.get("stop_loss", 0)
                tps = signal.get("take_profits", [])
                taken = signal.get("taken_tps", [])

                record = {
                    # Базовые
                    "symbol":       symbol,
                    "direction":    signal.get("direction", "?"),
                    "entry_price":  entry,
                    "close_price":  close_p,
                    "stop_loss":    sl_price,
                    "pnl":          round(pnl_pct, 4),  # ✅ both fields
                    "pnl_pct":      round(pnl_pct, 4),
                    "tp_level":     tp_level,
                    "close_type":   close_type,
                    "opened_at":    opened_at,
                    "closed_at":    closed_at,
                    "hold_minutes": hold_secs // 60,
                    # Debug
                    "_debug_be_done": signal.get("be_done", False),
                    "_debug_trailing": signal.get("trailing_active", False),
                    # Скоринг и паттерны
                    "score":        signal.get("score", 0),
                    "pattern":      signal.get("pattern", ""),
                    "leverage":     signal.get("leverage", "?"),
                    "risk":         signal.get("risk", "?"),
                    # Рыночные данные на момент входа
                    "rsi_1h":       signal.get("rsi_1h", 0),
                    "funding_rate": signal.get("funding_rate", 0),
                    "oi_change":    signal.get("oi_change", 0),
                    "long_short_ratio": signal.get("long_short_ratio", 0),
                    "volume_spike": signal.get("volume_spike_ratio", 0),
                    "atr_pct":      signal.get("atr_14_pct", 0),
                    # SMC данные
                    "smc_ob":       signal.get("smc_data", {}).get("has_ob", False),
                    "smc_fvg":      signal.get("smc_data", {}).get("has_fvg", False),
                    "smc_bonus":    signal.get("smc_data", {}).get("score_bonus", 0),
                    # TP детали
                    "tp_count":     len(tps),
                    "tp_taken":     len(taken),
                    "tp_prices":    [t[0] if isinstance(t, (list,tuple)) else t.get("price",0) for t in tps[:6]],
                    # Причины сигнала
                    "reasons":      signal.get("reasons", [])[:8],
                    "realtime_factors": signal.get("realtime_factors", [])[:5],
                }
                # Пишем в общую историю бота
                hkey = f"{self.bot_type}:history:{symbol}"
                self.redis.client.lpush(hkey, json.dumps(record))
                self.redis.client.ltrim(hkey, 0, 199)
                self.redis.client.expire(hkey, 2592000)
                # 🆕 Также пишем в глобальный лог для /alltradestat (все сделки)
                all_key = f"{self.bot_type}:all_trades"
                self.redis.client.lpush(all_key, json.dumps(record))
                self.redis.client.ltrim(all_key, 0, 9999)   # 10k сделок
                self.redis.client.expire(all_key, 7776000)  # 90 дней
                print(f"[PT][RECORD][{symbol}] tp_level={tp_level} pnl={pnl_pct:.2f}% close_type={close_type}")
            except Exception as e:
                print(f"[PT] history: {e}")

            if self.auto_trader:
                self.auto_trader.record_trade_result(pnl_pct)

        except Exception as e:
            print(f"[PT] _record_pnl: {e}")

    # =========================================================================
    # 🧟 ZOMBIE CLEANUP
    # =========================================================================

    async def cleanup_zombies(self) -> int:
        """
        🧹 Очистка zombie позиций (есть в Redis, но не на бирже)
        
        Returns:
            int: Количество удаленных позиций
        """
        if not self.bingx:
            print(f"[PT][ZOMBIE] ⚠️ Нет BingX клиента для проверки")
            return 0
            
        removed_count = 0
        try:
            # Получаем все позиции из Redis
            redis_positions = self.redis.get_all_positions(self.bot_type)
            if not redis_positions:
                print(f"[PT][ZOMBIE] ✅ Нет позиций в Redis для проверки")
                return 0
                
            # Получаем позиции с биржи
            try:
                bingx_positions = await self.bingx.get_positions()
            except Exception as e:
                print(f"[PT][ZOMBIE] ⚠️ Ошибка получения позиций с биржи: {e}")
                return 0
                
            # Создаем множество символов с позициями на бирже
            bingx_symbols = set()
            if isinstance(bingx_positions, list):
                for pos in bingx_positions:
                    symbol = pos.get("symbol", "")
                    if symbol:
                        # Нормализуем символ (убираем - если есть)
                        bingx_symbols.add(symbol.replace("-", ""))
                        bingx_symbols.add(symbol)  # И с дефисом тоже
                        
            print(f"[PT][ZOMBIE] 🔍 Проверяем {len(redis_positions)} позиций в Redis vs {len(bingx_symbols)} на бирже")
            
            # Проверяем каждую позицию из Redis
            for symbol in list(redis_positions.keys()):
                # Нормализуем символ для сравнения
                normalized = symbol.replace("-", "")
                
                # Проверяем есть ли позиция на бирже
                on_exchange = False
                for bx_sym in bingx_symbols:
                    if bx_sym.replace("-", "") == normalized:
                        on_exchange = True
                        break
                        
                if not on_exchange:
                    # Проверяем можно ли получить цену (символ существует)
                    try:
                        ticker = await self.bingx.get_ticker(symbol)
                        if ticker and ticker.get("price", 0) > 0:
                            # Символ существует, но позиции нет - это zombie
                            print(f"[PT][ZOMBIE] 🗑️ Удаляем {symbol} (нет на бирже, цена доступна)")
                            self.redis.remove_position(self.bot_type, symbol)
                            removed_count += 1
                        else:
                            # Не можем получить цену - возможно символ делистед
                            print(f"[PT][ZOMBIE] 🗑️ Удаляем {symbol} (нет на бирже, цена недоступна - делист)")
                            self.redis.remove_position(self.bot_type, symbol)
                            removed_count += 1
                    except Exception as e:
                        print(f"[PT][ZOMBIE] 🗑️ Удаляем {symbol} (ошибка проверки: {e})")
                        self.redis.remove_position(self.bot_type, symbol)
                        removed_count += 1
                        
            if removed_count > 0:
                print(f"[PT][ZOMBIE] ✅ Очищено {removed_count} zombie позиций")
                await self._send(f"🧹 Очищено {removed_count} ghost-позиций из {self.bot_type.upper()}")
            else:
                print(f"[PT][ZOMBIE] ✅ Все позиции актуальны")
                
        except Exception as e:
            print(f"[PT][ZOMBIE] 🔴 Ошибка cleanup: {e}")
            
        return removed_count

    # =========================================================================
    # HELPERS
    # =========================================================================

    async def _notify(self, signal: Dict, text: str):
        """Thread reply на исходный сигнал, fallback — обычное сообщение."""
        tg_msg_id = signal.get("tg_msg_id")
        if tg_msg_id:
            try:
                await self.tg.send_reply(text, reply_to_message_id=tg_msg_id)
                return
            except Exception as e:
                print(f"[PT] send_reply failed: {e}")
        await self._send(text)

    def _save(self, symbol: str, signal: Dict):
        """
        ✅ FIX v3.0: Обновляем ОБА ключа Redis:
        - positions:{symbol}  ← читает get_active_signals() (главный ключ!)
        - signals:{symbol}    ← история сигналов (для дашборда)
        Было: только signals → get_active_signals возвращал старые данные без taken_tps
        → TP1 срабатывал повторно на каждом цикле скана
        """
        try:
            signal["confirmed"] = True  # ← 7-day TTL, не 30min
            self.redis.save_position(self.bot_type, symbol, signal)   # ← MAIN FIX
            self.redis.save_signal(self.bot_type, symbol, signal)     # ← history
        except Exception as e:
            print(f"[PT] redis save: {e}")

    def _update_position_pnl(self, symbol: str, current_price: float, unrealized_pnl: float):
        """Обновляем текущий P&L и цену в позиции для дашборда"""
        try:
            # Получаем текущую позицию
            pos = self.redis.get_position(self.bot_type, symbol)
            if pos:
                pos["current_price"] = current_price
                pos["unrealized_pnl"] = round(unrealized_pnl, 2)
                pos["last_updated"] = datetime.utcnow().isoformat()
                self.redis.save_position(self.bot_type, symbol, pos)
        except Exception as e:
            print(f"[PT] _update_position_pnl error: {e}")

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
