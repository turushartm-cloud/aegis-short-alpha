"""
Telegram Bot Integration  v2.2

НОВОЕ v2.2:
  ✅ send_message() возвращает Optional[int] (message_id) — для thread replies
  ✅ send_reply(text, reply_to_msg_id) — ответ на исходное сообщение сигнала
  ✅ send_signal() возвращает Optional[int] — main.py сохраняет msg_id в Redis
  ✅ #SYMBOL вместо SYMBOL — удобный поиск в Telegram
  ✅ /daily_rep, /weekly_rep, /monthly_rep, /leaderswr — отчёты
  ✅ /stats — читает из правильных Redis ключей (stats:daily:{date})
  ✅ Уведомления о TP/SL/trailing читают tg_msg_id из Redis → reply
"""

import os
import json
import asyncio
from typing import Optional, Dict, List, Callable
from datetime import datetime, timedelta

import aiohttp


# ============================================================================
# SMART PRICE FORMATTER
# ============================================================================

def fmt_price(price: float) -> str:
    if price == 0:
        return "$0"
    abs_p = abs(price)
    if abs_p >= 1000:   return f"${price:,.2f}"
    elif abs_p >= 1:    return f"${price:,.4f}"
    elif abs_p >= 0.01: return f"${price:,.6f}"
    elif abs_p >= 0.0001: return f"${price:,.8f}"
    else:               return f"${price:,.12f}"


# ============================================================================
# TELEGRAM BOT
# ============================================================================

class TelegramBot:
    """Telegram бот для сигналов, уведомлений и команд."""

    def __init__(self,
                 bot_token: Optional[str] = None,
                 chat_id: Optional[str] = None,
                 topic_id: Optional[str] = None,
                 bot_type: str = ""):
        # ✅ v2.4 FIX: поддержка LONG_/SHORT_ префиксов + обычных имён
        # Порядок: явный аргумент → {PREFIX}_TELEGRAM_ → TELEGRAM_ (общий)
        prefix = (bot_type.upper() + "_") if bot_type else ""
        self.bot_token = (bot_token
                          or os.getenv(f"{prefix}TELEGRAM_BOT_TOKEN")
                          or os.getenv("LONG_TELEGRAM_BOT_TOKEN")
                          or os.getenv("SHORT_TELEGRAM_BOT_TOKEN")
                          or os.getenv("TELEGRAM_BOT_TOKEN"))
        self.chat_id   = (chat_id
                          or os.getenv(f"{prefix}TELEGRAM_CHAT_ID")
                          or os.getenv("LONG_TELEGRAM_CHAT_ID")
                          or os.getenv("SHORT_TELEGRAM_CHAT_ID")
                          or os.getenv("TELEGRAM_CHAT_ID"))
        self.topic_id  = (topic_id
                          or os.getenv(f"{prefix}TELEGRAM_TOPIC_ID")
                          or os.getenv("LONG_TELEGRAM_TOPIC_ID")
                          or os.getenv("SHORT_TELEGRAM_TOPIC_ID")
                          or os.getenv("TELEGRAM_TOPIC_ID"))

        if not self.bot_token:
            raise ValueError(
                "Telegram bot token not provided. "
                "Set LONG_TELEGRAM_BOT_TOKEN or SHORT_TELEGRAM_BOT_TOKEN in Render env vars."
            )
        if not self.chat_id:
            raise ValueError(
                "Telegram chat ID not provided. "
                "Set LONG_TELEGRAM_CHAT_ID or SHORT_TELEGRAM_CHAT_ID in Render env vars."
            )
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # =========================================================================
    # WEBHOOK
    # =========================================================================

    async def setup_webhook(self, webhook_url: str) -> bool:
        try:
            session = await self._get_session()
            payload = {
                "url": webhook_url,
                "allowed_updates": ["message", "callback_query"],
                "drop_pending_updates": True,
            }
            async with session.post(
                f"{self.base_url}/setWebhook",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                if data.get("ok"):
                    print(f"✅ Webhook registered: {webhook_url}")
                    return True
                print(f"❌ Webhook failed: {data}")
                return False
        except Exception as e:
            print(f"Error setting webhook: {e}")
            return False

    async def delete_webhook(self) -> bool:
        try:
            session = await self._get_session()
            async with session.post(
                f"{self.base_url}/deleteWebhook",
                json={"drop_pending_updates": True},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                return data.get("ok", False)
        except Exception as e:
            print(f"Error deleting webhook: {e}")
            return False

    async def get_webhook_info(self) -> Optional[Dict]:
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.base_url}/getWebhookInfo",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                return data.get("result")
        except Exception as e:
            print(f"Error getting webhook info: {e}")
            return None

    # =========================================================================
    # SEND — возвращает message_id для thread replies
    # =========================================================================

    async def _send_message(self,
                             text: str,
                             parse_mode: str = "HTML",
                             reply_to_message_id: Optional[int] = None,
                             chat_id: Optional[str] = None) -> Optional[int]:
        """
        Отправить сообщение.
        ✅ Возвращает message_id (int) при успехе, None при ошибке.
        ✅ reply_to_message_id — привязка к исходному сообщению сигнала.
        """
        try:
            payload: Dict = {
                "chat_id":                  chat_id or self.chat_id,
                "text":                     text,
                "parse_mode":               parse_mode,
                "disable_web_page_preview": True,
            }
            if self.topic_id and not chat_id:
                payload["message_thread_id"] = int(self.topic_id)
            if reply_to_message_id:
                payload["reply_to_message_id"]       = reply_to_message_id
                payload["allow_sending_without_reply"] = True   # не фейлить если оригинал удалён

            session = await self._get_session()
            async with session.post(
                f"{self.base_url}/sendMessage",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    msg_id = data.get("result", {}).get("message_id")
                    return msg_id  # ← возвращаем message_id!
                error_text = await resp.text()
                print(f"Telegram API error: {resp.status} — {error_text[:120]}")
                return None
        except Exception as e:
            print(f"Error sending Telegram message: {e}")
            return None

    async def send_message(self, text: str) -> Optional[int]:
        """Отправить сообщение. Возвращает message_id."""
        return await self._send_message(text)

    async def send_reply(self, text: str, reply_to_message_id: int) -> Optional[int]:
        """
        Отправить сообщение как ОТВЕТ на исходный сигнал.
        Использовать для: TP hit / SL hit / trailing stop update.
        """
        return await self._send_message(text, reply_to_message_id=reply_to_message_id)

    async def send_signal(self, direction: str, **kwargs) -> Optional[int]:
        """
        Отправить сигнал. Возвращает message_id для хранения в Redis.
        main.py сохраняет этот ID в signal["tg_msg_id"] → Redis.
        """
        if direction == "short":
            text = self.format_short_signal(**kwargs)
        else:
            text = self.format_long_signal(**kwargs)
        return await self._send_message(text)

    async def send_test_message(self) -> bool:
        result = await self._send_message(
            "🤖 <b>Bot Connected</b>\n\nСоединение с Telegram установлено!"
        )
        return result is not None

    async def send_error_alert(self, error: str, context: str = "") -> bool:
        result = await self._send_message(
            f"<b>⚠️ BOT ERROR</b>\n\n"
            f"<b>Context:</b> {context}\n"
            f"<b>Error:</b> <code>{error}</code>\n"
            f"<b>Time:</b> {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )
        return result is not None

    # =========================================================================
    # FORMAT: SIGNALS
    # =========================================================================

    def _calc_pct(self, entry: float, target: float) -> float:
        return ((target - entry) / entry * 100) if entry else 0.0

    def _score_grade(self, score: float):
        if score >= 85: return "🔥", "ЭКСТРЕМАЛЬНЫЙ"
        if score >= 75: return "⚡", "СИЛЬНЫЙ"
        if score >= 65: return "✅", "ХОРОШИЙ"
        return "⚠️", "СРЕДНИЙ"

    def format_long_signal(self,
                           symbol: str,
                           score: float,
                           price: float,
                           pattern: str,
                           indicators: Dict,
                           entry: float,
                           stop_loss: float,
                           take_profits: List[tuple],
                           leverage: str,
                           risk: str,
                           valid_minutes: int = 30) -> str:
        emoji, strength = self._score_grade(score)
        sl_pct = self._calc_pct(entry, stop_loss)

        tp_lines = ""
        for i, tp_item in enumerate(take_profits, 1):
            tp_price  = float(tp_item[0]) if isinstance(tp_item, (list, tuple)) else float(tp_item.get("price", 0))
            tp_weight = tp_item[1] if isinstance(tp_item, (list, tuple)) else tp_item.get("weight", 0)
            pct = abs(self._calc_pct(entry, tp_price))
            tp_lines += f"   TP{i}: <b>{fmt_price(tp_price)}</b>  (+{pct:.1f}%)  [{tp_weight}%]\n"

        ind_lines = "\n".join(f"   {k}: <b>{v}</b>" for k, v in indicators.items())

        # ✅ #SYMBOL для удобного поиска в Telegram
        return (
            f"\n{emoji} <b>LONG SIGNAL | {strength}</b>\n"
            f"<b>Score: {score:.0f}%</b>\n\n"
            f"<b>💎 SYMBOL:</b> <b>#{symbol}</b>\n"
            f"<b>📊 Pattern:</b> {pattern}\n\n"
            f"<b>📈 INDICATORS:</b>\n{ind_lines}\n\n"
            f"<b>🎯 LEVELS:</b>\n"
            f"   Entry: <b>{fmt_price(entry)}</b>\n"
            f"   Stop:  <b>{fmt_price(stop_loss)}</b>  (-{abs(sl_pct):.2f}%)\n"
            f"{tp_lines}\n"
            f"<b>⚡ Leverage:</b> {leverage}x\n"
            f"<b>💰 Risk:</b> {risk}\n"
            f"<b>⏱ Valid:</b> ~{valid_minutes} мин\n"
            f"<b>🕐 Time:</b> {datetime.utcnow().strftime('%H:%M UTC')}"
        )

    def format_short_signal(self,
                            symbol: str,
                            score: float,
                            price: float,
                            pattern: str,
                            indicators: Dict,
                            entry: float,
                            stop_loss: float,
                            take_profits: List[tuple],
                            leverage: str,
                            risk: str,
                            valid_minutes: int = 30) -> str:
        emoji, strength = self._score_grade(score)
        sl_pct = self._calc_pct(entry, stop_loss)

        tp_lines = ""
        for i, tp_item in enumerate(take_profits, 1):
            tp_price  = float(tp_item[0]) if isinstance(tp_item, (list, tuple)) else float(tp_item.get("price", 0))
            tp_weight = tp_item[1] if isinstance(tp_item, (list, tuple)) else tp_item.get("weight", 0)
            pct = abs(self._calc_pct(entry, tp_price))
            tp_lines += f"   TP{i}: <b>{fmt_price(tp_price)}</b>  (-{pct:.1f}%)  [{tp_weight}%]\n"

        ind_lines = "\n".join(f"   {k}: <b>{v}</b>" for k, v in indicators.items())

        return (
            f"\n{emoji} <b>SHORT SIGNAL | {strength}</b>\n"
            f"<b>Score: {score:.0f}%</b>\n\n"
            f"<b>💎 SYMBOL:</b> <b>#{symbol}</b>\n"
            f"<b>📊 Pattern:</b> {pattern}\n\n"
            f"<b>📈 INDICATORS:</b>\n{ind_lines}\n\n"
            f"<b>🎯 LEVELS:</b>\n"
            f"   Entry: <b>{fmt_price(entry)}</b>\n"
            f"   Stop:  <b>{fmt_price(stop_loss)}</b>  (+{abs(sl_pct):.2f}%)\n"
            f"{tp_lines}\n"
            f"<b>⚡ Leverage:</b> {leverage}x\n"
            f"<b>💰 Risk:</b> {risk}\n"
            f"<b>⏱ Valid:</b> ~{valid_minutes} мин\n"
            f"<b>🕐 Time:</b> {datetime.utcnow().strftime('%H:%M UTC')}"
        )

    # =========================================================================
    # FORMAT: TRADE UPDATES (для thread replies)
    # =========================================================================

    def format_tp_hit(self, symbol: str, direction: str, tp_num: int,
                      total_tps: int, entry: float, tp_price: float,
                      pnl_pct: float, duration_str: str,
                      tps_left: int) -> str:
        d_emoji = "🟢" if direction == "long" else "🔴"
        dir_str = "LONG" if direction == "long" else "SHORT"
        pnl_sign = "+" if pnl_pct >= 0 else ""
        return (
            f"🎯 <b>TP{tp_num}/{total_tps} взят!</b>\n\n"
            f"{d_emoji} <b>#{symbol}</b>  {dir_str}\n"
            f"📍 Вход:      <b>{fmt_price(entry)}</b>\n"
            f"🎯 TP{tp_num}:     <b>{fmt_price(tp_price)}</b>\n"
            f"📊 P&L:       <b>{pnl_sign}{pnl_pct:.2f}%</b>\n"
            f"⏱ В сделке:  {duration_str}\n"
            + (f"⏳ До следующего TP: {tps_left} шт." if tps_left > 0 else "✅ <b>Все TP закрыты!</b>")
        )

    def format_sl_hit(self, symbol: str, direction: str, entry: float,
                      sl_price: float, close_price: float,
                      pnl_pct: float, duration_str: str,
                      reason: str = "стоп") -> str:
        d_emoji = "🟢" if direction == "long" else "🔴"
        dir_str = "LONG" if direction == "long" else "SHORT"
        reason_map = {
            "trailing": "трейлинг-стоп",
            "sl":       "стоп-лосс",
            "manual":   "ручное закрытие",
        }
        reason_str = reason_map.get(reason, reason)
        pnl_sign = "+" if pnl_pct >= 0 else ""
        return (
            f"🛑 <b>Стоп выбит</b>  ({reason_str})\n\n"
            f"{d_emoji} <b>#{symbol}</b>  {dir_str}\n"
            f"📍 Вход:     <b>{fmt_price(entry)}</b>\n"
            f"🛑 Стоп:     <b>{fmt_price(sl_price)}</b>\n"
            f"💰 Закрыто:  <b>{fmt_price(close_price)}</b>\n"
            f"📊 P&L:      <b>{pnl_sign}{pnl_pct:.2f}%</b>\n"
            f"⏱ В сделке: {duration_str}\n"
            f"🕐 {datetime.utcnow().strftime('%H:%M UTC')}"
        )

    def format_trailing_update(self, symbol: str, direction: str,
                               entry: float, old_sl: float,
                               new_sl: float) -> str:
        d_emoji = "🟢" if direction == "long" else "🔴"
        dir_str = "LONG" if direction == "long" else "SHORT"
        old_pct = abs((old_sl - entry) / entry * 100)
        new_pct = abs((new_sl - entry) / entry * 100)
        sign_old = "+" if old_sl >= entry else "-"
        sign_new = "+" if new_sl >= entry else "-"
        return (
            f"🔄 <b>Стоп передвинут — ТРЕЙЛИНГ</b>\n\n"
            f"{d_emoji} <b>#{symbol}</b>  {dir_str}\n"
            f"📍 Вход:     <b>{fmt_price(entry)}</b>\n"
            f"🛑 Было SL:  <b>{fmt_price(old_sl)}</b>  ({sign_old}{old_pct:.2f}%)\n"
            f"✅ Теперь SL: <b>{fmt_price(new_sl)}</b>  ({sign_new}{new_pct:.2f}%)\n"
            f"🕐 {datetime.utcnow().strftime('%H:%M UTC')}"
        )


# ============================================================================
# COMMAND HANDLER
# ============================================================================

class TelegramCommandHandler:
    """Обработчик входящих команд."""

    ALLOWED_COMMANDS = {
        "/start", "/help", "/ping", "/status",
        "/signals", "/stats", "/scan",
        "/pause", "/resume", "/setscore", "/closeall", "/close_all",
        "/clearpos", "/balance", "/positions",
        "/emergency_stop", "/reset_stats", "/cleanup", "/clean", "/logs",
        # Новые команды
        "/sync", "/flushdb",
        # Новые отчёты
        "/daily_rep", "/weekly_rep", "/monthly_rep", "/leaderswr",
        "/alltradestat",
    }

    def __init__(self,
                 bot: TelegramBot,
                 redis_client,
                 bot_state,
                 bot_type: str,
                 scan_callback: Optional[Callable] = None,
                 config=None):
        self.bot           = bot
        self.redis         = redis_client
        self.state         = bot_state
        self.bot_type      = bot_type
        self.scan_callback = scan_callback
        self.config        = config

    async def _reply(self, chat_id: str, text: str) -> Optional[int]:
        try:
            payload: Dict = {
                "chat_id":                  chat_id,
                "text":                     text,
                "parse_mode":               "HTML",
                "disable_web_page_preview": True,
            }
            if self.bot.topic_id and str(chat_id) == str(self.bot.chat_id):
                payload["message_thread_id"] = int(self.bot.topic_id)

            session = await self.bot._get_session()
            async with session.post(
                f"{self.bot.base_url}/sendMessage",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("result", {}).get("message_id")
                err = await resp.text()
                print(f"[Telegram reply] Error {resp.status}: {err[:120]}")
                return None
        except Exception as e:
            print(f"Error sending reply: {e}")
            return None

    async def handle_update(self, update: Dict) -> bool:
        try:
            # ✅ FIX: handle regular messages + edited messages
            message = (update.get("message") 
                      or update.get("channel_post")
                      or update.get("edited_message"))
            if not message:
                return False

            text          = message.get("text", "").strip()
            reply_chat_id = str(message.get("chat", {}).get("id", ""))
            user_id       = str(message.get("from", {}).get("id", ""))
            chat_type     = message.get("chat", {}).get("type", "private")

            if not text.startswith("/"):
                return False

            parts = text.split()
            cmd   = parts[0].split("@")[0].lower()
            args  = parts[1:]

            print(f"📨 Command: {cmd} from chat {reply_chat_id} (user {user_id}, type={chat_type})")

            # ✅ FIX: Admin check для private И group чатов
            # В group/supergroup — только если user_id в ADMIN_USER_IDS
            admin_ids_raw = os.getenv("ADMIN_USER_IDS", "")
            if admin_ids_raw:
                allowed = {s.strip() for s in admin_ids_raw.split(",")}
                # Проверяем и строки и числа (Telegram может слать int или str)
                if user_id not in allowed and str(user_id) not in allowed:
                    # В private — блокируем; в group — логируем но пропускаем
                    if chat_type == "private":
                        print(f"⛔ Unauthorized private: user {user_id}")
                        await self._reply(reply_chat_id, "⛔ Нет доступа.")
                        return False
                    else:
                        print(f"⚠️ Non-admin group command from {user_id} — skip")
                        return False

            if cmd not in self.ALLOWED_COMMANDS:
                await self._reply(reply_chat_id,
                    f"❓ Неизвестная команда: <code>{cmd}</code>\nНапиши /help")
                return False

            handlers = {
                "/start":        self.cmd_start,
                "/help":         self.cmd_start,
                "/ping":         self.cmd_ping,
                "/status":       self.cmd_status,
                "/signals":      self.cmd_signals,
                "/stats":        self.cmd_stats,
                "/scan":         self.cmd_scan,
                "/pause":        self.cmd_pause,
                "/resume":       self.cmd_resume,
                "/setscore":     self.cmd_set_min_score,
                "/clearpos":     self.cmd_clearpos,
                "/closeall":     self.cmd_closeall,
                "/close_all":    self.cmd_closeall,
                "/balance":      self.cmd_balance,
                "/positions":    self.cmd_positions,
                "/emergency_stop": self.cmd_emergency_stop,
                "/reset_stats":  self.cmd_reset_stats,
                "/cleanup":      self.cmd_cleanup,
                "/clean":        self.cmd_clean,
                "/logs":         self.cmd_logs,
                # 🆕 Новые команды
                "/sync":         self.cmd_sync,
                "/flushdb":      self.cmd_flushdb,
                # Отчёты
                "/daily_rep":    self.cmd_daily_report,
                "/weekly_rep":   self.cmd_weekly_report,
                "/monthly_rep":  self.cmd_monthly_report,
                "/leaderswr":    self.cmd_leaders_wr,
                # 🆕 Полная аналитика
                "/alltradestat": self.cmd_alltradestat,
            }
            await handlers[cmd](args, reply_chat_id)
            return True

        except Exception as e:
            print(f"Error handling update: {e}")
            return False

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_trade_history(self, days: int = 30) -> List[Dict]:
        """Получить историю сделок из Redis за последние N дней."""
        if not self.redis:
            return []
        trades = []
        try:
            # История хранится по символам: {bot_type}:history:{symbol}
            pattern = f"{self.bot_type}:history:*"
            keys = self.redis.client.keys(pattern)
            for key in keys:
                sym_trades = self.redis.client.lrange(key, 0, -1)
                cutoff = datetime.utcnow() - timedelta(days=days)
                for t_json in sym_trades:
                    try:
                        import json
                        t = json.loads(t_json)
                        closed_at = t.get("closed_at", "")
                        if closed_at:
                            dt = datetime.fromisoformat(closed_at)
                            if dt >= cutoff:
                                trades.append(t)
                    except Exception:
                        pass
        except Exception as e:
            print(f"_get_trade_history error: {e}")
        return trades

    def _wr_emoji(self, wr: float) -> str:
        if wr >= 60: return "🟢"
        if wr >= 45: return "🟡"
        return "🔴"

    def _duration_str(self, seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}ч {m}м" if h else f"{m}м"

    def _get_daily_stats_from_redis(self, date_str: str) -> Dict:
        """Читает stats из Redis ключей stats:daily:{date}."""
        if not self.redis:
            return {}
        try:
            return self.redis.get_daily_stats(self.bot_type, date_str) or {}
        except Exception:
            return {}

    def _get_tp_stats_for_day(self, date_str: str) -> Dict:
        """🆕 Получает статистику по TP1-6 уровням за день"""
        result = {"total": 0}
        
        # Получаем из Redis (ключи tp_stats:YYYY-MM-DD:*)
        for level in ["TP1", "TP2", "TP3", "TP4", "TP5", "TP6", "BE", "SL"]:
            count_key = f"tp_stats:{date_str}:{level}:count"
            pnl_key = f"tp_stats:{date_str}:{level}:pnl"
            
            count = int(self.redis.get(count_key) or 0) if self.redis else 0
            pnl = float(self.redis.get(pnl_key) or 0) if self.redis else 0
            
            if count > 0:
                result[level] = {"count": count, "pnl": pnl}
                result["total"] += count
        
        return result

    # =========================================================================
    # COMMANDS — BASIC
    # =========================================================================

    async def cmd_start(self, args, reply_chat_id: str):
        bot_emoji = "🔴" if self.bot_type == "short" else "🟢"
        bot_name  = "SHORT" if self.bot_type == "short" else "LONG"
        await self._reply(reply_chat_id,
            f"{bot_emoji} <b>Liquidity {bot_name} Bot v2.3</b>\n\n"
            "<b>📋 Команды:</b>\n"
            "📊 /status — Статус бота\n"
            "🎯 /signals — Активные сигналы\n"
            "📉 /stats — Статистика + P&L\n"
            "🔍 /scan — Сканировать рынок сейчас\n\n"
            "<b>💰 Биржа:</b>\n"
            "💳 /balance — Баланс BingX\n"
            "📈 /positions — Открытые позиции\n"
            "❌ /closeall — Закрыть ВСЕ позиции\n"
            "🔄 /sync — Синхронизировать с биржей\n\n"
            "<b>📅 Отчёты:</b>\n"
            "📅 /daily_rep — Дневной отчёт\n"
            "📅 /weekly_rep — Недельный отчёт\n"
            "📅 /monthly_rep — Месячный отчёт\n"
            "🏆 /leaderswr — Топ пар по Win Rate\n"
            "📊 /alltradestat — полная статистика всех сделок для анализа\n\n"
            "<b>⚙️ Управление:</b>\n"
            "⏸ /pause — Остановить новые сигналы\n"
            "▶️ /resume — Возобновить\n"
            "🗑 /clearpos — Сбросить застрявшие позиции\n"
            "🧹 /cleanup — Удалить зависшие сделки\n"
            "🧼 /clean — Полная очистка\n"
            "🔄 /reset_stats — Сбросить статистику\n"
            "🗑 /flushdb yes — Очистить БД Redis\n"
            "🛑 /emergency_stop — Экстренный стоп\n"
            "📜 /logs — Посмотреть логи\n"
            "⚙️ /setscore 75 — Мин. скор\n"
            "🏓 /ping — Проверка связи"
        )

    async def cmd_ping(self, args, reply_chat_id: str):
        await self._reply(reply_chat_id, "🏓 Pong! Бот активен ✅")

    async def cmd_status(self, args, reply_chat_id: str):
        if not self.state:
            await self._reply(reply_chat_id, "✅ Бот работает")
            return
        wl        = len(self.state.watchlist)
        last      = self.state.last_scan.strftime("%H:%M UTC") if self.state.last_scan else "никогда"
        running   = "✅ Работает" if self.state.is_running else "❌ Остановлен"
        paused    = "⏸ На паузе" if self.state.is_paused else ""
        redis_ok  = "✅" if (self.redis and self.redis.health_check()) else "❌"
        min_score = getattr(self.config, "MIN_SCORE", 65) if self.config else 65
        max_pos   = getattr(self.config, "MAX_POSITIONS", 10) if self.config else 10
        
        # ✅ FIX: Считаем реальные позиции на бирже, а не Redis сигналы
        real_positions_count = 0
        if self.state and self.state.auto_trader:
            try:
                all_positions = await self.state.auto_trader.bingx.get_positions()
                expected_side = self.bot_type.upper()
                real_positions_count = len([p for p in all_positions if (
                    getattr(p, "position_side", "").upper() == expected_side or
                    getattr(p, "side", "").upper() == expected_side or
                    (expected_side == "SHORT" and getattr(p, "size", 0) < 0) or
                    (expected_side == "LONG" and getattr(p, "size", 0) > 0)
                )])
            except Exception:
                pass  # Fallback к сигналам если ошибка
        
        await self._reply(reply_chat_id,
            f"🤖 <b>Статус бота - {self.bot_type.upper()}</b>\n\n"
            f"Состояние: {running} {paused}\n"
            f"Watchlist: {wl} монет\n"
            f"Последний скан: {last}\n"
            f"📊 Позиций на бирже: {real_positions_count}/{max_pos}\n"
            f"📡 Redis сигналов: {self.state.active_signals}\n"
            f"Мин. скор: {min_score}%\n"
            f"Redis: {redis_ok}\n"
            f"🕐 {datetime.utcnow().strftime('%H:%M UTC')}"
        )

    async def cmd_signals(self, args, reply_chat_id: str):
        if not self.redis:
            await self._reply(reply_chat_id, "🎯 Нет активных сигналов")
            return
        try:
            signals = self.redis.get_active_signals(self.bot_type)
            if not signals:
                await self._reply(reply_chat_id, "🎯 Нет активных сигналов")
                return

            msg = f"🎯 <b>Активные сигналы ({len(signals)}):</b>\n\n"
            for s in signals[:8]:
                d      = "🔴" if s.get("direction") == "short" else "🟢"
                sym    = s.get("symbol", "?")
                score  = s.get("score", 0)
                entry  = s.get("entry_price", 0)
                taken  = len(s.get("taken_tps", []))
                total  = len(s.get("take_profits", []))
                try:
                    opened = datetime.fromisoformat(s.get("timestamp", ""))
                    age    = datetime.utcnow() - opened
                    h, r   = divmod(int(age.total_seconds()), 3600)
                    m      = r // 60
                    time_s = f"{h}ч {m}м" if h else f"{m}м"
                except Exception:
                    time_s = "N/A"
                msg += (
                    f"{d} <b>#{sym}</b> — Score: {score:.0f}%\n"
                    f"   Вход: {fmt_price(entry)}  |  TP: {taken}/{total}  |  ⏱ {time_s}\n\n"
                )
            await self._reply(reply_chat_id, msg)
        except Exception as e:
            print(f"cmd_signals error: {e}")
            await self._reply(reply_chat_id, "🎯 Нет активных сигналов")

    async def cmd_stats(self, args, reply_chat_id: str):
        """
        ✅ FIX: читаем stats из {bot_type}:stats:daily:{date}
        Именно туда PositionTracker пишет результаты сделок.
        """
        if not self.redis:
            await self._reply(reply_chat_id, "📉 Статистика недоступна")
            return
        try:
            total_trades = 0
            total_wins   = 0
            total_pnl    = 0.0
            lines        = []

            for i in range(7):
                date = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
                d    = self._get_daily_stats_from_redis(date)
                tr   = d.get("trades", 0)
                w    = d.get("wins",   0)
                pnl  = d.get("pnl",    0.0)
                if tr:
                    wr   = round(w / tr * 100, 1) if tr else 0
                    wemj = self._wr_emoji(wr)
                    lines.append(f"  {date}: {tr} сд  {wemj}{wr}%  P&L: {pnl:+.2f}%")
                total_trades += tr
                total_wins   += w
                total_pnl    += pnl

            winrate  = round(total_wins / total_trades * 100, 1) if total_trades else 0
            wr_emoji = self._wr_emoji(winrate)

            msg = (
                f"📉 <b>Статистика за 7 дней</b>\n\n"
                f"📨 Сигналов: {getattr(self.state, 'daily_signals', 0)}\n"
                f"🔄 Сделок закрыто: {total_trades}\n"
                f"✅ Победных: {total_wins}  ({wr_emoji}{winrate}%)\n"
                f"💵 P&L: <b>{total_pnl:+.2f}%</b>\n"
            )
            if lines:
                msg += "\n<b>По дням:</b>\n" + "\n".join(lines)
            msg += f"\n🕐 {datetime.utcnow().strftime('%d.%m.%Y %H:%M UTC')}"
            await self._reply(reply_chat_id, msg)

        except Exception as e:
            print(f"cmd_stats error: {e}")
            await self._reply(reply_chat_id, "📉 Статистика пока пуста")

    async def cmd_scan(self, args, reply_chat_id: str):
        await self._reply(reply_chat_id, "🔍 Запускаю скан рынка...")
        try:
            if not self.scan_callback:
                await self._reply(reply_chat_id, "❌ scan_callback не настроен")
                return
            if self.state and self.state.is_paused:
                await self._reply(reply_chat_id, "⏸ Бот на паузе. Сначала /resume")
                return
            asyncio.create_task(self.scan_callback())
            await self._reply(reply_chat_id, "✅ Скан запущен!")
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка: {e}")

    async def cmd_pause(self, args, reply_chat_id: str):
        if self.state:
            self.state.is_paused = True
        await self._reply(reply_chat_id,
            "⏸ <b>Бот на паузе</b>\n\nНовых сигналов не будет.\n"
            "PositionTracker продолжает следить за открытыми позициями.\n"
            "/resume для возобновления.")

    async def cmd_resume(self, args, reply_chat_id: str):
        if self.state:
            self.state.is_paused = False
        await self._reply(reply_chat_id, "▶️ <b>Бот возобновил работу!</b>")

    async def cmd_set_min_score(self, args, reply_chat_id: str):
        try:
            if not args:
                score = getattr(self.config, "MIN_SCORE", 65) if self.config else 65
                await self._reply(reply_chat_id, f"⚙️ Текущий мин. скор: {score}%")
                return
            new_score = int(args[0])
            if not (40 <= new_score <= 100):
                await self._reply(reply_chat_id, "❌ Скор должен быть от 40 до 100")
                return
            if self.config:
                self.config.MIN_SCORE = new_score
            if self.state and self.state.scorer:
                self.state.scorer.min_score = new_score
            await self._reply(reply_chat_id, f"✅ Мин. скор установлен: {new_score}%")
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка: {e}")

    async def cmd_balance(self, args, reply_chat_id: str):
        if not (self.state and self.state.auto_trader):
            await self._reply(reply_chat_id, "❌ AutoTrader не инициализирован")
            return
        try:
            bal  = await self.state.auto_trader.bingx.get_account_balance()
            mode = "DEMO" if getattr(self.config, "BINGX_DEMO", True) else "REAL"
            if not bal:
                await self._reply(reply_chat_id, "❌ Не удалось получить баланс")
                return
            eq   = float(bal.get("equity", 0))
            avail = float(bal.get("availableMargin", 0))
            upnl  = float(bal.get("unrealizedPNL", 0))
            await self._reply(reply_chat_id,
                f"💳 <b>Баланс BingX [{mode}]</b>\n\n"
                f"💰 Equity:     <b>${eq:,.2f}</b>\n"
                f"✅ Available:  <b>${avail:,.2f}</b>\n"
                f"📊 uPNL:       <b>${upnl:+,.2f}</b>\n"
                f"🕐 {datetime.utcnow().strftime('%H:%M UTC')}")
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка: {e}")

    async def cmd_positions(self, args, reply_chat_id: str):
        if not (self.state and self.state.auto_trader):
            await self._reply(reply_chat_id, "❌ AutoTrader не инициализирован")
            return
        try:
            all_positions = await self.state.auto_trader.bingx.get_positions()
            mode = "DEMO" if getattr(self.config, "BINGX_DEMO", True) else "REAL"
            
            # ✅ FIX: Фильтруем позиции по стороне бота
            # SHORT бот видит только SHORT, LONG — только LONG
            expected_side = self.bot_type.upper()
            positions = [p for p in all_positions if (
                getattr(p, "position_side", "").upper() == expected_side or
                getattr(p, "side", "").upper() == expected_side or
                (expected_side == "SHORT" and getattr(p, "size", 0) < 0) or
                (expected_side == "LONG" and getattr(p, "size", 0) > 0)
            )]
            
            if not positions:
                await self._reply(reply_chat_id, f"📈 Нет открытых {expected_side} позиций [{mode}]")
                return
            msg = f"📈 <b>{expected_side} Позиции [{mode}] ({len(positions)}):</b>\n\n"
            total_upnl = 0.0
            for p in positions:
                d_emoji = "🟢" if p.side == "LONG" else "🔴"
                upnl    = p.unrealized_pnl
                total_upnl += upnl
                pnl_sign = "+" if upnl >= 0 else ""
                msg += (
                    f"{d_emoji} <b>#{p.symbol}</b> {p.side}\n"
                    f"   Вход: <b>{fmt_price(p.entry_price)}</b> | Размер: <b>{p.size}</b>\n"
                    f"   uPNL: <b>{pnl_sign}${upnl:.2f}</b> | Плечо: {p.leverage}x\n\n"
                )
            pnl_sign = "+" if total_upnl >= 0 else ""
            msg += f"💵 Итого uPNL: <b>{pnl_sign}${total_upnl:.2f}</b>"
            await self._reply(reply_chat_id, msg)
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка: {e}")

    async def cmd_closeall(self, args, reply_chat_id: str):
        if not (self.state and self.state.auto_trader):
            await self._reply(reply_chat_id, "❌ AutoTrader не инициализирован")
            return
        await self._reply(reply_chat_id, "⏳ Закрываю все позиции...")
        try:
            closed = await self.state.auto_trader.close_all_positions()
            await self._reply(reply_chat_id, f"✅ Закрыто позиций: {closed}")
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка: {e}")

    async def cmd_clearpos(self, args, reply_chat_id: str):
        try:
            if not self.redis:
                await self._reply(reply_chat_id, "❌ Redis недоступен")
                return
            import json as _json
            keys = self.redis.client.keys(f"{self.bot_type}:signals:*")
            cleared = 0
            for key in keys:
                signals = self.redis.client.lrange(key, 0, -1)
                for sig_json in signals:
                    try:
                        sig = _json.loads(sig_json)
                        if sig.get("status") == "active":
                            sig["status"] = "closed"
                            sig["closed_at"] = datetime.utcnow().isoformat()
                            self.redis.client.lset(key, 0, _json.dumps(sig))
                            cleared += 1
                            break
                    except Exception:
                        pass
            if self.state:
                self.state.active_signals = 0
            await self._reply(reply_chat_id,
                f"🗑 <b>Позиции сброшены</b>\n\n"
                f"Сброшено записей: {cleared}\n"
                f"Счётчик сигналов: 0\n\n"
                "⚠️ Реальные позиции на бирже НЕ закрыты. Используй /closeall")
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка: {e}")

    async def cmd_emergency_stop(self, args, reply_chat_id: str):
        await self._reply(reply_chat_id, "🛑 <b>Экстренная остановка...</b>")
        try:
            if self.state:
                self.state.is_paused = True
            closed = 0
            if self.state and self.state.auto_trader:
                closed = await self.state.auto_trader.close_all_positions()
            await self._reply(reply_chat_id,
                f"🛑 <b>Экстренная остановка выполнена!</b>\n\n"
                f"⏸ Бот поставлен на паузу\n"
                f"❌ Закрыто позиций: {closed}\n\n"
                "Используй /resume для возобновления.")
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка при остановке: {e}")

    async def cmd_reset_stats(self, args, reply_chat_id: str):
        """✅ Полный сброс статистики из Redis и памяти"""
        try:
            deleted_keys = []
            
            # Сброс в AutoTrader
            if self.state and self.state.auto_trader:
                self.state.auto_trader.daily_pnl    = 0.0
                self.state.auto_trader.daily_trades  = 0
                self.state.auto_trader.total_pnl     = 0.0
                self.state.auto_trader.win_count     = 0
                self.state.auto_trader.loss_count    = 0
                self.state.auto_trader.last_reset    = datetime.utcnow().date()
            
            # ✅ Полная очистка всех stats ключей в Redis
            if self.redis:
                # Все возможные stats ключи
                stats_patterns = [
                    f"{self.bot_type}:daily_trades",
                    f"{self.bot_type}:daily_pnl", 
                    f"{self.bot_type}:stats:*",
                    f"{self.bot_type}:history:*",
                    f"{self.bot_type}:pnl:*",
                ]
                
                for pattern in stats_patterns:
                    keys = self.redis.client.keys(pattern)
                    for key in keys:
                        self.redis.client.delete(key)
                        key_str = key.decode() if isinstance(key, bytes) else key
                        deleted_keys.append(key_str)
                
                # Дополнительно: удаляем по точным именам
                exact_keys = ["daily_trades", "daily_pnl", "total_trades", "total_pnl", "win_count", "loss_count"]
                for key in exact_keys:
                    self.redis.client.delete(f"{self.bot_type}:{key}")
                    deleted_keys.append(f"{self.bot_type}:{key}")
            
            await self._reply(reply_chat_id,
                f"🔄 <b>Статистика полностью сброшена</b>\n\n"
                f"🗑 Очищено ключей: {len(deleted_keys)}\n"
                f"✅ Счётчики обнулены\n"
                f"✅ Готов к новому старту!")
                
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка сброса: {e}")

    async def cmd_sync(self, args, reply_chat_id: str):
        """🔄 Синхронизация позиций с биржей BingX"""
        try:
            if not (self.state and self.state.auto_trader):
                await self._reply(reply_chat_id, "❌ AutoTrader не инициализирован")
                return
            
            await self._reply(reply_chat_id, "🔄 Синхронизация с биржей...")
            
            # Получаем реальные позиции с биржи
            positions = await self.state.auto_trader.bingx.get_positions()
            mode = "DEMO" if getattr(self.config, "BINGX_DEMO", True) else "REAL"
            
            if positions is None:
                await self._reply(reply_chat_id, f"❌ Не удалось подключиться к BingX [{mode}]")
                return
            
            # Синхронизируем с Redis
            synced = 0
            added = 0
            
            for pos in positions:
                symbol = pos.symbol
                redis_key = f"{self.bot_type}:positions:{symbol}"
                
                # Проверяем есть ли в Redis
                existing = self.redis.client.get(redis_key) if self.redis else None
                
                if not existing:
                    # Создаём запись в Redis для новой позиции
                    signal_data = {
                        "symbol": symbol,
                        "direction": pos.side.lower(),
                        "entry_price": pos.entry_price,
                        "stop_loss": pos.stop_loss or 0,
                        "take_profits": [],
                        "status": "active",
                        "size": pos.size,
                        "leverage": pos.leverage,
                        "created_at": datetime.utcnow().isoformat(),
                        "tp_level": 0,
                        "taken_tps": [],
                        "be_done": False,
                        "trailing_active": False,
                    }
                    if self.redis:
                        self.redis.client.setex(redis_key, 86400, json.dumps(signal_data))
                    added += 1
                else:
                    synced += 1
            
            # Обновляем счётчик активных сигналов
            if self.state:
                self.state.active_signals = len(positions)
            
            await self._reply(reply_chat_id,
                f"✅ <b>Синхронизация завершена [{mode}]</b>\n\n"
                f"📊 Позиций на бирже: {len(positions)}\n"
                f"➕ Добавлено в трекер: {added}\n"
                f"🔄 Уже в системе: {synced}\n"
                f"✅ Готово к отслеживанию!")
                
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка синхронизации: {e}")

    async def cmd_flushdb(self, args, reply_chat_id: str):
        """⚠️ Полная очистка базы данных Redis (ОПАСНО!)"""
        try:
            # Подтверждение требуется
            if not args or args[0].lower() not in ["yes", "confirm", "да"]:
                await self._reply(reply_chat_id,
                    f"⚠️ <b>ВНИМАНИЕ: Полная очистка Redis!</b>\n\n"
                    f"Это удалит ВСЕ данные бота:\n"
                    f"• Все сигналы\n"
                    f"• Всю статистику\n"
                    f"• Все позиции\n"
                    f"• Историю сделок\n\n"
                    f"Для подтверждения отправь:\n"
                    f"<code>/flushdb yes</code>")
                return
            
            if not self.redis:
                await self._reply(reply_chat_id, "❌ Redis не подключен")
                return
            
            # Получаем статистику перед удалением
            all_keys = self.redis.client.keys(f"{self.bot_type}:*")
            key_count = len(all_keys)
            
            # Удаляем все ключи бота
            deleted = 0
            for key in all_keys:
                self.redis.client.delete(key)
                deleted += 1
            
            # Сбрасываем состояние
            if self.state:
                self.state.active_signals = 0
                self.state.is_paused = False
                if self.state.auto_trader:
                    self.state.auto_trader.daily_pnl = 0.0
                    self.state.auto_trader.daily_trades = 0
            
            await self._reply(reply_chat_id,
                f"🗑 <b>База данных очищена!</b>\n\n"
                f"🗑 Удалено ключей: {deleted}\n"
                f"✅ Все данные сброшены\n"
                f"🔄 Бот перезагружен\n\n"
                f"<b>Используйте /sync чтобы синхронизировать позиции с биржей</b>")
                
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка очистки: {e}")

    async def cmd_cleanup(self, args, reply_chat_id: str):
        try:
            real_positions = []
            if self.state and self.state.auto_trader:
                real_positions = await self.state.auto_trader.bingx.get_positions()
            real_symbols = {p.symbol for p in real_positions}

            cleaned = 0
            keys = self.redis.client.keys(f"{self.bot_type}:signals:*") if self.redis else []
            for key in keys:
                sym = (key.decode() if isinstance(key, bytes) else key).split(":")[-1]
                if sym not in real_symbols:
                    self.redis.client.delete(key)
                    cleaned += 1

            if self.state:
                self.state.active_signals = len(real_positions)

            await self._reply(reply_chat_id,
                f"🧹 <b>Cleanup завершён</b>\n\n"
                f"📊 Реальных позиций: {len(real_positions)}\n"
                f"🗑 Очищено из Redis: {cleaned}\n"
                f"✅ Активных сигналов: {len(real_positions)}")
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка: {e}")

    async def cmd_clean(self, args, reply_chat_id: str):
        try:
            if self.state and self.state.auto_trader:
                self.state.auto_trader.daily_trades = 0
                self.state.auto_trader.daily_pnl    = 0.0
            keys = self.redis.client.keys(f"{self.bot_type}:signals:*") if self.redis else []
            cnt  = len(keys)
            for key in keys:
                self.redis.client.delete(key)
            if self.redis:
                self.redis.client.delete(f"{self.bot_type}:daily_trades")
                self.redis.client.delete(f"{self.bot_type}:daily_pnl")
            if self.state:
                self.state.active_signals = 0
                self.state.is_paused = False
            await self._reply(reply_chat_id,
                f"🧼 <b>Полная очистка</b>\n\n"
                f"🗑 Сигналов: {cnt}\n📊 Статистика: сброшена\n✅ Готов к работе")
        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка: {e}")

    async def cmd_logs(self, args, reply_chat_id: str):
        uptime_str = "N/A"
        if self.state and hasattr(self.state, "start_time") and self.state.start_time:
            delta = datetime.utcnow() - self.state.start_time
            h = delta.seconds // 3600
            m = (delta.seconds % 3600) // 60
            uptime_str = f"{delta.days}д {h}ч {m}м"
        await self._reply(reply_chat_id,
            f"📋 <b>Статус бота</b>\n\n"
            f"🤖 Тип: {self.bot_type.upper()}\n"
            f"⏸️ Пауза: {'Да' if self.state and self.state.is_paused else 'Нет'}\n"
            f"📊 Активных сигналов: {getattr(self.state, 'active_signals', 0)}\n"
            f"🔄 AutoTrader: {'✅' if self.state and self.state.auto_trader else '❌'}\n"
            f"⏱ Uptime: {uptime_str}\n\n"
            "💡 Полные логи: Render Dashboard → Logs")


    async def cmd_daily_report(self, args, reply_chat_id: str):
        """📅 /daily_rep — дневной отчёт."""
        date   = datetime.utcnow().strftime("%Y-%m-%d")
        trades = self._get_trade_history(days=1)

        if not trades:
            await self._reply(reply_chat_id,
                f"📅 <b>Дневной отчёт {date}</b>\n\nСделок нет.")
            return

        wins   = [t for t in trades if t.get("pnl", 0) > 0]
        losses = [t for t in trades if t.get("pnl", 0) <= 0]
        total  = len(trades)
        wr     = round(len(wins) / total * 100, 1) if total else 0
        pnl    = sum(t.get("pnl", 0) for t in trades)

        durations = []
        for t in trades:
            try:
                opened = datetime.fromisoformat(t.get("opened_at", ""))
                closed = datetime.fromisoformat(t.get("closed_at", ""))
                durations.append((closed - opened).total_seconds())
            except Exception:
                pass
        avg_dur = self._duration_str(sum(durations) / len(durations)) if durations else "N/A"

        # TP breakdown
        tp_counts: Dict[str, int] = {}
        for t in wins:
            tp_lvl = t.get("tp_level", "TP?")
            tp_counts[tp_lvl] = tp_counts.get(tp_lvl, 0) + 1
        tp_lines = ""
        for tp, cnt in sorted(tp_counts.items()):
            bar = "█" * cnt
            tp_lines += f"  {tp}: {cnt} ✅  {bar}\n"

        # Последние сделки (до 5)
        last_trades = ""
        for t in sorted(trades, key=lambda x: x.get("closed_at",""), reverse=True)[:5]:
            sym  = t.get("symbol", "?")
            side = t.get("direction", "?").upper()
            pnl_ = t.get("pnl", 0)
            tp_l = t.get("tp_level", "SL")
            ico  = "✅" if pnl_ > 0 else "❌"
            try:
                dur_s = (datetime.fromisoformat(t.get("closed_at","")) -
                         datetime.fromisoformat(t.get("opened_at",""))).total_seconds()
                dur_str = self._duration_str(dur_s)
            except Exception:
                dur_str = "?"
            last_trades += f"{ico} <b>#{sym}</b> {side} → {tp_l} ({dur_str})\n"

        wr_emoji = self._wr_emoji(wr)
        msg = (
            f"📅 <b>Дневной отчёт {date}</b>\n\n"
            f"📊 Win Rate: {wr_emoji} {wr}%\n"
            f"✅ TP: {len(wins)}   ❌ SL: {len(losses)}\n"
            f"📈 Всего закрыто: {total}\n"
            f"💵 P&L: <b>{pnl:+.2f}%</b>\n"
            f"⏱ Ср. время: {avg_dur}\n"
        )
        if tp_lines:
            msg += f"\n<b>Разбивка по TP:</b>\n{tp_lines}"
        if last_trades:
            msg += f"\n<b>Последние сделки:</b>\n{last_trades}"
        await self._reply(reply_chat_id, msg)


    async def cmd_alltradestat(self, args, reply_chat_id: str):
        """📊 /alltradestat — полная статистика всех сделок для анализа."""
        try:
            all_key = f"{self.bot_type}:all_trades"
            raw_list = self.redis.client.lrange(all_key, 0, 999)
            if not raw_list:
                await self._reply(reply_chat_id,
                    "📊 <b>Полная статистика</b>\n\nСделок ещё нет.")
                return

            trades = []
            for r in raw_list:
                try:
                    trades.append(json.loads(r))
                except Exception:
                    continue

            if not trades:
                await self._reply(reply_chat_id, "Нет данных.")
                return

            total = len(trades)
            wins   = [t for t in trades if t.get("pnl", 0) > 0]
            losses = [t for t in trades if t.get("pnl", 0) < 0]
            be_tr  = [t for t in trades if t.get("tp_level") == "BE"]
            wr     = round(len(wins) / total * 100, 1) if total else 0
            pnl    = round(sum(t.get("pnl", 0) for t in trades), 2)

            # TP распределение
            tp_dist = {}
            for t in trades:
                lv = t.get("tp_level", "?")
                tp_dist[lv] = tp_dist.get(lv, 0) + 1

            # Лучшие / худшие паттерны
            pattern_stats = {}
            for t in trades:
                p = t.get("pattern") or "Unknown"
                if p not in pattern_stats:
                    pattern_stats[p] = {"n": 0, "wins": 0, "pnl": 0}
                pattern_stats[p]["n"] += 1
                if t.get("pnl", 0) > 0:
                    pattern_stats[p]["wins"] += 1
                pattern_stats[p]["pnl"] += t.get("pnl", 0)

            # Среднее время
            holds = [t.get("hold_minutes", 0) for t in trades if t.get("hold_minutes")]
            avg_hold = round(sum(holds) / len(holds)) if holds else 0

            # Худший / лучший трейд
            best  = max(trades, key=lambda t: t.get("pnl", 0))
            worst = min(trades, key=lambda t: t.get("pnl", 0))

            # Max consecutive losses
            max_loss_streak = streak = 0
            for t in sorted(trades, key=lambda t: t.get("opened_at", "")):
                if t.get("pnl", 0) < 0:
                    streak += 1
                    max_loss_streak = max(max_loss_streak, streak)
                else:
                    streak = 0

            # Score stats
            scores = [t.get("score", 0) for t in trades if t.get("score", 0) > 0]
            avg_score = round(sum(scores) / len(scores)) if scores else 0
            score_wins = [t.get("score",0) for t in wins if t.get("score",0) > 0]
            avg_score_win = round(sum(score_wins)/len(score_wins)) if score_wins else 0

            msg = (
                f"📊 <b>ПОЛНАЯ СТАТИСТИКА {self.bot_type.upper()} БОТА</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📈 <b>ОБЩЕЕ:</b>\n"
                f"   Сделок: {total}\n"
                f"   ✅ Прибыльных: {len(wins)} ({wr}%)\n"
                f"   ❌ Убыточных: {len(losses)} ({round(len(losses)/total*100,1)}%)\n"
                f"   ⚖️ BE: {len(be_tr)}\n"
                f"   💰 Итого P&L: {pnl:+.2f}%\n"
                f"   ⏱ Avg время: {avg_hold//60}ч {avg_hold%60}м\n"
                f"   🔴 Макс. серия SL: {max_loss_streak}\n\n"
                f"🎯 <b>РАСПРЕДЕЛЕНИЕ TP/SL:</b>\n"
            )
            for lv in sorted(tp_dist.keys()):
                cnt = tp_dist[lv]
                pct = round(cnt / total * 100, 1)
                bar = "█" * max(1, int(pct / 5))
                msg += f"   {lv:>4}: {cnt:>4} ({pct:>5.1f}%)  {bar}\n"

            msg += f"\n📋 <b>ПО ПАТТЕРНАМ:</b>\n"
            for pname, ps in sorted(pattern_stats.items(),
                                    key=lambda x: x[1]["n"], reverse=True)[:8]:
                if ps["n"] == 0:
                    continue
                p_wr = round(ps["wins"] / ps["n"] * 100)
                msg += f"   {pname[:20]:<20}: {ps['n']:>3} сд | WR {p_wr}% | {ps['pnl']:+.2f}%\n"

            msg += (
                f"\n🏆 <b>ЛУЧШАЯ СДЕЛКА:</b>\n"
                f"   {best.get('symbol')} {best.get('direction','').upper()} "
                f"→ {best.get('tp_level')} | P&L: {best.get('pnl',0):+.2f}%\n"
                f"   Score: {best.get('score',0)} | {best.get('pattern','')}\n\n"
                f"☠️ <b>ХУДШАЯ СДЕЛКА:</b>\n"
                f"   {worst.get('symbol')} {worst.get('direction','').upper()} "
                f"→ {worst.get('tp_level')} | P&L: {worst.get('pnl',0):+.2f}%\n"
                f"   Score: {worst.get('score',0)} | {worst.get('pattern','')}\n\n"
                f"🔬 <b>КАЧЕСТВО СИГНАЛОВ:</b>\n"
                f"   Avg Score всех: {avg_score}\n"
                f"   Avg Score прибыльных: {avg_score_win}\n"
            )
            await self._reply(reply_chat_id, msg)

            # Детальный лог последних 5 сделок
            await asyncio.sleep(0.3)
            detail_msg = "📝 <b>ПОСЛЕДНИЕ 5 СДЕЛОК (детали):</b>\n" + "─"*30 + "\n"
            for t in trades[:5]:
                ico = "✅" if t.get("pnl",0) > 0 else ("⚖️" if t.get("tp_level")=="BE" else "❌")
                detail_msg += (
                    f"\n{ico} <b>#{t.get('symbol')}</b> {t.get('direction','').upper()} "
                    f"→ <b>{t.get('tp_level','?')}</b>\n"
                    f"   Вход: ${t.get('entry_price',0):,.4f}\n"
                    f"   Выход: ${t.get('close_price',0):,.4f}\n"
                    f"   P&L: <b>{t.get('pnl',0):+.3f}%</b> | Время: {t.get('hold_minutes',0)//60}ч {t.get('hold_minutes',0)%60}м\n"
                    f"   Score: {t.get('score',0)} | Паттерн: {t.get('pattern','?')}\n"
                    f"   RSI: {t.get('rsi_1h',0):.1f} | FR: {t.get('funding_rate',0):.4f}%\n"
                    f"   OI: {t.get('oi_change',0):+.1f}% | L/S: {t.get('long_short_ratio',0):.0f}%\n"
                    f"   Vol spike: {t.get('volume_spike',0):.1f}x | ATR: {t.get('atr_pct',0):.2f}%\n"
                )
                reasons = t.get("reasons", [])
                if reasons:
                    detail_msg += f"   Причины: {' | '.join(str(r) for r in reasons[:3])}\n"
            await self._reply(reply_chat_id, detail_msg)

        except Exception as e:
            await self._reply(reply_chat_id, f"❌ Ошибка alltradestat: {e}")

    async def cmd_weekly_report(self, args, reply_chat_id: str):
        """📅 /weekly_rep — недельный отчёт."""
        now   = datetime.utcnow()
        start = now - timedelta(days=7)
        trades = self._get_trade_history(days=8)

        if not trades:
            await self._reply(reply_chat_id,
                f"📅 <b>Недельный отчёт</b>\n\nСделок нет.")
            return

        wins   = [t for t in trades if t.get("pnl", 0) > 0]
        losses = [t for t in trades if t.get("pnl", 0) <= 0]
        total  = len(trades)
        wr     = round(len(wins) / total * 100, 1) if total else 0
        pnl    = sum(t.get("pnl", 0) for t in trades)

        durations = []
        for t in trades:
            try:
                o = datetime.fromisoformat(t.get("opened_at",""))
                c = datetime.fromisoformat(t.get("closed_at",""))
                durations.append((c - o).total_seconds())
            except Exception:
                pass
        avg_dur = self._duration_str(sum(durations) / len(durations)) if durations else "N/A"

        # Активных дней
        active_days = len(set(
            t.get("closed_at","")[:10] for t in trades if t.get("closed_at","")
        ))

        # Топ тикеры по количеству
        sym_count: Dict[str, int] = {}
        for t in trades:
            sym = t.get("symbol", "?")
            sym_count[sym] = sym_count.get(sym, 0) + 1
        top_syms = sorted(sym_count.items(), key=lambda x: x[1], reverse=True)[:5]
        top_lines = "\n".join(f"  <b>#{sym}</b>: {cnt}" for sym, cnt in top_syms)

        wr_emoji = self._wr_emoji(wr)
        date_from = start.strftime("%d.%m")
        date_to   = now.strftime("%d.%m.%Y")
        msg = (
            f"📅 <b>Недельный отчёт</b>\n"
            f"с {date_from} по {date_to}\n\n"
            f"📊 Win Rate: {wr_emoji} {wr}%\n"
            f"✅ TP: {len(wins)}   ❌ SL: {len(losses)}\n"
            f"📈 Всего закрыто: {total}\n"
            f"💵 P&L: <b>{pnl:+.2f}%</b>\n"
            f"⏱ Ср. время: {avg_dur}\n"
            f"📅 Активных дней: {active_days}\n"
        )
        if top_syms:
            msg += f"\n<b>Топ тикеры:</b>\n{top_lines}"
        await self._reply(reply_chat_id, msg)

    async def cmd_monthly_report(self, args, reply_chat_id: str):
        """📅 /monthly_rep — месячный отчёт."""
        now    = datetime.utcnow()
        trades = self._get_trade_history(days=31)

        if not trades:
            await self._reply(reply_chat_id,
                f"📅 <b>Месячный отчёт</b>\n\nСделок нет.")
            return

        wins   = [t for t in trades if t.get("pnl", 0) > 0]
        losses = [t for t in trades if t.get("pnl", 0) <= 0]
        total  = len(trades)
        wr     = round(len(wins) / total * 100, 1) if total else 0
        pnl    = sum(t.get("pnl", 0) for t in trades)

        durations = []
        for t in trades:
            try:
                o = datetime.fromisoformat(t.get("opened_at",""))
                c = datetime.fromisoformat(t.get("closed_at",""))
                durations.append((c - o).total_seconds())
            except Exception:
                pass
        avg_dur = self._duration_str(sum(durations) / len(durations)) if durations else "N/A"

        active_days = len(set(
            t.get("closed_at","")[:10] for t in trades if t.get("closed_at","")
        ))

        # TP breakdown
        tp_counts: Dict[str, int] = {}
        for t in wins:
            tp_lvl = t.get("tp_level", "TP?")
            tp_counts[tp_lvl] = tp_counts.get(tp_lvl, 0) + 1
        tp_lines = ""
        for tp, cnt in sorted(tp_counts.items()):
            bar = "█" * min(cnt, 10)
            tp_lines += f"  {tp}: {cnt} ✅  {bar}\n"

        # Топ тикеры
        sym_count: Dict[str, int] = {}
        for t in trades:
            sym = t.get("symbol","?")
            sym_count[sym] = sym_count.get(sym, 0) + 1
        top_syms = sorted(sym_count.items(), key=lambda x: x[1], reverse=True)[:5]
        top_lines = "\n".join(f"  <b>#{sym}</b>: {cnt}" for sym, cnt in top_syms)

        wr_emoji = self._wr_emoji(wr)
        month_name = now.strftime("%B %Y")
        msg = (
            f"📅 <b>Месячный отчёт — {month_name}</b>\n\n"
            f"📊 Win Rate: {wr_emoji} {wr}%\n"
            f"✅ TP: {len(wins)}   ❌ SL: {len(losses)}\n"
            f"📈 Всего закрыто: {total}\n"
            f"💵 P&L: <b>{pnl:+.2f}%</b>\n"
            f"⏱ Ср. время: {avg_dur}\n"
            f"📅 Активных дней: {active_days}\n"
        )
        if tp_lines:
            msg += f"\n<b>Разбивка по TP:</b>\n{tp_lines}"
        if top_syms:
            msg += f"\n<b>Топ тикеры:</b>\n{top_lines}"
        await self._reply(reply_chat_id, msg)

    async def cmd_leaders_wr(self, args, reply_chat_id: str):
        """🏆 /leaderswr — топ пар по Win Rate (мин. 2 сделки)."""
        trades = self._get_trade_history(days=30)

        if not trades:
            await self._reply(reply_chat_id, "🏆 Данных для рейтинга нет.")
            return

        # Группируем по символу
        by_sym: Dict[str, Dict] = {}
        for t in trades:
            sym = t.get("symbol","?")
            if sym not in by_sym:
                by_sym[sym] = {"wins":0,"losses":0,"tps":{}}
            pnl = t.get("pnl", 0)
            tp_lvl = t.get("tp_level","")
            if pnl > 0:
                by_sym[sym]["wins"] += 1
                if tp_lvl:
                    by_sym[sym]["tps"][tp_lvl] = by_sym[sym]["tps"].get(tp_lvl, 0) + 1
            else:
                by_sym[sym]["losses"] += 1

        # Фильтр: мин. 2 сделки
        stats = []
        for sym, d in by_sym.items():
            total = d["wins"] + d["losses"]
            if total < 2:
                continue
            wr = round(d["wins"] / total * 100, 1)
            stats.append({"sym": sym, "wr": wr, "wins": d["wins"],
                          "losses": d["losses"], "total": total, "tps": d["tps"]})

        stats.sort(key=lambda x: (x["wr"], x["wins"]), reverse=True)

        medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

        # Лучшие (топ 5)
        best_lines = ""
        for i, s in enumerate(stats[:5]):
            tp_summary = " ".join(f"TP{k.replace('TP','')}×{v}"
                                   for k,v in sorted(s["tps"].items()))
            medal = medals[i] if i < len(medals) else "▪️"
            best_lines += (
                f"{medal} <b>#{s['sym']}</b> — WR {s['wr']}%"
                f" ({s['wins']}W/{s['losses']}L из {s['total']})\n"
                + (f"   {tp_summary}\n" if tp_summary else "")
            )

        # Худшие (низший WR, мин. 2 сделки)
        worst = [s for s in stats if s["wr"] < 60]
        worst.sort(key=lambda x: x["wr"])
        worst_lines = ""
        for s in worst[:5]:
            worst_lines += (
                f"🔻 <b>#{s['sym']}</b> — WR {s['wr']}%"
                f" ({s['wins']}W/{s['losses']}L)\n"
            )

        # Общий WR
        total_all   = sum(s["total"] for s in stats)
        total_wins  = sum(s["wins"]  for s in stats)
        overall_wr  = round(total_wins / total_all * 100, 1) if total_all else 0
        wr_emoji    = self._wr_emoji(overall_wr)

        now = datetime.utcnow()
        start = (now - timedelta(days=30)).strftime("%d.%m")
        end   = now.strftime("%d.%m")

        msg = (
            f"📊 <b>Топ пар по Win Rate</b>\n"
            f"с {start} по {end}\n\n"
        )
        if best_lines:
            msg += f"<b>🏆 Лучшие тикеры:</b>\n{best_lines}\n"
        if worst_lines:
            msg += f"<b>📉 Худшие тикеры:</b>\n{worst_lines}\n"

        msg += (
            f"Всего тикеров: {len(stats)} | Сделок: {total_all}\n"
            f"📊 Общий WR: {wr_emoji} <b>{overall_wr}%</b>"
            f"  ({total_wins}✅ / {total_all-total_wins}❌)"
        )
        await self._reply(reply_chat_id, msg)


# ============================================================================
# DUAL BOT MANAGER
# ============================================================================

class DualTelegramManager:
    def __init__(self,
                 short_bot_token=None, short_chat_id=None, short_topic_id=None,
                 long_bot_token=None,  long_chat_id=None,  long_topic_id=None):
        self.short_bot = TelegramBot(
            bot_token=short_bot_token or os.getenv("SHORT_TELEGRAM_BOT_TOKEN"),
            chat_id=short_chat_id     or os.getenv("SHORT_TELEGRAM_CHAT_ID"),
            topic_id=short_topic_id   or os.getenv("SHORT_TELEGRAM_TOPIC_ID"),
        )
        self.long_bot = TelegramBot(
            bot_token=long_bot_token or os.getenv("LONG_TELEGRAM_BOT_TOKEN"),
            chat_id=long_chat_id     or os.getenv("LONG_TELEGRAM_CHAT_ID"),
            topic_id=long_topic_id   or os.getenv("LONG_TELEGRAM_TOPIC_ID"),
        )

    async def send_signal(self, direction: str, **kwargs) -> Optional[int]:
        if direction == "short":
            return await self.short_bot.send_signal(direction="short", **kwargs)
        return await self.long_bot.send_signal(direction="long", **kwargs)

    async def test_connections(self) -> Dict[str, bool]:
        return {
            "short": await self.short_bot.send_test_message(),
            "long":  await self.long_bot.send_test_message(),
        }

    async def close(self):
        await self.short_bot.close()
        await self.long_bot.close()


# ============================================================================
# 🆕 AUTO-REPORTS SCHEDULER
# ============================================================================

class ReportScheduler:
    """
    🆕 Планировщик автоматических отчётов в Telegram.
    
    Отправляет:
    - 📅 Ежедневный отчёт в 23:59 МСК (20:59 UTC)
    - 📅 Еженедельный отчёт в воскресенье 23:59 МСК  
    - 📅 Ежемесячный отчёт в последний день месяца 23:59 МСК
    """
    
    def __init__(self, telegram_bot: TelegramBot, chat_id: str):
        self.bot = telegram_bot
        self.chat_id = chat_id
        self._running = False
        self._task = None
    
    async def start(self):
        """Запускает планировщик"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())
        print("🕐 Report scheduler started (Daily: 23:59 MSK, Weekly: Sun, Monthly: Last day)")
    
    async def stop(self):
        """Останавливает планировщик"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _scheduler_loop(self):
        """Главный цикл планировщика"""
        while self._running:
            try:
                now = datetime.utcnow()
                
                # 🕐 Проверяем время для отчётов (20:59 UTC = 23:59 МСК)
                if now.hour == 20 and now.minute == 59:
                    # 📅 Ежедневный отчёт (каждый день)
                    await self._send_daily_report()
                    
                    # 📅 Еженедельный (воскресенье = weekday 6)
                    if now.weekday() == 6:
                        await self._send_weekly_report()
                    
                    # 📅 Ежемесячный (последний день месяца)
                    if self._is_last_day_of_month(now):
                        await self._send_monthly_report()
                    
                    # Ждём 2 минуты чтобы не отправить дважды
                    await asyncio.sleep(120)
                else:
                    # Проверяем каждую минуту
                    await asyncio.sleep(60)
                    
            except Exception as e:
                print(f"⚠️ Scheduler error: {e}")
                await asyncio.sleep(60)
    
    def _is_last_day_of_month(self, dt: datetime) -> bool:
        """Проверяет, является ли день последним в месяце"""
        # Последний день = завтра будет другой месяц
        tomorrow = dt + timedelta(days=1)
        return tomorrow.month != dt.month
    
    async def _send_daily_report(self):
        """Отправляет ежедневный отчёт"""
        try:
            print(f"📅 Sending daily report to {self.chat_id}")
            await self.bot.cmd_daily_report("", self.chat_id)
        except Exception as e:
            print(f"⚠️ Daily report error: {e}")
    
    async def _send_weekly_report(self):
        """Отправляет еженедельный отчёт"""
        try:
            print(f"📅 Sending weekly report to {self.chat_id}")
            await self.bot.cmd_weekly_report("", self.chat_id)
        except Exception as e:
            print(f"⚠️ Weekly report error: {e}")
    
    async def _send_monthly_report(self):
        """Отправляет ежемесячный отчёт"""
        try:
            print(f"📅 Sending monthly report to {self.chat_id}")
            await self.bot.cmd_monthly_report("", self.chat_id)
        except Exception as e:
            print(f"⚠️ Monthly report error: {e}")


# ============================================================================
# SINGLETON + SCHEDULER INIT
# ============================================================================

_telegram_bot = None
_report_scheduler = None

def get_telegram_bot() -> TelegramBot:
    global _telegram_bot
    if _telegram_bot is None:
        _telegram_bot = TelegramBot()
    return _telegram_bot


def start_report_scheduler(chat_id: str):
    """🆕 Запускает планировщик отчётов"""
    global _report_scheduler
    if _report_scheduler is None:
        bot = get_telegram_bot()
        _report_scheduler = ReportScheduler(bot, chat_id)
        asyncio.create_task(_report_scheduler.start())


def stop_report_scheduler():
    """🆕 Останавливает планировщик отчётов"""
    global _report_scheduler
    if _report_scheduler:
        asyncio.create_task(_report_scheduler.stop())
        _report_scheduler = None
