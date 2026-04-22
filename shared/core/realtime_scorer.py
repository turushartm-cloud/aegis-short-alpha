"""
RealtimeScorer — shared/core/realtime_scorer.py

Добавляет баллы за аномалии ПРЯМО СЕЙЧАС (стиль скринера):
  • OI trend 15m         → до +15
  • Taker buy/sell ratio → до +10
  • Top trader L/S       → до +10
  • Ликвидации           → до +10
  • Объём spike          → до +5
  • CoinGecko trending   → до +5
  Max бонус: +55 баллов (поверх основного scorer)

Также генерирует EARLY сигналы (score 45–64) — watchlist без сделки.

─────────────────────────────────────────────────────────────────────
Использование в scan_symbol() (main.py):

    from core.realtime_scorer import RealtimeScorer, EarlySignal

    rt = RealtimeScorer()

    # После основного score_result:
    rt_result = await rt.score(
        direction   = "long",           # "long" | "short"
        market_data = market_data,      # MarketData из binance_client
        base_score  = score_result.total_score,
        hourly_deltas = hourly_deltas,
    )

    final_score = rt_result.final_score

    if rt_result.early_only:
        # Слабый сигнал — только Telegram, без сделки
        await state.telegram.send_early_signal(...)
        continue                        # не открываем позицию

    signal["realtime_bonus"]  = rt_result.bonus
    signal["realtime_factors"] = rt_result.factors
─────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Any, NamedTuple, Dict, Set
from datetime import datetime, timedelta
import aiohttp
import asyncio

# 🆕 Multi-timeframe support
from .multi_timeframe_detector import (
    MultiTimeframeDetector,
    Timeframe,
    AggregatedSignal,
    format_multi_tf_message
)

# ─────────────────────────────────────────────────────────────────────────────
# RESULT TYPES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RealtimeResult:
    """Результат realtime-скоринга."""
    base_score:   int
    bonus:        int           # дополнительные баллы
    final_score:  int           # base_score + bonus, cap 100
    factors:      List[str]     # что сработало
    early_only:   bool          # True = только EARLY сигнал, без сделки
    confidence:   str           # EXTREME / HIGH / MEDIUM / LOW


@dataclass
class EarlySignal:
    """Ранний сигнал для Telegram (без открытия позиции)."""
    symbol:    str
    direction: str
    score:     int
    price:     float
    factors:   List[str]


# ─────────────────────────────────────────────────────────────────────────────
# REALTIME SCORER
# ─────────────────────────────────────────────────────────────────────────────

class RealtimeScorer:
    """
    Добавляет баллы за рыночные аномалии прямо сейчас.
    Не имеет состояния — создавай один раз и переиспользуй.
    """

    # Пороги для EARLY сигналов
    EARLY_MIN_SCORE   = 45     # ниже — игнорируем
    TRADE_MIN_SCORE   = 65     # выше — открываем сделку

    # Пороги ликвидаций
    LIQ_MEDIUM   = 300_000     # $300k
    LIQ_LARGE    = 1_000_000   # $1M
    LIQ_EXTREME  = 5_000_000   # $5M

    # 🆕 CoinGecko trending cache
    _trending_cache: Set[str] = set()
    _trending_last_update: Optional[datetime] = None
    _trending_ttl_seconds: int = 300  # 5 минут кэш

    async def _fetch_trending_symbols(self) -> Set[str]:
        """
        Получить топ-7 трендовых монет с CoinGecko.
        Кэшируется на 5 минут для снижения нагрузки.
        """
        now = datetime.utcnow()

        # Проверяем кэш
        if (self._trending_last_update and
            self._trending_cache and
            (now - self._trending_last_update).seconds < self._trending_ttl_seconds):
            return self._trending_cache

        try:
            # trust_env=False — игнорируем прокси для CoinGecko (избегаем TLS in TLS)
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
                async with session.get(
                    "https://api.coingecko.com/api/v3/search/trending",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        coins = data.get("coins", [])
                        # Извлекаем символы (coin_id → symbol через search)
                        symbols = set()
                        for coin in coins[:7]:  # топ-7
                            item = coin.get("item", {})
                            symbol = item.get("symbol", "").upper()
                            if symbol:
                                symbols.add(symbol)

                        RealtimeScorer._trending_cache = symbols
                        RealtimeScorer._trending_last_update = now
                        return symbols
        except Exception:
            pass

        return set()

    async def score(
        self,
        direction:     str,
        market_data:   Any,           # MarketData из binance_client
        base_score:    int,
        hourly_deltas: List[float],
        trending_symbols: Optional[Set[str]] = None,
    ) -> RealtimeResult:
        """
        Рассчитать realtime бонус.

        Args:
            direction:     "long" | "short"
            market_data:   MarketData (price, taker_buy_sell_ratio, etc.)
            base_score:    результат основного scorer (0–100)
            hourly_deltas: список дельт по часам (из binance_client)
            trending_symbols: CoinGecko trending symbols (optional)
        """
        bonus   = 0
        factors: List[str] = []

        # 🆕 CoinGecko trending bonus (до +5)
        if trending_symbols is None:
            trending_symbols = await self._fetch_trending_symbols()

        symbol_base = getattr(market_data, "symbol", "").replace("USDT", "").upper()
        if symbol_base in trending_symbols:
            bonus += 5
            factors.append(f"🔥 {symbol_base} в CoinGecko топ-7 trending")

        # ── 1. OI trend 15m (до +15) ──────────────────────────────────────
        oi_trend = getattr(market_data, "oi_trend", None)
        if direction == "long":
            if oi_trend == "growing":
                bonus += 10
                factors.append("OI растёт 15m (позиции накапливаются)")
            elif oi_trend == "flat":
                bonus += 3
        else:  # short
            if oi_trend == "shrinking":
                bonus += 10
                factors.append("OI падает 15m (позиции закрываются, давление)")
            elif oi_trend == "growing":
                bonus += 8
                factors.append("OI растёт при падении (шорты перегружаются)")
            elif oi_trend == "flat":
                bonus += 3

        # ── 2. Taker buy/sell ratio (до +10) ─────────────────────────────
        # 0.0 = все продают агрессивно, 1.0 = все покупают агрессивно
        taker = getattr(market_data, "taker_buy_sell_ratio", None)
        if taker is not None:
            if direction == "long":
                if taker >= 0.65:
                    bonus += 10
                    factors.append(f"Taker ratio {taker:.2f} — агрессивные покупатели")
                elif taker >= 0.55:
                    bonus += 5
                    factors.append(f"Taker ratio {taker:.2f} — покупки преобладают")
                elif taker <= 0.35:
                    bonus -= 5   # сильные продажи — против лонга
            else:  # short
                if taker <= 0.35:
                    bonus += 10
                    factors.append(f"Taker ratio {taker:.2f} — агрессивные продавцы")
                elif taker <= 0.45:
                    bonus += 5
                    factors.append(f"Taker ratio {taker:.2f} — продажи преобладают")
                elif taker >= 0.65:
                    bonus -= 5

        # ── 3. Top trader L/S ratio (до +10) ─────────────────────────────
        # Крупные игроки умнее толпы → используем контрарно
        top_ls = getattr(market_data, "top_trader_long_short_ratio", None)
        if top_ls is not None:
            if direction == "long":
                # Топ-трейдеры в шортах = крупный short squeeze потенциал
                if top_ls < 0.8:
                    bonus += 10
                    factors.append(f"Top traders в шортах ({top_ls:.2f}) — squeeze риск")
                elif top_ls < 0.95:
                    bonus += 5
                    factors.append(f"Top traders перевес шортов ({top_ls:.2f})")
                elif top_ls > 1.5:
                    bonus -= 3   # топы в лонгах = переполненность
            else:  # short
                # Топ-трейдеры в лонгах = лонги переполнены = хорошо для шорта
                if top_ls > 1.5:
                    bonus += 10
                    factors.append(f"Top traders в лонгах ({top_ls:.2f}) — переполненность")
                elif top_ls > 1.2:
                    bonus += 5
                    factors.append(f"Top traders перевес лонгов ({top_ls:.2f})")
                elif top_ls < 0.8:
                    bonus -= 3

        # ── 4. Ликвидации (до +10) ───────────────────────────────────────
        liq_usd  = getattr(market_data, "recent_liquidations_usd", None) or 0.0
        liq_side = getattr(market_data, "liq_side", None)  # "LONG" | "SHORT" | None

        if liq_usd and liq_side:
            if direction == "long" and liq_side == "SHORT":
                # Шорты ликвидируют → short squeeze → хорошо для лонга
                if liq_usd >= self.LIQ_EXTREME:
                    bonus += 10
                    factors.append(f"🔥 КОРОТКИЕ ликвидации ${liq_usd/1e6:.1f}M — short squeeze!")
                elif liq_usd >= self.LIQ_LARGE:
                    bonus += 7
                    factors.append(f"Short squeeze ${liq_usd/1e6:.1f}M ликвидаций")
                elif liq_usd >= self.LIQ_MEDIUM:
                    bonus += 4
                    factors.append(f"Short ликвидации ${liq_usd/1e3:.0f}k")

            elif direction == "short" and liq_side == "LONG":
                # Лонги ликвидируют → обвал → хорошо для шорта
                if liq_usd >= self.LIQ_EXTREME:
                    bonus += 10
                    factors.append(f"🔥 ДЛИННЫЕ ликвидации ${liq_usd/1e6:.1f}M — каскад!")
                elif liq_usd >= self.LIQ_LARGE:
                    bonus += 7
                    factors.append(f"Long ликвидации ${liq_usd/1e6:.1f}M — давление шортов")
                elif liq_usd >= self.LIQ_MEDIUM:
                    bonus += 4
                    factors.append(f"Long ликвидации ${liq_usd/1e3:.0f}k")

            # Ликвидации ПРОТИВ нашего направления — слабый негатив
            elif direction == "long" and liq_side == "LONG" and liq_usd >= self.LIQ_LARGE:
                bonus -= 5
            elif direction == "short" and liq_side == "SHORT" and liq_usd >= self.LIQ_LARGE:
                bonus -= 5

        # ── 5. Объём — всплеск по hourly_deltas (до +5) ─────────────────
        if hourly_deltas and len(hourly_deltas) >= 2:
            last_hour  = abs(hourly_deltas[-1]) if hourly_deltas else 0
            avg_recent = sum(abs(d) for d in hourly_deltas[:-1]) / max(len(hourly_deltas) - 1, 1)
            if avg_recent > 0:
                volume_mult = last_hour / avg_recent
                if volume_mult >= 5:
                    bonus += 5
                    factors.append(f"Объём ×{volume_mult:.1f} в последний час — спайк!")
                elif volume_mult >= 2.5:
                    bonus += 3
                    factors.append(f"Объём ×{volume_mult:.1f} — повышенная активность")
                elif volume_mult >= 1.5:
                    bonus += 1

        # ── Итог ─────────────────────────────────────────────────────────
        bonus       = max(bonus, -20)          # не уходим в минус больше -20
        final_score = min(base_score + bonus, 100)

        # EARLY: слабый сигнал, только в Telegram, без сделки
        early_only = (
            final_score >= self.EARLY_MIN_SCORE
            and final_score < self.TRADE_MIN_SCORE
        )

        confidence = _confidence(final_score)

        return RealtimeResult(
            base_score  = base_score,
            bonus       = bonus,
            final_score = final_score,
            factors     = factors,
            early_only  = early_only,
            confidence  = confidence,
        )


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM FORMATTER
# ─────────────────────────────────────────────────────────────────────────────

def format_early_signal(
    symbol:    str,
    direction: str,
    score:     int,
    price:     float,
    base_factors: List[str],
    rt_factors:   List[str],
) -> str:
    """Форматировать EARLY сигнал для Telegram."""
    d_emoji = "🟢" if direction == "long" else "🔴"
    direction_up = direction.upper()

    
    price_str = _fmt_price(price)

    lines = [
        f"🛰️ <b>РАННИЙ {direction_up} WATCH</b>  |  Score: {score}%\n",
        f"{d_emoji} <code>{symbol}</code>   {price_str}",
        "",
        "<b>Аномалии прямо сейчас:</b>",
    ]
    for f in rt_factors:
        lines.append(f"  • {f}")

    if base_factors:
        lines.append("")
        lines.append("<b>Фундамент:</b>")
        for f in base_factors[:3]:
            lines.append(f"  • {f}")

    lines += [
        "",
        "⏳ <i>Ждём подтверждения. Сделка не открыта.</i>",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _confidence(score: int) -> str:
    if score >= 88:
        return "EXTREME"
    if score >= 78:
        return "HIGH"
    if score >= 65:
        return "MEDIUM"
    return "LOW"


def _fmt_price(price: float) -> str:
    """Умное форматирование цены."""
    if price == 0:
        return "$0"
    a = abs(price)
    if a >= 1000:
        return f"${price:,.2f}"
    if a >= 1:
        return f"${price:,.4f}"
    if a >= 0.01:
        return f"${price:,.6f}"
    if a >= 0.0001:
        return f"${price:,.8f}"
    return f"${price:,.12f}"


# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────────────────────────────────────

_realtime_scorer: Optional[RealtimeScorer] = None

def get_realtime_scorer() -> RealtimeScorer:
    global _realtime_scorer
    if _realtime_scorer is None:
        _realtime_scorer = RealtimeScorer()
    return _realtime_scorer
