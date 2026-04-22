"""
Short Filter — shared/core/short_filter.py

Фильтры СПЕЦИФИЧНЫЕ для SHORT бота.
Логика входа у SHORT принципиально отличается от LONG:

LONG  ищет: перепроданность → ждёт дно → входит С трендом разворота
SHORT ищет: перекупленность → ждёт вершину → входит ПРОТИВ тренда

Это значит у SHORT должно быть:
  1. Фильтр рыночного режима — не шортить в сильный аптренд BTC
  2. Подтверждение отката — ждём медвежью свечу-разворот, не просто RSI>70
  3. Фандинг-спайк — > +0.1% это САМЫЙ сильный SHORT сигнал
  4. Быстрые ТП — шорт контртрендовый, выходим раньше
  5. Тайминг трейлинга — шорт разворачивается быстрее, трейлим от +1%

─────────────────────────────────────────────────────────────────────
Использование в scan_symbol() SHORT бота:

    from core.short_filter import ShortFilter, ShortFilterResult

    sf = ShortFilter()
    filt = sf.check(
        market_data   = market_data,
        ohlcv_15m     = ohlcv_15m,          # List[CandleData]
        hourly_deltas = hourly_deltas,
        btc_price_1h_change = btc_change,   # % изменение BTC за 1ч (опц.)
    )

    if filt.blocked:
        print(f"SHORT blocked: {filt.block_reason}")
        return None   # не генерируем сигнал

    # Применяем штрафы/бонусы к скору
    final_score += filt.score_delta
    reasons.extend(filt.reasons)
─────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Any


@dataclass
class ShortFilterResult:
    """Результат SHORT фильтрации."""
    blocked:      bool         # True = сигнал заблокирован
    block_reason: str          # почему заблокирован (если blocked=True)
    score_delta:  int          # +/- к итоговому скору
    reasons:      List[str]    # факторы для отображения


class ShortFilter:
    """
    Набор фильтров специфичных для SHORT позиций.
    Без состояния — создавай один раз, переиспользуй.
    """

    # Режим рынка: блокируем шорт если BTC растёт
    # ✅ ADJUSTED: 2% → 3% (модерато настройка, меньше пропусков хороших шортов)
    BTC_STRONG_UP_THRESHOLD = 4.0    # ✅ FIX: было 3.0 — слишком часто блокировало
    BTC_MODERATE_UP         = 2.0    # ✅ FIX: было 1.5 — штрафовало при любом росте BTC

    # Фандинг-спайк (самый мощный SHORT сигнал)
    FUNDING_EXTREME  = 0.10   # > 0.10% → экстремально перегрет
    FUNDING_HIGH     = 0.05   # > 0.05%
    FUNDING_MODERATE = 0.02   # > 0.02%

    # Минимальный RSI для SHORT входа
    RSI_MIN_FOR_SHORT = 35    # ✅ FIX #2: было 50 — блокировало RSI 35-49 (лучшие шорт сетапы)

    def check(
        self,
        market_data:          Any,
        ohlcv_15m:            List[Any],      # List[CandleData]
        hourly_deltas:        List[float],
        btc_price_1h_change:  Optional[float] = None,  # % BTC за 1ч
    ) -> ShortFilterResult:
        """
        Выполнить все SHORT-специфичные проверки.
        """
        score_delta = 0
        reasons: List[str] = []

        # ── 1. Рыночный режим (BTC направление) ──────────────────────────
        if btc_price_1h_change is not None:
            if btc_price_1h_change >= self.BTC_STRONG_UP_THRESHOLD:
                # BTC сильно растёт → шортить очень рискованно
                return ShortFilterResult(
                    blocked      = True,
                    block_reason = f"BTC в сильном аптренде (+{btc_price_1h_change:.1f}% за 1ч) — SHORT заблокирован",
                    score_delta  = 0,
                    reasons      = [],
                )
            elif btc_price_1h_change >= self.BTC_MODERATE_UP:
                # Умеренный рост — штраф к скору
                score_delta -= 10
                reasons.append(f"BTC растёт +{btc_price_1h_change:.1f}%/1ч → -10 к скору")
            elif btc_price_1h_change <= -1.0:
                # BTC падает — бонус для SHORT
                score_delta += 8
                reasons.append(f"BTC падает {btc_price_1h_change:.1f}%/1ч → +8 к скору")

        # ── 2. RSI анализ (НЕ блокирующий!) ───────────────────────────────
        rsi = getattr(market_data, "rsi_1h", None)
        if rsi is not None:
            if rsi >= 70:
                # Перекупленность — отличный сигнал для шорта
                score_delta += 8
                reasons.append(f"🔥 RSI {rsi:.1f} перекуплен — сильный шорт сигнал +8")
            elif rsi >= 60:
                # Верхняя зона — хорошо для шорта
                score_delta += 5
                reasons.append(f"RSI {rsi:.1f} в верхней зоне — шорт фаворит +5")
            elif rsi >= 50:
                # Нейтрально
                score_delta += 2
                reasons.append(f"RSI {rsi:.1f} выше 50 — небольшой бонус +2")
            elif rsi >= 40:
                # Ниже 50 — даунтренд подтверждается
                score_delta += 3
                reasons.append(f"RSI {rsi:.1f} в зоне даунтренда — подтверждение +3")
            elif rsi >= 30:
                # Низкий RSI при падении — моментум продолжается
                score_delta += 5
                reasons.append(f"RSI {rsi:.1f} низкий при падении — моментум +5")
            else:
                # Очень низкий RSI (<30) — возможен отскок, но не блокируем!
                score_delta -= 3
                reasons.append(f"RSI {rsi:.1f} очень низкий — риск отскока -3")

        # ── 3. Фандинг-спайк (главный SHORT фактор) ──────────────────────
        funding = getattr(market_data, "funding_rate", 0) / 100  # уже в долях
        if funding >= self.FUNDING_EXTREME:
            score_delta += 15
            reasons.append(f"🔥 Фандинг-спайк {funding*100:.3f}% — лонги перегреты +15")
        elif funding >= self.FUNDING_HIGH:
            score_delta += 10
            reasons.append(f"Высокий фандинг {funding*100:.3f}% +10")
        elif funding >= self.FUNDING_MODERATE:
            score_delta += 5
            reasons.append(f"Повышенный фандинг {funding*100:.3f}% +5")
        elif funding < 0:
            # Отрицательный фандинг = шорты платят = идти в SHORT невыгодно
            score_delta -= 8
            reasons.append(f"Отрицательный фандинг {funding*100:.3f}% (шорты платят) -8")

        # ── 4. Подтверждение медвежьей свечи ─────────────────────────────
        # SHORT лучше входить ПОСЛЕ того как цена начала разворот
        # не на пике, а при первом движении вниз
        if ohlcv_15m and len(ohlcv_15m) >= 3:
            last   = ohlcv_15m[-1]
            prev   = ohlcv_15m[-2]
            prev2  = ohlcv_15m[-3]

            try:
                # Shooting star или медвежье поглощение = подтверждение
                last_close = last.close if hasattr(last, 'close') else last[3]
                last_open  = last.open  if hasattr(last, 'open')  else last[0]
                last_high  = last.high  if hasattr(last, 'high')  else last[1]
                last_low   = last.low   if hasattr(last, 'low')   else last[2]

                prev_close = prev.close if hasattr(prev, 'close') else prev[3]
                prev_open  = prev.open  if hasattr(prev, 'open')  else prev[0]
                prev_high  = prev.high  if hasattr(prev, 'high')  else prev[1]

                rng = last_high - last_low
                if rng > 0:
                    upper_wick = last_high - max(last_close, last_open)
                    body_size  = abs(last_close - last_open)

                    is_shooting_star = (
                        upper_wick > rng * 0.55 and
                        body_size  < rng * 0.35 and
                        last_close < last_open
                    )
                    is_bearish_engulf = (
                        last_close < last_open and            # текущая медвежья
                        prev_close > prev_open and            # предыдущая бычья
                        last_open  > prev_close and           # открытие выше
                        last_close < prev_open                # закрытие ниже открытия пред.
                    )

                    if is_shooting_star or is_bearish_engulf:
                        candle_type = "shooting star" if is_shooting_star else "bearish engulf"
                        score_delta += 8
                        reasons.append(f"Медвежий разворот: {candle_type} +8")
                    elif last_close < last_open:
                        # Просто медвежья — небольшой бонус
                        score_delta += 3
                    else:
                        # Бычья свеча прямо сейчас — штраф
                        score_delta -= 5
                        reasons.append("Текущая свеча бычья — вход преждевременный -5")

            except Exception:
                pass

        # ── 5. Объёмная дивергенция (продавцы появились) ─────────────────
        # Для SHORT важно видеть отрицательную дельту, а не просто рост OI
        if hourly_deltas and len(hourly_deltas) >= 3:
            last_delta  = hourly_deltas[-1]
            avg_delta   = sum(hourly_deltas[:-1]) / max(len(hourly_deltas) - 1, 1)

            if last_delta < 0 and last_delta < avg_delta * 2:
                score_delta += 7
                reasons.append(f"Продавцы появились: дельта {last_delta:.1f} < avg {avg_delta:.1f} +7")
            elif last_delta > 0 and last_delta > abs(avg_delta) * 1.5:
                # Покупатели агрессивно заходят — против шорта
                score_delta -= 8
                reasons.append(f"Агрессивные покупки в дельте — против шорта -8")

        # ── 6. L/S ratio: толпа перегружена лонгами ─────────────────────
        long_ratio = getattr(market_data, "long_short_ratio", 50)
        if long_ratio >= 70:
            score_delta += 8
            reasons.append(f"Толпа: {long_ratio:.0f}% в лонгах (перегруз) +8")
        elif long_ratio >= 60:
            score_delta += 4
            reasons.append(f"Лонги доминируют: {long_ratio:.0f}% +4")
        elif long_ratio <= 35:
            # Все уже в шортах — нет кого выбивать
            score_delta -= 10
            reasons.append(f"Толпа уже в шортах ({long_ratio:.0f}%) — нечего выбивать -10")

        return ShortFilterResult(
            blocked      = False,
            block_reason = "",
            score_delta  = score_delta,
            reasons      = reasons,
        )


# ─────────────────────────────────────────────────────────────────────────────
# SHORT-специфичный TP профиль
# ─────────────────────────────────────────────────────────────────────────────

# SHORT TP должны быть БЫСТРЕЕ чем LONG — шорт контртрендовый
# Берём прибыль раньше, не ждём большого движения
SHORT_TP_LEVELS_CONSERVATIVE  = [1.0, 2.0, 3.5, 5.0, 7.0, 10.0]  # осторожно
SHORT_TP_LEVELS_STANDARD      = [1.5, 3.0, 5.0, 6.3, 8.5, 12.2]  # текущие (стандарт)
SHORT_TP_LEVELS_AGGRESSIVE    = [2.0, 4.0, 6.5, 9.0, 12.0, 16.0] # агрессивно

# Веса: для SHORT берём БОЛЬШЕ на первых TP (быстрая фиксация)
SHORT_TP_WEIGHTS_CONSERVATIVE = [30, 25, 20, 15, 7, 3]   # 55% в TP1-2
SHORT_TP_WEIGHTS_STANDARD     = [20, 20, 20, 15, 15, 10] # текущие
SHORT_TP_WEIGHTS_FAST_EXIT    = [35, 30, 20, 10, 3, 2]   # 65% в TP1-2

# Рекомендации по TP стилю:
#   HIGH_FUNDING (>0.1%)     → CONSERVATIVE (фандинг убивает прибыль)
#   MEGA_SHORT pattern       → AGGRESSIVE (momentum работает)
#   DISTRIBUTION pattern     → STANDARD (медленное движение)
#   BTC в даунтренде         → AGGRESSIVE
#   BTC в нейтрали           → CONSERVATIVE


def get_short_tp_config(
    funding_rate: float,
    pattern_name: Optional[str],
    btc_trend: Optional[str],   # "up" | "down" | "sideways"
) -> tuple:
    """
    Выбрать оптимальный TP профиль для SHORT в зависимости от контекста.

    Returns:
        (tp_levels, tp_weights)
    """
    # Высокий фандинг = торопимся выйти
    if funding_rate >= 0.10:
        return SHORT_TP_LEVELS_CONSERVATIVE, SHORT_TP_WEIGHTS_FAST_EXIT

    # Momentum шорт = берём большое движение
    if pattern_name in ("MEGA_SHORT", "DISTRIBUTION"):
        if btc_trend == "down":
            return SHORT_TP_LEVELS_AGGRESSIVE, SHORT_TP_WEIGHTS_STANDARD
        return SHORT_TP_LEVELS_STANDARD, SHORT_TP_WEIGHTS_STANDARD

    # BTC падает — можно держать дольше
    if btc_trend == "down":
        return SHORT_TP_LEVELS_STANDARD, SHORT_TP_WEIGHTS_CONSERVATIVE

    # По умолчанию — осторожно
    return SHORT_TP_LEVELS_CONSERVATIVE, SHORT_TP_WEIGHTS_FAST_EXIT


# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────────────────────────────────────

_short_filter: Optional[ShortFilter] = None

def get_short_filter() -> ShortFilter:
    global _short_filter
    if _short_filter is None:
        _short_filter = ShortFilter()
    return _short_filter
