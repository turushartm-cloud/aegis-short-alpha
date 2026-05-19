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

import os
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
    # ENV-driven: понижены для более раннего детекта recovery
    BTC_STRONG_UP_THRESHOLD = float(os.getenv("SHORT_FILTER_BTC_BLOCK_PCT",    "4.0"))
    BTC_MODERATE_UP         = float(os.getenv("SHORT_FILTER_BTC_MODERATE_PCT", "1.0"))  # FIX: 2.0→1.0

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

        # ── 2. RSI анализ ─────────────────────────────────────────────────────
        # ПРАВИЛО: SHORT открывается с ПЕРЕКУПЛЕННОСТИ (RSI 60-80), не с дна.
        # FIX #4: RSI 30-45 давал бонусы (+3/+5) — КРИТИЧЕСКИ НЕВЕРНО.
        # RSI < 45 = актив уже упал → вероятность V-отскока растёт.
        # Hard block RSI < 30 в main.py; здесь — scoring-штрафы для пограничных зон.
        rsi = getattr(market_data, "rsi_1h", None)
        if rsi is not None:
            if rsi >= 70:
                score_delta += 8
                reasons.append(f"🔥 RSI {rsi:.1f} перекуплен — сильный шорт сигнал +8")
            elif rsi >= 60:
                score_delta += 5
                reasons.append(f"RSI {rsi:.1f} в верхней зоне — шорт фаворит +5")
            elif rsi >= 50:
                score_delta += 2
                reasons.append(f"RSI {rsi:.1f} выше 50 — небольшой бонус +2")
            elif rsi >= 45:
                score_delta += 0
                reasons.append(f"RSI {rsi:.1f} нейтрально-нижняя — без коррекции")
            elif rsi >= 35:
                # FIX #4: было +3 "подтверждение даунтренда" — НЕВЕРНО.
                score_delta -= 8
                reasons.append(f"⚠️ RSI {rsi:.1f} перепродан — риск V-отскока -8")
            elif rsi >= 30:
                # FIX #4: было +5 "моментум продолжается" — КРИТИЧЕСКИ НЕВЕРНО.
                score_delta -= 15
                reasons.append(f"⛔ RSI {rsi:.1f} сильно перепродан — высокий риск отскока -15")
            else:
                # RSI < 30: hard block сработал раньше в main.py.
                score_delta -= 25
                reasons.append(f"🔴 RSI {rsi:.1f} экстремально перепродан — SHORT ЗАПРЕЩЁН -25")

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
            score_delta -= 4
            reasons.append(f"Отрицательный фандинг {funding*100:.3f}% (шорты платят) -4")

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

        # ── 7. Momentum Exhaustion — актив на дне, нет топлива для шорта ──
        # SWARMS/BOME-паттерн: долгий даунтренд + дно + RSI перепродан
        _exhaustion_enabled = os.getenv("ENABLE_MOMENTUM_EXHAUSTION", "true").lower() == "true"
        if _exhaustion_enabled:
            _exh_drop_4h  = float(os.getenv("EXHAUSTION_DROP_4H",  "-6.0"))   # 4H упал на X%
            _exh_drop_24h = float(os.getenv("EXHAUSTION_DROP_24H", "-15.0"))  # 24H упал на X%
            _exh_rsi_max  = float(os.getenv("EXHAUSTION_RSI_MAX",   "38.0"))  # RSI < X = перепродан

            p4h  = getattr(market_data, "price_change_4h",  0.0) or 0.0
            p24h = getattr(market_data, "price_change_24h", 0.0) or 0.0
            rsi  = getattr(market_data, "rsi_1h", 50.0) or 50.0

            # Условие exhaustion: актив упал и на 4H и на 24H, RSI перепродан
            exhausted = (p4h  <= _exh_drop_4h and
                         p24h <= _exh_drop_24h and
                         rsi  <= _exh_rsi_max)

            if exhausted:
                return ShortFilterResult(
                    blocked      = True,
                    block_reason = (
                        f"Momentum Exhaustion: 4H={p4h:.1f}% 24H={p24h:.1f}% "
                        f"RSI={rsi:.1f} — актив на дне, нет топлива для шорта"
                    ),
                    score_delta  = 0,
                    reasons      = [],
                )

            # Мягкая зона: только 24H сильно упал + RSI низкий — штраф
            if p24h <= _exh_drop_24h and rsi <= _exh_rsi_max + 7:
                score_delta -= 15
                reasons.append(f"⚠️ Near-Exhaustion: 24H={p24h:.1f}% RSI={rsi:.1f} — риск дна -15")

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
# ✅ FIX v17: TP уровни улучшены для RR≥1.5 при SL 2.0%
# Старые уровни давали RR 1:0.6..1:0.8 — убыток после комиссий BingX
SHORT_TP_LEVELS_CONSERVATIVE  = [2.0, 3.5, 5.5, 7.5, 10.0, 14.0]  # осторожно (RR~1.0)
SHORT_TP_LEVELS_STANDARD      = [3.0, 5.0, 7.5, 10.0, 13.0, 18.0]  # стандарт (RR~1.5)
SHORT_TP_LEVELS_AGGRESSIVE    = [4.0, 6.5, 9.5, 13.0, 17.0, 23.0]  # агрессивно (RR~2.0)

# Веса: для SHORT берём БОЛЬШЕ на первых TP (быстрая фиксация)
# 4 TP (дефолт): суммарно 100% — [25, 30, 25, 20]
# 6 TP (extended): суммарно 100% — [15, 20, 20, 15, 15, 15]
SHORT_TP_WEIGHTS_CONSERVATIVE = [25, 30, 25, 20, 0, 0]    # 4 TP быстро
SHORT_TP_WEIGHTS_STANDARD     = [25, 30, 25, 20, 0, 0]    # 4 TP стандарт
SHORT_TP_WEIGHTS_FAST_EXIT    = [35, 30, 20, 15, 0, 0]    # 4 TP быстрый выход
SHORT_TP_WEIGHTS_EXTENDED     = [15, 20, 20, 15, 15, 15]  # 6 TP только для трендовых

# Паттерны, при которых оправдан Extended TP (6 уровней вместо 4)
# Это трендовые паттерны с потенциалом продолжения, а не контртрендовые
SHORT_EXTENDED_TP_PATTERNS = {
    "BREAKOUT_SHORT", "WYCKOFF_UPTHRUST", "LIQUIDITY_SWEEP_SHORT",
    "PUMP_DUMP_SHORT", "MOMENTUM_SHORT", "DISTRIBUTION",
    # HTF версии тоже
    "BREAKOUT_SHORT_4H", "WYCKOFF_UPTHRUST_4H", "LIQUIDITY_SWEEP_SHORT_4H",
    "PUMP_DUMP_SHORT_4H", "MOMENTUM_SHORT_4H", "DISTRIBUTION_4H",
}

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
    atr_pct: float = 0.0,       # ✅ v19: ATR как % цены для адаптивного RR
    extended_tp: bool = False,  # 🆕 ENV EXTENDED_TP_SHORT=true → 6 уровней для трендовых
) -> tuple:
    """
    Выбрать оптимальный TP профиль для SHORT в зависимости от контекста.

    Дефолт: 4 TP (быстрая фиксация — шорт контртрендовый).
    Extended: 6 TP только для трендовых паттернов (BREAKOUT, WYCKOFF, SWEEP).

    ENV: EXTENDED_TP_SHORT=true  → разрешить 6 TP для трендовых паттернов
         EXTENDED_TP_SHORT=false → всегда 4 TP (дефолт, безопаснее)

    Returns:
        (tp_levels, tp_weights)
    """
    import os
    _env_extended = os.getenv("EXTENDED_TP_SHORT", "false").lower() == "true"
    _pattern_extended = (pattern_name or "").replace("_30M", "").replace("_1D", "") \
                        in SHORT_EXTENDED_TP_PATTERNS
    _use_extended = extended_tp or (_env_extended and _pattern_extended)

    def _slice(levels, weights):
        """4 TP дефолт, 6 TP если extended."""
        n = 6 if _use_extended else 4
        lvls = levels[:n]
        # Нормализуем веса до 100%
        raw = weights[:n]
        total = sum(raw)
        if total > 0 and total != 100:
            raw = [round(w / total * 100) for w in raw]
            # Поправляем округление
            diff = 100 - sum(raw)
            raw[-1] += diff
        return lvls, raw

    # ✅ v19: Адаптивный RR под волатильность
    if atr_pct >= 3.0:
        _mult = min(atr_pct / 2.0, 2.5)
        base_lvls = [round(l * _mult, 2) for l in SHORT_TP_LEVELS_STANDARD]
        w = SHORT_TP_WEIGHTS_EXTENDED if _use_extended else SHORT_TP_WEIGHTS_STANDARD
        return _slice(base_lvls, w)
    elif atr_pct >= 2.0:
        _mult = 1.3
        base_lvls = [round(l * _mult, 2) for l in SHORT_TP_LEVELS_STANDARD]
        w = SHORT_TP_WEIGHTS_EXTENDED if _use_extended else SHORT_TP_WEIGHTS_CONSERVATIVE
        return _slice(base_lvls, w)

    # Высокий фандинг = торопимся выйти (всегда 4 TP независимо от extended)
    if funding_rate >= 0.10:
        return _slice(SHORT_TP_LEVELS_CONSERVATIVE, SHORT_TP_WEIGHTS_FAST_EXIT)

    # Трендовые паттерны — агрессивнее
    if _use_extended and _pattern_extended:
        if btc_trend == "down":
            return _slice(SHORT_TP_LEVELS_AGGRESSIVE, SHORT_TP_WEIGHTS_EXTENDED)
        return _slice(SHORT_TP_LEVELS_STANDARD, SHORT_TP_WEIGHTS_EXTENDED)

    # Momentum шорт = берём большое движение
    if pattern_name in ("MEGA_SHORT", "DISTRIBUTION", "MEGA_SHORT_4H", "DISTRIBUTION_4H"):
        if btc_trend == "down":
            return _slice(SHORT_TP_LEVELS_AGGRESSIVE, SHORT_TP_WEIGHTS_STANDARD)
        return _slice(SHORT_TP_LEVELS_STANDARD, SHORT_TP_WEIGHTS_STANDARD)

    # BTC падает — можно держать дольше
    if btc_trend == "down":
        return _slice(SHORT_TP_LEVELS_STANDARD, SHORT_TP_WEIGHTS_CONSERVATIVE)

    # По умолчанию — осторожно, 4 TP
    return _slice(SHORT_TP_LEVELS_CONSERVATIVE, SHORT_TP_WEIGHTS_FAST_EXIT)


# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────────────────────────────────────

_short_filter: Optional[ShortFilter] = None

def get_short_filter() -> ShortFilter:
    global _short_filter
    if _short_filter is None:
        _short_filter = ShortFilter()
    return _short_filter
