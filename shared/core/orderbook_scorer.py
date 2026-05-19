"""
OrderBook Scorer v1.0
Анализирует стакан заявок для определения давления покупателей/продавцов.

Метрика: bid/ask imbalance ratio в ±1% от текущей цены
  ratio > 2.0  → LONG давление  → +8 к score
  ratio < 0.5  → SHORT давление → +8 к score
  ratio > 1.5  → умеренный LONG → +4
  ratio < 0.67 → умеренный SHORT → +4

Order walls (стены): ≥5% объёма уровня = крупный ордер
  → Потенциальный уровень S/R
"""
import os
import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)

_ENABLE_ORDERBOOK = os.getenv("ENABLE_ORDERBOOK_SCORER", "true").lower() == "true"
_OB_STRONG_RATIO  = float(os.getenv("OB_STRONG_RATIO", "2.0"))   # ratio > этого = сильный сигнал
_OB_WEAK_RATIO    = float(os.getenv("OB_WEAK_RATIO", "1.5"))     # ratio > этого = слабый сигнал
_OB_WALL_PCT      = float(os.getenv("OB_WALL_PCT", "0.05"))      # ≥5% от объёма уровня = стена


@dataclass
class OrderBookResult:
    bid_volume: float        # Объём bids в ±1% от цены
    ask_volume: float        # Объём asks в ±1% от цены
    ratio: float             # bid/ask ratio
    score: int               # итоговый score (0-8)
    description: str
    has_bid_wall: bool       # Крупная стена покупателей
    has_ask_wall: bool       # Крупная стена продавцов
    bid_wall_price: Optional[float] = None
    ask_wall_price: Optional[float] = None


def calculate_orderbook_score(
    order_book: Optional[Dict],
    current_price: float,
    direction: str = "long",
    price_range_pct: float = 0.01,   # ±1% от цены
) -> Tuple[int, str, Optional[OrderBookResult]]:
    """
    Рассчитывает score на основе стакана заявок.

    Returns:
        (score: int, description: str, result: Optional[OrderBookResult])
    """
    if not _ENABLE_ORDERBOOK or not order_book or not current_price:
        return 0, "", None

    try:
        bids = order_book.get("bids", [])
        asks = order_book.get("asks", [])

        if not bids or not asks:
            return 0, "Нет данных стакана", None

        # Фильтруем заявки в ±1% от цены
        price_min = current_price * (1 - price_range_pct)
        price_max = current_price * (1 + price_range_pct)

        bid_volume = 0.0
        bid_wall_volume = 0.0
        bid_wall_price = None
        total_bid_vol = 0.0

        for level in bids:
            try:
                p, q = float(level[0]), float(level[1])
                total_bid_vol += p * q
                if price_min <= p <= price_max:
                    vol = p * q
                    bid_volume += vol
                    if vol > bid_wall_volume:
                        bid_wall_volume = vol
                        bid_wall_price = p
            except (ValueError, IndexError):
                continue

        ask_volume = 0.0
        ask_wall_volume = 0.0
        ask_wall_price = None
        total_ask_vol = 0.0

        for level in asks:
            try:
                p, q = float(level[0]), float(level[1])
                total_ask_vol += p * q
                if price_min <= p <= price_max:
                    vol = p * q
                    ask_volume += vol
                    if vol > ask_wall_volume:
                        ask_wall_volume = vol
                        ask_wall_price = p
            except (ValueError, IndexError):
                continue

        if ask_volume <= 0 and bid_volume <= 0:
            return 0, "Нет объёма в ±1% от цены", None

        # Ratio
        if ask_volume <= 0:
            ratio = 10.0
        elif bid_volume <= 0:
            ratio = 0.1
        else:
            ratio = bid_volume / ask_volume

        # Детекция стен (≥5% от объёма уровня)
        total_near_vol = bid_volume + ask_volume
        has_bid_wall = (total_near_vol > 0 and bid_wall_volume / max(total_near_vol, 1) >= _OB_WALL_PCT)
        has_ask_wall = (total_near_vol > 0 and ask_wall_volume / max(total_near_vol, 1) >= _OB_WALL_PCT)

        # Score calculation
        score = 0
        desc_parts = []

        if direction == "long":
            if ratio >= _OB_STRONG_RATIO:
                score = 8
                desc_parts.append(f"📊 Стакан: bid/ask={ratio:.2f} → LONG давление 🔥")
            elif ratio >= _OB_WEAK_RATIO:
                score = 4
                desc_parts.append(f"📊 Стакан: bid/ask={ratio:.2f} → умеренный LONG")
            elif ratio <= (1 / _OB_STRONG_RATIO):
                score = 0
                desc_parts.append(f"📊 Стакан: bid/ask={ratio:.2f} → SHORT давление ⚠️")
            else:
                score = 2
                desc_parts.append(f"📊 Стакан: bid/ask={ratio:.2f} нейтраль")
            if has_bid_wall:
                desc_parts.append(f"🧱 Стена покупателей @ {bid_wall_price:.4f}")
            if has_ask_wall:
                score = max(score - 2, 0)
                desc_parts.append(f"🧱 Стена продавцов @ {ask_wall_price:.4f}")
        else:  # short
            if ratio <= (1 / _OB_STRONG_RATIO):
                score = 8
                desc_parts.append(f"📊 Стакан: bid/ask={ratio:.2f} → SHORT давление 🔥")
            elif ratio <= (1 / _OB_WEAK_RATIO):
                score = 4
                desc_parts.append(f"📊 Стакан: bid/ask={ratio:.2f} → умеренный SHORT")
            elif ratio >= _OB_STRONG_RATIO:
                score = 0
                desc_parts.append(f"📊 Стакан: bid/ask={ratio:.2f} → LONG давление ⚠️")
            else:
                score = 2
                desc_parts.append(f"📊 Стакан: bid/ask={ratio:.2f} нейтраль")
            if has_ask_wall:
                desc_parts.append(f"🧱 Стена продавцов @ {ask_wall_price:.4f}")
            if has_bid_wall:
                score = max(score - 2, 0)
                desc_parts.append(f"🧱 Стена покупателей @ {bid_wall_price:.4f}")

        result = OrderBookResult(
            bid_volume=bid_volume, ask_volume=ask_volume, ratio=ratio,
            score=score, description=" | ".join(desc_parts),
            has_bid_wall=has_bid_wall, has_ask_wall=has_ask_wall,
            bid_wall_price=bid_wall_price, ask_wall_price=ask_wall_price,
        )

        logger.info(f"[OB] ratio={ratio:.2f} score={score} {' | '.join(desc_parts)}")
        return score, " | ".join(desc_parts), result

    except Exception as e:
        logger.warning(f"[OrderBook] Ошибка расчёта: {e}")
        return 0, "", None
