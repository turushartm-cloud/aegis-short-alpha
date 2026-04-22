"""
SMC / ICT Detector
Smart Money Concepts: Order Blocks, Fair Value Gaps, уточнённые SL/TP

Как использовать в scan_symbol():
    from utils.smc_ict_detector import SMCDetector
    smc = SMCDetector(ohlcv_15m)
    result = smc.analyze(direction="short")
    # result содержит: ob_entry, fvg_zone, refined_sl, score_bonus
"""

from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass


@dataclass
class OrderBlock:
    """Ордер-блок — зона где крупный игрок набирал позицию"""
    direction: str          # "bullish" | "bearish"
    high: float
    low: float
    open: float
    close: float
    index: int              # Индекс свечи в массиве
    strength: int           # 0-100


@dataclass
class FairValueGap:
    """Fair Value Gap — ценовой разрыв из трёх свечей"""
    direction: str          # "bullish" (цена уйдёт вверх) | "bearish"
    upper: float
    lower: float
    index: int              # Средняя свеча
    filled: bool = False


@dataclass
class SMCResult:
    """Результат SMC анализа"""
    direction: str
    has_ob: bool
    has_fvg: bool
    ob_entry: Optional[float]       # Уточнённый вход через OB
    fvg_zone: Optional[Tuple[float, float]]  # (lower, upper) FVG
    refined_sl: Optional[float]     # SL за Order Block
    score_bonus: int                # Бонус к скору: 0-15
    reasons: List[str]


class SMCDetector:
    """
    SMC/ICT анализ на 15m свечах.
    
    Args:
        ohlcv: Список свечей [open, high, low, close, volume]
               ohlcv[-1] — последняя (текущая) свеча
    """

    def __init__(self, ohlcv: List[List[float]]):
        self.ohlcv = ohlcv
        self.n = len(ohlcv)

    def _o(self, i: int) -> float: return self.ohlcv[i][0]
    def _h(self, i: int) -> float: return self.ohlcv[i][1]
    def _l(self, i: int) -> float: return self.ohlcv[i][2]
    def _c(self, i: int) -> float: return self.ohlcv[i][3]
    def _v(self, i: int) -> float: return self.ohlcv[i][4] if len(self.ohlcv[i]) > 4 else 0

    # =========================================================================
    # ORDER BLOCKS
    # =========================================================================

    def find_bearish_order_blocks(self, lookback: int = 30) -> List[OrderBlock]:
        """
        Медвежий Order Block — последняя бычья свеча перед сильным падением.
        После неё должен быть импульс вниз ≥ 2 свечей подряд.
        
        В SHORT боте: ищем зону где шортисты набирали позиции.
        Цена часто возвращается к OB для ретеста — там и входим.
        """
        blocks = []
        start = max(0, self.n - lookback)

        for i in range(start, self.n - 2):
            # Свеча i должна быть бычьей (close > open)
            if self._c(i) <= self._o(i):
                continue

            # После неё должно быть сильное падение
            # Проверяем 2 следующие свечи
            next_bearish = 0
            for j in range(i + 1, min(i + 4, self.n)):
                if self._c(j) < self._o(j):
                    next_bearish += 1

            if next_bearish < 2:
                continue

            # Размер импульса — насколько упала цена после OB
            low_after = min(self._l(j) for j in range(i + 1, min(i + 4, self.n)))
            drop_pct = (self._h(i) - low_after) / self._h(i) * 100

            if drop_pct < 0.5:  # Слишком маленькое движение
                continue

            strength = min(100, int(drop_pct * 20))

            blocks.append(OrderBlock(
                direction="bearish",
                high=self._h(i),
                low=self._l(i),
                open=self._o(i),
                close=self._c(i),
                index=i,
                strength=strength
            ))

        # Сортируем по свежести (ближе к текущей свече) и силе
        blocks.sort(key=lambda b: (self.n - b.index) * -1 + b.strength, reverse=True)
        return blocks[:3]  # Топ-3

    def find_bullish_order_blocks(self, lookback: int = 30) -> List[OrderBlock]:
        """
        Бычий Order Block — последняя медвежья свеча перед сильным ростом.
        В LONG боте: зона где покупатели набирали позиции.
        """
        blocks = []
        start = max(0, self.n - lookback)

        for i in range(start, self.n - 2):
            if self._c(i) >= self._o(i):
                continue

            next_bullish = 0
            for j in range(i + 1, min(i + 4, self.n)):
                if self._c(j) > self._o(j):
                    next_bullish += 1

            if next_bullish < 2:
                continue

            high_after = max(self._h(j) for j in range(i + 1, min(i + 4, self.n)))
            rise_pct = (high_after - self._l(i)) / self._l(i) * 100

            if rise_pct < 0.5:
                continue

            strength = min(100, int(rise_pct * 20))

            blocks.append(OrderBlock(
                direction="bullish",
                high=self._h(i),
                low=self._l(i),
                open=self._o(i),
                close=self._c(i),
                index=i,
                strength=strength
            ))

        blocks.sort(key=lambda b: (self.n - b.index) * -1 + b.strength, reverse=True)
        return blocks[:3]

    # =========================================================================
    # FAIR VALUE GAPS
    # =========================================================================

    def find_bearish_fvg(self, lookback: int = 20) -> List[FairValueGap]:
        """
        Медвежий FVG: low свечи i > high свечи i+2.
        Разрыв между ними — зона несправедливой цены.
        В SHORT: цена часто возвращается в FVG и отталкивается вниз.
        """
        gaps = []
        start = max(0, self.n - lookback)

        for i in range(start, self.n - 2):
            # FVG: gap между high[i+2] и low[i]
            gap_upper = self._l(i)
            gap_lower = self._h(i + 2)

            if gap_lower >= gap_upper:
                continue  # Нет разрыва

            gap_size = (gap_upper - gap_lower) / gap_lower * 100
            if gap_size < 0.1:  # Слишком маленький разрыв
                continue

            # Проверяем, не закрылся ли уже FVG
            filled = any(
                self._h(j) >= gap_upper
                for j in range(i + 3, self.n)
            )

            gaps.append(FairValueGap(
                direction="bearish",
                upper=gap_upper,
                lower=gap_lower,
                index=i + 1,
                filled=filled
            ))

        # Только незаполненные, ближайшие к текущей цене
        gaps = [g for g in gaps if not g.filled]
        current_price = self._c(-1)
        gaps.sort(key=lambda g: abs((g.upper + g.lower) / 2 - current_price))
        return gaps[:2]

    def find_bullish_fvg(self, lookback: int = 20) -> List[FairValueGap]:
        """
        Бычий FVG: high свечи i+2 < low свечи i.
        В LONG: поддержка, от которой цена отбивается вверх.
        """
        gaps = []
        start = max(0, self.n - lookback)

        for i in range(start, self.n - 2):
            gap_lower = self._h(i)
            gap_upper = self._l(i + 2)

            if gap_upper <= gap_lower:
                continue

            gap_size = (gap_upper - gap_lower) / gap_lower * 100
            if gap_size < 0.1:
                continue

            filled = any(
                self._l(j) <= gap_lower
                for j in range(i + 3, self.n)
            )

            gaps.append(FairValueGap(
                direction="bullish",
                upper=gap_upper,
                lower=gap_lower,
                index=i + 1,
                filled=filled
            ))

        gaps = [g for g in gaps if not g.filled]
        current_price = self._c(-1)
        gaps.sort(key=lambda g: abs((g.upper + g.lower) / 2 - current_price))
        return gaps[:2]

    # =========================================================================
    # PRICE NEAR ZONE CHECK
    # =========================================================================

    def _price_near_zone(self, price: float, zone_low: float, zone_high: float,
                          tolerance_pct: float = 0.5) -> bool:
        """Проверить, находится ли цена рядом с зоной (с допуском %)"""
        mid = (zone_low + zone_high) / 2
        tolerance = mid * tolerance_pct / 100
        return abs(price - mid) <= tolerance + (zone_high - zone_low) / 2

    # =========================================================================
    # MAIN ANALYZE
    # =========================================================================

    def analyze(self, direction: str,
                base_sl_pct: float = 0.5,
                base_entry: Optional[float] = None) -> SMCResult:
        """
        Полный SMC анализ.
        
        Args:
            direction: "short" | "long"
            base_sl_pct: базовый SL в % (из Config.SL_BUFFER)
            base_entry: текущая цена входа
        
        Returns:
            SMCResult с уточнёнными уровнями и бонусом к скору
        """
        current_price = self._c(-1) if base_entry is None else base_entry
        reasons = []
        score_bonus = 0
        ob_entry = None
        fvg_zone = None
        refined_sl = None

        if direction == "short":
            obs = self.find_bearish_order_blocks()
            fvgs = self.find_bearish_fvg()

            # Проверяем Order Block
            for ob in obs:
                if self._price_near_zone(current_price, ob.low, ob.high, tolerance_pct=0.3):
                    ob_entry = (ob.low + ob.high) / 2  # Вход в середину OB
                    refined_sl = ob.high * 1.002        # SL чуть выше OB
                    bonus = min(10, ob.strength // 10)
                    score_bonus += bonus
                    reasons.append(f"Bearish OB [{ob.low:.4f}-{ob.high:.4f}] +{bonus}pts")
                    break

            # FVG
            for fvg in fvgs:
                if fvg.upper >= current_price >= fvg.lower:
                    fvg_zone = (fvg.lower, fvg.upper)
                    score_bonus += 5
                    reasons.append(f"In Bearish FVG [{fvg.lower:.4f}-{fvg.upper:.4f}] +5pts")
                    break
                elif current_price > fvg.upper and self._price_near_zone(current_price, fvg.lower, fvg.upper, 0.2):
                    fvg_zone = (fvg.lower, fvg.upper)
                    score_bonus += 3
                    reasons.append(f"Near Bearish FVG [{fvg.lower:.4f}-{fvg.upper:.4f}] +3pts")
                    break

        else:  # long
            obs = self.find_bullish_order_blocks()
            fvgs = self.find_bullish_fvg()

            for ob in obs:
                if self._price_near_zone(current_price, ob.low, ob.high, tolerance_pct=0.3):
                    ob_entry = (ob.low + ob.high) / 2
                    refined_sl = ob.low * 0.998         # SL чуть ниже OB
                    bonus = min(10, ob.strength // 10)
                    score_bonus += bonus
                    reasons.append(f"Bullish OB [{ob.low:.4f}-{ob.high:.4f}] +{bonus}pts")
                    break

            for fvg in fvgs:
                if fvg.lower <= current_price <= fvg.upper:
                    fvg_zone = (fvg.lower, fvg.upper)
                    score_bonus += 5
                    reasons.append(f"In Bullish FVG [{fvg.lower:.4f}-{fvg.upper:.4f}] +5pts")
                    break
                elif current_price < fvg.lower and self._price_near_zone(current_price, fvg.lower, fvg.upper, 0.2):
                    fvg_zone = (fvg.lower, fvg.upper)
                    score_bonus += 3
                    reasons.append(f"Near Bullish FVG [{fvg.lower:.4f}-{fvg.upper:.4f}] +3pts")
                    break

        # Если SL не уточнён через OB — используем базовый
        if refined_sl is None:
            if direction == "short":
                refined_sl = current_price * (1 + base_sl_pct / 100)
            else:
                refined_sl = current_price * (1 - base_sl_pct / 100)

        return SMCResult(
            direction=direction,
            has_ob=ob_entry is not None,
            has_fvg=fvg_zone is not None,
            ob_entry=ob_entry,
            fvg_zone=fvg_zone,
            refined_sl=refined_sl,
            score_bonus=min(15, score_bonus),  # Максимум +15 к скору
            reasons=reasons
        )


# ============================================================================
# SINGLETON DETECTOR
# ============================================================================

def get_smc_result(ohlcv: List[List[float]], direction: str,
                   base_sl_pct: float = 0.5,
                   base_entry: Optional[float] = None) -> SMCResult:
    """
    Удобная функция — создаёт детектор и возвращает результат.
    
    Использование в scan_symbol():
    
        from utils.smc_ict_detector import get_smc_result
        
        smc = get_smc_result(ohlcv_15m, "short",
                             base_sl_pct=Config.SL_BUFFER,
                             base_entry=market_data.price)
        
        final_score += smc.score_bonus
        
        if smc.refined_sl:
            signal["stop_loss"] = smc.refined_sl  # Точнее чем price * 1.005
        
        if smc.ob_entry:
            signal["entry_price"] = smc.ob_entry  # Лучше чем market price
        
        signal["reasons"] += smc.reasons
        signal["smc"] = {
            "has_ob": smc.has_ob,
            "has_fvg": smc.has_fvg,
            "fvg_zone": smc.fvg_zone,
            "score_bonus": smc.score_bonus
        }
    """
    detector = SMCDetector(ohlcv)
    return detector.analyze(direction, base_sl_pct, base_entry)
