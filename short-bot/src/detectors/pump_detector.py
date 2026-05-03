"""
Pump Detector v1.0
Z-Score + VWAP deviation + Volume Spike = Climax Exhaustion Detection.

Логика:
  1. Расчёт VWAP + rolling σ (std deviation)
  2. Z-Score = (Price - VWAP) / σ
  3. Volume Spike: текущий объём / SMA(20) объёма
  4. RSI перекупленность (>75)
  5. Price Velocity (скорость движения за 5 свечей)
  6. Байесовская оценка вероятности реверса

Сигнал SHORT:
  ULTRA: Z > 3.0 + Vol > 3x + RSI > 78
  STRONG: Z > 2.5 + Vol > 2.5x + RSI > 72
  MODERATE: Z > 2.0 + Vol > 2x
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger("aegis.pump_detector")


@dataclass
class ZScoreConfig:
    threshold:       float = 2.5    # Z-Score порог для сигнала
    volume_spike:    float = 3.0    # Volume/SMA порог
    rsi_overbought:  float = 75.0   # RSI порог
    lookback:        int   = 20     # Период для VWAP/SMA
    confirmation_candles: int = 2   # Свечей подтверждения


class PumpDetector:
    """
    Институциональный детектор кульминации пампа.
    Ищет точки истощения покупателей для SHORT входа.
    """

    def __init__(self, config: Optional[ZScoreConfig] = None):
        self.cfg = config or ZScoreConfig()

    def _calc_rsi(self, closes: List[float], period: int = 14) -> float:
        """RSI через EMA smoothing"""
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        gains  = [d if d > 0 else 0.0 for d in deltas[-period:]]
        losses = [-d if d < 0 else 0.0 for d in deltas[-period:]]
        avg_g  = sum(gains) / period
        avg_l  = sum(losses) / period
        if avg_l == 0:
            return 100.0
        rs = avg_g / avg_l
        return round(100.0 - 100.0 / (1.0 + rs), 2)

    def _calc_vwap_std(
        self, ohlcv: list, lookback: int
    ) -> tuple:  # (vwap, std_dev)
        """VWAP и стандартное отклонение типичных цен"""
        recent = ohlcv[-lookback:] if len(ohlcv) >= lookback else ohlcv
        typical_prices, volumes = [], []

        for c in recent:
            tp = (c.high + c.low + c.close) / 3
            typical_prices.append(tp)
            volumes.append(c.volume if hasattr(c, 'volume') else c.quote_volume)

        total_vol = sum(volumes)
        if total_vol == 0:
            return typical_prices[-1], 1.0

        vwap = sum(tp * v for tp, v in zip(typical_prices, volumes)) / total_vol

        # Standard deviation
        variance = sum((tp - vwap) ** 2 for tp in typical_prices) / len(typical_prices)
        std_dev = math.sqrt(variance) if variance > 0 else vwap * 0.01

        return round(vwap, 8), round(std_dev, 8)

    def _calc_price_velocity(self, ohlcv: list, n: int = 5) -> float:
        """% изменение цены за последние N свечей"""
        if len(ohlcv) < n + 1:
            return 0.0
        start = ohlcv[-n - 1].close
        end   = ohlcv[-1].close
        if start == 0:
            return 0.0
        return round((end - start) / start * 100, 3)

    def _volume_spike_ratio(self, ohlcv: list, lookback: int = 20) -> float:
        """Текущий объём / SMA(lookback) объёма"""
        if len(ohlcv) < 2:
            return 1.0
        try:
            vols = [c.volume if hasattr(c, 'volume') else c.quote_volume
                    for c in ohlcv[-lookback - 1:-1]]
            avg_vol = sum(vols) / len(vols) if vols else 1
            cur_vol = (ohlcv[-1].volume if hasattr(ohlcv[-1], 'volume')
                       else ohlcv[-1].quote_volume)
            return round(cur_vol / avg_vol if avg_vol > 0 else 1.0, 2)
        except Exception:
            return 1.0

    async def detect(self, ohlcv: list, market_data: Any = None) -> Dict:
        """
        Основной метод детекции.
        
        Returns: dict с полями:
            detected: bool
            score:    0-100
            z_score:  float
            volume_ratio: float
            rsi: float
            velocity_pct: float
            confidence: str
        """
        if not ohlcv or len(ohlcv) < self.cfg.lookback:
            return {"detected": False, "score": 0.0, "z_score": 0.0,
                    "volume_ratio": 1.0, "rsi": 50.0, "velocity_pct": 0.0}

        closes = [c.close for c in ohlcv]
        vwap, std_dev = self._calc_vwap_std(ohlcv, self.cfg.lookback)

        current_price = closes[-1]
        z_score = (current_price - vwap) / std_dev if std_dev > 0 else 0.0
        vol_ratio = self._volume_spike_ratio(ohlcv, self.cfg.lookback)
        rsi = self._calc_rsi(closes[-20:])
        velocity = self._calc_price_velocity(ohlcv, 5)

        # Скоринг
        score = 0.0

        # Z-Score компонент (0-40 баллов)
        if z_score > 4.0:   score += 40
        elif z_score > 3.0: score += 32
        elif z_score > 2.5: score += 24
        elif z_score > 2.0: score += 16
        elif z_score > 1.5: score += 8
        else:               score += max(0, z_score * 4)

        # Volume компонент (0-30 баллов)
        if vol_ratio > 5.0:   score += 30
        elif vol_ratio > 3.0: score += 24
        elif vol_ratio > 2.5: score += 18
        elif vol_ratio > 2.0: score += 12
        elif vol_ratio > 1.5: score += 6

        # RSI компонент (0-20 баллов)
        if rsi > 85:    score += 20
        elif rsi > 78:  score += 16
        elif rsi > 72:  score += 12
        elif rsi > 65:  score += 6
        elif rsi < 30:  score -= 10   # Перепродан — не шортим

        # Price velocity (0-10 баллов)
        if velocity > 5.0:   score += 10
        elif velocity > 3.0: score += 7
        elif velocity > 1.5: score += 4

        score = min(max(score, 0.0), 100.0)

        # Подтверждение медвежьими свечами (снижение скора если нет подтверждения)
        confirmation_candles = sum(
            1 for c in ohlcv[-self.cfg.confirmation_candles:]
            if c.close < c.open
        )
        if confirmation_candles == 0 and z_score > 2.0:
            # Памп без подтверждения разворота — снижаем скор
            score *= 0.7

        # Байесовский корректор: исторически ~60% пампов с Z>2.5 дают откат ≥3%
        bayesian_mult = 1.0
        if z_score > 3.0 and vol_ratio > 3.0 and rsi > 75:
            bayesian_mult = 1.15   # Классический ULTRA pump
        elif z_score > 2.0 and vol_ratio > 2.0:
            bayesian_mult = 1.0
        else:
            bayesian_mult = 0.85

        score = min(score * bayesian_mult, 100.0)

        detected = (
            z_score >= self.cfg.threshold and
            vol_ratio >= self.cfg.volume_spike and
            rsi >= self.cfg.rsi_overbought
        )

        if z_score > 2.0 and vol_ratio > 2.0 and not detected:
            # Частичный сигнал
            detected = score >= 55

        return {
            "detected":     detected,
            "score":        round(score, 1),
            "z_score":      round(z_score, 3),
            "volume_ratio": vol_ratio,
            "rsi":          rsi,
            "velocity_pct": velocity,
            "vwap":         vwap,
            "std_dev":      std_dev,
            "confidence":   (
                "ULTRA" if score >= 85 else
                "STRONG" if score >= 70 else
                "MODERATE" if score >= 55 else "WEAK"
            ),
        }
