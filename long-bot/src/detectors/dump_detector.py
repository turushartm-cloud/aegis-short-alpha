"""
Dump Exhaustion Detector v1.0 — Aegis Long Alpha
Зеркало PumpDetector: ищет ДНО = истощение продавцов для LONG входа.

Логика:
  1. Z-Score < -2.5σ ниже VWAP (цена экстремально перепродана)
  2. Volume Spike = продавцы иссякают (climax selling = дно)
  3. RSI < 25 (экстремальная перепроданность)
  4. Price Velocity отрицательная (скорость падения)
  5. Байесовская оценка вероятности разворота вверх

Сигнал LONG:
  ULTRA:    Z < -3.0 + Vol > 3x + RSI < 20  (capitulation)
  STRONG:   Z < -2.5 + Vol > 2.5x + RSI < 25
  MODERATE: Z < -2.0 + Vol > 2x              (selling climax)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger("aegis.dump_detector")


@dataclass
class DumpDetectorConfig:
    threshold:        float = 2.5   # |Z-Score| порог (отрицательный)
    volume_spike:     float = 2.5   # Volume/SMA порог
    rsi_oversold:     float = 25.0  # RSI порог перепроданности
    lookback:         int   = 20    # Период для VWAP/SMA
    confirmation_candles: int = 2   # Свечей подтверждения разворота


class DumpExhaustionDetector:
    """
    Детектор истощения продавцов (Selling Climax) для LONG входа.
    Ищет точки капитуляции — максимальный страх = минимальная цена.
    """

    def __init__(self, config: Optional[DumpDetectorConfig] = None):
        self.cfg = config or DumpDetectorConfig()

    def _calc_rsi(self, closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        gains  = [d if d > 0 else 0.0 for d in deltas[-period:]]
        losses = [-d if d < 0 else 0.0 for d in deltas[-period:]]
        avg_g  = sum(gains) / period
        avg_l  = sum(losses) / period
        if avg_l == 0:
            return 100.0
        return round(100.0 - 100.0 / (1.0 + avg_g / avg_l), 2)

    def _calc_vwap_std(self, ohlcv: list, lookback: int):
        recent = ohlcv[-lookback:] if len(ohlcv) >= lookback else ohlcv
        tps, vols = [], []
        for c in recent:
            tps.append((c.high + c.low + c.close) / 3)
            vols.append(getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0))

        total_vol = sum(vols)
        if total_vol == 0:
            return tps[-1], tps[-1] * 0.01

        vwap = sum(tp * v for tp, v in zip(tps, vols)) / total_vol
        variance = sum((tp - vwap) ** 2 for tp in tps) / len(tps)
        std_dev = math.sqrt(variance) if variance > 0 else vwap * 0.01
        return round(vwap, 8), round(std_dev, 8)

    def _calc_price_velocity(self, ohlcv: list, n: int = 5) -> float:
        """% изменение цены за N свечей (отрицательное = дамп)"""
        if len(ohlcv) < n + 1:
            return 0.0
        start = ohlcv[-n - 1].close
        end   = ohlcv[-1].close
        return round((end - start) / start * 100, 3) if start > 0 else 0.0

    def _volume_spike_ratio(self, ohlcv: list, lookback: int = 20) -> float:
        if len(ohlcv) < 2:
            return 1.0
        try:
            vols = [getattr(c, 'volume', 0) or getattr(c, 'quote_volume', 0)
                    for c in ohlcv[-lookback - 1:-1]]
            avg_vol = sum(vols) / len(vols) if vols else 1
            cur_vol = getattr(ohlcv[-1], 'volume', 0) or getattr(ohlcv[-1], 'quote_volume', 0)
            return round(cur_vol / avg_vol if avg_vol > 0 else 1.0, 2)
        except Exception:
            return 1.0

    async def detect(self, ohlcv: list, market_data: Any = None) -> Dict:
        """
        Детекция истощения продавцов.

        Returns: dict с полями:
            detected: bool        — сигнал капитуляции
            score:    0-100       — сила сигнала
            z_score:  float       — отрицательный (цена ниже VWAP)
            volume_ratio: float
            rsi: float
            velocity_pct: float   — отрицательный при дампе
            confidence: str
        """
        if not ohlcv or len(ohlcv) < self.cfg.lookback:
            return {"detected": False, "score": 0.0, "z_score": 0.0,
                    "volume_ratio": 1.0, "rsi": 50.0, "velocity_pct": 0.0}

        closes = [c.close for c in ohlcv]
        vwap, std_dev = self._calc_vwap_std(ohlcv, self.cfg.lookback)

        current_price = closes[-1]
        # Z-Score отрицательный при дампе
        z_score = (current_price - vwap) / std_dev if std_dev > 0 else 0.0
        vol_ratio = self._volume_spike_ratio(ohlcv, self.cfg.lookback)
        rsi       = self._calc_rsi(closes[-20:])
        velocity  = self._calc_price_velocity(ohlcv, 5)

        # Z-Score по абсолютному значению (для LONG нужен z < -threshold)
        abs_z = abs(z_score)
        is_below_vwap = z_score < 0   # Цена ниже VWAP — нужно для LONG

        score = 0.0

        # Z-Score компонент (0-55 баллов) — ГЛАВНЫЙ сигнал mean-reversion LONG
        # Цена экстремально ниже VWAP = перепродана = отскок вероятен
        if is_below_vwap:
            if abs_z > 4.5:   score += 55
            elif abs_z > 4.0: score += 48
            elif abs_z > 3.0: score += 38
            elif abs_z > 2.5: score += 28
            elif abs_z > 2.0: score += 18
            elif abs_z > 1.5: score += 9
            else:             score += max(0, abs_z * 5)
        else:
            score += 0  # Цена выше VWAP — не дамп

        # Volume компонент (0-30 баллов) — selling climax подтверждение
        if vol_ratio > 5.0:   score += 30
        elif vol_ratio > 3.0: score += 24
        elif vol_ratio > 2.5: score += 18
        elif vol_ratio > 2.0: score += 12
        elif vol_ratio > 1.5: score += 6

        # Price Velocity (0-10 баллов) — скорость падения
        if velocity < -5.0:   score += 10
        elif velocity < -3.0: score += 7
        elif velocity < -1.5: score += 4
        elif velocity > 1.0:  score -= 5   # Уже растёт — момент пропущен

        # RSI — вспомогательный (0-10 баллов, НЕ блокирует)
        if rsi < 15:    score += 10
        elif rsi < 20:  score += 7
        elif rsi < 25:  score += 5
        elif rsi < 30:  score += 3
        elif rsi > 70:  score -= 5   # Перекуплен при дампе — осторожно (мягко)

        score = min(max(score, 0.0), 100.0)

        # Подтверждение бычьими разворотными свечами (hammer, doji)
        reversal_candles = 0
        for c in ohlcv[-self.cfg.confirmation_candles:]:
            body = abs(c.close - c.open)
            rng  = c.high - c.low
            lower_wick = min(c.open, c.close) - c.low
            if rng > 0 and lower_wick > body and lower_wick > rng * 0.4:
                reversal_candles += 1

        if reversal_candles >= 1:
            score *= 1.15  # Молоток/пин-бар — усиливаем

        # Байесовский корректор: Z < -3.0 + Vol > 3x = капитуляция
        if abs_z > 3.0 and vol_ratio > 3.0:
            bayesian = 1.20
        elif abs_z > 2.5 and vol_ratio > 2.0:
            bayesian = 1.05
        else:
            bayesian = 0.90

        score = min(score * bayesian, 100.0)

        # PRIMARY: Z-score + Volume — RSI вспомогательный, НЕ gate
        detected = (
            z_score < -self.cfg.threshold and
            vol_ratio >= self.cfg.volume_spike
        )

        # RSI даёт бонусное подтверждение перепроданности
        if detected and rsi <= self.cfg.rsi_oversold:
            score = min(score * 1.1, 100.0)  # +10% confidence bonus

        # Частичный сигнал: abs_z > 2.0 + Vol > 1.8
        if not detected and abs_z > 2.0 and vol_ratio > 1.8:
            detected = score >= 48

        return {
            "detected":     detected,
            "score":        round(score, 1),
            "z_score":      round(z_score, 3),     # Отрицательный при дампе
            "volume_ratio": vol_ratio,
            "rsi":          rsi,
            "velocity_pct": velocity,
            "vwap":         vwap,
            "reversal_candles": reversal_candles,
            "confidence": (
                "ULTRA"    if score >= 85 else
                "STRONG"   if score >= 70 else
                "MODERATE" if score >= 55 else "WEAK"
            ),
        }
