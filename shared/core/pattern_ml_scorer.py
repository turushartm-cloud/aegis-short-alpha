"""
Pattern ML Scorer v1.0
=======================
Байесовский корректор весов паттернов на основе исторических win/loss данных.

Алгоритм:
1. Читает {bot}:all_trades из Redis (последние 10k сделок)
2. Группирует по паттерну → вычисляет win_rate и avg_pnl
3. Возвращает бонус/штраф к score: хорошие паттерны +5..+15, плохие -5..-10
4. Кэширует результат в Redis 24ч — не пересчитывает каждый скан

Использование:
    from shared.core.pattern_ml_scorer import PatternMLScorer
    scorer = PatternMLScorer(redis_client, bot_type="short")
    bonus, reason = scorer.get_bonus(pattern_names)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("aegis.pattern_ml")

# Минимум сделок по паттерну чтобы доверять статистике
# Управляется через ENV PATTERN_ML_MIN_TRADES (default=3 — бот новый, данных мало)
MIN_TRADES_FOR_CONFIDENCE = int(os.getenv("PATTERN_ML_MIN_TRADES", "3"))
# Бонус диапазоны
MAX_BONUS  = int(os.getenv("PATTERN_ML_MAX_BONUS", "15"))
MAX_PENALTY = -int(os.getenv("PATTERN_ML_MAX_PENALTY", "15"))
# TTL кэша в секундах (24ч → ENV PATTERN_ML_CACHE_TTL)
CACHE_TTL = int(os.getenv("PATTERN_ML_CACHE_TTL", "86400"))
# Минимальный win-rate считается «хорошим»
GOOD_WIN_RATE  = float(os.getenv("PATTERN_ML_GOOD_WIN_RATE", "0.60"))
BAD_WIN_RATE   = float(os.getenv("PATTERN_ML_BAD_WIN_RATE", "0.35"))


class PatternMLScorer:
    """
    Статистический корректор весов паттернов.
    Читает историю сделок и вычисляет win_rate по каждому паттерну.
    """

    _stats_cache: Optional[Dict] = None  # in-memory, сбрасывается раз в CACHE_TTL

    def __init__(self, redis_client, bot_type: str = "short"):
        self.redis    = redis_client
        self.bot_type = bot_type
        self._cache_key = f"{bot_type}:pattern_ml_stats"

    def _load_stats(self) -> Dict[str, Dict]:
        """Загружает или вычисляет статистику паттернов."""
        # 1. Пробуем Redis кэш
        try:
            raw = self.redis.cache_get(self._cache_key)
            if raw and isinstance(raw, dict):
                return raw
        except Exception:
            pass

        # 2. Читаем историю сделок
        stats: Dict[str, Dict] = {}
        try:
            items = self.redis.client.lrange(f"{self.bot_type}:all_trades", 0, 4999)
            for raw_item in items:
                try:
                    trade = json.loads(raw_item)
                    pattern = trade.get("pattern", "")
                    pnl     = trade.get("pnl", trade.get("pnl_pct", 0)) or 0
                    if not pattern:
                        continue
                    # Нормализуем: убираем суффиксы _4H, _1D
                    base_pattern = pattern.replace("_4H", "").replace("_1D", "")
                    if base_pattern not in stats:
                        stats[base_pattern] = {"wins": 0, "losses": 0, "total_pnl": 0.0}
                    if pnl > 0:
                        stats[base_pattern]["wins"]  += 1
                    else:
                        stats[base_pattern]["losses"] += 1
                    stats[base_pattern]["total_pnl"] += pnl
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"[PatternML] Redis read error: {e}")
            return {}

        # Добавляем производные метрики
        for name, s in stats.items():
            total = s["wins"] + s["losses"]
            s["total"] = total
            s["win_rate"] = round(s["wins"] / total, 3) if total > 0 else 0.5
            s["avg_pnl"]  = round(s["total_pnl"] / total, 3) if total > 0 else 0.0

        # 3. Сохраняем в Redis кэш на 24ч
        try:
            self.redis.cache_set(self._cache_key, stats, ttl=CACHE_TTL)
        except Exception:
            pass

        total_patterns = len(stats)
        total_trades   = sum(s["total"] for s in stats.values())
        logger.info(f"[PatternML] Загружено: {total_patterns} паттернов, {total_trades} сделок")
        return stats

    def get_bonus(self, patterns: List[str]) -> Tuple[int, str]:
        """
        Возвращает (bonus_points, reason_string) для списка паттернов сигнала.

        Логика:
        - Если данных мало (<MIN_TRADES) → нейтрально (0)
        - win_rate > GOOD_WIN_RATE → бонус пропорционально win_rate
        - win_rate < BAD_WIN_RATE  → штраф
        """
        if not patterns:
            return 0, ""

        stats = self._load_stats()
        if not stats:
            return 0, ""

        best_bonus  = 0
        best_reason = ""

        for p in patterns:
            base = p.replace("_4H", "").replace("_1D", "")
            s = stats.get(base)
            if not s or s["total"] < MIN_TRADES_FOR_CONFIDENCE:
                continue

            wr  = s["win_rate"]
            tot = s["total"]
            confidence = min(1.0, tot / 20)  # до 20 сделок — частичное доверие

            if wr >= GOOD_WIN_RATE:
                # Бонус: от +5 до +MAX_BONUS, пропорционально win_rate и confidence
                raw_bonus = int((wr - GOOD_WIN_RATE) / (1.0 - GOOD_WIN_RATE) * MAX_BONUS)
                bonus = max(5, int(raw_bonus * confidence))
                bonus = min(bonus, MAX_BONUS)
                if bonus > best_bonus:
                    best_bonus  = bonus
                    best_reason = (f"PatternML: {base} wr={wr:.0%} "
                                   f"({s['wins']}W/{s['losses']}L/{tot}) +{bonus}")

            elif wr <= BAD_WIN_RATE:
                # Штраф: от -3 до MAX_PENALTY
                raw_pen = int((BAD_WIN_RATE - wr) / BAD_WIN_RATE * abs(MAX_PENALTY))
                penalty = -max(3, int(raw_pen * confidence))
                penalty = max(penalty, MAX_PENALTY)
                if penalty < best_bonus or best_bonus == 0:
                    best_bonus  = penalty
                    best_reason = (f"PatternML: {base} wr={wr:.0%} "
                                   f"({s['wins']}W/{s['losses']}L/{tot}) {penalty}")

        return best_bonus, best_reason

    def invalidate_cache(self):
        """Сбросить кэш статистики (при добавлении новой сделки)."""
        try:
            self.redis.client.delete(self._cache_key)
        except Exception:
            pass

    def get_summary(self) -> List[Dict]:
        """Топ паттернов для дашборда или /stats команды."""
        stats = self._load_stats()
        result = []
        for name, s in stats.items():
            if s["total"] < MIN_TRADES_FOR_CONFIDENCE:
                continue
            result.append({
                "pattern":  name,
                "win_rate": s["win_rate"],
                "wins":     s["wins"],
                "losses":   s["losses"],
                "total":    s["total"],
                "avg_pnl":  s["avg_pnl"],
            })
        return sorted(result, key=lambda x: x["win_rate"], reverse=True)


# Singleton factory
_scorers: Dict[str, PatternMLScorer] = {}

def get_pattern_ml_scorer(redis_client, bot_type: str = "short") -> PatternMLScorer:
    global _scorers
    if bot_type not in _scorers:
        _scorers[bot_type] = PatternMLScorer(redis_client, bot_type)
    return _scorers[bot_type]
