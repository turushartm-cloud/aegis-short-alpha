"""
Enhanced Trade Manager
Управление позициями с:
- TP Splitting (6 уровня фиксации прибыли)
- Trail Stop (подтягивание SL после TP1)
- Win Rate Statistics (отслеживание по TP1/TP2/TP3/TP4)
- Scale In (добавление к позиции)
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum


class PositionStatus(Enum):
    """Статус позиции"""
    OPEN = "open"
    TP1_HIT = "tp1_hit"
    TP2_HIT = "tp2_hit"
    TP3_HIT = "tp3_hit"
    TP4_HIT = "tp4_hit"
    TP5_HIT = "tp5_hit"
    TP6_HIT = "tp6_hit"
    SL_HIT = "sl_hit"
    CLOSED = "closed"


@dataclass
class TakeProfitLevel:
    """Уровень Take Profit"""
    level: int  # 1-6
    price: float
    size_pct: float  # % позиции для закрытия (15%, 20%, 20%, 15%, 15%, 15%)
    hit: bool = False
    hit_time: Optional[str] = None
    pnl: float = 0.0


@dataclass
class TradePosition:
    """Позиция с полной информацией"""
    # ═══════════════════════════════════════════════════════════════════════
    # ОБЯЗАТЕЛЬНЫЕ ПОЛЯ (без default) - должны быть первыми!
    # ═══════════════════════════════════════════════════════════════════════
    trade_id: str
    symbol: str
    direction: str  # "SHORT" или "LONG"
    entry_price: float
    total_qty: float
    remaining_qty: float
    stop_loss: float
    take_profits: List[TakeProfitLevel]
    leverage: int
    initial_margin: float
    max_loss: float
    status: PositionStatus
    opened_at: str
    
    # ═══════════════════════════════════════════════════════════════════════
    # ОПЦИОНАЛЬНЫЕ ПОЛЯ (со default) - все в конце!
    # ═══════════════════════════════════════════════════════════════════════
    liquidation_data: Optional[Dict] = None
    be_activated: bool = False
    trail_active: bool = False
    trail_sl_price: Optional[float] = None
    tp1_hit: bool = False
    tp3_hit: bool = False
    closed_at: Optional[str] = None
    final_pnl: Optional[float] = None
    highest_tp_hit: int = 0
    scale_ins: List[Dict] = None
    avg_entry_price: Optional[float] = None
    
    def __post_init__(self):
        if self.avg_entry_price is None:
            self.avg_entry_price = self.entry_price
        if self.scale_ins is None:
            self.scale_ins = []


class TradeManager:
    """
    Менеджер торговли с продвинутыми фичами
    
    TP Splitting Strategy:
    - TP1: 40% позиции при +2-3%
    - TP2: 30% позиции при +4-6%
    - TP3: 20% позиции при +6-10%
    - TP4: 10% позиции при +10%+
    
    Trail Stop:
    - Активируется после TP1
    - SL переносится на точку безубытка + 0.5%
    - Следует за ценой с шагом 1%
    """
    
    # Настройки TP (по умолчанию для SHORT, для LONG инвертируем)
    DEFAULT_TP_LEVELS = [
        {"level": 1, "size_pct": 0.40, "price_pct": 0.025},  # 2.5%
        {"level": 2, "size_pct": 0.30, "price_pct": 0.050},  # 5%
        {"level": 3, "size_pct": 0.20, "price_pct": 0.080},  # 8%
        {"level": 4, "size_pct": 0.10, "price_pct": 0.120},  # 12%
        {"level": 5, "size_pct": 0.10, "price_pct": 0.120},  # 12%
        {"level": 6, "size_pct": 0.10, "price_pct": 0.120},  # 12%
    ]
    
    # 🆕 Trail активируется после TP2 (не TP1), на +1% от ТВХ
    TRAIL_ACTIVATION_TP = 2  # Активировать trail после TP2
    TRAIL_STEP_PCT = 0.01    # Шаг трейла 1%
    # 🆕 Увеличено с 0.5% до 1%: защищаем сделку но не выбиваем рано
    BREAKEVEN_BUFFER = float(os.getenv("TRAIL_BE_BUFFER_PCT", "0.010"))  # 1% от ТВХ
    
    def __init__(self, data_dir: str = None):
        self.data_dir = data_dir or os.getenv("DATA_DIR", "/tmp")
        self.positions_file = os.path.join(self.data_dir, "positions.json")
        self.stats_file = os.path.join(self.data_dir, "trade_stats.json")
        
        # Загружаем данные
        self.positions: Dict[str, TradePosition] = self._load_positions()
        self.stats: Dict = self._load_stats()
    
    def _load_positions(self) -> Dict[str, TradePosition]:
        """Загрузить позиции из файла"""
        try:
            with open(self.positions_file, 'r') as f:
                data = json.load(f)
                positions = {}
                for k, v in data.items():
                    # Восстанавливаем TP levels
                    tps = [TakeProfitLevel(**tp) for tp in v.pop('take_profits', [])]
                    v['take_profits'] = tps
                    v['status'] = PositionStatus(v.get('status', 'open'))
                    positions[k] = TradePosition(**v)
                return positions
        except:
            return {}
    
    def _save_positions(self):
        """Сохранить позиции в файл"""
        data = {}
        for k, v in self.positions.items():
            d = asdict(v)
            d['status'] = v.status.value
            data[k] = d
        
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.positions_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def _load_stats(self) -> Dict:
        """Загрузить статистику"""
        try:
            with open(self.stats_file, 'r') as f:
                return json.load(f)
        except:
            return {
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "tp1_hits": 0,
                "tp2_hits": 0,
                "tp3_hits": 0,
                "tp4_hits": 0,
                "sl_hits": 0,
                "total_pnl": 0.0,
                "avg_duration_min": 0,
                "win_rate": 0.0,
                "by_symbol": {}
            }
    
    def _save_stats(self):
        """Сохранить статистику"""
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2, default=str)

    def optimize_levels_with_liquidation(
        self,
        direction: str,
        entry_price: float,
        default_sl: float,
        default_tp: float,
        liq_analysis: Optional[Any],
    ) -> Tuple[float, float, List[str]]:
        """
        🆕 Оптимизирует SL/TP на основе магнитов ликвидации.
        
        Returns:
            (optimized_sl, optimized_tp, reasons)
        """
        if not liq_analysis:
            return default_sl, default_tp, []
        
        reasons = []
        sl = default_sl
        tp = default_tp
        
        try:
            if direction == "LONG":
                # TP: к ближайшему магниту выше
                if liq_analysis.nearest_above:
                    magnet_price = liq_analysis.nearest_above.price_level
                    dist_pct = abs(liq_analysis.nearest_above.distance_pct)
                    
                    if 2 <= dist_pct <= 10:  # Оптимальное расстояние
                        # Ставим TP чуть ниже магнита (0.2% запас)
                        tp = magnet_price * 0.998
                        reasons.append(f"🧲 TP у магнита +{dist_pct:.1f}%")
                
                # SL: защищаем от магнита ниже
                if liq_analysis.nearest_below:
                    magnet_price = liq_analysis.nearest_below.price_level
                    dist_pct = abs(liq_analysis.nearest_below.distance_pct)
                    
                    if dist_pct < 1.5:  # Магнит близко
                        # Ставим SL ниже магнита (0.5% запас)
                        sl = magnet_price * 0.995
                        reasons.append(f"🛡️ SL за магнитом -{dist_pct:.1f}%")
                        
            else:  # SHORT
                # TP: к ближайшему магниту ниже
                if liq_analysis.nearest_below:
                    magnet_price = liq_analysis.nearest_below.price_level
                    dist_pct = abs(liq_analysis.nearest_below.distance_pct)
                    
                    if 2 <= dist_pct <= 10:
                        tp = magnet_price * 1.002
                        reasons.append(f"🧲 TP у магнита -{dist_pct:.1f}%")
                
                # SL: защищаем от магнита выше
                if liq_analysis.nearest_above:
                    magnet_price = liq_analysis.nearest_above.price_level
                    dist_pct = abs(liq_analysis.nearest_above.distance_pct)
                    
                    if dist_pct < 1.5:
                        sl = magnet_price * 1.005
                        reasons.append(f"🛡️ SL за магнитом +{dist_pct:.1f}%")
        
        except Exception as e:
            print(f"⚠️ Liquidation optimization error: {e}")
        
        return sl, tp, reasons
    
    def create_position(self,
                       symbol: str,
                       direction: str,
                       entry_price: float,
                       qty: float,
                       stop_loss: float,
                       leverage: int = 10,
                       custom_tp_levels: List[Dict] = None) -> TradePosition:
        """
        Создать новую позицию с TP уровнями
        
        Args:
            custom_tp_levels: Пользовательские TP уровни (иначе default)
        """
        trade_id = f"{symbol}_{direction}_{int(datetime.utcnow().timestamp())}"
        
        # Рассчитываем TP уровни
        tp_levels = custom_tp_levels or self.DEFAULT_TP_LEVELS
        is_short = direction == "SHORT"
        
        take_profits = []
        for tp in tp_levels:
            if is_short:
                # Для SHORT: TP ниже entry
                tp_price = entry_price * (1 - tp["price_pct"])
            else:
                # Для LONG: TP выше entry
                tp_price = entry_price * (1 + tp["price_pct"])
            
            take_profits.append(TakeProfitLevel(
                level=tp["level"],
                price=tp_price,
                size_pct=tp["size_pct"],
                hit=False
            ))
        
        margin = (qty * entry_price) / leverage
        max_loss = abs(entry_price - stop_loss) * qty
        
        position = TradePosition(
            trade_id=trade_id,
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            total_qty=qty,
            remaining_qty=qty,
            stop_loss=stop_loss,
            take_profits=take_profits,
            leverage=leverage,
            initial_margin=margin,
            max_loss=max_loss,
            status=PositionStatus.OPEN,
            opened_at=datetime.utcnow().isoformat()
        )
        
        # Сохраняем
        self.positions[trade_id] = position
        self._save_positions()
        
        return position
    
    def _close_position(self, trade_id: str, reason: str, price: float) -> Dict:
        """Закрыть позицию"""
        pos = self.positions[trade_id]
        
        pnl = self._calculate_pnl(pos, pos.remaining_qty, price)
        
        pos.status = PositionStatus.SL_HIT if reason == "SL_HIT" else PositionStatus.CLOSED
        pos.closed_at = datetime.utcnow().isoformat()
        pos.final_pnl = pnl
        
        # Обновляем статистику
        is_win = pnl > 0
        self._update_stats_close(is_win, pnl, pos.highest_tp_hit)
        
        self._save_positions()
        
        return {
            "event": reason,
            "trade_id": trade_id,
            "symbol": pos.symbol,
            "final_pnl": pnl,
            "highest_tp": pos.highest_tp_hit,
            "price": price,
            "duration": self._calc_duration(pos)
        }
    
    def _calculate_pnl(self, pos: TradePosition, qty: float, price: float) -> float:
        """Рассчитать P&L"""
        if pos.direction == "SHORT":
            return (pos.avg_entry_price - price) * qty
        else:
            return (price - pos.avg_entry_price) * qty
    
    def _calc_duration(self, pos: TradePosition) -> int:
        """Рассчитать длительность в минутах"""
        opened = datetime.fromisoformat(pos.opened_at)
        closed = datetime.fromisoformat(pos.closed_at) if pos.closed_at else datetime.utcnow()
        return int((closed - opened).total_seconds() / 60)
    
    def _update_stats_tp_hit(self, tp_level: int):
        """Обновить статистику TP"""
        key = f"tp{tp_level}_hits"
        self.stats[key] = self.stats.get(key, 0) + 1
        self._save_stats()
    
    def _update_stats_close(self, is_win: bool, pnl: float, highest_tp: int):
        """Обновить статистику закрытия"""
        self.stats["total_trades"] += 1
        
        if is_win:
            self.stats["wins"] += 1
        else:
            self.stats["losses"] += 1
            if highest_tp == 0:
                self.stats["sl_hits"] = self.stats.get("sl_hits", 0) + 1
        
        self.stats["total_pnl"] += pnl
        
        # Пересчитываем win rate
        total = self.stats["total_trades"]
        wins = self.stats["wins"]
        self.stats["win_rate"] = (wins / total * 100) if total > 0 else 0
        
        self._save_stats()
    


# Singleton
_trade_manager = None

def get_trade_manager() -> TradeManager:
    """Получить singleton TradeManager"""
    global _trade_manager
    if _trade_manager is None:
        _trade_manager = TradeManager()
    return _trade_manager
