"""Aegis Short Alpha — Core Package"""
from .signal_engine import AegisSignalEngine, SignalStrength, AegisSignal
from .smart_dca import SmartDCAEngine, GridConfig, GridType
from .risk_manager import AegisRiskManager, RiskLimits
from .performance_tracker import PerformanceTracker, TradeRecord

__all__ = [
    "AegisSignalEngine", "SignalStrength", "AegisSignal",
    "SmartDCAEngine", "GridConfig", "GridType",
    "AegisRiskManager", "RiskLimits",
    "PerformanceTracker", "TradeRecord",
]
