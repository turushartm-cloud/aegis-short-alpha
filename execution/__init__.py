# Execution module — lazy imports to avoid cascade on first load
# (position_tracker.py imports execution.micro_trailing_stop directly;
#  AutoTrader/TradeManager loaded on demand by callers that need them)

__all__ = ["AutoTrader", "TradeConfig", "TradeManager"]


def __getattr__(name):
    if name in ("AutoTrader", "TradeConfig"):
        from .auto_trader import AutoTrader, TradeConfig
        globals()["AutoTrader"] = AutoTrader
        globals()["TradeConfig"] = TradeConfig
        return globals()[name]
    if name == "TradeManager":
        from .trade_manager import TradeManager
        globals()["TradeManager"] = TradeManager
        return TradeManager
    raise AttributeError(f"module 'execution' has no attribute {name!r}")
