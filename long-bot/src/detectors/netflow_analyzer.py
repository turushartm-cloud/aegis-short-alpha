"""
NetflowAnalyzerLong v1.0
Анализ потоков монет на/с бирж (Coinglass).

Логика:
  Outflow (монеты УХОДЯТ с биржи) = институциональное накопление = BULLISH → LONG
  Inflow  (монеты ПРИХОДЯТ на биржу) = распределение / подготовка к продаже = BEARISH

Используется в AegisLongSignalEngine как компонент OI/накопление.
"""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class NetflowAnalyzerLong:
    """
    Анализирует exchange netflow с Coinglass.
    Возвращает score 0-100: выше = сильнее outflow = накопление = LONG.
    """

    def __init__(self, coinglass_client=None):
        self.client = coinglass_client

    async def analyze(self, symbol: str) -> Dict[str, Any]:
        """
        Возвращает dict совместимый с AegisLongSignalEngine:
          score:    0-100
          reasons:  list[str]
          metadata: {"signal": str, "total_netflow": float, ...}
        """
        result: Dict[str, Any] = {
            "score": 40,
            "reasons": [],
            "metadata": {"signal": "neutral"},
        }

        if not self.client:
            return result

        try:
            nf = await self.client.get_exchange_netflow(symbol, period="8h")
            if not nf:
                return result

            total = nf.get("total_netflow", 0.0)
            inflow  = nf.get("inflow",  0.0)
            outflow = nf.get("outflow", 0.0)
            ex_cnt  = nf.get("exchange_count", 0)

            # total_netflow: inflow - outflow
            # Отрицательный = outflow превышает inflow = BULLISH для LONG
            net = total  # отрицательный = outflow-dominant

            if net < -5_000_000:       # > $5M outflow
                score, signal = 90, "strong_outflow"
                result["reasons"].append(
                    f"Институциональный outflow: ${abs(net)/1e6:.1f}M с бирж — накопление"
                )
            elif net < -1_000_000:     # > $1M outflow
                score, signal = 75, "outflow"
                result["reasons"].append(
                    f"Outflow с бирж: ${abs(net)/1e6:.1f}M — накопление"
                )
            elif net < -200_000:       # > $200K outflow
                score, signal = 60, "mild_outflow"
                result["reasons"].append(
                    f"Слабый outflow: ${abs(net)/1e3:.0f}K с бирж"
                )
            elif net > 5_000_000:      # > $5M inflow = сильное давление продаж
                score, signal = 10, "strong_inflow"
                result["reasons"].append(
                    f"Сильный inflow на биржи: ${net/1e6:.1f}M — распределение"
                )
            elif net > 1_000_000:
                score, signal = 25, "inflow"
                result["reasons"].append(
                    f"Inflow на биржи: ${net/1e6:.1f}M — распределение"
                )
            else:
                score, signal = 40, "neutral"

            result["score"] = score
            result["metadata"] = {
                "signal":       signal,
                "total_netflow": round(net, 0),
                "inflow":        round(inflow, 0),
                "outflow":       round(outflow, 0),
                "exchanges":     ex_cnt,
            }

        except Exception as e:
            logger.debug(f"[NetflowAnalyzer] Error for {symbol}: {e}")

        return result
