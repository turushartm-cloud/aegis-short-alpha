"""
Signal Queue System
Надежная очередь сигналов с retry логикой
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum


class SignalStatus(Enum):
    """Статус сигнала"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"


@dataclass
class Signal:
    """Сигнал для обработки"""
    id: str
    symbol: str
    direction: str  # SHORT/LONG
    score: int
    price: float
    pattern: str
    indicators: Dict
    entry: float
    stop_loss: float
    take_profits: List[tuple]
    leverage: str
    risk: str
    created_at: str
    status: SignalStatus = SignalStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    error: Optional[str] = None
    processed_at: Optional[str] = None


class SignalQueue:
    """
    Очередь сигналов с гарантией доставки
    
    Features:
    - Сохранение в файл (не теряем при рестарте)
    - Retry с экспоненциальной задержкой
    - Приоритизация по score
    - Dead letter queue для failed signals
    """
    
    def __init__(self, data_dir: str = None, max_retries: int = 3):
        self.data_dir = data_dir or os.getenv("DATA_DIR", "/tmp")
        self.queue_file = os.path.join(self.data_dir, "signal_queue.json")
        self.failed_file = os.path.join(self.data_dir, "failed_signals.json")
        
        self.max_retries = max_retries
        self.queue: List[Signal] = self._load_queue()
        self.failed: List[Signal] = self._load_failed()
        
        self._running = False
        self._processor: Optional[Callable] = None
        self._task: Optional[asyncio.Task] = None
    
    def _load_queue(self) -> List[Signal]:
        """Загрузить очередь из файла"""
        try:
            with open(self.queue_file, 'r') as f:
                data = json.load(f)
                return [Signal(**item) for item in data]
        except:
            return []
    
    def _save_queue(self):
        """Сохранить очередь в файл"""
        os.makedirs(self.data_dir, exist_ok=True)
        data = []
        for s in self.queue:
            d = s.__dict__.copy()
            d['status'] = s.status.value
            d['indicators'] = s.indicators if isinstance(s.indicators, dict) else {}
            data.append(d)
        
        with open(self.queue_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def _load_failed(self) -> List[Signal]:
        """Загрузить failed сигналы"""
        try:
            with open(self.failed_file, 'r') as f:
                data = json.load(f)
                return [Signal(**item) for item in data]
        except:
            return []
    
    def _save_failed(self):
        """Сохранить failed сигналы"""
        os.makedirs(self.data_dir, exist_ok=True)
        data = []
        for s in self.failed:
            d = s.__dict__.copy()
            d['status'] = s.status.value
            data.append(d)
        
        with open(self.failed_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def add_signal(self, signal: Signal) -> bool:
        """
        Добавить сигнал в очередь
        
        Returns:
            bool: True если добавлен успешно
        """
        # Проверяем дубликаты
        for existing in self.queue:
            if (existing.symbol == signal.symbol and 
                existing.direction == signal.direction and
                existing.status == SignalStatus.PENDING):
                # Обновляем score если новый выше
                if signal.score > existing.score:
                    existing.score = signal.score
                    existing.indicators = signal.indicators
                    self._save_queue()
                return True
        
        self.queue.append(signal)
        self._save_queue()
        
        print(f"📡 Signal queued: {signal.symbol} {signal.direction} (Score: {signal.score})")
        return True
    
    def add_from_detection(self,
                          symbol: str,
                          direction: str,
                          score: int,
                          price: float,
                          pattern: str,
                          indicators: Dict,
                          entry: float,
                          stop_loss: float,
                          take_profits: List[tuple],
                          leverage: str,
                          risk: str) -> str:
        """Создать сигнал из данных детектора"""
        signal_id = f"{symbol}_{direction}_{int(datetime.utcnow().timestamp())}"
        
        signal = Signal(
            id=signal_id,
            symbol=symbol,
            direction=direction,
            score=score,
            price=price,
            pattern=pattern,
            indicators=indicators,
            entry=entry,
            stop_loss=stop_loss,
            take_profits=take_profits,
            leverage=leverage,
            risk=risk,
            created_at=datetime.utcnow().isoformat()
        )
        
        self.add_signal(signal)
        return signal_id
    
    async def start_processing(self, processor: Callable[[Signal], bool]):
        """
        Запустить обработку очереди
        
        Args:
            processor: Функция для обработки сигнала, должна вернуть True/False
        """
        self._processor = processor
        self._running = True
        self._task = asyncio.create_task(self._process_loop())
        print("🚀 Signal queue processor started")
    
    async def stop(self):
        """Остановить обработку"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        print("🛑 Signal queue processor stopped")
    
    async def _process_loop(self):
        """Главный цикл обработки"""
        while self._running:
            try:
                # Сортируем по score (высокие первыми)
                pending = [s for s in self.queue if s.status == SignalStatus.PENDING]
                pending.sort(key=lambda x: x.score, reverse=True)
                
                for signal in pending[:5]:  # Обрабатываем топ-5
                    signal.status = SignalStatus.PROCESSING
                    self._save_queue()
                    
                    try:
                        success = await self._process_signal(signal)
                        
                        if success:
                            signal.status = SignalStatus.COMPLETED
                            signal.processed_at = datetime.utcnow().isoformat()
                            print(f"✅ Signal processed: {signal.symbol} {signal.direction}")
                        else:
                            await self._handle_failure(signal, "Processor returned False")
                    
                    except Exception as e:
                        await self._handle_failure(signal, str(e))
                    
                    self._save_queue()
                    await asyncio.sleep(0.1)  # Небольшая пауза между сигналами
                
                # Очистка completed сигналов старше 24 часов
                self._cleanup_old_signals()
                
                await asyncio.sleep(1)  # Проверяем очередь каждую секунду
                
            except Exception as e:
                print(f"❌ Queue processing error: {e}")
                await asyncio.sleep(5)
    
    async def _process_signal(self, signal: Signal) -> bool:
        """Обработать один сигнал"""
        if not self._processor:
            return False
        
        # Вызываем процессор
        try:
            return await self._processor(signal)
        except Exception as e:
            print(f"❌ Signal processor error: {e}")
            return False
    
    async def _handle_failure(self, signal: Signal, error: str):
        """Обработать неудачу"""
        signal.attempts += 1
        signal.error = error
        
        if signal.attempts >= signal.max_attempts:
            signal.status = SignalStatus.FAILED
            self.failed.append(signal)
            self._save_failed()
            print(f"❌ Signal failed after {signal.max_attempts} attempts: {signal.symbol}")
        else:
            signal.status = SignalStatus.RETRY
            # Экспоненциальная задержка: 5s, 10s, 20s
            delay = 5 * (2 ** (signal.attempts - 1))
            print(f"⏳ Signal retry {signal.attempts}/{signal.max_attempts} for {signal.symbol} in {delay}s")
            await asyncio.sleep(delay)
            signal.status = SignalStatus.PENDING
    
    def _cleanup_old_signals(self):
        """Очистить старые сигналы"""
        cutoff = datetime.utcnow().timestamp() - 24 * 3600
        
        old_completed = [s for s in self.queue 
                        if s.status == SignalStatus.COMPLETED and 
                        datetime.fromisoformat(s.created_at).timestamp() < cutoff]
        
        for s in old_completed:
            self.queue.remove(s)
        
        if old_completed:
            self._save_queue()
    
    def get_stats(self) -> Dict:
        """Получить статистику очереди"""
        pending = len([s for s in self.queue if s.status == SignalStatus.PENDING])
        processing = len([s for s in self.queue if s.status == SignalStatus.PROCESSING])
        completed = len([s for s in self.queue if s.status == SignalStatus.COMPLETED])
        failed = len([s for s in self.queue if s.status == SignalStatus.FAILED])
        retry = len([s for s in self.queue if s.status == SignalStatus.RETRY])
        
        return {
            "pending": pending,
            "processing": processing,
            "completed": completed,
            "failed": failed,
            "retry": retry,
            "total_in_queue": len(self.queue),
            "total_failed_permanently": len(self.failed),
            "avg_score_pending": sum(s.score for s in self.queue if s.status == SignalStatus.PENDING) / pending if pending > 0 else 0
        }
    
    def retry_failed(self) -> int:
        """Повторно запустить failed сигналы"""
        count = 0
        for signal in self.failed:
            signal.status = SignalStatus.PENDING
            signal.attempts = 0
            signal.error = None
            self.queue.append(signal)
            count += 1
        
        self.failed.clear()
        self._save_queue()
        self._save_failed()
        
        print(f"🔄 Retrying {count} failed signals")
        return count


# Singleton
_signal_queue = None

def get_signal_queue() -> SignalQueue:
    """Получить singleton SignalQueue"""
    global _signal_queue
    if _signal_queue is None:
        _signal_queue = SignalQueue()
    return _signal_queue
