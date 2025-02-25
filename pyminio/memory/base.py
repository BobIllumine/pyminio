from abc import ABC, abstractmethod
import asyncio
from collections.abc import AsyncIterator
import datetime
from typing import Any, Generic, Self, TypeVar
from umini.logger import Logger, LoggedObject

class CacheStats:
    def __init__(self, size: int = 0):
        self.hit_count: int = 0
        self.last_hit: datetime.datetime = datetime.datetime.now()
        self.size: int = size
        self.recency: float = 1.0
        self.size_factor: float = 1.0
        self._score: float = 0.5
    
    def hit(self, size: int | None = None):
        self.hit_count += 1
        self.last_hit = datetime.datetime.now()
        if size:
            self.size = size

    @property
    def score(self):
        self.recency = 1.0 / (1.0 + (datetime.datetime.now() - self.last_hit).total_seconds() / 3600)
        self.size_factor = 1.0 / (1.0 + self.size / (1024 * 1024))
        self._score = (0.5 * self.hit_count) + (0.3 * self.recency) + (0.2 * self.size_factor)
        return self._score
    

class MemoryCell(ABC, LoggedObject):
    @abstractmethod
    def __init__(
        self,
        key: str,
        logger: Logger = Logger(),
        entity: str | bytes | None = None,
    ):
        super(LoggedObject, self).__init__(logger)
        self.key = key
        self.entity = entity
        size = len(entity) if isinstance(entity, bytes) else 0
        self.stats = CacheStats(size)

    @abstractmethod
    async def __get__(self) -> bytes:
        ...

    @abstractmethod
    async def __set__(self, value: bytes):
        ...

    @abstractmethod
    async def __del__(self):
        ...

    @abstractmethod
    async def __sizeof__(self) -> int:
        ...
    
    @abstractmethod
    def morph(self, to: type['MemoryCell']) -> 'MemoryCell':
        ...

T = TypeVar('T', bound='MemoryCell')

class MemoryManager(Generic[T], ABC, LoggedObject):
    @abstractmethod
    def __init__(self, capacity: int = 1024 * 1024 * 1024, logger: Logger = Logger()):
        super(LoggedObject, self).__init__(logger)
        self._capacity = capacity
        self._cells: dict[str, T] = {}
        self._size: int = 0
        self._rerank_event = asyncio.Event()
        self._running = True
        self._periodic_task: asyncio.Task | None = None
        self._forced_task: asyncio.Task | None = None

    @property
    def used(self) -> int:
        return self._size
    
    @property
    def free(self) -> int:
        return self._capacity - self._size
    
    @property
    def capacity(self) -> int:
        return self._capacity
    
    def __aenter__(self) -> Self:
        self.start()
        return self
    
    def __aexit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        self._running = True
        self._periodic_task = asyncio.create_task(self._periodic_rerank())
        self._forced_task = asyncio.create_task(self._forced_rerank())

    def stop(self):
        self._running = False
        self._rerank_event.clear()
        if self._periodic_task:
            self._periodic_task.cancel()
        if self._forced_task:
            self._forced_task.cancel()
    
    @abstractmethod
    async def promote_request(self, memory_cell: MemoryCell) -> T:
        ...

    @abstractmethod
    async def __getitem__(self, key: str) -> T:
        ...

    @abstractmethod
    async def __setitem__(self, key: str, value: T):
        ...

    @abstractmethod
    async def __delitem__(self, key: str):
        ...

    @abstractmethod
    async def __sizeof__(self) -> int:
        ...

    @abstractmethod
    async def __contains__(self, key: str) -> bool:
        ...

    @abstractmethod
    async def __iter__(self) -> AsyncIterator[T]:
        ...

    @abstractmethod
    async def __len__(self) -> int:
        ...

    @abstractmethod
    async def __sizeof__(self) -> int:
        ...

    @abstractmethod
    async def _forced_rerank(self):
        ...
    
    @abstractmethod
    async def _periodic_rerank(self):
        ...
