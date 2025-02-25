from pyminio.memory.base import MemoryCell, MemoryManager
from typing import Any, Callable
from asyncio import Future

class CloudCell(MemoryCell):
    def __init__(
        self, 
        key: str, 
        entity: tuple[list[Any], dict[str, Any]], 
        call_fn: Callable | Future, 
        *args, 
        **kwargs
    ):
        super().__init__(key, entity, *args, **kwargs)
        self.entity = entity
        self.call_fn = call_fn

    async def __get__(self) -> bytes:
        await self.call_fn(self.entity)
    
    async def __set__(self, value: bytes):
        self.entity = value

    async def __del__(self):
        self.entity = None

    async def __sizeof__(self) -> int:
        return len(self.entity) if self.entity else 0

class CloudManager(MemoryManager):
    def __init__(self, capacity: int = 1024 * 1024 * 1024, *args, **kwargs):
        super().__init__(capacity, *args, **kwargs)
        self.cells: dict[str, CloudCell] = {}

    async def __getitem__(self, key: str) -> CloudCell:
        return self.cells[key]