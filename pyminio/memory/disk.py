import os
from os import PathLike
from pyminio.memory.base import MemoryCell, MemoryManager
import io

class DiskCell(MemoryCell):
    def __init__(self, key: str, entity: str, *args, **kwargs):
        super().__init__(key, entity, *args, **kwargs)

    async def __get__(self) -> io.BytesIO:
        return io.BytesIO(self.entity)
    
    async def __set__(self, value: str | PathLike):
        self.entity = value

    async def __del__(self):
        self.entity = None

    async def __sizeof__(self) -> int:
        return os.path.getsize(self.entity) if self.entity else 0
    
class DiskManager(MemoryManager):
    def __init__(self, capacity: int = 1024 * 1024 * 1024, *args, **kwargs):
        super().__init__(capacity, *args, **kwargs)
        self.cells: dict[str, DiskCell] = {}

    async def __getitem__(self, key: str) -> DiskCell:
        return self.cells[key]
