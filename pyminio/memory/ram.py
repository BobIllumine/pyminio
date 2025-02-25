from pyminio.memory.base import MemoryCell, MemoryManager

class RAMCell(MemoryCell):
    def __init__(self, key: str, entity: bytes):
        super().__init__(key, entity)

    def __get__(self) -> bytes:
        return self.entity
    
    def __set__(self, value: bytes):
        self.entity = value

    def __del__(self):
        self.entity = None

    def __sizeof__(self) -> int:
        return len(self.entity) if self.entity else 0
    
class RAMManager(MemoryManager):
    def __init__(self, capacity: int = 1024 * 1024 * 1024, *args, **kwargs):
        super().__init__(capacity, *args, **kwargs)
        self.cells: dict[str, RAMCell] = {}

    async def __getitem__(self, key: str) -> RAMCell:
        return self.cells[key]
    