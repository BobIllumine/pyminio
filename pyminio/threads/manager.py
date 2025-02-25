import threading
import asyncio


class AsyncThreadManager:
    def __init__(self):
        self._loop = None
        self._thread = None