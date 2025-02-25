import minio
from typing import Literal
from abc import ABC, abstractmethod
from umini.logger import Logger, LoggedObject
from pyminio.base.errors import BackendNotSupportedError

class Storage(ABC, LoggedObject):

    @abstractmethod
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        backend: Literal['minio', 'boto3'] = 'minio', 
        logger: Logger = Logger()
    ):
        if backend != 'minio':
            raise BackendNotSupportedError(backend)
        super().__init__(logger)


    @abstractmethod
    def _get_client(self):
        ...