import os
from typing import Any
import io

import pandas as pd

def get_file_size(file_path: str) -> int:
    return os.path.getsize(file_path)

def bytes_to_io(data: bytes) -> io.BytesIO:
    return io.BytesIO(data)

def io_to_bytes(data: io.BytesIO) -> bytes:
    return data.getvalue()

def pd_from_bytes(data: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(data))

if __name__ == '__main__':
    data = b'a,b,c\n1,2,3\n4,5,6'
    df = pd_from_bytes(data)
