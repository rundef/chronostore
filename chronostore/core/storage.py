import struct
import numpy as np
import mmap
import os
from typing import Dict, Any, Optional

from .schema import TableSchema

class Storage:
    def __init__(self, schema: TableSchema):
        """
        Storage handles packing and reading records using the provided TableSchema.
        """
        self.schema = schema
        self.record_format = schema.struct_format()
        self.record_size = schema.record_size()

    def pack_row(self, row: Dict[str, Any]) -> bytes:
        """
        Pack a row dict into binary format.
        """
        values = [row[col.name] for col in self.schema.columns]
        return struct.pack(self.record_format, *values)

    def read_file(self, path: str, start: Optional[int] = None, end: Optional[int] = None) -> Dict[str, np.ndarray]:
        """
        Read binary records from a file, return as dict of NumPy arrays.
        """
        if not os.path.exists(path):
            return {}

        with open(path, "rb") as f:
            mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
            records = np.frombuffer(mm, dtype=self.schema.dtype_def())
            sliced = records[start:end]
            return {name: sliced[name] for name, _ in self.schema.dtype_def()}