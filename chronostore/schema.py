import struct
import numpy as np
from dataclasses import dataclass
from typing import List, Tuple
from functools import cached_property

@dataclass
class ColumnSchema:
    name: str
    fmt: str  # struct format, e.g., 'q' or 'd'

@dataclass
class TableSchema:
    columns: List[ColumnSchema]

    @cached_property
    def struct_format(self) -> str:
        return "".join(col.fmt for col in self.columns)

    @cached_property
    def numpy_dtype(self) -> List[Tuple[str, str]]:
        return [(col.name, np.dtype(col.fmt)) for col in self.columns]

    @cached_property
    def record_size(self) -> int:
        return struct.calcsize(self.struct_format)
