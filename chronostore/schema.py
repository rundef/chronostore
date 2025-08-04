import struct
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
        fmt_to_dtype = {
            "q": "int64",
            "Q": "uint64",
            "i": "int32",
            "I": "uint32",
            "h": "int16",
            "H": "uint16",
            "b": "int8",
            "B": "uint8",
            "f": "float32",
            "d": "float64",
        }

        return [(col.name, fmt_to_dtype[col.fmt]) for col in self.columns]

    @cached_property
    def record_size(self) -> int:
        return struct.calcsize(self.struct_format)
