import struct
from dataclasses import dataclass
from typing import List, Tuple

@dataclass
class ColumnSchema:
    name: str
    fmt: str  # struct format, e.g., 'q' or 'd'

@dataclass
class TableSchema:
    columns: List[ColumnSchema]

    def struct_format(self) -> str:
        return "".join(col.fmt for col in self.columns)

    def dtype_def(self) -> List[Tuple[str, str]]:
        return [(col.name, col.fmt.replace('q', 'int64').replace('d', 'float64')) for col in self.columns]

    def record_size(self) -> int:
        return struct.calcsize(self.struct_format())
