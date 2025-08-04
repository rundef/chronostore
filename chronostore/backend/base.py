from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any, Union, Callable
import struct
import numpy as np
import pandas as pd

from ..schema import TableSchema

class Backend(ABC):
    def __init__(self, schema: TableSchema):
        self.schema = schema

    @abstractmethod
    def append(
        self,
        table_name: str,
        date_str: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        pass

    @abstractmethod
    def read(
        self,
        table_name: str,
        date: Union[str, tuple[str, str]],
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
        where: Optional[Callable[[Dict[str, np.ndarray]], np.ndarray]] = None,
    ) -> Dict[str, np.ndarray]:
        pass
    
    def read_dataframe(self, *args, **kwargs) -> pd.DataFrame:
        data = self.read(*args, **kwargs)
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)

    @abstractmethod
    def flush(self) -> None:
        pass

    def pack_row(self, row: Dict[str, Any]) -> bytes:
        """
        Pack a row dict into binary format.
        """
        values = [row[col.name] for col in self.schema.columns]
        return struct.pack(self.schema.struct_format, *values)

    def pack_rows(self, rows: List[Dict[str, Any]]) -> bytes:
        """
        Pack a list of rows into binary format.
        """
        dtype = self.schema.numpy_dtype

        arr = np.array(
            [tuple(row[col.name] for col in self.schema.columns) for row in rows],
            dtype=dtype,
        )
        return arr.tobytes()
    
    def pack_dataframe(self, df: pd.DataFrame) -> bytes:
        """
        Pack a pandas dataframe into binary format.
        """
        return df.to_records(index=False).tobytes()
    
    def pack_series(self, series: pd.Series) -> bytes:
        """
        Pack a pandas series into binary format.
        """
        return series.to_numpy().tobytes()

    def pack(self, data) -> bytes:
        if isinstance(data, pd.DataFrame):
            return self.pack_dataframe(data)

        elif isinstance(data, pd.Series):
            return self.pack_series(data)

        elif isinstance(data, list):
            return self.pack_rows(data)

        return self.pack_row(data)