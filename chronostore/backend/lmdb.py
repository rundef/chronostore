import lmdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Union, Dict, List, Any, Optional, Callable

from .base import Backend
from ..schema import TableSchema


class LmdbBackend(Backend):
    def __init__(self, schema: TableSchema, base_dir: str, **kwargs):
        """
        Initialize the backend with a data directory and user-defined schema.
        """
        super().__init__(schema)

        kwargs.setdefault('max_dbs', 1)
        kwargs.setdefault('map_size', 64 * 1024 ** 3) # Default to 64 GiB

        self.env = lmdb.open(base_dir, **kwargs)
        self._counters: Dict[tuple[str, str], int] = defaultdict(int)

        self._buffers: Dict[tuple[str, str], list] = defaultdict(list)

    def _counter_key(self, table: str, date_str: str) -> bytes:
        return f"__meta__:{table}:{date_str}".encode()

    def _row_key(self, table: str, date_str: str, counter: int) -> bytes:
        return f"{table}:{date_str}:{counter:06d}".encode()

    def read_partition(self, table_name, date_str, start=None, end=None):
        key = f"{table_name}:{date_str}".encode()
        with self.env.begin() as txn:
           raw = txn.get(key)
           if raw is None:
               return {}

        arr = np.frombuffer(raw, dtype=self.schema.numpy_dtype)
        sliced = arr[start:end]
        return {name: sliced[name] for name, _ in self.schema.numpy_dtype}

    def append(
        self,
        table_name: str,
        date_str: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        key = (table_name, date_str)
        self._buffers[key].append(data)

    def read(
        self,
        table_name: str,
        date: Union[str, tuple[str, str]],
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
        where: Optional[Callable[[Dict[str, np.ndarray]], np.ndarray]] = None,
    ) -> Dict[str, np.ndarray]:

        if isinstance(date, str):
            # Read a single day
            data = self.read_partition(table_name, date, start, end)
        else:
            # Read a date range
            start_dt = datetime.strptime(date[0], "%Y-%m-%d")
            end_dt = datetime.strptime(date[1], "%Y-%m-%d")
            date_list = [
                (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
                for i in range((end_dt - start_dt).days + 1)
            ]

            all_data: Dict[str, list] = {name: [] for name, _ in self.schema.numpy_dtype}
            for date_str in date_list:
                day_data = self.read_partition(table_name, date_str)
                for name, values in day_data.items():
                    all_data[name].append(values)

            # Concatenate arrays
            data = {name: np.concatenate(values) for name, values in all_data.items() if values}

            # Apply slicing at the range level (not per-partition)
            if start is not None or end is not None:
                data = {name: arr[start:end] for name, arr in data.items()}

            # Apply filtering
        if where and data:
            mask = where(data)
            data = {name: arr[mask] for name, arr in data.items()}

        return data

    def flush(self) -> None:
        with self.env.begin(write=True) as txn:
            for (table_name, date_str), rows in self._buffers.items():
                if not rows:
                    continue

                key = f"{table_name}:{date_str}".encode()
                new_data = b"".join(self.pack(row) for row in rows)

                existing = txn.get(key)
                if existing:
                    combined = existing + new_data
                else:
                    combined = new_data

                txn.put(key, combined)

        self._buffers.clear()