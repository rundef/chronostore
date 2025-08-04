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

    #@profile
    def read_partition(self, table_name, date_str, start=None, end=None):
        key = f"{table_name}:{date_str}".encode()
        with self.env.begin() as txn:
           raw = txn.get(key)
           if raw is None:
               return {}

        arr = np.frombuffer(raw, dtype=self.schema.numpy_dtype)
        sliced = arr[start:end]
        return {name: sliced[name] for name, _ in self.schema.numpy_dtype}

        # prefix = f"{table_name}:{date_str}:".encode()
        # rows = []
        #
        # with self.env.begin() as txn:
        #     cursor = txn.cursor()
        #     if cursor.set_range(prefix):
        #         for key, value in cursor:
        #             if not key.startswith(prefix):
        #                 break
        #             arr = np.frombuffer(value, dtype=self.schema.numpy_dtype)
        #             rows.append(arr)
        #
        # if not rows:
        #     return {}
        #
        # all_data = np.concatenate(rows)
        # sliced = all_data[start:end]
        # return {name: sliced[name] for name, _ in self.schema.numpy_dtype}

    #@profile
    def append(
        self,
        table_name: str,
        date_str: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        key = f"{table_name}:{date_str}".encode()
        packed = self.pack(data)

        with self.env.begin(write=True) as txn:
            existing = txn.get(key)
            txn.put(key, existing + packed if existing else packed)
        """

        """
        with self.env.begin(write=True) as txn:
            counter_key = self._counter_key(table_name, date_str)

            # Lazy-load counter from LMDB if needed
            if (table_name, date_str) not in self._counters:
                raw = txn.get(counter_key)
                self._counters[(table_name, date_str)] = int.from_bytes(raw, "big") if raw else 0

            counter = self._counters[(table_name, date_str)]

            # Batch-pack the entire input
            packed = self.pack(data)
            num_rows = len(packed) // self.schema.record_size

            # Slice and write each row
            for i in range(num_rows):
                row_bytes = packed[i * self.schema.record_size : (i + 1) * self.schema.record_size]
                key = self._row_key(table_name, date_str, counter + i)
                txn.put(key, row_bytes)

            # Update and persist the counter
            new_counter = counter + num_rows
            self._counters[(table_name, date_str)] = new_counter
            txn.put(counter_key, new_counter.to_bytes(8, "big"))
        """

        key = (table_name, date_str)
        self._buffers[key].append(data)

    #@profile
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