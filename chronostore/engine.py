import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional
import numpy as np

from .core import Partitioner, Reader, TableSchema, Storage, Writer

class TimeSeriesEngine:
    def __init__(self, data_dir: str, schema: TableSchema):
        """
        Initialize the engine with a data directory and user-defined schema.
        """
        self.data_dir = data_dir
        self.schema = schema
        self.partitioner = Partitioner(data_dir)
        self.storage = Storage(schema)
        self.reader = Reader(schema)
        self.open_writers: Dict[str, Writer] = {}

    def append(self, table_name: str, date_str: str, row: Dict[str, Any]) -> None:
        """
        Append a single row (dict) to the day's binary file.
        """
        partition_path = self.partitioner.get_partition_path(table_name, date_str)
        os.makedirs(partition_path, exist_ok=True)
        file_path = os.path.join(partition_path, "data.bin")

        if file_path not in self.open_writers:
            self.open_writers[file_path] = Writer(file_path)

        packed = self.storage.pack_row(row)
        self.open_writers[file_path].append(packed)

    def flush(self) -> None:
        """
        Flush and close all open writers.
        """
        for writer in self.open_writers.values():
            writer.flush()
        self.open_writers.clear()

    def read(self, table_name: str, date_str: str, start: Optional[int] = None, end: Optional[int] = None) -> Dict[str, np.ndarray]:
        """
        Read rows for a single day, optionally slicing from start to end index.
        """
        partition_path = self.partitioner.get_partition_path(table_name, date_str)
        file_path = os.path.join(partition_path, "data.bin")
        return self.reader.read_day(file_path, start, end)

    def read_range(self, table_name: str, start_date: str, end_date: str) -> Dict[str, np.ndarray]:
        """
        Read and concatenate data across a date range [start_date, end_date] inclusive.
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        date_list = []
        current = start_dt
        while current <= end_dt:
            date_list.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        all_data: Dict[str, list] = {col.name: [] for col in self.schema.columns}

        def load_day(date_str):
            return self.read(table_name, date_str)

        with ThreadPoolExecutor() as executor:
            results = executor.map(load_day, date_list)
            for day_data in results:
                for col in all_data:
                    if col in day_data:
                        all_data[col].append(day_data[col])

        return {k: np.concatenate(v) for k, v in all_data.items() if v}

    def filter(self, table_name: str, date_str: str, where) -> Dict[str, np.ndarray]:
        """
        Apply a NumPy-compatible filter function to a day's data.
        """
        data = self.read(table_name, date_str)
        if not data:
            return {}
        mask = where(data)
        return {k: v[mask] for k, v in data.items()}
