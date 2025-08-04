import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Union, Any, Optional, Callable
import numpy as np
import pandas as pd

from ...schema import TableSchema
from ..base import Backend
from .storage import Storage
from .writer import Writer
from .partitioner import Partitioner

class FlatFileBackend(Backend):
    def __init__(self, schema: TableSchema, base_dir: str, **kwargs):
        """
        Initialize the backend with a data directory and user-defined schema.
        """
        super().__init__(schema)
        self.storage = Storage(schema)
        self.partitioner = Partitioner(base_dir)
        self.base_dir = base_dir
        self.open_writers: Dict[str, Writer] = {}

    def append(
        self,
        table_name: str,
        date_str: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Append to the day's binary file
        - a single row (dict)
        - multiple rows (list of dicts)
        - dataframe
        """
        partition_path = self.partitioner.get_partition_path(table_name, date_str)
        os.makedirs(partition_path, exist_ok=True)
        file_path = os.path.join(partition_path, "data.bin")

        if file_path not in self.open_writers:
            self.open_writers[file_path] = Writer(file_path)

        packed = self.pack(data)
        self.open_writers[file_path].append(packed)

    def flush(self) -> None:
        """
        Flush and close all open writers.
        """
        for writer in self.open_writers.values():
            writer.flush()
        self.open_writers.clear()

    def read(
        self,
        table_name: str,
        date: Union[str, tuple[str, str]],
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
        where: Optional[Callable[[Dict[str, np.ndarray]], np.ndarray]] = None,
    ) -> Dict[str, np.ndarray]:
        """
        Read data for a day or date range.

        - `date`: a single date ("YYYY-MM-DD") or a tuple (start_date, end_date).
        - `start` and `end`: optional slice indices (only applies to single day read).
        - `where`: optional filter function that takes a dict of columns and returns a boolean mask.
        """
        if isinstance(date, str):
            # Single day read
            partition_path = self.partitioner.get_partition_path(table_name, date)
            file_path = os.path.join(partition_path, "data.bin")
            data = self.storage.read_file(file_path, start, end)
        else:
            # Date range read
            start_dt = datetime.strptime(date[0], "%Y-%m-%d")
            end_dt = datetime.strptime(date[1], "%Y-%m-%d")
            date_list = [
                (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
                for i in range((end_dt - start_dt).days + 1)
            ]
            all_data: Dict[str, list] = {col.name: [] for col in self.schema.columns}

            def load_day(date_str):
                return self.read(table_name, date_str)

            with ThreadPoolExecutor() as executor:
                results = executor.map(load_day, date_list)
                for day_data in results:
                    for col in all_data:
                        if col in day_data:
                            all_data[col].append(day_data[col])

            data = {k: np.concatenate(v) for k, v in all_data.items() if v}

        # Apply optional filtering
        if where and data:
            mask = where(data)
            data = {k: v[mask] for k, v in data.items()}

        return data
