from .storage import Storage
from typing import Dict, Any, Optional

class Reader:
    def __init__(self, schema):
        """
        Reader uses Storage with a given schema to read data files.
        """
        self.storage = Storage(schema)

    def read_day(self, file_path: str, start: Optional[int] = None, end: Optional[int] = None) -> Dict[str, Any]:
        """
        Read a day's binary file and return column data.
        """
        return self.storage.read_file(file_path, start, end)