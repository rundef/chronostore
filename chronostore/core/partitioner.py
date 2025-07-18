from pathlib import Path

class Partitioner:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def get_partition_path(self, table_name: str, date_str: str) -> Path:
        """
        Return the directory path for a given table_name and date.
        """
        return self.base_dir / table_name / date_str