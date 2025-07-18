from typing import BinaryIO

class Writer:
    def __init__(self, file_path: str):
        """
        Append-only writer for binary data.
        """
        self.file: BinaryIO = open(file_path, "ab")

    def append(self, packed_bytes: bytes) -> None:
        """
        Write packed bytes to file.
        """
        self.file.write(packed_bytes)

    def flush(self) -> None:
        """
        Flush and close the file handle.
        """
        self.file.flush()
        self.file.close()