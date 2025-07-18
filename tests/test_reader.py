import numpy as np
from chronostore.core import Reader
from chronostore import TableSchema, ColumnSchema

def test_reader_reads_structured(tmp_path):
    schema = TableSchema(columns=[
        ColumnSchema("timestamp", "q"),
        ColumnSchema("value", "d")
    ])
    path = tmp_path / "data.bin"
    arr = np.array([(1, 2.5), (2, 3.5)], dtype=[('timestamp', 'int64'), ('value', 'float64')])
    with open(path, "wb") as f:
        f.write(arr.tobytes())

    reader = Reader(schema)
    data = reader.read_day(str(path))
    assert list(data["timestamp"]) == [1, 2]
    assert np.allclose(data["value"], [2.5, 3.5])