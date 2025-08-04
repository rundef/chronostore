import numpy as np
from chronostore import TableSchema, ColumnSchema
from chronostore.backend import FlatFileBackend
from chronostore.backend.flatfile.storage import Storage

def test_pack_and_read(tmp_path):
    schema = TableSchema(columns=[
        ColumnSchema("timestamp", "q"),
        ColumnSchema("price", "d")
    ])
    backend = FlatFileBackend(schema, tmp_path)
    storage = Storage(schema)
    row = {"timestamp": 1234567890, "price": 100.5}
    packed = backend.pack_row(row)

    path = tmp_path / "data.bin"
    with open(path, "wb") as f:
        f.write(packed * 5)

    data = storage.read_file(str(path))
    assert len(data["timestamp"]) == 5
    assert np.allclose(data["price"], [100.5]*5)
