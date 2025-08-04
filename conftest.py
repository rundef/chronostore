import pytest
from chronostore import TimeSeriesEngine, TableSchema, ColumnSchema
from chronostore.backend import FlatFileBackend, LmdbBackend


@pytest.fixture
def default_schema():
    return TableSchema(columns=[
        ColumnSchema("timestamp", "q"),
        ColumnSchema("open", "d"),
        ColumnSchema("high", "d"),
        ColumnSchema("low", "d"),
        ColumnSchema("close", "d"),
        ColumnSchema("volume", "q"),
        ColumnSchema("delta", "q"),
    ])

@pytest.fixture(params=["flatfile", "lmdb"])
def engine(request, tmp_path, default_schema):
    if request.param == "flatfile":
        backend = FlatFileBackend(default_schema, str(tmp_path))
    elif request.param == "lmdb":
        backend = LmdbBackend(default_schema, str(tmp_path))
    else:
        raise ValueError(f"Unknown backend: {request.param}")
    return TimeSeriesEngine(backend=backend)