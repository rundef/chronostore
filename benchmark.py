import time
import os
import tempfile
import numpy as np
import pandas as pd
import gc

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from arcticdb import Arctic, QueryBuilder

from chronostore import TimeSeriesEngine, TableSchema, ColumnSchema
from chronostore.backend import FlatFileBackend, LmdbBackend

# Configuration
N_ROWS = 10_000_000

# Generate synthetic float64 + timestamp data
data = pd.DataFrame({
    "timestamp": np.arange(N_ROWS, dtype=np.int64),
    "value1": np.random.uniform(0, 100, N_ROWS),
    "value2": np.random.uniform(0, 100, N_ROWS),
    "value3": np.random.uniform(0, 100, N_ROWS),
})

def get_dir_size_mb(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total += os.path.getsize(fp)
    return total / (1024 * 1024)  # MB


def benchmark_csv(base_dir):
    os.makedirs(base_dir, exist_ok=True)
    path = os.path.join(base_dir, "data.csv")
    t0 = time.time()
    data.to_csv(path, index=False)
    print(f"CSV write: {time.time() - t0:.2f} seconds")

    t0 = time.time()
    df_read = pd.read_csv(path)
    print(f"CSV read all: {time.time() - t0:.2f} seconds")
    assert df_read.shape[0] == N_ROWS

    print(f"CSV disk usage: {get_dir_size_mb(base_dir):.2f} MB")



def benchmark_parquet(base_dir):
    os.makedirs(base_dir, exist_ok=True)
    path = os.path.join(base_dir, "data.parquet")
    t0 = time.time()
    table = pa.Table.from_pandas(data)
    pq.write_table(table, path, compression="snappy")
    print(f"Parquet write: {time.time() - t0:.2f} seconds")

    t0 = time.time()
    df_read = pq.read_table(path).to_pandas()
    print(f"Parquet read all: {time.time() - t0:.2f} seconds")
    assert df_read.shape[0] == N_ROWS

    t0 = time.time()
    pq.read_table(path, filters=[("value1", ">", 90.0)])
    print(f"Parquet filtered read: {time.time() - t0:.2f} seconds")

    print(f"Parquet disk usage: {get_dir_size_mb(base_dir):.2f} MB")

def benchmark_duckdb(base_dir):
    os.makedirs(base_dir, exist_ok=True)
    db_path = os.path.join(base_dir, "data.duckdb")
    con = duckdb.connect(db_path)
    con.register("data_df", data)

    t0 = time.time()
    con.execute("CREATE TABLE data AS SELECT * FROM data_df")
    print(f"DuckDB write: {time.time() - t0:.2f} seconds")

    t0 = time.time()
    df_read = con.execute("SELECT * FROM data").df()
    print(f"DuckDB read all: {time.time() - t0:.2f} seconds")
    assert df_read.shape[0] == N_ROWS

    t0 = time.time()
    df_filtered = con.execute("SELECT * FROM data WHERE value1 > 90.0").df()
    print(f"DuckDB filtered read: {time.time() - t0:.2f} seconds")

    con.close()

    print(f"DuckDB disk usage: {get_dir_size_mb(base_dir):.2f} MB")


def benchmark_arcticdb(base_dir):
    os.makedirs(base_dir, exist_ok=True)

    # Setup ArcticDB in local mode (LMDB)
    ac = Arctic(f"lmdb://{base_dir}")
    lib_name = "benchmark_lib"

    ac.create_library(lib_name)
    lib = ac[lib_name]

    t0 = time.time()
    lib.write("Sensor", data)
    write_time = time.time() - t0

    del lib
    del ac
    gc.collect()

    ac = Arctic(f"lmdb://{base_dir}")
    lib = ac[lib_name]

    t0 = time.time()
    df_read = lib.read("Sensor").data
    read_time = time.time() - t0
    assert df_read.shape[0] == N_ROWS

    # Filtered read
    t0 = time.time()
    q = QueryBuilder()
    q = q[q["value1"] > 90.0]
    result = lib.read("Sensor", query_builder=q).data
    filter_time = time.time() - t0

    disk_usage = get_dir_size_mb(base_dir)

    print(f"ArcticDB write: {write_time:.2f} seconds")
    print(f"ArcticDB read all: {read_time:.2f} seconds")
    print(f"ArcticDB filtered read: {filter_time:.2f} seconds")
    print(f"ArcticDB disk usage: {disk_usage:.2f} MB")

def benchmark_chronostore(base_dir):
    os.makedirs(base_dir, exist_ok=True)
    schema = TableSchema(columns=[
        ColumnSchema("timestamp", "q"),
        ColumnSchema("value1", "d"),
        ColumnSchema("value2", "d"),
        ColumnSchema("value3", "d"),
    ])
    #backend = LmdbBackend(schema, base_dir)
    backend = FlatFileBackend(schema, base_dir)
    engine = TimeSeriesEngine(backend=backend)

    # Write
    to_write = {}
    rows_per_day = N_ROWS // 31
    for day in range(1, 31 + 1):
        day_str = f"2025-07-{day:02d}"
        start = (day - 1) * rows_per_day
        end = start + rows_per_day if day < 31 else N_ROWS
        day_df = data.iloc[start:end]
        to_write[day_str] = day_df

    t0 = time.time()
    for day_str, day_df in to_write.items():
        engine.append("Sensor", day_str, day_df)#.to_dict(orient='records'))
    engine.flush()
    print(f"Chronostore write: {time.time() - t0:.2f} seconds")

    # Read all
    t0 = time.time()
    df = engine.read_dataframe("Sensor", ["2025-07-01", "2025-07-31"])
    print(f"Chronostore read all: {time.time() - t0:.2f} seconds")
    assert df.shape[0] == N_ROWS, f"Hello {df.shape[0]}"

    # Filter
    t0 = time.time()
    result = engine.read("Sensor", ["2025-07-01", "2025-07-31"], where=lambda d: d["value1"] > 90.0)
    print(f"Chronostore filtered read: {time.time() - t0:.2f} seconds")

    print(f"Chronostore disk usage: {get_dir_size_mb(base_dir):.2f} MB")

if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        print(f"\nRunning benchmarks in {tmp}")

        #benchmark_csv(os.path.join(tmp, "csv"))
        #print("----------")
        benchmark_parquet(os.path.join(tmp, "parquet"))
        print("----------")
        benchmark_duckdb(os.path.join(tmp, "duckdb"))
        print("----------")
        benchmark_arcticdb(os.path.join(tmp, "arcticdb"))
        print("----------")
        benchmark_chronostore(os.path.join(tmp, "chronostore"))
