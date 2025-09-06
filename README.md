# ⏱️ Chronostore

[![PyPI - Version](https://img.shields.io/pypi/v/chronostore)](https://pypi.org/project/chronostore/)
[![CI](https://github.com/rundef/chronostore/actions/workflows/ci.yml/badge.svg)](https://github.com/rundef/chronostore/actions/workflows/ci.yml)

**Chronostore** is a fast, binary time series storage engine for local workloads.
No server. No database. Just append-only daily files backed by memory-mapping or LMDB, with zero-copy NumPy reads and schema control.

## 📦 Installation

```bash
pip install chronostore
```

## ⚙️ Features

- 🔌 **Pandas-compatible**: Read and write directly from DataFrames or lists of dicts
- ⚡ **Fast reads**: Zero-copy access via NumPy with optional memory-mapping or LMDB backend
- 🧠 **Schema-defined layout**: Define your own typed schema for precise control over storage format
- 📅 **Daily partitioning**: Each day's data is saved to a single compact binary file for fast lookups
- 🔄 **Append-only design**: Ideal for logs, metrics, sensor data, or financial data
- 🧱 **Pluggable backends**: Choose between FlatFile (mmap) and LMDB
- 🚫 **No server or database required**: Pure Python. Runs anywhere (no setup, no infra)

## ⚠️ Limitations

- Not designed for concurrent writes
- No built-in indexing or compression
- Best suited for SSD/NVMe; HDD can be slow for large date ranges

## 📂 Data Layout (flatfile backend)

```
data/
└── TableName/
    ├── 2025-06-13/
    │   └── data.bin
    └── 2025-06-14/
        └── data.bin
```

Each `data.bin` is an append-only binary file containing rows packed according to the user schema (e.g., `int64`, `float64`, etc).

[The list of format characters is available here.](https://docs.python.org/3/library/struct.html#format-characters)

## 🧪 Example Usage

```python
from chronostore import TimeSeriesEngine, TableSchema, ColumnSchema
from chronostore.backend import FlatFileBackend, LmdbBackend

schema = TableSchema(columns=[
    ColumnSchema("timestamp", "q"), # int64
    ColumnSchema("value", "d"),     # float64
])

# Choose your backend
backend = FlatFileBackend(schema, "./data_folder")  # Memory-mapped files
# backend = LmdbBackend(schema, "./data_folder")    # Alternatively: LMDB-backed

# Create engine
engine = TimeSeriesEngine(backend=backend)

# Append data
engine.append("Sensor1", "2025-06-14", {"timestamp": 1234567890, "value": 42.0})
engine.append("Sensor1", "2025-06-14", {"timestamp": 1234567891, "value": 43.0})
engine.flush()

# Read the last 5 rows from that day
recent = engine.read("Sensor1", "2025-06-14", start=-5)
print(recent)
```

## 📓 Explore in Notebooks:

Practical examples that mirror real workloads:

- [1M rows read/write →](notebooks/1_million_rows.ipynb)
- [IoT sensor stream →](notebooks/iot_sensor.ipynb)
- [Financial tick data storage →](notebooks/financial_tick_data.ipynb)

## 🚀 Benchmarks

> Benchmarked on 10M rows of 4-column float64 data

| Format                         | Append all | Read all | Filter (> threshold) | Disk usage  |
|--------------------------------|------------|----------|----------------------| ----------- |
| CSV                            | 58.6s      | 7.84s    | ❌                    | 595MB       |
| Parquet                        | 2.03s      | 0.44s    | 0.30s                | 277MB       |
| DuckDB                         | 3.33s      | 0.81s    | 0.42s                | 203MB       |
| Chronostore (flatfile backend) | 0.43s      | 0.24s    | 0.40s                | 305MB       |
| Chronostore (lmdb backend)     | 0.58s      | 0.52s    | 0.57s                | 305MB       |

## 📈 Use Cases

- Time series storage for sensor or IoT data
- Event logs or telemetry storage
- Custom domain-specific timeseries archiving

## 🧠 Why Not Use a DB?

Chronostore is ideal when:

- You need max speed with minimal overhead
- You know your schema in advance
- You want total control over layout and access patterns
- You want to learn low-level I/O, memory mapping, and binary formats

| Feature         | Chronostore | CSV | Parquet    | DuckDB     |
| --------------- | ----------- | --- | ---------- | ---------- |
| Server required | ❌          | ❌  | ❌         | ❌         |
| Schema enforced | ✅          | ❌  | ✅         | ✅         |
| Compression     | ❌          | ❌  | ✅         | ✅         |
| Append-only     | ✅          | ✅  | ❌         | ❌         |
| Memory mapped   | ✅          | ❌  | ❌         | ⚠️ internal only |

## 📜 License

[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

