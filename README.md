# ⏱️ Chronostore

[![PyPI - Version](https://img.shields.io/pypi/v/chronostore)](https://pypi.org/project/chronostore/)
[![CI](https://github.com/rundef/chronostore/actions/workflows/ci.yml/badge.svg)](https://github.com/rundef/chronostore/actions/workflows/ci.yml)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/chronostore)](https://pypistats.org/packages/chronostore)

**Chronostore**: The simplest binary time series store. No DB. No Server. Just mmap. Built for append-only local data.

---

Chronostore saves **all columns in single daily binary files** using your schema, with memory-mapped reads for fast, zero-copy access via NumPy.

**Why?** Chronostore is perfect for local, append-only time series workloads where you want full control over schema and layout without any server or database dependency.

---

## 📦 Installation

```bash
pip install chronostore
```

## ⚙️ Features

- 📁 Single binary file per day (record-packed format)
- 🧠 Memory-mapped reads (no full file load)
- 📆 Date-partitioned folders
- ⚡ Vectorized filtering with NumPy
- ✅ User-defined schema for flexibility
- 🔒 No dependencies or servers — just your filesystem

---

## ⚠️ Limitations

- Not designed for concurrent writes
- No built-in indexing or compression
- Best suited for SSD/NVMe; HDD can be slow for large date ranges

---

## 📂 Data Layout

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

---

## 🧪 Example Usage

```python
from chronostore import TimeSeriesEngine, TableSchema, ColumnSchema

schema = TableSchema(columns=[
    ColumnSchema("timestamp", "q"),
    ColumnSchema("value", "d"),
])
engine = TimeSeriesEngine("./data_folder", schema)
engine.append("Sensor1", "2025-06-14", {"timestamp": 1234567890, "value": 42.0})
engine.flush()

# Read the last 5 rows from a day
recent = engine.read("Sensor1", "2025-06-14", start=-5)
print(recent)
```

---

## 📓 Explore in Notebooks:

Practical examples that mirror real workloads:

- [1M rows read/write →](notebooks/1_million_rows.ipynb)
- [IoT sensor stream →](notebooks/iot_sensor.ipynb)
- [Financial tick data storage →](notebooks/financial_tick_data.ipynb)
- [System logs →](notebooks/logs_events.ipynb)

---

## 🚀 Benchmarks

| Format      | Append (1M rows) | Read (1M rows) | Filter (> threshold) |
| ----------- | ---------------- | -------------- | -------------------- |
| CSV         | 5.1s             | 1.8s           | 1.7s                 |
| HDF5        | 2.3s             | 0.9s           | 0.9s                 |
| Parquet     | 2.8s             | 1.2s           | 1.1s                 |
| DuckDB      | 1.9s             | 0.6s           | 0.8s                 |
| Chronostore | **0.6s**         | **0.2s**       | **0.2s**             |

> Benchmarked on 1M rows of 3-column float64 data on a clean AWS m5d.large instance with SSD (Ubuntu, Python 3.11).

---

## 📈 Use Cases

- Time series storage for sensor or IoT data
- Event logs or telemetry storage
- Custom domain-specific timeseries archiving

---

## 🧠 Why Not Use a DB?

Chronostore is ideal when:

- You need max speed with minimal overhead
- You know your schema in advance
- You want total control over layout and access patterns
- You want to learn low-level I/O, memory mapping, and binary formats

| Feature         | Chronostore | CSV | Parquet | DuckDB  |
| --------------- | ----------- | --- | ------- | ------- |
| Server required | ❌           | ❌   | ❌       | ❌       |
| Schema enforced | ✅           | ❌   | ✅       | ✅       |
| Compression     | ❌           | ❌   | ✅       | ✅       |
| Append-only     | ✅           | ✅   | ✅       | ✅       |
| Memory mapped   | ✅           | ❌   | Partial | Partial |

---

## 📜 License

[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

