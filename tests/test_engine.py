import pytest
import pandas as pd
import numpy as np
from datetime import datetime


def test_append_and_read(engine):
    date_str = "2025-06-14"
    now = int(datetime(2025, 6, 14, 9, 30).timestamp() * 1e9)

    for i in range(5):
        engine.append("ES", date_str, {
            "timestamp": now + i * 60_000_000_000,
            "open": 5400.0 + i,
            "high": 5401.0 + i,
            "low": 5399.0 + i,
            "close": 5400.5 + i,
            "volume": 100 + i,
            "delta": -50 + i
        })
    engine.flush()

    data = engine.read("ES", date_str)
    assert len(data["timestamp"]) == 5
    assert np.allclose(data["open"], [5400.0 + i for i in range(5)])

def test_read_range(engine):
    now = int(datetime(2025, 6, 13, 9, 30).timestamp() * 1e9)
    for day in ["2025-06-13", "2025-06-14"]:
        for i in range(3):
            engine.append("ES", day, {
                "timestamp": now + i * 60_000_000_000,
                "open": 5400.0 + i,
                "high": 5401.0 + i,
                "low": 5399.0 + i,
                "close": 5400.5 + i,
                "volume": 100 + i,
                "delta": -50 + i
            })
    engine.flush()

    result = engine.read("ES", ["2025-06-13", "2025-06-14"])
    assert len(result["timestamp"]) == 6
    assert result["open"].shape == (6,)

@pytest.mark.parametrize("deltas,expected", [
    ([10, -5, 20], 2),
    ([-10, -5, -2], 0)
])
def test_filter(engine, deltas, expected):
    date_str = "2025-06-14"
    now = int(datetime(2025, 6, 14, 9, 30).timestamp() * 1e9)

    for i, delta in enumerate(deltas):
        engine.append("ES", date_str, {
            "timestamp": now + i * 60_000_000_000,
            "open": 5430.0 + i,
            "high": 5431.0 + i,
            "low": 5429.0 + i,
            "close": 5430.5 + i,
            "volume": 100 + i,
            "delta": delta
        })
    engine.flush()

    result = engine.read("ES", date_str, where=lambda d: d["delta"] > 0)
    assert len(result["delta"]) == expected


def test_append_and_read(engine):
    date_str = "2025-06-14"
    now = int(datetime(2025, 6, 14, 9, 30).timestamp() * 1e9)

    for i in range(5):
        engine.append("ES", date_str, {
            "timestamp": now + i * 60_000_000_000,
            "open": 5400.0 + i,
            "high": 5401.0 + i,
            "low": 5399.0 + i,
            "close": 5400.5 + i,
            "volume": 100 + i,
            "delta": -50 + i
        })
    engine.flush()

    data = engine.read("ES", date_str)
    assert len(data["timestamp"]) == 5
    assert np.allclose(data["open"], [5400.0 + i for i in range(5)])

def test_append_and_read_dataframe(engine):
    day = "2025-06-13"
    now = int(datetime(2025, 6, 13, 9, 30).timestamp() * 1e9)

    df = pd.DataFrame({
        "timestamp": [now + i * 60_000_000_000 for i in range(3)],
        "open": [5400.0 + i for i in range(3)],
        "high": [5401.0 + i for i in range(3)],
        "low": [5399.0 + i for i in range(3)],
        "close": [5400.5 + i for i in range(3)],
        "volume": [100 + i for i in range(3)],
        "delta": [-50 + i for i in range(3)],
    })

    engine.append("ES", day, df)
    engine.flush()

    result_df = engine.read_dataframe("ES", day)

    pd.testing.assert_frame_equal(result_df, df)
