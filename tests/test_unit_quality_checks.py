"""Unit tests for the reusable data-quality check functions in src/validate.py."""
import pandas as pd

from src.validate import check_completeness, check_uniqueness, check_validity

SETTINGS = {"target": "popularity", "features": ["danceability", "tempo"]}


def _frame(**overrides):
    data = {
        "source_row_key": [0, 1, 2],
        "track_id": ["a", "b", "c"],
        "popularity": [10, 50, 80],
        "danceability": [0.2, 0.5, 0.9],
        "tempo": [100.0, 120.0, 140.0],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def test_uniqueness_detects_duplicate_track_id():
    assert check_uniqueness(_frame())["passed"] is True
    result = check_uniqueness(_frame(track_id=["a", "a", "c"]))
    assert result["passed"] is False
    assert result["category"] == "uniqueness"


def test_completeness_detects_null_target():
    assert check_completeness(_frame(), SETTINGS)["passed"] is True
    result = check_completeness(_frame(popularity=[10, None, 80]), SETTINGS)
    assert result["passed"] is False
    assert result["category"] == "completeness"


def test_validity_fails_on_range_violation_but_not_on_warn_only():
    contract = {"columns": [
        {"name": "danceability", "type": "float", "min": 0.0, "max": 1.0,
         "on_violation": "fail"},
        {"name": "tempo", "type": "float", "min": 0.0, "max": 300.0,
         "on_violation": "warn"},
    ]}
    assert check_validity(_frame(), contract)["passed"] is True
    assert check_validity(_frame(danceability=[0.2, 1.5, 0.9]), contract)["passed"] is False
    assert check_validity(_frame(tempo=[100.0, 400.0, 140.0]), contract)["passed"] is True
