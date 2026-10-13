"""Integration tests: curated Parquet file + config, contract, validate and train modules."""
import copy

import pandas as pd
import pytest

from src.config import load_settings, resolve
from src.train import ModelError, build_model_input
from src.validate import run_predictive_integrity_checks, run_quality_checks

CURATED_FILE = "curated.parquet"


@pytest.fixture()
def settings():
    loaded = load_settings()
    curated = resolve(loaded["paths"]["curated_dir"]) / CURATED_FILE
    source = resolve(loaded["paths"]["source_file"])
    if not curated.exists() or not source.exists():
        pytest.skip("run the pipeline first: curated layer or source file not found")
    return loaded


def _settings_with_curated_copy(settings, tmp_path, frame):
    changed = copy.deepcopy(settings)
    changed["paths"]["curated_dir"] = str(tmp_path)
    frame.to_parquet(tmp_path / CURATED_FILE, index=False)
    return changed


def test_real_curated_layer_passes_all_quality_and_integrity_checks(settings):
    results = run_quality_checks(settings) + run_predictive_integrity_checks(settings)
    failed = [r for r in results if not r["passed"]]
    assert len(results) == 9
    assert failed == []


def test_corrupted_curated_copy_is_detected_and_blocked_from_model_input(settings, tmp_path):
    real = pd.read_parquet(resolve(settings["paths"]["curated_dir"]) / CURATED_FILE)

    duplicated = real.copy()
    duplicated.loc[0, "track_id"] = duplicated.loc[1, "track_id"]
    bad = _settings_with_curated_copy(settings, tmp_path, duplicated)
    uniqueness = [r for r in run_quality_checks(bad) if r["category"] == "uniqueness"][0]
    assert uniqueness["passed"] is False

    null_target = real.copy()
    null_target[settings["target"]] = null_target[settings["target"]].astype("float64")
    null_target.loc[0, settings["target"]] = None
    bad = _settings_with_curated_copy(settings, tmp_path, null_target)
    completeness = [r for r in run_quality_checks(bad) if r["category"] == "completeness"][0]
    assert completeness["passed"] is False
    with pytest.raises(ModelError):
        build_model_input(bad)

    dropped = real.iloc[1:].copy()
    bad = _settings_with_curated_copy(settings, tmp_path, dropped)
    business = [r for r in run_quality_checks(bad) if r["category"] == "business_rule"][0]
    assert business["passed"] is False
