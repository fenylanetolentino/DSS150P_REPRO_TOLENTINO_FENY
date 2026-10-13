"""Evaluation and the automatic baseline comparison (DSS150P Sections 8 and 10.2)."""
import json
import platform
import time

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.metrics import mean_absolute_error, r2_score

from src.config import log_event, resolve, utc_now_iso
from src.train import (MODEL_FILE, SCALER_FILE, ModelError, _write_json,
                       build_model_input, ids_checksum)

METRIC_DEFINITIONS = {
    "rmse": "square root of the mean of squared errors on the test rows (primary metric)",
    "mae": "mean of absolute errors on the test rows",
    "r2": "coefficient of determination: 1 - residual sum of squares / total sum of squares, "
          "on the test rows",
}
LOWER_IS_BETTER = {"rmse": True, "mae": True, "r2": False}


def evaluate_model(settings, run_id):
    """Predict the test rows with the saved model and write metrics.json and predictions.csv."""
    started = time.time()
    model_dir = resolve(settings["paths"]["model_dir"])
    for name in (MODEL_FILE, SCALER_FILE):
        if not (model_dir / name).exists():
            raise ModelError(f"{name} not found in {model_dir}; run train-model first")
    manifest_path = resolve(settings["paths"]["split_manifest_file"])
    if not manifest_path.exists():
        raise ModelError(f"{manifest_path} not found; run train-model first")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    log_event(run_id, "evaluate", "evaluation started")

    model_input = build_model_input(settings)
    keys = model_input["frame"]["source_row_key"].to_numpy()
    if (ids_checksum(keys[model_input["train_pos"]]) != manifest["train_ids_sha256_in_order"]
            or ids_checksum(keys[model_input["test_pos"]]) != manifest["test_ids_sha256_in_order"]):
        raise ModelError("the rebuilt split differs from split_manifest.json; "
                         "the curated data or split settings changed since training")

    model = joblib.load(model_dir / MODEL_FILE)
    scaler = joblib.load(model_dir / SCALER_FILE)
    x_test = scaler.transform(model_input["x_test_raw"])      # the saved scaler, not a new one
    y_test = model_input["y_test"]
    y_pred = model.predict(x_test)

    metrics = {
        "rmse": float(np.sqrt(np.mean((y_test - y_pred) ** 2))),
        "mae": float(mean_absolute_error(y_test, y_pred)),
        "r2": float(r2_score(y_test, y_pred)),
    }

    frame = model_input["frame"]
    test_rows = frame.iloc[model_input["test_pos"]]
    tier_name = [c for c in frame.columns if c == "popularity_tier"][0]
    predictions = pd.DataFrame({
        "source_row_key": test_rows["source_row_key"].to_numpy(),
        "track_id": test_rows["track_id"].to_numpy(),
        tier_name: test_rows[tier_name].to_numpy(),
        "actual_popularity": test_rows[settings["target"]].to_numpy(),
        "predicted_popularity": y_pred,
        "pipeline_run_id": run_id,
    }).sort_values("source_row_key", kind="mergesort")
    predictions_path = resolve(settings["paths"]["predictions_file"])
    predictions_path.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(predictions_path, index=False)

    output = {
        "pipeline_run_id": run_id,
        "generated_at_utc": utc_now_iso(),
        "primary_metric": "rmse",
        "metrics": metrics,
        "metric_definitions": METRIC_DEFINITIONS,
        "counts": {"train_rows": int(len(model_input["train_pos"])),
                   "test_rows": int(len(model_input["test_pos"])),
                   "predictions": int(len(predictions))},
        "model": {"family": settings["model"]["family"],
                  "hyperparameters": {**settings["model"]["params"],
                                      "random_state": settings["random_seed"]}},
        "features": settings["features"],
        "target": settings["target"],
        "random_seed": settings["random_seed"],
        "split_test_ids_sha256_in_order": manifest["test_ids_sha256_in_order"],
        "curated_data_checksum": manifest.get("curated_data_checksum"),
        "library_versions": {"python": platform.python_version(),
                             "scikit-learn": sklearn.__version__,
                             "numpy": np.__version__, "pandas": pd.__version__},
    }
    _write_json(resolve(settings["paths"]["metrics_file"]), output)
    log_event(run_id, "evaluate", "evaluation finished", rmse=round(metrics["rmse"], 6),
              mae=round(metrics["mae"], 6), r2=round(metrics["r2"], 6),
              predictions=len(predictions), duration_seconds=round(time.time() - started, 2))
    return output


def reproducibility_check(settings, run_id):
    """Compare outputs/metrics.json with metadata/expected_metrics.json automatically."""
    expected_path = resolve(settings["paths"]["expected_metrics_file"])
    actual_path = resolve(settings["paths"]["metrics_file"])
    for path in (expected_path, actual_path):
        if not path.exists():
            raise ModelError(f"{path} not found; run evaluate-model first")
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    actual = json.loads(actual_path.read_text(encoding="utf-8"))
    tolerance = float(expected["tolerance"]["absolute"])

    rows, improved, worse = {}, [], []
    for name, baseline in expected["metrics"].items():
        value = actual["metrics"][name]
        difference = value - baseline
        within = abs(difference) <= tolerance + 1e-12
        rows[name] = {
            "baseline": baseline,
            "reproduced": round(value, 6),
            "difference": round(difference, 6),
            "within_tolerance": within,
            "matches_to_3_decimals": round(value, 3) == baseline,
        }
        if not within:
            better = difference < 0 if LOWER_IS_BETTER[name] else difference > 0
            (improved if better else worse).append(name)

    checks = {
        "features_and_order_match": actual["features"] == expected["features"],
        "train_rows_match": actual["counts"]["train_rows"] == expected["split"]["train_rows"],
        "test_rows_match": actual["counts"]["test_rows"] == expected["split"]["test_rows"],
        "random_state_matches": (actual["model"]["hyperparameters"]["random_state"]
                                 == expected["model"]["hyperparameters"]["random_state"]),
        "predictions_count_matches_test_rows":
            actual["counts"]["predictions"] == actual["counts"]["test_rows"],
    }
    structure_ok = all(checks.values())
    if structure_ok and not worse and not improved:
        outcome = "matches_within_tolerance"
    elif structure_ok and not worse:
        outcome = "better_than_baseline_explanation_required"
    else:
        outcome = "materially_different"

    comparison = {
        "pipeline_run_id": run_id,
        "generated_at_utc": utc_now_iso(),
        "baseline_id": expected["baseline_id"],
        "primary_metric": expected["primary_metric"],
        "tolerance_absolute": tolerance,
        "metrics": rows,
        "structure_checks": checks,
        "outcome": outcome,
    }
    _write_json(resolve("outputs/metric_comparison.json"), comparison)

    print()
    print(f"{'metric':<8}{'baseline':>12}{'reproduced':>14}{'difference':>14}{'within_tol':>12}")
    for name, row in rows.items():
        print(f"{name:<8}{row['baseline']:>12}{row['reproduced']:>14}"
              f"{row['difference']:>14}{str(row['within_tolerance']):>12}")
    log_event(run_id, "reproducibility_check", "comparison finished", outcome=outcome,
              primary_metric=expected["primary_metric"], checks_ok=structure_ok)
    if outcome == "materially_different":
        raise ModelError(
            f"reproduction failed: outside tolerance {tolerance} or structure checks failed: "
            f"{ {k: v for k, v in checks.items() if not v} } worse_metrics={worse}")
    return comparison