"""Model input layer and model training (DSS150P Section 8, Predictive Reproducibility).

Reads ONLY the curated layer. The split is deterministic (fixed random_state and
stable row order), preprocessing is fitted on the training rows only, every seed
is set, and the split is recorded in metadata/split_manifest.json.
"""
import hashlib
import json
import platform
import random
import time

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from src.config import log_event, resolve, utc_now_iso
from src.transform import CURATED_FILE, load_contract

MODEL_FILE = "model.joblib"
SCALER_FILE = "scaler.joblib"
CONFIG_FILE = "model_config.json"


class ModelError(Exception):
    """Raised when model input, training, evaluation, or the baseline check fails."""


def set_all_seeds(seed):
    """Set the Python and NumPy seeds (scikit-learn gets random_state directly)."""
    random.seed(seed)
    np.random.seed(seed)


def ids_checksum(keys):
    """SHA-256 of a sequence of integer row keys, joined in the given order."""
    return hashlib.sha256("\n".join(str(int(k)) for k in keys).encode("utf-8")).hexdigest()


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def load_model_frame(settings):
    """Read the curated layer and run the predictive-integrity checks."""
    path = resolve(settings["paths"]["curated_dir"]) / CURATED_FILE
    if not path.exists():
        raise ModelError(f"curated layer not found at {path}; run build-curated first")
    features, target = settings["features"], settings["target"]
    tier_name = load_contract(settings)["derived_tier"]["name"]
    leaked = {target, tier_name} & set(features)
    if leaked:
        raise ModelError(f"target leakage: {sorted(leaked)} found in the feature list")
    frame = pd.read_parquet(path)
    needed = ["source_row_key", "track_id", target, tier_name] + features
    missing = [name for name in needed if name not in frame.columns]
    if missing:
        raise ModelError(f"curated data is missing model columns: {missing}")
    if frame[features + [target]].isna().any().any():
        raise ModelError("curated model columns contain nulls")
    return frame.sort_values("source_row_key", kind="mergesort").reset_index(drop=True)


def build_model_input(settings):
    """Deterministic training and evaluation matrices (the Model Input layer)."""
    frame = load_model_frame(settings)
    features, target = settings["features"], settings["target"]
    seed = settings["random_seed"]
    tier_name = load_contract(settings)["derived_tier"]["name"]

    labels = sorted(frame[tier_name].unique())          # alphabetical: High, Low, Mid
    tier_codes = {label: code for code, label in enumerate(labels)}
    stratify = frame[tier_name].map(tier_codes).to_numpy()
    positions = np.arange(len(frame))
    train_pos, test_pos = train_test_split(
        positions, test_size=settings["split"]["test_size"],
        random_state=seed, stratify=stratify)           # train order is NOT sorted
    if set(train_pos.tolist()) & set(test_pos.tolist()):
        raise ModelError("train and test rows overlap")

    x_all = frame[features].to_numpy(dtype="float64")
    y_all = frame[target].to_numpy(dtype="float64")
    scaler = StandardScaler().fit(x_all[train_pos])     # fitted on training rows only
    return {
        "frame": frame, "tier_codes": tier_codes,
        "train_pos": train_pos, "test_pos": test_pos,
        "x_train_raw": x_all[train_pos], "x_test_raw": x_all[test_pos],
        "x_train": scaler.transform(x_all[train_pos]),
        "x_test": scaler.transform(x_all[test_pos]),
        "y_train": y_all[train_pos], "y_test": y_all[test_pos],
        "scaler": scaler,
    }


def write_split_manifest(settings, run_id, model_input):
    """Record how the data was split, with checksums of the train and test row ids."""
    frame = model_input["frame"]
    keys = frame["source_row_key"].to_numpy()
    tier_name = load_contract(settings)["derived_tier"]["name"]
    tiers = frame[tier_name].to_numpy()
    train_keys = keys[model_input["train_pos"]]
    test_keys = keys[model_input["test_pos"]]

    def tier_counts(positions):
        counts = pd.Series(tiers[positions]).value_counts()
        return {label: int(counts.get(label, 0)) for label in model_input["tier_codes"]}

    summary_path = resolve(settings["paths"]["curated_dir"]) / "curated_summary.json"
    curated_checksum = None
    if summary_path.exists():
        curated_checksum = json.loads(summary_path.read_text(encoding="utf-8")).get("data_checksum")

    manifest = {
        "pipeline_run_id": run_id,
        "created_at_utc": utc_now_iso(),
        "method": settings["split"]["method"],
        "random_state": settings["random_seed"],
        "test_size": settings["split"]["test_size"],
        "stratify_on": tier_name,
        "tier_encoding": model_input["tier_codes"],
        "id_column": "source_row_key",
        "input_order": "source_row_key ascending (curated layer)",
        "train_order": "as returned by train_test_split (not sorted)",
        "total_rows": int(len(frame)),
        "train_rows": int(len(train_keys)),
        "test_rows": int(len(test_keys)),
        "train_tier_counts": tier_counts(model_input["train_pos"]),
        "test_tier_counts": tier_counts(model_input["test_pos"]),
        "train_ids_sha256_in_order": ids_checksum(train_keys),
        "train_ids_sha256_sorted": ids_checksum(sorted(train_keys)),
        "test_ids_sha256_in_order": ids_checksum(test_keys),
        "test_ids_sha256_sorted": ids_checksum(sorted(test_keys)),
        "curated_data_checksum": curated_checksum,
    }
    _write_json(resolve(settings["paths"]["split_manifest_file"]), manifest)
    return manifest


def train_model(settings, run_id):
    """Split, fit the scaler on train, train the forest, and persist the artifacts."""
    started = time.time()
    seed = settings["random_seed"]
    config = settings["model"]
    if config["family"] != "RandomForestRegressor":
        raise ModelError(f"unsupported model family in settings: {config['family']}")
    set_all_seeds(seed)
    log_event(run_id, "train", "training started", random_seed=seed)

    model_input = build_model_input(settings)
    manifest = write_split_manifest(settings, run_id, model_input)
    log_event(run_id, "train", "split written", train_rows=manifest["train_rows"],
              test_rows=manifest["test_rows"])

    model = RandomForestRegressor(**config["params"], random_state=seed)
    fit_started = time.time()
    model.fit(model_input["x_train"], model_input["y_train"])
    fit_seconds = round(time.time() - fit_started, 2)

    model_dir = resolve(settings["paths"]["model_dir"])
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_dir / MODEL_FILE)
    joblib.dump(model_input["scaler"], model_dir / SCALER_FILE)
    _write_json(model_dir / CONFIG_FILE, {
        "pipeline_run_id": run_id,
        "trained_at_utc": utc_now_iso(),
        "family": config["family"],
        "hyperparameters": model.get_params(),
        "random_state": seed,
        "features": settings["features"],
        "target": settings["target"],
        "preprocessing": "StandardScaler fitted on the training rows only",
        "train_ids_sha256_in_order": manifest["train_ids_sha256_in_order"],
        "library_versions": {
            "python": platform.python_version(),
            "scikit-learn": sklearn.__version__,
            "numpy": np.__version__,
            "pandas": pd.__version__,
        },
    })
    summary = {
        "train_rows": manifest["train_rows"],
        "test_rows": manifest["test_rows"],
        "fit_seconds": fit_seconds,
        "model_bytes": (model_dir / MODEL_FILE).stat().st_size,
        "duration_seconds": round(time.time() - started, 2),
    }
    log_event(run_id, "train", "training finished", **summary)
    return summary