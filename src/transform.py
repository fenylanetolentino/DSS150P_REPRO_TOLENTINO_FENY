"""Staging and curated transformations (DSS150P Section 8).

Raw batches -> staging (typed, validated, deduplicated, quarantine)
            -> curated (business rules, selected columns, deterministic order).

Rules come from metadata/data_contract.yml and config/settings.yml.
Both build functions return a summary dictionary so the CLI and the
Airflow tasks can reuse them.
"""
import hashlib
import json
import re
import time

import pandas as pd
import yaml

from src.config import load_settings, log_event, resolve, utc_now_iso

RAW_AUDIT_COLUMNS = {"pipeline_run_id", "ingested_at_utc", "source_file",
                     "source_checksum", "batch_id"}
STAGING_FILE = "staging.parquet"
CURATED_FILE = "curated.parquet"
QUARANTINE_FILE = "staging_quarantine.csv"


class ContractViolation(Exception):
    """Raised when data breaks a contract rule marked 'fail'."""


def load_contract(settings):
    """Read the data contract (the 'contract' block of the YAML file)."""
    with open(resolve(settings["paths"]["contract_file"]), encoding="utf-8") as handle:
        return yaml.safe_load(handle)["contract"]


def expected_count(contract, rule_id):
    """Expected count of a dataset rule: 'expected_count' or the number in its text."""
    for rule in contract["dataset_rules"]:
        if rule["id"] == rule_id:
            if "expected_count" in rule:
                return int(rule["expected_count"])
            return int(re.search(r"\d+", rule["rule"]).group())
    raise ContractViolation(f"dataset rule not found in contract: {rule_id}")


def _expect(actual, expected, label):
    """Fail with a clear message if a count differs from the contract."""
    if actual != expected:
        raise ContractViolation(
            f"{label}: expected {expected} but found {actual} (contract rule)")


def read_raw_batches(settings):
    """Read all committed raw batches as text, in batch order."""
    batch_dir = resolve(settings["paths"]["raw_dir"]) / "batches"
    files = sorted(batch_dir.glob("batch_*.csv"))
    expected = settings["ingestion"]["num_batches"]
    if len(files) != expected:
        raise ContractViolation(
            f"found {len(files)} raw batch files in {batch_dir}, expected {expected}; "
            "run ingest-batches first")
    frames = [pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
              for path in files]
    return pd.concat(frames, ignore_index=True)


def _convert(series, col_type):
    """Convert a text column to its declared type; also flag unparseable values."""
    present = series.notna()
    if col_type == "string":
        return series, pd.Series(False, index=series.index)
    if col_type == "bool":
        mapped = series.map({"True": True, "False": False})
        return mapped, present & mapped.isna()
    numeric = pd.to_numeric(series, errors="coerce")
    unparseable = present & numeric.isna()
    if col_type == "int":
        unparseable = unparseable | (numeric.notna() & (numeric % 1 != 0))
    return numeric, unparseable


def _violation_mask(spec, converted, unparseable):
    """Rows that break the column's nullability, range, or allowed-values rule."""
    mask = unparseable.copy()
    if not spec.get("nullable", True):
        mask |= converted.isna() & ~unparseable
    if spec["type"] in ("int", "float"):
        valid = converted.notna()
        if "min" in spec:
            mask |= valid & (converted < spec["min"])
        if "max" in spec:
            mask |= valid & (converted > spec["max"])
        if "allowed" in spec:
            mask |= valid & ~converted.isin(spec["allowed"])
    return mask


def _finalize_dtypes(frame, specs):
    """Give each column its final declared pandas type."""
    for spec in specs:
        name, col_type = spec["name"], spec["type"]
        has_na = frame[name].isna().any()
        if col_type == "int":
            frame[name] = frame[name].astype("Int64" if has_na else "int64")
        elif col_type == "float":
            frame[name] = frame[name].astype("float64")
        elif col_type == "bool":
            frame[name] = frame[name].astype("boolean" if has_na else "bool")
    return frame


def _record_hashes(frame, columns):
    """SHA-256 of each row's values in a fixed column order."""
    joined = frame[columns].astype(str).agg("|".join, axis=1)
    return joined.map(lambda text: hashlib.sha256(text.encode("utf-8")).hexdigest())


def _dataset_checksum(hashes):
    """One checksum for a whole layer, from its ordered row hashes."""
    return hashlib.sha256("\n".join(hashes).encode("utf-8")).hexdigest()


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def build_staging(settings, run_id):
    """Raw batches -> typed, validated, deduplicated staging layer."""
    started = time.time()
    contract = load_contract(settings)
    specs = contract["columns"]
    names = [spec["name"] for spec in specs]
    row_key = contract["source_row_key"]
    business_key = settings["cleaning"]["dedupe_key"]
    log_event(run_id, "staging", "staging started")

    raw = read_raw_batches(settings)
    missing = [name for name in names if name not in raw.columns]
    if missing:
        raise ContractViolation(f"required columns missing from raw data: {missing}")
    unexpected = [c for c in raw.columns if c not in names and c not in RAW_AUDIT_COLUMNS]
    if unexpected:
        log_event(run_id, "staging", "unexpected columns (warn)", columns=unexpected)
    _expect(len(raw), expected_count(contract, "raw_row_count"), "raw row count")

    # 1. Explicit type conversion and per-column contract checks.
    typed = pd.DataFrame(index=raw.index)
    quarantine_masks = {}
    for spec in specs:
        name = spec["name"]
        converted, unparseable = _convert(raw[name], spec["type"])
        mask = _violation_mask(spec, converted, unparseable)
        count = int(mask.sum())
        action = spec["on_violation"]
        if count and action == "fail":
            raise ContractViolation(
                f"column {name}: {count} rows violate the contract (on_violation=fail)")
        if count:
            log_event(run_id, "staging", "contract violation", column=name,
                      rows=count, action=action)
            if action == "quarantine":
                quarantine_masks[name] = mask
        typed[name] = converted
    if not typed[row_key].is_unique:
        raise ContractViolation(f"{row_key} must be unique (source_row_key_unique)")

    # 2. Quarantine rows with missing key/metadata values.
    reason = pd.Series("", index=typed.index)
    for name, mask in quarantine_masks.items():
        reason.loc[mask] = reason.loc[mask].map(
            lambda text, label=f"contract_{name}": label if text == "" else text + "; " + label)
    is_bad = reason != ""
    meta_cols = settings["cleaning"]["required_metadata_columns"]
    meta_mask = pd.Series(False, index=typed.index)
    for name in meta_cols:
        if name in quarantine_masks:
            meta_mask |= quarantine_masks[name]
    _expect(int(meta_mask.sum()), expected_count(contract, "null_metadata_rows"),
            "rows with null metadata")
    bad_rows = raw.loc[is_bad, names].copy()
    bad_rows["quarantine_reason"] = reason[is_bad]

    # 3. Deterministic order, final types, then duplicate handling.
    clean = typed.loc[~is_bad].copy()
    clean = clean.sort_values(settings["cleaning"]["order_by"], kind="mergesort")
    clean = _finalize_dtypes(clean.reset_index(drop=True), specs)
    duplicate_mask = clean.duplicated(subset=[business_key],
                                      keep=settings["cleaning"]["dedupe_keep"])
    duplicate_rows = clean.loc[duplicate_mask].copy()
    duplicate_rows["quarantine_reason"] = "duplicate_" + business_key
    staging = clean.loc[~duplicate_mask].reset_index(drop=True)
    _expect(int(duplicate_mask.sum()), expected_count(contract, "duplicate_track_id_rows"),
            "duplicate track_id rows")
    _expect(len(staging), expected_count(contract, "staging_row_count"), "staging row count")
    if not staging[business_key].is_unique:
        raise ContractViolation(f"{business_key} is not unique in staging")
    if staging[contract["target"]].isna().any():
        raise ContractViolation("target column has nulls in staging (target_complete)")

    # 4. Audit columns.
    processed_at = utc_now_iso()
    staging = staging.rename(columns={row_key: "source_row_key"})
    business_cols = [name for name in names if name != row_key]
    staging["record_hash"] = _record_hashes(staging, business_cols)
    staging["pipeline_run_id"] = run_id
    staging["processed_at_utc"] = processed_at
    ordered = (["source_row_key"] + business_cols
               + ["pipeline_run_id", "processed_at_utc", "record_hash"])
    staging = staging[ordered]

    # 5. Write outputs (replaced on every run, so reruns never append).
    staging_dir = resolve(settings["paths"]["staging_dir"])
    quarantine_dir = resolve(settings["paths"]["quarantine_dir"])
    staging_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    staging.to_parquet(staging_dir / STAGING_FILE, index=False)
    quarantine = pd.concat([bad_rows, duplicate_rows.astype({c: "object" for c in names})],
                           ignore_index=True)
    quarantine = quarantine.rename(columns={row_key: "source_row_key"})
    quarantine["source_row_key"] = pd.to_numeric(quarantine["source_row_key"]).astype("int64")
    quarantine = quarantine.sort_values("source_row_key", kind="mergesort")
    quarantine["pipeline_run_id"] = run_id
    quarantine["processed_at_utc"] = processed_at
    quarantine.to_csv(quarantine_dir / QUARANTINE_FILE, index=False)

    summary = {
        "pipeline_run_id": run_id,
        "stage": "staging",
        "raw_rows": int(len(raw)),
        "quarantined_null_rows": int(len(bad_rows)),
        "quarantined_duplicate_rows": int(len(duplicate_rows)),
        "staging_rows": int(len(staging)),
        "data_checksum": _dataset_checksum(staging["record_hash"]),
        "duration_seconds": round(time.time() - started, 2),
    }
    _write_json(staging_dir / "staging_summary.json", summary)
    log_event(run_id, "staging", "staging finished",
              **{k: v for k, v in summary.items() if k not in ("stage", "pipeline_run_id")})
    return summary


def build_curated(settings, run_id):
    """Staging -> curated: business rules, derived tier, selected columns."""
    started = time.time()
    contract = load_contract(settings)
    target = settings["target"]
    features = settings["features"]
    log_event(run_id, "curated", "curated started")

    staging_path = resolve(settings["paths"]["staging_dir"]) / STAGING_FILE
    if not staging_path.exists():
        raise ContractViolation(f"staging layer not found at {staging_path}; run staging first")
    staging = pd.read_parquet(staging_path)

    forbidden = {target, contract["derived_tier"]["name"]} & set(features)
    if forbidden:
        raise ContractViolation(f"target leakage: {sorted(forbidden)} in feature list")
    missing = [name for name in features if name not in staging.columns]
    if missing:
        raise ContractViolation(f"feature columns missing from staging: {missing}")

    low_max = settings["split"]["tiers"]["low_max"]
    mid_max = settings["split"]["tiers"]["mid_max"]
    tier = pd.cut(staging[target], bins=[-float("inf"), low_max, mid_max, float("inf")],
                  labels=["Low", "Mid", "High"])
    curated = staging.copy()
    curated[contract["derived_tier"]["name"]] = tier.astype(str)

    processed_at = utc_now_iso()
    curated["pipeline_run_id"] = run_id
    curated["processed_at_utc"] = processed_at
    columns = (["source_row_key", "track_id", "track_genre"] + features
               + [target, contract["derived_tier"]["name"],
                  "pipeline_run_id", "processed_at_utc", "record_hash"])
    curated = curated[columns].sort_values("source_row_key", kind="mergesort")
    curated = curated.reset_index(drop=True)

    _expect(len(curated), expected_count(contract, "staging_row_count"), "curated row count")
    if not curated["track_id"].is_unique:
        raise ContractViolation("track_id is not unique in curated")

    curated_dir = resolve(settings["paths"]["curated_dir"])
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(curated_dir / CURATED_FILE, index=False)

    content_cols = ["track_id", "track_genre"] + features + [target, contract["derived_tier"]["name"]]
    tier_counts = curated[contract["derived_tier"]["name"]].value_counts()
    summary = {
        "pipeline_run_id": run_id,
        "stage": "curated",
        "curated_rows": int(len(curated)),
        "tier_counts": {label: int(tier_counts.get(label, 0)) for label in ["Low", "Mid", "High"]},
        "feature_count": len(features),
        "data_checksum": _dataset_checksum(_record_hashes(curated, content_cols)),
        "duration_seconds": round(time.time() - started, 2),
    }
    _write_json(curated_dir / "curated_summary.json", summary)
    log_event(run_id, "curated", "curated finished",
              **{k: v for k, v in summary.items() if k not in ("stage", "pipeline_run_id")})
    return summary