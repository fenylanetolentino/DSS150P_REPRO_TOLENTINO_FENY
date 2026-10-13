"""Pipeline run record (DSS150P Section 9): start/end status and layer counts per run.

After every CLI command, record_stage() updates outputs/run_summary.json for that
run ID. Counts come from files the pipeline already writes; a count is filled in
only when its source file carries the same pipeline_run_id, so a failed run never
shows counts that belong to an earlier run.
"""
import json

import pandas as pd

from src.config import log_event, resolve, utc_now_iso

SUMMARY_FILE = "run_summary.json"
FINAL_STAGE = "publish-outputs"


def _read_json(path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return None


def collect_counts(settings, run_id):
    """Row counts for raw, staging, curated, train, test and prediction outputs."""
    paths = settings["paths"]
    staging = _read_json(resolve(paths["staging_dir"]) / "staging_summary.json") or {}
    curated = _read_json(resolve(paths["curated_dir"]) / "curated_summary.json") or {}
    split = _read_json(resolve(paths["split_manifest_file"])) or {}
    metrics = _read_json(resolve(paths["metrics_file"])) or {}

    def from_this_run(document, key):
        if document.get("pipeline_run_id") == run_id:
            return document.get(key)
        return None

    predictions = None
    predictions_path = resolve(paths["predictions_file"])
    if metrics.get("pipeline_run_id") == run_id and predictions_path.exists():
        predictions = int(len(pd.read_csv(predictions_path)))
    return {
        "raw_rows": from_this_run(staging, "raw_rows"),
        "staging_rows": from_this_run(staging, "staging_rows"),
        "curated_rows": from_this_run(curated, "curated_rows"),
        "train_rows": from_this_run(split, "train_rows"),
        "test_rows": from_this_run(split, "test_rows"),
        "prediction_rows": predictions,
    }


def overall_status(stages):
    """failed > interrupted > success (final stage done) > running."""
    statuses = {stage["status"] for stage in stages}
    if "failed" in statuses:
        return "failed"
    if "interrupted" in statuses:
        return "interrupted"
    finished = {s["stage"] for s in stages if s["status"] == "success"}
    return "success" if FINAL_STAGE in finished else "running"


def record_stage(settings, run_id, command, status, started_at,
                 duration_seconds, exit_code, error=None):
    """Add or replace this stage in the run record. Never raises."""
    try:
        path = resolve(settings["paths"]["metrics_file"]).parent / SUMMARY_FILE
        record = _read_json(path) or {}
        if record.get("pipeline_run_id") != run_id:
            record = {"pipeline_run_id": run_id, "started_at_utc": started_at,
                      "stages": []}
        previous = next((s for s in record["stages"] if s["stage"] == command), None)
        stages = [s for s in record["stages"] if s["stage"] != command]
        stages.append({
            "stage": command,
            "status": status,
            "started_at_utc": started_at,
            "finished_at_utc": utc_now_iso(),
            "duration_seconds": duration_seconds,
            "exit_code": exit_code,
            "error": error,
            "attempts": (previous["attempts"] if previous else 0) + 1,
        })
        record["stages"] = stages
        record["status"] = overall_status(stages)
        record["updated_at_utc"] = utc_now_iso()
        record["counts"] = collect_counts(settings, run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    except Exception as problem:  # the record must never change a pipeline's result
        log_event(run_id, "run_record", "could not update run record",
                  error=str(problem))
