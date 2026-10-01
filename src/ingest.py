"""Full and incremental (batch) ingestion of the immutable source file.

Raw layer rules: the source file is only read, never changed. Everything
written here is a faithful copy of the source text plus audit metadata.
"""
import json
import math
import os
import shutil
import time
from pathlib import Path

import pandas as pd
import yaml

from src.config import load_settings, log_event, resolve, utc_now_iso
from src.validate import sha256_of_file


class IngestionError(Exception):
    """An unrecoverable ingestion problem (the CLI exits with a failure code)."""


class SimulatedInterruption(Exception):
    """Raised on purpose to simulate a crash after a completed batch."""


def read_source(path):
    """Read the CSV as text only, so no value is converted or altered."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def check_source_identity(settings, run_id):
    """Fail unless the source file matches the checksum in the manifest."""
    source_path = resolve(settings["paths"]["source_file"])
    manifest_path = resolve(settings["paths"]["manifest_file"])
    if not source_path.exists():
        raise IngestionError(
            f"Source file not found at {settings['paths']['source_file']}. "
            "Place the dataset as described in the README.")
    with open(manifest_path, encoding="utf-8") as handle:
        expected = yaml.safe_load(handle)["file"]
    actual_sha = sha256_of_file(source_path)
    if actual_sha != expected["sha256"]:
        raise IngestionError(
            "Source checksum does not match source_manifest.yml "
            f"(expected {expected['sha256']}, found {actual_sha}).")
    log_event(run_id, "ingest", "source checksum verified",
              source_checksum=actual_sha)
    return source_path, actual_sha, expected


def full_ingest(settings, run_id):
    """Copy the whole source into a run-specific raw folder with audit data."""
    started = time.time()
    source_path, checksum, expected = check_source_identity(settings, run_id)
    run_dir = resolve(settings["paths"]["raw_dir"]) / "full" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    raw_copy = run_dir / "dataset.csv"
    shutil.copyfile(source_path, raw_copy)  # byte-for-byte copy
    raw_checksum = sha256_of_file(raw_copy)
    frame = read_source(raw_copy)
    if raw_checksum != checksum:
        raise IngestionError("Raw copy checksum differs from the source.")
    if len(frame) != expected["rows"]:
        raise IngestionError(
            f"Raw row count {len(frame)} differs from manifest {expected['rows']}.")
    audit = {
        "pipeline_run_id": run_id,
        "ingested_at_utc": utc_now_iso(),
        "source_file": settings["paths"]["source_file"],
        "source_checksum": checksum,
        "raw_checksum": raw_checksum,
        "rows": len(frame),
        "columns": frame.shape[1],
    }
    (run_dir / "ingestion_audit.json").write_text(
        json.dumps(audit, indent=2), encoding="utf-8")
    log_event(run_id, "ingest_full", "full ingestion complete",
              rows=len(frame), raw_dir=str(run_dir.relative_to(resolve("."))),
              duration_s=round(time.time() - started, 2))
    return audit


# ---------------------------------------------------------------- batches --

def batch_ranges(total_rows, num_batches):
    """Fixed, deterministic (batch_id, start, end) ranges by row position."""
    size = math.ceil(total_rows / num_batches)
    return [(i + 1, i * size, min((i + 1) * size, total_rows))
            for i in range(num_batches) if i * size < total_rows]


def batch_dir(settings):
    return resolve(settings["paths"]["raw_dir"]) / "batches"


def batch_path(settings, batch_id):
    return batch_dir(settings) / f"batch_{batch_id:04d}.csv"


def load_checkpoint(settings):
    path = resolve(settings["paths"]["checkpoint_file"])
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def save_checkpoint(settings, checkpoint):
    """Write the checkpoint atomically (temp file, then rename)."""
    path = resolve(settings["paths"]["checkpoint_file"])
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")
    os.replace(temp, path)


def incremental_ingest(settings, run_id, force=False, stop_after=None):
    """Ingest the source as deterministic batches, resuming from a checkpoint.

    A batch counts as done only after its file is written AND validated.
    The checkpoint advances after that, never before.
    """
    source_path, checksum, expected = check_source_identity(settings, run_id)
    sequence_column = settings["ingestion"]["sequence_column"]
    num_batches = settings["ingestion"]["num_batches"]

    frame = read_source(source_path)
    frame = frame.sort_values(
        by=sequence_column, key=lambda col: col.astype(int), kind="stable"
    ).reset_index(drop=True)
    if frame[sequence_column].duplicated().any():
        raise IngestionError(f"{sequence_column} is not unique in the source.")
    ranges = batch_ranges(len(frame), num_batches)

    out_dir = batch_dir(settings)
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob("*.tmp"):  # leftovers from a crashed run
        stale.unlink()

    checkpoint = load_checkpoint(settings)
    if force:
        for old in out_dir.glob("batch_*.csv"):
            old.unlink()
        checkpoint = None
        log_event(run_id, "ingest_batches", "force: checkpoint and batches reset")
    if checkpoint is None:
        checkpoint = {"source_checksum": checksum, "num_batches": num_batches,
                      "completed_batches": []}
    elif (checkpoint["source_checksum"] != checksum
          or checkpoint["num_batches"] != num_batches):
        raise IngestionError(
            "The checkpoint belongs to a different source file or batch plan. "
            "Rerun with --force to start over.")

    done_now, skipped = [], []
    for batch_id, start, end in ranges:
        if batch_id in checkpoint["completed_batches"]:
            skipped.append(batch_id)
            log_event(run_id, "ingest_batches", "batch already committed, skipped",
                      batch_id=batch_id)
            continue
        began = time.time()
        chunk = frame.iloc[start:end].copy()
        chunk["pipeline_run_id"] = run_id
        chunk["ingested_at_utc"] = utc_now_iso()
        chunk["source_file"] = settings["paths"]["source_file"]
        chunk["source_checksum"] = checksum
        chunk["batch_id"] = str(batch_id)

        final_path = batch_path(settings, batch_id)
        temp_path = final_path.with_suffix(".csv.tmp")
        chunk.to_csv(temp_path, index=False, lineterminator="\n", encoding="utf-8")

        written = read_source(temp_path)  # validate before committing
        if len(written) != len(chunk) or written[sequence_column].duplicated().any():
            temp_path.unlink()
            raise IngestionError(f"Batch {batch_id} failed validation.")
        os.replace(temp_path, final_path)

        checkpoint["completed_batches"] = sorted(
            checkpoint["completed_batches"] + [batch_id])
        checkpoint["last_pipeline_run_id"] = run_id
        checkpoint["updated_at_utc"] = utc_now_iso()
        save_checkpoint(settings, checkpoint)  # advance only now
        done_now.append(batch_id)
        log_event(run_id, "ingest_batches", "batch committed",
                  batch_id=batch_id, rows=len(chunk),
                  first_key=int(chunk[sequence_column].iloc[0]),
                  last_key=int(chunk[sequence_column].iloc[-1]),
                  duration_s=round(time.time() - began, 2))
        if stop_after is not None and batch_id == stop_after:
            raise SimulatedInterruption(
                f"Simulated interruption after batch {batch_id}.")

    summary = {"pipeline_run_id": run_id, "committed_this_run": done_now,
               "skipped": skipped,
               "completed_total": len(checkpoint["completed_batches"]),
               "num_batches": num_batches}
    log_event(run_id, "ingest_batches", "incremental ingestion finished", **summary)
    return summary


def verify_batches(settings, run_id):
    """Count rows and duplicate keys across all committed batch files."""
    sequence_column = settings["ingestion"]["sequence_column"]
    checkpoint = load_checkpoint(settings)
    if checkpoint is None:
        raise IngestionError("No checkpoint found; nothing to verify.")
    with open(resolve(settings["paths"]["manifest_file"]), encoding="utf-8") as handle:
        expected_rows = yaml.safe_load(handle)["file"]["rows"]
    frames = [read_source(batch_path(settings, b))
              for b in checkpoint["completed_batches"]]
    combined = pd.concat(frames, ignore_index=True)
    result = {
        "batches_committed": checkpoint["completed_batches"],
        "total_rows": len(combined),
        "distinct_keys": int(combined[sequence_column].nunique()),
        "duplicate_keys": int(combined[sequence_column].duplicated().sum()),
        "expected_rows_when_complete": expected_rows,
        "complete": len(checkpoint["completed_batches"]) == checkpoint["num_batches"],
    }
    log_event(run_id, "verify_batches", "row and duplicate verification", **result)
    return result