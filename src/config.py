"""Settings, run identifiers, and structured logging for the pipeline.

All paths in settings.yml are relative to the project root, so the code runs
the same way on any machine.
"""
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SETTINGS_FILE = PROJECT_ROOT / "config" / "settings.yml"


def load_settings():
    """Return the settings dictionary, with .env overrides applied."""
    load_dotenv(PROJECT_ROOT / ".env")
    with open(SETTINGS_FILE, encoding="utf-8") as handle:
        settings = yaml.safe_load(handle)
    source_override = os.getenv("SOURCE_FILE_PATH")
    if source_override:
        settings["paths"]["source_file"] = source_override
    settings["random_seed"] = int(os.getenv("RANDOM_SEED", "42"))
    return settings


def resolve(relative_path):
    """Turn a project-relative path from the settings into a full Path."""
    return PROJECT_ROOT / relative_path


def utc_now_iso():
    """Current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_run_id():
    """A unique identifier that tags every output of one pipeline run."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{stamp}_{uuid.uuid4().hex[:6]}"


def log_event(run_id, stage, message, **fields):
    """Print one structured log line (JSON) for the given pipeline stage."""
    record = {
        "timestamp": utc_now_iso(),
        "pipeline_run_id": run_id,
        "stage": stage,
        "message": message,
    }
    record.update(fields)
    print(json.dumps(record, default=str), flush=True)