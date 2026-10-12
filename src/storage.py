"""PostgreSQL load for the curated layer (DSS150P Section 8).

The load is rerun-safe: curated_tracks has a primary key on track_id and the
load uses INSERT ... ON CONFLICT DO UPDATE, rewriting a row only when its
record_hash changed. Every load is recorded in pipeline_runs.
"""
import time

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.config import (get_database_settings, log_event, resolve,
                        utc_now_iso)
from src.transform import CURATED_FILE

STAGE = "load_postgres"
SCHEMA_FILE = "sql/schema.sql"


class StorageError(Exception):
    """Raised when the database load or its verification fails."""


def connect():
    """Open a PostgreSQL connection using settings from the environment."""
    try:
        return psycopg2.connect(connect_timeout=10, **get_database_settings())
    except (psycopg2.OperationalError, RuntimeError) as error:
        raise StorageError(f"cannot connect to PostgreSQL: {error}") from error


def load_columns(settings):
    """Columns loaded into curated_tracks, in table order."""
    return (["track_id", "source_row_key", "track_genre"] + settings["features"]
            + ["popularity", "popularity_tier", "record_hash",
               "pipeline_run_id", "processed_at_utc"])


def apply_schema(conn):
    """Create the tables if they do not exist yet (safe to repeat)."""
    sql = resolve(SCHEMA_FILE).read_text(encoding="utf-8")
    with conn, conn.cursor() as cur:
        cur.execute(sql)


def _start_run(conn, run_id):
    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO pipeline_runs
                   (pipeline_run_id, stage, status, started_at_utc)
               VALUES (%s, %s, 'running', %s)
               ON CONFLICT (pipeline_run_id, stage) DO UPDATE SET
                   status = 'running',
                   started_at_utc = EXCLUDED.started_at_utc,
                   finished_at_utc = NULL,
                   rows_read = NULL, rows_inserted = NULL,
                   rows_updated = NULL, rows_unchanged = NULL,
                   error_message = NULL""",
            (run_id, STAGE, utc_now_iso()))


def _finish_run(conn, run_id, status, counts=None, error=None):
    counts = counts or {}
    with conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE pipeline_runs SET status = %s, finished_at_utc = %s,
                   rows_read = %s, rows_inserted = %s, rows_updated = %s,
                   rows_unchanged = %s, error_message = %s
               WHERE pipeline_run_id = %s AND stage = %s""",
            (status, utc_now_iso(), counts.get("rows_read"),
             counts.get("rows_inserted"), counts.get("rows_updated"),
             counts.get("rows_unchanged"), error, run_id, STAGE))


def _upsert(conn, rows, columns):
    """Load all rows in ONE transaction; returns insert/update/unchanged counts."""
    column_list = ", ".join(columns)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "track_id")
    with conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE load_buffer "
                    "(LIKE curated_tracks INCLUDING DEFAULTS) ON COMMIT DROP")
        execute_values(cur, f"INSERT INTO load_buffer ({column_list}) VALUES %s",
                       rows, page_size=5000)
        cur.execute("SELECT count(*) FROM load_buffer b WHERE NOT EXISTS "
                    "(SELECT 1 FROM curated_tracks t WHERE t.track_id = b.track_id)")
        inserted = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM load_buffer b JOIN curated_tracks t "
                    "ON t.track_id = b.track_id "
                    "WHERE t.record_hash IS DISTINCT FROM b.record_hash")
        updated = cur.fetchone()[0]
        cur.execute(
            f"""INSERT INTO curated_tracks ({column_list})
                SELECT {column_list} FROM load_buffer ORDER BY source_row_key
                ON CONFLICT (track_id) DO UPDATE SET {set_clause},
                    loaded_at_utc = now()
                WHERE curated_tracks.record_hash IS DISTINCT FROM EXCLUDED.record_hash""")
        if cur.rowcount != inserted + updated:
            raise StorageError(
                f"load count mismatch: database wrote {cur.rowcount} rows, "
                f"expected {inserted + updated}")
    return {"rows_read": len(rows), "rows_inserted": inserted,
            "rows_updated": updated,
            "rows_unchanged": len(rows) - inserted - updated}


def load_curated(settings, run_id):
    """Load the curated Parquet layer into PostgreSQL (rerun-safe)."""
    started = time.time()
    path = resolve(settings["paths"]["curated_dir"]) / CURATED_FILE
    if not path.exists():
        raise StorageError(f"curated layer not found at {path}; run build-curated first")
    columns = load_columns(settings)
    frame = pd.read_parquet(path)
    missing = [name for name in columns if name not in frame.columns]
    if missing:
        raise StorageError(f"curated data is missing columns: {missing}")
    frame = frame[columns].sort_values("source_row_key", kind="mergesort")
    frame = frame.astype(object).where(frame.notna(), None)
    rows = list(frame.itertuples(index=False, name=None))
    log_event(run_id, STAGE, "load started", rows_read=len(rows))

    conn = connect()
    try:
        apply_schema(conn)
        _start_run(conn, run_id)
        try:
            counts = _upsert(conn, rows, columns)
        except Exception as error:
            _finish_run(conn, run_id, "failed", error=str(error)[:500])
            raise StorageError(f"load failed and was rolled back: {error}") from error
        _finish_run(conn, run_id, "success", counts)
    finally:
        conn.close()
    counts["duration_seconds"] = round(time.time() - started, 2)
    log_event(run_id, STAGE, "load finished", **counts)
    return counts


def verify_load(settings, run_id):
    """Check the table against the curated file: row count and duplicate keys."""
    path = resolve(settings["paths"]["curated_dir"]) / CURATED_FILE
    if not path.exists():
        raise StorageError(f"curated layer not found at {path}")
    expected = len(pd.read_parquet(path, columns=["track_id"]))
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT count(*), count(DISTINCT track_id), "
                        "count(DISTINCT source_row_key) FROM curated_tracks")
            total, distinct_keys, distinct_rows = cur.fetchone()
    finally:
        conn.close()
    summary = {"table_rows": total, "distinct_track_ids": distinct_keys,
               "duplicate_track_ids": total - distinct_keys,
               "expected_rows": expected, "matches_curated": total == expected}
    log_event(run_id, "verify_postgres", "table verification", **summary)
    if total != expected or distinct_keys != total or distinct_rows != total:
        raise StorageError(f"verification failed: {summary}")
    return summary