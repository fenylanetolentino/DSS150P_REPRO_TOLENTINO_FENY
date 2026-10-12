"""Storage experiment (DSS150P Section 8).

Writes the same logical records (the curated layer) as CSV, JSONL, Parquet,
partitioned Parquet, and a PostgreSQL table, then measures size, write time,
and read time, and checks every copy against the curated data.

Method notes (also stated in docs/benchmark_report.md):
- Write time is measured once per representation.
- Each read test runs `benchmark.repeats` times. The first (cold) run is
  INCLUDED in the median and also reported separately. The operating system
  file cache is not cleared, so "cold" means first run in this process.
"""
import json
import platform
import shutil
import statistics
import time

import pandas as pd
import pyarrow
from psycopg2.extras import execute_values

from src.config import log_event, resolve, utc_now_iso
from src.storage import StorageError, connect
from src.transform import CURATED_FILE

STRING_COLUMNS = ["track_id", "track_genre", "popularity_tier", "record_hash"]
PG_TYPES = {
    "track_id": "TEXT PRIMARY KEY",
    "source_row_key": "INTEGER NOT NULL UNIQUE",
    "track_genre": "TEXT NOT NULL",
    "key": "SMALLINT NOT NULL",
    "mode": "SMALLINT NOT NULL",
    "time_signature": "SMALLINT NOT NULL",
    "popularity": "SMALLINT NOT NULL",
    "popularity_tier": "TEXT NOT NULL",
    "record_hash": "CHAR(64) NOT NULL",
}


class BenchmarkError(StorageError):
    """Raised when a storage experiment check fails."""


def logical_columns(settings):
    """The logical record: data columns plus record_hash (no run-specific audit times)."""
    return (["track_id", "source_row_key", "track_genre"] + settings["features"]
            + ["popularity", "popularity_tier", "record_hash"])


def load_logical(settings):
    """Read the curated layer as the reference records, ordered by source_row_key."""
    path = resolve(settings["paths"]["curated_dir"]) / CURATED_FILE
    if not path.exists():
        raise BenchmarkError(f"curated layer not found at {path}; run build-curated first")
    columns = logical_columns(settings)
    frame = pd.read_parquet(path)
    missing = [name for name in columns if name not in frame.columns]
    if missing:
        raise BenchmarkError(f"curated data is missing columns: {missing}")
    return (frame[columns].sort_values("source_row_key", kind="mergesort")
            .reset_index(drop=True))


def assert_same_records(expected, actual, label):
    """Fail unless `actual` holds the same logical records as `expected`."""
    columns = list(expected.columns)
    try:
        got = (actual[columns].sort_values("source_row_key", kind="mergesort")
               .reset_index(drop=True))
        pd.testing.assert_frame_equal(expected, got, check_dtype=False,
                                      check_exact=False, rtol=1e-9)
    except (AssertionError, KeyError) as error:
        raise BenchmarkError(
            f"{label}: records differ from the curated data: {str(error)[:300]}") from error


def _timed(func):
    start = time.perf_counter()
    result = func()
    return time.perf_counter() - start, result


def _time_reads(func, repeats):
    """Run `func` several times; return timing statistics and the last result."""
    runs, result = [], None
    for _ in range(repeats):
        seconds, result = _timed(func)
        runs.append(round(seconds, 4))
    return {
        "runs_seconds": runs,
        "first_run_seconds": runs[0],
        "median_seconds": round(statistics.median(runs), 4),
        "min_seconds": min(runs),
        "max_seconds": max(runs),
    }, result


# ---- file formats -------------------------------------------------------

def _write_csv(frame, path):
    frame.to_csv(path, index=False)


def _read_csv(path):
    return pd.read_csv(path, float_precision="round_trip")


def _write_jsonl(frame, path):
    frame.to_json(path, orient="records", lines=True, double_precision=15)


def _read_jsonl(path):
    return pd.read_json(path, lines=True, dtype={c: str for c in STRING_COLUMNS})


def _write_parquet(frame, path):
    frame.to_parquet(path, index=False)


def _read_parquet(path):
    return pd.read_parquet(path)


def _read_then_filter(reader, path, column, value):
    frame = reader(path)
    return frame[frame[column] == value]


def _read_parquet_filtered(path, column, value):
    return pd.read_parquet(path, filters=[(column, "==", value)])


def _read_partitioned(root, column, value=None):
    filters = [(column, "==", value)] if value is not None else None
    frame = pd.read_parquet(root, filters=filters)
    frame[column] = frame[column].astype(str)
    return frame


def _dir_bytes(folder):
    return sum(p.stat().st_size for p in folder.rglob("*") if p.is_file())


# ---- PostgreSQL ---------------------------------------------------------

def _pg_write(conn, table, frame, columns):
    """Recreate the experiment table and insert all rows; returns write seconds."""
    definitions = ", ".join(
        f"{c} {PG_TYPES.get(c, 'DOUBLE PRECISION NOT NULL')}" for c in columns)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} ({definitions})")
    conn.commit()

    def insert():
        rows = list(frame.astype(object).where(frame.notna(), None)
                    .itertuples(index=False, name=None))
        with conn.cursor() as cur:
            execute_values(cur, f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s",
                           rows, page_size=5000)
        conn.commit()

    return _timed(insert)[0]


def _pg_read_full(conn, table, columns):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(columns)} FROM {table} ORDER BY source_row_key")
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _pg_full_table_query(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*), avg(popularity), min(popularity), "
                    f"max(popularity) FROM {table}")
        return cur.fetchone()


def _pg_selective_query(conn, table, columns, part_column, value):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(columns)} FROM {table} "
                    f"WHERE {part_column} = %s ORDER BY source_row_key", (value,))
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


# ---- the experiment -----------------------------------------------------

def run_benchmark(settings, run_id):
    """Run the whole storage experiment and write benchmark_results.json."""
    started = time.time()
    cfg = settings["benchmark"]
    repeats = int(cfg["repeats"])
    if repeats < 5:
        raise BenchmarkError("benchmark.repeats must be at least 5 (DSS150P Section 8)")
    part_column, part_value = cfg["partition_column"], cfg["partition_value"]
    table = cfg["table"]
    columns = logical_columns(settings)
    if part_column not in columns:
        raise BenchmarkError(f"partition column {part_column!r} is not a logical column")

    expected = load_logical(settings)
    expected_part = expected[expected[part_column] == part_value].reset_index(drop=True)
    if expected_part.empty:
        raise BenchmarkError(f"partition value {part_value!r} not found in {part_column}")
    log_event(run_id, "benchmark", "experiment started", rows=len(expected),
              partition_rows=len(expected_part), repeats=repeats)

    formats_dir = resolve(cfg["formats_dir"])
    formats_dir.mkdir(parents=True, exist_ok=True)
    results = {}

    file_formats = [
        ("csv", "csv", _write_csv, _read_csv,
         lambda p: _read_then_filter(_read_csv, p, part_column, part_value)),
        ("jsonl", "jsonl", _write_jsonl, _read_jsonl,
         lambda p: _read_then_filter(_read_jsonl, p, part_column, part_value)),
        ("parquet", "parquet", _write_parquet, _read_parquet,
         lambda p: _read_parquet_filtered(p, part_column, part_value)),
    ]
    for name, extension, writer, reader, filtered_reader in file_formats:
        path = formats_dir / f"curated.{extension}"
        if path.exists():
            path.unlink()
        write_seconds, _ = _timed(lambda: writer(expected, path))
        full_stats, full_frame = _time_reads(lambda: reader(path), repeats)
        assert_same_records(expected, full_frame, name)
        part_stats, part_frame = _time_reads(lambda: filtered_reader(path), repeats)
        assert_same_records(expected_part, part_frame, f"{name} (one partition value)")
        results[name] = {
            "size_bytes": path.stat().st_size,
            "write_seconds": round(write_seconds, 4),
            "read_full": full_stats,
            "read_one_genre": part_stats,
            "same_records_as_curated": True,
        }
        log_event(run_id, "benchmark", f"{name} done",
                  size_bytes=results[name]["size_bytes"],
                  read_full_median=full_stats["median_seconds"])

    # Partitioned Parquet: one folder per genre (hive style).
    root = resolve(settings["paths"]["partitioned_dir"]) / "curated_by_genre"
    if root.exists():
        shutil.rmtree(root)
    write_seconds, _ = _timed(
        lambda: expected.to_parquet(root, partition_cols=[part_column], index=False))
    full_stats, full_frame = _time_reads(lambda: _read_partitioned(root, part_column), repeats)
    assert_same_records(expected, full_frame, "partitioned parquet")
    part_stats, part_frame = _time_reads(
        lambda: _read_partitioned(root, part_column, part_value), repeats)
    assert_same_records(expected_part, part_frame, "partitioned parquet (one partition)")
    partition_dir = root / f"{part_column}={part_value}"
    results["parquet_partitioned"] = {
        "size_bytes": _dir_bytes(root),
        "write_seconds": round(write_seconds, 4),
        "read_full": full_stats,
        "read_one_genre": part_stats,
        "same_records_as_curated": True,
        "partition_count": len([p for p in root.iterdir() if p.is_dir()]),
        "selected_partition_bytes": _dir_bytes(partition_dir),
    }
    log_event(run_id, "benchmark", "parquet_partitioned done",
              partitions=results["parquet_partitioned"]["partition_count"])

    # PostgreSQL
    conn = connect()
    try:
        write_seconds = _pg_write(conn, table, expected, columns)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size(%s::regclass), "
                        "pg_relation_size(%s::regclass), version()", (table, table))
            total_bytes, table_bytes, version = cur.fetchone()
        full_stats, full_frame = _time_reads(
            lambda: _pg_read_full(conn, table, columns), repeats)
        assert_same_records(expected, full_frame, "postgresql")
        agg_stats, agg_row = _time_reads(lambda: _pg_full_table_query(conn, table), repeats)
        if agg_row[0] != len(expected):
            raise BenchmarkError(f"postgresql full-table query counted {agg_row[0]} rows, "
                                 f"expected {len(expected)}")
        part_stats, part_frame = _time_reads(
            lambda: _pg_selective_query(conn, table, columns, part_column, part_value),
            repeats)
        assert_same_records(expected_part, part_frame, "postgresql (selective query)")
    finally:
        conn.close()
    results["postgresql"] = {
        "size_bytes": int(total_bytes),
        "table_only_bytes": int(table_bytes),
        "size_note": "pg_total_relation_size (table + indexes); table_only_bytes excludes indexes",
        "write_seconds": round(write_seconds, 4),
        "read_full": full_stats,
        "read_one_genre": part_stats,
        "full_table_query_aggregate": agg_stats,
        "same_records_as_curated": True,
    }

    partition_demo = {
        "partition_column": part_column,
        "partition_value": part_value,
        "expected_rows": int(len(expected_part)),
        "parquet_partition_rows": int(len(part_frame_from_partition(root, part_column, part_value))),
        "postgresql_rows": int(len(part_frame)),
    }
    partition_demo["row_counts_match"] = (
        partition_demo["expected_rows"] == partition_demo["parquet_partition_rows"]
        == partition_demo["postgresql_rows"])
    if not partition_demo["row_counts_match"]:
        raise BenchmarkError(f"partition row counts differ: {partition_demo}")

    output = {
        "pipeline_run_id": run_id,
        "generated_at_utc": utc_now_iso(),
        "dataset_rows": int(len(expected)),
        "logical_columns": columns,
        "method": {
            "repeats_per_read_test": repeats,
            "cold_run_included_in_median": True,
            "writes_measured": "once per representation",
            "os_cache_cleared": False,
            "partition_note": "no natural date/time/region column; track_genre is a "
                              "pedagogical partition for this experiment",
        },
        "environment": {
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "pyarrow": pyarrow.__version__,
            "platform": platform.platform(),
            "postgres": version.split(",")[0],
        },
        "formats": results,
        "partition_demo": partition_demo,
        "duration_seconds": round(time.time() - started, 2),
    }
    results_path = resolve(cfg["results_file"])
    results_path.parent.mkdir(parents=True, exist_ok=True)
    with open(results_path, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2)
    log_event(run_id, "benchmark", "experiment finished",
              results_file=cfg["results_file"], partition_demo=partition_demo,
              duration_seconds=output["duration_seconds"])
    _print_summary(results)
    return output


def part_frame_from_partition(root, column, value):
    """Load only one partition (its folder) from the partitioned Parquet files."""
    return _read_partitioned(root, column, value)


def _print_summary(results):
    print()
    print(f"{'representation':<22}{'size_bytes':>12}{'write_s':>10}"
          f"{'read_full_median_s':>20}{'read_one_genre_median_s':>25}")
    for name, item in results.items():
        print(f"{name:<22}{item['size_bytes']:>12}{item['write_seconds']:>10}"
              f"{item['read_full']['median_seconds']:>20}"
              f"{item['read_one_genre']['median_seconds']:>25}")
    agg = results["postgresql"]["full_table_query_aggregate"]["median_seconds"]
    print(f"postgresql full-table aggregate query median: {agg} s")