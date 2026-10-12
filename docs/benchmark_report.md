# Storage Benchmark Report (DSS150P Lab 4, Milestone 06)

Author: Feny Lane Tolentino (student number 2024110095, section DSS150)
Source of numbers: `docs/evidence/benchmark_results.json` (run `run_20261012T052223Z_1732e6`, 2026-10-12)
Reproduce with: `python -m src.cli benchmark-storage`

## 1. Purpose and scope

DSS150P Section 8 asks for the same logical records to be materialized in CSV, JSON/JSONL, Parquet, and
PostgreSQL, and compared for size, write time, read time, filtered access, and partitioning. This report
compares those representations for **this project's workload only**: 89,740 curated Spotify track records
with 18 logical columns. It does not claim any format is best in general.

## 2. Method

- **Dataset:** the curated layer (`data/curated/curated.parquet`), 89,740 rows, columns: `track_id`,
  `source_row_key`, `track_genre`, the 12 audio features, `popularity`, `popularity_tier`, `record_hash`.
  Run-specific audit timestamps are left out so every representation holds the same logical record.
- **Representations:** CSV, JSONL, Parquet (single file), Parquet partitioned by `track_genre`
  (hive-style, one folder per genre), and a PostgreSQL 16.4 table (`benchmark_tracks`, separate from the
  pipeline's `curated_tracks` table).
- **Write time:** measured once per representation.
- **Read time:** each read test ran 5 times. **The first (cold) run is included in the median** and is also
  stored separately in the JSON. The operating system file cache was not cleared, so "cold" means the first
  run in the process, not an empty disk cache.
- **Filtered read:** read only the rows for one genre (`acoustic`, 1,000 rows). CSV and JSONL must read the
  whole file and then filter. Parquet uses a predicate filter, partitioned Parquet reads only the matching
  folder, and PostgreSQL uses `WHERE track_genre = 'acoustic'`.
- **PostgreSQL queries:** one full-table aggregate query (`count`, `avg`, `min`, `max` of `popularity`) and
  one selective query (`WHERE track_genre = ...`). No index exists on `track_genre`, so the selective query
  is a sequential scan.
- **Environment:** Windows 11, Python 3.12.10, pandas 3.0.6, pyarrow 25.0.1, PostgreSQL 16.4 in a Docker
  container reached through a mapped host port.

## 3. Same logical records (acceptance test A10)

After each copy was written, it was read back and compared with the curated reference data: same 89,740
rows, same columns, same values (strings exactly, floating-point values within a relative tolerance of
1e-9 to allow text round trips). All five representations passed (`same_records_as_curated: true`). A
failed comparison stops the run.

## 4. Results

| Representation | Size (bytes) | Size (MB) | Write (s) | Full read, median of 5 (s) | One-genre read, median of 5 (s) |
|---|---:|---:|---:|---:|---:|
| CSV | 15,659,845 | 15.66 | 1.518 | 0.902 | 1.118 |
| JSONL | 38,254,773 | 38.25 | 1.509 | 2.608 | 1.800 |
| Parquet (single file) | 10,567,133 | 10.57 | 0.282 | 0.040 | 0.035 |
| Parquet (partitioned, 113 folders) | 14,159,374 | 14.16 | 0.314 | 0.224 | 0.070 |
| PostgreSQL (table + indexes) | 26,697,728 | 26.70 | 11.725 | 1.704 | 0.049 |

PostgreSQL details: the table alone is 19,898,368 bytes; the rest is the primary-key and unique indexes. The
full-table aggregate query had a median of **0.0495 s** (5 runs, 0.036 to 0.063 s).

Cold-run effect: the first run was close to the median for most representations. The one clear outlier is
partitioned Parquet, whose first full read took 2.107 s against a median of 0.224 s. The median includes
that run but is not much affected by a single outlier.

## 5. Partition demonstration (acceptance test A11)

The dataset has no date, time, or region column, so **no natural partition exists**. As Section 8 allows,
`track_genre` was used as a pedagogical partition for the experiment. It splits the data into 113
partitions of 74 to 1,000 rows each, with no empty or dominant partition and no unsafe folder names.

Loading only the `acoustic` partition from the partitioned Parquet files returned **1,000 rows**. The curated
reference data and the PostgreSQL selective query also returned 1,000 rows (`row_counts_match: true`). The
selected partition occupies 149,495 bytes, about 1.1% of the 14,159,374 bytes in the partitioned copy.

Reading that one partition took a median of 0.070 s against 0.224 s for reading all partitions, about 3.2
times faster. However, the single-file Parquet filtered read (0.035 s) was faster than the partitioned one.
At this dataset size, the cost of discovering and opening 113 small files outweighs the saving from
skipping data. The partitioned copy is also about 34% larger than the single file (14.16 MB against 10.57 MB)
because of per-file overhead.

## 6. Observations

- **Parquet was smallest and fastest to write and to read in full.** It is about 1.5 times smaller than CSV
  and 3.6 times smaller than JSONL, wrote about 5 times faster than CSV, and read the full dataset about 22
  times faster than CSV. It also keeps column types, which CSV and JSONL do not.
- **JSONL was the largest and slowest to read.** It repeats every column name on every record and carries
  no benefit here because the data is flat.
- **CSV is simple and universal but untyped.** Types must be re-inferred on every read. The CSV read used
  the exact float parser (`float_precision="round_trip"`) so values compare exactly, which makes its read
  somewhat slower than pandas' default parser.
- **PostgreSQL answers selective and aggregate queries quickly** (about 0.05 s) without a custom index, and
  enforces keys and ranges. Pulling the whole table into pandas was slow (1.7 s), and the write was the
  slowest (11.7 s).
- **Partitioning did not help at this scale** compared with a single Parquet file (see Section 5).

## 7. Conclusions for this project (no universal "best")

| Use | Choice for this project | Reason |
|---|---|---|
| Source exchange | **CSV** | The source dataset is a CSV, it is readable anywhere, and its SHA-256 checksum is already recorded in the manifest. JSONL adds size without benefit because the records are flat. |
| Pipeline intermediate storage | **Parquet (single file)** | Smallest, fastest to write and read, and keeps column types between staging and curated. The pipeline already uses it for those layers. |
| Model input | **Parquet (single file)** | Training reads every row, and a full read took about 0.04 s. Partitioning gives no advantage because the model needs the whole dataset. |
| Analytical querying | **PostgreSQL** | Fast filtered and aggregate queries, enforced uniqueness and ranges, rerun-safe loading, and run records in `pipeline_runs`. For pulling the entire table into a DataFrame, Parquet was faster. |

Partitioned Parquet is not the choice for any of these uses at the current size. It becomes worth
reconsidering if the dataset grew a lot and queries usually target one genre.

## 8. Limitations

- Single machine, single environment, one run of this experiment. Absolute times will differ elsewhere, and
  the ranking could change with other data sizes, hardware, or library versions.
- Each write was measured once, so write times are noisier than read times.
- The operating system cache was not cleared, so no read is truly cold.
- PostgreSQL ran in a Docker container reached through a port mapping, which adds overhead. Its full-read
  time includes fetching rows through psycopg2 and building a DataFrame, which is client cost and not only
  query cost.
- PostgreSQL was loaded with batched `INSERT` statements, the same method as the pipeline, not `COPY`. Its write time
  is therefore not a limit of what PostgreSQL can do.
- No index was created on `track_genre`, and the selective query used a sequential scan.
- The data is about 90,000 rows. Results should not be generalized to much larger data or to other workloads.