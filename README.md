# DSS150P Lab 4: Reproducible Spotify Popularity Pipeline

Individual laboratory activity for DSS150P (Fundamentals of Data Engineering), Mapua University.
Author: Feny Lane Tolentino (student number 2024110095, section DSS150).

## 1. Purpose

This repository rebuilds a predictive analytics project from a previous group (a Random Forest Regressor that predicts Spotify track `popularity` from 12 audio features) as a reproducible, rerun-safe data pipeline. The goal is not a better model. The goal is that another person can clone the repository, place the same source file, run the pipeline, and get the same data layers and a model result within a documented tolerance of the baseline.

The pipeline covers: source identity (SHA-256 checksum and manifest), a data contract, full and incremental (checkpointed) ingestion, raw, staging and curated layers, a rerun-safe PostgreSQL load, a storage benchmark, an Airflow DAG, data-quality and code tests, model training and evaluation, and an automatic comparison with the baseline.

## 2. Architecture

See `docs/architecture.png` (components and ports) and `docs/lineage.png` (data artifacts and transformations).

| Layer | Location | Content |
|---|---|---|
| Source | `data/source/dataset.csv` | Original file, never modified (114,000 rows, 21 columns) |
| Raw | `data/raw/` | Four deterministic batches plus `checkpoint.json`; full copy in `data/raw/full/` |
| Staging | `data/staging/` | Typed, validated, deduplicated rows (89,740) with audit columns |
| Quarantine | `data/quarantine/` | Rows rejected from staging |
| Curated | `data/curated/` | Model-ready dataset (89,740 rows) |
| PostgreSQL | table `curated_tracks`, run records in `pipeline_runs` | Rerun-safe load of the curated layer |
| Model input | built from curated by `train-model` | Deterministic train and test split (`metadata/split_manifest.json`) |
| Outputs | `outputs/` | `metrics.json`, `predictions.csv`, `model/`, `metric_comparison.json`, `run_summary.json` |

Docker Compose runs three services: `postgres` (PostgreSQL 16.4 with a health check), `pipeline` (the CLI in a container), and `airflow` (Airflow 3.3.2, standalone). Business logic lives in `src/`; the DAG in `dags/predictive_repro_pipeline.py` only orchestrates it.

## 3. Prerequisites

- Git
- Docker Desktop with Docker Compose (developed with Docker 29.7.2 and Compose v5.4.0)
- Python 3.12 (developed with 3.12.10) to run the CLI and the tests on the host
- A terminal with Bash (developed on Windows 11 with Git Bash)

## 4. Setup (clean machine)

1. Clone the repository:

```bash
git clone https://github.com/fenylanetolentino/DSS150P_REPRO_TOLENTINO_FENY.git
cd DSS150P_REPRO_TOLENTINO_FENY
```

2. Place the source dataset (section 5).

3. Create the environment file and set a database password. Never commit `.env`:

```bash
cp .env.example .env
```

Edit `.env` and replace `change_me` in `POSTGRES_PASSWORD`. The other values can stay. If port 5432 is already used on your machine, change `POSTGRES_PORT`. `AIRFLOW_PORT` (default 8088) can also be set in `.env`.

4. Create the Python environment from the pinned dependencies:

```bash
python -m venv .venv
source .venv/Scripts/activate        # Windows Git Bash; on Linux or macOS: source .venv/bin/activate
pip install -r requirements.txt
```

5. Start PostgreSQL and wait until it is healthy:

```bash
docker compose up -d postgres
docker ps --format "table {{.Names}}\t{{.Status}}" | grep dss150p
```

## 5. Data placement and acquisition

The dataset is the Kaggle "Spotify Tracks Dataset" by Maharshi Pandya (license shown on Kaggle: ODbL). The data folders are git-ignored (`data/*/*` in `.gitignore`), so the source file is not in the repository. Download `dataset_csv.zip` from Kaggle, extract `dataset.csv`, and place it here:

Verify that it is the exact snapshot the pipeline expects. The result must equal the `sha256` value in `metadata/source_manifest.yml`:

```bash
sha256sum data/source/dataset.csv
```

Expected: `b202fa49909b2d5cef71a04b1d21243cfeb36414535f2ca9272aa646721177bd` (20,118,244 bytes).

## 6. Run the pipeline

With the virtual environment active and PostgreSQL healthy, run all stages with one run ID. The loop stops at the first command that fails:

```bash
RID=run_$(date -u +%Y%m%dT%H%M%SZ)
for cmd in ingest-batches verify-batches build-staging build-curated load-postgres validate-curated train-model evaluate-model reproducibility-check publish-outputs; do
  python -m src.cli $cmd --run-id "$RID" || { echo "stopped at $cmd"; break; }
done
```

Exit codes: 0 success, 1 unrecoverable error, 2 simulated interruption. Commands can also run inside the pipeline container, for example:

```bash
docker compose run --rm pipeline python -m src.cli verify-postgres --run-id "$RID"
```

Other commands: `ingest-full` (copy the whole source to raw), `benchmark-storage` (CSV, JSONL, Parquet, partition and PostgreSQL comparison), and `ingest-batches --stop-after N` (simulate a crash after batch N) or `--force` (full refresh). Run `python -m src.cli --help` for the full list.

## 7. Run with Airflow

```bash
docker compose up -d airflow
docker exec dss150p-airflow airflow dags unpause predictive_repro_pipeline
docker exec dss150p-airflow airflow dags trigger predictive_repro_pipeline --run-id my_run_01
docker exec dss150p-airflow airflow tasks states-for-dag-run predictive_repro_pipeline my_run_01
```

The UI is at http://localhost:8088 (no login). The DAG `predictive_repro_pipeline` has ten tasks in this order: ingest_source, validate_raw, transform_staging, build_curated, load_postgres, validate_curated, train_model, evaluate_model, reproducibility_check, publish_outputs. It has `schedule=None` (manual trigger, because the dataset is static), `catchup=False`, a `force_refresh` parameter, 2 retries with a one-minute delay, and a 30-minute task timeout. The pipeline run ID is `af_` plus the Airflow run ID with `:`, `+` and `-` removed.

## 8. Validation

```bash
python -m src.cli validate-curated      # nine quality and predictive-integrity checks, then a PostgreSQL check
python -m src.cli verify-postgres       # row count and duplicate track_id count
python -m src.cli reproducibility-check # actual metrics against the baseline
python -m pytest tests -v               # unit and integration tests
```

`validate-curated` covers schema, completeness, uniqueness, validity, a business rule (row count from the contract), source checksum, no target leakage, feature order and train/test overlap. The tests are in `tests/test_unit_quality_checks.py` and `tests/test_integration_quality.py`.

To show that a rerun adds no duplicates, run section 6 a second time and check that `verify-postgres` still reports 89,740 rows and 0 duplicate `track_id` values.

## 9. Expected outputs

| Item | Expected |
|---|---|
| Raw rows | 114,000 |
| Staging rows | 89,740 (114,000 minus 1 null row and 24,259 duplicate rows, both quarantined) |
| Curated rows | 89,740 (Low 45,159, Mid 40,061, High 4,520) |
| Train / test rows | 71,792 / 17,948 |
| `curated_tracks` in PostgreSQL | 89,740 rows, 0 duplicate `track_id` |
| Staging checksum | `dfbce832035abc56757e3ed57cef70e61bef2862292087d83db8ad2faeef8085` |

Baseline against reproduced metrics (from `outputs/metric_comparison.json`):

| Metric | Baseline | Reproduced | Difference |
|---|---|---|---|
| RMSE (primary) | 17.959 | 17.972665 | +0.013665 |
| MAE | 13.741 | 13.752547 | +0.011547 |
| R2 | 0.236 | 0.234386 | -0.001614 |

The result is `matches_within_tolerance` with the approved absolute tolerance of 0.03. The default tolerance of 0.01 is not met for RMSE and MAE; see `docs/reproducibility_report.md` for the explanation and the instructor approval. Between two Python patch versions the predictions differed by at most 2.13e-14.

## 10. Documentation and evidence

| File | Content |
|---|---|
| `docs/architecture.png` | Components, ports and data movement |
| `docs/lineage.png` | Data artifacts and transformations, with field-level lineage |
| `docs/runbook.md` | Normal execution, common failures, restart and rerun, checkpoint recovery, database reset, rollback and cleanup |
| `docs/benchmark_report.md` | CSV, JSONL, Parquet, partitioned Parquet and PostgreSQL comparison |
| `docs/reproducibility_report.md` | Prior project against reproduction, metric consistency, deterministic controls, deviations, limitations |
| `docs/reflection_answers.md` | Answers to the 15 reflection questions |
| `docs/evidence/` | Run logs and screenshots: ingestion (interrupted and resumed), PostgreSQL load, benchmark, Airflow success, failure and recovery runs, quality-check failure injection |
| `metadata/` | Source profile, manifest, data contract, expected metrics, split manifest |

## 11. Troubleshooting

See section 7 of `docs/runbook.md` for the full table. The most common cases:

| Problem | What to do |
|---|---|
| A host port is already in use | Set `POSTGRES_PORT` or `AIRFLOW_PORT` in `.env` and start the service again |
| Ingestion stops on a checksum error | The source file is not the expected snapshot; replace it and recheck the checksum (section 5) |
| The checkpoint belongs to another source or batch plan | `python -m src.cli ingest-batches --force`, then rerun all later stages |
| `validate-curated` exits with 1 | Read the failed check names in the log, rebuild staging and curated, and rerun the check; do not train until it passes |
| Airflow `load_postgres` fails | Start PostgreSQL (`docker compose up -d postgres`), wait until it is healthy, and trigger a new run |

## 12. Deviations from the suggested repository structure

The structure follows Section 6 of the laboratory PDF, with these additions and differences:

- `Dockerfile.airflow` (a separate image for Airflow) and `.dockerignore` at the project root.
- `src/benchmark.py` for the storage experiment, with the `benchmark` settings block in `config/settings.yml`.
- `docs/evidence/` for run logs and screenshots, and `docs/reflection_answers.md` for the 15 reflection answers.
- `outputs/run_summary.json` and `outputs/metric_comparison.json` in addition to `metrics.json` and `predictions.csv`.
- Staging and curated layers are stored as Parquet files (`staging.parquet`, `curated.parquet`) with a JSON summary each.
- `outputs/model/` and the contents of the `data/` folders are git-ignored; they are regenerated by the pipeline.
- Ingestion uses four batches ordered by the `Unnamed: 0` column. This is a laboratory simulation, not a business watermark.
- The approved reproducibility tolerance is 0.03 instead of the recommended 0.01 (see the reproducibility report).

## 13. Attribution

The problem statement, target, feature list, preprocessing rules, model family and baseline metrics come from the group project "Mood versus Technical Audio Features in Predicting Spotify Track Popularity: An Ablation Study Across 125 Genres" (Aguaviva, Aguirre, Tolentino; Mapua University) and its repository https://github.com/Yuvsii/spotify-popularity-ablation. This repository is an individual reimplementation: the code, configuration, Docker setup, DAG, tests, documentation and evidence are my own work for DSS150P.

## 14. AI use acknowledgment

I used Claude (Anthropic) as an assistant during this laboratory: for step-by-step guidance, drafts of code, SQL, configuration and documentation, debugging help, and for generating the two diagrams from scripts based on this repository's configuration. I ran each step on my own machine and checked the results. I reviewed the content, I am responsible for it, and I can explain the implementation, its failure behavior and its design choices.
