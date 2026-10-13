# Runbook: DSS150P Lab 4 Spotify Popularity Pipeline

This runbook explains how to run the pipeline, check a run, restart or rerun it, recover the ingestion checkpoint, handle common failures, reset the database, and roll back or clean up. Commands assume a Git Bash terminal at the project root with the virtual environment active (`source .venv/Scripts/activate` on Windows, `source .venv/bin/activate` on Linux or macOS).

## 1. Services and ports

| Service | Container | Purpose | Host port |
|---|---|---|---|
| postgres | dss150p-postgres | PostgreSQL 16.4 target database, with a health check | `POSTGRES_PORT` in `.env` (default 5432) |
| pipeline | dss150p-pipeline | Runs the pipeline CLI in a container | none |
| airflow | dss150p-airflow | Airflow UI and scheduler (standalone), no login | 8088 (`AIRFLOW_PORT`) |

Airflow UI: http://localhost:8088. The Airflow container mounts the whole project folder, so edited code is visible to it without a rebuild.

## 2. Before a run

1. Copy `.env.example` to `.env` and set a database password. Never commit `.env`.
2. Place the source file at `data/source/dataset.csv`.
3. Verify the source checksum. The result must equal the `sha256` value in `metadata/source_manifest.yml`:

```bash
sha256sum data/source/dataset.csv
```

Expected: `b202fa49909b2d5cef71a04b1d21243cfeb36414535f2ca9272aa646721177bd`

4. Start the database and wait until it is healthy:

```bash
docker compose up -d postgres
docker ps --format "table {{.Names}}\t{{.Status}}" | grep dss150p
```

When running outside Docker, `.env` must use `POSTGRES_HOST=localhost` and the same `POSTGRES_PORT` as the container mapping.

## 3. Normal execution

### Option A: command line

Every command accepts `--run-id`. Use one run ID for the whole run so outputs can be traced to it. The loop stops at the first command that fails.

```bash
RID=run_$(date -u +%Y%m%dT%H%M%SZ)
for cmd in ingest-batches verify-batches build-staging build-curated load-postgres validate-curated train-model evaluate-model reproducibility-check publish-outputs; do
  python -m src.cli $cmd --run-id "$RID" || { echo "stopped at $cmd"; break; }
done
```

Exit codes: 0 success, 1 unrecoverable error, 2 simulated interruption.

To run a single command inside the pipeline container instead, prefix it:

```bash
docker compose run --rm pipeline python -m src.cli verify-postgres --run-id "$RID"
```

### Option B: Airflow

```bash
docker exec dss150p-airflow airflow dags unpause predictive_repro_pipeline
docker exec dss150p-airflow airflow dags trigger predictive_repro_pipeline --run-id my_run_01
docker exec dss150p-airflow airflow tasks states-for-dag-run predictive_repro_pipeline my_run_01
```

The DAG can also be triggered from the UI, which offers the `force_refresh` parameter (`--force` on ingestion). The ten tasks run in this order: ingest_source, validate_raw, transform_staging, build_curated, load_postgres, validate_curated, train_model, evaluate_model, reproducibility_check, publish_outputs. Tasks retry twice with a one-minute delay and time out after 30 minutes. The pipeline run ID is `af_` plus the Airflow run ID with `:`, `+` and `-` removed. The DAG has no schedule (`schedule=None`) because the dataset is static, and `catchup=False`.

## 4. Checking a run

| What to check | Where |
|---|---|
| Status of each stage, durations, errors, and counts for raw, staging, curated, train, test and prediction rows | `outputs/run_summary.json` |
| Data-quality and predictive-integrity results (nine checks) | log lines from `validate-curated` (stage `validate_curated`) |
| Baseline vs reproduced metrics | `outputs/metric_comparison.json` |
| Metrics and library versions | `outputs/metrics.json` |
| Split identity | `metadata/split_manifest.json` |
| Database tables | see below |

```bash
MSYS_NO_PATHCONV=1 docker exec dss150p-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt"'
python -m src.cli verify-postgres
```

A healthy result is 89,740 rows in `curated_tracks` and 0 duplicate `track_id` values. Every CLI command updates `outputs/run_summary.json` for its run ID.

## 5. Restart and rerun

Every stage is safe to rerun.

| Stage | Behavior on rerun |
|---|---|
| ingest-batches | Skips batches already committed in the checkpoint; resumes at the first uncommitted batch |
| build-staging, build-curated | Rebuilt deterministically from the layer before; the same input gives the same output |
| load-postgres | Upsert keyed on `track_id` (`ON CONFLICT`, with `record_hash`); a second load reports all rows unchanged |
| train-model, evaluate-model | Fixed seed and fixed split; the same curated data gives the same metrics |

To restart a failed command-line run, fix the cause and run the failed command and every command after it again, with a new run ID. To restart a failed Airflow run, fix the cause and trigger a new DAG run. Rerunning does not create duplicate records (check with `verify-postgres`).

## 6. Checkpoint recovery

Ingestion splits the source into four deterministic batches by `Unnamed: 0`. The checkpoint is stored outside the code in `data/raw/checkpoint.json`. It records the source checksum, the batch plan, and the completed batches. It advances only after a batch is written and validated.

- Interrupted run (exit code 2, or a crashed process): run `python -m src.cli ingest-batches` again. Committed batches are skipped and ingestion resumes. No batch is ingested twice.
- To rehearse an interruption: `python -m src.cli ingest-batches --stop-after 1` stops after batch 1 and exits with code 2.
- Checkpoint belongs to a different source or batch plan: ingestion stops and tells you to rerun with `--force`.
- `--force` resets the checkpoint and the raw batches. After it, rerun everything downstream (staging, curated, load, and the rest).
- `python -m src.cli verify-batches` checks the committed batches for duplicate keys and, when all batches are committed, for the 114,000-row total.

## 7. Common failures

| Symptom | Likely cause | Recovery |
|---|---|---|
| `load_postgres` fails, then downstream tasks show `upstream_failed`; error such as "could not translate host name" | PostgreSQL container is stopped or unreachable | `docker compose up -d postgres`, wait for healthy, then trigger a new run. Outputs from the previous successful run stay unchanged. Evidence: `docs/evidence/airflow_failed_run.txt`, `docs/evidence/airflow_recovery_run.txt` |
| `validate-curated` exits 1 with "check failed" lines | Curated data broke a rule (duplicate key, out-of-range value, null, wrong row count, train/test overlap, feature order) | Read the failed check names in the log. Rebuild the layers from the cause: `build-staging`, then `build-curated`. Rerun `validate-curated`. Do not train until it passes. Evidence: `docs/evidence/quality_failure_injection.txt` |
| Ingestion stops with `IngestionError` before any batch is written | Source file differs from the manifest checksum | Replace `data/source/dataset.csv` with the correct file and recheck the checksum (section 2) |
| Ingestion says the checkpoint belongs to a different source or batch plan | Source or batch count changed since the checkpoint was written | `python -m src.cli ingest-batches --force`, then rerun all later stages |
| Staging or curated stops with `ContractViolation` (for example a row count differing from `metadata/data_contract.yml`) | Raw batches are incomplete or the source differs | Run `verify-batches`, confirm the source checksum, rerun ingestion, then staging |
| `reproducibility-check` exits 1 ("reproduction failed") | Metrics fell outside the declared tolerance, or structure checks failed | Open `outputs/metric_comparison.json`. Compare library versions in `outputs/metrics.json` with `requirements.txt`, and run `validate-curated` to check the split. Do not edit `metadata/expected_metrics.json` to force a pass |
| Port already in use when starting a service | Another program uses the host port | Set `POSTGRES_PORT` or `AIRFLOW_PORT` in `.env` and start the service again |

For an Airflow failure, the failure callback writes a JSON line with the DAG, task, run ID, try number and error to the task log (UI: open the run, then the task, then Logs).

## 8. Database reset

Clear the loaded data and keep the container:

```bash
MSYS_NO_PATHCONV=1 docker exec dss150p-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "TRUNCATE curated_tracks, pipeline_runs;"'
python -m src.cli load-postgres
python -m src.cli verify-postgres
```

Remove the database completely. This deletes the two named volumes of this project: `pgdata` (the PostgreSQL data) and `airflow_state` (the Airflow metadata database `airflow.db`, which holds the DAG run history). It does not touch `data/`, `outputs/` or the ingestion checkpoint:

```bash
docker compose down -v
docker compose up -d postgres
python -m src.cli load-postgres
python -m src.cli verify-postgres
```

Either way the curated layer in `data/curated/` is the source for the reload, so build it first if it is missing. After `docker compose down -v`, wait until postgres is healthy (section 2, step 4) before `load-postgres`. The Airflow container is removed too and its database is new, so start it again with `docker compose up -d airflow` and unpause the DAG (section 3, option B).

## 9. Rollback and cleanup

- Restore the generated outputs to the last commit: `git checkout -- outputs metadata/split_manifest.json`
- Rebuild the derived layers from raw: delete `data/staging/staging.parquet`, `data/staging/staging_summary.json`, `data/curated/curated.parquet` and `data/curated/curated_summary.json`, then run `build-staging` and `build-curated`.
- Rebuild raw from the source: `python -m src.cli ingest-batches --force`, then rerun every later stage.
- Stop the services and keep the data: `docker compose stop`. Remove the containers and keep the volumes: `docker compose down`.
- Never edit or delete `data/source/dataset.csv`. It is the immutable source, identified by its checksum.

## 10. Evidence files

All evidence is in `docs/evidence/`: ingestion run log (first, interrupted, resumed run), PostgreSQL load log, storage benchmark results, Airflow success run and graph, Airflow failed run and recovery run, quality-check failure injection and recovery, run record snapshots, and the prediction comparison between the local run and the Airflow container run.
