# Reflection Answers: DSS150P Lab 4

Author: Feny Lane Tolentino (student number 2024110095, section DSS150)
These answers refer to the files, numbers and evidence in this repository. Answers 9 and 13 to 15 are design reasoning, not measurements, and say so.

## Q1. What aspects of the previous predictive analytics project prevented another person from reproducing it immediately?

The notebook loaded data from Colab paths (`/content/spotify.zip`, `/content/spotify_data`) and picked "the first .csv in the folder", so the input was not identified. Cells ran out of order, and cleaning happened in place on a shared DataFrame, so the result depended on execution history. The deduplication kept the first row, which depends on file order, and 720 of 16,641 repeated `track_id` values have differing popularity, so the target could change with the order. The `StandardScaler` was fit on all rows before the split. No library versions were recorded. Metrics were only printed, predictions were never saved, and the tuning description was inconsistent (paper: 3-fold and untuned; notebook text: 5-fold; the tuning cells never ran). The paper also reports 125 genres while the file has 114. These are listed in the gap table in `docs/reproducibility_report.md`.

## Q2. Which artifact is the true authoritative source: the original file, the raw ingested copy, the curated dataset, or the model-ready dataset? Explain the role of each.

The original file `data/source/dataset.csv` is the only authoritative source. It is never modified, and its SHA-256 (`b202fa49909b2d5cef71a04b1d21243cfeb36414535f2ca9272aa646721177bd`) is recorded in `metadata/source_manifest.yml` and checked before ingestion. The raw copy (four batches plus `checkpoint.json`, 114,000 rows) is the faithful record of what was ingested and when. Staging (89,740 rows) is the typed, validated, deduplicated version. The curated dataset (89,740 rows, 20 columns) is the stable model-ready table that is loaded into PostgreSQL. The model-ready matrices are the deterministic train and test split of the curated data, described by `metadata/split_manifest.json`. Everything after the source can be rebuilt from the source with the code and configuration in the repository, so none of them is authoritative on its own.

## Q3. What identifier or rule did you use to prevent duplicates, and what assumption makes that rule valid?

The business key is `track_id`. In staging, duplicates are removed by keeping the first row ordered by `Unnamed: 0`, and the other rows (24,259) are quarantined. In PostgreSQL, `curated_tracks` has a primary key on `track_id` and a unique `source_row_key`, and the load is an upsert (`ON CONFLICT`) that compares a `record_hash`. In ingestion, committed batches are skipped through the checkpoint. The rule assumes that one `track_id` is one logical record. That assumption comes from the prior project and is not proven by the data: 720 of 16,641 repeated `track_id` values have different popularity values. Keeping the first row by a fixed order makes the choice deterministic, but it is still a choice.

## Q4. If your dataset has no natural update timestamp, how did you simulate incremental ingestion without pretending that the generated checkpoint is a real business timestamp?

The dataset has no event or update timestamp. The source is split into four deterministic batches by the sequence column `Unnamed: 0`, and the checkpoint is stored outside the code in `data/raw/checkpoint.json`. It records the source checksum, the batch plan and the completed batches, and it advances only after a batch is written and validated. The checkpoint is a batch counter, not a time, and it is documented as a laboratory simulation. The evidence in `docs/evidence/ingestion_run_log.txt` shows a first run, an interrupted run (`--stop-after 1`), a resumed run, a final count of 114,000 rows with 0 duplicates, and a rerun that skips all batches. `--force` resets the checkpoint.

## Q5. Which transformations belong in staging versus curated, and why?

Staging holds the technical cleaning: type conversion from the data contract, null and range checks, duplicate handling (`track_id`, keep first), quarantine of rejected rows, and the audit columns `source_row_key`, `pipeline_run_id`, `processed_at_utc` and `record_hash`. It keeps all source fields (24 columns), so nothing is lost before the model decision. Curated holds the decisions that serve the model: it drops `artists`, `album_name`, `track_name`, `duration_ms` and `explicit`, derives `popularity_tier` (cut-offs 33 and 66), and selects the 12 features, the target and the keys (20 columns). Keeping them apart means a modelling change only rebuilds curated, while the cleaning rules stay in one place.

## Q6. Which storage format performed best for your workload? Why should your result not be generalized to every data engineering workload?

It depends on the use (`docs/benchmark_report.md`). Single-file Parquet was best for the pipeline: smallest (10.57 MB), fastest write (0.282 s) and fastest full read (0.040 s median of 5). PostgreSQL was best for selective queries (0.049 s for one genre) but slowest to write (11.725 s) and slow to pull in full (1.704 s). CSV was 15.66 MB and JSONL was largest (38.25 MB) and slowest to read in full (2.608 s). The result should not be generalized because it comes from one machine and one run, about 90,000 rows, writes measured once, an operating system cache that was not cleared, PostgreSQL reached through a Docker port mapping, batched `INSERT` instead of `COPY`, no index on `track_genre`, and a partition that gave no advantage at this size.

## Q7. What happens if Airflow retries your load task after the database committed but the task process did not receive confirmation?

The retry runs the same `load-postgres` command with the same run ID and the same curated data. The first attempt's rows are already committed, because the load runs in one transaction. The second attempt tries to insert the same 89,740 rows and finds them already present, so it changes nothing: `ON CONFLICT (track_id) DO UPDATE` rewrites a row only if its `record_hash` changed, and none did. The table stays at 89,740 rows with 0 duplicate `track_id` values, and the rows are reported as unchanged. The task is configured with 2 retries and a one-minute delay.

## Q8. How does your design make that retry safe?

Four things work together. The primary key on `track_id` (and the unique `source_row_key`) makes duplicates impossible at the database level. The upsert compares `record_hash`, so an identical row is not rewritten. The whole load is one transaction, so a failure part-way leaves no half-loaded table. The run record in `pipeline_runs` is also an upsert keyed on `(pipeline_run_id, stage)`: a retry resets that one row to `running` and updates it again, instead of adding a second row. The curated data is deterministic, so the retry loads the same values. This was checked by loading twice (89,740 rows inserted the first time, all unchanged the second) and by the failed-then-recovered Airflow run (`docs/evidence/airflow_failed_run.txt` and `airflow_recovery_run.txt`).

## Q9. Which data-quality check is most likely to detect a change that would silently damage model accuracy?

In my view it is `train_test_overlap_and_manifest_match`, a predictive-integrity check. A silent change in the dedup order, the row set or the cleaning rules can keep every schema, null and range check passing and still change which rows go to training and test. This check recomputes the split and compares the train and test row counts and the checksums of their row ids with `metadata/split_manifest.json`, and it also checks that train and test do not overlap. In the failure injection (`docs/evidence/quality_failure_injection.txt`) it failed with overlapping `track_id` 1 when a duplicate key was injected. `source_checksum_matches` covers a different source file, and `reproducibility-check` catches the final metric moving. This answer is reasoning about the checks, not a measurement of which check would fire first in every case.

## Q10. How do you know that your training and test rows are the same logical rows used in your baseline result?

I know it up to what the evidence allows, but not row by row. The cleaned dataset has 89,740 rows, as in the paper. The split uses the same method as the paper (stratified on popularity tier, `random_state=42`, test size 0.2), and the test tier counts (904, 9,032 and 8,012) match the paper's Table IV. `metadata/split_manifest.json` records the split with checksums of the train and test row ids, and `outputs/metric_comparison.json` shows the structure checks `train_rows_match` and `test_rows_match` as true. The group did not save the row ids of its own split, so I cannot prove the rows are identical to theirs, only that the counts and the method are.

## Q11. What sources of nondeterminism remain in your model or environment?

The baseline library versions are unknown, so differences from them cannot be controlled. Different Python versions give tiny differences: the local run (Python 3.12.10) and the Airflow run (Python 3.12.14) differ by at most 2.13e-14 in 822 of 17,948 predictions, so they are equal within floating-point precision but not byte-identical. The Random Forest uses `n_jobs=-1` and a seed, and the model seed itself changes RMSE (seeds 42, 1, 2, 3 and 4 gave a spread of 0.034). Audit values such as `pipeline_run_id` and `processed_at_utc` change on every run by design.

## Q12. If the reproduced metric differs from the original, how can you determine whether the cause is data, preprocessing, split logic, dependencies, model parameters, or nondeterministic execution?

Check one cause at a time, from the cheapest. Data: compare the source SHA-256, the raw, staging and curated row counts, and the staging checksum. Split logic: compare the tier counts and the checksums in `split_manifest.json`. Preprocessing: rerun with the scaler fit on all rows, on train only, and without scaling. Model parameters: compare `outputs/model/model_config.json` with the baseline settings. Dependencies: compare the library versions in `outputs/metrics.json` with `requirements.txt`. Nondeterminism: rerun with several model seeds. I did this for the 0.0137 RMSE difference: the scaler variants all stayed near 17.97, the data and split matched, and the seed spread (0.034) contains the baseline of 17.959. The baseline library versions could not be recovered, so that cause was not ruled out.

## Q13. What would you monitor if this pipeline ran every day in production?

This is a design proposal. I would monitor: the status and duration of each stage (`outputs/run_summary.json` and `pipeline_runs`), Airflow task failures and retries, and the row counts for raw, staging, curated, train, test and predictions. I would add the quarantine counts (currently 1 null row and 24,259 duplicates), since a jump would mean the source changed. I would also watch the result of the nine quality checks, the source checksum, the duplicate `track_id` count in `curated_tracks` (expected 0), and the metrics against the baseline, including drift over time. For the database I would add its health check and disk use.

## Q14. Which part of your current architecture would become the first bottleneck if the dataset grew by 100x?

This is an estimate, not a measurement. I expect the single-machine, in-memory pandas steps. `build-staging` reads all raw batches into memory (42.43 s at the current size), and the PostgreSQL load uses batched `INSERT` statements (11.725 s for 89,740 rows in the benchmark), which would grow to the order of 20 minutes if it scaled linearly. Model training would also grow. The Airflow setup (standalone, SQLite metadata, LocalExecutor) is a second limit. The first fix would be reading and loading in chunks and using `COPY` for the database load.

## Q15. What would you change if the dataset became sensitive or regulated data?

This is a design proposal. The source and raw data would not be committed to GitHub, as PDF section 4.4 says; the repository would keep only the manifest, acquisition instructions, filename and checksum. I would move credentials out of `.env` into a secrets manager. Airflow currently runs with no login, so I would turn on authentication and roles. PostgreSQL would not be exposed on a host port, and I would add encrypted connections, restricted database roles and encryption at rest. Identifying fields would be masked or pseudonymized before staging, access would be logged, and logs, the quarantine file and the predictions would have to be checked so they do not leak values. I would also add a retention and deletion rule.
