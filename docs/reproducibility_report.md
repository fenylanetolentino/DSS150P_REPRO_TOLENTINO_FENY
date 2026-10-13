# Reproducibility Report: DSS150P Lab 4

## Student Information
- Name: Feny Lane Tolentino
- Student Number: 2024110095
- Section: DSS150
- Previous Predictive Analytics Group / Project Title: Aguaviva, Aguirre, Tolentino. "Mood versus Technical Audio Features in Predicting Spotify Track Popularity: An Ablation Study Across 125 Genres" (Mapua University)
- Dataset Name / Source: Spotify Tracks Dataset, Maharshi Pandya, Kaggle (ODbL)
- Predictive Task: Regression (Random Forest Regressor, target `popularity`)

## Attribution
The baseline, problem statement, preprocessing steps, and reported metrics come from the group paper above and its repository (https://github.com/Yuvsii/spotify-popularity-ablation). This repository is an individual reimplementation.

## 1. Engineering-Gap Table

Each current behavior is tagged by type: [environment-specific], [notebook-only], [hidden state], [manual], or [non-deterministic risk]. All were observed in the group notebook.

| Current behavior | Risk | DSS150P concept | Remediation |
|---|---|---|---|
| [environment-specific] Data loaded from Colab paths `/content/spotify.zip` and `/content/spotify_data` | Cannot run on any other machine | Reproducible environments; source manifest | Relative, configured paths in `config/settings.yml`; dataset placement documented in README |
| [hidden state] CSV chosen as "the first .csv in the folder" | A different or extra file silently changes the input | Source identity and checksum | Fixed filename plus SHA-256 verified against `source_manifest.yml` before ingestion |
| [hidden state] Notebook cells executed out of order (for example, the Gradio cell ran as execution 54 before the sensitivity cells 55 and 56; the tuning cells never ran) | Results depend on execution history | Modular code with a command entry point | `src/` modules run through `src/cli.py` |
| [manual] Cleaning done in place on a shared DataFrame (`inplace=True` for the null drop and the dedup) | Not repeatable; re-running a cell changes state | Staging transformation | Pure, rerunnable functions in `src/transform.py` writing to staging |
| [non-deterministic risk] `drop_duplicates(keep='first')` depends on file row order; 720 of 16,641 repeated `track_id` values have differing popularity | The kept row, and so the target, could change if order changes | Deterministic ordering; duplicate prevention | Order by `Unnamed: 0` before dedup; quarantine dropped rows with a reason |
| [hidden state] `StandardScaler` fit on all rows, before the split, overwriting the feature columns | Mild leakage; hidden mutation of the model frame | Preprocessing fitted correctly | Fit on training rows only inside the pipeline; confirm the Random Forest metric is unchanged |
| [notebook-only] Metrics only printed in cell output | No machine-readable output | Persist `metrics.json` plus comparison check | `outputs/metrics.json` compared automatically with `metadata/expected_metrics.json` |
| [environment-specific] No library versions pinned; `xgboost` and `gradio` installed at runtime with `!pip install` | Different versions can change results | Dependency pinning; Docker | Pinned `requirements.txt`; Dockerfile |
| [manual] Tuning documented inconsistently: the paper says 3-fold and untuned, the notebook text says 5-fold, and the tuning cells never ran | Unclear which configuration produced the reported numbers | Baseline capture | Reproduce the paper's untuned defaults; record the discrepancy here |
| [notebook-only] Paper reports 125 genres, but the file has 114 distinct genres; predictions never saved to a file | Documentation does not match data; no output artifact to check | Source profile; delivery outputs | Document in the profile and report; write `outputs/predictions.csv` |

## 2. Prior Project vs. Engineered Reproduction

| Aspect | Prior group project (baseline) | Engineered reproduction |
|---|---|---|
| Model | RandomForestRegressor, untuned defaults, `n_estimators=100`, `random_state=42` (paper Section IV.A) | RandomForestRegressor, `n_estimators=100`, `n_jobs=-1`, `random_state` from `RANDOM_SEED` (default 42) in `config/settings.yml` |
| Target and features | `popularity`; 12 audio features | Same target; the same 12 features, with a fixed order declared in `config/settings.yml` |
| Split | Stratified 80/20 split on popularity tier, `random_state=42` (paper Section III.G) | Same method; test tier counts 904, 9,032 and 8,012 match the paper's Table IV; train 71,792 rows, test 17,948 rows |
| Scaling | `StandardScaler` fit on all rows before the split | `StandardScaler` fit on the training rows only |
| Cleaning | Null drop on `artists`, `album_name`, `track_name`; dedup on `track_id`, keep first (paper Section III.B) | Same rules in `src/transform.py`; explicit order by `Unnamed: 0`; dropped rows go to `data/quarantine/` |
| Rows after cleaning | 89,740 | 89,740 (114,000 raw - 1 null row - 24,259 duplicate rows) |
| Execution | Colab notebook cells, run in varying order | `src/cli.py` commands, or the Airflow DAG `predictive_repro_pipeline` |
| Outputs | Metrics printed in cell output only | `outputs/metrics.json`, `outputs/predictions.csv`, `outputs/model/`, `outputs/metric_comparison.json` |
| Environment | No library versions recorded | Pinned `requirements.txt`, Dockerfile, Docker Compose |

## 3. Metric Consistency

The primary metric is RMSE, as in the prior project. The comparison below is produced automatically by `reproducibility-check` and stored in `outputs/metric_comparison.json` (run `af_quality_checks_run_01`).

| Metric | Baseline | Reproduced | Difference | Within approved tolerance (0.03) | Within default tolerance (0.01) | Matches to 3 decimals |
|---|---|---|---|---|---|---|
| RMSE | 17.959 | 17.972665 | +0.013665 | Yes | No | No |
| MAE | 13.741 | 13.752547 | +0.011547 | Yes | No | No |
| R2 | 0.236 | 0.234386 | -0.001614 | Yes | Yes | No |

All five structure checks pass: features and column order, train rows, test rows, random state, and prediction count equal to the test rows. The outcome recorded in the file is `matches_within_tolerance`.

In the PDF's terms this is "worse but within approved tolerance". RMSE and MAE are slightly worse than the baseline. They fall outside the PDF's default of 0.01 and pass only because of the approved tolerance of 0.03.

### Tolerance approval

The PDF recommends a maximum of 0.01 on the primary metric unless the instructor approves another rule. I asked the instructor by email to approve 0.03. The email said the result against 0.01 would still be reported, and the instructor replied "yes it's okay". The approval is an informal reply, not a formal document. The tolerance is stored in `metadata/expected_metrics.json` as an absolute 0.03 for all metrics, and the default 0.01 is kept in the same file.

### Investigation of the difference

The difference could not be traced to a single cause. Diagnostics were run outside the pipeline, in scripts that are not part of this repository:

- **Scaler.** RMSE was 17.971458 with the scaler fit on all rows, 17.972665 with it fit on the training rows only, and 17.972701 with no scaling. The gap to the baseline stays in all three cases, so the scaler change does not explain it.
- **Data and split.** Cleaned rows (89,740) and test tier counts match the paper. The curated row order equals the cleaned source row order.
- **Random seed.** With the split fixed at seed 42, model seeds 42, 1, 2, 3 and 4 gave RMSE 17.972665, 17.944984, 17.953204, 17.978112 and 17.979166. The spread is 0.034, and the baseline of 17.959 falls inside it.
- **Feature order.** Using the CSV column order instead of the declared order at seed 42 gave RMSE 17.980298.
- **Library versions.** The group notebook installs only `xgboost` and `gradio` at runtime. It records no scikit-learn, numpy, pandas or Python version, so the baseline environment cannot be recovered.

The evidence is consistent with seed and library-version variation, but this was not proven.

### Consistency across environments

`docs/evidence/predictions_comparison.txt` compares the predictions committed from the local run (Python 3.12.10) with the Airflow DAG run `af_manual__20261013T070658` (Python 3.12.14). Both have 17,948 rows with identical `source_row_key`, `track_id`, `popularity_tier` and `actual_popularity`. 822 rows differ in `predicted_popularity`, with a maximum absolute difference of 2.13e-14 and no row differing by more than 1e-9. The results are equal within floating-point precision but not byte-identical across environments.

## 4. Deterministic Controls

- Source identity: SHA-256 `b202fa49909b2d5cef71a04b1d21243cfeb36414535f2ca9272aa646721177bd` of `data/source/dataset.csv`, recorded in `metadata/source_manifest.yml` and verified before ingestion.
- Deterministic ingestion: four batches ordered by `Unnamed: 0`, with the checkpoint in `data/raw/checkpoint.json`.
- Deterministic cleaning: order by `Unnamed: 0`, then dedup on `track_id` keeping the first row. The staging checksum is the same across runs (`dfbce832035abc56757e3ed57cef70e61bef2862292087d83db8ad2faeef8085`).
- Fixed split: stratified on `popularity_tier`, `test_size` 0.2, seed from `RANDOM_SEED`; `metadata/split_manifest.json` records the split with checksums of the train and test row ids.
- Seeds: the Python and NumPy seeds are set in `src/train.py`, and `random_state` is passed to scikit-learn.
- Preprocessing fit on the training rows only, with the feature order declared in `config/settings.yml`.
- Pinned dependencies in `requirements.txt`, a Dockerfile for the pipeline and a Dockerfile for Airflow.
- Automatic check of actual against expected metrics (`reproducibility-check`), with nine data-quality and predictive-integrity checks in `validate-curated`.
- Rerun-safe load: upsert on `track_id` in PostgreSQL.

## 5. Deviations from the Prior Project

1. **Scaler fit on the training rows only**, instead of on all rows before the split. This removes a mild leakage. The diagnostic RMSE changes by about 0.001 (17.971458 vs 17.972665).
2. **Library versions differ from the baseline.** The baseline versions are unknown (see Section 3).
3. **Tolerance of 0.03 instead of the recommended 0.01**, approved informally by the instructor (see Section 3).
4. **Scope.** Only the full-feature Random Forest Regressor was reproduced. The classifier, the ablation study and the Gradio app from the group project are out of scope for this laboratory.
5. **Genre count.** The paper reports 125 genres. The raw file has 114 distinct genres and the curated layer has 113. This difference is not explained.
6. **Explicit ordering.** The prior code deduplicated in file order. The pipeline orders by `Unnamed: 0` before deduplication, and the curated row order was checked against the cleaned source order.

## 6. Known Limitations

- The baseline environment is unrecoverable, so a small metric difference cannot be traced to a library version.
- The approved tolerance of 0.03 is wider than the PDF's recommended 0.01, and RMSE and MAE exceed 0.01. It rests on an informal email reply.
- Results are equal within floating-point precision (up to about 2e-14) across Python 3.12.10 and 3.12.14, not byte-identical.
- The 125 vs 114 vs 113 genre counts are unexplained.
- Only one model (the full-feature Random Forest Regressor) is reproduced.
- Airflow runs in standalone mode with a SQLite metadata database and no login. This setup is for local laboratory use, not for production.
- The diagnostic scripts behind Section 3 are not part of the repository, so a reviewer cannot re-run them.
