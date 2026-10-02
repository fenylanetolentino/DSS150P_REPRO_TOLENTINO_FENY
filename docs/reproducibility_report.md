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