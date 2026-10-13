"""DSS150P Lab 4 - Airflow DAG for the reproducible predictive pipeline.

The DAG holds no business logic: every task runs `python -m src.cli <command>`.

schedule=None: the source is a static, immutable dataset (see
metadata/source_manifest.yml), so there is nothing new to process on a timer.
The DAG is triggered manually. catchup=False: no past intervals are back-filled.
"""
import json
import logging
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PROJECT_DIR = os.environ.get("PIPELINE_PROJECT_DIR", "/opt/airflow/project")
PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/airflow/pipeline-venv/bin/python")

# One pipeline_run_id for the whole DAG run, derived from the Airflow run_id.
RUN_ID = "af_{{ run_id | replace(':', '') | replace('+', '') | replace('-', '') }}"

log = logging.getLogger("dss150p.dag")


def log_failure(context):
    """Log useful context when a task fails."""
    ti = context["task_instance"]
    log.error(json.dumps({
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "airflow_run_id": context.get("run_id"),
        "try_number": ti.try_number,
        "error": str(context.get("exception")),
    }))


default_args = {
    "owner": "dss150p",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": log_failure,
}


def cli(task_id, command, extra=""):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} -m src.cli {command} "
                     f"--run-id {RUN_ID} {extra}".rstrip(),
    )


with DAG(
    dag_id="predictive_repro_pipeline",
    description="Spotify popularity: ingest -> validate -> transform -> load -> model -> publish",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"force_refresh": Param(False, type="boolean",
                                   description="True = full refresh of raw batches (--force)")},
    default_args=default_args,
    tags=["dss150p"],
) as dag:
    ingest_source = cli("ingest_source", "ingest-batches",
                        "{% if params.force_refresh %}--force{% endif %}")
    validate_raw = cli("validate_raw", "verify-batches")
    transform_staging = cli("transform_staging", "build-staging")
    build_curated = cli("build_curated", "build-curated")
    load_postgres = cli("load_postgres", "load-postgres")
    validate_curated = cli("validate_curated", "verify-postgres")
    train_model = cli("train_model", "train-model")
    evaluate_model = cli("evaluate_model", "evaluate-model")
    reproducibility_check = cli("reproducibility_check", "reproducibility-check")
    publish_outputs = cli("publish_outputs", "publish-outputs")

    (ingest_source >> validate_raw >> transform_staging >> build_curated
     >> load_postgres >> validate_curated >> train_model >> evaluate_model
     >> reproducibility_check >> publish_outputs)
