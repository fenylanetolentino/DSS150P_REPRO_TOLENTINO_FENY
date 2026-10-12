"""Command-line entry point for the DSS150P pipeline.

Run from the project root, for example:
    python -m src.cli ingest-full
    python -m src.cli ingest-batches
    python -m src.cli verify-batches
    python -m src.cli build-staging
    python -m src.cli build-curated
    python -m src.cli load-postgres
    python -m src.cli verify-postgres

Exit codes: 0 = success, 1 = unrecoverable error, 2 = simulated interruption.
"""
import argparse
import sys

from src.config import load_settings, log_event, new_run_id
from src.ingest import (IngestionError, SimulatedInterruption, full_ingest,
                        incremental_ingest, verify_batches)
from src.storage import StorageError, load_curated, verify_load
from src.transform import ContractViolation, build_curated, build_staging


def build_parser():
    parser = argparse.ArgumentParser(description="DSS150P reproducible pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    full = commands.add_parser("ingest-full", help="copy the whole source to raw")
    full.add_argument("--run-id", help="reuse a run id (default: generate one)")

    batches = commands.add_parser(
        "ingest-batches", help="ingest deterministic batches with a checkpoint")
    batches.add_argument("--run-id", help="reuse a run id (default: generate one)")
    batches.add_argument("--force", action="store_true",
                         help="full refresh: reset the checkpoint and batches")
    batches.add_argument("--stop-after", type=int, metavar="N",
                         help="simulate a crash after batch N is committed")

    simple = {
        "verify-batches": "count rows and duplicate keys in raw batches",
        "build-staging": "raw batches -> typed, validated, deduplicated staging",
        "build-curated": "staging -> curated, model-ready columns",
        "load-postgres": "load the curated layer into PostgreSQL (rerun-safe)",
        "verify-postgres": "check PostgreSQL row count and duplicate keys",
    }
    for name, help_text in simple.items():
        sub = commands.add_parser(name, help=help_text)
        sub.add_argument("--run-id", help="reuse a run id (default: generate one)")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    settings = load_settings()
    run_id = args.run_id or new_run_id()
    log_event(run_id, "cli", "run started", command=args.command)
    try:
        if args.command == "ingest-full":
            full_ingest(settings, run_id)
        elif args.command == "ingest-batches":
            incremental_ingest(settings, run_id, force=args.force,
                               stop_after=args.stop_after)
        elif args.command == "verify-batches":
            verify_batches(settings, run_id)
        elif args.command == "build-staging":
            build_staging(settings, run_id)
        elif args.command == "build-curated":
            build_curated(settings, run_id)
        elif args.command == "load-postgres":
            load_curated(settings, run_id)
        elif args.command == "verify-postgres":
            verify_load(settings, run_id)
    except SimulatedInterruption as error:
        log_event(run_id, "cli", "run finished", status="interrupted",
                  exit_code=2, error=str(error))
        return 2
    except (IngestionError, ContractViolation, StorageError) as error:
        log_event(run_id, "cli", "run finished", status="failed",
                  exit_code=1, error=str(error))
        return 1
    log_event(run_id, "cli", "run finished", status="success", exit_code=0)
    return 0


if __name__ == "__main__":
    sys.exit(main())