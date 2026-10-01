"""Command-line entry point for the DSS150P pipeline.

Run from the project root, for example:
    python -m src.cli ingest-full
    python -m src.cli ingest-batches
    python -m src.cli verify-batches

Exit codes: 0 = success, 1 = unrecoverable error, 2 = simulated interruption.
"""
import argparse
import sys

from src.config import load_settings, log_event, new_run_id
from src.ingest import (IngestionError, SimulatedInterruption, full_ingest,
                        incremental_ingest, verify_batches)


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

    verify = commands.add_parser(
        "verify-batches", help="count rows and duplicate keys in raw batches")
    verify.add_argument("--run-id", help="reuse a run id (default: generate one)")
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
    except SimulatedInterruption as error:
        log_event(run_id, "cli", "run finished", status="interrupted",
                  exit_code=2, error=str(error))
        return 2
    except IngestionError as error:
        log_event(run_id, "cli", "run finished", status="failed",
                  exit_code=1, error=str(error))
        return 1
    log_event(run_id, "cli", "run finished", status="success", exit_code=0)
    return 0


if __name__ == "__main__":
    sys.exit(main())