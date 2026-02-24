"""
Main Entry Point
=================
Runs the complete PII Detection & Data Quality Validation Pipeline
with production features: human-in-the-loop review, dead letter queue,
metrics export, versioning, cloud upload, and notifications.

Usage:
    python main.py
    python main.py --input data/customers_raw.csv
    python main.py --rollback                      # Roll back to previous version
    python main.py --no-review                     # Skip human review prompts
"""

import argparse
import logging
import sys
from pathlib import Path

from src.config import (
    RAW_CSV,
    REPORTS_DIR,
    DATA_DIR,
    QUARANTINE_DIR,
    VERSIONS_DIR,
    METRICS_DIR,
    REVIEW_DIR,
)
from src.pipeline import Pipeline, PipelineError, ThresholdBreachError


def configure_logging() -> None:
    """Configure structured logging for the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Data Quality Governance Pipeline (Production)",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(RAW_CSV),
        help=f"Path to raw CSV file (default: {RAW_CSV})",
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Roll back to the previous clean dataset version and exit",
    )
    parser.add_argument(
        "--no-review",
        action="store_true",
        help="Disable human-in-the-loop review prompts (auto-quarantine)",
    )
    parser.add_argument(
        "--list-versions",
        action="store_true",
        help="List all dataset versions and exit",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for the pipeline."""
    configure_logging()
    args = parse_args()

    logger = logging.getLogger(__name__)
    logger.info("Starting Data Quality Governance Pipeline (Production)")

    # Ensure all output directories exist
    for directory in [REPORTS_DIR, DATA_DIR, QUARANTINE_DIR, VERSIONS_DIR, METRICS_DIR, REVIEW_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

    # Handle --rollback
    if args.rollback:
        from src.rollback_manager import RollbackManager
        rm = RollbackManager()
        result = rm.rollback()
        if result is not None:
            logger.info("Rolled back successfully. Rows: %d", len(result))
        else:
            logger.error("No versions available to roll back to.")
            sys.exit(1)
        return

    # Handle --list-versions
    if args.list_versions:
        from src.rollback_manager import RollbackManager
        rm = RollbackManager()
        versions = rm.list_versions()
        if versions:
            print(f"\n{'='*60}")
            print("  DATASET VERSIONS")
            print(f"{'='*60}")
            for v in versions:
                print(f"  {v['version_id']}  |  {v.get('run_id', 'N/A')}  |  {v.get('rows', '?')} rows")
            print()
        else:
            print("No versions found.")
        return

    # Disable human review if requested
    if args.no_review:
        import os
        os.environ["HUMAN_REVIEW_ENABLED"] = "false"

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        sys.exit(1)

    try:
        pipeline = Pipeline(input_path=input_path)
        pipeline.run()
        logger.info("Pipeline complete. Reports saved to %s", REPORTS_DIR)
    except ThresholdBreachError as exc:
        logger.error("Pipeline halted by human review: %s", exc)
        sys.exit(2)
    except PipelineError as exc:
        logger.error("Pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
