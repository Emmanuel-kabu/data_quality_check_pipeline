"""
Main Entry Point
=================
Runs the complete PII Detection & Data Quality Validation Pipeline.

Usage:
    python main.py
    python main.py --input data/customers_raw.csv
"""

import argparse
import logging
import sys
from pathlib import Path

from src.config import RAW_CSV, REPORTS_DIR, DATA_DIR
from src.pipeline import Pipeline


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
        description="PII Detection & Data Quality Validation Pipeline",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(RAW_CSV),
        help=f"Path to raw CSV file (default: {RAW_CSV})",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for the pipeline."""
    configure_logging()
    args = parse_args()

    logger = logging.getLogger(__name__)
    logger.info("Starting Data Quality Governance Pipeline")

    # Ensure output directories exist
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        sys.exit(1)

    pipeline = Pipeline(input_path=input_path)
    pipeline.run()

    logger.info("Pipeline complete. Reports saved to %s", REPORTS_DIR)


if __name__ == "__main__":
    main()
