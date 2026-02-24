"""
Dead Letter Queue (DLQ) Module
==============================
Manages records that fail validation and cannot be processed.
Quarantined records are stored separately for investigation,
retry, or manual review.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import QUARANTINE_DIR, REPORTS_DIR

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """Manages quarantined records that failed pipeline processing."""

    def __init__(self) -> None:
        self.quarantined_records: list[dict[str, Any]] = []
        self.quarantine_summary: dict[str, int] = {}
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    def add_record(
        self,
        row_index: int,
        row_data: dict,
        reason: str,
        severity: str,
        stage: str,
    ) -> None:
        """Add a record to the dead letter queue."""
        entry = {
            "row_index": row_index,
            "row_data": {k: self._serialize_value(v) for k, v in row_data.items()},
            "reason": reason,
            "severity": severity,
            "stage": stage,
            "timestamp": datetime.now().isoformat(),
            "status": "quarantined",
            "retry_count": 0,
        }
        self.quarantined_records.append(entry)
        self.quarantine_summary[severity] = self.quarantine_summary.get(severity, 0) + 1
        logger.debug("Quarantined row %d: %s [%s]", row_index, reason, severity)

    def add_dataframe_rows(
        self,
        df: pd.DataFrame,
        row_indices: list[int],
        reason: str,
        severity: str,
        stage: str,
    ) -> None:
        """Add multiple DataFrame rows to the dead letter queue."""
        for idx in row_indices:
            if 0 <= idx < len(df):
                row_data = df.iloc[idx].to_dict()
                self.add_record(idx + 1, row_data, reason, severity, stage)

    @staticmethod
    def _serialize_value(val: Any) -> Any:
        """Make a value JSON-serializable."""
        if pd.isna(val):
            return None
        if hasattr(val, "item"):  # numpy types
            return val.item()
        return val

    def save_quarantine(self, run_id: str = "") -> Path:
        """Save quarantined records to a JSON file."""
        if not run_id:
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        filepath = QUARANTINE_DIR / f"quarantine_{run_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "run_id": run_id,
                    "timestamp": datetime.now().isoformat(),
                    "total_quarantined": len(self.quarantined_records),
                    "summary": self.quarantine_summary,
                    "records": self.quarantined_records,
                },
                f,
                indent=2,
                default=str,
            )
        logger.info("Quarantine saved: %d records -> %s", len(self.quarantined_records), filepath)
        return filepath

    def save_quarantine_csv(self, run_id: str = "") -> Path:
        """Save quarantined records as CSV for easy analysis."""
        if not run_id:
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        filepath = QUARANTINE_DIR / f"quarantine_{run_id}.csv"

        if self.quarantined_records:
            rows = []
            for rec in self.quarantined_records:
                row = {**rec["row_data"]}
                row["_dlq_reason"] = rec["reason"]
                row["_dlq_severity"] = rec["severity"]
                row["_dlq_stage"] = rec["stage"]
                row["_dlq_timestamp"] = rec["timestamp"]
                rows.append(row)
            pd.DataFrame(rows).to_csv(filepath, index=False)
            logger.info("Quarantine CSV saved: %s", filepath)
        return filepath

    def get_records_by_severity(self, severity: str) -> list[dict]:
        """Filter quarantined records by severity level."""
        return [r for r in self.quarantined_records if r["severity"] == severity]

    def get_retryable_records(self, max_retries: int = 3) -> list[dict]:
        """Get records eligible for retry."""
        return [
            r for r in self.quarantined_records
            if r["retry_count"] < max_retries and r["status"] == "quarantined"
        ]

    @property
    def count(self) -> int:
        return len(self.quarantined_records)

    def generate_report(self, filepath=None) -> str:
        """Generate a dead letter queue report."""
        filepath = filepath or REPORTS_DIR / "dead_letter_queue_report.txt"
        lines: list[str] = []

        lines.append("DEAD LETTER QUEUE REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Total Quarantined Records: {len(self.quarantined_records)}")
        lines.append("")

        lines.append("QUARANTINE SUMMARY BY SEVERITY:")
        lines.append("-" * 40)
        for sev, count in sorted(self.quarantine_summary.items()):
            lines.append(f"  {sev}: {count} record(s)")
        lines.append("")

        lines.append("QUARANTINED RECORDS (first 20):")
        lines.append("-" * 40)
        for rec in self.quarantined_records[:20]:
            lines.append(f"  Row {rec['row_index']}: {rec['reason']} [{rec['severity']}]")
            lines.append(f"    Stage: {rec['stage']} | Time: {rec['timestamp']}")
        lines.append("")

        if len(self.quarantined_records) > 20:
            lines.append(f"  ... and {len(self.quarantined_records) - 20} more records")
            lines.append("")

        report_text = "\n".join(lines)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("DLQ report saved to %s", filepath)
        return report_text
