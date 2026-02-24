"""
Human-in-the-Loop Review Module
================================
When validation quality drops below a configurable threshold,
this module pauses the pipeline and asks a human operator to
review flagged records and decide: continue, discard, or quarantine.

Supports both interactive (CLI) and file-based review workflows.
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import HUMAN_REVIEW_CONFIG, REVIEW_DIR, REPORTS_DIR

logger = logging.getLogger(__name__)


class ReviewDecision:
    """Represents a human review decision for a row or batch."""

    CONTINUE = "continue"
    DISCARD = "discard"
    QUARANTINE = "quarantine"

    def __init__(
        self,
        decision: str,
        reviewer: str = "system",
        reason: str = "",
        rows: list[int] | None = None,
    ) -> None:
        self.decision = decision
        self.reviewer = reviewer
        self.reason = reason
        self.rows = rows or []
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> dict:
        return {
            "decision": self.decision,
            "reviewer": self.reviewer,
            "reason": self.reason,
            "rows": self.rows,
            "timestamp": self.timestamp,
        }


class HumanReviewManager:
    """
    Manages human-in-the-loop review for flagged records.

    Workflow:
    1. Pipeline detects quality below threshold
    2. Flagged records are written to review queue
    3. Human is notified and prompted for decision
    4. Decision is recorded and pipeline acts accordingly
    """

    def __init__(self) -> None:
        self.review_queue: list[dict[str, Any]] = []
        self.decisions: list[ReviewDecision] = []
        self.config = HUMAN_REVIEW_CONFIG
        REVIEW_DIR.mkdir(parents=True, exist_ok=True)

    @property
    def is_enabled(self) -> bool:
        return self.config.get("enabled", True)

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------
    def add_to_review(
        self,
        df: pd.DataFrame,
        row_indices: list[int],
        reason: str,
        severity: str,
        column: str = "",
    ) -> None:
        """Add rows to the review queue."""
        for idx in row_indices:
            if 0 <= idx < len(df):
                row_data = df.iloc[idx].to_dict()
                # Make values JSON-serializable
                row_data = {
                    k: (None if pd.isna(v) else v.item() if hasattr(v, "item") else v)
                    for k, v in row_data.items()
                }
                self.review_queue.append({
                    "row_index": idx + 1,
                    "row_data": row_data,
                    "reason": reason,
                    "severity": severity,
                    "column": column,
                    "status": "pending",
                    "added_at": datetime.now().isoformat(),
                })

    def save_review_queue(self) -> Path:
        """Save the review queue to a JSON file for external review."""
        filepath = self.config["review_queue_file"]
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "generated_at": datetime.now().isoformat(),
                    "total_items": len(self.review_queue),
                    "items": self.review_queue,
                },
                f,
                indent=2,
                default=str,
            )
        logger.info("Review queue saved: %d items -> %s", len(self.review_queue), filepath)
        return filepath

    # ------------------------------------------------------------------
    # Interactive CLI review
    # ------------------------------------------------------------------
    def request_review_interactive(
        self,
        df: pd.DataFrame,
        failed_rows: list[int],
        pass_rate: float,
        threshold: float,
    ) -> ReviewDecision:
        """
        Prompt the operator interactively for a decision.
        Called when validation pass rate drops below threshold.
        """
        if not self.is_enabled:
            logger.info("Human review disabled — auto-applying timeout action: %s",
                        self.config["timeout_action"])
            return ReviewDecision(
                decision=self.config["timeout_action"],
                reviewer="system-auto",
                reason="Human review disabled",
                rows=failed_rows,
            )

        # Check if running in non-interactive mode
        if not sys.stdin.isatty():
            logger.warning(
                "Non-interactive environment — applying timeout action: %s",
                self.config["timeout_action"],
            )
            return ReviewDecision(
                decision=self.config["timeout_action"],
                reviewer="system-auto",
                reason="Non-interactive environment, cannot prompt user",
                rows=failed_rows,
            )

        print("\n" + "=" * 60)
        print("  HUMAN REVIEW REQUIRED")
        print("=" * 60)
        print(f"\n  Validation pass rate: {pass_rate:.1f}%")
        print(f"  Required threshold:   {threshold:.1f}%")
        print(f"  Failed rows:          {len(failed_rows)}")
        print(f"  Total rows:           {len(df)}")
        print()

        # Show sample of failed rows (max 5)
        sample_count = min(5, len(failed_rows))
        if failed_rows and sample_count > 0:
            print("  Sample of failed rows:")
            print("  " + "-" * 40)
            for row_num in failed_rows[:sample_count]:
                idx = row_num - 1
                if 0 <= idx < len(df):
                    row = df.iloc[idx]
                    print(f"    Row {row_num}: id={row.get('customer_id', '?')}, "
                          f"name={row.get('first_name', '?')} {row.get('last_name', '?')}, "
                          f"email={row.get('email', '?')}")
            if len(failed_rows) > sample_count:
                print(f"    ... and {len(failed_rows) - sample_count} more")
            print()

        print("  Options:")
        print("    [C] Continue — process all rows including failed ones")
        print("    [D] Discard  — remove failed rows and continue with valid ones")
        print("    [Q] Quarantine — move failed rows to DLQ, continue with valid ones")
        print("    [H] Halt     — stop the pipeline entirely")
        print()

        timeout = self.config.get("review_timeout_seconds", 300)
        print(f"  (Auto-{self.config['timeout_action']} in {timeout}s if no response)")
        print()

        try:
            # Simple input with a message
            choice = input("  Your decision [C/D/Q/H]: ").strip().upper()

            decision_map = {
                "C": ReviewDecision.CONTINUE,
                "D": ReviewDecision.DISCARD,
                "Q": ReviewDecision.QUARANTINE,
                "H": "halt",
            }

            decision_str = decision_map.get(choice, self.config["timeout_action"])
            reason = input("  Reason (optional): ").strip() or "Manual review decision"

            decision = ReviewDecision(
                decision=decision_str,
                reviewer="operator",
                reason=reason,
                rows=failed_rows,
            )
            self.decisions.append(decision)
            self._save_decision(decision)

            print(f"\n  Decision recorded: {decision_str.upper()}")
            print("=" * 60 + "\n")

            return decision

        except (EOFError, KeyboardInterrupt):
            logger.warning("Review interrupted — applying timeout action")
            decision = ReviewDecision(
                decision=self.config["timeout_action"],
                reviewer="system-timeout",
                reason="Review interrupted or timed out",
                rows=failed_rows,
            )
            self.decisions.append(decision)
            self._save_decision(decision)
            return decision

    # ------------------------------------------------------------------
    # File-based review (for non-interactive / Airflow environments)
    # ------------------------------------------------------------------
    def request_review_file_based(
        self,
        df: pd.DataFrame,
        failed_rows: list[int],
        pass_rate: float,
        threshold: float,
    ) -> ReviewDecision:
        """
        File-based review workflow:
        1. Write review queue to file
        2. Wait for decision file to appear
        3. Read and apply decision
        """
        # Save review queue
        self.add_to_review(
            df, [r - 1 for r in failed_rows],
            reason=f"Pass rate {pass_rate:.1f}% below threshold {threshold:.1f}%",
            severity="HIGH",
        )
        self.save_review_queue()

        decisions_file = self.config["review_decisions_file"]
        timeout = self.config.get("review_timeout_seconds", 300)

        logger.info(
            "Waiting for review decision at %s (timeout: %ds)",
            decisions_file, timeout,
        )

        start_time = time.time()
        while time.time() - start_time < timeout:
            if Path(decisions_file).exists():
                try:
                    with open(decisions_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    decision = ReviewDecision(
                        decision=data.get("decision", self.config["timeout_action"]),
                        reviewer=data.get("reviewer", "file-based"),
                        reason=data.get("reason", "File-based review decision"),
                        rows=failed_rows,
                    )
                    self.decisions.append(decision)
                    # Clean up the decision file
                    Path(decisions_file).unlink(missing_ok=True)
                    logger.info("Review decision received: %s", decision.decision)
                    return decision
                except (json.JSONDecodeError, KeyError) as exc:
                    logger.warning("Invalid decision file: %s", exc)

            time.sleep(5)  # Poll every 5 seconds

        # Timeout — apply default action
        logger.warning("Review timeout — applying: %s", self.config["timeout_action"])
        decision = ReviewDecision(
            decision=self.config["timeout_action"],
            reviewer="system-timeout",
            reason=f"No response within {timeout}s timeout",
            rows=failed_rows,
        )
        self.decisions.append(decision)
        self._save_decision(decision)
        return decision

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _save_decision(self, decision: ReviewDecision) -> None:
        """Persist a review decision for audit trail."""
        log_file = REVIEW_DIR / "review_audit_log.jsonl"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(decision.to_dict(), default=str) + "\n")

    def get_review_summary(self) -> dict:
        """Get a summary of all review decisions made."""
        return {
            "total_reviews": len(self.decisions),
            "decisions": [d.to_dict() for d in self.decisions],
            "queue_size": len(self.review_queue),
        }

    def generate_report(self, filepath=None) -> str:
        """Generate a human review report."""
        filepath = filepath or REPORTS_DIR / "human_review_report.txt"
        lines: list[str] = []

        lines.append("HUMAN-IN-THE-LOOP REVIEW REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Review Enabled: {self.is_enabled}")
        lines.append(f"Threshold: {self.config.get('review_threshold_pct', 80)}%")
        lines.append("")

        lines.append("REVIEW DECISIONS:")
        lines.append("-" * 40)
        if self.decisions:
            for d in self.decisions:
                lines.append(f"  Decision: {d.decision.upper()}")
                lines.append(f"  Reviewer: {d.reviewer}")
                lines.append(f"  Reason:   {d.reason}")
                lines.append(f"  Rows:     {len(d.rows)} affected")
                lines.append(f"  Time:     {d.timestamp}")
                lines.append("")
        else:
            lines.append("  No review decisions were needed.")
        lines.append("")

        lines.append("REVIEW QUEUE:")
        lines.append("-" * 40)
        lines.append(f"  Items in queue: {len(self.review_queue)}")
        lines.append("")

        report_text = "\n".join(lines)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        return report_text
