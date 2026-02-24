"""
Pipeline Orchestrator (Production-Ready)
=========================================
Coordinates all stages of the data quality governance pipeline:
  1.  LOAD         — Read raw CSV (local or cloud)
  2.  IDEMPOTENCY  — Skip if input unchanged
  3.  PROFILE      — Data quality analysis
  4.  CONTRACT     — Data contract & SLA validation
  5.  VALIDATE     — Schema validation (pre-clean)
  6.  STATISTICAL  — Outlier & distribution analysis
  7.  THRESHOLD    — Check failure rate, trigger human review if needed
  8.  CLEAN        — Normalize formats & handle missing values
  9.  QUARANTINE   — Move failed records to dead letter queue
  10. GE VALIDATE  — Great Expectations declarative validation
  11. DETECT PII   — Identify sensitive fields
  12. MASK PII     — Protect PII
  13. VERSION      — Create versioned snapshot with rollback support
  14. METRICS      — Collect & export quality metrics (Prometheus/Grafana)
  15. UPLOAD       — Upload outputs to cloud storage
  16. NOTIFY       — Send alerts via Slack/Email/PagerDuty
  17. REPORT       — Generate final pipeline execution report

Supports:
  - Soft/hard failure classification with threshold-based halting
  - Human-in-the-loop review for borderline quality
  - Dead letter queue for unprocessable records
  - Retry with exponential backoff for transient failures
  - Rollback to previous clean dataset version
  - Idempotent execution (skip if input unchanged)
"""

import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import (
    RAW_CSV,
    CLEANED_CSV,
    MASKED_CSV,
    PIPELINE_REPORT,
    REPORTS_DIR,
    DATA_DIR,
    FAILURE_THRESHOLDS,
    FAILURE_ACTIONS,
    HUMAN_REVIEW_CONFIG,
)
from src.profiler import DataProfiler
from src.pii_detector import PIIDetector
from src.validator import DataValidator
from src.cleaner import DataCleaner
from src.masker import PIIMasker
from src.statistical_validator import StatisticalValidator
from src.dead_letter_queue import DeadLetterQueue
from src.human_review import HumanReviewManager, ReviewDecision
from src.rollback_manager import RollbackManager
from src.cloud_storage import CloudStorageClient
from src.data_contract import DataContractValidator
from src.expectations import GreatExpectationsValidator
from src.metrics_collector import MetricsCollector
from src.notifier import Notifier

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Raised when the pipeline encounters a hard failure."""
    pass


class ThresholdBreachError(PipelineError):
    """Raised when validation failure rate exceeds the configured threshold."""
    pass


class Pipeline:
    """Production-ready end-to-end data quality governance pipeline."""

    def __init__(self, input_path: Path = RAW_CSV, force: bool = False) -> None:
        self.input_path = Path(input_path)
        self.force = force
        self.df_raw: pd.DataFrame | None = None
        self.df_cleaned: pd.DataFrame | None = None
        self.df_masked: pd.DataFrame | None = None
        self.stages: list[dict] = []
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.run_id: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Production components
        self.dlq = DeadLetterQueue()
        self.review_manager = HumanReviewManager()
        self.rollback_manager = RollbackManager()
        self.cloud_storage = CloudStorageClient()
        self.metrics = MetricsCollector(run_id=self.run_id)
        self.notifier = Notifier()

    # ------------------------------------------------------------------
    # Stage helpers
    # ------------------------------------------------------------------
    def _log_stage(self, stage: str, status: str, details: list[str]) -> None:
        """Record a pipeline stage result."""
        entry = {
            "stage": stage,
            "status": status,
            "details": details,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.stages.append(entry)
        symbol = "[OK]" if status == "SUCCESS" else "[!!]" if status == "WARNING" else "[--]" if status == "SKIPPED" else "[XX]"
        logger.info("%s Stage: %s - %s", symbol, stage, status)
        for d in details:
            logger.info("  %s", d)

    def _timed_stage(self, stage_name: str):
        """Context-manager-like helper to time stages."""
        class _Timer:
            def __init__(self):
                self.start = time.time()
            @property
            def elapsed_ms(self):
                return (time.time() - self.start) * 1000
        return _Timer()

    # ------------------------------------------------------------------
    # Stage 1: LOAD
    # ------------------------------------------------------------------
    def load(self) -> pd.DataFrame:
        """Load the raw CSV into a DataFrame."""
        logger.info("=" * 60)
        logger.info("STAGE 1: LOAD")
        logger.info("=" * 60)
        timer = self._timed_stage("LOAD")
        try:
            # Try cloud download first
            if self.cloud_storage.enabled:
                raw_prefix = self.cloud_storage.config.get("raw_prefix", "raw/")
                try:
                    self.cloud_storage.download_file(
                        f"{raw_prefix}{self.input_path.name}",
                        self.input_path,
                    )
                except Exception as exc:
                    logger.warning("Cloud download failed, using local file: %s", exc)

            self.df_raw = pd.read_csv(self.input_path)
            rows, cols = self.df_raw.shape
            self._log_stage("LOAD", "SUCCESS", [
                f"Loaded {self.input_path.name}",
                f"{rows} rows, {cols} columns",
            ])
            self.metrics.record_stage("load", "SUCCESS", timer.elapsed_ms, {"rows": rows, "cols": cols})
            return self.df_raw
        except Exception as exc:
            self._log_stage("LOAD", "FAILURE", [str(exc)])
            self.metrics.record_stage("load", "FAILURE", timer.elapsed_ms)
            raise PipelineError(f"Failed to load data: {exc}") from exc

    # ------------------------------------------------------------------
    # Stage 2: IDEMPOTENCY CHECK
    # ------------------------------------------------------------------
    def check_idempotency(self) -> bool:
        """Check if input has changed since last run. Returns True if pipeline should run."""
        logger.info("=" * 60)
        logger.info("STAGE 2: IDEMPOTENCY CHECK")
        logger.info("=" * 60)
        needs_run = self.rollback_manager.check_idempotency(self.input_path)
        if not needs_run:
            self._log_stage("IDEMPOTENCY", "SKIPPED", [
                "Input file unchanged since last run",
                "Pipeline execution skipped (idempotent)",
            ])
        else:
            self._log_stage("IDEMPOTENCY", "SUCCESS", ["Input changed — proceeding with pipeline"])
        return needs_run

    # ------------------------------------------------------------------
    # Stage 3: PROFILE
    # ------------------------------------------------------------------
    def profile(self) -> str:
        """Run data quality profiling and generate report."""
        logger.info("=" * 60)
        logger.info("STAGE 3: PROFILE")
        logger.info("=" * 60)
        timer = self._timed_stage("PROFILE")
        profiler = DataProfiler(self.df_raw)
        report = profiler.generate_report()
        results = profiler.run_full_profile()

        details = []
        for col, info in results["completeness"].items():
            if info["missing"] > 0:
                details.append(f"{col}: {info['missing']} missing ({info['percentage']}% complete)")

        issue_count = len(results["issues"])
        details.append(f"Total quality issues found: {issue_count}")
        self._log_stage("PROFILE", "SUCCESS", details)
        self.metrics.record_stage("profile", "SUCCESS", timer.elapsed_ms, {"issues": issue_count})
        return report

    # ------------------------------------------------------------------
    # Stage 4: DATA CONTRACT VALIDATION
    # ------------------------------------------------------------------
    def validate_contract(self) -> dict:
        """Validate data against the formal data contract and SLAs."""
        logger.info("=" * 60)
        logger.info("STAGE 4: DATA CONTRACT VALIDATION")
        logger.info("=" * 60)
        timer = self._timed_stage("CONTRACT")
        contract_validator = DataContractValidator(self.df_raw)
        contract_validator.generate_report()
        results = contract_validator.validate_all()

        details = [
            f"Contract version: {results['contract_version']}",
            f"Overall: {'PASS' if results['all_passed'] else 'FAIL'}",
            f"Violations: {results['total_violations']}",
        ]
        for sla_name, sla_result in results["sla_results"].items():
            status_txt = "PASS" if sla_result.get("passed") else "FAIL"
            details.append(f"  SLA {sla_name}: {status_txt}")

        status = "SUCCESS" if results["all_passed"] else "WARNING"
        self._log_stage("CONTRACT", status, details)
        self.metrics.record_stage("contract", status, timer.elapsed_ms, {
            "violations": results["total_violations"],
        })

        # Critical contract violations halt the pipeline
        critical = [v for v in results["violations"] if v["severity"] == "CRITICAL"]
        if critical:
            action = FAILURE_ACTIONS.get("CRITICAL", "halt")
            if action == "halt":
                self.notifier.notify(
                    f"Data contract has {len(critical)} CRITICAL violations. Pipeline halted.",
                    severity="CRITICAL",
                )
                raise PipelineError(f"Data contract CRITICAL violations: {critical}")

        return results

    # ------------------------------------------------------------------
    # Stage 5: VALIDATE (pre-clean)
    # ------------------------------------------------------------------
    def validate_raw(self) -> dict:
        """Validate the raw data and return results."""
        logger.info("=" * 60)
        logger.info("STAGE 5: VALIDATE (pre-clean)")
        logger.info("=" * 60)
        timer = self._timed_stage("VALIDATE")
        validator = DataValidator(self.df_raw)
        validator.generate_report()
        results = validator.run_all_validations()

        self._log_stage("VALIDATE (pre-clean)", "SUCCESS", [
            f"Passed: {results['passed']} rows",
            f"Failed: {results['failed']} rows",
            f"Total failures: {len(results['failures'])}",
        ])
        self.metrics.record_stage("validate", "SUCCESS", timer.elapsed_ms, {
            "passed": results["passed"],
            "failed": results["failed"],
        })
        return results

    # ------------------------------------------------------------------
    # Stage 6: STATISTICAL VALIDATION
    # ------------------------------------------------------------------
    def validate_statistical(self) -> dict:
        """Run statistical validation — outliers and distribution checks."""
        logger.info("=" * 60)
        logger.info("STAGE 6: STATISTICAL VALIDATION")
        logger.info("=" * 60)
        timer = self._timed_stage("STATISTICAL")
        stat_validator = StatisticalValidator(self.df_raw)
        results = stat_validator.run_all()
        stat_validator.generate_report()

        details = [
            f"Outliers detected: {results['total_outliers']}",
            f"Distribution issues: {results['total_distribution_issues']}",
        ]
        for col, stats in results.get("stats_summary", {}).items():
            details.append(
                f"  {col}: mean={stats['mean']:,.0f}, "
                f"median={stats['median']:,.0f}, "
                f"outliers={stats['outlier_count']}"
            )

        self._log_stage("STATISTICAL", "SUCCESS", details)
        self.metrics.record_stage("statistical", "SUCCESS", timer.elapsed_ms, {
            "outliers": results["total_outliers"],
        })
        return results

    # ------------------------------------------------------------------
    # Stage 7: THRESHOLD CHECK & HUMAN REVIEW
    # ------------------------------------------------------------------
    def check_threshold_and_review(self, validation_results: dict) -> str:
        """
        Check failure rate against thresholds.
        If too high, trigger human-in-the-loop review.
        Returns: "continue" | "discard" | "quarantine" | "halt"
        """
        logger.info("=" * 60)
        logger.info("STAGE 7: THRESHOLD CHECK & HUMAN REVIEW")
        logger.info("=" * 60)

        total = validation_results["total_rows"]
        failed = validation_results["failed"]
        failure_pct = (failed / total * 100) if total > 0 else 0
        pass_rate = 100 - failure_pct

        hard_threshold = FAILURE_THRESHOLDS["hard_failure_pct"]
        review_threshold = HUMAN_REVIEW_CONFIG.get("review_threshold_pct", 80.0)

        details = [
            f"Failure rate: {failure_pct:.1f}% ({failed}/{total} rows)",
            f"Hard threshold: {hard_threshold}%",
            f"Review threshold (pass rate): {review_threshold}%",
        ]

        # Case 1: Below hard failure threshold — continue normally
        if failure_pct <= hard_threshold:
            details.append("Status: BELOW threshold — continuing normally")
            self._log_stage("THRESHOLD", "SUCCESS", details)
            return ReviewDecision.CONTINUE

        # Case 2: Pass rate below review threshold — human review
        if pass_rate < review_threshold:
            details.append(f"Status: Pass rate {pass_rate:.1f}% below review threshold {review_threshold}%")
            self._log_stage("THRESHOLD", "WARNING", details)

            self.notifier.notify_threshold_breach(failure_pct, hard_threshold)

            if self.review_manager.is_enabled:
                self.notifier.notify_human_review_needed(
                    f"Pass rate {pass_rate:.1f}% is below {review_threshold}%. "
                    f"{failed} out of {total} rows failed validation."
                )

                decision = self.review_manager.request_review_interactive(
                    self.df_raw,
                    validation_results["failed_rows"],
                    pass_rate,
                    review_threshold,
                )

                self._log_stage("HUMAN REVIEW", "COMPLETED", [
                    f"Decision: {decision.decision.upper()}",
                    f"Reviewer: {decision.reviewer}",
                    f"Reason: {decision.reason}",
                ])

                if decision.decision == "halt":
                    raise ThresholdBreachError(
                        f"Pipeline halted by reviewer. "
                        f"Failure rate: {failure_pct:.1f}%"
                    )

                return decision.decision

        # Case 3: Above threshold but review disabled — quarantine
        details.append("Status: ABOVE threshold — quarantining failed records")
        self._log_stage("THRESHOLD", "WARNING", details)
        return ReviewDecision.QUARANTINE

    # ------------------------------------------------------------------
    # Stage 8: CLEAN
    # ------------------------------------------------------------------
    def clean(self, validation_before: int = 0, decision: str = "continue") -> pd.DataFrame:
        """Clean the data. If decision is 'discard', remove failed rows first."""
        logger.info("=" * 60)
        logger.info("STAGE 8: CLEAN")
        logger.info("=" * 60)
        timer = self._timed_stage("CLEAN")

        working_df = self.df_raw.copy()

        # If human decided to discard failed rows
        if decision == ReviewDecision.DISCARD:
            validator = DataValidator(working_df)
            results = validator.run_all_validations()
            failed_indices = [r - 1 for r in results["failed_rows"]]
            discarded = len(failed_indices)
            working_df = working_df.drop(index=failed_indices).reset_index(drop=True)
            logger.info("Discarded %d failed rows per review decision", discarded)

        cleaner = DataCleaner(working_df)
        self.df_cleaned = cleaner.run_full_cleaning()

        # Validate after cleaning
        post_validator = DataValidator(self.df_cleaned)
        post_results = post_validator.run_all_validations()
        validation_after = post_results["failed"]

        cleaner.save_cleaned_data()
        cleaner.generate_report(
            validation_before=validation_before,
            validation_after=validation_after,
        )

        details = []
        for action in cleaner.actions:
            details.append(action["action"])
        details.append(f"Validation before: {validation_before} rows failed")
        details.append(f"Validation after:  {validation_after} rows failed")

        self._log_stage("CLEAN", "SUCCESS", details)
        self.metrics.record_stage("clean", "SUCCESS", timer.elapsed_ms, {
            "before_failures": validation_before,
            "after_failures": validation_after,
        })
        return self.df_cleaned

    # ------------------------------------------------------------------
    # Stage 9: QUARANTINE
    # ------------------------------------------------------------------
    def quarantine_failed_records(self, decision: str = "continue") -> int:
        """Move remaining failed records to the dead letter queue."""
        logger.info("=" * 60)
        logger.info("STAGE 9: QUARANTINE (Dead Letter Queue)")
        logger.info("=" * 60)
        timer = self._timed_stage("QUARANTINE")

        if decision == ReviewDecision.DISCARD:
            self._log_stage("QUARANTINE", "SKIPPED", [
                "Failed rows already discarded by reviewer"
            ])
            return 0

        # Validate cleaned data to find remaining failures
        validator = DataValidator(self.df_cleaned)
        results = validator.run_all_validations()

        if results["failed_rows"]:
            # Quarantine remaining failures
            for failure in results["failures"]:
                severity = self._classify_failure_severity(failure)
                action = FAILURE_ACTIONS.get(severity, "log_continue")

                if action in ("quarantine", "halt"):
                    row_idx = failure.row - 1
                    self.dlq.add_record(
                        row_index=failure.row,
                        row_data=self.df_cleaned.iloc[row_idx].to_dict() if row_idx < len(self.df_cleaned) else {},
                        reason=failure.message,
                        severity=severity,
                        stage="post_clean_validation",
                    )

            if self.dlq.count > 0:
                self.dlq.save_quarantine(self.run_id)
                self.dlq.save_quarantine_csv(self.run_id)
                self.dlq.generate_report()

        details = [
            f"Quarantined records: {self.dlq.count}",
            f"Still in dataset: {len(self.df_cleaned)} rows",
        ]
        for sev, count in self.dlq.quarantine_summary.items():
            details.append(f"  {sev}: {count}")

        self._log_stage("QUARANTINE", "SUCCESS", details)
        self.metrics.record_stage("quarantine", "SUCCESS", timer.elapsed_ms, {
            "quarantined": self.dlq.count,
        })
        return self.dlq.count

    def _classify_failure_severity(self, failure) -> str:
        """Map a validation failure to a severity level."""
        critical_rules = {"not_null", "unique", "integer"}
        high_rules = {"email_format", "phone_format", "valid_date", "allowed_value"}
        medium_rules = {"date_format", "min_length", "max_length", "non_negative"}

        if failure.rule in critical_rules and failure.column in ("customer_id",):
            return "CRITICAL"
        if failure.rule in high_rules:
            return "HIGH"
        if failure.rule in medium_rules:
            return "MEDIUM"
        return "LOW"

    # ------------------------------------------------------------------
    # Stage 10: GREAT EXPECTATIONS VALIDATION
    # ------------------------------------------------------------------
    def validate_expectations(self) -> dict:
        """Run Great Expectations declarative validation on cleaned data."""
        logger.info("=" * 60)
        logger.info("STAGE 10: GREAT EXPECTATIONS VALIDATION")
        logger.info("=" * 60)
        timer = self._timed_stage("GE_VALIDATE")
        ge_validator = GreatExpectationsValidator(self.df_cleaned)
        results = ge_validator.run_validation()
        ge_validator.generate_report()

        details = [
            f"Engine: {results.get('engine', 'unknown')}",
            f"Expectations: {results['total']}",
            f"Passed: {results['passed']}",
            f"Failed: {results['failed']}",
            f"Pass rate: {results['pass_rate_pct']:.1f}%",
        ]
        status = "SUCCESS" if results["failed"] == 0 else "WARNING"
        self._log_stage("GE VALIDATE", status, details)
        self.metrics.record_stage("ge_validate", status, timer.elapsed_ms, {
            "pass_rate": results["pass_rate_pct"],
        })
        return results

    # ------------------------------------------------------------------
    # Stage 11: DETECT PII
    # ------------------------------------------------------------------
    def detect_pii(self) -> str:
        """Run PII detection on the cleaned data."""
        logger.info("=" * 60)
        logger.info("STAGE 11: DETECT PII")
        logger.info("=" * 60)
        timer = self._timed_stage("DETECT_PII")
        detector = PIIDetector(self.df_cleaned)
        report = detector.generate_report()
        findings = detector.pii_findings

        details = []
        total_pii = 0
        for key, finding in findings.items():
            count = finding.get("count", 0)
            total_pii += count
            details.append(f"{key}: {count} found")

        self._log_stage("DETECT PII", "SUCCESS", details)
        self.metrics.record_stage("detect_pii", "SUCCESS", timer.elapsed_ms, {"total_pii": total_pii})
        self._total_pii = total_pii
        return report

    # ------------------------------------------------------------------
    # Stage 12: MASK PII
    # ------------------------------------------------------------------
    def mask_pii(self) -> pd.DataFrame:
        """Mask PII in the cleaned data."""
        logger.info("=" * 60)
        logger.info("STAGE 12: MASK PII")
        logger.info("=" * 60)
        timer = self._timed_stage("MASK_PII")
        masker = PIIMasker(self.df_cleaned)
        self.df_masked = masker.mask_all_pii()
        masker.save_masked_data()
        masker.generate_report()

        total_masked = sum(masker.mask_stats.values())
        details = [
            f"Names masked: {masker.mask_stats['names']}",
            f"Emails masked: {masker.mask_stats['emails']}",
            f"Phones masked: {masker.mask_stats['phones']}",
            f"Addresses masked: {masker.mask_stats['addresses']}",
            f"DOBs masked: {masker.mask_stats['dobs']}",
        ]
        self._log_stage("MASK PII", "SUCCESS", details)
        self.metrics.record_stage("mask_pii", "SUCCESS", timer.elapsed_ms, {"masked": total_masked})

        # Record PII metrics
        self.metrics.record_pii_metrics(
            total_pii_fields=getattr(self, "_total_pii", total_masked),
            masked_fields=total_masked,
            pii_categories=masker.mask_stats,
        )
        return self.df_masked

    # ------------------------------------------------------------------
    # Stage 13: VERSION
    # ------------------------------------------------------------------
    def create_version(self) -> str:
        """Create a versioned snapshot of the cleaned dataset."""
        logger.info("=" * 60)
        logger.info("STAGE 13: VERSION SNAPSHOT")
        logger.info("=" * 60)
        timer = self._timed_stage("VERSION")

        version_id = self.rollback_manager.create_version(
            self.df_cleaned,
            run_id=self.run_id,
            metadata={
                "input_path": str(self.input_path),
                "input_checksum": self.rollback_manager._calculate_checksum(self.input_path),
                "quarantined_records": self.dlq.count,
            },
        )

        # Cleanup old versions
        removed = self.rollback_manager.cleanup_old_versions(keep=5)

        details = [
            f"Version: {version_id}",
            f"Rows: {len(self.df_cleaned)}",
            f"Old versions cleaned: {removed}",
        ]
        self._log_stage("VERSION", "SUCCESS", details)
        self.metrics.record_stage("version", "SUCCESS", timer.elapsed_ms)
        return version_id

    # ------------------------------------------------------------------
    # Stage 14: METRICS EXPORT
    # ------------------------------------------------------------------
    def export_metrics(self, validation_results: dict, stat_results: dict) -> None:
        """Collect and export all pipeline metrics."""
        logger.info("=" * 60)
        logger.info("STAGE 14: METRICS EXPORT")
        logger.info("=" * 60)

        self.metrics.record_quality_metrics(
            total_rows=validation_results["total_rows"],
            passed_rows=validation_results["passed"],
            failed_rows=validation_results["failed"],
            quarantined_rows=self.dlq.count,
            outlier_count=stat_results.get("total_outliers", 0),
        )

        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        rows = validation_results["total_rows"]
        self.metrics.record_performance(
            total_duration_seconds=duration,
            rows_per_second=(rows / duration) if duration > 0 else 0,
        )

        self.metrics.save_metrics()
        self.metrics.export_prometheus_metrics()

        # Push metrics to Pushgateway (for Prometheus/Grafana in Docker)
        pushed = self.metrics.push_to_pushgateway()

        # Check for quality degradation trends
        degradation_alerts = self.metrics.detect_quality_degradation()
        for alert in degradation_alerts:
            self.notifier.notify(alert["message"], severity=alert["severity"])

        self._log_stage("METRICS", "SUCCESS", [
            "Metrics saved and exported to Prometheus format",
            f"Pushgateway: {'pushed' if pushed else 'skipped'}",
            f"Degradation alerts: {len(degradation_alerts)}",
        ])

    # ------------------------------------------------------------------
    # Stage 15: CLOUD UPLOAD
    # ------------------------------------------------------------------
    def upload_to_cloud(self) -> None:
        """Upload pipeline outputs to cloud storage."""
        logger.info("=" * 60)
        logger.info("STAGE 15: CLOUD UPLOAD")
        logger.info("=" * 60)

        if not self.cloud_storage.enabled:
            self._log_stage("CLOUD UPLOAD", "SKIPPED", ["Cloud storage disabled"])
            return

        timer = self._timed_stage("CLOUD_UPLOAD")
        results = self.cloud_storage.upload_pipeline_outputs(
            CLEANED_CSV, MASKED_CSV, REPORTS_DIR,
        )
        self._log_stage("CLOUD UPLOAD", "SUCCESS", [
            f"Uploaded: {results}",
        ])
        self.metrics.record_stage("cloud_upload", "SUCCESS", timer.elapsed_ms)

    # ------------------------------------------------------------------
    # Stage 16: NOTIFY
    # ------------------------------------------------------------------
    def send_notifications(self, status: str = "SUCCESS") -> None:
        """Send pipeline completion/failure notifications."""
        logger.info("=" * 60)
        logger.info("STAGE 16: NOTIFICATIONS")
        logger.info("=" * 60)

        if status == "SUCCESS":
            summary = self._build_success_summary()
            self.notifier.notify_pipeline_success(summary)
        else:
            self.notifier.notify_pipeline_failure(status)

        self._log_stage("NOTIFY", "SUCCESS", [
            f"Notifications dispatched ({status})",
        ])

    def _build_success_summary(self) -> str:
        rows = len(self.df_raw) if self.df_raw is not None else 0
        cleaned = len(self.df_cleaned) if self.df_cleaned is not None else 0
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else 0
        return (
            f"Pipeline completed successfully!\n"
            f"  Run ID: {self.run_id}\n"
            f"  Input: {rows} rows\n"
            f"  Cleaned: {cleaned} rows\n"
            f"  Quarantined: {self.dlq.count} records\n"
            f"  Duration: {duration:.2f}s"
        )

    # ------------------------------------------------------------------
    # Stage 17: REPORT
    # ------------------------------------------------------------------
    def generate_pipeline_report(self) -> str:
        """Generate the final pipeline execution report."""
        logger.info("=" * 60)
        logger.info("STAGE 17: GENERATE REPORT")
        logger.info("=" * 60)
        lines: list[str] = []

        lines.append("PIPELINE EXECUTION REPORT (PRODUCTION)")
        lines.append("=" * 60)
        lines.append(f"Run ID:      {self.run_id}")
        lines.append(f"Start Time:  {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"End Time:    {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        duration = (self.end_time - self.start_time).total_seconds()
        lines.append(f"Duration:    {duration:.2f} seconds")
        lines.append("")

        for entry in self.stages:
            if entry["status"] == "SUCCESS":
                symbol = "[OK]"
            elif entry["status"] == "WARNING":
                symbol = "[!!]"
            elif entry["status"] == "SKIPPED":
                symbol = "[--]"
            else:
                symbol = "[XX]"
            lines.append(f"Stage: {entry['stage']}")
            lines.append(f"  Status: {symbol} {entry['status']}")
            for detail in entry["details"]:
                lines.append(f"    {detail}")
            lines.append("")

        # Summary
        lines.append("SUMMARY:")
        lines.append("-" * 40)
        if self.df_raw is not None:
            lines.append(f"  Input:        {self.df_raw.shape[0]} rows (raw)")
        if self.df_cleaned is not None:
            lines.append(f"  Cleaned:      {self.df_cleaned.shape[0]} rows (validated)")
        if self.df_masked is not None:
            lines.append(f"  Masked:       {self.df_masked.shape[0]} rows (PII protected)")
        lines.append(f"  Quarantined:  {self.dlq.count} rows (in DLQ)")
        lines.append(f"  Version:      {self.rollback_manager.get_current_version() or 'N/A'}")
        lines.append("")

        # Production features status
        lines.append("PRODUCTION FEATURES:")
        lines.append("-" * 40)
        lines.append(f"  Human Review:      {'Enabled' if self.review_manager.is_enabled else 'Disabled'}")
        lines.append(f"  Cloud Storage:     {'Enabled' if self.cloud_storage.enabled else 'Disabled'}")
        lines.append(f"  Notifications:     Active")
        lines.append(f"  Dead Letter Queue: {self.dlq.count} records")
        lines.append(f"  Versioning:        {len(self.rollback_manager.list_versions())} versions")
        lines.append(f"  Metrics Export:    Prometheus + JSON")
        lines.append("")

        # Review decisions
        review_summary = self.review_manager.get_review_summary()
        if review_summary["total_reviews"] > 0:
            lines.append("HUMAN REVIEW DECISIONS:")
            lines.append("-" * 40)
            for d in review_summary["decisions"]:
                lines.append(f"  {d['decision'].upper()} by {d['reviewer']} — {d['reason']}")
            lines.append("")

        lines.append("OUTPUT FILES:")
        lines.append("-" * 40)
        lines.append(f"  - {CLEANED_CSV.name}")
        lines.append(f"  - {MASKED_CSV.name}")
        lines.append(f"  - data_quality_report.txt")
        lines.append(f"  - pii_detection_report.txt")
        lines.append(f"  - validation_results.txt")
        lines.append(f"  - cleaning_log.txt")
        lines.append(f"  - masked_sample.txt")
        lines.append(f"  - statistical_validation_report.txt")
        lines.append(f"  - data_contract_report.txt")
        lines.append(f"  - ge_validation_report.txt")
        lines.append(f"  - dead_letter_queue_report.txt")
        lines.append(f"  - human_review_report.txt")
        lines.append(f"  - pipeline_execution_report.txt")
        lines.append(f"  - metrics/ (Prometheus + JSON)")
        lines.append("")
        lines.append("Status: SUCCESS [OK]")

        report_text = "\n".join(lines)

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(PIPELINE_REPORT, "w", encoding="utf-8") as f:
            f.write(report_text)

        self._log_stage("REPORT", "SUCCESS", [
            "Pipeline execution report saved",
            f"All outputs written to {REPORTS_DIR}",
        ])
        return report_text

    # ------------------------------------------------------------------
    # Run the complete pipeline
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Execute the full production pipeline end-to-end."""
        self.start_time = datetime.now()
        logger.info("Pipeline started at %s (run_id: %s)", self.start_time, self.run_id)
        print(f"\n{'='*60}")
        print("  DATA QUALITY GOVERNANCE PIPELINE (PRODUCTION)")
        print(f"  Run ID: {self.run_id}")
        print(f"{'='*60}\n")

        try:
            # Stage 1: Load
            self.load()

            # Stage 2: Idempotency check
            if self.force:
                needs_run = True
                self._log_stage("IDEMPOTENCY", "SKIPPED", ["--force flag: bypassing idempotency check"])
            else:
                needs_run = self.check_idempotency()
            if not needs_run:
                self.end_time = datetime.now()
                print(f"\n{'='*60}")
                print("  PIPELINE SKIPPED -- INPUT UNCHANGED")
                print(f"{'='*60}\n")
                return

            # Stage 3: Profile
            self.profile()

            # Stage 4: Data contract validation
            self.validate_contract()

            # Stage 5: Validate raw data
            validation_results = self.validate_raw()

            # Stage 6: Statistical validation
            stat_results = self.validate_statistical()

            # Stage 7: Threshold check & human review
            decision = self.check_threshold_and_review(validation_results)

            # Stage 8: Clean
            self.clean(
                validation_before=validation_results["failed"],
                decision=decision,
            )

            # Stage 9: Quarantine failed records
            self.quarantine_failed_records(decision=decision)

            # Stage 10: Great Expectations validation
            self.validate_expectations()

            # Stage 11: Detect PII
            self.detect_pii()

            # Stage 12: Mask PII
            self.mask_pii()

            # Stage 13: Version snapshot
            self.create_version()

            # Finalize timing
            self.end_time = datetime.now()

            # Stage 14: Metrics export
            self.export_metrics(validation_results, stat_results)

            # Stage 15: Cloud upload
            self.upload_to_cloud()

            # Stage 16: Notifications
            self.send_notifications("SUCCESS")

            # Stage 17: Report
            self.review_manager.generate_report()
            report = self.generate_pipeline_report()

            print(f"\n{'='*60}")
            print("  PIPELINE COMPLETED SUCCESSFULLY")
            print(f"{'='*60}\n")
            print(report)

        except ThresholdBreachError as exc:
            self.end_time = datetime.now()
            logger.error("Pipeline halted by threshold/review: %s", exc)
            self._log_stage("PIPELINE", "HALTED", [str(exc)])
            self.notifier.notify_pipeline_failure(str(exc))
            self.review_manager.generate_report()
            self.generate_pipeline_report()
            print(f"\n{'='*60}")
            print(f"  PIPELINE HALTED: {exc}")
            print(f"{'='*60}\n")
            raise

        except PipelineError as exc:
            self.end_time = datetime.now()
            logger.error("Pipeline failed: %s", exc, exc_info=True)
            self._log_stage("PIPELINE", "FAILURE", [str(exc)])
            self.notifier.notify_pipeline_failure(str(exc))
            print(f"\n{'='*60}")
            print(f"  PIPELINE FAILED: {exc}")
            print(f"{'='*60}\n")
            raise

        except Exception as exc:
            self.end_time = datetime.now()
            logger.error("Pipeline unexpected error: %s", exc, exc_info=True)
            self._log_stage("PIPELINE", "FAILURE", [str(exc)])
            self.notifier.notify_pipeline_failure(str(exc))
            print(f"\n{'='*60}")
            print(f"  PIPELINE FAILED: {exc}")
            print(f"{'='*60}\n")
            raise
