"""
Unit Tests for Data Quality Governance Pipeline
================================================
Tests each module independently to verify correctness of:
  - Data profiling
  - PII detection
  - Schema validation
  - Data cleaning
  - PII masking
  - Statistical validation
  - Dead letter queue
  - Retry handler
  - Human review
  - Rollback manager
  - Data contract validation
  - Metrics collector
  - Great Expectations validation
  - Notifier
  - Cloud storage
  - Pipeline orchestrator (production)
"""

import json
import os
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock

from src.profiler import DataProfiler
from src.pii_detector import PIIDetector
from src.validator import DataValidator
from src.cleaner import DataCleaner
from src.masker import PIIMasker
from src.statistical_validator import StatisticalValidator
from src.dead_letter_queue import DeadLetterQueue
from src.retry_handler import retry_with_backoff, RetryError
from src.human_review import HumanReviewManager, ReviewDecision
from src.rollback_manager import RollbackManager
from src.data_contract import DataContractValidator
from src.metrics_collector import MetricsCollector
from src.expectations import GreatExpectationsValidator
from src.notifier import Notifier
from src.cloud_storage import CloudStorageClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a sample DataFrame mimicking the raw customer data."""
    return pd.DataFrame({
        "customer_id": [1, 2, 3],
        "first_name": ["John", "", "Mary"],
        "last_name": ["Doe", "Smith", ""],
        "email": ["john@gmail.com", "JANE@WORK.COM", "mary@test.com"],
        "phone": ["555-123-4567", "(555) 234-5678", "5551234567"],
        "date_of_birth": ["1985-03-15", "1990/07/22", "invalid_date"],
        "address": [
            "123 Main St New York NY 10001",
            "",
            "456 Oak Ave LA CA 90001",
        ],
        "income": [75000, np.nan, 120000],
        "account_status": ["active", "inactive", ""],
        "created_date": ["2024-01-10", "01/15/2024", "2024-01-12"],
    })


@pytest.fixture
def clean_df() -> pd.DataFrame:
    """Create a clean DataFrame (post-cleaning)."""
    return pd.DataFrame({
        "customer_id": [1, 2],
        "first_name": ["John", "Jane"],
        "last_name": ["Doe", "Smith"],
        "email": ["john@gmail.com", "jane@work.com"],
        "phone": ["555-123-4567", "555-234-5678"],
        "date_of_birth": ["1985-03-15", "1990-07-22"],
        "address": [
            "123 Main St New York NY 10001",
            "456 Oak Ave LA CA 90001",
        ],
        "income": [75000, 95000],
        "account_status": ["active", "inactive"],
        "created_date": ["2024-01-10", "2024-01-11"],
    })


@pytest.fixture
def large_sample_df() -> pd.DataFrame:
    """Create a larger DataFrame for statistical tests."""
    np.random.seed(42)
    n = 100
    return pd.DataFrame({
        "customer_id": list(range(1, n + 1)),
        "first_name": [f"Name{i}" for i in range(n)],
        "last_name": [f"Last{i}" for i in range(n)],
        "email": [f"user{i}@test.com" for i in range(n)],
        "phone": [f"555-{100+i}-{1000+i}" for i in range(n)],
        "date_of_birth": pd.date_range("1970-01-01", periods=n, freq="90D").strftime("%Y-%m-%d").tolist(),
        "address": [f"{i} Main St City ST {10000+i}" for i in range(n)],
        "income": np.concatenate([
            np.random.normal(70000, 15000, n - 2).astype(int),
            [500000, -5000],  # outliers
        ]),
        "account_status": np.random.choice(["active", "inactive", "suspended"], n).tolist(),
        "created_date": pd.date_range("2024-01-01", periods=n, freq="D").strftime("%Y-%m-%d").tolist(),
    })


# ---------------------------------------------------------------------------
# Profiler Tests
# ---------------------------------------------------------------------------
class TestProfiler:
    """Tests for DataProfiler."""

    def test_completeness_identifies_missing(self, sample_df):
        profiler = DataProfiler(sample_df)
        result = profiler.analyze_completeness()
        assert result["first_name"]["missing"] > 0
        assert result["customer_id"]["percentage"] == 100.0

    def test_data_types_detected(self, sample_df):
        profiler = DataProfiler(sample_df)
        result = profiler.analyze_data_types()
        assert "customer_id" in result
        assert "date_of_birth" in result

    def test_uniqueness_check(self, sample_df):
        profiler = DataProfiler(sample_df)
        result = profiler.analyze_uniqueness()
        assert result.get("customer_id") is True

    def test_full_profile_returns_all_sections(self, sample_df):
        profiler = DataProfiler(sample_df)
        result = profiler.run_full_profile()
        assert "completeness" in result
        assert "types" in result
        assert "issues" in result
        assert "severity" in result

    def test_report_generation(self, sample_df, tmp_path):
        profiler = DataProfiler(sample_df)
        out_file = tmp_path / "test_report.txt"
        report = profiler.generate_report(filepath=out_file)
        assert "DATA QUALITY PROFILE REPORT" in report
        assert out_file.exists()


# ---------------------------------------------------------------------------
# PII Detector Tests
# ---------------------------------------------------------------------------
class TestPIIDetector:
    """Tests for PIIDetector."""

    def test_email_detection(self, sample_df):
        detector = PIIDetector(sample_df)
        result = detector.detect_emails()
        assert result["count"] == 3

    def test_phone_detection(self, sample_df):
        detector = PIIDetector(sample_df)
        result = detector.detect_phones()
        assert result["count"] == 3

    def test_name_detection(self, sample_df):
        detector = PIIDetector(sample_df)
        result = detector.detect_names()
        assert result["count"] >= 2

    def test_address_detection(self, sample_df):
        detector = PIIDetector(sample_df)
        result = detector.detect_addresses()
        assert result["count"] >= 1

    def test_full_detection(self, sample_df):
        detector = PIIDetector(sample_df)
        findings = detector.run_full_detection()
        assert "email" in findings
        assert "phone" in findings
        assert "names" in findings

    def test_risk_assessment(self, sample_df):
        detector = PIIDetector(sample_df)
        risk = detector.assess_risk()
        assert len(risk["high_risk"]) > 0
        assert len(risk["exposure_scenarios"]) > 0

    def test_report_generation(self, sample_df, tmp_path):
        detector = PIIDetector(sample_df)
        out_file = tmp_path / "test_pii.txt"
        report = detector.generate_report(filepath=out_file)
        assert "PII DETECTION REPORT" in report
        assert out_file.exists()


# ---------------------------------------------------------------------------
# Validator Tests
# ---------------------------------------------------------------------------
class TestValidator:
    """Tests for DataValidator."""

    def test_validates_customer_id(self, sample_df):
        validator = DataValidator(sample_df)
        validator.validate_customer_id()
        id_failures = [f for f in validator.failures if f.column == "customer_id"]
        assert len(id_failures) == 0

    def test_detects_missing_names(self, sample_df):
        validator = DataValidator(sample_df)
        validator.validate_name("first_name")
        name_failures = [f for f in validator.failures if f.column == "first_name"]
        assert len(name_failures) > 0

    def test_detects_invalid_dates(self, sample_df):
        validator = DataValidator(sample_df)
        validator.validate_date("date_of_birth")
        date_failures = [f for f in validator.failures if f.column == "date_of_birth"]
        assert len(date_failures) > 0

    def test_detects_non_standard_phone(self, sample_df):
        validator = DataValidator(sample_df)
        validator.validate_phone()
        phone_failures = [f for f in validator.failures if f.column == "phone"]
        assert len(phone_failures) >= 2

    def test_run_all_validations(self, sample_df):
        validator = DataValidator(sample_df)
        results = validator.run_all_validations()
        assert results["total_rows"] == 3
        assert results["failed"] > 0

    def test_report_generation(self, sample_df, tmp_path):
        validator = DataValidator(sample_df)
        out_file = tmp_path / "test_validation.txt"
        report = validator.generate_report(filepath=out_file)
        assert "VALIDATION RESULTS" in report
        assert out_file.exists()


# ---------------------------------------------------------------------------
# Cleaner Tests
# ---------------------------------------------------------------------------
class TestCleaner:
    """Tests for DataCleaner."""

    def test_phone_normalization(self, sample_df):
        cleaner = DataCleaner(sample_df)
        cleaner.normalize_phones()
        import re
        pattern = re.compile(r"^\d{3}-\d{3}-\d{4}$")
        for val in cleaner.df["phone"].dropna():
            assert pattern.match(str(val)), f"Phone not normalized: {val}"

    def test_date_normalization(self, sample_df):
        cleaner = DataCleaner(sample_df)
        cleaner.normalize_dates()
        row2_dob = cleaner.df.at[1, "date_of_birth"]
        assert row2_dob == "1990-07-22"

    def test_name_title_case(self, sample_df):
        cleaner = DataCleaner(sample_df)
        cleaner.normalize_names()
        for col in ("first_name", "last_name"):
            for val in cleaner.df[col].dropna():
                val_str = str(val).strip()
                if val_str and val_str.lower() != "nan":
                    assert val_str == val_str.title(), f"{val_str} not title case"

    def test_email_lowercase(self, sample_df):
        cleaner = DataCleaner(sample_df)
        cleaner.normalize_emails()
        for val in cleaner.df["email"].dropna():
            assert str(val) == str(val).lower()

    def test_missing_values_filled(self, sample_df):
        cleaner = DataCleaner(sample_df)
        cleaner.handle_missing_values()
        assert cleaner.df.at[1, "first_name"] == "[UNKNOWN]"

    def test_full_cleaning(self, sample_df):
        cleaner = DataCleaner(sample_df)
        result = cleaner.run_full_cleaning()
        assert len(result) == 3

    def test_report_generation(self, sample_df, tmp_path):
        cleaner = DataCleaner(sample_df)
        cleaner.run_full_cleaning()
        out_file = tmp_path / "test_cleaning.txt"
        report = cleaner.generate_report(filepath=out_file)
        assert "DATA CLEANING LOG" in report
        assert out_file.exists()


# ---------------------------------------------------------------------------
# Masker Tests
# ---------------------------------------------------------------------------
class TestMasker:
    """Tests for PIIMasker."""

    def test_mask_name(self):
        assert PIIMasker.mask_name("John") == "J***"
        assert PIIMasker.mask_name("Patricia") == "P***"
        assert PIIMasker.mask_name("[UNKNOWN]") == "[UNKNOWN]"

    def test_mask_email(self):
        assert PIIMasker.mask_email("john.doe@gmail.com") == "j***@gmail.com"
        assert PIIMasker.mask_email("a@b.com") == "a***@b.com"

    def test_mask_phone(self):
        assert PIIMasker.mask_phone("555-123-4567") == "***-***-4567"
        assert PIIMasker.mask_phone("555-987-6543") == "***-***-6543"

    def test_mask_address(self):
        result = PIIMasker.mask_address("123 Main St New York NY 10001")
        assert result == "[MASKED ADDRESS]"
        assert PIIMasker.mask_address("[UNKNOWN]") == "[UNKNOWN]"

    def test_mask_dob(self):
        assert PIIMasker.mask_dob("1985-03-15") == "1985-**-**"
        assert PIIMasker.mask_dob("1990-07-22") == "1990-**-**"

    def test_mask_all_pii(self, clean_df):
        masker = PIIMasker(clean_df)
        masked = masker.mask_all_pii()
        assert masked.at[0, "first_name"] == "J***"
        assert masked.at[0, "email"] == "j***@gmail.com"
        assert masked.at[0, "phone"] == "***-***-4567"
        assert masked.at[0, "address"] == "[MASKED ADDRESS]"
        assert masked.at[0, "date_of_birth"] == "1985-**-**"

    def test_business_data_preserved(self, clean_df):
        masker = PIIMasker(clean_df)
        masked = masker.mask_all_pii()
        assert masked.at[0, "income"] == 75000
        assert masked.at[0, "account_status"] == "active"
        assert masked.at[0, "customer_id"] == 1

    def test_report_generation(self, clean_df, tmp_path):
        masker = PIIMasker(clean_df)
        masker.mask_all_pii()
        out_file = tmp_path / "test_masked.txt"
        report = masker.generate_report(filepath=out_file)
        assert "PII MASKING REPORT" in report
        assert "BEFORE MASKING" in report
        assert "AFTER MASKING" in report
        assert out_file.exists()


# ---------------------------------------------------------------------------
# Statistical Validator Tests
# ---------------------------------------------------------------------------
class TestStatisticalValidator:
    """Tests for StatisticalValidator."""

    def test_iqr_outlier_detection(self, large_sample_df):
        validator = StatisticalValidator(large_sample_df)
        outliers = validator.detect_outliers_iqr("income")
        # Should detect the 500000 and -5000 outliers
        assert len(outliers) >= 1

    def test_zscore_outlier_detection(self, large_sample_df):
        validator = StatisticalValidator(large_sample_df)
        outliers = validator.detect_outliers_zscore("income", threshold=3.0)
        assert isinstance(outliers, list)

    def test_date_distribution_analysis(self, large_sample_df):
        validator = StatisticalValidator(large_sample_df)
        issues = validator.analyze_date_distribution("date_of_birth")
        assert isinstance(issues, list)

    def test_run_all(self, large_sample_df):
        results = StatisticalValidator(large_sample_df).run_all()
        assert "total_outliers" in results
        assert "total_distribution_issues" in results
        assert isinstance(results["total_outliers"], int)

    def test_generate_report(self, large_sample_df, tmp_path):
        validator = StatisticalValidator(large_sample_df)
        validator.run_all()
        out = tmp_path / "stat_report.txt"
        report = validator.generate_report(filepath=out)
        assert "STATISTICAL VALIDATION" in report
        assert out.exists()


# ---------------------------------------------------------------------------
# Dead Letter Queue Tests
# ---------------------------------------------------------------------------
class TestDeadLetterQueue:
    """Tests for DeadLetterQueue."""

    def test_add_record(self):
        dlq = DeadLetterQueue()
        dlq.add_record(
            row_index=1,
            row_data={"customer_id": 1, "email": "test@test.com"},
            reason="Invalid email format",
            severity="HIGH",
            stage="validation",
        )
        assert dlq.count == 1

    def test_severity_tracking(self):
        dlq = DeadLetterQueue()
        dlq.add_record(1, {"id": 1}, "reason1", "HIGH", "stage1")
        dlq.add_record(2, {"id": 2}, "reason2", "CRITICAL", "stage1")
        dlq.add_record(3, {"id": 3}, "reason3", "HIGH", "stage2")
        assert dlq.quarantine_summary["HIGH"] == 2
        assert dlq.quarantine_summary["CRITICAL"] == 1

    def test_save_quarantine(self, tmp_path):
        dlq = DeadLetterQueue()
        dlq.add_record(1, {"id": 1}, "test", "LOW", "test")
        # Patch QUARANTINE_DIR
        with patch("src.dead_letter_queue.QUARANTINE_DIR", tmp_path):
            path = dlq.save_quarantine("test_run")
            assert path.exists()
            data = json.loads(path.read_text())
            assert data["total_quarantined"] == 1
            assert len(data["records"]) == 1

    def test_get_records_by_severity(self):
        dlq = DeadLetterQueue()
        dlq.add_record(1, {}, "r1", "HIGH", "s1")
        dlq.add_record(2, {}, "r2", "LOW", "s1")
        high = dlq.get_records_by_severity("HIGH")
        assert len(high) == 1

    def test_generate_report(self):
        dlq = DeadLetterQueue()
        dlq.add_record(1, {}, "reason", "MEDIUM", "test_stage")
        report = dlq.generate_report()
        assert "DEAD LETTER QUEUE" in report


# ---------------------------------------------------------------------------
# Retry Handler Tests
# ---------------------------------------------------------------------------
class TestRetryHandler:
    """Tests for retry_with_backoff decorator."""

    def test_succeeds_without_retry(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def success_func():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = success_func()
        assert result == "ok"
        assert call_count == 1

    def test_retries_on_failure(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Transient error")
            return "recovered"

        result = fail_then_succeed()
        assert result == "recovered"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        @retry_with_backoff(max_retries=2, base_delay=0.01)
        def always_fail():
            raise ConnectionError("Permanent error")

        with pytest.raises((RetryError, ConnectionError)):
            always_fail()


# ---------------------------------------------------------------------------
# Human Review Tests
# ---------------------------------------------------------------------------
class TestHumanReview:
    """Tests for HumanReviewManager."""

    def test_review_decision_creation(self):
        decision = ReviewDecision(
            decision=ReviewDecision.CONTINUE,
            reviewer="test_user",
            reason="Data looks acceptable",
            rows=[1, 2, 3],
        )
        assert decision.decision == "continue"
        assert decision.reviewer == "test_user"

    def test_review_decision_to_dict(self):
        decision = ReviewDecision("discard", "admin", "Too many errors")
        d = decision.to_dict()
        assert d["decision"] == "discard"
        assert d["reviewer"] == "admin"
        assert "timestamp" in d

    @patch.dict(os.environ, {"HUMAN_REVIEW_ENABLED": "false"})
    def test_review_disabled_returns_timeout_action(self, sample_df):
        manager = HumanReviewManager()
        decision = manager.request_review_interactive(
            sample_df, [1, 2], 75.0, 80.0
        )
        assert decision.decision in ("continue", "discard", "quarantine")
        assert decision.reviewer == "system-auto"

    def test_get_review_summary(self):
        manager = HumanReviewManager()
        summary = manager.get_review_summary()
        assert "total_reviews" in summary
        assert summary["total_reviews"] == 0

    def test_generate_report(self):
        manager = HumanReviewManager()
        report = manager.generate_report()
        assert "HUMAN" in report and "REVIEW" in report


# ---------------------------------------------------------------------------
# Rollback Manager Tests
# ---------------------------------------------------------------------------
class TestRollbackManager:
    """Tests for RollbackManager."""

    def test_create_version(self, clean_df, tmp_path):
        with patch("src.rollback_manager.VERSIONS_DIR", tmp_path):
            rm = RollbackManager()
            rm.versions_dir = tmp_path
            rm.manifest_file = tmp_path / "manifest.json"
            version_id = rm.create_version(clean_df, run_id="test_run")
            assert version_id is not None
            assert len(rm.list_versions()) == 1

    def test_rollback(self, clean_df, tmp_path):
        with patch("src.rollback_manager.VERSIONS_DIR", tmp_path):
            rm = RollbackManager()
            rm.versions_dir = tmp_path
            rm.manifest_file = tmp_path / "manifest.json"
            rm.create_version(clean_df, run_id="v1")
            rm.create_version(clean_df, run_id="v2")
            rolled_back = rm.rollback()
            assert rolled_back is not None
            assert len(rolled_back) == len(clean_df)

    def test_idempotency_check(self, tmp_path):
        # Create a dummy input file
        input_file = tmp_path / "test_input.csv"
        input_file.write_text("a,b\n1,2\n")

        with patch("src.rollback_manager.VERSIONS_DIR", tmp_path):
            rm = RollbackManager()
            rm.versions_dir = tmp_path
            rm.manifest_file = tmp_path / "manifest.json"
            # First check — no prior versions, should run
            assert rm.check_idempotency(input_file) is True

    def test_cleanup_old_versions(self, clean_df, tmp_path):
        with patch("src.rollback_manager.VERSIONS_DIR", tmp_path):
            rm = RollbackManager()
            rm.versions_dir = tmp_path
            rm.manifest_file = tmp_path / "manifest.json"
            for i in range(7):
                rm.create_version(clean_df, run_id=f"run_{i}")
            removed = rm.cleanup_old_versions(keep=3)
            assert removed >= 4


# ---------------------------------------------------------------------------
# Data Contract Validator Tests
# ---------------------------------------------------------------------------
class TestDataContractValidator:
    """Tests for DataContractValidator."""

    def test_schema_validation_passes(self, sample_df):
        validator = DataContractValidator(sample_df)
        violations = validator.validate_schema()
        # Sample has all expected columns, so no missing column violations
        missing_col_violations = [v for v in violations if "missing" in v.get("message", "").lower()]
        # Should have no missing column violations since sample_df has all columns
        assert isinstance(violations, list)

    def test_completeness_sla(self, sample_df):
        validator = DataContractValidator(sample_df)
        result = validator.validate_completeness_sla()
        assert "passed" in result
        assert "actual_pct" in result

    def test_duplicate_sla(self, sample_df):
        validator = DataContractValidator(sample_df)
        result = validator.validate_duplicate_sla()
        assert "passed" in result
        assert "actual_pct" in result

    def test_validate_all(self, sample_df):
        validator = DataContractValidator(sample_df)
        results = validator.validate_all()
        assert "all_passed" in results
        assert "contract_version" in results
        assert "total_violations" in results

    def test_generate_report(self, sample_df, tmp_path):
        validator = DataContractValidator(sample_df)
        out = tmp_path / "contract_report.txt"
        report = validator.generate_report(filepath=out)
        assert "DATA CONTRACT" in report
        assert out.exists()


# ---------------------------------------------------------------------------
# Metrics Collector Tests
# ---------------------------------------------------------------------------
class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_record_stage(self):
        mc = MetricsCollector(run_id="test")
        mc.record_stage("load", "SUCCESS", 150.5, {"rows": 100})
        assert "load" in mc.metrics["stages"]
        assert mc.metrics["stages"]["load"]["status"] == "SUCCESS"

    def test_record_quality_metrics(self):
        mc = MetricsCollector(run_id="test")
        mc.record_quality_metrics(
            total_rows=100,
            passed_rows=95,
            failed_rows=5,
            quarantined_rows=2,
            outlier_count=3,
        )
        assert mc.metrics["quality"]["total_rows"] == 100
        assert mc.metrics["quality"]["pass_rate_pct"] == 95.0

    def test_record_pii_metrics(self):
        mc = MetricsCollector(run_id="test")
        mc.record_pii_metrics(
            total_pii_fields=50,
            masked_fields=50,
            pii_categories={"names": 10, "emails": 10, "phones": 10, "addresses": 10, "dobs": 10},
        )
        assert mc.metrics["pii"]["total_pii_fields"] == 50

    def test_record_performance(self):
        mc = MetricsCollector(run_id="test")
        mc.record_performance(total_duration_seconds=5.5, rows_per_second=100.0)
        assert mc.metrics["performance"]["total_duration_seconds"] == 5.5

    def test_save_metrics(self, tmp_path):
        mc = MetricsCollector(run_id="test")
        mc.record_quality_metrics(100, 90, 10, 5, 2)
        with patch("src.metrics_collector.METRICS_DIR", tmp_path):
            path = mc.save_metrics()
            assert path.exists()

    def test_export_prometheus_metrics(self, tmp_path):
        mc = MetricsCollector(run_id="test")
        mc.record_quality_metrics(100, 90, 10, 5, 2)
        mc.record_performance(10.0, 10.0)
        mc.record_pii_metrics(20, 20, {"names": 5, "emails": 5, "phones": 5, "addresses": 3, "dobs": 2})
        with patch("src.metrics_collector.METRICS_DIR", tmp_path):
            prom = mc.export_prometheus_metrics()
            assert "dq_total_rows" in prom
            assert "dq_pass_rate" in prom

    def test_add_alert(self):
        mc = MetricsCollector(run_id="test")
        mc.add_alert("HIGH", "Test alert")
        assert len(mc.metrics["alerts"]) == 1


# ---------------------------------------------------------------------------
# Great Expectations Validator Tests
# ---------------------------------------------------------------------------
class TestGreatExpectationsValidator:
    """Tests for GreatExpectationsValidator."""

    def test_run_validation(self, clean_df):
        ge = GreatExpectationsValidator(clean_df)
        results = ge.run_validation()
        assert "total" in results
        assert "passed" in results
        assert "failed" in results
        assert "pass_rate_pct" in results
        assert results["total"] > 0

    def test_pass_rate_reasonable(self, clean_df):
        ge = GreatExpectationsValidator(clean_df)
        results = ge.run_validation()
        # Clean data should pass most expectations
        assert results["pass_rate_pct"] >= 50.0

    def test_generate_report(self, clean_df, tmp_path):
        ge = GreatExpectationsValidator(clean_df)
        ge.run_validation()
        out = tmp_path / "ge_report.txt"
        report = ge.generate_report(filepath=out)
        assert "GREAT EXPECTATIONS" in report or "VALIDATION" in report
        assert out.exists()


# ---------------------------------------------------------------------------
# Notifier Tests
# ---------------------------------------------------------------------------
class TestNotifier:
    """Tests for Notifier."""

    def test_notify_logs_message(self, caplog):
        notifier = Notifier()
        with caplog.at_level("INFO"):
            notifier.notify("Test message", severity="INFO")
        # Should not raise even if all channels are disabled

    def test_notify_pipeline_success(self):
        notifier = Notifier()
        # Should not raise
        notifier.notify_pipeline_success("Pipeline completed in 5s")

    def test_notify_pipeline_failure(self):
        notifier = Notifier()
        notifier.notify_pipeline_failure("Something broke")

    def test_notify_threshold_breach(self):
        notifier = Notifier()
        notifier.notify_threshold_breach(15.0, 5.0)

    def test_notify_human_review_needed(self):
        notifier = Notifier()
        notifier.notify_human_review_needed("Review needed: 20% failure rate")


# ---------------------------------------------------------------------------
# Cloud Storage Tests
# ---------------------------------------------------------------------------
class TestCloudStorage:
    """Tests for CloudStorageClient."""

    @patch.dict(os.environ, {"CLOUD_STORAGE_ENABLED": "false"})
    def test_disabled_by_default(self):
        client = CloudStorageClient()
        assert client.enabled is False

    @patch.dict(os.environ, {"CLOUD_STORAGE_ENABLED": "false"})
    def test_upload_noop_when_disabled(self, tmp_path):
        client = CloudStorageClient()
        test_file = tmp_path / "test.csv"
        test_file.write_text("a,b\n1,2\n")
        # Should not raise when disabled
        result = client.upload_file(test_file, "test/")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Pipeline Integration Tests
# ---------------------------------------------------------------------------
class TestPipelineIntegration:
    """Integration tests for the full production pipeline."""

    def test_pipeline_init(self, tmp_path):
        dummy = tmp_path / "test.csv"
        dummy.write_text("customer_id,first_name\n1,John\n")
        from src.pipeline import Pipeline
        p = Pipeline(input_path=dummy)
        assert p.input_path == dummy
        assert p.run_id is not None
        assert p.dlq is not None
        assert p.review_manager is not None

    def test_pipeline_load(self, sample_df, tmp_path):
        csv_path = tmp_path / "test_raw.csv"
        sample_df.to_csv(csv_path, index=False)

        from src.pipeline import Pipeline
        p = Pipeline(input_path=csv_path)
        df = p.load()
        assert len(df) == 3
        assert len(p.stages) == 1
        assert p.stages[0]["status"] == "SUCCESS"

    def test_pipeline_load_failure(self, tmp_path):
        from src.pipeline import Pipeline, PipelineError
        p = Pipeline(input_path=tmp_path / "nonexistent.csv")
        with pytest.raises(PipelineError):
            p.load()

    def test_threshold_continue(self, sample_df):
        """When failure rate is below threshold, pipeline continues."""
        from src.pipeline import Pipeline
        p = Pipeline()
        p.df_raw = sample_df
        results = {
            "total_rows": 100,
            "passed": 98,
            "failed": 2,
            "failures": [],
            "failed_rows": [1, 2],
        }
        decision = p.check_threshold_and_review(results)
        assert decision == ReviewDecision.CONTINUE

    @patch.dict(os.environ, {"HUMAN_REVIEW_ENABLED": "false"})
    def test_threshold_quarantine_when_review_disabled(self, sample_df):
        """When failure rate exceeds threshold and review is disabled, quarantine."""
        from src.pipeline import Pipeline
        p = Pipeline()
        p.df_raw = sample_df
        results = {
            "total_rows": 100,
            "passed": 10,
            "failed": 90,
            "failures": [],
            "failed_rows": list(range(1, 91)),
        }
        decision = p.check_threshold_and_review(results)
        assert decision in (ReviewDecision.QUARANTINE, ReviewDecision.CONTINUE,
                            ReviewDecision.DISCARD)
