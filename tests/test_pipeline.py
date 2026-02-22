"""
Unit Tests for Data Quality Governance Pipeline
================================================
Tests each module independently to verify correctness of:
  - Data profiling
  - PII detection
  - Schema validation
  - Data cleaning
  - PII masking
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile

from src.profiler import DataProfiler
from src.pii_detector import PIIDetector
from src.validator import DataValidator
from src.cleaner import DataCleaner
from src.masker import PIIMasker


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
        assert result["count"] >= 2  # At least rows with some name

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
        # IDs 1, 2, 3 are valid positive integers
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
        assert len(phone_failures) >= 2  # (555) and 10-digit formats

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
        # All phones should now be XXX-XXX-XXXX
        import re
        pattern = re.compile(r"^\d{3}-\d{3}-\d{4}$")
        for val in cleaner.df["phone"].dropna():
            assert pattern.match(str(val)), f"Phone not normalized: {val}"

    def test_date_normalization(self, sample_df):
        cleaner = DataCleaner(sample_df)
        cleaner.normalize_dates()
        # 1990/07/22 should become 1990-07-22
        row2_dob = cleaner.df.at[1, "date_of_birth"]
        assert row2_dob == "1990-07-22"

    def test_name_title_case(self, sample_df):
        cleaner = DataCleaner(sample_df)
        cleaner.normalize_names()
        # Names should be title case
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
        # first_name row 2 was empty -> should be "[UNKNOWN]"
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
