"""
Data Validator Module (Part 3)
==============================
Defines and applies schema-based validation rules for each column
in the customer dataset.

Generates a validation results report listing all failures by column and row.
"""

import logging
import re
from datetime import datetime
from typing import Any

import pandas as pd
import numpy as np

from src.config import (
    SCHEMA_RULES,
    VALID_ACCOUNT_STATUSES,
    EMAIL_PATTERN,
    DATE_PATTERNS,
    VALIDATION_RESULTS,
    REPORTS_DIR,
)

logger = logging.getLogger(__name__)


class ValidationFailure:
    """Represents a single validation failure."""

    def __init__(self, row: int, column: str, value: Any, rule: str, message: str) -> None:
        self.row = row
        self.column = column
        self.value = value
        self.rule = rule
        self.message = message

    def __repr__(self) -> str:
        return f"Row {self.row}, {self.column}: {self.message} (value='{self.value}')"


class DataValidator:
    """Validates a DataFrame against the expected schema rules."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        self.total_rows = len(df)
        self.failures: list[ValidationFailure] = []
        self.passed_rows: set[int] = set()
        self.failed_rows: set[int] = set()

    
    # Individual column validators
    
    def validate_customer_id(self) -> None:
        """Validate customer_id: unique, positive integer."""
        col = "customer_id"
        if col not in self.df.columns:
            return

        for idx, val in self.df[col].items():
            row = idx + 1
            if pd.isna(val):
                self._add_failure(row, col, val, "not_null", "Missing customer_id")
                continue
            try:
                int_val = int(val)
                if int_val <= 0:
                    self._add_failure(row, col, val, "positive", "Must be positive")
            except (ValueError, TypeError):
                self._add_failure(row, col, val, "integer", "Must be an integer")

        # Uniqueness check
        if self.df[col].dropna().duplicated().any():
            dupes = self.df[col][self.df[col].duplicated(keep=False)]
            for idx in dupes.index:
                self._add_failure(
                    idx + 1, col, self.df.at[idx, col],
                    "unique", "Duplicate customer_id"
                )

    def validate_name(self, col: str) -> None:
        """Validate first_name or last_name: non-null, alphabetic, 2-50 chars."""
        if col not in self.df.columns:
            return

        for idx, val in self.df[col].items():
            row = idx + 1
            val_str = str(val).strip() if pd.notna(val) else ""

            if not val_str or val_str.lower() == "nan":
                self._add_failure(row, col, val, "not_null", "Empty (should be non-empty)")
                continue

            if len(val_str) < 2 or len(val_str) > 50:
                self._add_failure(
                    row, col, val_str, "length",
                    f"Length {len(val_str)} outside 2-50 range"
                )

            if not val_str.isalpha():
                # Allow title-case check but don't fail on case alone
                cleaned = val_str.replace(" ", "").replace("-", "").replace("'", "")
                if not cleaned.isalpha():
                    self._add_failure(
                        row, col, val_str, "alphabetic",
                        "Contains non-alphabetic characters"
                    )

            if val_str != val_str.title() and val_str.isalpha():
                self._add_failure(
                    row, col, val_str, "title_case",
                    f"'{val_str}' should be title case ('{val_str.title()}')"
                )

    def validate_email(self) -> None:
        """Validate email: valid format."""
        col = "email"
        if col not in self.df.columns:
            return

        for idx, val in self.df[col].items():
            row = idx + 1
            val_str = str(val).strip() if pd.notna(val) else ""

            if not val_str or val_str.lower() == "nan":
                self._add_failure(row, col, val, "not_null", "Missing email")
                continue

            if not EMAIL_PATTERN.match(val_str):
                self._add_failure(
                    row, col, val_str, "email_format", "Invalid email format"
                )

            if val_str != val_str.lower():
                self._add_failure(
                    row, col, val_str, "email_case",
                    f"'{val_str}' contains uppercase (should be lowercase)"
                )

    def validate_phone(self) -> None:
        """Validate phone: reasonable format, normalizable."""
        col = "phone"
        if col not in self.df.columns:
            return

        standard = re.compile(r"^\d{3}-\d{3}-\d{4}$")

        for idx, val in self.df[col].items():
            row = idx + 1
            val_str = str(val).strip() if pd.notna(val) else ""

            if not val_str or val_str.lower() == "nan":
                self._add_failure(row, col, val, "not_null", "Missing phone")
                continue

            if not standard.match(val_str):
                self._add_failure(
                    row, col, val_str, "phone_format",
                    f"Non-standard format (should be XXX-XXX-XXXX)"
                )

    def validate_date(self, col: str) -> None:
        """Validate a date column: valid date, YYYY-MM-DD format."""
        if col not in self.df.columns:
            return

        for idx, val in self.df[col].items():
            row = idx + 1
            val_str = str(val).strip() if pd.notna(val) else ""

            if not val_str or val_str.lower() == "nan":
                self._add_failure(row, col, val, "not_null", "Missing date")
                continue

            if val_str.lower() == "invalid_date":
                self._add_failure(
                    row, col, val_str, "valid_date",
                    "'invalid_date' is not a valid date value"
                )
                continue

            # Try parsing with standard format first
            try:
                datetime.strptime(val_str, "%Y-%m-%d")
                continue  # Valid and correct format
            except ValueError:
                pass

            # Try other formats (valid but wrong format)
            parsed = False
            for fmt, label in DATE_PATTERNS[1:]:  # Skip YYYY-MM-DD (already tried)
                try:
                    datetime.strptime(val_str, fmt)
                    self._add_failure(
                        row, col, val_str, "date_format",
                        f"Wrong format '{label}' (should be YYYY-MM-DD)"
                    )
                    parsed = True
                    break
                except ValueError:
                    continue

            if not parsed:
                self._add_failure(
                    row, col, val_str, "valid_date",
                    f"'{val_str}' cannot be parsed as a valid date"
                )

    def validate_address(self) -> None:
        """Validate address: non-empty, 10-200 chars."""
        col = "address"
        if col not in self.df.columns:
            return

        for idx, val in self.df[col].items():
            row = idx + 1
            val_str = str(val).strip() if pd.notna(val) else ""

            if not val_str or val_str.lower() == "nan":
                self._add_failure(row, col, val, "not_null", "Missing address")
                continue

            if len(val_str) < 10:
                self._add_failure(
                    row, col, val_str, "min_length",
                    f"Address too short ({len(val_str)} chars, min 10)"
                )
            elif len(val_str) > 200:
                self._add_failure(
                    row, col, val_str, "max_length",
                    f"Address too long ({len(val_str)} chars, max 200)"
                )

    def validate_income(self) -> None:
        """Validate income: non-negative, <= 10M."""
        col = "income"
        if col not in self.df.columns:
            return

        for idx, val in self.df[col].items():
            row = idx + 1

            if pd.isna(val):
                self._add_failure(row, col, val, "not_null", "Missing income")
                continue

            try:
                num_val = float(val)
                if num_val < 0:
                    self._add_failure(
                        row, col, val, "non_negative", "Income must be non-negative"
                    )
                if num_val > 10_000_000:
                    self._add_failure(
                        row, col, val, "max_value", "Income exceeds $10M limit"
                    )
            except (ValueError, TypeError):
                self._add_failure(
                    row, col, val, "numeric", "Income must be numeric"
                )

    def validate_account_status(self) -> None:
        """Validate account_status: must be active, inactive, or suspended."""
        col = "account_status"
        if col not in self.df.columns:
            return

        for idx, val in self.df[col].items():
            row = idx + 1
            val_str = str(val).strip().lower() if pd.notna(val) else ""

            if not val_str or val_str == "nan":
                self._add_failure(
                    row, col, val, "not_null",
                    "Missing account_status (should be one of: active, inactive, suspended)"
                )
                continue

            if val_str not in VALID_ACCOUNT_STATUSES:
                self._add_failure(
                    row, col, val_str, "allowed_value",
                    f"'{val_str}' is not a valid status (allowed: active, inactive, suspended)"
                )

    # ------------------------------------------------------------------
    # Run all validations
    # ------------------------------------------------------------------
    def run_all_validations(self) -> dict:
        """Execute all validation rules and return results summary."""
        logger.info("Running schema validation...")
        self.failures.clear()
        self.passed_rows.clear()
        self.failed_rows.clear()

        self.validate_customer_id()
        self.validate_name("first_name")
        self.validate_name("last_name")
        self.validate_email()
        self.validate_phone()
        self.validate_date("date_of_birth")
        self.validate_date("created_date")
        self.validate_address()
        self.validate_income()
        self.validate_account_status()

        # Determine which rows passed vs. failed
        all_rows = set(range(1, self.total_rows + 1))
        self.failed_rows = {f.row for f in self.failures}
        self.passed_rows = all_rows - self.failed_rows

        return {
            "total_rows": self.total_rows,
            "passed": len(self.passed_rows),
            "failed": len(self.failed_rows),
            "failures": self.failures,
            "passed_rows": sorted(self.passed_rows),
            "failed_rows": sorted(self.failed_rows),
        }

    
    # Helpers
    
    def _add_failure(self, row: int, column: str, value: Any,
                      rule: str, message: str) -> None:
        """Record a validation failure."""
        self.failures.append(ValidationFailure(row, column, value, rule, message))

    def get_failures_by_column(self) -> dict[str, list[ValidationFailure]]:
        """Group validation failures by column name."""
        by_column: dict[str, list[ValidationFailure]] = {}
        for f in self.failures:
            by_column.setdefault(f.column, []).append(f)
        return by_column

    # ------------------------------------------------------------------
    # Report generation
    
    def generate_report(self, filepath=None) -> str:
        """Generate a formatted validation results report."""
        filepath = filepath or VALIDATION_RESULTS
        results = self.run_all_validations()
        by_column = self.get_failures_by_column()
        lines: list[str] = []

        lines.append("VALIDATION RESULTS")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Dataset: customers_raw.csv")
        lines.append("")
        lines.append(f"PASS: {results['passed']} rows passed all checks")
        lines.append(f"FAIL: {results['failed']} rows failed")
        lines.append(f"Failed rows: {results['failed_rows']}")
        lines.append("")

        lines.append("FAILURES BY COLUMN:")
        lines.append("-" * 40)

        for col in [
            "customer_id", "first_name", "last_name", "email", "phone",
            "date_of_birth", "created_date", "address", "income", "account_status"
        ]:
            if col in by_column:
                lines.append(f"\n  {col}:")
                for failure in by_column[col]:
                    display_val = failure.value
                    if pd.isna(display_val):
                        display_val = "(empty)"
                    lines.append(f"    - Row {failure.row}: {failure.message} [value: {display_val}]")

        lines.append("")
        lines.append("SUMMARY BY RULE TYPE:")
        lines.append("-" * 40)
        rule_counts: dict[str, int] = {}
        for f in self.failures:
            rule_counts[f.rule] = rule_counts.get(f.rule, 0) + 1
        for rule, count in sorted(rule_counts.items()):
            lines.append(f"  - {rule}: {count} failure(s)")
        lines.append("")

        report_text = "\n".join(lines)

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("Validation results saved to %s", filepath)
        return report_text
