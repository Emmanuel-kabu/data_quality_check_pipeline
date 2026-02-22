"""
Data Quality Profiler (Part 1)
==============================
Profiles raw data to assess completeness, data types, format issues,
uniqueness, invalid values, and categorical validity.

Generates a comprehensive data quality report.
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd
import numpy as np

from src.config import (
    EXPECTED_COLUMNS,
    VALID_ACCOUNT_STATUSES,
    DATE_PATTERNS,
    EMAIL_PATTERN,
    PHONE_PATTERNS,
    SCHEMA_RULES,
    DATA_QUALITY_REPORT,
    REPORTS_DIR,
)

logger = logging.getLogger(__name__)


class DataProfiler:
    """Profiles a DataFrame for data quality issues."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        self.total_rows = len(df)
        self.issues: list[dict[str, Any]] = []
        self.completeness: dict[str, dict] = {}
        self.type_checks: dict[str, dict] = {}
        self.format_issues: list[dict] = []
        self.severity_counts = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}

    # ------------------------------------------------------------------
    # Completeness analysis
    # ------------------------------------------------------------------
    def analyze_completeness(self) -> dict[str, dict]:
        """Calculate completeness percentage for each column."""
        logger.info("Analyzing completeness...")
        for col in self.df.columns:
            missing = self.df[col].isna().sum()
            # Also check for empty strings
            if pd.api.types.is_string_dtype(self.df[col]):
                empty_str = (self.df[col].fillna("").astype(str).str.strip() == "").sum()
                missing = max(missing, empty_str)

            present = self.total_rows - missing
            pct = round((present / self.total_rows) * 100, 1)

            self.completeness[col] = {
                "present": present,
                "missing": missing,
                "percentage": pct,
            }

            if missing > 0:
                self.issues.append({
                    "type": "Missing Values",
                    "column": col,
                    "description": f"{missing} missing value(s) ({100 - pct}% incomplete)",
                    "severity": "High" if col in ("customer_id", "email") else "Medium",
                    "rows": self.get_missing_row_indices(col),
                })
        return self.completeness

    def get_missing_row_indices(self, col: str) -> list[int]:
        """Return 1-based row indices where a column is missing or empty."""
        mask = self.df[col].isna()
        if pd.api.types.is_string_dtype(self.df[col]):
            mask = mask | (self.df[col].fillna("").astype(str).str.strip() == "")
        return (mask[mask].index + 1).tolist()

    # ------------------------------------------------------------------
    # Data type analysis
    # ------------------------------------------------------------------
    def analyze_data_types(self) -> dict[str, dict]:
        """Check whether each column matches its expected type."""
        logger.info("Analyzing data types...")
        for col, rules in SCHEMA_RULES.items():
            if col not in self.df.columns:
                continue

            expected = rules["type"]
            actual_dtype = str(self.df[col].dtype)
            match = False

            if expected == "integer":
                match = pd.api.types.is_integer_dtype(self.df[col])
            elif expected == "string":
                match = pd.api.types.is_string_dtype(self.df[col]) or self.df[col].dtype == object
            elif expected == "numeric":
                match = pd.api.types.is_numeric_dtype(self.df[col])
            elif expected == "date":
                match = pd.api.types.is_datetime64_any_dtype(self.df[col])
            elif expected == "categorical":
                match = pd.api.types.is_string_dtype(self.df[col]) or self.df[col].dtype == object

            status = "✓" if match else "✗"
            note = ""
            if not match:
                if expected == "date":
                    note = f"(should be DATE, stored as {actual_dtype})"
                else:
                    note = f"(expected {expected.upper()}, got {actual_dtype})"

            self.type_checks[col] = {
                "expected": expected.upper(),
                "actual": actual_dtype,
                "match": match,
                "status": status,
                "note": note,
            }

            if not match:
                self.issues.append({
                    "type": "Wrong Data Type",
                    "column": col,
                    "description": f"Expected {expected.upper()} but got {actual_dtype}",
                    "severity": "High",
                    "rows": self.get_missing_row_indices(col),
                })
        return self.type_checks

    # ------------------------------------------------------------------
    # Format analysis
    # ------------------------------------------------------------------
    def analyze_formats(self) -> list[dict]:
        """Detect inconsistent formats in phone numbers and dates."""
        logger.info("Analyzing formats...")
        self._analyze_phone_formats()
        self._analyze_date_formats("date_of_birth")
        self._analyze_date_formats("created_date")
        self._analyze_email_formats()
        self._analyze_name_case()
        return self.format_issues

    def _analyze_phone_formats(self) -> None:
        """Identify all phone number formats present."""
        if "phone" not in self.df.columns:
            return

        formats_found: dict[str, list] = {}
        for idx, val in self.df["phone"].dropna().items():
            val_str = str(val).strip()
            fmt = self.detect_phone_format(val_str)
            formats_found.setdefault(fmt, []).append({"row": idx + 1, "value": val_str})

        for fmt, examples in formats_found.items():
            issue = {
                "type": "Phone Format",
                "column": "phone",
                "format": fmt,
                "count": len(examples),
                "examples": examples[:3],
            }
            self.format_issues.append(issue)

            if fmt != "XXX-XXX-XXXX":
                self.issues.append({
                    "type": "Non-standard Phone Format",
                    "column": "phone",
                    "description": f"Format '{fmt}' found in {len(examples)} row(s)",
                    "severity": "Medium",
                    "rows": [e["row"] for e in examples],
                })

    @staticmethod
    def detect_phone_format(phone: str) -> str:
        """Return a human-readable description of the phone format."""
        import re
        if re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
            return "XXX-XXX-XXXX"
        if re.match(r"^\(\d{3}\)\s*\d{3}-\d{4}$", phone):
            return "(XXX) XXX-XXXX"
        if re.match(r"^\d{3}\.\d{3}\.\d{4}$", phone):
            return "XXX.XXX.XXXX"
        if re.match(r"^\d{10}$", phone):
            return "XXXXXXXXXX (no separators)"
        return "Unknown"

    def _analyze_date_formats(self, col: str) -> None:
        """Identify date format variants and invalid dates in a column."""
        if col not in self.df.columns:
            return

        for idx, val in self.df[col].dropna().items():
            val_str = str(val).strip()
            if val_str.lower() in ("", "nan"):
                continue

            parsed = False
            for fmt, label in DATE_PATTERNS:
                try:
                    datetime.strptime(val_str, fmt)
                    if fmt != "%Y-%m-%d":
                        self.format_issues.append({
                            "type": "Date Format",
                            "column": col,
                            "format": label,
                            "row": idx + 1,
                            "value": val_str,
                        })
                        self.issues.append({
                            "type": "Non-standard Date Format",
                            "column": col,
                            "description": f"Row {idx + 1}: '{val_str}' uses {label} instead of YYYY-MM-DD",
                            "severity": "Medium",
                            "rows": [idx + 1],
                        })
                    parsed = True
                    break
                except ValueError:
                    continue

            if not parsed:
                self.format_issues.append({
                    "type": "Invalid Date",
                    "column": col,
                    "row": idx + 1,
                    "value": val_str,
                })
                self.issues.append({
                    "type": "Invalid Date Value",
                    "column": col,
                    "description": f"Row {idx + 1}: '{val_str}' is not a valid date",
                    "severity": "Critical",
                    "rows": [idx + 1],
                })

    def _analyze_email_formats(self) -> None:
        """Check for uppercase emails (case normalization needed)."""
        if "email" not in self.df.columns:
            return
        for idx, val in self.df["email"].dropna().items():
            val_str = str(val).strip()
            if val_str != val_str.lower():
                self.issues.append({
                    "type": "Email Case Issue",
                    "column": "email",
                    "description": f"Row {idx + 1}: '{val_str}' contains uppercase characters",
                    "severity": "Low",
                    "rows": [idx + 1],
                })

    def _analyze_name_case(self) -> None:
        """Detect names not in title case."""
        for col in ("first_name", "last_name"):
            if col not in self.df.columns:
                continue
            for idx, val in self.df[col].dropna().items():
                val_str = str(val).strip()
                if val_str and val_str != val_str.title():
                    self.issues.append({
                        "type": "Name Case Issue",
                        "column": col,
                        "description": f"Row {idx + 1}: '{val_str}' is not title case",
                        "severity": "Low",
                        "rows": [idx + 1],
                    })

    # ------------------------------------------------------------------
    # Uniqueness analysis
    # ------------------------------------------------------------------
    def analyze_uniqueness(self) -> dict[str, bool]:
        """Check uniqueness for columns that require it."""
        logger.info("Analyzing uniqueness...")
        results = {}
        for col, rules in SCHEMA_RULES.items():
            if rules.get("unique") and col in self.df.columns:
                is_unique = self.df[col].is_unique
                results[col] = is_unique
                if not is_unique:
                    dupes = self.df[col][self.df[col].duplicated(keep=False)]
                    self.issues.append({
                        "type": "Duplicate Values",
                        "column": col,
                        "description": f"Found {len(dupes)} duplicate entries",
                        "severity": "Critical",
                        "rows": (dupes.index + 1).tolist(),
                    })
        return results

    # ------------------------------------------------------------------
    # Categorical validity
    # ------------------------------------------------------------------
    def analyze_categorical_validity(self) -> dict:
        """Check that categorical columns only contain valid values."""
        logger.info("Analyzing categorical validity...")
        results = {}
        if "account_status" in self.df.columns:
            values = self.df["account_status"].dropna().str.strip().str.lower().unique()
            invalid = [v for v in values if v not in VALID_ACCOUNT_STATUSES and v != ""]
            results["account_status"] = {
                "valid_values": list(VALID_ACCOUNT_STATUSES),
                "found_values": list(values),
                "invalid_values": invalid,
            }
            if invalid:
                self.issues.append({
                    "type": "Invalid Categorical Value",
                    "column": "account_status",
                    "description": f"Invalid values: {invalid}",
                    "severity": "High",
                    "rows": [],
                })
        return results

    # ------------------------------------------------------------------
    # Run full profile
    # ------------------------------------------------------------------
    def run_full_profile(self) -> dict:
        """Execute all profiling analyses and return combined results."""
        logger.info("Running full data quality profile...")
        completeness = self.analyze_completeness()
        types = self.analyze_data_types()
        formats = self.analyze_formats()
        uniqueness = self.analyze_uniqueness()
        categorical = self.analyze_categorical_validity()

        # Tally severity counts
        for issue in self.issues:
            sev = issue.get("severity", "Medium")
            self.severity_counts[sev] = self.severity_counts.get(sev, 0) + 1

        return {
            "completeness": completeness,
            "types": types,
            "formats": formats,
            "uniqueness": uniqueness,
            "categorical": categorical,
            "issues": self.issues,
            "severity": self.severity_counts,
        }

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------
    def generate_report(self, filepath=None) -> str:
        """Generate a formatted data quality report and save to file."""
        filepath = filepath or DATA_QUALITY_REPORT
        results = self.run_full_profile()
        lines: list[str] = []

        lines.append("DATA QUALITY PROFILE REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Dataset: customers_raw.csv")
        lines.append(f"Total Rows: {self.total_rows}")
        lines.append(f"Total Columns: {len(self.df.columns)}")
        lines.append("")

        # Completeness section
        lines.append("COMPLETENESS:")
        lines.append("-" * 40)
        for col, info in results["completeness"].items():
            missing_str = ""
            if info["missing"] > 0:
                missing_str = f" ({info['missing']} missing)"
            lines.append(f"  - {col}: {info['percentage']}%{missing_str}")
        lines.append("")

        # Data types section
        lines.append("DATA TYPES:")
        lines.append("-" * 40)
        for col, info in results["types"].items():
            note = f" {info['note']}" if info["note"] else ""
            lines.append(f"  - {col}: {info['expected']} {info['status']}{note}")
        lines.append("")

        # Quality issues section
        lines.append("QUALITY ISSUES:")
        lines.append("-" * 40)
        for i, issue in enumerate(results["issues"], 1):
            rows_str = ""
            if issue["rows"]:
                rows_str = f", Rows: {issue['rows']}"
            lines.append(
                f"  {i}. [{issue['severity']}] {issue['type']} in '{issue['column']}': "
                f"{issue['description']}{rows_str}"
            )
        lines.append("")

        # Severity summary
        lines.append("SEVERITY SUMMARY:")
        lines.append("-" * 40)
        lines.append(f"  - Critical (blocks processing): {self.severity_counts.get('Critical', 0)}")
        lines.append(f"  - High (data incorrect):        {self.severity_counts.get('High', 0)}")
        lines.append(f"  - Medium (needs cleaning):      {self.severity_counts.get('Medium', 0)}")
        lines.append(f"  - Low (cosmetic):               {self.severity_counts.get('Low', 0)}")
        lines.append("")

        # Impact summary
        lines.append("ESTIMATED IMPACT:")
        lines.append("-" * 40)
        for col, info in results["completeness"].items():
            if info["missing"] > 0:
                pct = round((info["missing"] / self.total_rows) * 100)
                lines.append(
                    f"  - {info['missing']} rows have missing {col} = "
                    f"{pct}% incomplete"
                )
        lines.append("")

        report_text = "\n".join(lines)

        # Ensure output directory exists
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("Data quality report saved to %s", filepath)
        return report_text
