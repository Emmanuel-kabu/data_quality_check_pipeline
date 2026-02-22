"""
Data Cleaner Module (Part 4)
============================
Normalizes formats, handles missing values, and fixes data quality
issues identified during profiling and validation.

Generates a cleaning log and outputs a cleaned CSV.
"""

import logging
import re
from datetime import datetime
from typing import Any

import pandas as pd
import numpy as np

from src.config import (
    DATE_PATTERNS,
    VALID_ACCOUNT_STATUSES,
    MISSING_VALUE_STRATEGIES,
    CLEANED_CSV,
    CLEANING_LOG,
    REPORTS_DIR,
    DATA_DIR,
)

logger = logging.getLogger(__name__)


class DataCleaner:
    """Cleans and normalizes a customer DataFrame."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        self.original_df = df.copy()
        self.total_rows = len(df)
        self.actions: list[dict[str, Any]] = []
        self.normalization_counts = {
            "phone_format": 0,
            "date_format": 0,
            "name_case": 0,
            "email_case": 0,
        }
        self.missing_value_fills: dict[str, list[int]] = {}

    # ------------------------------------------------------------------
    # Phone normalization
    # ------------------------------------------------------------------
    def normalize_phones(self) -> pd.DataFrame:
        """Convert all phone formats to XXX-XXX-XXXX."""
        logger.info("Normalizing phone numbers...")
        col = "phone"
        if col not in self.df.columns:
            return self.df

        affected_rows = []
        for idx, val in self.df[col].items():
            if pd.isna(val):
                continue
            val_str = str(val).strip()
            digits = re.sub(r"\D", "", val_str)

            if len(digits) == 10:
                normalized = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
                if normalized != val_str:
                    self.df.at[idx, col] = normalized
                    affected_rows.append(idx + 1)
                    self.normalization_counts["phone_format"] += 1

        if affected_rows:
            self.actions.append({
                "category": "Normalization",
                "action": f"Phone format: Converted to XXX-XXX-XXXX ({len(affected_rows)} rows affected)",
                "rows": affected_rows,
            })

        return self.df

    # ------------------------------------------------------------------
    # Date normalization
    # ------------------------------------------------------------------
    def normalize_dates(self) -> pd.DataFrame:
        """Convert all date columns to YYYY-MM-DD format."""
        logger.info("Normalizing date formats...")
        for col in ("date_of_birth", "created_date"):
            if col not in self.df.columns:
                continue
            self._normalize_date_column(col)
        return self.df

    def _normalize_date_column(self, col: str) -> None:
        """Normalize a single date column."""
        affected_rows = []
        for idx, val in self.df[col].items():
            if pd.isna(val):
                continue
            val_str = str(val).strip()

            if val_str.lower() in ("invalid_date", "nan", ""):
                self.df.at[idx, col] = np.nan  # Mark as missing for fill step
                continue

            # Already in correct format?
            try:
                datetime.strptime(val_str, "%Y-%m-%d")
                continue
            except ValueError:
                pass

            # Try other formats
            for fmt, label in DATE_PATTERNS[1:]:
                try:
                    parsed = datetime.strptime(val_str, fmt)
                    self.df.at[idx, col] = parsed.strftime("%Y-%m-%d")
                    affected_rows.append(idx + 1)
                    self.normalization_counts["date_format"] += 1
                    break
                except ValueError:
                    continue

        if affected_rows:
            self.actions.append({
                "category": "Normalization",
                "action": f"Date format ({col}): Converted to YYYY-MM-DD ({len(affected_rows)} rows affected)",
                "rows": affected_rows,
            })

    # ------------------------------------------------------------------
    # Name normalization
    # ------------------------------------------------------------------
    def normalize_names(self) -> pd.DataFrame:
        """Apply title case to first_name and last_name."""
        logger.info("Normalizing name casing...")
        for col in ("first_name", "last_name"):
            if col not in self.df.columns:
                continue
            affected_rows = []
            for idx, val in self.df[col].items():
                if pd.isna(val):
                    continue
                val_str = str(val).strip()
                if not val_str or val_str.lower() == "nan":
                    continue
                title_val = val_str.title()
                if title_val != val_str:
                    self.df.at[idx, col] = title_val
                    affected_rows.append(idx + 1)
                    self.normalization_counts["name_case"] += 1

            if affected_rows:
                self.actions.append({
                    "category": "Normalization",
                    "action": f"Name case ({col}): Applied title case ({len(affected_rows)} rows affected)",
                    "rows": affected_rows,
                })
        return self.df

    # ------------------------------------------------------------------
    # Email normalization
    # ------------------------------------------------------------------
    def normalize_emails(self) -> pd.DataFrame:
        """Lowercase all email addresses."""
        logger.info("Normalizing email casing...")
        col = "email"
        if col not in self.df.columns:
            return self.df

        affected_rows = []
        for idx, val in self.df[col].items():
            if pd.isna(val):
                continue
            val_str = str(val).strip()
            lower_val = val_str.lower()
            if lower_val != val_str:
                self.df.at[idx, col] = lower_val
                affected_rows.append(idx + 1)
                self.normalization_counts["email_case"] += 1

        if affected_rows:
            self.actions.append({
                "category": "Normalization",
                "action": f"Email case: Lowered to standard format ({len(affected_rows)} rows affected)",
                "rows": affected_rows,
            })
        return self.df

    # ------------------------------------------------------------------
    # Handle missing values
    # ------------------------------------------------------------------
    def handle_missing_values(self) -> pd.DataFrame:
        """Fill missing values according to the configured strategy."""
        logger.info("Handling missing values...")

        for col, fill_value in MISSING_VALUE_STRATEGIES.items():
            if col not in self.df.columns:
                continue

            if pd.api.types.is_string_dtype(self.df[col]) or self.df[col].dtype == object:
                mask = self.df[col].isna() | (self.df[col].fillna("").astype(str).str.strip().isin(["", "nan"]))
            else:
                mask = self.df[col].isna()

            missing_rows = (mask[mask].index + 1).tolist()

            if missing_rows:
                self.df.loc[mask, col] = fill_value
                self.missing_value_fills[col] = missing_rows
                self.actions.append({
                    "category": "Missing Values",
                    "action": f"{col}: {len(missing_rows)} row(s) missing -> filled with '{fill_value}'",
                    "rows": missing_rows,
                })

        return self.df

    # ------------------------------------------------------------------
    # Strip whitespace
    # ------------------------------------------------------------------
    def strip_whitespace(self) -> pd.DataFrame:
        """Remove leading/trailing whitespace from all string columns."""
        logger.info("Stripping whitespace...")
        for col in self.df.columns:
            if pd.api.types.is_string_dtype(self.df[col]) or self.df[col].dtype == object:
                self.df[col] = self.df[col].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )
        return self.df

    # ------------------------------------------------------------------
    # Run full cleaning
    # ------------------------------------------------------------------
    def run_full_cleaning(self) -> pd.DataFrame:
        """Execute the complete cleaning pipeline in the correct order."""
        logger.info("Running full data cleaning pipeline...")
        self.strip_whitespace()
        self.normalize_phones()
        self.normalize_dates()
        self.normalize_names()
        self.normalize_emails()
        self.handle_missing_values()
        return self.df

    # ------------------------------------------------------------------
    # Save cleaned data
    # ------------------------------------------------------------------
    def save_cleaned_data(self, filepath=None) -> str:
        """Save the cleaned DataFrame to CSV."""
        filepath = filepath or CLEANED_CSV
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(filepath, index=False)
        logger.info("Cleaned data saved to %s", filepath)
        return str(filepath)

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------
    def generate_report(self, validation_before: int = 0,
                        validation_after: int = 0,
                        filepath=None) -> str:
        """Generate a formatted cleaning log report."""
        filepath = filepath or CLEANING_LOG
        lines: list[str] = []

        lines.append("DATA CLEANING LOG")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Dataset: customers_raw.csv -> customers_cleaned.csv")
        lines.append("")

        lines.append("ACTIONS TAKEN:")
        lines.append("-" * 40)

        # Group by category
        categories: dict[str, list] = {}
        for action in self.actions:
            cat = action["category"]
            categories.setdefault(cat, []).append(action)

        for cat, cat_actions in categories.items():
            lines.append(f"\n  {cat}:")
            for act in cat_actions:
                lines.append(f"    - {act['action']}")
                if act.get("rows"):
                    lines.append(f"      Affected rows: {act['rows']}")

        lines.append("")
        lines.append("VALIDATION AFTER CLEANING:")
        lines.append("-" * 40)
        lines.append(f"  - Before cleaning: {validation_before} rows failed validation")
        lines.append(f"  - After cleaning:  {validation_after} rows failed validation")
        status = "PASS" if validation_after == 0 else "PARTIAL PASS"
        lines.append(f"  - Status: {status}")
        lines.append("")

        lines.append("OUTPUT:")
        lines.append("-" * 40)
        lines.append(f"  - File: customers_cleaned.csv")
        lines.append(f"  - Rows: {len(self.df)}")
        lines.append(f"  - Columns: {len(self.df.columns)}")
        lines.append("")

        report_text = "\n".join(lines)

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("Cleaning log saved to %s", filepath)
        return report_text
