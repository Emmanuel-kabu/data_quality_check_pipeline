"""
PII Detection Module (Part 2)
==============================
Identifies personally identifiable information in the dataset using
regex pattern matching and column classification.

Generates a PII detection report with risk assessment.
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd

from src.config import (
    EMAIL_PATTERN,
    PHONE_PATTERNS,
    PII_COLUMNS,
    PII_DETECTION_REPORT,
    REPORTS_DIR,
)

logger = logging.getLogger(__name__)


class PIIDetector:
    """Detects and catalogues PII exposure in a DataFrame."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        self.total_rows = len(df)
        self.pii_findings: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Email detection
    # ------------------------------------------------------------------
    def detect_emails(self) -> dict:
        """Find rows that contain valid email addresses."""
        logger.info("Detecting email PII...")
        results = {"column": "email", "rows_with_pii": [], "count": 0, "examples": []}

        if "email" not in self.df.columns:
            return results

        for idx, val in self.df["email"].dropna().items():
            val_str = str(val).strip()
            if EMAIL_PATTERN.match(val_str):
                results["rows_with_pii"].append(idx + 1)
                if len(results["examples"]) < 3:
                    results["examples"].append({"row": idx + 1, "value": val_str})

        results["count"] = len(results["rows_with_pii"])
        self.pii_findings["email"] = results
        return results

    # ------------------------------------------------------------------
    # Phone detection
    # ------------------------------------------------------------------
    def detect_phones(self) -> dict:
        """Find rows that contain phone numbers in any format."""
        logger.info("Detecting phone PII...")
        results = {"column": "phone", "rows_with_pii": [], "count": 0, "examples": []}

        if "phone" not in self.df.columns:
            return results

        for idx, val in self.df["phone"].dropna().items():
            val_str = str(val).strip()
            for pattern in PHONE_PATTERNS:
                if pattern.search(val_str):
                    results["rows_with_pii"].append(idx + 1)
                    if len(results["examples"]) < 3:
                        results["examples"].append({"row": idx + 1, "value": val_str})
                    break

        results["count"] = len(results["rows_with_pii"])
        self.pii_findings["phone"] = results
        return results

    # ------------------------------------------------------------------
    # Name detection
    # ------------------------------------------------------------------
    def detect_names(self) -> dict:
        """Find rows with non-empty name fields (direct identifiers)."""
        logger.info("Detecting name PII...")
        combined_results = {
            "columns": ["first_name", "last_name"],
            "rows_with_pii": [],
            "count": 0,
            "examples": [],
        }

        for idx in range(self.total_rows):
            first = self.df.at[idx, "first_name"] if "first_name" in self.df.columns else None
            last = self.df.at[idx, "last_name"] if "last_name" in self.df.columns else None

            has_name = False
            if pd.notna(first) and str(first).strip():
                has_name = True
            if pd.notna(last) and str(last).strip():
                has_name = True

            if has_name:
                combined_results["rows_with_pii"].append(idx + 1)
                if len(combined_results["examples"]) < 3:
                    combined_results["examples"].append({
                        "row": idx + 1,
                        "first_name": str(first).strip() if pd.notna(first) else "",
                        "last_name": str(last).strip() if pd.notna(last) else "",
                    })

        combined_results["count"] = len(combined_results["rows_with_pii"])
        self.pii_findings["names"] = combined_results
        return combined_results

    # ------------------------------------------------------------------
    # Address detection
    # ------------------------------------------------------------------
    def detect_addresses(self) -> dict:
        """Find rows with non-empty physical addresses."""
        logger.info("Detecting address PII...")
        results = {"column": "address", "rows_with_pii": [], "count": 0, "examples": []}

        if "address" not in self.df.columns:
            return results

        for idx, val in self.df["address"].items():
            if pd.notna(val) and str(val).strip():
                results["rows_with_pii"].append(idx + 1)
                if len(results["examples"]) < 3:
                    results["examples"].append({"row": idx + 1, "value": str(val).strip()})

        results["count"] = len(results["rows_with_pii"])
        self.pii_findings["address"] = results
        return results

    # ------------------------------------------------------------------
    # Date of birth detection
    # ------------------------------------------------------------------
    def detect_dob(self) -> dict:
        """Find rows with date of birth values (sensitive personal data)."""
        logger.info("Detecting date-of-birth PII...")
        results = {
            "column": "date_of_birth",
            "rows_with_pii": [],
            "count": 0,
            "examples": [],
        }

        if "date_of_birth" not in self.df.columns:
            return results

        for idx, val in self.df["date_of_birth"].items():
            val_str = str(val).strip() if pd.notna(val) else ""
            if val_str and val_str.lower() not in ("nan", "", "invalid_date"):
                results["rows_with_pii"].append(idx + 1)
                if len(results["examples"]) < 3:
                    results["examples"].append({"row": idx + 1, "value": val_str})

        results["count"] = len(results["rows_with_pii"])
        self.pii_findings["date_of_birth"] = results
        return results

    # ------------------------------------------------------------------
    # Run full detection
    # ------------------------------------------------------------------
    def run_full_detection(self) -> dict:
        """Execute all PII detection checks."""
        logger.info("Running full PII detection scan...")
        self.detect_emails()
        self.detect_phones()
        self.detect_names()
        self.detect_addresses()
        self.detect_dob()
        return self.pii_findings

    # ------------------------------------------------------------------
    # Risk assessment
    # ------------------------------------------------------------------
    def assess_risk(self) -> dict:
        """Generate a risk assessment based on detected PII."""
        if not self.pii_findings:
            self.run_full_detection()

        risk = {
            "high_risk": [],
            "medium_risk": [],
            "exposure_scenarios": [],
        }

        # Classify risk levels
        for category, config in PII_COLUMNS.items():
            if category in ("direct_identifiers", "contact_info", "sensitive_personal"):
                risk["high_risk"].append({
                    "category": category,
                    "columns": config,
                    "reason": "Direct or quasi-identifier — enables re-identification",
                })
            elif category == "financial":
                risk["medium_risk"].append({
                    "category": category,
                    "columns": config,
                    "reason": "Financial sensitivity — income data",
                })

        # Exposure scenarios
        risk["exposure_scenarios"] = [
            "Phish customers using exposed email addresses",
            "Spoof identities using names + DOB + address combinations",
            "Social-engineer targets using phone numbers",
            "Infer financial status from income data",
            "Build comprehensive profiles combining all PII fields",
        ]

        return risk

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------
    def generate_report(self, filepath=None) -> str:
        """Generate a formatted PII detection report and save to file."""
        filepath = filepath or PII_DETECTION_REPORT
        findings = self.run_full_detection()
        risk = self.assess_risk()
        lines: list[str] = []

        lines.append("PII DETECTION REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Dataset: customers_raw.csv")
        lines.append(f"Total Rows: {self.total_rows}")
        lines.append("")

        # Risk assessment
        lines.append("RISK ASSESSMENT:")
        lines.append("-" * 40)
        lines.append("  HIGH RISK:")
        for item in risk["high_risk"]:
            cols = ", ".join(item["columns"])
            lines.append(f"    - {cols}")
            lines.append(f"      Reason: {item['reason']}")
        lines.append("  MEDIUM RISK:")
        for item in risk["medium_risk"]:
            cols = ", ".join(item["columns"])
            lines.append(f"    - {cols}")
            lines.append(f"      Reason: {item['reason']}")
        lines.append("")

        # Detected PII summary
        lines.append("DETECTED PII:")
        lines.append("-" * 40)
        pii_map = {
            "email": "Emails",
            "phone": "Phone numbers",
            "names": "Names (first + last)",
            "address": "Addresses",
            "date_of_birth": "Dates of birth",
        }
        for key, label in pii_map.items():
            if key in findings:
                count = findings[key]["count"]
                pct = round((count / self.total_rows) * 100)
                lines.append(f"  - {label} found: {count} ({pct}%)")
        lines.append("")

        # Detailed findings with examples
        lines.append("DETAILED FINDINGS:")
        lines.append("-" * 40)
        for key, label in pii_map.items():
            if key in findings and findings[key]["count"] > 0:
                lines.append(f"  {label}:")
                for ex in findings[key].get("examples", []):
                    if isinstance(ex.get("value"), str):
                        lines.append(f"    Row {ex['row']}: {ex['value']}")
                    else:
                        first = ex.get("first_name", "")
                        last = ex.get("last_name", "")
                        lines.append(f"    Row {ex['row']}: {first} {last}")
        lines.append("")

        # Exposure risk
        lines.append("EXPOSURE RISK:")
        lines.append("-" * 40)
        lines.append("  If this dataset were breached, attackers could:")
        for scenario in risk["exposure_scenarios"]:
            lines.append(f"    - {scenario}")
        lines.append("")

        # Mitigation
        lines.append("MITIGATION:")
        lines.append("-" * 40)
        lines.append("  - Mask all PII before sharing with analytics teams")
        lines.append("  - Encrypt PII at rest and in transit")
        lines.append("  - Apply role-based access control to raw data")
        lines.append("  - Implement data retention policies")
        lines.append("  - Log all access to PII columns for auditing")
        lines.append("")

        report_text = "\n".join(lines)

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("PII detection report saved to %s", filepath)
        return report_text
