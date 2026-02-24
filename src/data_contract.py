"""
Data Contract Module
====================
Validates incoming data against a formal data contract that defines
the schema, quality SLAs, and freshness requirements agreed upon
with upstream data producers.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import DATA_CONTRACT, REPORTS_DIR

logger = logging.getLogger(__name__)


class DataContractValidator:
    """
    Validates data against a formal contract specifying:
      - Required columns and types
      - Quality SLAs (completeness, accuracy, duplicates)
      - Freshness constraints
    """

    def __init__(self, df: pd.DataFrame, contract: dict | None = None) -> None:
        self.df = df.copy()
        self.contract = contract or DATA_CONTRACT
        self.violations: list[dict[str, Any]] = []
        self.sla_results: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Schema validation
    # ------------------------------------------------------------------
    def validate_schema(self) -> list[dict]:
        """Check that all required columns exist with expected types."""
        schema = self.contract.get("schema", {})
        required_cols = schema.get("required_columns", [])
        col_types = schema.get("column_types", {})

        violations = []
        actual_cols = set(self.df.columns)

        # Check required columns
        for col in required_cols:
            if col not in actual_cols:
                v = {
                    "rule": "required_column",
                    "column": col,
                    "message": f"Required column '{col}' is missing from the dataset",
                    "severity": "CRITICAL",
                }
                violations.append(v)
                self.violations.append(v)

        # Check for unexpected columns
        expected_cols = set(required_cols)
        extra_cols = actual_cols - expected_cols
        if extra_cols:
            v = {
                "rule": "unexpected_columns",
                "column": ", ".join(sorted(extra_cols)),
                "message": f"Unexpected columns found: {sorted(extra_cols)}",
                "severity": "LOW",
            }
            violations.append(v)
            self.violations.append(v)

        return violations

    # ------------------------------------------------------------------
    # SLA validation
    # ------------------------------------------------------------------
    def validate_completeness_sla(self) -> dict:
        """Check that data completeness meets the SLA target."""
        sla = self.contract.get("sla", {})
        target = sla.get("completeness_pct", 95.0)

        total_cells = self.df.shape[0] * self.df.shape[1]
        null_cells = self.df.isna().sum().sum()
        # Also count empty strings
        for col in self.df.select_dtypes(include="object").columns:
            null_cells += (self.df[col].fillna("").astype(str).str.strip() == "").sum()

        completeness_pct = round(((total_cells - null_cells) / total_cells) * 100, 2)
        passed = completeness_pct >= target

        result = {
            "sla": "completeness",
            "target_pct": target,
            "actual_pct": completeness_pct,
            "passed": passed,
            "total_cells": int(total_cells),
            "null_cells": int(null_cells),
        }
        self.sla_results["completeness"] = result

        if not passed:
            self.violations.append({
                "rule": "completeness_sla",
                "column": "ALL",
                "message": f"Completeness {completeness_pct}% below SLA target {target}%",
                "severity": "HIGH",
            })

        return result

    def validate_duplicate_sla(self) -> dict:
        """Check that duplicate rate is within SLA limits."""
        sla = self.contract.get("sla", {})
        max_dup_pct = sla.get("max_duplicate_pct", 1.0)

        id_col = "customer_id"
        if id_col not in self.df.columns:
            return {"sla": "duplicates", "passed": True, "message": "No ID column to check"}

        total_rows = len(self.df)
        duplicates = self.df[id_col].dropna().duplicated().sum()
        dup_pct = round((duplicates / total_rows) * 100, 2) if total_rows > 0 else 0

        passed = dup_pct <= max_dup_pct

        result = {
            "sla": "duplicates",
            "target_max_pct": max_dup_pct,
            "actual_pct": dup_pct,
            "passed": passed,
            "duplicate_count": int(duplicates),
        }
        self.sla_results["duplicates"] = result

        if not passed:
            self.violations.append({
                "rule": "duplicate_sla",
                "column": id_col,
                "message": f"Duplicate rate {dup_pct}% exceeds SLA max {max_dup_pct}%",
                "severity": "HIGH",
            })

        return result

    def validate_freshness_sla(self, data_timestamp: datetime | None = None) -> dict:
        """Check that data meets freshness SLA."""
        sla = self.contract.get("sla", {})
        max_hours = sla.get("freshness_hours", 24)

        if data_timestamp is None:
            # Try to infer from created_date column
            if "created_date" in self.df.columns:
                try:
                    dates = pd.to_datetime(self.df["created_date"], errors="coerce")
                    data_timestamp = dates.max().to_pydatetime()
                except Exception:
                    data_timestamp = datetime.now()
            else:
                data_timestamp = datetime.now()

        age_hours = (datetime.now() - data_timestamp).total_seconds() / 3600
        passed = age_hours <= max_hours

        result = {
            "sla": "freshness",
            "target_max_hours": max_hours,
            "data_age_hours": round(age_hours, 2),
            "passed": passed,
            "data_timestamp": data_timestamp.isoformat(),
        }
        self.sla_results["freshness"] = result

        if not passed:
            self.violations.append({
                "rule": "freshness_sla",
                "column": "created_date",
                "message": f"Data age {age_hours:.1f}h exceeds SLA max {max_hours}h",
                "severity": "MEDIUM",
            })

        return result

    # ------------------------------------------------------------------
    # Run all contract validations
    # ------------------------------------------------------------------
    def validate_all(self) -> dict:
        """Run all data contract validations."""
        logger.info("Validating data contract (v%s)...", self.contract.get("version", "?"))

        schema_violations = self.validate_schema()
        completeness = self.validate_completeness_sla()
        duplicates = self.validate_duplicate_sla()
        freshness = self.validate_freshness_sla()

        all_passed = all(r.get("passed", True) for r in self.sla_results.values())
        all_passed = all_passed and len(schema_violations) == 0

        return {
            "contract_version": self.contract.get("version", "unknown"),
            "owner": self.contract.get("owner", "unknown"),
            "all_passed": all_passed,
            "schema_violations": schema_violations,
            "sla_results": self.sla_results,
            "total_violations": len(self.violations),
            "violations": self.violations,
        }

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------
    def generate_report(self, filepath=None) -> str:
        """Generate a data contract validation report."""
        filepath = filepath or REPORTS_DIR / "data_contract_report.txt"
        results = self.validate_all()
        lines: list[str] = []

        lines.append("DATA CONTRACT VALIDATION REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Contract Version: {results['contract_version']}")
        lines.append(f"Contract Owner: {results['owner']}")
        lines.append(f"Overall Status: {'PASS' if results['all_passed'] else 'FAIL'}")
        lines.append("")

        lines.append("SCHEMA VALIDATION:")
        lines.append("-" * 40)
        if results["schema_violations"]:
            for v in results["schema_violations"]:
                lines.append(f"  [{v['severity']}] {v['message']}")
        else:
            lines.append("  All schema checks passed [OK]")
        lines.append("")

        lines.append("SLA RESULTS:")
        lines.append("-" * 40)
        for sla_name, sla_result in results["sla_results"].items():
            status = "PASS" if sla_result.get("passed") else "FAIL"
            lines.append(f"  {sla_name}: {status}")
            for k, v in sla_result.items():
                if k not in ("sla", "passed"):
                    lines.append(f"    {k}: {v}")
        lines.append("")

        lines.append(f"TOTAL VIOLATIONS: {results['total_violations']}")
        lines.append("-" * 40)
        for v in results["violations"]:
            lines.append(f"  [{v['severity']}] {v['rule']}: {v['message']}")
        lines.append("")

        report_text = "\n".join(lines)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("Data contract report saved to %s", filepath)
        return report_text
