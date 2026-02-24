"""
Statistical Validator Module
============================
Detects outliers in numeric fields (income) and unusual distributions
in date fields using IQR and z-score methods.

Complements the rule-based DataValidator with statistical analysis.
"""

import logging
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from src.config import STATISTICAL_CONFIG, REPORTS_DIR

logger = logging.getLogger(__name__)


class StatisticalValidator:
    """Performs statistical validation — outlier and distribution checks."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        self.total_rows = len(df)
        self.outliers: dict[str, list[dict]] = {}
        self.distribution_issues: list[dict] = []
        self.stats_summary: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # IQR-based outlier detection
    # ------------------------------------------------------------------
    def detect_outliers_iqr(self, col: str, multiplier: float = 1.5) -> list[dict]:
        """Detect outliers using the Interquartile Range method."""
        if col not in self.df.columns:
            return []

        numeric_vals = pd.to_numeric(self.df[col], errors="coerce").dropna()
        if len(numeric_vals) < 4:
            return []

        q1 = numeric_vals.quantile(0.25)
        q3 = numeric_vals.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        outlier_records = []
        for idx, val in numeric_vals.items():
            if val < lower_bound or val > upper_bound:
                outlier_records.append({
                    "row": int(idx) + 1,
                    "column": col,
                    "value": float(val),
                    "lower_bound": float(lower_bound),
                    "upper_bound": float(upper_bound),
                    "method": "IQR",
                    "severity": "HIGH" if abs(val - numeric_vals.median()) > 3 * iqr else "MEDIUM",
                })

        self.stats_summary[col] = {
            "mean": float(numeric_vals.mean()),
            "median": float(numeric_vals.median()),
            "std": float(numeric_vals.std()),
            "q1": float(q1),
            "q3": float(q3),
            "iqr": float(iqr),
            "lower_bound": float(lower_bound),
            "upper_bound": float(upper_bound),
            "outlier_count": len(outlier_records),
        }

        return outlier_records

    # ------------------------------------------------------------------
    # Z-score outlier detection
    # ------------------------------------------------------------------
    def detect_outliers_zscore(self, col: str, threshold: float = 3.0) -> list[dict]:
        """Detect outliers using z-score method."""
        if col not in self.df.columns:
            return []

        numeric_vals = pd.to_numeric(self.df[col], errors="coerce").dropna()
        if len(numeric_vals) < 2:
            return []

        mean = numeric_vals.mean()
        std = numeric_vals.std()
        if std == 0:
            return []

        outlier_records = []
        for idx, val in numeric_vals.items():
            z = abs((val - mean) / std)
            if z > threshold:
                outlier_records.append({
                    "row": int(idx) + 1,
                    "column": col,
                    "value": float(val),
                    "z_score": float(z),
                    "threshold": threshold,
                    "method": "z-score",
                    "severity": "HIGH" if z > threshold * 1.5 else "MEDIUM",
                })

        return outlier_records

    # ------------------------------------------------------------------
    # Date distribution analysis
    # ------------------------------------------------------------------
    def analyze_date_distribution(self, col: str) -> list[dict]:
        """Detect unusual date distributions — future dates, extreme ages, clustering."""
        if col not in self.df.columns:
            return []

        config = STATISTICAL_CONFIG.get("dates", {})
        min_year = config.get("min_year", 1920)
        max_year = config.get("max_year", 2026)

        issues = []
        parsed_dates = []

        for idx, val in self.df[col].items():
            if pd.isna(val):
                continue
            val_str = str(val).strip()
            if val_str.lower() in ("nan", "", "invalid_date"):
                continue

            try:
                dt = pd.to_datetime(val_str)
                parsed_dates.append({"idx": idx, "date": dt})

                if dt.year < min_year:
                    issues.append({
                        "row": int(idx) + 1,
                        "column": col,
                        "value": val_str,
                        "issue": f"Date year {dt.year} is before {min_year}",
                        "severity": "HIGH",
                    })
                elif dt.year > max_year:
                    issues.append({
                        "row": int(idx) + 1,
                        "column": col,
                        "value": val_str,
                        "issue": f"Date year {dt.year} is in the future (max {max_year})",
                        "severity": "CRITICAL",
                    })
            except (ValueError, TypeError):
                continue

        # Check for date clustering (more than 50% in same month)
        if len(parsed_dates) >= 5:
            months = [d["date"].month for d in parsed_dates]
            from collections import Counter
            month_counts = Counter(months)
            most_common_month, most_common_count = month_counts.most_common(1)[0]
            if most_common_count / len(parsed_dates) > 0.5:
                issues.append({
                    "row": 0,
                    "column": col,
                    "value": f"Month {most_common_month}",
                    "issue": f"Unusual clustering: {most_common_count}/{len(parsed_dates)} dates in month {most_common_month}",
                    "severity": "MEDIUM",
                })

        self.distribution_issues.extend(issues)
        return issues

    # ------------------------------------------------------------------
    # Income-specific validation
    # ------------------------------------------------------------------
    def validate_income_stats(self) -> list[dict]:
        """Run statistical validation on income column."""
        col = "income"
        config = STATISTICAL_CONFIG.get("income", {})
        method = config.get("method", "iqr")

        if method == "iqr":
            multiplier = config.get("iqr_multiplier", 1.5)
            outliers = self.detect_outliers_iqr(col, multiplier)
        else:
            threshold = config.get("zscore_threshold", 3.0)
            outliers = self.detect_outliers_zscore(col, threshold)

        self.outliers[col] = outliers
        logger.info("Income outliers detected: %d", len(outliers))
        return outliers

    # ------------------------------------------------------------------
    # Run all statistical validations
    # ------------------------------------------------------------------
    def run_all(self) -> dict:
        """Execute all statistical validations and return combined results."""
        logger.info("Running statistical validation...")

        income_outliers = self.validate_income_stats()
        dob_issues = self.analyze_date_distribution("date_of_birth")
        created_issues = self.analyze_date_distribution("created_date")

        return {
            "outliers": self.outliers,
            "distribution_issues": self.distribution_issues,
            "stats_summary": self.stats_summary,
            "total_outliers": sum(len(v) for v in self.outliers.values()),
            "total_distribution_issues": len(self.distribution_issues),
        }

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------
    def generate_report(self, filepath=None) -> str:
        """Generate a statistical validation report."""
        filepath = filepath or REPORTS_DIR / "statistical_validation_report.txt"
        results = self.run_all()
        lines: list[str] = []

        lines.append("STATISTICAL VALIDATION REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Total Rows: {self.total_rows}")
        lines.append("")

        # Stats summary
        lines.append("STATISTICAL SUMMARY:")
        lines.append("-" * 40)
        for col, stats in self.stats_summary.items():
            lines.append(f"  {col}:")
            lines.append(f"    Mean:         {stats['mean']:,.2f}")
            lines.append(f"    Median:       {stats['median']:,.2f}")
            lines.append(f"    Std Dev:      {stats['std']:,.2f}")
            lines.append(f"    Q1:           {stats['q1']:,.2f}")
            lines.append(f"    Q3:           {stats['q3']:,.2f}")
            lines.append(f"    IQR:          {stats['iqr']:,.2f}")
            lines.append(f"    Lower Bound:  {stats['lower_bound']:,.2f}")
            lines.append(f"    Upper Bound:  {stats['upper_bound']:,.2f}")
            lines.append(f"    Outliers:     {stats['outlier_count']}")
        lines.append("")

        # Outliers detail
        lines.append("OUTLIERS DETECTED:")
        lines.append("-" * 40)
        for col, outlier_list in self.outliers.items():
            if outlier_list:
                lines.append(f"  {col}: {len(outlier_list)} outlier(s)")
                for o in outlier_list[:10]:  # Show max 10
                    lines.append(
                        f"    Row {o['row']}: value={o['value']:,.2f} "
                        f"[{o['severity']}] ({o['method']})"
                    )
            else:
                lines.append(f"  {col}: No outliers detected")
        lines.append("")

        # Distribution issues
        lines.append("DATE DISTRIBUTION ISSUES:")
        lines.append("-" * 40)
        if self.distribution_issues:
            for issue in self.distribution_issues:
                lines.append(
                    f"  Row {issue['row']}: {issue['column']} — "
                    f"{issue['issue']} [{issue['severity']}]"
                )
        else:
            lines.append("  No distribution issues detected")
        lines.append("")

        report_text = "\n".join(lines)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("Statistical validation report saved to %s", filepath)
        return report_text
