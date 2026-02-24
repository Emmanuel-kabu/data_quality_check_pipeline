"""
Great Expectations Integration Module
======================================
Wraps Great Expectations for declarative, version-controlled
data validation with built-in profiling and data docs.

Falls back to built-in validation if GE is not installed.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import REPORTS_DIR

logger = logging.getLogger(__name__)

# Try to import Great Expectations
GE_AVAILABLE = False
try:
    import great_expectations as gx
    from great_expectations.dataset import PandasDataset
    GE_AVAILABLE = True
    logger.info("Great Expectations v%s available", gx.__version__)
except ImportError:
    logger.info(
        "Great Expectations not installed. "
        "Using built-in validation. Install with: pip install great-expectations"
    )


# ---------------------------------------------------------------------------
# Expectation suite definition (declarative)
# ---------------------------------------------------------------------------
CUSTOMER_EXPECTATIONS = {
    "expectation_suite_name": "customer_data_suite",
    "expectations": [
        # customer_id
        {"type": "expect_column_to_exist", "kwargs": {"column": "customer_id"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer_id"}},
        {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "customer_id"}},
        {"type": "expect_column_values_to_be_of_type", "kwargs": {"column": "customer_id", "type_": "int64"}},
        # first_name
        {"type": "expect_column_to_exist", "kwargs": {"column": "first_name"}},
        {"type": "expect_column_value_lengths_to_be_between", "kwargs": {"column": "first_name", "min_value": 2, "max_value": 50, "mostly": 0.9}},
        # last_name
        {"type": "expect_column_to_exist", "kwargs": {"column": "last_name"}},
        {"type": "expect_column_value_lengths_to_be_between", "kwargs": {"column": "last_name", "min_value": 2, "max_value": 50, "mostly": 0.9}},
        # email
        {"type": "expect_column_to_exist", "kwargs": {"column": "email"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "email"}},
        {"type": "expect_column_values_to_match_regex", "kwargs": {"column": "email", "regex": r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", "mostly": 0.95}},
        # phone
        {"type": "expect_column_to_exist", "kwargs": {"column": "phone"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "phone", "mostly": 0.9}},
        # income
        {"type": "expect_column_to_exist", "kwargs": {"column": "income"}},
        {"type": "expect_column_values_to_be_between", "kwargs": {"column": "income", "min_value": 0, "max_value": 10000000, "mostly": 0.95}},
        # account_status
        {"type": "expect_column_to_exist", "kwargs": {"column": "account_status"}},
        {"type": "expect_column_values_to_be_in_set", "kwargs": {"column": "account_status", "value_set": ["active", "inactive", "suspended"], "mostly": 0.9}},
        # date columns
        {"type": "expect_column_to_exist", "kwargs": {"column": "date_of_birth"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "created_date"}},
        # address
        {"type": "expect_column_to_exist", "kwargs": {"column": "address"}},
    ],
}


class GreatExpectationsValidator:
    """
    Validates data using Great Expectations when available,
    falls back to a built-in declarative validator otherwise.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        self.results: dict[str, Any] = {}
        self.ge_available = GE_AVAILABLE

    def run_validation(self) -> dict:
        """Run expectations against the DataFrame."""
        if self.ge_available:
            return self._run_ge_validation()
        return self._run_builtin_validation()

    # ------------------------------------------------------------------
    # Great Expectations validation
    # ------------------------------------------------------------------
    def _run_ge_validation(self) -> dict:
        """Run validation using Great Expectations library."""
        logger.info("Running Great Expectations validation...")

        try:
            ge_df = PandasDataset(self.df)
            results = []
            passed = 0
            failed = 0

            for exp in CUSTOMER_EXPECTATIONS["expectations"]:
                exp_type = exp["type"]
                kwargs = exp.get("kwargs", {})

                try:
                    method = getattr(ge_df, exp_type)
                    result = method(**kwargs)
                    success = result.get("success", False)

                    results.append({
                        "expectation": exp_type,
                        "kwargs": kwargs,
                        "success": success,
                        "result": {
                            k: v for k, v in result.items()
                            if k in ("success", "result", "exception_info")
                        },
                    })

                    if success:
                        passed += 1
                    else:
                        failed += 1

                except AttributeError:
                    results.append({
                        "expectation": exp_type,
                        "kwargs": kwargs,
                        "success": False,
                        "result": {"error": f"Method {exp_type} not found"},
                    })
                    failed += 1

            self.results = {
                "engine": "great_expectations",
                "suite": CUSTOMER_EXPECTATIONS["expectation_suite_name"],
                "total": passed + failed,
                "passed": passed,
                "failed": failed,
                "pass_rate_pct": round(passed / (passed + failed) * 100, 2) if (passed + failed) > 0 else 0,
                "results": results,
            }
            return self.results

        except Exception as exc:
            logger.warning("GE validation failed, falling back to built-in: %s", exc)
            return self._run_builtin_validation()

    # ------------------------------------------------------------------
    # Built-in declarative validation (fallback)
    # ------------------------------------------------------------------
    def _run_builtin_validation(self) -> dict:
        """Run a built-in declarative validation mimicking GE expectations."""
        logger.info("Running built-in declarative validation (GE not available)...")

        results = []
        passed = 0
        failed = 0

        for exp in CUSTOMER_EXPECTATIONS["expectations"]:
            exp_type = exp["type"]
            kwargs = exp.get("kwargs", {})
            success = self._evaluate_expectation(exp_type, kwargs)

            results.append({
                "expectation": exp_type,
                "kwargs": kwargs,
                "success": success,
            })

            if success:
                passed += 1
            else:
                failed += 1

        self.results = {
            "engine": "built-in",
            "suite": CUSTOMER_EXPECTATIONS["expectation_suite_name"],
            "total": passed + failed,
            "passed": passed,
            "failed": failed,
            "pass_rate_pct": round(passed / (passed + failed) * 100, 2) if (passed + failed) > 0 else 0,
            "results": results,
        }
        return self.results

    def _evaluate_expectation(self, exp_type: str, kwargs: dict) -> bool:
        """Evaluate a single expectation against the DataFrame."""
        col = kwargs.get("column", "")
        mostly = kwargs.get("mostly", 1.0)

        try:
            if exp_type == "expect_column_to_exist":
                return col in self.df.columns

            if col not in self.df.columns:
                return False

            if exp_type == "expect_column_values_to_not_be_null":
                null_rate = self.df[col].isna().mean()
                return (1 - null_rate) >= mostly

            elif exp_type == "expect_column_values_to_be_unique":
                return not self.df[col].dropna().duplicated().any()

            elif exp_type == "expect_column_values_to_be_of_type":
                expected_type = kwargs.get("type_", "")
                return str(self.df[col].dtype) == expected_type

            elif exp_type == "expect_column_value_lengths_to_be_between":
                min_val = kwargs.get("min_value", 0)
                max_val = kwargs.get("max_value", 999)
                str_vals = self.df[col].dropna().astype(str)
                lengths = str_vals.str.len()
                valid_rate = ((lengths >= min_val) & (lengths <= max_val)).mean()
                return valid_rate >= mostly

            elif exp_type == "expect_column_values_to_match_regex":
                import re
                regex = kwargs.get("regex", "")
                pattern = re.compile(regex)
                str_vals = self.df[col].dropna().astype(str)
                match_rate = str_vals.apply(lambda x: bool(pattern.match(x))).mean()
                return match_rate >= mostly

            elif exp_type == "expect_column_values_to_be_between":
                min_val = kwargs.get("min_value", float("-inf"))
                max_val = kwargs.get("max_value", float("inf"))
                num_vals = pd.to_numeric(self.df[col], errors="coerce").dropna()
                if len(num_vals) == 0:
                    return True
                valid_rate = ((num_vals >= min_val) & (num_vals <= max_val)).mean()
                return valid_rate >= mostly

            elif exp_type == "expect_column_values_to_be_in_set":
                value_set = set(kwargs.get("value_set", []))
                str_vals = self.df[col].dropna().astype(str).str.strip().str.lower()
                valid_rate = str_vals.isin(value_set).mean()
                return valid_rate >= mostly

            return True  # Unknown expectation type — pass by default

        except Exception as exc:
            logger.debug("Expectation %s failed: %s", exp_type, exc)
            return False

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------
    def generate_report(self, filepath=None) -> str:
        """Generate a Great Expectations style validation report."""
        filepath = filepath or REPORTS_DIR / "ge_validation_report.txt"

        if not self.results:
            self.run_validation()

        lines: list[str] = []
        lines.append("GREAT EXPECTATIONS VALIDATION REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Engine: {self.results.get('engine', 'unknown')}")
        lines.append(f"Suite: {self.results.get('suite', 'unknown')}")
        lines.append("")

        lines.append("SUMMARY:")
        lines.append("-" * 40)
        lines.append(f"  Total Expectations: {self.results.get('total', 0)}")
        lines.append(f"  Passed: {self.results.get('passed', 0)}")
        lines.append(f"  Failed: {self.results.get('failed', 0)}")
        lines.append(f"  Pass Rate: {self.results.get('pass_rate_pct', 0):.1f}%")
        lines.append("")

        lines.append("DETAILED RESULTS:")
        lines.append("-" * 40)
        for r in self.results.get("results", []):
            status = "[OK]" if r["success"] else "[XX]"
            col = r["kwargs"].get("column", "")
            lines.append(f"  {status} {r['expectation']}")
            if col:
                lines.append(f"      Column: {col}")
            for k, v in r["kwargs"].items():
                if k != "column":
                    lines.append(f"      {k}: {v}")
        lines.append("")

        report_text = "\n".join(lines)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("GE validation report saved to %s", filepath)
        return report_text
