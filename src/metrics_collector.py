"""
Metrics Collector Module
========================
Collects, stores, and exports pipeline quality metrics for:
  - Grafana dashboards (via Prometheus/JSON)
  - Historical trend tracking
  - Alerting on quality degradation
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import METRICS_DIR, METRICS_FILE, METRICS_EXPORT_CONFIG, REPORTS_DIR

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Collects pipeline execution metrics and exports them
    in formats consumable by Grafana, Prometheus, or JSON APIs.
    """

    def __init__(self, run_id: str = "") -> None:
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.metrics: dict[str, Any] = {
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "stages": {},
            "quality": {},
            "performance": {},
            "alerts": [],
        }
        METRICS_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Record metrics
    # ------------------------------------------------------------------
    def record_stage(
        self,
        stage_name: str,
        status: str,
        duration_ms: float = 0,
        details: dict | None = None,
    ) -> None:
        """Record metrics for a pipeline stage."""
        self.metrics["stages"][stage_name] = {
            "status": status,
            "duration_ms": duration_ms,
            "timestamp": datetime.now().isoformat(),
            **(details or {}),
        }

    def record_quality_metrics(
        self,
        total_rows: int,
        passed_rows: int,
        failed_rows: int,
        quarantined_rows: int = 0,
        completeness_pct: float = 0,
        accuracy_pct: float = 0,
        outlier_count: int = 0,
    ) -> None:
        """Record data quality metrics."""
        self.metrics["quality"] = {
            "total_rows": total_rows,
            "passed_rows": passed_rows,
            "failed_rows": failed_rows,
            "quarantined_rows": quarantined_rows,
            "pass_rate_pct": round((passed_rows / total_rows * 100), 2) if total_rows > 0 else 0,
            "failure_rate_pct": round((failed_rows / total_rows * 100), 2) if total_rows > 0 else 0,
            "completeness_pct": completeness_pct,
            "accuracy_pct": accuracy_pct,
            "outlier_count": outlier_count,
        }

    def record_pii_metrics(
        self,
        total_pii_fields: int,
        masked_fields: int,
        pii_categories: dict | None = None,
    ) -> None:
        """Record PII detection and masking metrics."""
        self.metrics["pii"] = {
            "total_pii_fields": total_pii_fields,
            "masked_fields": masked_fields,
            "masking_coverage_pct": (
                round((masked_fields / total_pii_fields * 100), 2)
                if total_pii_fields > 0 else 100
            ),
            "categories": pii_categories or {},
        }

    def record_performance(
        self,
        total_duration_seconds: float,
        rows_per_second: float = 0,
    ) -> None:
        """Record performance metrics."""
        self.metrics["performance"] = {
            "total_duration_seconds": round(total_duration_seconds, 2),
            "rows_per_second": round(rows_per_second, 2),
        }

    def add_alert(self, severity: str, message: str) -> None:
        """Record an alert."""
        self.metrics["alerts"].append({
            "severity": severity,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        })

    # ------------------------------------------------------------------
    # Export metrics
    # ------------------------------------------------------------------
    def save_metrics(self) -> Path:
        """Save current run metrics to JSON file."""
        filepath = METRICS_DIR / f"metrics_{self.run_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.metrics, f, indent=2, default=str)

        # Also update the latest metrics file
        with open(METRICS_FILE, "w", encoding="utf-8") as f:
            json.dump(self.metrics, f, indent=2, default=str)

        logger.info("Metrics saved to %s", filepath)
        return filepath

    def export_prometheus_metrics(self) -> str:
        """
        Export metrics in Prometheus text exposition format
        for scraping by Prometheus/Grafana.
        """
        lines: list[str] = []
        q = self.metrics.get("quality", {})

        # Quality metrics
        lines.append("# HELP dq_total_rows Total rows processed")
        lines.append("# TYPE dq_total_rows gauge")
        lines.append(f'dq_total_rows{{run_id="{self.run_id}"}} {q.get("total_rows", 0)}')

        lines.append("# HELP dq_passed_rows Rows that passed validation")
        lines.append("# TYPE dq_passed_rows gauge")
        lines.append(f'dq_passed_rows{{run_id="{self.run_id}"}} {q.get("passed_rows", 0)}')

        lines.append("# HELP dq_failed_rows Rows that failed validation")
        lines.append("# TYPE dq_failed_rows gauge")
        lines.append(f'dq_failed_rows{{run_id="{self.run_id}"}} {q.get("failed_rows", 0)}')

        lines.append("# HELP dq_quarantined_rows Rows quarantined in DLQ")
        lines.append("# TYPE dq_quarantined_rows gauge")
        lines.append(f'dq_quarantined_rows{{run_id="{self.run_id}"}} {q.get("quarantined_rows", 0)}')

        lines.append("# HELP dq_pass_rate_pct Validation pass rate percentage")
        lines.append("# TYPE dq_pass_rate_pct gauge")
        lines.append(f'dq_pass_rate_pct{{run_id="{self.run_id}"}} {q.get("pass_rate_pct", 0)}')

        lines.append("# HELP dq_completeness_pct Data completeness percentage")
        lines.append("# TYPE dq_completeness_pct gauge")
        lines.append(f'dq_completeness_pct{{run_id="{self.run_id}"}} {q.get("completeness_pct", 0)}')

        lines.append("# HELP dq_outlier_count Number of statistical outliers")
        lines.append("# TYPE dq_outlier_count gauge")
        lines.append(f'dq_outlier_count{{run_id="{self.run_id}"}} {q.get("outlier_count", 0)}')

        # Performance metrics
        p = self.metrics.get("performance", {})
        lines.append("# HELP dq_duration_seconds Pipeline execution duration")
        lines.append("# TYPE dq_duration_seconds gauge")
        lines.append(f'dq_duration_seconds{{run_id="{self.run_id}"}} {p.get("total_duration_seconds", 0)}')

        lines.append("# HELP dq_rows_per_second Processing throughput")
        lines.append("# TYPE dq_rows_per_second gauge")
        lines.append(f'dq_rows_per_second{{run_id="{self.run_id}"}} {p.get("rows_per_second", 0)}')

        # PII metrics
        pii = self.metrics.get("pii", {})
        lines.append("# HELP dq_pii_fields_total Total PII fields detected")
        lines.append("# TYPE dq_pii_fields_total gauge")
        lines.append(f'dq_pii_fields_total{{run_id="{self.run_id}"}} {pii.get("total_pii_fields", 0)}')

        lines.append("# HELP dq_pii_masked_fields Total PII fields masked")
        lines.append("# TYPE dq_pii_masked_fields gauge")
        lines.append(f'dq_pii_masked_fields{{run_id="{self.run_id}"}} {pii.get("masked_fields", 0)}')

        # Stage durations
        for stage_name, stage_data in self.metrics.get("stages", {}).items():
            safe_name = stage_name.lower().replace(" ", "_").replace("-", "_")
            lines.append(f"# HELP dq_stage_{safe_name}_duration_ms Stage duration in ms")
            lines.append(f"# TYPE dq_stage_{safe_name}_duration_ms gauge")
            lines.append(
                f'dq_stage_{safe_name}_duration_ms{{run_id="{self.run_id}"}} '
                f'{stage_data.get("duration_ms", 0)}'
            )

        prom_text = "\n".join(lines) + "\n"

        # Save to file for Prometheus file_sd scraping
        prom_file = METRICS_DIR / "metrics.prom"
        with open(prom_file, "w", encoding="utf-8") as f:
            f.write(prom_text)

        logger.info("Prometheus metrics exported to %s", prom_file)
        return prom_text

    def load_historical_metrics(self) -> list[dict]:
        """Load all historical metrics for trend analysis."""
        history = []
        for mf in sorted(METRICS_DIR.glob("metrics_*.json")):
            try:
                with open(mf, "r", encoding="utf-8") as f:
                    history.append(json.load(f))
            except (json.JSONDecodeError, IOError):
                continue
        return history

    def detect_quality_degradation(self, lookback: int = 5) -> list[dict]:
        """
        Compare current metrics against recent history to detect
        quality degradation trends.
        """
        history = self.load_historical_metrics()
        if len(history) < 2:
            return []

        recent = history[-lookback:]
        alerts = []

        # Check pass rate trend
        pass_rates = [
            h.get("quality", {}).get("pass_rate_pct", 100)
            for h in recent
        ]

        current_rate = self.metrics.get("quality", {}).get("pass_rate_pct", 100)
        avg_rate = sum(pass_rates) / len(pass_rates) if pass_rates else 100

        if current_rate < avg_rate - 10:
            alert = {
                "type": "quality_degradation",
                "message": (
                    f"Pass rate dropped to {current_rate:.1f}% "
                    f"(avg of last {len(pass_rates)} runs: {avg_rate:.1f}%)"
                ),
                "severity": "HIGH",
            }
            alerts.append(alert)
            self.add_alert("HIGH", alert["message"])

        # Check completeness trend
        completeness_vals = [
            h.get("quality", {}).get("completeness_pct", 100)
            for h in recent
        ]
        current_completeness = self.metrics.get("quality", {}).get("completeness_pct", 100)
        avg_completeness = sum(completeness_vals) / len(completeness_vals) if completeness_vals else 100

        if current_completeness < avg_completeness - 5:
            alert = {
                "type": "completeness_degradation",
                "message": (
                    f"Completeness dropped to {current_completeness:.1f}% "
                    f"(avg: {avg_completeness:.1f}%)"
                ),
                "severity": "MEDIUM",
            }
            alerts.append(alert)
            self.add_alert("MEDIUM", alert["message"])

        return alerts

    # ------------------------------------------------------------------
    # Push to Prometheus Pushgateway (for batch jobs)
    # ------------------------------------------------------------------
    def push_to_pushgateway(self, gateway_url: str = "") -> bool:
        """
        Push current metrics to Prometheus Pushgateway.
        This is the standard pattern for batch/short-lived jobs.
        """
        url = gateway_url or METRICS_EXPORT_CONFIG.get("push_gateway_url", "http://localhost:9091")

        try:
            from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
        except ImportError:
            logger.warning("prometheus_client not installed -- skipping pushgateway")
            return False

        try:
            registry = CollectorRegistry()
            q = self.metrics.get("quality", {})
            p = self.metrics.get("performance", {})
            pii = self.metrics.get("pii", {})

            # -- Quality gauges ------------------------------------------------
            def _gauge(name: str, doc: str, value: float) -> None:
                g = Gauge(name, doc, registry=registry)
                g.set(value)

            _gauge("dq_total_rows", "Total rows processed", q.get("total_rows", 0))
            _gauge("dq_passed_rows", "Rows that passed validation", q.get("passed_rows", 0))
            _gauge("dq_failed_rows", "Rows that failed validation", q.get("failed_rows", 0))
            _gauge("dq_quarantined_rows", "Rows quarantined in DLQ", q.get("quarantined_rows", 0))
            _gauge("dq_pass_rate_pct", "Validation pass rate percentage", q.get("pass_rate_pct", 0))
            _gauge("dq_completeness_pct", "Data completeness percentage", q.get("completeness_pct", 0))
            _gauge("dq_outlier_count", "Number of statistical outliers", q.get("outlier_count", 0))

            # -- Performance gauges --------------------------------------------
            _gauge("dq_duration_seconds", "Pipeline execution duration", p.get("total_duration_seconds", 0))
            _gauge("dq_rows_per_second", "Processing throughput", p.get("rows_per_second", 0))

            # -- PII gauges ----------------------------------------------------
            _gauge("dq_pii_fields_total", "Total PII fields detected", pii.get("total_pii_fields", 0))
            _gauge("dq_pii_masked_fields", "Total PII fields masked", pii.get("masked_fields", 0))

            # -- Stage duration gauges -----------------------------------------
            for stage_name, stage_data in self.metrics.get("stages", {}).items():
                safe = stage_name.lower().replace(" ", "_").replace("-", "_")
                g = Gauge(
                    f"dq_stage_{safe}_duration_ms",
                    f"{stage_name} stage duration in ms",
                    registry=registry,
                )
                g.set(stage_data.get("duration_ms", 0))

            push_to_gateway(url, job="data_quality_pipeline", registry=registry)
            logger.info("Metrics pushed to Pushgateway at %s", url)
            return True

        except Exception as exc:
            logger.warning("Failed to push metrics to Pushgateway: %s", exc)
            return False
