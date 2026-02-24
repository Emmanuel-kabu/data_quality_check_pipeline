"""
Configuration Module
====================
Defines schema rules, validation constants, file paths, regex patterns,
production thresholds, notification settings, and cloud storage config
used throughout the data quality governance pipeline.
"""

import os
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Load .env file into os.environ (before any os.environ.get calls)
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
except ImportError:
    # Fallback: manual .env loader when python-dotenv is not installed
    def load_dotenv(dotenv_path=None, **kwargs):
        """Minimal .env loader fallback."""
        path = dotenv_path or Path(__file__).resolve().parent.parent / ".env"
        if not Path(path).exists():
            return False
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if key and key not in os.environ:
                        os.environ[key] = value
        return True

_project_root = Path(__file__).resolve().parent.parent
_env_file = _project_root / ".env"
_env_example = _project_root / ".env.example"

# Prefer .env; fall back to .env.example
if _env_file.exists():
    load_dotenv(dotenv_path=_env_file, override=False)
elif _env_example.exists():
    load_dotenv(dotenv_path=_env_example, override=False)

# ---------------------------------------------------------------------------
# Directory paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
QUARANTINE_DIR = DATA_DIR / "quarantine"
VERSIONS_DIR = DATA_DIR / "versions"
METRICS_DIR = REPORTS_DIR / "metrics"
REVIEW_DIR = DATA_DIR / "review"

# Input / output file paths
RAW_CSV = DATA_DIR / "customers_raw.csv"
CLEANED_CSV = DATA_DIR / "customers_cleaned.csv"
MASKED_CSV = DATA_DIR / "customers_masked.csv"

# Report file paths
DATA_QUALITY_REPORT = REPORTS_DIR / "data_quality_report.txt"
PII_DETECTION_REPORT = REPORTS_DIR / "pii_detection_report.txt"
VALIDATION_RESULTS = REPORTS_DIR / "validation_results.txt"
CLEANING_LOG = REPORTS_DIR / "cleaning_log.txt"
MASKED_SAMPLE = REPORTS_DIR / "masked_sample.txt"
PIPELINE_REPORT = REPORTS_DIR / "pipeline_execution_report.txt"
METRICS_FILE = METRICS_DIR / "quality_metrics.json"

# ---------------------------------------------------------------------------
# Production thresholds
# ---------------------------------------------------------------------------
FAILURE_THRESHOLDS = {
    # If more than this % of rows fail validation, halt the pipeline
    "hard_failure_pct": 5.0,
    # If more than this % of rows have warnings, flag for review
    "soft_warning_pct": 10.0,
    # Minimum rows expected (sanity check)
    "min_row_count": 1,
    # Maximum % of nulls in any single column before escalation
    "max_null_pct_per_column": 20.0,
}

# Severity-to-action mapping for failure handling
FAILURE_ACTIONS = {
    "CRITICAL": "halt",       # Stop pipeline, quarantine bad records, notify team
    "HIGH": "quarantine",     # Move bad records to dead letter queue, continue
    "MEDIUM": "log_continue", # Log warning and continue processing
    "LOW": "log_continue",    # Log info and continue
}

# ---------------------------------------------------------------------------
# Human-in-the-loop configuration
# ---------------------------------------------------------------------------
HUMAN_REVIEW_CONFIG = {
    "enabled": os.environ.get("HUMAN_REVIEW_ENABLED", "true").lower() == "true",
    # Threshold below which human must confirm to continue
    "review_threshold_pct": 80.0,
    # Timeout waiting for human response (seconds)
    "review_timeout_seconds": int(os.environ.get("REVIEW_TIMEOUT", "300")),
    # Auto-action if timeout expires: "discard" | "continue" | "quarantine"
    "timeout_action": os.environ.get("REVIEW_TIMEOUT_ACTION", "quarantine"),
    # Path for pending review queue
    "review_queue_file": REVIEW_DIR / "pending_review.json",
    "review_decisions_file": REVIEW_DIR / "review_decisions.json",
}

# ---------------------------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------------------------
RETRY_CONFIG = {
    "max_retries": int(os.environ.get("MAX_RETRIES", "3")),
    "base_delay_seconds": 2,
    "max_delay_seconds": 60,
    "exponential_base": 2,
    "retryable_exceptions": ["ConnectionError", "TimeoutError", "OSError"],
}

# ---------------------------------------------------------------------------
# Notification configuration
# ---------------------------------------------------------------------------
NOTIFICATION_CONFIG = {
    "slack": {
        "enabled": os.environ.get("SLACK_ENABLED", "false").lower() == "true",
        "webhook_url": os.environ.get("SLACK_WEBHOOK_URL", ""),
        "channel": os.environ.get("SLACK_CHANNEL", "#data-quality-alerts"),
    },
    "email": {
        "enabled": os.environ.get("EMAIL_ENABLED", "false").lower() == "true",
        "smtp_host": os.environ.get("SMTP_HOST", "smtp.gmail.com"),
        "smtp_port": int(os.environ.get("SMTP_PORT", "587")),
        "sender": os.environ.get("EMAIL_SENDER", ""),
        "password": os.environ.get("EMAIL_PASSWORD", ""),
        "recipients": {
            "critical": os.environ.get("EMAIL_CRITICAL", "").split(","),
            "warning": os.environ.get("EMAIL_WARNING", "").split(","),
            "weekly_report": os.environ.get("EMAIL_WEEKLY", "").split(","),
        },
    },
    "pagerduty": {
        "enabled": os.environ.get("PAGERDUTY_ENABLED", "false").lower() == "true",
        "routing_key": os.environ.get("PAGERDUTY_ROUTING_KEY", ""),
    },
}

# ---------------------------------------------------------------------------
# Cloud storage configuration
# ---------------------------------------------------------------------------
CLOUD_STORAGE_CONFIG = {
    "enabled": os.environ.get("CLOUD_STORAGE_ENABLED", "false").lower() == "true",
    "provider": os.environ.get("CLOUD_PROVIDER", "s3"),  # "s3" or "gcs"
    "bucket": os.environ.get("CLOUD_BUCKET", "data-quality-pipeline"),
    "raw_prefix": os.environ.get("CLOUD_RAW_PREFIX", "raw/"),
    "cleaned_prefix": os.environ.get("CLOUD_CLEANED_PREFIX", "cleaned/"),
    "reports_prefix": os.environ.get("CLOUD_REPORTS_PREFIX", "reports/"),
    "quarantine_prefix": os.environ.get("CLOUD_QUARANTINE_PREFIX", "quarantine/"),
    "region": os.environ.get("CLOUD_REGION", "us-east-1"),
}

# ---------------------------------------------------------------------------
# Statistical validation configuration
# ---------------------------------------------------------------------------
STATISTICAL_CONFIG = {
    "income": {
        "method": "iqr",  # "iqr" | "zscore"
        "iqr_multiplier": 1.5,
        "zscore_threshold": 3.0,
    },
    "dates": {
        "min_year": 1920,
        "max_year": 2026,
        "check_distribution": True,
    },
}

# ---------------------------------------------------------------------------
# Data contract definition
# ---------------------------------------------------------------------------
DATA_CONTRACT = {
    "version": "1.0.0",
    "owner": "data-engineering-team",
    "description": "Customer data contract for upstream producers",
    "sla": {
        "freshness_hours": 24,
        "completeness_pct": 95.0,
        "accuracy_pct": 98.0,
        "max_duplicate_pct": 1.0,
    },
    "schema": {
        "required_columns": [
            "customer_id", "first_name", "last_name", "email",
            "phone", "date_of_birth", "address", "income",
            "account_status", "created_date",
        ],
        "column_types": {
            "customer_id": "integer",
            "first_name": "string",
            "last_name": "string",
            "email": "string",
            "phone": "string",
            "date_of_birth": "date",
            "address": "string",
            "income": "numeric",
            "account_status": "categorical",
            "created_date": "date",
        },
    },
    "quality_rules": {
        "customer_id": {"unique": True, "not_null": True},
        "email": {"format": "email", "not_null": True},
        "income": {"min": 0, "max": 10_000_000},
        "account_status": {"allowed": ["active", "inactive", "suspended"]},
    },
}

# ---------------------------------------------------------------------------
# Expected schema definition
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "date_of_birth",
    "address",
    "income",
    "account_status",
    "created_date",
]

VALID_ACCOUNT_STATUSES = {"active", "inactive", "suspended"}

SCHEMA_RULES = {
    "customer_id": {
        "type": "integer",
        "unique": True,
        "positive": True,
    },
    "first_name": {
        "type": "string",
        "min_length": 2,
        "max_length": 50,
        "alphabetic": True,
        "nullable": False,
    },
    "last_name": {
        "type": "string",
        "min_length": 2,
        "max_length": 50,
        "alphabetic": True,
        "nullable": False,
    },
    "email": {
        "type": "string",
        "format": "email",
        "nullable": False,
    },
    "phone": {
        "type": "string",
        "format": "phone",
        "nullable": False,
    },
    "date_of_birth": {
        "type": "date",
        "format": "YYYY-MM-DD",
        "nullable": False,
    },
    "address": {
        "type": "string",
        "min_length": 10,
        "max_length": 200,
        "nullable": False,
    },
    "income": {
        "type": "numeric",
        "min_value": 0,
        "max_value": 10_000_000,
        "nullable": False,
    },
    "account_status": {
        "type": "categorical",
        "allowed_values": VALID_ACCOUNT_STATUSES,
        "nullable": False,
    },
    "created_date": {
        "type": "date",
        "format": "YYYY-MM-DD",
        "nullable": False,
    },
}

# ---------------------------------------------------------------------------
# Regex patterns for PII detection
# ---------------------------------------------------------------------------
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE
)

PHONE_PATTERNS = [
    re.compile(r"\d{3}-\d{3}-\d{4}"),              # 555-123-4567
    re.compile(r"\(\d{3}\)\s*\d{3}-\d{4}"),         # (555) 234-5678
    re.compile(r"\d{3}\.\d{3}\.\d{4}"),             # 555.789.0123
    re.compile(r"\d{10}"),                           # 5557890123
]

# Combined phone pattern for extraction
PHONE_DIGITS_PATTERN = re.compile(r"[\d]+")

# Date patterns for parsing
DATE_PATTERNS = [
    ("%Y-%m-%d", "YYYY-MM-DD"),     # 1985-03-15
    ("%Y/%m/%d", "YYYY/MM/DD"),     # 1975/05/10
    ("%m/%d/%Y", "MM/DD/YYYY"),     # 01/15/2024
]

# Normalized phone format
PHONE_NORMALIZED_FORMAT = "XXX-XXX-XXXX"

# ---------------------------------------------------------------------------
# PII column classifications
# ---------------------------------------------------------------------------
PII_COLUMNS = {
    "direct_identifiers": ["first_name", "last_name"],
    "contact_info": ["email", "phone"],
    "sensitive_personal": ["date_of_birth", "address"],
    "financial": ["income"],
}

# ---------------------------------------------------------------------------
# Missing value placeholders
# ---------------------------------------------------------------------------
MISSING_VALUE_STRATEGIES = {
    "first_name": "[UNKNOWN]",
    "last_name": "[UNKNOWN]",
    "address": "[UNKNOWN]",
    "income": 0,
    "account_status": "unknown",
}

# ---------------------------------------------------------------------------
# Severity levels for quality issues
# ---------------------------------------------------------------------------
SEVERITY_LEVELS = {
    "CRITICAL": "Blocks processing",
    "HIGH": "Data incorrect",
    "MEDIUM": "Needs cleaning",
    "LOW": "Cosmetic issue",
}

# ---------------------------------------------------------------------------
# Grafana / Prometheus metrics export
# ---------------------------------------------------------------------------
METRICS_EXPORT_CONFIG = {
    "prometheus_port": int(os.environ.get("PROMETHEUS_PORT", "9090")),
    "grafana_port": int(os.environ.get("GRAFANA_PORT", "3000")),
    "push_gateway_url": os.environ.get("PUSH_GATEWAY_URL", "http://localhost:9091"),
}
