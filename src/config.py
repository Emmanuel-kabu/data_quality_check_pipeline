"""
Configuration Module
====================
Defines schema rules, validation constants, file paths, and regex patterns
used throughout the data quality governance pipeline.
"""

import os
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Directory paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"

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
