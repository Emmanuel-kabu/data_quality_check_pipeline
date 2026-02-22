# PII Detection & Data Quality Validation Pipeline

A production-ready Python pipeline for data profiling, PII detection, data validation, cleaning, and masking — designed for fintech customer data governance.

## Overview

Data engineers spend ~40% of their time cleaning data before analysis. This pipeline automates the entire data quality lifecycle:

1. **Data Profiling** — Assess completeness, types, formats, and anomalies
2. **PII Detection** — Identify personally identifiable information using regex patterns
3. **Data Validation** — Apply schema-based validation rules per column
4. **Data Cleaning** — Normalize formats, handle missing values, fix inconsistencies
5. **PII Masking** — Protect sensitive data while preserving structure for analytics
6. **Pipeline Orchestration** — End-to-end automated workflow with logging and error handling

## Project Structure

```
data_quality_governance/
├── data/
│   ├── customers_raw.csv          # Raw input data
│   ├── customers_cleaned.csv      # Cleaned output
│   └── customers_masked.csv       # Masked output (GDPR-safe)
├── src/
│   ├── __init__.py
│   ├── config.py                  # Schema definitions & constants
│   ├── profiler.py                # Data quality profiling (Part 1)
│   ├── pii_detector.py            # PII detection engine (Part 2)
│   ├── validator.py               # Schema validation rules (Part 3)
│   ├── cleaner.py                 # Data cleaning & normalization (Part 4)
│   ├── masker.py                  # PII masking functions (Part 5)
│   └── pipeline.py                # End-to-end orchestration (Part 6)
├── reports/
│   ├── data_quality_report.txt    # Part 1 deliverable
│   ├── pii_detection_report.txt   # Part 2 deliverable
│   ├── validation_results.txt     # Part 3 deliverable
│   ├── cleaning_log.txt           # Part 4 deliverable
│   ├── masked_sample.txt          # Part 5 deliverable
│   └── pipeline_execution_report.txt  # Part 6 deliverable
├── tests/
│   ├── __init__.py
│   └── test_pipeline.py           # Unit tests
├── main.py                        # Entry point
├── requirements.txt               # Dependencies
├── reflection.md                  # Part 7: Governance reflection
└── README.md
```

## Quick Start

### Prerequisites
- Python 3.9+
- pip

### Installation

```bash
pip install -r requirements.txt
```

### Run the Pipeline

```bash
python main.py
```

This will:
- Load `data/customers_raw.csv`
- Profile, validate, clean, detect PII, and mask data
- Save cleaned CSV to `data/customers_cleaned.csv`
- Save masked CSV to `data/customers_masked.csv`
- Generate all reports in `reports/`

### Run Tests

```bash
python -m pytest tests/ -v
```

## Expected Schema

| Column          | Type    | Rules                                      |
|-----------------|---------|--------------------------------------------|
| customer_id     | Integer | Unique, positive                           |
| first_name      | String  | Non-empty, 2-50 chars, alphabetic          |
| last_name       | String  | Non-empty, 2-50 chars, alphabetic          |
| email           | String  | Valid email format                          |
| phone           | String  | Valid phone (normalized to XXX-XXX-XXXX)   |
| date_of_birth   | Date    | Valid date, YYYY-MM-DD                     |
| address         | String  | Non-empty, 10-200 chars                    |
| income          | Numeric | Non-negative, ≤ $10M                      |
| account_status  | String  | Must be: active, inactive, suspended       |
| created_date    | Date    | Valid date, YYYY-MM-DD                     |

## Key Concepts

- Data profiling & quality assessment
- PII detection & masking (GDPR compliance)
- Schema-based data validation
- ETL pipeline design
- Production-ready code structure
- Data governance & compliance

## License

MIT
