# Data Quality Governance Pipeline

A production-grade Python pipeline for end-to-end data quality governance -- covering profiling, validation, cleaning, PII protection, cloud storage, alerting, and human-in-the-loop review. Built for fintech customer data with GDPR compliance in mind.

## Overview

Data engineers spend ~40% of their time cleaning data before analysis. This pipeline automates the entire data quality lifecycle across **17 stages** with production-grade features:

| Stage | Name | Description |
|-------|------|-------------|
| 1 | **Load** | Read raw CSV from local or S3 |
| 2 | **Idempotency** | Skip if input unchanged (checksum-based) |
| 3 | **Profile** | Assess completeness, types, formats, anomalies |
| 4 | **Contract** | Validate against data contracts and SLA thresholds |
| 5 | **Validate** | Schema-based validation rules per column |
| 6 | **Statistical** | Outlier and distribution analysis (IQR, z-score) |
| 7 | **Threshold & Review** | Check failure rates, trigger human-in-the-loop review |
| 8 | **Clean** | Normalize formats, handle missing values |
| 9 | **Quarantine** | Isolate failed records in a dead letter queue |
| 10 | **GE Validate** | Great Expectations declarative validation |
| 11 | **Detect PII** | Identify sensitive fields via regex patterns |
| 12 | **Mask PII** | Protect PII while preserving analytic structure |
| 13 | **Version** | Create versioned snapshot with rollback support |
| 14 | **Metrics** | Export quality metrics (Prometheus/Grafana) |
| 15 | **Upload** | Upload timestamped outputs to AWS S3 |
| 16 | **Notify** | Send alerts via Slack, Email, PagerDuty |
| 17 | **Report** | Generate final pipeline execution report |

## Project Structure

```
data_quality_governance/
├── main.py                        # Entry point (CLI with --rollback, --no-review, --list-versions)
├── requirements.txt               # Dependencies
├── .env.example                   # Environment config template (AWS, Slack, Email)
├── Dockerfile                     # Container build
├── docker-compose-quality.yml     # Full stack (pipeline + Prometheus + Grafana)
├── setup_aws.ps1                  # AWS S3 setup helper script
├── reflection.md                  # Governance reflection & analysis
├── README.md
│
├── src/
│   ├── config.py                  # Central configuration (auto-loads .env)
│   ├── pipeline.py                # 17-stage production pipeline orchestrator
│   ├── profiler.py                # Data quality profiling
│   ├── validator.py               # Schema validation rules
│   ├── cleaner.py                 # Data cleaning & normalization
│   ├── pii_detector.py            # PII detection engine
│   ├── masker.py                  # PII masking functions
│   ├── statistical_validator.py   # Outlier & distribution analysis
│   ├── data_contract.py           # Data contracts & SLA validation
│   ├── expectations.py            # Great Expectations integration
│   ├── dead_letter_queue.py       # Quarantine for failed records
│   ├── human_review.py            # Human-in-the-loop review system
│   ├── rollback_manager.py        # Versioned snapshots & rollback
│   ├── metrics_collector.py       # Prometheus metrics exporter
│   ├── cloud_storage.py           # S3/GCS upload with timestamped filenames
│   ├── notifier.py                # Slack, Email, PagerDuty notifications
│   └── retry_handler.py           # Retry with exponential backoff
│
├── data/
│   ├── customers_raw.csv          # Raw input data
│   ├── customers_cleaned.csv      # Cleaned output
│   ├── customers_masked.csv       # Masked output (GDPR-safe)
│   └── versions/                  # Versioned snapshots for rollback
│
├── reports/                       # All generated reports
│   ├── data_quality_report.txt
│   ├── pii_detection_report.txt
│   ├── validation_results.txt
│   ├── cleaning_log.txt
│   ├── masked_sample.txt
│   ├── statistical_validation_report.txt
│   ├── data_contract_report.txt
│   ├── ge_validation_report.txt
│   ├── dead_letter_queue_report.txt
│   ├── human_review_report.txt
│   ├── pipeline_execution_report.txt
│   └── metrics/                   # Prometheus + JSON metrics
│
├── tests/
│   └── test_pipeline.py           # 82 unit tests across 14 test classes
│
├── dags/
│   └── data_quality_dag.py        # Apache Airflow DAG
│
├── grafana/
│   ├── dashboards/                # Pre-built Grafana dashboard JSON
│   └── provisioning/              # Auto-provisioning config
│
└── prometheus/
    ├── prometheus.yml              # Prometheus scrape config
    └── alerts.yml                  # Alert rules for quality degradation
```

## Quick Start

### Prerequisites

- Python 3.9+
- pip

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env` with your credentials (AWS, Slack, Email). The pipeline auto-loads this file on startup.

### Run the Pipeline

```bash
python main.py
```

### CLI Options

```bash
python main.py                    # Full pipeline run
python main.py --no-review        # Skip human-in-the-loop review
python main.py --rollback v_20260224_110843  # Rollback to a version
python main.py --list-versions    # List all available versions
```

### Run Tests

```bash
python -m pytest tests/ -v        # 82 tests, 14 test classes
```

### Docker (Full Stack)

```bash
docker-compose -f docker-compose-quality.yml up
```

This starts the pipeline, Prometheus (port 9090), and Grafana (port 3000) with pre-built dashboards.

## Cloud Storage (AWS S3)

The pipeline uploads all outputs to S3 with **timestamped filenames** for data lineage tracking:

```
s3://your-bucket/
  cleaned/
    customers_cleaned_20260224_111743.csv
    customers_masked_20260224_111743.csv
  reports/
    data_quality_report_20260224_111743.txt
    pipeline_execution_report_20260224_111743.txt
    ...
  quarantine/
    quarantined_records_20260224_111743.json
```

Configure in `.env`:
```
CLOUD_STORAGE_ENABLED=true
CLOUD_PROVIDER=s3
CLOUD_BUCKET=your-bucket-name
CLOUD_REGION=us-west-2
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=your-secret
```

The IAM user needs `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, and `s3:HeadObject` permissions on the bucket.

## Notifications

| Channel | Trigger | Configuration |
|---------|---------|---------------|
| **Slack** | All severities | Webhook URL in `.env` |
| **Email** | HIGH and CRITICAL | Gmail SMTP with App Password |
| **PagerDuty** | CRITICAL only | Routing key in `.env` |

## Monitoring (Prometheus + Grafana)

- **Prometheus** scrapes pipeline metrics (row counts, failure rates, stage durations)
- **Grafana** pre-built dashboard with quality trends, PII exposure, SLA compliance
- **Alert rules** trigger on quality degradation (e.g., completeness drops below 95%)

## Expected Schema

| Column | Type | Rules |
|--------|------|-------|
| customer_id | Integer | Unique, positive |
| first_name | String | Non-empty, 2-50 chars, alphabetic |
| last_name | String | Non-empty, 2-50 chars, alphabetic |
| email | String | Valid email format |
| phone | String | Valid phone (normalized to XXX-XXX-XXXX) |
| date_of_birth | Date | Valid date, YYYY-MM-DD |
| address | String | Non-empty, 10-200 chars |
| income | Numeric | Non-negative, <= $10M |
| account_status | String | Must be: active, inactive, suspended |
| created_date | Date | Valid date, YYYY-MM-DD |

## Key Concepts

- 17-stage production data pipeline with idempotent execution
- Data profiling, validation, and cleaning automation
- PII detection and masking (GDPR/CCPA compliance)
- Data contracts and SLA enforcement
- Statistical validation (outlier detection, distribution analysis)
- Human-in-the-loop review for threshold breaches
- Dead letter queue for quarantined records
- Versioned snapshots with rollback support
- Cloud storage with timestamped uploads for data lineage
- Real-time notifications (Slack, Email, PagerDuty)
- Prometheus metrics and Grafana dashboards
- Containerized deployment with Docker Compose
- Apache Airflow DAG for scheduled orchestration
- 82 unit tests across 14 test classes

## License

MIT
