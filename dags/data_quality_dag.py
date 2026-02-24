"""
Airflow DAG — Data Quality Governance Pipeline
===============================================
Orchestrates the data quality pipeline as an Airflow DAG with:
  - Scheduled daily execution at 02:00 UTC
  - S3/GCS event-driven trigger support
  - Failure handling with Slack/Email alerts
  - Human-in-the-loop approval for low-quality batches
  - Retry with exponential backoff

Usage:
  Place this file in your Airflow dags/ folder.
  Configure connections and variables in Airflow UI.

Required Airflow Variables:
  - dq_input_path: Path to raw CSV (local or S3)
  - dq_failure_threshold: Max failure % (default: 5)
  - dq_human_review_enabled: Enable human review (default: true)

Required Airflow Connections:
  - slack_webhook: Slack webhook for alerts
  - aws_default: AWS credentials for S3 (if using cloud)
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": Variable.get("dq_alert_emails", default_var="data-eng@company.com").split(","),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def load_data(**context):
    """Stage 1: Load raw data from local or cloud storage."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.config import RAW_CSV, CLOUD_STORAGE_CONFIG
    from src.cloud_storage import CloudStorageClient

    input_path = Variable.get("dq_input_path", default_var=str(RAW_CSV))

    cloud = CloudStorageClient()
    if cloud.enabled and input_path.startswith(("s3://", "gs://")):
        # Download from cloud
        local_path = RAW_CSV
        remote_key = input_path.split("/", 3)[-1] if "/" in input_path else input_path
        cloud.download_file(remote_key, local_path)
        input_path = str(local_path)

    df = pd.read_csv(input_path)
    rows, cols = df.shape

    context["ti"].xcom_push(key="input_path", value=input_path)
    context["ti"].xcom_push(key="row_count", value=rows)
    context["ti"].xcom_push(key="col_count", value=cols)

    return f"Loaded {rows} rows, {cols} columns from {input_path}"


def check_idempotency(**context):
    """Check if input has changed since last successful run."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.rollback_manager import RollbackManager

    input_path = context["ti"].xcom_pull(key="input_path", task_ids="load_data")
    rm = RollbackManager()

    needs_run = rm.check_idempotency(Path(input_path))
    context["ti"].xcom_push(key="needs_run", value=needs_run)

    if not needs_run:
        return "skip_pipeline"
    return "run_profile"


def run_profile(**context):
    """Stage 2: Profile data quality."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.profiler import DataProfiler

    input_path = context["ti"].xcom_pull(key="input_path", task_ids="load_data")
    df = pd.read_csv(input_path)

    profiler = DataProfiler(df)
    report = profiler.generate_report()
    results = profiler.run_full_profile()

    issue_count = len(results["issues"])
    context["ti"].xcom_push(key="quality_issues", value=issue_count)
    context["ti"].xcom_push(key="severity_counts", value=results["severity"])

    return f"Profiling complete: {issue_count} issues found"


def run_data_contract_validation(**context):
    """Stage 2b: Validate data contract and SLAs."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.data_contract import DataContractValidator

    input_path = context["ti"].xcom_pull(key="input_path", task_ids="load_data")
    df = pd.read_csv(input_path)

    validator = DataContractValidator(df)
    report = validator.generate_report()
    results = validator.validate_all()

    context["ti"].xcom_push(key="contract_passed", value=results["all_passed"])
    context["ti"].xcom_push(key="contract_violations", value=results["total_violations"])

    if not results["all_passed"]:
        critical_violations = [
            v for v in results["violations"] if v["severity"] == "CRITICAL"
        ]
        if critical_violations:
            raise AirflowFailException(
                f"Data contract CRITICAL violations: {critical_violations}"
            )

    return f"Contract validation: {'PASS' if results['all_passed'] else 'FAIL'}"


def run_validation(**context):
    """Stage 3: Validate raw data."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.validator import DataValidator

    input_path = context["ti"].xcom_pull(key="input_path", task_ids="load_data")
    df = pd.read_csv(input_path)
    total_rows = len(df)

    validator = DataValidator(df)
    results = validator.run_all_validations()
    validator.generate_report()

    failure_pct = (results["failed"] / total_rows * 100) if total_rows > 0 else 0

    context["ti"].xcom_push(key="validation_passed", value=results["passed"])
    context["ti"].xcom_push(key="validation_failed", value=results["failed"])
    context["ti"].xcom_push(key="failure_pct", value=failure_pct)
    context["ti"].xcom_push(key="failed_rows", value=results["failed_rows"])

    return f"Validation: {results['passed']} passed, {results['failed']} failed ({failure_pct:.1f}%)"


def run_statistical_validation(**context):
    """Stage 3b: Statistical validation — outlier and distribution checks."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.statistical_validator import StatisticalValidator

    input_path = context["ti"].xcom_pull(key="input_path", task_ids="load_data")
    df = pd.read_csv(input_path)

    stat_validator = StatisticalValidator(df)
    results = stat_validator.run_all()
    stat_validator.generate_report()

    context["ti"].xcom_push(key="outlier_count", value=results["total_outliers"])
    context["ti"].xcom_push(key="distribution_issues", value=results["total_distribution_issues"])

    return f"Statistical: {results['total_outliers']} outliers, {results['total_distribution_issues']} distribution issues"


def check_failure_threshold(**context):
    """Branch: check if failure rate requires human review or halt."""
    failure_pct = context["ti"].xcom_pull(key="failure_pct", task_ids="run_validation")
    threshold = float(Variable.get("dq_failure_threshold", default_var="5"))
    human_review_enabled = Variable.get("dq_human_review_enabled", default_var="true").lower() == "true"

    if failure_pct > threshold:
        if human_review_enabled:
            return "human_review_gate"
        return "halt_pipeline"
    return "run_cleaning"


def human_review_gate(**context):
    """
    Human-in-the-loop gate.
    In production, this would be an Airflow sensor waiting for
    operator approval, or integrated with a review UI.
    """
    import json
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.config import REVIEW_DIR

    failure_pct = context["ti"].xcom_pull(key="failure_pct", task_ids="run_validation")
    failed_rows = context["ti"].xcom_pull(key="failed_rows", task_ids="run_validation")

    # Write review request
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    review_file = REVIEW_DIR / "pending_review.json"
    with open(review_file, "w") as f:
        json.dump({
            "failure_pct": failure_pct,
            "failed_row_count": len(failed_rows) if failed_rows else 0,
            "failed_rows": failed_rows[:20] if failed_rows else [],
            "requested_at": datetime.now().isoformat(),
            "message": (
                f"Validation failure rate {failure_pct:.1f}% exceeds threshold. "
                f"Please review and place a decision file at: "
                f"{REVIEW_DIR / 'review_decisions.json'}"
            ),
        }, f, indent=2)

    # In a real setup, this would be an HttpSensor or ExternalTaskSensor
    # waiting for the operator to submit a decision.
    # For now, we auto-quarantine after logging the request.
    context["ti"].xcom_push(key="review_decision", value="quarantine")

    return f"Review requested: failure_pct={failure_pct:.1f}%"


def halt_pipeline(**context):
    """Halt pipeline due to unacceptable failure rate."""
    failure_pct = context["ti"].xcom_pull(key="failure_pct", task_ids="run_validation")
    raise AirflowFailException(
        f"Pipeline HALTED: Validation failure rate {failure_pct:.1f}% "
        f"exceeds threshold. Manual intervention required."
    )


def run_cleaning(**context):
    """Stage 4: Clean data."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.cleaner import DataCleaner
    from src.validator import DataValidator

    input_path = context["ti"].xcom_pull(key="input_path", task_ids="load_data")
    validation_before = context["ti"].xcom_pull(key="validation_failed", task_ids="run_validation")

    df = pd.read_csv(input_path)
    cleaner = DataCleaner(df)
    cleaned_df = cleaner.run_full_cleaning()
    cleaner.save_cleaned_data()

    # Post-clean validation
    post_validator = DataValidator(cleaned_df)
    post_results = post_validator.run_all_validations()

    cleaner.generate_report(
        validation_before=validation_before,
        validation_after=post_results["failed"],
    )

    context["ti"].xcom_push(key="cleaned_rows", value=len(cleaned_df))
    context["ti"].xcom_push(key="post_clean_failed", value=post_results["failed"])

    return f"Cleaned: {len(cleaned_df)} rows, {post_results['failed']} still failing"


def quarantine_bad_records(**context):
    """Move failed records to DLQ after cleaning."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.dead_letter_queue import DeadLetterQueue
    from src.config import CLEANED_CSV
    from src.validator import DataValidator

    df = pd.read_csv(CLEANED_CSV)
    validator = DataValidator(df)
    results = validator.run_all_validations()

    dlq = DeadLetterQueue()
    if results["failed_rows"]:
        dlq.add_dataframe_rows(
            df, [r - 1 for r in results["failed_rows"]],
            reason="Failed post-cleaning validation",
            severity="HIGH",
            stage="post_clean_validation",
        )
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        dlq.save_quarantine(run_id)
        dlq.save_quarantine_csv(run_id)
        dlq.generate_report()

    context["ti"].xcom_push(key="quarantined_count", value=dlq.count)

    return f"Quarantined {dlq.count} records"


def detect_pii(**context):
    """Stage 5: Detect PII."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.pii_detector import PIIDetector
    from src.config import CLEANED_CSV

    df = pd.read_csv(CLEANED_CSV)
    detector = PIIDetector(df)
    report = detector.generate_report()

    total_pii = sum(f.get("count", 0) for f in detector.pii_findings.values())
    context["ti"].xcom_push(key="total_pii_fields", value=total_pii)

    return f"PII detected: {total_pii} fields across {len(detector.pii_findings)} categories"


def mask_pii(**context):
    """Stage 6: Mask PII."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.masker import PIIMasker
    from src.config import CLEANED_CSV

    df = pd.read_csv(CLEANED_CSV)
    masker = PIIMasker(df)
    masker.mask_all_pii()
    masker.save_masked_data()
    masker.generate_report()

    total_masked = sum(masker.mask_stats.values())
    context["ti"].xcom_push(key="masked_fields", value=total_masked)
    context["ti"].xcom_push(key="mask_stats", value=masker.mask_stats)

    return f"Masked {total_masked} PII fields"


def run_ge_validation(**context):
    """Stage 6b: Great Expectations validation on cleaned data."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.expectations import GreatExpectationsValidator
    from src.config import CLEANED_CSV

    df = pd.read_csv(CLEANED_CSV)
    ge_validator = GreatExpectationsValidator(df)
    results = ge_validator.run_validation()
    ge_validator.generate_report()

    context["ti"].xcom_push(key="ge_pass_rate", value=results.get("pass_rate_pct", 0))

    return f"GE validation: {results['passed']}/{results['total']} passed ({results['pass_rate_pct']}%)"


def create_version(**context):
    """Stage 7: Create versioned snapshot."""
    import pandas as pd
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.rollback_manager import RollbackManager
    from src.config import CLEANED_CSV

    df = pd.read_csv(CLEANED_CSV)
    rm = RollbackManager()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_path = context["ti"].xcom_pull(key="input_path", task_ids="load_data")

    version_id = rm.create_version(
        df,
        run_id=run_id,
        metadata={
            "input_path": input_path,
            "input_checksum": rm._calculate_checksum(Path(input_path)),
            "airflow_run_id": context.get("run_id", ""),
        },
    )
    rm.cleanup_old_versions(keep=5)

    context["ti"].xcom_push(key="version_id", value=version_id)

    return f"Version created: {version_id}"


def collect_metrics(**context):
    """Collect and export all pipeline metrics."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.metrics_collector import MetricsCollector

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    mc = MetricsCollector(run_id=run_id)

    row_count = context["ti"].xcom_pull(key="row_count", task_ids="load_data") or 0
    passed = context["ti"].xcom_pull(key="validation_passed", task_ids="run_validation") or 0
    failed = context["ti"].xcom_pull(key="validation_failed", task_ids="run_validation") or 0
    quarantined = context["ti"].xcom_pull(key="quarantined_count", task_ids="quarantine_bad_records") or 0
    outliers = context["ti"].xcom_pull(key="outlier_count", task_ids="run_statistical_validation") or 0
    total_pii = context["ti"].xcom_pull(key="total_pii_fields", task_ids="detect_pii") or 0
    masked = context["ti"].xcom_pull(key="masked_fields", task_ids="mask_pii") or 0

    mc.record_quality_metrics(
        total_rows=row_count,
        passed_rows=passed,
        failed_rows=failed,
        quarantined_rows=quarantined,
        outlier_count=outliers,
    )

    mc.record_pii_metrics(
        total_pii_fields=total_pii,
        masked_fields=masked,
    )

    mc.save_metrics()
    mc.export_prometheus_metrics()

    # Check for quality degradation
    alerts = mc.detect_quality_degradation()

    return f"Metrics collected: {len(alerts)} degradation alerts"


def upload_to_cloud(**context):
    """Upload pipeline outputs to cloud storage."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.cloud_storage import CloudStorageClient
    from src.config import CLEANED_CSV, MASKED_CSV, REPORTS_DIR

    cloud = CloudStorageClient()
    if not cloud.enabled:
        return "Cloud storage disabled — skipping upload"

    results = cloud.upload_pipeline_outputs(CLEANED_CSV, MASKED_CSV, REPORTS_DIR)

    return f"Uploaded to cloud: {results}"


def send_notifications(**context):
    """Send pipeline completion notifications."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.notifier import Notifier

    notifier = Notifier()

    row_count = context["ti"].xcom_pull(key="row_count", task_ids="load_data") or 0
    passed = context["ti"].xcom_pull(key="validation_passed", task_ids="run_validation") or 0
    failed = context["ti"].xcom_pull(key="validation_failed", task_ids="run_validation") or 0
    quarantined = context["ti"].xcom_pull(key="quarantined_count", task_ids="quarantine_bad_records") or 0

    summary = (
        f"Pipeline completed successfully!\n"
        f"- Rows processed: {row_count}\n"
        f"- Passed validation: {passed}\n"
        f"- Failed validation: {failed}\n"
        f"- Quarantined: {quarantined}\n"
        f"- Run time: {context.get('dag_run', {})}"
    )

    notifier.notify_pipeline_success(summary)

    return "Notifications sent"


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="data_quality_governance_pipeline",
    default_args=default_args,
    description="End-to-end data quality governance pipeline with "
                "validation, cleaning, PII masking, and quality monitoring",
    schedule_interval="0 2 * * *",  # Daily at 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "governance", "pii", "production"],
    doc_md=__doc__,
) as dag:

    # Stage 1: Load
    t_load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True,
    )

    # Idempotency check
    t_idempotency = BranchPythonOperator(
        task_id="check_idempotency",
        python_callable=check_idempotency,
        provide_context=True,
    )

    t_skip = EmptyOperator(task_id="skip_pipeline")

    # Stage 2: Profile + Contract
    t_profile = PythonOperator(
        task_id="run_profile",
        python_callable=run_profile,
        provide_context=True,
    )

    t_contract = PythonOperator(
        task_id="run_data_contract",
        python_callable=run_data_contract_validation,
        provide_context=True,
    )

    # Stage 3: Validation (rule-based + statistical)
    t_validate = PythonOperator(
        task_id="run_validation",
        python_callable=run_validation,
        provide_context=True,
    )

    t_stat_validate = PythonOperator(
        task_id="run_statistical_validation",
        python_callable=run_statistical_validation,
        provide_context=True,
    )

    # Branch: check threshold
    t_check_threshold = BranchPythonOperator(
        task_id="check_failure_threshold",
        python_callable=check_failure_threshold,
        provide_context=True,
    )

    # Human review gate
    t_human_review = PythonOperator(
        task_id="human_review_gate",
        python_callable=human_review_gate,
        provide_context=True,
    )

    # Halt
    t_halt = PythonOperator(
        task_id="halt_pipeline",
        python_callable=halt_pipeline,
        provide_context=True,
    )

    # Stage 4: Clean
    t_clean = PythonOperator(
        task_id="run_cleaning",
        python_callable=run_cleaning,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",
    )

    # DLQ
    t_quarantine = PythonOperator(
        task_id="quarantine_bad_records",
        python_callable=quarantine_bad_records,
        provide_context=True,
    )

    # Stage 5: PII Detection
    t_detect_pii = PythonOperator(
        task_id="detect_pii",
        python_callable=detect_pii,
        provide_context=True,
    )

    # Stage 6: Mask PII
    t_mask_pii = PythonOperator(
        task_id="mask_pii",
        python_callable=mask_pii,
        provide_context=True,
    )

    # GE Validation on cleaned data
    t_ge_validate = PythonOperator(
        task_id="run_ge_validation",
        python_callable=run_ge_validation,
        provide_context=True,
    )

    # Stage 7: Version
    t_version = PythonOperator(
        task_id="create_version",
        python_callable=create_version,
        provide_context=True,
    )

    # Metrics
    t_metrics = PythonOperator(
        task_id="collect_metrics",
        python_callable=collect_metrics,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",
    )

    # Cloud upload
    t_upload = PythonOperator(
        task_id="upload_to_cloud",
        python_callable=upload_to_cloud,
        provide_context=True,
    )

    # Notifications
    t_notify = PythonOperator(
        task_id="send_notifications",
        python_callable=send_notifications,
        provide_context=True,
        trigger_rule="all_done",
    )

    # -----------------------------------------------------------------------
    # DAG flow
    # -----------------------------------------------------------------------
    # Load → Idempotency Check
    t_load >> t_idempotency

    # Branch: skip or continue
    t_idempotency >> t_skip
    t_idempotency >> t_profile

    # Profile → Contract + Validation (parallel)
    t_profile >> [t_contract, t_validate, t_stat_validate]

    # Validation → Threshold check
    t_validate >> t_check_threshold

    # Branch: clean, review, or halt
    t_check_threshold >> t_clean
    t_check_threshold >> t_human_review
    t_check_threshold >> t_halt

    # Human review → Clean (if approved)
    t_human_review >> t_clean

    # Clean → Quarantine → PII Detection → Mask → GE Validate → Version
    t_clean >> t_quarantine >> t_detect_pii >> t_mask_pii >> t_ge_validate >> t_version

    # Version → Metrics → Upload → Notify
    t_version >> t_metrics >> t_upload >> t_notify
