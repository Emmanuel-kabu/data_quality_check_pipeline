"""
Cloud Storage Module
====================
Abstracts file I/O for S3 and GCS so the pipeline can read/write
data from cloud buckets in production while using local files in
development.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import CLOUD_STORAGE_CONFIG
from src.retry_handler import retry_with_backoff

logger = logging.getLogger(__name__)


class CloudStorageClient:
    """
    Unified interface for local, S3, and GCS file operations.
    Falls back to local filesystem when cloud storage is disabled.
    """

    def __init__(self) -> None:
        self.config = CLOUD_STORAGE_CONFIG
        self.enabled = self.config.get("enabled", False)
        self.provider = self.config.get("provider", "s3")
        self.bucket = self.config.get("bucket", "")
        self._client: Any = None

        if self.enabled:
            self._init_client()

    def _init_client(self) -> None:
        """Initialize the cloud storage client."""
        try:
            if self.provider == "s3":
                import boto3
                self._client = boto3.client(
                    "s3",
                    region_name=self.config.get("region", "us-east-1"),
                )
                logger.info("S3 client initialized (bucket: %s)", self.bucket)
            elif self.provider == "gcs":
                from google.cloud import storage
                self._client = storage.Client()
                logger.info("GCS client initialized (bucket: %s)", self.bucket)
            else:
                logger.error("Unknown cloud provider: %s", self.provider)
        except ImportError as exc:
            logger.warning(
                "Cloud storage library not installed (%s). "
                "Falling back to local storage. Install with: "
                "pip install boto3 (for S3) or pip install google-cloud-storage (for GCS)",
                exc,
            )
            self.enabled = False
        except Exception as exc:
            logger.error("Failed to initialize cloud client: %s", exc)
            self.enabled = False

    @retry_with_backoff(max_retries=3)
    def upload_file(self, local_path: Path, remote_prefix: str = "") -> str:
        """
        Upload a local file to cloud storage.
        Returns the remote path/URI.
        """
        if not self.enabled:
            logger.debug("Cloud storage disabled -- file stays local: %s", local_path)
            return str(local_path)

        # Add timestamp to filename for data lineage tracking
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = local_path.stem
        suffix = local_path.suffix
        timestamped_name = f"{stem}_{timestamp}{suffix}"
        remote_key = f"{remote_prefix}{timestamped_name}"

        try:
            if self.provider == "s3":
                self._client.upload_file(str(local_path), self.bucket, remote_key)
                uri = f"s3://{self.bucket}/{remote_key}"
            elif self.provider == "gcs":
                bucket = self._client.bucket(self.bucket)
                blob = bucket.blob(remote_key)
                blob.upload_from_filename(str(local_path))
                uri = f"gs://{self.bucket}/{remote_key}"
            else:
                return str(local_path)

            logger.info("Uploaded %s -> %s", local_path.name, uri)
            return uri
        except Exception as exc:
            logger.error("Upload failed for %s: %s", local_path, exc)
            raise

    @retry_with_backoff(max_retries=3)
    def download_file(self, remote_key: str, local_path: Path) -> Path:
        """
        Download a file from cloud storage to local path.
        Returns the local path.
        """
        if not self.enabled:
            logger.debug("Cloud storage disabled -- using local: %s", local_path)
            return local_path

        try:
            if self.provider == "s3":
                self._client.download_file(self.bucket, remote_key, str(local_path))
            elif self.provider == "gcs":
                bucket = self._client.bucket(self.bucket)
                blob = bucket.blob(remote_key)
                blob.download_to_filename(str(local_path))

            logger.info("Downloaded %s -> %s", remote_key, local_path)
            return local_path
        except Exception as exc:
            logger.error("Download failed for %s: %s", remote_key, exc)
            raise

    def upload_reports(self, reports_dir: Path) -> list[str]:
        """Upload all report files from the reports directory."""
        if not self.enabled:
            return []

        uploaded = []
        prefix = self.config.get("reports_prefix", "reports/")

        for report_file in reports_dir.glob("*.txt"):
            uri = self.upload_file(report_file, prefix)
            uploaded.append(uri)

        for report_file in reports_dir.glob("*.json"):
            uri = self.upload_file(report_file, prefix)
            uploaded.append(uri)

        logger.info("Uploaded %d report files to cloud", len(uploaded))
        return uploaded

    def upload_pipeline_outputs(
        self,
        cleaned_csv: Path,
        masked_csv: Path,
        reports_dir: Path,
    ) -> dict[str, str | list[str]]:
        """Upload all pipeline outputs to cloud storage."""
        results: dict[str, str | list[str]] = {}

        if cleaned_csv.exists():
            results["cleaned"] = self.upload_file(
                cleaned_csv,
                self.config.get("cleaned_prefix", "cleaned/"),
            )

        if masked_csv.exists():
            results["masked"] = self.upload_file(
                masked_csv,
                self.config.get("cleaned_prefix", "cleaned/"),
            )

        results["reports"] = self.upload_reports(reports_dir)

        return results
