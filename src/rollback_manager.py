"""
Rollback & Versioning Module
=============================
Maintains versioned snapshots of cleaned datasets so the pipeline
can roll back to a previous known-good state if needed.
"""

import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import VERSIONS_DIR, CLEANED_CSV, DATA_DIR

logger = logging.getLogger(__name__)


class RollbackManager:
    """Manages versioned snapshots of pipeline outputs."""

    def __init__(self) -> None:
        VERSIONS_DIR.mkdir(parents=True, exist_ok=True)
        self.versions_manifest = VERSIONS_DIR / "manifest.json"
        self.manifest = self._load_manifest()

    # ------------------------------------------------------------------
    # Manifest management
    # ------------------------------------------------------------------
    def _load_manifest(self) -> dict:
        """Load or initialize the versions manifest."""
        if self.versions_manifest.exists():
            with open(self.versions_manifest, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"versions": [], "current": None}

    def _save_manifest(self) -> None:
        """Save the versions manifest."""
        with open(self.versions_manifest, "w", encoding="utf-8") as f:
            json.dump(self.manifest, f, indent=2, default=str)

    # ------------------------------------------------------------------
    # Versioning
    # ------------------------------------------------------------------
    def create_version(
        self,
        df: pd.DataFrame,
        run_id: str = "",
        metadata: dict | None = None,
    ) -> str:
        """
        Create a versioned snapshot of the cleaned dataset.
        Returns the version ID.
        """
        if not run_id:
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        version_id = f"v_{run_id}"
        version_dir = VERSIONS_DIR / version_id
        version_dir.mkdir(parents=True, exist_ok=True)

        # Save the dataset
        csv_path = version_dir / "customers_cleaned.csv"
        df.to_csv(csv_path, index=False)

        # Calculate checksum
        checksum = self._calculate_checksum(csv_path)

        # Save metadata
        version_meta = {
            "version_id": version_id,
            "created_at": datetime.now().isoformat(),
            "run_id": run_id,
            "rows": len(df),
            "columns": len(df.columns),
            "checksum": checksum,
            "file_path": str(csv_path),
            "metadata": metadata or {},
        }

        meta_path = version_dir / "metadata.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(version_meta, f, indent=2, default=str)

        # Update manifest
        self.manifest["versions"].append(version_meta)
        self.manifest["current"] = version_id
        self._save_manifest()

        logger.info("Version created: %s (%d rows, checksum: %s)", version_id, len(df), checksum[:12])
        return version_id

    def rollback(self, version_id: str | None = None) -> pd.DataFrame | None:
        """
        Roll back to a specific version (or the previous version).
        Copies the versioned file back to the main cleaned CSV location.
        """
        if version_id is None:
            # Roll back to previous version
            versions = self.manifest.get("versions", [])
            if len(versions) < 2:
                logger.error("No previous version to roll back to")
                return None
            version_id = versions[-2]["version_id"]

        # Find the version
        version_meta = None
        for v in self.manifest["versions"]:
            if v["version_id"] == version_id:
                version_meta = v
                break

        if version_meta is None:
            logger.error("Version not found: %s", version_id)
            return None

        version_csv = Path(version_meta["file_path"])
        if not version_csv.exists():
            logger.error("Version file missing: %s", version_csv)
            return None

        # Copy back to main location
        shutil.copy2(version_csv, CLEANED_CSV)
        self.manifest["current"] = version_id
        self._save_manifest()

        df = pd.read_csv(CLEANED_CSV)
        logger.info("Rolled back to %s (%d rows)", version_id, len(df))
        return df

    def get_current_version(self) -> str | None:
        """Get the current active version ID."""
        return self.manifest.get("current")

    def list_versions(self) -> list[dict]:
        """List all available versions."""
        return self.manifest.get("versions", [])

    def check_idempotency(self, input_path: Path) -> bool:
        """
        Check if the input file has changed since the last run.
        Returns True if the pipeline needs to run (input changed).
        """
        if not input_path.exists():
            return True

        current_checksum = self._calculate_checksum(input_path)
        versions = self.manifest.get("versions", [])

        if not versions:
            return True  # No previous runs

        last_version = versions[-1]
        last_input_checksum = last_version.get("metadata", {}).get("input_checksum")

        if last_input_checksum == current_checksum:
            logger.info("Input unchanged (checksum match) -- pipeline run is idempotent")
            return False

        return True

    def cleanup_old_versions(self, keep: int = 5) -> int:
        """Remove old versions, keeping the most recent `keep` versions."""
        versions = self.manifest.get("versions", [])
        if len(versions) <= keep:
            return 0

        to_remove = versions[:-keep]
        removed = 0

        for v in to_remove:
            version_dir = VERSIONS_DIR / v["version_id"]
            if version_dir.exists():
                shutil.rmtree(version_dir)
                removed += 1

        self.manifest["versions"] = versions[-keep:]
        self._save_manifest()

        logger.info("Cleaned up %d old versions (kept %d)", removed, keep)
        return removed

    @staticmethod
    def _calculate_checksum(filepath: Path) -> str:
        """Calculate SHA-256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
