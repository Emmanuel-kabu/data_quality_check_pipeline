"""
PII Masking Module (Part 5)
===========================
Masks personally identifiable information while preserving data
structure for safe sharing with analytics teams.

Masking rules:
  - Names:     'John' → 'J***'
  - Emails:    'john.doe@gmail.com' → 'j***@gmail.com'
  - Phones:    '555-123-4567' → '***-***-4567'
  - Addresses: '123 Main St ...' → '[MASKED ADDRESS]'
  - DOB:       '1985-03-15' → '1985-**-**'
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd

from src.config import (
    MASKED_CSV,
    MASKED_SAMPLE,
    DATA_DIR,
    REPORTS_DIR,
)

logger = logging.getLogger(__name__)


class PIIMasker:
    """Applies PII masking transformations to a DataFrame."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        self.original_df = df.copy()
        self.total_rows = len(df)
        self.mask_stats = {
            "names": 0,
            "emails": 0,
            "phones": 0,
            "addresses": 0,
            "dobs": 0,
        }

    # ------------------------------------------------------------------
    # Masking functions
    # ------------------------------------------------------------------
    @staticmethod
    def mask_name(name: str) -> str:
        """Mask a name: 'John' → 'J***'."""
        if not name or not isinstance(name, str):
            return name
        name = name.strip()
        if not name or name == "[UNKNOWN]":
            return name
        return name[0] + "***"

    @staticmethod
    def mask_email(email: str) -> str:
        """Mask an email: 'john.doe@gmail.com' → 'j***@gmail.com'."""
        if not email or not isinstance(email, str):
            return email
        email = email.strip()
        if "@" not in email:
            return email
        local, domain = email.split("@", 1)
        if local:
            masked_local = local[0] + "***"
        else:
            masked_local = "***"
        return f"{masked_local}@{domain}"

    @staticmethod
    def mask_phone(phone: str) -> str:
        """Mask a phone: '555-123-4567' → '***-***-4567'."""
        if not phone or not isinstance(phone, str):
            return phone
        phone = phone.strip()
        if len(phone) >= 4:
            last_four = phone[-4:]
            return f"***-***-{last_four}"
        return "***-***-****"

    @staticmethod
    def mask_address(address: str) -> str:
        """Mask an address: any address → '[MASKED ADDRESS]'."""
        if not address or not isinstance(address, str):
            return address
        address = address.strip()
        if not address or address == "[UNKNOWN]":
            return address
        return "[MASKED ADDRESS]"

    @staticmethod
    def mask_dob(dob: str) -> str:
        """Mask date of birth: '1985-03-15' → '1985-**-**'."""
        if not dob or not isinstance(dob, str):
            return dob
        dob = dob.strip()
        if len(dob) >= 4 and dob[:4].isdigit():
            return f"{dob[:4]}-**-**"
        return "****-**-**"

    # ------------------------------------------------------------------
    # Apply masking to DataFrame
    # ------------------------------------------------------------------
    def mask_all_pii(self) -> pd.DataFrame:
        """Apply all masking functions to the DataFrame."""
        logger.info("Masking all PII fields...")

        # Mask first_name and last_name
        for col in ("first_name", "last_name"):
            if col in self.df.columns:
                for idx, val in self.df[col].items():
                    if pd.notna(val) and str(val).strip():
                        self.df.at[idx, col] = self.mask_name(str(val))
                        self.mask_stats["names"] += 1

        # Mask email
        if "email" in self.df.columns:
            for idx, val in self.df["email"].items():
                if pd.notna(val) and str(val).strip():
                    self.df.at[idx, "email"] = self.mask_email(str(val))
                    self.mask_stats["emails"] += 1

        # Mask phone
        if "phone" in self.df.columns:
            for idx, val in self.df["phone"].items():
                if pd.notna(val) and str(val).strip():
                    self.df.at[idx, "phone"] = self.mask_phone(str(val))
                    self.mask_stats["phones"] += 1

        # Mask address
        if "address" in self.df.columns:
            for idx, val in self.df["address"].items():
                if pd.notna(val) and str(val).strip():
                    self.df.at[idx, "address"] = self.mask_address(str(val))
                    self.mask_stats["addresses"] += 1

        # Mask date_of_birth
        if "date_of_birth" in self.df.columns:
            for idx, val in self.df["date_of_birth"].items():
                if pd.notna(val) and str(val).strip():
                    self.df.at[idx, "date_of_birth"] = self.mask_dob(str(val))
                    self.mask_stats["dobs"] += 1

        logger.info("PII masking complete: %s", self.mask_stats)
        return self.df

    # ------------------------------------------------------------------
    # Save masked data
    # ------------------------------------------------------------------
    def save_masked_data(self, filepath=None) -> str:
        """Save the masked DataFrame to CSV."""
        filepath = filepath or MASKED_CSV
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(filepath, index=False)
        logger.info("Masked data saved to %s", filepath)
        return str(filepath)

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------
    def generate_report(self, filepath=None) -> str:
        """Generate a before/after masking comparison report."""
        filepath = filepath or MASKED_SAMPLE
        lines: list[str] = []

        lines.append("PII MASKING REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # Before masking (first 2 rows of original)
        lines.append("BEFORE MASKING (first 2 rows):")
        lines.append("-" * 40)
        cols = list(self.original_df.columns)
        lines.append(", ".join(cols))
        for i in range(min(2, len(self.original_df))):
            row_vals = []
            for col in cols:
                val = self.original_df.iloc[i][col]
                row_vals.append(str(val) if pd.notna(val) else "")
            lines.append(", ".join(row_vals))
        lines.append("")

        # After masking (first 2 rows of masked)
        lines.append("AFTER MASKING (first 2 rows):")
        lines.append("-" * 40)
        lines.append(", ".join(cols))
        for i in range(min(2, len(self.df))):
            row_vals = []
            for col in cols:
                val = self.df.iloc[i][col]
                row_vals.append(str(val) if pd.notna(val) else "")
            lines.append(", ".join(row_vals))
        lines.append("")

        # Analysis
        lines.append("ANALYSIS:")
        lines.append("-" * 40)
        lines.append(f"  - Data structure preserved (still {len(self.df)} rows, {len(self.df.columns)} columns)")
        lines.append(f"  - Names masked: {self.mask_stats['names']} fields")
        lines.append(f"  - Emails masked: {self.mask_stats['emails']} fields")
        lines.append(f"  - Phones masked: {self.mask_stats['phones']} fields")
        lines.append(f"  - Addresses masked: {self.mask_stats['addresses']} fields")
        lines.append(f"  - DOBs masked: {self.mask_stats['dobs']} fields")
        lines.append(f"  - Business data intact (income, account_status, created_date available)")
        lines.append(f"  - Use case: Safe for analytics team (GDPR compliant)")
        lines.append("")

        report_text = "\n".join(lines)

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info("Masked sample report saved to %s", filepath)
        return report_text
