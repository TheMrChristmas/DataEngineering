from __future__ import annotations

import pandas as pd


CRITICAL_COLUMNS = [
    "patient_id",
    "admission_date",
    "discharge_date",
    "cost_eur",
    "length_of_stay_days",
]


def run_backup_validation(df: pd.DataFrame) -> dict[str, int]:
    if df.empty:
        raise ValueError(
            "Backup validator: no valid rows available after processing")

    missing_columns = [
        col for col in CRITICAL_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Backup validator failed: missing critical columns {missing_columns}"
        )

    cost_series = pd.to_numeric(df["cost_eur"], errors="coerce")
    stay_series = pd.to_numeric(df["length_of_stay_days"], errors="coerce")

    null_critical = int(df[CRITICAL_COLUMNS].isna().any(axis=1).sum())
    invalid_cost_type = int(
        (cost_series.isna() & df["cost_eur"].notna()).sum())
    invalid_stay_type = int(
        (stay_series.isna() & df["length_of_stay_days"].notna()).sum())
    negative_cost = int((cost_series < 0).sum())
    negative_stay = int((stay_series < 0).sum())
    remaining_duplicates = int(
        df.duplicated(
            subset=["patient_id", "admission_date", "discharge_date"]).sum()
    )

    if null_critical > 0:
        raise ValueError(
            f"Backup validator failed: {null_critical} rows with null critical fields")
    if negative_cost > 0:
        raise ValueError(
            f"Backup validator failed: {negative_cost} rows with negative cost")
    if invalid_cost_type > 0:
        raise ValueError(
            f"Backup validator failed: {invalid_cost_type} rows with non-numeric cost")
    if negative_stay > 0:
        raise ValueError(
            f"Backup validator failed: {negative_stay} rows with negative stay")
    if invalid_stay_type > 0:
        raise ValueError(
            f"Backup validator failed: {invalid_stay_type} rows with non-numeric stay")
    if remaining_duplicates > 0:
        raise ValueError(
            f"Backup validator failed: {remaining_duplicates} duplicate keys remain")

    return {
        "backup_checked_rows": int(len(df)),
        "backup_null_critical": null_critical,
        "backup_negative_cost": negative_cost,
        "backup_negative_stay": negative_stay,
        "backup_invalid_cost_type": invalid_cost_type,
        "backup_invalid_stay_type": invalid_stay_type,
        "backup_remaining_duplicates": remaining_duplicates,
    }
