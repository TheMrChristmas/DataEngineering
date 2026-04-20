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

    null_critical = int(df[CRITICAL_COLUMNS].isna().any(axis=1).sum())
    negative_cost = int((df["cost_eur"] < 0).sum())
    negative_stay = int((df["length_of_stay_days"] < 0).sum())
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
    if negative_stay > 0:
        raise ValueError(
            f"Backup validator failed: {negative_stay} rows with negative stay")
    if remaining_duplicates > 0:
        raise ValueError(
            f"Backup validator failed: {remaining_duplicates} duplicate keys remain")

    return {
        "backup_checked_rows": int(len(df)),
        "backup_null_critical": null_critical,
        "backup_negative_cost": negative_cost,
        "backup_negative_stay": negative_stay,
        "backup_remaining_duplicates": remaining_duplicates,
    }
