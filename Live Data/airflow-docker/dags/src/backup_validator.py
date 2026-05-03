from __future__ import annotations

import pandas as pd

CRITICAL_COLUMNS = [
    "patient_id",
    "admission_date",
    "discharge_date",
    "cost_eur",
    "length_of_stay_days",
    "age_group",
    "cost_per_day",
]

DERIVED_COLUMNS = ["age_group", "cost_per_day", "length_of_stay_days", "readmission"]

VALID_AGE_GROUPS = {"child", "young_adult", "adult", "middle_age", "senior"}

MAX_STAY_DAYS = 365


def run_backup_validation(df: pd.DataFrame, metrics: dict[str, int]) -> dict[str, int]:
    if df.empty:
        raise ValueError("Backup validator: no valid rows available after processing")

    # 1. Processed columns existence — derived columns must have been added by processor
    missing_derived = [col for col in DERIVED_COLUMNS if col not in df.columns]
    if missing_derived:
        raise ValueError(
            f"Backup validator failed: processor did not produce columns {missing_derived}"
        )

    # 2. Null check across critical + derived columns
    null_critical = int(df[CRITICAL_COLUMNS].isna().any(axis=1).sum())
    if null_critical > 0:
        raise ValueError(
            f"Backup validator failed: {null_critical} rows with null critical fields"
        )

    # 3. Row count reconciliation — verify no rows were silently lost or gained
    expected_rows = metrics["valid_rows"] - metrics["duplicates_removed"]
    actual_rows = len(df)
    if actual_rows != expected_rows:
        raise ValueError(
            f"Backup validator failed: row count mismatch — "
            f"expected {expected_rows} (valid_rows={metrics['valid_rows']} minus "
            f"duplicates_removed={metrics['duplicates_removed']}), got {actual_rows}"
        )

    # 4. Transformation correctness checks

    # 4a. length_of_stay_days must match the date difference
    admission = pd.to_datetime(df["admission_date"], errors="coerce")
    discharge = pd.to_datetime(df["discharge_date"], errors="coerce")
    expected_stay = (discharge - admission).dt.days
    stay_mismatch = int((df["length_of_stay_days"].astype("int64") != expected_stay).sum())
    if stay_mismatch > 0:
        raise ValueError(
            f"Backup validator failed: {stay_mismatch} rows where length_of_stay_days "
            f"does not match (discharge_date - admission_date)"
        )

    # 4b. cost_per_day must equal round(cost_eur / max(length_of_stay_days, 1), 2)
    divisor = df["length_of_stay_days"].astype("int64").clip(lower=1)
    expected_cpd = (df["cost_eur"] / divisor).round(2)
    cost_per_day_mismatch = int((df["cost_per_day"].sub(expected_cpd).abs() > 0.01).sum())
    if cost_per_day_mismatch > 0:
        raise ValueError(
            f"Backup validator failed: {cost_per_day_mismatch} rows where cost_per_day "
            f"does not match cost_eur / max(length_of_stay_days, 1)"
        )

    # 4c. age_group values must be one of the valid labels
    invalid_age_group = int(
        (~df["age_group"].astype(str).isin(VALID_AGE_GROUPS)).sum()
    )
    if invalid_age_group > 0:
        raise ValueError(
            f"Backup validator failed: {invalid_age_group} rows with invalid age_group value"
        )

    # 4d. readmission must be boolean (CSV round-trip produces "True"/"False" strings)
    valid_readmission = df["readmission"].isin([True, False, "True", "False"])
    invalid_readmission_type = int((~valid_readmission).sum())
    if invalid_readmission_type > 0:
        raise ValueError(
            f"Backup validator failed: {invalid_readmission_type} rows where readmission "
            f"was not converted to boolean by processor"
        )

    # 5. Hospital domain bounds

    # 5a. Length of stay must be within realistic hospital bounds
    stay_series = df["length_of_stay_days"].astype("int64")
    stay_out_of_bounds = int(((stay_series < 0) | (stay_series > MAX_STAY_DAYS)).sum())
    if stay_out_of_bounds > 0:
        raise ValueError(
            f"Backup validator failed: {stay_out_of_bounds} rows with length_of_stay_days "
            f"outside domain bounds [0, {MAX_STAY_DAYS}]"
        )

    # 5b. cost_per_day must be non-negative
    negative_cost_per_day = int((df["cost_per_day"] < 0).sum())
    if negative_cost_per_day > 0:
        raise ValueError(
            f"Backup validator failed: {negative_cost_per_day} rows with negative cost_per_day"
        )

    # 5c. cost_eur final safety net
    negative_cost = int((df["cost_eur"] < 0).sum())
    if negative_cost > 0:
        raise ValueError(
            f"Backup validator failed: {negative_cost} rows with negative cost_eur"
        )

    # 6. Duplicate final check
    remaining_duplicates = int(
        df.duplicated(subset=["patient_id", "admission_date", "discharge_date"]).sum()
    )
    if remaining_duplicates > 0:
        raise ValueError(
            f"Backup validator failed: {remaining_duplicates} duplicate keys remain"
        )

    return {
        "backup_checked_rows": actual_rows,
        "backup_null_critical": null_critical,
        "backup_row_count_expected": expected_rows,
        "backup_row_count_actual": actual_rows,
        "backup_stay_mismatch": stay_mismatch,
        "backup_cost_per_day_mismatch": cost_per_day_mismatch,
        "backup_invalid_age_group": invalid_age_group,
        "backup_invalid_readmission_type": invalid_readmission_type,
        "backup_stay_out_of_bounds": stay_out_of_bounds,
        "backup_negative_cost_per_day": negative_cost_per_day,
        "backup_negative_cost": negative_cost,
        "backup_remaining_duplicates": remaining_duplicates,
    }
