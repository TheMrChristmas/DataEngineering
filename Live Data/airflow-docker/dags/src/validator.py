from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = [
    "patient_id",
    "name",
    "age",
    "gender",
    "admission_date",
    "discharge_date",
    "diagnosis",
    "ward",
    "doctor_id",
    "cost_eur",
    "insurance",
    "readmission",
]


def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    missing_columns = [
        col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    work = df.copy()

    work["patient_id"] = work["patient_id"].astype("string").str.strip()
    work["name"] = work["name"].astype("string").str.strip()
    work["gender"] = work["gender"].astype("string").str.strip().str.upper()
    work["gender"] = work["gender"].replace({"MALE": "M", "FEMALE": "F"})
    work["admission_date"] = pd.to_datetime(
        work["admission_date"], errors="coerce")
    work["discharge_date"] = pd.to_datetime(
        work["discharge_date"], errors="coerce")
    work["diagnosis"] = work["diagnosis"].astype("string").str.strip()
    work["ward"] = work["ward"].astype("string").str.strip()
    work["doctor_id"] = work["doctor_id"].astype("string").str.strip()
    work["cost_eur"] = pd.to_numeric(work["cost_eur"], errors="coerce")
    work["age"] = pd.to_numeric(work["age"], errors="coerce")
    work["insurance"] = work["insurance"].astype("string").str.strip()
    work["readmission"] = work["readmission"].astype(
        "string").str.strip().str.lower()

    valid_gender = work["gender"].isin(["M", "F"])
    valid_readmission = work["readmission"].isin(["true", "false"])
    valid_patient = work["patient_id"].notna(
    ) & work["patient_id"].str.match(r"^P\d{4}$", na=False)
    valid_doctor = work["doctor_id"].notna(
    ) & work["doctor_id"].str.match(r"^DR\d{3}$", na=False)
    valid_age = work["age"].between(0, 120, inclusive="both")
    valid_cost = work["cost_eur"] >= 0
    valid_dates = work["admission_date"].notna() & work["discharge_date"].notna() & (
        work["discharge_date"] >= work["admission_date"])
    valid_required_text = (
        work["name"].notna()
        & work["name"].ne("")
        & work["diagnosis"].notna()
        & work["diagnosis"].ne("")
        & work["ward"].notna()
        & work["ward"].ne("")
        & work["insurance"].notna()
        & work["insurance"].ne("")
    )

    row_is_valid = (
        valid_gender
        & valid_readmission
        & valid_patient
        & valid_doctor
        & valid_age
        & valid_cost
        & valid_dates
        & valid_required_text
    ).fillna(False)

    valid_df = work.loc[row_is_valid].copy()
    invalid_df = work.loc[~row_is_valid].copy()

    metrics = {
        "total_rows": int(len(work)),
        "valid_rows": int(len(valid_df)),
        "invalid_rows": int(len(invalid_df)),
    }

    return valid_df, invalid_df, metrics
