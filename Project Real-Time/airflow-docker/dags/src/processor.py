from __future__ import annotations

import pandas as pd


def process_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    work = df.copy()

    work["gender"] = work["gender"].replace({"MALE": "M", "FEMALE": "F"})
    work["readmission"] = work["readmission"].eq("true")

    work["length_of_stay_days"] = (
        (work["discharge_date"] - work["admission_date"]).dt.days.astype("int64")
    )
    work["age_group"] = pd.cut(
        work["age"],
        bins=[0, 18, 35, 50, 65, 120],
        labels=["child", "young_adult", "adult", "middle_age", "senior"],
        include_lowest=True,
    )
    work["cost_per_day"] = (
        work["cost_eur"] / work["length_of_stay_days"].replace(0, 1)).round(2)

    before = len(work)
    work = work.drop_duplicates(
        subset=["patient_id", "admission_date", "discharge_date"], keep="first")
    after = len(work)

    metrics = {
        "duplicates_removed": int(before - after),
        "processed_rows": int(after),
    }

    work["admission_date"] = work["admission_date"].dt.strftime("%Y-%m-%d")
    work["discharge_date"] = work["discharge_date"].dt.strftime("%Y-%m-%d")

    return work, metrics
