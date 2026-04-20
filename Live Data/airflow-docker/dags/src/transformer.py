from __future__ import annotations


def _safe_float(value: str) -> float | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if cleaned == "":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _normalize_gender(value: str) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip().upper()
    mapping = {
        "M": "M",
        "MALE": "M",
        "F": "F",
        "FEMALE": "F",
    }
    return mapping.get(cleaned)


def transform_records(records: list[dict[str, str]]) -> list[dict[str, object]]:
    transformed: list[dict[str, object]] = []

    for record in records:
        cost_eur = _safe_float(record.get("cost_eur", ""))
        age = _safe_float(record.get("age", ""))
        gender = _normalize_gender(record.get("gender", ""))

        transformed_record = {
            "patient_id": (record.get("patient_id", "") or "").strip(),
            "name": (record.get("name", "") or "").strip(),
            "age": int(age) if age is not None else None,
            "gender": gender,
            "admission_date": (record.get("admission_date", "") or "").strip(),
            "discharge_date": (record.get("discharge_date", "") or "").strip(),
            "diagnosis": (record.get("diagnosis", "") or "").strip(),
            "ward": (record.get("ward", "") or "").strip(),
            "doctor_id": (record.get("doctor_id", "") or "").strip(),
            "cost_eur": cost_eur,
            "insurance": (record.get("insurance", "") or "").strip(),
            "readmission": str(record.get("readmission", "")).strip().lower() == "true",
        }

        transformed.append(transformed_record)

    return transformed
