from __future__ import annotations

import hashlib
import json
from pathlib import Path
import os
import re
import time

import pandas as pd


DEFAULT_MIN_STABLE_SECONDS = int(os.getenv("INPUT_FILE_MIN_AGE_SECONDS", "15"))
MAX_INPUT_FILE_BYTES = int(os.getenv("MAX_INPUT_FILE_BYTES", "52428800"))


def _normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    return normalized.strip("_")


def _hash_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _is_stable_file(path: Path, base_dir: Path, min_stable_seconds: int) -> bool:
    try:
        if not path.is_file():
            return False

        resolved = path.resolve()
        if base_dir not in resolved.parents:
            return False

        size = path.stat().st_size
        if size <= 0 or size > MAX_INPUT_FILE_BYTES:
            return False
        age_seconds = time.time() - path.stat().st_mtime
        return age_seconds >= min_stable_seconds
    except OSError:
        return False


def pick_next_input_file(
    input_dir: str,
    extension: str = ".csv",
    min_stable_seconds: int | None = None,
) -> str | None:
    base = Path(input_dir)
    if not base.exists():
        return None

    stable_seconds = (
        DEFAULT_MIN_STABLE_SECONDS
        if min_stable_seconds is None
        else int(min_stable_seconds)
    )
    base_resolved = base.resolve()

    pattern = f"*{extension.lower()}"
    files = sorted(
        [
            p for p in base.glob(pattern)
            if _is_stable_file(p, base_resolved, stable_seconds)
        ],
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        return None

    return str(files[0])


def describe_input_file(input_path: str) -> dict[str, object]:
    path = Path(input_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    stat = path.stat()
    return {
        "path": str(path),
        "name": path.name,
        "extension": path.suffix.lower(),
        "size_bytes": int(stat.st_size),
        "modified_at_ns": int(stat.st_mtime_ns),
        "sha256": _hash_file(path),
    }


def load_processing_state(state_path: str) -> dict[str, object]:
    path = Path(state_path)
    if not path.exists():
        return {}

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_processing_state(state_path: str, state: dict[str, object]) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def read_csv_to_dataframe(input_path: str) -> pd.DataFrame:
    path = Path(input_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() != ".csv":
        raise ValueError(
            f"Unsupported file type: {path.suffix}. This pipeline currently supports .csv")

    size = path.stat().st_size
    if size <= 0:
        raise ValueError(f"Input file is empty: {path}")
    if size > MAX_INPUT_FILE_BYTES:
        raise ValueError(
            f"Input file too large ({size} bytes), max allowed is {MAX_INPUT_FILE_BYTES}"
        )

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Input CSV has no rows: {path}")

    df.columns = [_normalize_column_name(c) for c in df.columns]

    duplicated_columns = pd.Index(
        df.columns)[pd.Index(df.columns).duplicated()].unique()
    if len(duplicated_columns) > 0:
        raise ValueError(
            f"Duplicate column names after normalization: {list(duplicated_columns)}"
        )

    if len(df.columns) > 200:
        raise ValueError(
            f"Unexpectedly large number of columns ({len(df.columns)}); possible malformed CSV"
        )

    return df
