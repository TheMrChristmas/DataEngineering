from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


def pick_next_input_file(input_dir: str, extension: str = ".csv") -> str | None:
    base = Path(input_dir)
    if not base.exists():
        return None

    pattern = f"*{extension.lower()}"
    files = sorted(
        [p for p in base.glob(pattern) if p.is_file()],
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        return None

    return str(files[0])


def _normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    return normalized.strip("_")


def read_csv_to_dataframe(input_path: str) -> pd.DataFrame:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() != ".csv":
        raise ValueError(
            f"Unsupported file type: {path.suffix}. This pipeline currently supports .csv")

    df = pd.read_csv(path)
    df.columns = [_normalize_column_name(c) for c in df.columns]
    return df
