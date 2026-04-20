from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def write_outputs(
        processed_df: pd.DataFrame,
        invalid_df: pd.DataFrame,
        metrics: dict[str, int],
        source_file_path: str,
        output_dir: str,
        error_dir: str,
) -> dict[str, str]:
    source = Path(source_file_path)
    base_name = source.stem

    output_base = Path(output_dir)
    error_base = Path(error_dir)

    output_base.mkdir(parents=True, exist_ok=True)
    error_base.mkdir(parents=True, exist_ok=True)

    processed_path = output_base / f"{base_name}_clean.csv"
    summary_path = output_base / f"{base_name}_summary.json"
    invalid_path = error_base / f"{base_name}_invalid.csv"

    processed_df.to_csv(processed_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)

    combined_metrics = dict(metrics)
    combined_metrics["output_rows"] = int(len(processed_df))
    combined_metrics["invalid_rows_written"] = int(len(invalid_df))

    with summary_path.open("w", encoding="utf-8") as summary_file:
        json.dump(combined_metrics, summary_file, indent=2)

    return {
        "processed_csv_path": str(processed_path),
        "summary_json_path": str(summary_path),
        "invalid_csv_path": str(invalid_path),
        "source_file_path": str(source),
    }
