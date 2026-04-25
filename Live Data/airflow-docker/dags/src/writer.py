from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Azure upload helper
# ---------------------------------------------------------------------------

def _upload_to_azure(local_path: Path) -> None:
    """Upload a single file to Azure Blob Storage."""
    from azure.storage.blob import BlobServiceClient

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    container = (
        os.getenv("AZURE_STORAGE_CONTAINER_LIVE")
        or os.getenv("AZURE_STORAGE_CONTAINER")
        or "live-data"
    )

    if not conn_str:
        raise EnvironmentError(
            "AZURE_STORAGE_CONNECTION_STRING is not set — cannot upload to Azure."
        )

    client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = client.get_blob_client(
        container=container, blob=local_path.name)

    with local_path.open("rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    logger.info(f"Uploaded to Azure Blob [{container}]: {local_path.name}")


def _maybe_upload(path: Path) -> None:
    """Upload file to Azure only when AZURE_STORAGE_UPLOAD_ENABLED=true."""
    enabled = os.getenv("AZURE_STORAGE_UPLOAD_ENABLED",
                        "false").strip().lower()
    if enabled != "true":
        return

    try:
        _upload_to_azure(path)
    except Exception as e:
        # Log but don't crash the pipeline — local output is already written
        logger.error(f"Azure upload failed for {path.name}: {e}")


# ---------------------------------------------------------------------------
# Main writer function
# ---------------------------------------------------------------------------

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

    # --- Local writes (unchanged) ---
    processed_df.to_csv(processed_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)

    combined_metrics = dict(metrics)
    combined_metrics["output_rows"] = int(len(processed_df))
    combined_metrics["invalid_rows_written"] = int(len(invalid_df))

    with summary_path.open("w", encoding="utf-8") as summary_file:
        json.dump(combined_metrics, summary_file, indent=2)

    logger.info(
        f"Written locally: {processed_path.name}, {summary_path.name}, {invalid_path.name}")

    # --- Azure uploads (only when enabled) ---
    _maybe_upload(processed_path)
    _maybe_upload(summary_path)
    _maybe_upload(invalid_path)

    return {
        "processed_csv_path": str(processed_path),
        "summary_json_path":  str(summary_path),
        "invalid_csv_path":   str(invalid_path),
        "source_file_path":   str(source),
    }
