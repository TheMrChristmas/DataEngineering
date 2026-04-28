from __future__ import annotations
from src.writer import write_outputs
from src.validator import validate_dataframe
from src.reader import (
    describe_input_file,
    read_csv_to_dataframe,
    save_processing_state,
)
from src.processor import process_dataframe
from src.backup_validator import run_backup_validation

import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, get_current_context, task

DAGS_DIR = Path(__file__).resolve().parent
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))


INPUT_DIR = "/opt/airflow/data/raw"
STAGING_DIR = "/opt/airflow/data/processed/_staging"
OUTPUT_DIR = "/opt/airflow/data/output"
ERROR_DIR = "/opt/airflow/data/error"
ARCHIVE_DIR = "/opt/airflow/data/archive/raw"
STATE_FILE = "/opt/airflow/data/processed/_state/hospital_admissions_last_processed.json"


def _archive_source_file(source_file: Path, archive_dir: str) -> str:
    if not source_file.exists():
        print(f"[Archive] Source file already deleted or moved: {source_file}")
        return str(source_file)

    archive_base = Path(archive_dir)
    archive_base.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived_name = f"{source_file.stem}_{timestamp}{source_file.suffix}"
    archived_path = archive_base / archived_name

    suffix = 1
    while archived_path.exists():
        archived_path = archive_base / (
            f"{source_file.stem}_{timestamp}_{suffix}{source_file.suffix}"
        )
        suffix += 1

    shutil.move(str(source_file), str(archived_path))
    return str(archived_path)


@dag(
    dag_id="hospital_admissions_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["hospital", "csv", "etl", "monitoring"],
)
def hospital_admissions_pipeline() -> None:
    @task(task_id="reader")
    def reader_task() -> dict[str, object]:
        context = get_current_context()
        dag_run = context["dag_run"]
        conf = dict(dag_run.conf or {})
        source_file = str(conf.get("source_file", "")).strip()
        if not source_file:
            raise AirflowSkipException(
                "No source_file provided. This DAG is meant to be started by the file watcher."
            )

        Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
        source = Path(source_file)
        if not source.exists():
            raise FileNotFoundError(f"Triggered source file no longer exists: {source}")

        source_snapshot = conf.get("source_snapshot")
        if not isinstance(source_snapshot, dict):
            source_snapshot = describe_input_file(str(source))

        archived_source = _archive_source_file(source, ARCHIVE_DIR)
        save_processing_state(
            STATE_FILE,
            {
                "status": "picked",
                "last_seen_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_snapshot": source_snapshot,
                "archived_source_file": archived_source,
            },
        )

        run_token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        raw_df = read_csv_to_dataframe(archived_source)

        read_path = Path(STAGING_DIR) / f"{source.stem}_{run_token}_read.csv"
        raw_df.to_csv(read_path, index=False)

        return {
            "source_file": str(source),
            "archived_source_file": archived_source,
            "read_path": str(read_path),
            "run_token": run_token,
            "source_snapshot": source_snapshot,
        }

    @task(task_id="validator")
    def validator_task(payload: dict[str, object]) -> dict[str, object]:
        df = pd.read_csv(payload["read_path"])
        valid_df, invalid_df, metrics = validate_dataframe(df)

        source = Path(payload["source_file"])
        run_token = payload["run_token"]
        valid_path = Path(STAGING_DIR) / f"{source.stem}_{run_token}_valid.csv"
        invalid_path = Path(STAGING_DIR) / \
            f"{source.stem}_{run_token}_invalid.csv"
        metrics_path = Path(STAGING_DIR) / \
            f"{source.stem}_{run_token}_metrics.json"

        valid_df.to_csv(valid_path, index=False)
        invalid_df.to_csv(invalid_path, index=False)
        metrics_path.write_text(json.dumps(metrics), encoding="utf-8")

        payload["valid_path"] = str(valid_path)
        payload["invalid_path"] = str(invalid_path)
        payload["metrics_path"] = str(metrics_path)
        return payload

    @task(task_id="processor")
    def processor_task(payload: dict[str, object]) -> dict[str, object]:
        df_valid = pd.read_csv(payload["valid_path"], parse_dates=[
                               "admission_date", "discharge_date"])
        processed_df, process_metrics = process_dataframe(df_valid)

        source = Path(payload["source_file"])
        run_token = payload["run_token"]
        processed_path = Path(STAGING_DIR) / \
            f"{source.stem}_{run_token}_processed.csv"
        processed_df.to_csv(processed_path, index=False)

        metrics = json.loads(
            Path(payload["metrics_path"]).read_text(encoding="utf-8"))
        metrics.update(process_metrics)
        Path(payload["metrics_path"]).write_text(
            json.dumps(metrics), encoding="utf-8")

        payload["processed_path"] = str(processed_path)
        return payload

    @task(task_id="backup_validator")
    def backup_validator_task(payload: dict[str, object]) -> dict[str, object]:
        df_processed = pd.read_csv(payload["processed_path"])
        backup_metrics = run_backup_validation(df_processed)

        metrics = json.loads(
            Path(payload["metrics_path"]).read_text(encoding="utf-8"))
        metrics.update(backup_metrics)
        Path(payload["metrics_path"]).write_text(
            json.dumps(metrics), encoding="utf-8")

        return payload

    @task(task_id="writer")
    def writer_task(payload: dict[str, object]) -> dict[str, str]:
        metrics = json.loads(
            Path(payload["metrics_path"]).read_text(encoding="utf-8"))
        processed_df = pd.read_csv(payload["processed_path"])
        invalid_df = pd.read_csv(payload["invalid_path"])

        write_result = write_outputs(
            processed_df=processed_df,
            invalid_df=invalid_df,
            metrics=metrics,
            source_file_path=payload["source_file"],
            output_dir=OUTPUT_DIR,
            error_dir=ERROR_DIR,
        )

        archived_source = str(payload["archived_source_file"])
        save_processing_state(
            STATE_FILE,
            {
                "status": "succeeded",
                "last_seen_at_utc": datetime.now(timezone.utc).isoformat(),
                "processed_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_snapshot": payload["source_snapshot"],
                "archived_source_file": archived_source,
                "processed_csv_path": write_result["processed_csv_path"],
                "summary_json_path": write_result["summary_json_path"],
                "invalid_csv_path": write_result["invalid_csv_path"],
            },
        )

        write_result["archived_source_file"] = archived_source
        write_result["state_file"] = STATE_FILE
        return write_result

    writer_task(backup_validator_task(
        processor_task(validator_task(reader_task()))))


hospital_admissions_pipeline()
