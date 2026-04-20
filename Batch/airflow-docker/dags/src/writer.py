import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class Writer:
    def __init__(self, output_path: str = "/opt/airflow/data/output"):
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.azure_upload_enabled = os.getenv(
            "AZURE_STORAGE_UPLOAD_ENABLED", "false"
        ).strip().lower() == "true"

    def write(self, parquet_path: str) -> str:
        source_path = Path(parquet_path)
        if not source_path.exists():
            raise FileNotFoundError(
                f"[Writer] Input parquet not found: {source_path}")

        local_file = self._write_local_copy(source_path)
        if self.azure_upload_enabled:
            self._upload_to_azure(local_file)
        else:
            print("[Writer] Azure upload disabled; skipping cloud upload")
        return str(local_file)

    def _write_local_copy(self, source_path: Path) -> Path:
        output_file = self.output_path / source_path.name
        source_file = pq.ParquetFile(source_path)

        writer = None
        rows_written = 0
        try:
            for batch in source_file.iter_batches(batch_size=50_000):
                df = pd.DataFrame(batch.to_pandas())
                table = pa.Table.from_pandas(df, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(output_file, table.schema)

                writer.write_table(table)
                rows_written += len(df)
        finally:
            if writer is not None:
                writer.close()

        if rows_written == 0:
            raise ValueError(
                f"[Writer] Input parquet contains 0 rows: {source_path}")

        print(f"[Writer] Local output written to {output_file}")
        print(f"[Writer] Local output rows: {rows_written}")
        return output_file

    def _upload_to_azure(self, local_file: Path) -> None:
        try:
            from azure.core.exceptions import ResourceExistsError
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise ImportError(
                "[Writer] Azure upload enabled but azure-storage-blob is not installed"
            ) from exc

        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER")
        blob_prefix = os.getenv("AZURE_STORAGE_BLOB_PREFIX", "yellow-taxi")

        if not connection_string:
            raise ValueError(
                "[Writer] Missing AZURE_STORAGE_CONNECTION_STRING environment variable"
            )
        if not container_name:
            raise ValueError(
                "[Writer] Missing AZURE_STORAGE_CONTAINER environment variable"
            )

        blob_service = BlobServiceClient.from_connection_string(
            connection_string)
        container_client = blob_service.get_container_client(container_name)

        try:
            container_client.create_container()
            print(f"[Writer] Created Azure container: {container_name}")
        except ResourceExistsError:
            # Container already exists; continue.
            pass

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        blob_name = f"{blob_prefix}/{local_file.stem}_{timestamp}{local_file.suffix}"

        with local_file.open("rb") as data:
            container_client.upload_blob(
                name=blob_name, data=data, overwrite=True)

        schema_field_count = len(pq.ParquetFile(local_file).schema_arrow.names)
        print(f"[Writer] Uploaded to Azure Blob: {container_name}/{blob_name}")
        print(f"[Writer] Uploaded parquet schema fields: {schema_field_count}")
