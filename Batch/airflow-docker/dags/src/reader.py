from pathlib import Path
import re
import pyarrow.parquet as pq


YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class Reader:
    def __init__(self, base_path: str = "/opt/airflow/data/raw"):
        self.base_path = Path(base_path).resolve()

    def _build_input_path(self, year_month: str) -> Path:
        if not YEAR_MONTH_PATTERN.match(year_month):
            raise ValueError(
                f"Invalid year_month '{year_month}'. Expected format YYYY-MM"
            )

        parquet_path = (
            self.base_path / f"yellow_tripdata_{year_month}.parquet"
        ).resolve()
        if self.base_path not in parquet_path.parents:
            raise ValueError(f"Unsafe input path detected: {parquet_path}")

        return parquet_path

    def read(self, year_month: str):
        parquet_path = self._build_input_path(year_month)

        if not parquet_path.exists():
            available = sorted(self.base_path.glob(
                "yellow_tripdata_*.parquet"))
            available_names = [p.name for p in available]
            raise FileNotFoundError(
                f"Missing file: {parquet_path}. Available files: {available_names}"
            )

        if parquet_path.stat().st_size == 0:
            raise ValueError(f"Input parquet file is empty: {parquet_path}")

        parquet_file = pq.ParquetFile(parquet_path)
        row_count = parquet_file.metadata.num_rows
        if row_count <= 0:
            raise ValueError(f"Input parquet contains 0 rows: {parquet_path}")

        print(f"Found {row_count} rows in {parquet_path}")

        return str(parquet_path)   # pass path to next task via XCom
