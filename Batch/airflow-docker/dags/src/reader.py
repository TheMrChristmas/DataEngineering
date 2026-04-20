from pathlib import Path
import pyarrow.parquet as pq


class Reader:
    def __init__(self, base_path: str = "/opt/airflow/data/raw"):
        self.base_path = Path(base_path)

    def read(self, year_month: str):
        parquet_path = self.base_path / f"yellow_tripdata_{year_month}.parquet"

        if not parquet_path.exists():
            available = sorted(self.base_path.glob(
                "yellow_tripdata_*.parquet"))
            available_names = [p.name for p in available]
            raise FileNotFoundError(
                f"Missing file: {parquet_path}. Available files: {available_names}"
            )

        parquet_file = pq.ParquetFile(parquet_path)
        row_count = parquet_file.metadata.num_rows
        print(f"Found {row_count} rows in {parquet_path}")

        return str(parquet_path)   # pass path to next task via XCom
