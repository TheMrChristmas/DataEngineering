from pathlib import Path
import gc
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np


REQUIRED_INPUT_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

TRANSFORM_BATCH_SIZE = int(os.getenv("TRANSFORM_BATCH_SIZE", "5000"))


class Transformer:
    def __init__(self, output_path: str = "/opt/airflow/data/processed"):
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

    def process(self, parquet_path: str) -> str:
        input_path = Path(parquet_path).resolve()
        if not input_path.exists():
            raise FileNotFoundError(
                f"[Transformer] Input parquet not found: {input_path}")
        if input_path.suffix.lower() != ".parquet":
            raise ValueError(
                f"[Transformer] Unsupported input extension: {input_path.suffix}")
        if input_path.stat().st_size == 0:
            raise ValueError(
                f"[Transformer] Input parquet file is empty: {input_path}")

        print(f"[Transformer] Reading {input_path}")
        input_file = pq.ParquetFile(input_path)

        missing = [
            col for col in REQUIRED_INPUT_COLUMNS if col not in input_file.schema.names
        ]
        if missing:
            raise ValueError(
                f"[Transformer] Missing required columns in input parquet: {missing}"
            )

        filename = input_path.name
        output_path = self.output_path / filename
        temp_output_path = output_path.with_name(f".{output_path.name}.tmp")
        if temp_output_path.exists():
            temp_output_path.unlink()

        writer = None
        writer_schema = None
        rows_written = 0
        failed = False

        try:
            for batch_index, batch in enumerate(
                input_file.iter_batches(batch_size=TRANSFORM_BATCH_SIZE), start=1
            ):
                df = self._to_pandas(batch)
                df = self._transform_batch(df)

                table = pa.Table.from_pandas(df, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(temp_output_path, table.schema)
                    writer_schema = table.schema
                else:
                    table = table.cast(writer_schema)

                writer.write_table(table)
                rows_written += len(df)

                if batch_index % 10 == 0:
                    print(
                        f"[Transformer] Processed {rows_written} rows so far")

                del table, df, batch
                gc.collect()

        except Exception:
            failed = True
            raise
        finally:
            if writer is not None:
                writer.close()
            if failed and temp_output_path.exists():
                temp_output_path.unlink()

        if rows_written <= 0:
            if temp_output_path.exists():
                temp_output_path.unlink()
            raise ValueError(
                f"[Transformer] No rows written for input: {input_path}")

        temp_output_path.replace(output_path)
        print(f"[Transformer] Written {rows_written} rows to {output_path}")
        return str(output_path)

    def _transform_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        for dt_col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
            if dt_col in df.columns:
                df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")

        for numeric_col in ("trip_distance", "fare_amount", "total_amount"):
            if numeric_col in df.columns:
                df[numeric_col] = pd.to_numeric(
                    df[numeric_col], errors="coerce")

        df = self._remove_columns(df)
        df = self._add_trip_duration(df)
        df = self._add_average_speed(df)
        df = self._add_pickup_date_parts(df)
        df = self._add_revenue_per_mile(df)
        df = self._add_trip_distance_category(df)
        df = self._add_fare_category(df)
        df = self._add_time_of_day(df)

        if "passenger_count" in df.columns:
            df["passenger_count"] = pd.to_numeric(
                df["passenger_count"], errors="coerce"
            )

        df = self._optimize_numeric_types(df)

        return df

    def _to_pandas(self, batch) -> pd.DataFrame:
        try:
            return batch.to_pandas(split_blocks=True, self_destruct=True)
        except TypeError:
            return batch.to_pandas(split_blocks=True)

    def _optimize_numeric_types(self, df: pd.DataFrame) -> pd.DataFrame:
        float_cols = [
            "trip_distance",
            "fare_amount",
            "total_amount",
            "passenger_count",
            "trip_duration_minutes",
            "average_speed_mph",
            "revenue_per_mile",
        ]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

        if "pickup_year" in df.columns:
            df["pickup_year"] = df["pickup_year"].astype("Int16")
        if "pickup_month" in df.columns:
            df["pickup_month"] = df["pickup_month"].astype("Int8")

        return df

    #  Step 1 — Remove columns                                            #

    def _remove_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_to_drop = ["VendorID", "store_and_fwd_flag", "RatecodeID"]
        existing = [c for c in cols_to_drop if c in df.columns]
        dropped = [c for c in cols_to_drop if c not in df.columns]

        if dropped:
            print(
                f"[Transformer] WARNING: columns not found, skipping drop: {dropped}")

        df.drop(columns=existing, inplace=True)
        print(f"[Transformer] Dropped columns: {existing}")
        return df

    #  Step 2 — trip_duration_minutes                                     #

    def _add_trip_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        df["trip_duration_minutes"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds()
            / 60
        )
        print("[Transformer] Added column: trip_duration_minutes")
        return df

    #  Step 3 — average_speed_mph                                         #

    def _add_average_speed(self, df: pd.DataFrame) -> pd.DataFrame:
        distance = pd.to_numeric(df["trip_distance"], errors="coerce")
        duration_minutes = pd.to_numeric(df["trip_duration_minutes"], errors="coerce")
        duration_hours = duration_minutes / 60.0
        speed = np.where(duration_hours > 0, distance / duration_hours, np.nan)
        df["average_speed_mph"] = pd.Series(speed, index=df.index, dtype="float32")
        print("[Transformer] Added column: average_speed_mph")
        return df

    #  Step 4 — pickup_year and pickup_month                              #

    def _add_pickup_date_parts(self, df: pd.DataFrame) -> pd.DataFrame:
        df["pickup_year"] = df["tpep_pickup_datetime"].dt.year
        df["pickup_month"] = df["tpep_pickup_datetime"].dt.month
        print("[Transformer] Added columns: pickup_year, pickup_month")
        return df

    #  Step 5 — revenue_per_mile                                          #

    def _add_revenue_per_mile(self, df: pd.DataFrame) -> pd.DataFrame:
        distance = pd.to_numeric(df["trip_distance"], errors="coerce")
        total = pd.to_numeric(df["total_amount"], errors="coerce")
        rpm = np.where(distance > 0, total / distance, np.nan)
        df["revenue_per_mile"] = pd.Series(rpm, index=df.index, dtype="float32")
        print("[Transformer] Added column: revenue_per_mile")
        return df

    #  Step 6 — trip_distance_category                                    #

    def _add_trip_distance_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Short  < 2 miles
        Medium 2–10 miles
        Long   > 10 miles
        Handles all-NaN batches safely.
        """
        # FIX: guard against all-NaN batch which crashes pd.cut
        if df["trip_distance"].isna().all():
            df["trip_distance_category"] = pd.Categorical(
                [None] * len(df),
                categories=["Short", "Medium", "Long"],
                ordered=True
            )
            print("[Transformer] Added column: trip_distance_category (all NaN batch)")
            return df

        df["trip_distance_category"] = pd.cut(
            df["trip_distance"],
            bins=[-float("inf"), 2, 10, float("inf")],
            labels=["Short", "Medium", "Long"],
            right=False
        )
        print("[Transformer] Added column: trip_distance_category")
        return df

    #  Step 7 — fare_category                                             #

    def _add_fare_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Low    < 20
        Medium 20–50
        High   > 50
        Handles all-NaN batches safely.
        """
        if df["fare_amount"].isna().all():
            df["fare_category"] = pd.Categorical(
                [None] * len(df),
                categories=["Low", "Medium", "High"],
                ordered=True
            )
            print("[Transformer] Added column: fare_category (all NaN batch)")
            return df

        df["fare_category"] = pd.cut(
            df["fare_amount"],
            bins=[-float("inf"), 20, 50, float("inf")],
            labels=["Low", "Medium", "High"],
            right=False
        )
        print("[Transformer] Added column: fare_category")
        return df

    # Step 8 — trip_time_of_day

    def _add_time_of_day(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Night     00:00 – 05:59
        Morning   06:00 – 11:59
        Afternoon 12:00 – 17:59
        Evening   18:00 – 23:59
        """
        hour = df["tpep_pickup_datetime"].dt.hour

        if hour.isna().all():
            df["trip_time_of_day"] = pd.Categorical(
                [None] * len(df),
                categories=["Night", "Morning", "Afternoon", "Evening"],
                ordered=False
            )
            print("[Transformer] Added column: trip_time_of_day (all NaN batch)")
            return df

        df["trip_time_of_day"] = pd.cut(
            hour,
            bins=[-1, 5, 11, 17, 24],
            labels=["Night", "Morning", "Afternoon", "Evening"]
        )
        print("[Transformer] Added column: trip_time_of_day")
        return df
