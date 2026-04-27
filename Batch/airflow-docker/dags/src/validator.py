from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


MANDATORY_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount",
]

NON_MANDATORY_COLUMNS = [
    "passenger_count",
    "tip_amount",
    "tolls_amount",
    "extra",
    "Airport_fee",
    "congestion_surcharge",
    "cbd_congestion_fee",
    "store_and_fwd_flag",
    "RatecodeID",
]

VALID_PAYMENT_TYPES = {1, 2, 3, 4, 5, 6}
VALID_RATE_CODES = {1, 2, 3, 4, 5, 6}
VALID_STORE_FWD = {"Y", "N"}


class Validator:
    def __init__(self, error_path: str = "/opt/airflow/data/error"):
        self.error_path = Path(error_path)
        self.error_path.mkdir(parents=True, exist_ok=True)
        self.errors = []
        self.warning_counts = {}
        self._error_index = {}
        self._expected_year = None

    def validate(self, parquet_path: str) -> bool:
        """
        Validates the parquet file at parquet_path.
        Returns True if valid, raises ValueError if not.
        Writes a log file to the error folder on failure.
        """
        self.errors = []
        self.warning_counts = {}
        self._error_index = {}

        input_path = Path(parquet_path).resolve()
        if not input_path.exists():
            raise FileNotFoundError(
                f"[Validator] Input parquet not found: {input_path}"
            )
        if input_path.suffix.lower() != ".parquet":
            raise ValueError(
                f"[Validator] Unsupported input extension: {input_path.suffix}"
            )

        try:
            self._expected_year = int(input_path.stem.split("_")[-1].split("-")[0])
            print(f"[Validator] Expected year from filename: {self._expected_year}")
        except (ValueError, IndexError):
            self._expected_year = None
            print(
                "[Validator] WARNING: Could not extract year from filename; year check skipped"
            )

        print(f"[Validator] Reading {input_path}")
        parquet_file = pq.ParquetFile(input_path)
        all_columns = parquet_file.schema.names

        missing_mandatory = [c for c in MANDATORY_COLUMNS if c not in all_columns]
        if missing_mandatory:
            self._add_error(
                check="mandatory_columns_exist",
                column=missing_mandatory,
                detail=f"Mandatory columns missing from file: {missing_mandatory}",
                affected_rows="all",
                aggregate=False,
            )
            self._write_error_log(str(input_path))
            raise ValueError(
                f"[Validator] Missing mandatory columns: {missing_mandatory}. "
                f"Cannot continue validation."
            )

        missing_optional = [c for c in NON_MANDATORY_COLUMNS if c not in all_columns]
        for col in missing_optional:
            print(f"[Validator] Optional column '{col}' not present; skipping")

        columns_to_read = [
            c for c in (MANDATORY_COLUMNS + NON_MANDATORY_COLUMNS) if c in all_columns
        ]

        rows_processed = 0
        dtype_checked = False
        for batch in parquet_file.iter_batches(batch_size=100_000, columns=columns_to_read):
            df = batch.to_pandas()
            rows_processed += len(df)

            if not dtype_checked:
                self._check_data_types(df)
                dtype_checked = True

            self._check_mandatory_nulls(df)
            self._check_ranges(df)
            self._check_semantic_rules(df)
            self._check_non_mandatory_columns(df, warn_missing=False)

        print(f"[Validator] Loaded {rows_processed} rows")

        if self.warning_counts:
            print("[Validator] Warning summary:")
            for warning, count in sorted(self.warning_counts.items()):
                print(f"[Validator] WARNING: {warning} (affected rows: {count})")

        if self.errors:
            self._write_error_log(str(input_path))
            raise ValueError(
                f"[Validator] Validation failed with {len(self.errors)} error(s). "
                f"See error log in {self.error_path}"
            )

        print("[Validator] All validation checks passed.")
        return True

    def _add_error(
        self,
        check: str,
        column,
        detail: str,
        affected_rows,
        aggregate: bool = True,
    ):
        if isinstance(affected_rows, int) and affected_rows <= 0:
            return

        record = {
            "check": check,
            "column": column,
            "detail": detail,
            "affected_rows": affected_rows,
        }

        if not aggregate:
            self.errors.append(record)
            return

        key = (check, str(column), detail)
        existing_index = self._error_index.get(key)
        if existing_index is None:
            self._error_index[key] = len(self.errors)
            self.errors.append(record)
            return

        existing_rows = self.errors[existing_index]["affected_rows"]
        if isinstance(existing_rows, int) and isinstance(affected_rows, int):
            self.errors[existing_index]["affected_rows"] = existing_rows + affected_rows

    def _add_warning(self, detail: str, affected_rows: int):
        if int(affected_rows) <= 0:
            return
        current = self.warning_counts.get(detail, 0)
        self.warning_counts[detail] = current + int(affected_rows)

    def _check_mandatory_nulls(self, df: pd.DataFrame):
        for col in MANDATORY_COLUMNS:
            if col not in df.columns:
                continue
            null_count = int(df[col].isnull().sum())
            if null_count > 0:
                self._add_error(
                    check="mandatory_null_check",
                    column=col,
                    detail="Null values found in mandatory column",
                    affected_rows=null_count,
                )

    def _check_data_types(self, df: pd.DataFrame):
        datetime_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
        numeric_columns = [
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "total_amount",
        ]

        for col in datetime_columns:
            if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
                self._add_error(
                    check="data_type_check",
                    column=col,
                    detail="Expected datetime column",
                    affected_rows="all",
                    aggregate=False,
                )

        for col in numeric_columns:
            if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                self._add_error(
                    check="data_type_check",
                    column=col,
                    detail="Expected numeric column",
                    affected_rows="all",
                    aggregate=False,
                )

    def _check_ranges(self, df: pd.DataFrame):
        range_rules = [
            # Common TLC refunds/adjustments can create negatives in fare/total.
            ("passenger_count", 0, 9, "warning"),
            ("trip_distance", 0, None, "error"),
            ("fare_amount", 0, None, "warning"),
            ("total_amount", 0, None, "warning"),
            ("PULocationID", 1, 265, "error"),
            ("DOLocationID", 1, 265, "error"),
        ]

        for col, min_val, max_val, severity in range_rules:
            if col not in df.columns:
                continue

            numeric_series = pd.to_numeric(df[col], errors="coerce")
            invalid_numeric = int(numeric_series.isna().sum() - df[col].isna().sum())
            if invalid_numeric > 0:
                if severity == "error":
                    self._add_error(
                        check="range_check",
                        column=col,
                        detail="Non-numeric values found",
                        affected_rows=invalid_numeric,
                    )
                else:
                    self._add_warning(f"non-numeric values in '{col}'", invalid_numeric)

            series = numeric_series.dropna()
            below = int((series < min_val).sum()) if min_val is not None else 0
            above = int((series > max_val).sum()) if max_val is not None else 0

            if below > 0:
                if severity == "error":
                    self._add_error(
                        check="range_check",
                        column=col,
                        detail=f"Values below minimum ({min_val})",
                        affected_rows=below,
                    )
                else:
                    self._add_warning(f"'{col}' values below minimum ({min_val})", below)

            if above > 0:
                if severity == "error":
                    self._add_error(
                        check="range_check",
                        column=col,
                        detail=f"Values above maximum ({max_val})",
                        affected_rows=above,
                    )
                else:
                    self._add_warning(f"'{col}' values above maximum ({max_val})", above)

        if "payment_type" in df.columns:
            payment_series = pd.to_numeric(df["payment_type"], errors="coerce")
            invalid_numeric = int(payment_series.isna().sum() - df["payment_type"].isna().sum())
            if invalid_numeric > 0:
                self._add_error(
                    check="range_check",
                    column="payment_type",
                    detail="Non-numeric values found",
                    affected_rows=invalid_numeric,
                )

            invalid_codes = int(
                (~payment_series.isin(VALID_PAYMENT_TYPES) & payment_series.notna()).sum()
            )
            if invalid_codes > 0:
                self._add_warning(
                    f"payment_type outside valid set {VALID_PAYMENT_TYPES}",
                    invalid_codes,
                )

    def _check_semantic_rules(self, df: pd.DataFrame):
        if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
            pickup_dt = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
            dropoff_dt = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

            invalid_datetimes = int(
                (pickup_dt.isna() & df["tpep_pickup_datetime"].notna()).sum()
                + (dropoff_dt.isna() & df["tpep_dropoff_datetime"].notna()).sum()
            )
            if invalid_datetimes > 0:
                self._add_error(
                    check="semantic_check",
                    column=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
                    detail="Invalid datetime values detected",
                    affected_rows=invalid_datetimes,
                )

            reversed_datetimes = int((dropoff_dt < pickup_dt).sum())
            if reversed_datetimes > 0:
                self._add_warning(
                    "rows where dropoff is earlier than pickup",
                    reversed_datetimes,
                )

            zero_duration = int((dropoff_dt == pickup_dt).sum())
            if zero_duration > 0:
                self._add_warning(
                    "rows where dropoff equals pickup",
                    zero_duration,
                )

            if self._expected_year is not None:
                valid_pickup = pickup_dt.dropna()
                wrong_year = int((valid_pickup.dt.year != self._expected_year).sum())
                if wrong_year > 0:
                    self._add_warning(
                        f"pickup year outside expected year {self._expected_year}",
                        wrong_year,
                    )

        if "total_amount" in df.columns and "fare_amount" in df.columns:
            total_amount = pd.to_numeric(df["total_amount"], errors="coerce")
            fare_amount = pd.to_numeric(df["fare_amount"], errors="coerce")
            comparable = (
                total_amount.notna()
                & fare_amount.notna()
                & (total_amount >= 0)
                & (fare_amount >= 0)
            )
            lower_total = int((total_amount[comparable] < fare_amount[comparable]).sum())
            if lower_total > 0:
                self._add_warning(
                    "rows where total_amount < fare_amount",
                    lower_total,
                )

    def _check_non_mandatory_columns(self, df: pd.DataFrame, warn_missing: bool = True):
        non_mandatory_numeric = [
            "tip_amount",
            "tolls_amount",
            "extra",
            "Airport_fee",
            "congestion_surcharge",
            "cbd_congestion_fee",
        ]

        for col in non_mandatory_numeric:
            if col not in df.columns:
                if warn_missing:
                    print(f"[Validator] Optional column '{col}' not present; skipping")
                continue

            numeric_series = pd.to_numeric(df[col], errors="coerce")
            null_count = int(numeric_series.isnull().sum())
            if null_count > 0:
                self._add_warning(f"'{col}' has nulls (non-mandatory)", null_count)

            invalid_numeric = int(numeric_series.isna().sum() - df[col].isna().sum())
            if invalid_numeric > 0:
                self._add_warning(f"non-numeric values in '{col}'", invalid_numeric)

            negative = int((numeric_series.dropna() < 0).sum())
            if negative > 0:
                self._add_warning(f"negative values in '{col}'", negative)

        if "store_and_fwd_flag" in df.columns:
            store_fwd = df["store_and_fwd_flag"].astype("string").str.strip().str.upper()
            invalid_store_fwd = int((~store_fwd.isin(VALID_STORE_FWD) & store_fwd.notna()).sum())
            if invalid_store_fwd > 0:
                self._add_warning(
                    f"store_and_fwd_flag not in {VALID_STORE_FWD}",
                    invalid_store_fwd,
                )

        if "RatecodeID" in df.columns:
            rate_code = pd.to_numeric(df["RatecodeID"], errors="coerce")
            invalid_rate_code = int((~rate_code.isin(VALID_RATE_CODES) & rate_code.notna()).sum())
            if invalid_rate_code > 0:
                self._add_warning(
                    f"RatecodeID outside valid range {VALID_RATE_CODES}",
                    invalid_rate_code,
                )

    def _write_error_log(self, parquet_path: str):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.error_path / f"validation_errors_{timestamp}.log"
        safe_source_path = parquet_path.replace("\n", " ").replace("\r", " ")

        total_affected_rows = sum(
            err["affected_rows"]
            for err in self.errors
            if isinstance(err["affected_rows"], int)
        )

        with open(log_path, "w", encoding="utf-8") as f:
            f.write("Validation Error Report\n")
            f.write(f"File:      {safe_source_path}\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Total error categories: {len(self.errors)}\n")
            f.write(f"Total affected rows:    {total_affected_rows}\n")
            f.write("=" * 60 + "\n\n")
            for i, err in enumerate(self.errors, 1):
                f.write(f"[{i}] Check:        {err['check']}\n")
                f.write(f"    Column:       {err['column']}\n")
                f.write(f"    Detail:       {err['detail']}\n")
                f.write(f"    Affected rows:{err['affected_rows']}\n\n")

        print(f"[Validator] Error log written to {log_path}")
