from pathlib import Path
from datetime import datetime
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
    "tip_amount",
    "tolls_amount",
    "extra",
    "Airport_fee",           # capital A — matches your actual data
    "congestion_surcharge",
    "cbd_congestion_fee",
    "store_and_fwd_flag",
    "RatecodeID",
]

# Valid lookup values based on NYC TLC data dictionary
# 1=Credit, 2=Cash, 3=No charge, etc.
VALID_PAYMENT_TYPES = {1, 2, 3, 4, 5, 6}
VALID_RATE_CODES = {1, 2, 3, 4, 5, 6}
VALID_STORE_FWD = {"Y", "N"}


class Validator:
    def __init__(self, error_path: str = "/opt/airflow/data/error"):
        self.error_path = Path(error_path)
        self.error_path.mkdir(parents=True, exist_ok=True)
        self.errors = []
        self.warning_counts = {}

    # ------------------------------------------------------------------ #
    #  Public entry point                                                  #
    # ------------------------------------------------------------------ #

    def validate(self, parquet_path: str) -> bool:
        """
        Validates the parquet file at parquet_path.
        Returns True if valid, raises ValueError if not.
        Writes a log file to the error folder on failure.
        """
        print(f"[Validator] Reading {parquet_path}")
        self.errors = []
        self.warning_counts = {}

        parquet_file = pq.ParquetFile(parquet_path)
        all_columns = parquet_file.schema.names

        # Validate column presence from parquet schema first.
        schema_df = pd.DataFrame(columns=all_columns)
        self._check_mandatory_columns_exist(schema_df)

        missing_optional = [
            c for c in NON_MANDATORY_COLUMNS if c not in all_columns]
        for col in missing_optional:
            print(
                f"[Validator] Optional column '{col}' not present — skipping")

        columns_to_read = [
            c for c in (MANDATORY_COLUMNS + NON_MANDATORY_COLUMNS)
            if c in all_columns
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
            for warning, count in self.warning_counts.items():
                print(
                    f"[Validator] WARNING: {warning} (affected rows: {count})")

        if self.errors:
            self._write_error_log(parquet_path)
            raise ValueError(
                f"[Validator] Validation failed with {len(self.errors)} error(s). "
                f"See error log in {self.error_path}"
            )

        print("[Validator] All validation checks passed.")
        return True

    def _add_warning(self, detail: str, affected_rows: int):
        current = self.warning_counts.get(detail, 0)
        self.warning_counts[detail] = current + int(affected_rows)

    # ------------------------------------------------------------------ #
    #  Presence validation — mandatory columns                            #
    # ------------------------------------------------------------------ #

    def _check_mandatory_columns_exist(self, df: pd.DataFrame):
        """Ensures all mandatory columns are present in the file."""
        missing = [c for c in MANDATORY_COLUMNS if c not in df.columns]
        if missing:
            self.errors.append({
                "check": "mandatory_columns_exist",
                "column": missing,
                "detail": f"Mandatory columns missing from file: {missing}",
                "affected_rows": "all"
            })

    def _check_mandatory_nulls(self, df: pd.DataFrame):
        """Ensures mandatory columns have no null values."""
        for col in MANDATORY_COLUMNS:
            if col not in df.columns:
                continue  # already logged above
            null_count = df[col].isnull().sum()
            if null_count > 0:
                self.errors.append({
                    "check": "mandatory_null_check",
                    "column": col,
                    "detail": f"{null_count} null values found in mandatory column",
                    "affected_rows": null_count
                })

    # ------------------------------------------------------------------ #
    #  Data type validation                                                #
    # ------------------------------------------------------------------ #

    def _check_data_types(self, df: pd.DataFrame):
        """
        Validates that key columns are the expected dtype.
        Based on actual dtypes observed in the January 2026 file.
        """
        expected_types = {
            "tpep_pickup_datetime":  {"datetime64[us]", "datetime64[ns]"},
            "tpep_dropoff_datetime": {"datetime64[us]", "datetime64[ns]"},
            "passenger_count":       {"float64", "int64", "Int64"},
            "trip_distance":         {"float64"},
            "PULocationID":          {"int32", "int64"},
            "DOLocationID":          {"int32", "int64"},
            "payment_type":          {"int64", "int32"},
            "fare_amount":           {"float64"},
            "total_amount":          {"float64"},
        }
        for col, expected in expected_types.items():
            if col not in df.columns:
                continue
            actual = str(df[col].dtype)
            if actual not in expected:
                self.errors.append({
                    "check": "data_type_check",
                    "column": col,
                    "detail": f"Expected dtype in {sorted(expected)}, got '{actual}'",
                    "affected_rows": "all"
                })

    # ------------------------------------------------------------------ #
    #  Range validation                                                    #
    # ------------------------------------------------------------------ #

    def _check_ranges(self, df: pd.DataFrame):
        """Validates numeric columns fall within acceptable ranges."""

        range_rules = [
            # (column, min_value, max_value, allow_zero)
            ("passenger_count", 0,    9,      True),
            # 0 is allowed (cancelled trips)
            ("trip_distance",   0,    None,   True),
            ("PULocationID",    1,    265,    False),   # NYC TLC zone range
            ("DOLocationID",    1,    265,    False),
        ]

        for col, min_val, max_val, allow_zero in range_rules:
            if col not in df.columns:
                continue
            series = df[col].dropna()

            if not allow_zero:
                below = (series < min_val).sum()
            else:
                below = (series < min_val).sum() if min_val is not None else 0

            above = (series > max_val).sum() if max_val is not None else 0

            if below > 0:
                self.errors.append({
                    "check": "range_check",
                    "column": col,
                    "detail": f"{below} values below minimum ({min_val})",
                    "affected_rows": below
                })
            if above > 0:
                self.errors.append({
                    "check": "range_check",
                    "column": col,
                    "detail": f"{above} values above maximum ({max_val})",
                    "affected_rows": above
                })

        # payment_type must be one of the valid codes
        if "payment_type" in df.columns:
            invalid = (~df["payment_type"].isin(VALID_PAYMENT_TYPES)).sum()
            if invalid > 0:
                self._add_warning(
                    f"payment_type outside valid set {VALID_PAYMENT_TYPES}",
                    invalid,
                )

    # ------------------------------------------------------------------ #
    #  Semantic / consistency validation                                   #
    # ------------------------------------------------------------------ #

    def _check_semantic_rules(self, df: pd.DataFrame):
        """Validates that data makes logical sense."""

        # Dropoff must be after pickup
        if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
            invalid = (df["tpep_dropoff_datetime"] <=
                       df["tpep_pickup_datetime"]).sum()
            if invalid > 0:
                self._add_warning("dropoff is not after pickup", invalid)

        # Dates must be in 2026 (our file is January 2026)
        if "tpep_pickup_datetime" in df.columns:
            wrong_year = (df["tpep_pickup_datetime"].dt.year != 2026).sum()
            if wrong_year > 0:
                self._add_warning("pickup year outside 2026", wrong_year)

        # total_amount should be >= fare_amount (tips/tolls/surcharges add up)
        if "total_amount" in df.columns and "fare_amount" in df.columns:
            invalid = (df["total_amount"] < df["fare_amount"]).sum()
            if invalid > 0:
                self._add_warning("total_amount < fare_amount", invalid)

    # ------------------------------------------------------------------ #
    #  Non-mandatory column validation                                     #
    # ------------------------------------------------------------------ #

    def _check_non_mandatory_columns(self, df: pd.DataFrame, warn_missing: bool = True):
        """
        Validates non-mandatory columns only if they are present.
        Logs a warning for nulls but does NOT fail the pipeline.
        """
        non_mandatory_numeric = [
            "tip_amount", "tolls_amount", "extra",
            "Airport_fee", "congestion_surcharge", "cbd_congestion_fee"
        ]

        for col in non_mandatory_numeric:
            if col not in df.columns:
                if warn_missing:
                    print(
                        f"[Validator] Optional column '{col}' not present — skipping")
                continue
            null_count = df[col].isnull().sum()
            if null_count > 0:
                self._add_warning(
                    f"'{col}' has nulls (non-mandatory)", null_count)
            negative = (df[col].dropna() < 0).sum()
            if negative > 0:
                self._add_warning(f"negative values in '{col}'", negative)

        # store_and_fwd_flag must be Y or N if present
        if "store_and_fwd_flag" in df.columns:
            invalid = (~df["store_and_fwd_flag"].isin(VALID_STORE_FWD)
                       & df["store_and_fwd_flag"].notna()).sum()
            if invalid > 0:
                self._add_warning(
                    f"store_and_fwd_flag not in {VALID_STORE_FWD}",
                    invalid,
                )

        # RatecodeID must be 1–6 if present
        if "RatecodeID" in df.columns:
            invalid = (~df["RatecodeID"].isin(VALID_RATE_CODES)
                       & df["RatecodeID"].notna()).sum()
            if invalid > 0:
                self._add_warning(
                    f"RatecodeID outside valid range {VALID_RATE_CODES}",
                    invalid,
                )

    # ------------------------------------------------------------------ #
    #  Error logging                                                       #
    # ------------------------------------------------------------------ #

    def _write_error_log(self, parquet_path: str):
        """
        Writes a structured error log to the error folder.
        Matches professor's recommendation: row/column of each error,
        combined where many of the same error appear.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.error_path / f"validation_errors_{timestamp}.log"

        with open(log_path, "w") as f:
            f.write(f"Validation Error Report\n")
            f.write(f"File:      {parquet_path}\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Total errors: {len(self.errors)}\n")
            f.write("=" * 60 + "\n\n")
            for i, err in enumerate(self.errors, 1):
                f.write(f"[{i}] Check:        {err['check']}\n")
                f.write(f"    Column:       {err['column']}\n")
                f.write(f"    Detail:       {err['detail']}\n")
                f.write(f"    Affected rows:{err['affected_rows']}\n\n")

        print(f"[Validator] Error log written to {log_path}")
