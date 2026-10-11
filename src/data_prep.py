"""
Data Preparation Module for NYC Yellow Taxi Dataset (January 2025).

Performs:
- Schema inspection
- Data cleaning
- Anomaly filtering
- Missing value handling
- Multi-scale sample generation (1%, 10%, 50%, 100%)
"""

import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, Tuple

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# ============================================================
# ADD PROJECT ROOT TO SYS.PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

from src.config import (
    RAW_DATASET_PATH,
    PROCESSED_DATA_DIR,
    SAMPLES_DATA_DIR,
    SAMPLE_SCALES
)


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("DataPrep")


# ============================================================
# DATA PREPARATION PIPELINE
# ============================================================

class DataPreparationPipeline:

    def __init__(self, raw_path: Path = RAW_DATASET_PATH):

        self.raw_path = Path(raw_path)

        self.cleaned_parquet_path = (
            PROCESSED_DATA_DIR / "cleaned_yellow_tripdata.parquet"
        )

        self.cleaning_stats_path = (
            PROCESSED_DATA_DIR / "cleaning_summary.json"
        )

        self.csv_baseline_path = (
            PROCESSED_DATA_DIR / "baseline_raw_sample.csv"
        )

        # Make sure required directories exist
        PROCESSED_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        SAMPLES_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True
        )


    # ========================================================
    # SCHEMA INSPECTION
    # ========================================================

    def inspect_schema(self) -> Dict[str, Any]:

        """
        Inspects the raw parquet schema, column types,
        row counts, and file size.
        """

        if not self.raw_path.exists():

            raise FileNotFoundError(
                f"Raw dataset not found at {self.raw_path}"
            )

        logger.info(
            f"Inspecting dataset: {self.raw_path}"
        )

        parquet_file = pq.ParquetFile(self.raw_path)

        schema = parquet_file.schema_arrow

        num_rows = parquet_file.metadata.num_rows

        num_cols = parquet_file.metadata.num_columns

        file_size_mb = (
            self.raw_path.stat().st_size
            / (1024 * 1024)
        )

        column_info = []

        for col_name, field in zip(
            schema.names,
            schema
        ):

            column_info.append(
                {
                    "column_name": col_name,
                    "arrow_type": str(field.type),
                    "nullable": field.nullable
                }
            )

        inspection_report = {

            "file_name": self.raw_path.name,

            "total_rows": num_rows,

            "total_columns": num_cols,

            "file_size_mb": round(
                file_size_mb,
                2
            ),

            "columns": column_info
        }

        logger.info(
            f"Inspected dataset: "
            f"{num_rows:,} rows, "
            f"{num_cols} columns, "
            f"{file_size_mb:.2f} MB"
        )

        return inspection_report


    # ========================================================
    # DATA CLEANING
    # ========================================================

    def clean_and_prepare(
        self
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:

        """
        Loads the raw parquet dataset into Pandas
        and applies domain-specific cleaning rules.

        Cleaning rules:

        1. Positive fare and total amount
        2. Fare amount below $1000
        3. Valid trip distance
        4. Valid pickup/dropoff timestamps
        5. Passenger count between 1 and 8
        6. Missing categorical values are filled
        """

        logger.info(
            f"Loading raw dataset from {self.raw_path}..."
        )

        # ----------------------------------------------------
        # Check dataset
        # ----------------------------------------------------

        if not self.raw_path.exists():

            raise FileNotFoundError(
                f"Raw dataset not found at {self.raw_path}"
            )

        # ----------------------------------------------------
        # Load parquet
        # ----------------------------------------------------

        df = pd.read_parquet(
            self.raw_path,
            engine="pyarrow"
        )

        initial_count = len(df)

        logger.info(
            f"Loaded {initial_count:,} rows"
        )

        # ----------------------------------------------------
        # Check required columns
        # ----------------------------------------------------

        required_columns = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "fare_amount",
            "total_amount",
            "trip_distance",
            "passenger_count",
            "payment_type",
            "RatecodeID",
            "store_and_fwd_flag",
            "congestion_surcharge",
            "Airport_fee",
            "cbd_congestion_fee"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:

            raise ValueError(
                "The following required columns "
                f"are missing from the dataset: "
                f"{missing_columns}"
            )

        # ====================================================
        # 1. HANDLE MISSING VALUES
        # ====================================================

        logger.info("Handling missing values...")

        df["passenger_count"] = (
            df["passenger_count"]
            .fillna(1)
            .astype("int64")
        )

        df["payment_type"] = (
            df["payment_type"]
            .fillna(0)
            .astype("int64")
        )

        df["RatecodeID"] = (
            df["RatecodeID"]
            .fillna(1)
            .astype("int64")
        )

        df["store_and_fwd_flag"] = (
            df["store_and_fwd_flag"]
            .fillna("N")
            .astype(str)
        )

        df["congestion_surcharge"] = (
            df["congestion_surcharge"]
            .fillna(0.0)
            .astype("float64")
        )

        df["Airport_fee"] = (
            df["Airport_fee"]
            .fillna(0.0)
            .astype("float64")
        )

        df["cbd_congestion_fee"] = (
            df["cbd_congestion_fee"]
            .fillna(0.0)
            .astype("float64")
        )

        # ====================================================
        # 2. TIMESTAMP VALIDATION
        # ====================================================

        logger.info("Validating timestamps...")

        # Convert timestamp columns to datetime
        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"],
            errors="coerce"
        )

        df["tpep_dropoff_datetime"] = pd.to_datetime(
            df["tpep_dropoff_datetime"],
            errors="coerce"
        )

        valid_time_mask = (
            df["tpep_dropoff_datetime"]
            > df["tpep_pickup_datetime"]
        )

        invalid_time_count = int(
            (~valid_time_mask).sum()
        )

        # ====================================================
        # 3. FARE VALIDATION
        # ====================================================

        logger.info("Validating fares...")

        valid_fare_mask = (
            (df["fare_amount"] > 0)
            &
            (df["total_amount"] > 0)
            &
            (df["fare_amount"] < 1000)
        )

        invalid_fare_count = int(
            (~valid_fare_mask).sum()
        )

        # ====================================================
        # 4. TRIP DISTANCE VALIDATION
        # ====================================================

        logger.info(
            "Validating trip distances..."
        )

        valid_distance_mask = (
            (df["trip_distance"] > 0.05)
            &
            (df["trip_distance"] <= 300)
        )

        invalid_distance_count = int(
            (~valid_distance_mask).sum()
        )

        # ====================================================
        # 5. PASSENGER COUNT VALIDATION
        # ====================================================

        logger.info(
            "Validating passenger counts..."
        )

        valid_passenger_mask = (
            (df["passenger_count"] >= 1)
            &
            (df["passenger_count"] <= 8)
        )

        invalid_passenger_count = int(
            (~valid_passenger_mask).sum()
        )

        # ====================================================
        # 6. COMBINE CLEANING MASKS
        # ====================================================

        clean_mask = (
            valid_time_mask
            &
            valid_fare_mask
            &
            valid_distance_mask
            &
            valid_passenger_mask
        )

        df_clean = (
            df[clean_mask]
            .copy()
            .reset_index(drop=True)
        )

        final_count = len(df_clean)

        dropped_count = (
            initial_count - final_count
        )

        # ====================================================
        # CLEANING SUMMARY
        # ====================================================

        if initial_count > 0:

            retention_rate = (
                final_count
                / initial_count
                * 100
            )

        else:

            retention_rate = 0.0

        summary = {

            "initial_rows": initial_count,

            "cleaned_rows": final_count,

            "dropped_total": dropped_count,

            "retention_rate_pct": round(
                retention_rate,
                2
            ),

            "breakdown": {

                "invalid_timestamps":
                    invalid_time_count,

                "invalid_or_negative_fares":
                    invalid_fare_count,

                "invalid_distances":
                    invalid_distance_count,

                "invalid_passenger_counts":
                    invalid_passenger_count
            }
        }

        # ====================================================
        # SAVE CLEANED DATASET
        # ====================================================

        logger.info(
            f"Saving cleaned dataset "
            f"({final_count:,} rows) to "
            f"{self.cleaned_parquet_path}..."
        )

        df_clean.to_parquet(
            self.cleaned_parquet_path,
            index=False,
            engine="pyarrow",
            compression="snappy"
        )

        # ====================================================
        # SAVE CLEANING SUMMARY
        # ====================================================

        with open(
            self.cleaning_stats_path,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                summary,
                f,
                indent=2
            )

        logger.info(
            "Cleaning completed successfully."
        )

        return df_clean, summary


    # ========================================================
    # MULTI-SCALE SAMPLE GENERATION
    # ========================================================

    def generate_multi_scale_samples(
        self,
        df_clean: pd.DataFrame = None
    ) -> Dict[str, Dict[str, Any]]:

        """
        Creates representative samples at:

        - 1%
        - 10%
        - 50%
        - 100%

        Each sample is saved as both Parquet and CSV.
        """

        # ----------------------------------------------------
        # Load cleaned dataset if not provided
        # ----------------------------------------------------

        if df_clean is None:

            if self.cleaned_parquet_path.exists():

                logger.info(
                    "Loading existing cleaned dataset..."
                )

                df_clean = pd.read_parquet(
                    self.cleaned_parquet_path,
                    engine="pyarrow"
                )

            else:

                logger.info(
                    "Cleaned dataset not found. "
                    "Running cleaning pipeline..."
                )

                df_clean, _ = (
                    self.clean_and_prepare()
                )

        total_rows = len(df_clean)

        logger.info(
            f"Generating samples from "
            f"{total_rows:,} cleaned rows..."
        )

        scale_metadata = {}

        # ====================================================
        # GENERATE EACH SCALE
        # ====================================================

        for scale_name, ratio in SAMPLE_SCALES.items():

            logger.info(
                f"Processing scale: "
                f"{scale_name} ({ratio * 100:.0f}%)"
            )

            sample_file_parquet = (
                SAMPLES_DATA_DIR
                / f"{scale_name}.parquet"
            )

            sample_file_csv = (
                SAMPLES_DATA_DIR
                / f"{scale_name}_raw.csv"
            )

            # ------------------------------------------------
            # FULL DATASET
            # ------------------------------------------------

            if ratio >= 1.0:

                sampled_df = df_clean.copy()

            # ------------------------------------------------
            # SAMPLE DATASET
            # ------------------------------------------------

            else:

                sample_count = max(
                    1,
                    int(total_rows * ratio)
                )

                sampled_df = (
                    df_clean
                    .sample(
                        n=sample_count,
                        random_state=42
                    )
                    .reset_index(drop=True)
                )

            # =================================================
            # SAVE PARQUET
            # =================================================

            sampled_df.to_parquet(
                sample_file_parquet,
                index=False,
                engine="pyarrow",
                compression="snappy"
            )

            # =================================================
            # SAVE CSV
            # =================================================

            sampled_df.to_csv(
                sample_file_csv,
                index=False
            )

            # =================================================
            # FILE SIZE CALCULATION
            # =================================================

            raw_csv_size_bytes = (
                sample_file_csv.stat().st_size
            )

            parquet_size_bytes = (
                sample_file_parquet.stat().st_size
            )

            raw_csv_size_mb = (
                raw_csv_size_bytes
                / (1024 * 1024)
            )

            parquet_size_mb = (
                parquet_size_bytes
                / (1024 * 1024)
            )

            # =================================================
            # STORE METADATA
            # =================================================

            scale_metadata[scale_name] = {

                "ratio": ratio,

                "row_count": len(sampled_df),

                "parquet_path":
                    str(sample_file_parquet),

                "parquet_size_mb":
                    round(
                        parquet_size_mb,
                        2
                    ),

                "csv_path":
                    str(sample_file_csv),

                "csv_raw_size_mb":
                    round(
                        raw_csv_size_mb,
                        2
                    ),

                "baseline_csv_bytes":
                    raw_csv_size_bytes
            }

            logger.info(
                f"Generated scale "
                f"'{scale_name}': "
                f"{len(sampled_df):,} rows | "
                f"CSV: {raw_csv_size_mb:.2f} MB | "
                f"Parquet: {parquet_size_mb:.2f} MB"
            )

        # ====================================================
        # SAVE SCALE METADATA
        # ====================================================

        metadata_file = (
            SAMPLES_DATA_DIR
            / "scales_metadata.json"
        )

        with open(
            metadata_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                scale_metadata,
                f,
                indent=2
            )

        logger.info(
            f"Scale metadata saved to "
            f"{metadata_file}"
        )

        return scale_metadata


# ============================================================
# MAIN PREPARATION FUNCTION
# ============================================================

def run_preparation():

    logger.info(
        "Starting NYC Yellow Taxi "
        "Data Preparation Pipeline"
    )

    # --------------------------------------------------------
    # CREATE PIPELINE
    # --------------------------------------------------------

    pipeline = DataPreparationPipeline()

    # --------------------------------------------------------
    # STEP 1: INSPECT DATASET
    # --------------------------------------------------------

    inspection = pipeline.inspect_schema()

    # --------------------------------------------------------
    # STEP 2: CLEAN DATASET
    # --------------------------------------------------------

    df_clean, summary = (
        pipeline.clean_and_prepare()
    )

    # --------------------------------------------------------
    # STEP 3: GENERATE SAMPLES
    # --------------------------------------------------------

    scales = (
        pipeline.generate_multi_scale_samples(
            df_clean
        )
    )

    # ========================================================
    # FINAL OUTPUT
    # ========================================================

    print("\n" + "=" * 70)

    print(
        "DATA PREPARATION & SAMPLING COMPLETE"
    )

    print("=" * 70)

    print(
        f"Initial Rows   : "
        f"{summary['initial_rows']:,}"
    )

    print(
        f"Cleaned Rows   : "
        f"{summary['cleaned_rows']:,}"
    )

    print(
        f"Dropped Rows   : "
        f"{summary['dropped_total']:,}"
    )

    print(
        f"Retention Rate : "
        f"{summary['retention_rate_pct']}%"
    )

    print("-" * 70)

    print("Generated Samples:")

    for scale_name, metadata in scales.items():

        print(
            f" - Scale {scale_name:10s}: "
            f"{metadata['row_count']:,} rows | "
            f"CSV: "
            f"{metadata['csv_raw_size_mb']} MB | "
            f"Parquet: "
            f"{metadata['parquet_size_mb']} MB"
        )

    print("=" * 70)

    print(
        "\nData preparation completed successfully!\n"
    )

    return scales


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_preparation()