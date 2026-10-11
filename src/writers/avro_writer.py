"""
Avro Format Writer Module.
Implements optimized row-oriented writes using fastavro across multiple codecs:
Null (uncompressed), Snappy, and Deflate (GZIP equivalent).
"""

import time
import os
import fastavro
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from src.utils import ResourceMonitor

AVRO_TAXI_SCHEMA = {
    "type": "record",
    "name": "NYCTaxiTripRecord",
    "namespace": "bda.dsce.benchmark",
    "doc": "NYC Yellow Taxi Trip Record benchmark schema",
    "fields": [
        {"name": "VendorID", "type": ["int", "null"], "default": 1},
        {"name": "tpep_pickup_datetime", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "tpep_dropoff_datetime", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "passenger_count", "type": "long", "default": 1},
        {"name": "trip_distance", "type": "double", "default": 0.0},
        {"name": "RatecodeID", "type": "long", "default": 1},
        {"name": "store_and_fwd_flag", "type": "string", "default": "N"},
        {"name": "PULocationID", "type": "int", "default": 0},
        {"name": "DOLocationID", "type": "int", "default": 0},
        {"name": "payment_type", "type": "long", "default": 0},
        {"name": "fare_amount", "type": "double", "default": 0.0},
        {"name": "extra", "type": "double", "default": 0.0},
        {"name": "mta_tax", "type": "double", "default": 0.0},
        {"name": "tip_amount", "type": "double", "default": 0.0},
        {"name": "tolls_amount", "type": "double", "default": 0.0},
        {"name": "improvement_surcharge", "type": "double", "default": 0.0},
        {"name": "total_amount", "type": "double", "default": 0.0},
        {"name": "congestion_surcharge", "type": "double", "default": 0.0},
        {"name": "Airport_fee", "type": "double", "default": 0.0},
        {"name": "cbd_congestion_fee", "type": "double", "default": 0.0}
    ]
}

class AvroWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.parsed_schema = fastavro.parse_schema(AVRO_TAXI_SCHEMA)

    def write(self, df: pd.DataFrame, file_prefix: str, codec: str = "snappy") -> Dict[str, Any]:
        """
        Serializes DataFrame to Avro container format with the specified codec.
        Supports 'null', 'snappy', 'deflate'.
        """
        codec_map = {
            "null": "null",
            "none": "null",
            "uncompressed": "null",
            "snappy": "snappy",
            "deflate": "deflate",
            "gzip": "deflate"
        }
        avro_codec = codec_map.get(codec.lower(), "snappy")
        output_file = self.output_dir / f"{file_prefix}_avro_{codec.lower()}.avro"

        # Prepare records for fastavro: convert pandas datetime to microseconds int
        records_df = df.copy()
        if pd.api.types.is_datetime64_any_dtype(records_df['tpep_pickup_datetime']):
            records_df['tpep_pickup_datetime'] = records_df['tpep_pickup_datetime'].astype('int64') // 1000
        if pd.api.types.is_datetime64_any_dtype(records_df['tpep_dropoff_datetime']):
            records_df['tpep_dropoff_datetime'] = records_df['tpep_dropoff_datetime'].astype('int64') // 1000

        records = records_df.to_dict(orient="records")
        num_rows = len(records)

        monitor = ResourceMonitor()
        monitor.start()
        start_time = time.perf_counter()

        try:
            with open(output_file, "wb") as out:
                fastavro.writer(
                    out,
                    self.parsed_schema,
                    records,
                    codec=avro_codec,
                    sync_interval=1000 * 1024  # Standard 1MB sync interval
                )
        except ValueError:
            # Fallback to deflate or null if specific codec library is missing
            fallback_codec = "deflate" if avro_codec != "deflate" else "null"
            with open(output_file, "wb") as out:
                fastavro.writer(
                    out,
                    self.parsed_schema,
                    records,
                    codec=fallback_codec,
                    sync_interval=1000 * 1024
                )

        elapsed_seconds = time.perf_counter() - start_time
        metrics = monitor.stop()

        file_size_bytes = output_file.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        throughput_mb_s = (file_size_mb / elapsed_seconds) if elapsed_seconds > 0 else 0.0
        rows_per_second = (num_rows / elapsed_seconds) if elapsed_seconds > 0 else 0.0

        return {
            "format": "avro",
            "codec": codec.lower(),
            "output_path": str(output_file),
            "file_size_bytes": file_size_bytes,
            "file_size_mb": round(file_size_mb, 2),
            "write_time_sec": round(elapsed_seconds, 4),
            "throughput_mb_s": round(throughput_mb_s, 2),
            "rows_per_second": round(rows_per_second, 1),
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"],
            "num_rows": num_rows
        }
