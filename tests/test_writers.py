"""
Unit tests for Parquet, Avro, and SequenceFile Writers & Readers.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.config import BENCHMARK_OUTPUT_DIR
from src.writers.parquet_writer import ParquetWriter
from src.writers.avro_writer import AvroWriter
from src.writers.sequencefile_writer import SequenceFileWriter
from src.readers.parquet_reader import ParquetReader
from src.readers.avro_reader import AvroReader
from src.readers.sequencefile_reader import SequenceFileReader

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "VendorID": [1, 2, 1, 2],
        "tpep_pickup_datetime": pd.to_datetime(["2025-01-01 10:00:00", "2025-01-01 11:00:00", "2025-01-01 12:00:00", "2025-01-01 13:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2025-01-01 10:30:00", "2025-01-01 11:45:00", "2025-01-01 12:30:00", "2025-01-01 13:20:00"]),
        "passenger_count": [1, 2, 1, 4],
        "trip_distance": [2.5, 4.0, 1.8, 8.5],
        "RatecodeID": [1, 1, 1, 1],
        "store_and_fwd_flag": ["N", "N", "N", "Y"],
        "PULocationID": [132, 138, 161, 132],
        "DOLocationID": [138, 161, 230, 138],
        "payment_type": [1, 2, 1, 2],
        "fare_amount": [12.0, 18.5, 9.0, 32.0],
        "extra": [0.5, 0.5, 0.5, 0.5],
        "mta_tax": [0.5, 0.5, 0.5, 0.5],
        "tip_amount": [2.5, 0.0, 1.5, 5.0],
        "tolls_amount": [0.0, 6.55, 0.0, 0.0],
        "improvement_surcharge": [0.3, 0.3, 0.3, 0.3],
        "total_amount": [15.8, 26.35, 11.8, 38.3],
        "congestion_surcharge": [2.5, 2.5, 2.5, 2.5],
        "Airport_fee": [1.25, 0.0, 0.0, 1.25],
        "cbd_congestion_fee": [0.0, 0.0, 0.0, 0.0]
    })

def test_parquet_writer_reader(sample_df, tmp_path):
    writer = ParquetWriter(tmp_path)
    res = writer.write(sample_df, "test", codec="snappy")
    assert Path(res["output_path"]).exists()
    assert res["file_size_bytes"] > 0

    reader = ParquetReader(Path(res["output_path"]))
    full_res = reader.read_full_scan()
    assert full_res["rows_read"] == 4

    proj_res = reader.read_column_subset(["fare_amount", "trip_distance"])
    assert proj_res["rows_read"] == 4

def test_avro_writer_reader(sample_df, tmp_path):
    writer = AvroWriter(tmp_path)
    res = writer.write(sample_df, "test", codec="snappy")
    assert Path(res["output_path"]).exists()

    reader = AvroReader(Path(res["output_path"]))
    full_res = reader.read_full_scan()
    assert full_res["rows_read"] == 4

def test_sequencefile_writer_reader(sample_df, tmp_path):
    writer = SequenceFileWriter(tmp_path)
    res = writer.write(sample_df, "test", mode="block_deflate")
    assert Path(res["output_path"]).exists()

    reader = SequenceFileReader(Path(res["output_path"]))
    full_res = reader.read_full_scan()
    assert full_res["rows_read"] == 4
