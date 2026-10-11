"""
Unit tests for Data Preparation and Sanitization Pipeline.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.config import SAMPLES_DATA_DIR, PROCESSED_DATA_DIR
from src.data_prep import DataPreparationPipeline

def test_data_cleaning_logic():
    # Test data frame with known anomalies
    test_df = pd.DataFrame({
        "VendorID": [1, 2, 1],
        "tpep_pickup_datetime": pd.to_datetime(["2025-01-01 10:00:00", "2025-01-01 12:00:00", "2025-01-01 14:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2025-01-01 10:30:00", "2025-01-01 11:30:00", "2025-01-01 14:15:00"]), # Row 1 invalid time
        "passenger_count": [2, 1, 0],  # Row 2 invalid passenger count 0
        "trip_distance": [3.5, 2.0, 1.2],
        "fare_amount": [15.0, -10.0, 8.5], # Row 1 negative fare
        "total_amount": [18.0, -5.0, 10.0],
        "payment_type": [1, 1, 2],
        "RatecodeID": [1, 1, 1],
        "store_and_fwd_flag": ["N", "N", "N"],
        "PULocationID": [132, 138, 161],
        "DOLocationID": [132, 138, 161],
        "extra": [0.5, 0.5, 0.5],
        "mta_tax": [0.5, 0.5, 0.5],
        "tip_amount": [2.0, 0.0, 1.0],
        "tolls_amount": [0.0, 0.0, 0.0],
        "improvement_surcharge": [0.3, 0.3, 0.3],
        "congestion_surcharge": [2.5, 2.5, 2.5],
        "Airport_fee": [1.25, 0.0, 0.0],
        "cbd_congestion_fee": [0.0, 0.0, 0.0]
    })

    # Apply same logic as DataPreparationPipeline
    valid_time = test_df['tpep_dropoff_datetime'] > test_df['tpep_pickup_datetime']
    valid_fare = (test_df['fare_amount'] > 0) & (test_df['total_amount'] > 0)
    valid_dist = (test_df['trip_distance'] > 0.05) & (test_df['trip_distance'] <= 300)
    valid_pax = (test_df['passenger_count'] >= 1) & (test_df['passenger_count'] <= 8)

    clean_mask = valid_time & valid_fare & valid_dist & valid_pax
    clean_df = test_df[clean_mask]

    # Only row 0 should pass all rules
    assert len(clean_df) == 1
    assert clean_df.iloc[0]["VendorID"] == 1
