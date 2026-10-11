"""
Configuration module for the Hadoop Storage Format Benchmarking Framework.
Defines filesystem paths, codec configurations, benchmark parameters, and query predicates.
"""

from pathlib import Path
import os
import sys

# Base directory paths
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
SAMPLES_DATA_DIR = DATA_DIR / "samples"
BENCHMARK_OUTPUT_DIR = DATA_DIR / "benchmark_output"
DOCS_DIR = BASE_DIR / "docs"

# Ensure directories exist
for p in [RAW_DATA_DIR, PROCESSED_DATA_DIR, SAMPLES_DATA_DIR, BENCHMARK_OUTPUT_DIR, DOCS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

# Default raw dataset path
RAW_DATASET_NAME = "yellow_tripdata_2025-01.parquet"
RAW_DATASET_PATH = RAW_DATA_DIR / RAW_DATASET_NAME

# Multi-scale sample ratios
SAMPLE_SCALES = {
    "quick_1pct": 0.01,    # ~34,000 rows (Instant test)
    "sample_10pct": 0.10,  # ~340,000 rows (Fast laptop benchmarking)
    "sample_50pct": 0.50,  # ~1,700,000 rows (Medium scale)
    "full_100pct": 1.00    # ~3,400,000 rows (Full scale production benchmark)
}

# Supported Storage Formats and Codecs
FORMAT_CODECS = {
    "parquet": ["snappy", "gzip", "zstd", "none"],
    "avro": ["null", "snappy", "deflate"],
    "sequencefile": ["none", "record_deflate", "block_deflate", "record_snappy", "block_snappy"]
}

# Default benchmark settings
DEFAULT_ITERATIONS = 5  # Run N=5 times to calculate mean and standard deviation
WARMUP_RUNS = 1

# Benchmarking Queries and Workloads
BENCHMARK_QUERIES = {
    # 1. Full Scan: Select all columns and scan all rows
    "full_scan": {
        "description": "Full scan across all columns and rows"
    },
    # 2. Column Subset / Projection: Select 3 critical analytical columns
    "column_subset": {
        "columns": ["fare_amount", "trip_distance", "PULocationID", "payment_type"],
        "description": "Column projection on 4 high-frequency analytical columns"
    },
    # 3. Filtered Query: Pickups originating from JFK Airport (LocationID = 132)
    "filtered_query": {
        "filter_column": "PULocationID",
        "filter_value": 132,
        "operator": "==",
        "description": "Filter trips originating from JFK Airport (PULocationID = 132)"
    },
    # 4. Aggregation Query: Average fare amount and total distance grouped by payment_type
    "aggregation_query": {
        "group_by": "payment_type",
        "agg_column": "fare_amount",
        "agg_func": "mean",
        "description": "Analytical aggregation: Mean fare_amount grouped by payment_type"
    }
}

# HDFS Docker and Cluster Configuration
HDFS_CONFIG = {
    "namenode_host": os.environ.get("HDFS_NAMENODE_HOST", "localhost"),
    "webhdfs_port": int(os.environ.get("HDFS_WEBHDFS_PORT", 9870)),
    "ipc_port": int(os.environ.get("HDFS_IPC_PORT", 9000)),
    "hdfs_benchmark_dir": "/bda_benchmarks/yellow_taxi_2025",
    "user": "root"
}

# Use-Case Scoring Weights for Recommendation Engine
USE_CASE_PROFILES = {
    "Streaming / Ingestion Heavy": {
        "description": "High-velocity event streaming, append-heavy pipelines, rapid row serialization, frequent schema evolution.",
        "weights": {
            "write_throughput": 0.35,
            "read_speed": 0.10,
            "column_projection": 0.05,
            "compression_ratio": 0.15,
            "schema_evolution": 0.30,
            "resource_efficiency": 0.05
        }
    },
    "ML Pipeline / Analytics Heavy": {
        "description": "Large-scale batch data processing, feature engineering, analytical aggregations, column projection pushdown.",
        "weights": {
            "write_throughput": 0.10,
            "read_speed": 0.25,
            "column_projection": 0.35,
            "compression_ratio": 0.15,
            "schema_evolution": 0.10,
            "resource_efficiency": 0.05
        }
    },
    "Archival / Cold Storage": {
        "description": "Long-term data preservation with maximum compression to minimize storage costs; infrequent read operations.",
        "weights": {
            "write_throughput": 0.05,
            "read_speed": 0.10,
            "column_projection": 0.05,
            "compression_ratio": 0.65,
            "schema_evolution": 0.10,
            "resource_efficiency": 0.05
        }
    },
    "Legacy Hadoop / MapReduce Key-Value": {
        "description": "Classic Hadoop MapReduce workflows requiring binary key-value streaming with sync-marker splitability.",
        "weights": {
            "write_throughput": 0.20,
            "read_speed": 0.20,
            "column_projection": 0.05,
            "compression_ratio": 0.25,
            "schema_evolution": 0.10,
            "resource_efficiency": 0.20
        }
    }
}
