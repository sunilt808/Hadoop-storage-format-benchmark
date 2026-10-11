"""
Parquet Format Reader Module.
Evaluates:
1. Full scan reading
2. Column projection pushdown (reading only required columns)
3. Predicate pushdown filtering (e.g. PULocationID == 132)
4. Groupby analytical aggregations
"""

import time
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from src.utils import ResourceMonitor

class ParquetReader:
    def __init__(self, file_path: Path):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.file_path}")

    def read_full_scan(self) -> Dict[str, Any]:
        """Reads all columns and all rows."""
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        table = pq.read_table(self.file_path)
        df = table.to_pandas()
        row_count = len(df)

        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "full_scan",
            "format": "parquet",
            "elapsed_sec": round(elapsed, 4),
            "rows_read": row_count,
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_column_subset(self, columns: List[str]) -> Dict[str, Any]:
        """Leverages Parquet columnar projection pushdown to load only requested columns."""
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        table = pq.read_table(self.file_path, columns=columns)
        df = table.to_pandas()
        row_count = len(df)

        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "column_subset",
            "format": "parquet",
            "elapsed_sec": round(elapsed, 4),
            "rows_read": row_count,
            "columns_requested": columns,
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_filtered(self, col: str = "PULocationID", val: Any = 132) -> Dict[str, Any]:
        """Leverages Parquet predicate pushdown to filter records at the chunk metadata level."""
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        # PyArrow parquet filter predicate pushdown
        table = pq.read_table(self.file_path, filters=[(col, "==", val)])
        df = table.to_pandas()
        matched_rows = len(df)

        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "filtered_query",
            "format": "parquet",
            "elapsed_sec": round(elapsed, 4),
            "matched_rows": matched_rows,
            "filter": f"{col} == {val}",
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_aggregation(self, group_by: str = "payment_type", agg_col: str = "fare_amount", agg_func: str = "mean") -> Dict[str, Any]:
        """Reads minimal required columns and computes analytical aggregation."""
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        table = pq.read_table(self.file_path, columns=[group_by, agg_col])
        df = table.to_pandas()
        if agg_func == "mean":
            result = df.groupby(group_by)[agg_col].mean().to_dict()
        elif agg_func == "sum":
            result = df.groupby(group_by)[agg_col].sum().to_dict()
        else:
            result = df.groupby(group_by)[agg_col].count().to_dict()

        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "aggregation_query",
            "format": "parquet",
            "elapsed_sec": round(elapsed, 4),
            "groups_computed": len(result),
            "aggregation_result": {str(k): round(float(v), 2) for k, v in result.items()},
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }
