"""
Avro Format Reader Module.
Evaluates:
1. Full scan reading
2. Column subset reading (row-oriented extraction)
3. Predicate filtering
4. Groupby analytical aggregations
"""

import time
import fastavro
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from src.utils import ResourceMonitor

class AvroReader:
    def __init__(self, file_path: Path):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"Avro file not found: {self.file_path}")

    def read_full_scan(self) -> Dict[str, Any]:
        """Reads all records from the Avro container file into memory."""
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        with open(self.file_path, "rb") as f:
            reader = fastavro.reader(f)
            records = [r for r in reader]

        row_count = len(records)
        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "full_scan",
            "format": "avro",
            "elapsed_sec": round(elapsed, 4),
            "rows_read": row_count,
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_column_subset(self, columns: List[str]) -> Dict[str, Any]:
        """Reads records and projects requested columns in memory."""
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        with open(self.file_path, "rb") as f:
            reader = fastavro.reader(f)
            projected = [{col: r.get(col) for col in columns} for r in reader]

        row_count = len(projected)
        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "column_subset",
            "format": "avro",
            "elapsed_sec": round(elapsed, 4),
            "rows_read": row_count,
            "columns_requested": columns,
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_filtered(self, col: str = "PULocationID", val: Any = 132) -> Dict[str, Any]:
        """Scans records and filters matching records in memory."""
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        matched = []
        with open(self.file_path, "rb") as f:
            reader = fastavro.reader(f)
            for r in reader:
                if r.get(col) == val:
                    matched.append(r)

        matched_rows = len(matched)
        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "filtered_query",
            "format": "avro",
            "elapsed_sec": round(elapsed, 4),
            "matched_rows": matched_rows,
            "filter": f"{col} == {val}",
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_aggregation(self, group_by: str = "payment_type", agg_col: str = "fare_amount", agg_func: str = "mean") -> Dict[str, Any]:
        """Scans records and computes aggregation."""
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        sums = {}
        counts = {}
        with open(self.file_path, "rb") as f:
            reader = fastavro.reader(f)
            for r in reader:
                g_val = r.get(group_by)
                a_val = r.get(agg_col)
                if g_val is not None and a_val is not None:
                    sums[g_val] = sums.get(g_val, 0.0) + float(a_val)
                    counts[g_val] = counts.get(g_val, 0) + 1

        result = {}
        for g, count in counts.items():
            if agg_func == "mean":
                result[str(g)] = round(sums[g] / count, 2)
            elif agg_func == "sum":
                result[str(g)] = round(sums[g], 2)
            else:
                result[str(g)] = count

        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "aggregation_query",
            "format": "avro",
            "elapsed_sec": round(elapsed, 4),
            "groups_computed": len(result),
            "aggregation_result": result,
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }
