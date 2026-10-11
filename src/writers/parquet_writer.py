"""
Parquet Format Writer Module.
Implements optimized columnar writes across multiple compression codecs:
Snappy, GZIP, ZSTD, and Uncompressed (None).
"""

import time
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Tuple
from src.utils import ResourceMonitor

class ParquetWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, df: pd.DataFrame, file_prefix: str, codec: str = "snappy") -> Dict[str, Any]:
        """
        Writes a pandas DataFrame to Parquet format using the specified compression codec.
        Records write execution time, peak memory, and file size.
        """
        codec_map = {
            "snappy": "SNAPPY",
            "gzip": "GZIP",
            "zstd": "ZSTD",
            "none": "NONE",
            "uncompressed": "NONE"
        }
        pyarrow_codec = codec_map.get(codec.lower(), "SNAPPY")
        output_file = self.output_dir / f"{file_prefix}_parquet_{codec.lower()}.parquet"

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df, preserve_index=False)
        num_rows = len(df)

        monitor = ResourceMonitor()
        monitor.start()
        start_time = time.perf_counter()

        # Write Parquet with standard 128MB row group size matching HDFS block boundaries
        pq.write_table(
            table,
            output_file,
            compression=pyarrow_codec,
            row_group_size=128 * 1024 * 1024,
            use_dictionary=True,
            write_statistics=True
        )

        elapsed_seconds = time.perf_counter() - start_time
        metrics = monitor.stop()

        file_size_bytes = output_file.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        throughput_mb_s = (file_size_mb / elapsed_seconds) if elapsed_seconds > 0 else 0.0
        rows_per_second = (num_rows / elapsed_seconds) if elapsed_seconds > 0 else 0.0

        return {
            "format": "parquet",
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
