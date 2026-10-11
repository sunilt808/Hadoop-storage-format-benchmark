"""
Benchmark Engine Module.
Runs statistical benchmarking (N=5 iterations) across Parquet, Avro, and SequenceFile.
Evaluates:
- Write time & throughput (MB/s, rows/s)
- Full-scan read time
- Column subset projection read
- Filtered query (predicate pushdown)
- Aggregation query (mean fare by payment_type)
- File size, compression ratio vs raw CSV, and space saving %
- Memory & CPU resource tracking
"""

import time
import os
import json
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from src.config import (
    SAMPLES_DATA_DIR,
    BENCHMARK_OUTPUT_DIR,
    DEFAULT_ITERATIONS,
    WARMUP_RUNS,
    BENCHMARK_QUERIES
)
from src.utils import compute_statistics, format_bytes, format_time_ms
from src.hdfs_client import HDFSClient
from src.writers.parquet_writer import ParquetWriter
from src.writers.avro_writer import AvroWriter
from src.writers.sequencefile_writer import SequenceFileWriter
from src.readers.parquet_reader import ParquetReader
from src.readers.avro_reader import AvroReader
from src.readers.sequencefile_reader import SequenceFileReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BenchmarkEngine")

class BenchmarkEngine:
    def __init__(self, output_dir: Path = BENCHMARK_OUTPUT_DIR, hdfs_client: Optional[HDFSClient] = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.hdfs = hdfs_client or HDFSClient()

    def load_sample_dataset(self, scale_name: str = "sample_10pct") -> Tuple[pd.DataFrame, float]:
        """Loads the sample dataset and its baseline CSV size."""
        sample_path = SAMPLES_DATA_DIR / f"{scale_name}.parquet"
        csv_path = SAMPLES_DATA_DIR / f"{scale_name}_raw.csv"

        if not sample_path.exists():
            from src.data_prep import DataPreparationPipeline
            logger.info("Sample datasets not found. Running data preparation pipeline...")
            pipeline = DataPreparationPipeline()
            pipeline.generate_multi_scale_samples()

        df = pd.read_parquet(sample_path)
        csv_size_bytes = csv_path.stat().st_size if csv_path.exists() else (len(df) * 120)  # ~120B/row estimate
        return df, float(csv_size_bytes)

    def run_benchmark(
        self,
        scale_name: str = "sample_10pct",
        iterations: int = DEFAULT_ITERATIONS,
        formats_to_run: Optional[List[str]] = None,
        progress_callback: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Executes complete benchmark suite across Parquet, Avro, SequenceFile.
        """
        df, raw_csv_bytes = self.load_sample_dataset(scale_name)
        num_rows = len(df)
        raw_csv_mb = raw_csv_bytes / (1024 * 1024)
        logger.info(f"Starting Benchmark on scale '{scale_name}' ({num_rows:,} rows | Raw CSV: {raw_csv_mb:.2f} MB)")

        selected_formats = formats_to_run or ["parquet", "avro", "sequencefile"]

        # Define configurations to test
        configs = []
        if "parquet" in selected_formats:
            for codec in ["snappy", "gzip", "zstd", "none"]:
                configs.append(("parquet", codec))
        if "avro" in selected_formats:
            for codec in ["null", "snappy", "deflate"]:
                configs.append(("avro", codec))
        if "sequencefile" in selected_formats:
            for codec in ["none", "record_deflate", "block_deflate"]:
                configs.append(("sequencefile", codec))

        parquet_writer = ParquetWriter(self.output_dir)
        avro_writer = AvroWriter(self.output_dir)
        seq_writer = SequenceFileWriter(self.output_dir)

        total_steps = len(configs)
        current_step = 0
        all_results = []

        for fmt, codec in configs:
            current_step += 1
            config_id = f"{fmt}_{codec}"
            logger.info(f"[{current_step}/{total_steps}] Benchmarking {fmt.upper()} (codec: {codec})...")

            if progress_callback:
                progress_callback(current_step, total_steps, f"Testing {fmt.upper()} ({codec})")

            # -------------------------------------------------------------
            # 1. WRITE BENCHMARK (N iterations)
            # -------------------------------------------------------------
            write_times = []
            write_throughputs_mb_s = []
            write_throughputs_rows_s = []
            write_cpus = []
            write_mems = []
            final_file_path = None
            final_file_size_bytes = 0

            # Warmup run
            if fmt == "parquet":
                _ = parquet_writer.write(df, f"warmup_{scale_name}", codec=codec)
            elif fmt == "avro":
                _ = avro_writer.write(df, f"warmup_{scale_name}", codec=codec)
            else:
                _ = seq_writer.write(df, f"warmup_{scale_name}", mode=codec)

            for i in range(iterations):
                prefix = f"{scale_name}_run{i}"
                if fmt == "parquet":
                    w_res = parquet_writer.write(df, prefix, codec=codec)
                elif fmt == "avro":
                    w_res = avro_writer.write(df, prefix, codec=codec)
                else:
                    w_res = seq_writer.write(df, prefix, mode=codec)

                write_times.append(w_res["write_time_sec"])
                write_throughputs_mb_s.append(w_res["throughput_mb_s"])
                write_throughputs_rows_s.append(w_res["rows_per_second"])
                write_cpus.append(w_res["avg_cpu_percent"])
                write_mems.append(w_res["peak_memory_mb"])
                final_file_path = Path(w_res["output_path"])
                final_file_size_bytes = w_res["file_size_bytes"]

            # Upload to HDFS if available
            if self.hdfs.is_available and final_file_path:
                self.hdfs.upload_file(final_file_path)

            file_size_mb = final_file_size_bytes / (1024 * 1024)
            compression_ratio = round(raw_csv_bytes / final_file_size_bytes, 2) if final_file_size_bytes > 0 else 1.0
            space_saving_pct = round((1.0 - (final_file_size_bytes / raw_csv_bytes)) * 100, 2) if raw_csv_bytes > 0 else 0.0

            # -------------------------------------------------------------
            # 2. READ BENCHMARKS (N iterations)
            # -------------------------------------------------------------
            # Instantiate Reader
            if fmt == "parquet":
                reader = ParquetReader(final_file_path)
            elif fmt == "avro":
                reader = AvroReader(final_file_path)
            else:
                reader = SequenceFileReader(final_file_path)

            # 2.1 Full Scan
            full_scan_times = []
            for _ in range(iterations):
                r_res = reader.read_full_scan()
                full_scan_times.append(r_res["elapsed_sec"])

            # 2.2 Column Subset Read
            subset_cols = BENCHMARK_QUERIES["column_subset"]["columns"]
            subset_times = []
            for _ in range(iterations):
                r_res = reader.read_column_subset(subset_cols)
                subset_times.append(r_res["elapsed_sec"])

            # 2.3 Filtered Query
            filter_cfg = BENCHMARK_QUERIES["filtered_query"]
            filter_times = []
            for _ in range(iterations):
                r_res = reader.read_filtered(filter_cfg["filter_column"], filter_cfg["filter_value"])
                filter_times.append(r_res["elapsed_sec"])

            # 2.4 Aggregation Query
            agg_cfg = BENCHMARK_QUERIES["aggregation_query"]
            agg_times = []
            for _ in range(iterations):
                r_res = reader.read_aggregation(agg_cfg["group_by"], agg_cfg["agg_column"], agg_cfg["agg_func"])
                agg_times.append(r_res["elapsed_sec"])

            # Compute stats
            entry = {
                "format": fmt,
                "codec": codec,
                "config_label": f"{fmt.upper()} ({codec})",
                "scale": scale_name,
                "num_rows": num_rows,
                "file_size_bytes": final_file_size_bytes,
                "file_size_mb": round(file_size_mb, 2),
                "raw_csv_mb": round(raw_csv_mb, 2),
                "compression_ratio": compression_ratio,
                "space_saving_pct": space_saving_pct,
                # Write Statistics
                "write_time_sec": compute_statistics(write_times),
                "write_throughput_mb_s": compute_statistics(write_throughputs_mb_s),
                "write_throughput_rows_s": compute_statistics(write_throughputs_rows_s),
                "write_cpu_percent": compute_statistics(write_cpus),
                "write_peak_ram_mb": compute_statistics(write_mems),
                # Read Statistics
                "full_scan_sec": compute_statistics(full_scan_times),
                "column_subset_sec": compute_statistics(subset_times),
                "filtered_query_sec": compute_statistics(filter_times),
                "aggregation_query_sec": compute_statistics(agg_times)
            }
            all_results.append(entry)

        benchmark_summary = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "scale": scale_name,
            "iterations_per_test": iterations,
            "total_rows": num_rows,
            "raw_csv_mb": round(raw_csv_mb, 2),
            "hdfs_status": self.hdfs.get_cluster_health(),
            "results": all_results
        }

        # Save JSON output
        results_json_path = self.output_dir / "benchmark_results.json"
        with open(results_json_path, "w") as f:
            json.dump(benchmark_summary, f, indent=2)

        # Export CSV Summary Table
        summary_rows = []
        for r in all_results:
            summary_rows.append({
                "Format": r["format"].upper(),
                "Codec": r["codec"],
                "File Size (MB)": r["file_size_mb"],
                "Compression Ratio": f"{r['compression_ratio']}x",
                "Space Saving (%)": f"{r['space_saving_pct']}%",
                "Write Time (s) [mean ± std]": f"{r['write_time_sec']['mean']:.3f} ± {r['write_time_sec']['std_dev']:.3f}",
                "Write Throughput (MB/s)": f"{r['write_throughput_mb_s']['mean']:.2f}",
                "Full Scan (s)": f"{r['full_scan_sec']['mean']:.4f}",
                "Column Subset (s)": f"{r['column_subset_sec']['mean']:.4f}",
                "Filtered Query (s)": f"{r['filtered_query_sec']['mean']:.4f}",
                "Aggregation (s)": f"{r['aggregation_query_sec']['mean']:.4f}",
                "Avg Write CPU (%)": f"{r['write_cpu_percent']['mean']:.1f}%",
                "Peak RAM (MB)": f"{r['write_peak_ram_mb']['mean']:.1f}"
            })
        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_csv(self.output_dir / "benchmark_summary.csv", index=False)

        logger.info(f"Benchmark completed successfully! Results saved to {results_json_path}")
        return benchmark_summary


if __name__ == "__main__":
    engine = BenchmarkEngine()
    summary = engine.run_benchmark(scale_name="quick_1pct", iterations=3)
    print("Benchmark complete!")
