"""
Master CLI Entry Point for Hadoop Storage Format Benchmarking Framework.
Department of Computer Science & Engineering | DSCE Bengaluru

Usage:
    python run_all.py --scale sample_10pct --iterations 5
    python run_all.py --scale quick_1pct --dashboard
"""

import argparse
import sys
import os
import json
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("HSFB-Master")

from src.data_prep import DataPreparationPipeline
from src.benchmark_engine import BenchmarkEngine
from src.schema_evolution import SchemaEvolutionTester
from src.recommendation_engine import RecommendationEngine
from src.hdfs_client import HDFSClient

def print_banner():
    print("=" * 80)
    print("   HADOOP STORAGE FORMAT BENCHMARKING FRAMEWORK (HSFB)")
    print("   Dayananda Sagar College of Engineering (DSCE), Bengaluru")
    print("   Department of Computer Science and Engineering | BDA Lab")
    print("=" * 80)

def main():
    parser = argparse.ArgumentParser(description="Hadoop Storage Format Benchmarking Framework CLI")
    parser.add_argument("--scale", type=str, default="sample_10pct", choices=["quick_1pct", "sample_10pct", "sample_50pct", "full_100pct"], help="Dataset scale to benchmark")
    parser.add_argument("--iterations", type=int, default=5, help="Number of benchmark iterations (N=5 recommended)")
    parser.add_argument("--prep-only", action="store_true", help="Run only data preparation and sampling")
    parser.add_argument("--evolution-only", action="store_true", help="Run only schema evolution tests")
    parser.add_argument("--dashboard", action="store_true", help="Launch Streamlit dashboard upon completion")
    args = parser.parse_args()

    print_banner()

    # Step 1: Data Preparation & Sampling
    logger.info("=== STEP 1: DATA PREPARATION & SANITIZATION ===")
    pipeline = DataPreparationPipeline()
    if not (pipeline.cleaned_parquet_path.exists()):
        logger.info("Cleaned dataset not detected. Starting data cleaning...")
        df_clean, summary = pipeline.clean_and_prepare()
        pipeline.generate_multi_scale_samples(df_clean)
    else:
        logger.info("Cleaned dataset verified. Checking sample splits...")
        if not (pipeline.raw_path.parent.parent / "samples" / f"{args.scale}.parquet").exists():
            pipeline.generate_multi_scale_samples()

    if args.prep_only:
        logger.info("Data preparation completed successfully.")
        return

    # Step 2: Schema Evolution Testing Suite
    logger.info("=== STEP 2: SCHEMA EVOLUTION COMPATIBILITY TESTS ===")
    evo_tester = SchemaEvolutionTester()
    evo_results = evo_tester.run_all_evolution_tests()
    logger.info(f"Schema evolution tests complete: {evo_results['summary_count']} scenarios evaluated.")

    if args.evolution_only:
        logger.info("Schema evolution testing completed.")
        return

    # Step 3: Master Benchmark Engine Execution
    logger.info(f"=== STEP 3: STATISTICAL BENCHMARKING (Scale: {args.scale}, N={args.iterations} trials) ===")
    engine = BenchmarkEngine()
    bench_summary = engine.run_benchmark(scale_name=args.scale, iterations=args.iterations)

    # Step 4: Recommendation Engine Evaluation
    logger.info("=== STEP 4: RECOMMENDATION ENGINE EVALUATION ===")
    rec_engine = RecommendationEngine(bench_summary)
    
    print("\n" + "#" * 80)
    print("                     BENCHMARK RESULTS SUMMARY TABLE")
    print("#" * 80)
    print(f"{'FORMAT & CODEC':<32} | {'SIZE (MB)':<10} | {'COMPR':<7} | {'WRITE (s)':<12} | {'FULL SCAN':<10} | {'COL SUBSET':<10}")
    print("-" * 80)
    for r in bench_summary["results"]:
        w_time = f"{r['write_time_sec']['mean']:.2f}s"
        f_scan = f"{r['full_scan_sec']['mean']:.3f}s"
        c_sub = f"{r['column_subset_sec']['mean']:.3f}s"
        print(f"{r['config_label']:<32} | {r['file_size_mb']:<10.2f} | {r['compression_ratio']:<6.1f}x | {w_time:<12} | {f_scan:<10} | {c_sub:<10}")
    print("#" * 80)

    print("\n" + "=" * 80)
    print("                 USE-CASE AUTOMATED RECOMMENDATIONS")
    print("=" * 80)
    for use_case in ["Streaming / Ingestion Heavy", "ML Pipeline / Analytics Heavy", "Archival / Cold Storage"]:
        res = rec_engine.score_formats(use_case_name=use_case)
        print(f"[*] {use_case.upper()}:")
        print(f"    --> Recommended: {res['recommended_format']} (Score: {res['top_score']:.1f}/100)")
        print(f"    --> Justification: {res['justification']['summary']}\n")
    print("=" * 80)

    # Launch Dashboard if requested
    if args.dashboard:
        logger.info("Launching Streamlit Interactive Dashboard...")
        os.system("streamlit run dashboard/app.py")

if __name__ == "__main__":
    main()
