@echo off
echo ==============================================================================
echo [3/5] Running Statistical Benchmark Engine (Parquet, Avro, SequenceFile)
echo Department of Computer Science and Engineering ^| DSCE Bengaluru
echo ==============================================================================

python run_all.py --scale sample_10pct --iterations 5
pause
