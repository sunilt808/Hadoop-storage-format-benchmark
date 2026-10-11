@echo off
echo ==============================================================================
echo [1/5] Setting up Hadoop Storage Format Benchmarking Framework Environment
echo Department of Computer Science and Engineering ^| DSCE Bengaluru
echo ==============================================================================

echo [INFO] Installing required Python dependencies from requirements.txt...
python -m pip install -r requirements.txt

echo [INFO] Checking Docker status for Hadoop HDFS cluster...
docker --version >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [INFO] Docker detected. Starting Hadoop HDFS cluster (NameNode + DataNode)...
    cd docker && docker-compose up -d && cd ..
    echo [INFO] Hadoop NameNode Web UI available at: http://localhost:9870
    echo [INFO] Hadoop DataNode Web UI available at: http://localhost:9864
) else (
    echo [WARN] Docker not running. Benchmarking framework will operate in high-performance local FS fallback mode.
)

echo [INFO] Setup complete!
pause
