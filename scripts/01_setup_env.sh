#!/bin/bash
echo "=============================================================================="
echo "[1/5] Setting up Hadoop Storage Format Benchmarking Framework Environment"
echo "Department of Computer Science and Engineering | DSCE Bengaluru"
echo "=============================================================================="

echo "[INFO] Installing dependencies..."
python3 -m pip install -r requirements.txt

if command -v docker &> /dev/null; then
    echo "[INFO] Docker detected. Starting Hadoop HDFS cluster..."
    cd docker && docker-compose up -d && cd ..
    echo "[INFO] NameNode Web UI: http://localhost:9870"
    echo "[INFO] DataNode Web UI: http://localhost:9864"
else
    echo "[WARN] Docker not installed. Operating in local mode."
fi

echo "[INFO] Setup complete!"
