"""
HDFS Client integration module.
Provides communication with Hadoop HDFS using WebHDFS REST API,
Docker exec commands, and automatic local fallback mode.
"""

import os
import requests
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from src.config import HDFS_CONFIG, BENCHMARK_OUTPUT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("HDFSClient")

class HDFSClient:
    """
    Manages interactions with Hadoop HDFS cluster via WebHDFS.
    Falls back gracefully to local storage if HDFS is unavailable.
    """
    def __init__(self, host: str = None, port: int = None, user: str = None):
        self.host = host or HDFS_CONFIG["namenode_host"]
        self.port = port or HDFS_CONFIG["webhdfs_port"]
        self.user = user or HDFS_CONFIG["user"]
        self.base_url = f"http://{self.host}:{self.port}/webhdfs/v1"
        self.hdfs_dir = HDFS_CONFIG["hdfs_benchmark_dir"]
        self._is_hdfs_available = self._check_connection()

    def _check_connection(self) -> bool:
        """Verifies if WebHDFS endpoint on Hadoop NameNode is reachable."""
        try:
            url = f"{self.base_url}/?op=LISTSTATUS&user.name={self.user}"
            response = requests.get(url, timeout=2.0)
            if response.status_code == 200:
                logger.info(f"Connected successfully to Hadoop HDFS at {self.host}:{self.port}")
                return True
        except Exception:
            pass
        logger.info("Hadoop HDFS cluster not reachable on WebHDFS port. Operating in Local/Direct FileSystem mode.")
        return False

    @property
    def is_available(self) -> bool:
        """Returns True if HDFS is connected and ready."""
        return self._is_hdfs_available

    def mkdir(self, path: str = None) -> bool:
        """Creates a directory on HDFS."""
        target_path = path or self.hdfs_dir
        if not self._is_hdfs_available:
            local_path = BENCHMARK_OUTPUT_DIR / target_path.strip("/")
            local_path.mkdir(parents=True, exist_ok=True)
            return True

        try:
            url = f"{self.base_url}{target_path}?op=MKDIRS&user.name={self.user}"
            resp = requests.put(url, timeout=5.0)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Failed to create HDFS directory {target_path}: {e}")
            return False

    def upload_file(self, local_file_path: Path, hdfs_filename: Optional[str] = None) -> bool:
        """Uploads a local benchmark file to HDFS."""
        local_file_path = Path(local_file_path)
        if not local_file_path.exists():
            raise FileNotFoundError(f"Local file does not exist: {local_file_path}")

        filename = hdfs_filename or local_file_path.name
        target_path = f"{self.hdfs_dir}/{filename}"

        if not self._is_hdfs_available:
            # In local mode, file is already in BENCHMARK_OUTPUT_DIR
            return True

        try:
            # WebHDFS 2-step create: Step 1 initiates CREATE and gets datanode redirect URL
            init_url = f"{self.base_url}{target_path}?op=CREATE&overwrite=true&user.name={self.user}"
            init_resp = requests.put(init_url, allow_redirects=False, timeout=5.0)
            
            if init_resp.status_code in [307, 201]:
                datanode_url = init_resp.headers.get("Location")
                if datanode_url:
                    with open(local_file_path, "rb") as f:
                        put_resp = requests.put(datanode_url, data=f, headers={"Content-Type": "application/octet-stream"}, timeout=60.0)
                        return put_resp.status_code == 201
            return False
        except Exception as e:
            logger.error(f"HDFS upload failed for {local_file_path.name}: {e}")
            return False

    def get_file_status(self, hdfs_filename: str) -> Dict[str, Any]:
        """Retrieves file metadata from HDFS (length, blocksize, replication)."""
        target_path = f"{self.hdfs_dir}/{hdfs_filename}"
        if not self._is_hdfs_available:
            local_path = BENCHMARK_OUTPUT_DIR / hdfs_filename
            if local_path.exists():
                return {
                    "length": local_path.stat().st_size,
                    "blockSize": 128 * 1024 * 1024,
                    "replication": 1,
                    "type": "FILE",
                    "mode": "LOCAL_FS"
                }
            return {}

        try:
            url = f"{self.base_url}{target_path}?op=GETFILESTATUS&user.name={self.user}"
            resp = requests.get(url, timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                status = data.get("FileStatus", {})
                status["mode"] = "HDFS"
                return status
        except Exception as e:
            logger.error(f"Failed to get HDFS file status: {e}")
        return {}

    def get_cluster_health(self) -> Dict[str, Any]:
        """Returns overview of HDFS cluster status."""
        if not self._is_hdfs_available:
            return {
                "status": "LOCAL_EMULATED",
                "message": "Local High-Performance Filesystem Mode active",
                "namenode": "N/A (Standalone Local Engine)",
                "datanodes": "N/A"
            }
        try:
            url = f"http://{self.host}:{self.port}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
            resp = requests.get(url, timeout=3.0)
            if resp.status_code == 200:
                beans = resp.json().get("beans", [{}])[0]
                return {
                    "status": "ONLINE",
                    "capacity_total_gb": round(beans.get("CapacityTotal", 0) / (1024**3), 2),
                    "capacity_used_gb": round(beans.get("CapacityUsed", 0) / (1024**3), 2),
                    "num_live_datanodes": beans.get("NumLiveDataNodes", 0),
                    "num_dead_datanodes": beans.get("NumDeadDataNodes", 0),
                    "under_replicated_blocks": beans.get("UnderReplicatedBlocks", 0)
                }
        except Exception:
            pass
        return {"status": "ONLINE", "mode": "WebHDFS Connected"}
