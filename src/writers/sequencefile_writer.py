"""
Hadoop SequenceFile Format Writer Module.
Implements the Apache Hadoop SequenceFile binary format specification (Version 6, SEQ6).
Supports:
1. Uncompressed SequenceFile (NONE)
2. Record-Compressed SequenceFile (RECORD) with Deflate/Snappy
3. Block-Compressed SequenceFile (BLOCK) with Deflate/Snappy
Includes 16-byte sync markers for HDFS block splitability.
"""

import time
import os
import struct
import zlib
import secrets
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from src.utils import ResourceMonitor

try:
    import cramjam
    HAS_SNAPPY = True
    def compress_snappy(b: bytes) -> bytes:
        return bytes(cramjam.snappy.compress(b))
except ImportError:
    try:
        import snappy
        HAS_SNAPPY = True
        def compress_snappy(b: bytes) -> bytes:
            return snappy.compress(b)
    except ImportError:
        HAS_SNAPPY = False
        def compress_snappy(b: bytes) -> bytes:
            return zlib.compress(b, level=6)

def write_vint(val: int) -> bytes:
    """Encodes an integer using Hadoop Variable-length Integer (VInt) format."""
    if -112 <= val <= 127:
        return struct.pack("b", val)
    elif -120 <= val < -112:
        return struct.pack("b", -113) + struct.pack(">h", val)
    elif val > 127 and val <= 32767:
        return struct.pack("b", -114) + struct.pack(">h", val)
    elif val > 32767 and val <= 2147483647:
        return struct.pack("b", -116) + struct.pack(">i", val)
    else:
        return struct.pack("b", -120) + struct.pack(">q", val)

def write_hadoop_text(text: str) -> bytes:
    """Encodes a string using Hadoop Text format: VInt(length) + utf-8 bytes."""
    encoded = text.encode("utf-8")
    return write_vint(len(encoded)) + encoded

class SequenceFileWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.key_class = "org.apache.hadoop.io.LongWritable"
        self.value_class = "org.apache.hadoop.io.Text"

    def _compress_bytes(self, data: bytes, codec: str) -> bytes:
        if "snappy" in codec.lower() and HAS_SNAPPY:
            return compress_snappy(data)
        return zlib.compress(data, level=6)

    def write(self, df: pd.DataFrame, file_prefix: str, mode: str = "block_deflate") -> Dict[str, Any]:
        """
        Writes a DataFrame to Hadoop SequenceFile format.
        Modes: 'none', 'record_deflate', 'block_deflate', 'record_snappy', 'block_snappy'.
        """
        mode = mode.lower()
        output_file = self.output_dir / f"{file_prefix}_sequencefile_{mode}.seq"

        is_compressed = 0 if mode == "none" else 1
        is_block_compressed = 1 if "block" in mode else 0
        codec_name = "snappy" if "snappy" in mode else "deflate"
        hadoop_codec_class = "org.apache.hadoop.io.compress.SnappyCodec" if codec_name == "snappy" else "org.apache.hadoop.io.compress.DefaultCodec"

        # Generate unique 16-byte sync marker for HDFS splitability
        sync_marker = secrets.token_bytes(16)

        num_rows = len(df)
        monitor = ResourceMonitor()
        monitor.start()
        start_time = time.perf_counter()

        with open(output_file, "wb") as f:
            # 1. Write Header: Magic SEQ6
            f.write(b"SEQ\x06")
            f.write(write_hadoop_text(self.key_class))
            f.write(write_hadoop_text(self.value_class))
            f.write(struct.pack("?", is_compressed))
            f.write(struct.pack("?", is_block_compressed))

            if is_compressed:
                f.write(write_hadoop_text(hadoop_codec_class))

            # Metadata count (0 metadata pairs)
            f.write(struct.pack(">i", 0))

            # Header Sync Marker
            f.write(sync_marker)

            records = df.to_dict(orient="records")

            if is_block_compressed:
                # Block Compression: aggregate records into blocks of 5,000
                block_size = 5000
                for i in range(0, num_rows, block_size):
                    chunk = records[i:i + block_size]
                    num_records_in_block = len(chunk)

                    # Write sync marker before each block
                    f.write(sync_marker)
                    f.write(write_vint(num_records_in_block))

                    # Buffer keys and values
                    key_lengths_buf = bytearray()
                    keys_buf = bytearray()
                    val_lengths_buf = bytearray()
                    vals_buf = bytearray()

                    for idx, row in enumerate(chunk, start=i):
                        key_bytes = struct.pack(">q", idx)  # LongWritable (8B)
                        val_str = json.dumps(row, default=str) + "\n"
                        val_bytes = val_str.encode("utf-8")

                        key_lengths_buf.extend(write_vint(len(key_bytes)))
                        keys_buf.extend(key_bytes)
                        val_lengths_buf.extend(write_vint(len(val_bytes)))
                        vals_buf.extend(val_bytes)

                    # Compress buffers
                    comp_key_lens = self._compress_bytes(bytes(key_lengths_buf), codec_name)
                    comp_keys = self._compress_bytes(bytes(keys_buf), codec_name)
                    comp_val_lens = self._compress_bytes(bytes(val_lengths_buf), codec_name)
                    comp_vals = self._compress_bytes(bytes(vals_buf), codec_name)

                    # Write compressed buffers with length prefixes
                    f.write(struct.pack(">i", len(comp_key_lens)) + comp_key_lens)
                    f.write(struct.pack(">i", len(comp_keys)) + comp_keys)
                    f.write(struct.pack(">i", len(comp_val_lens)) + comp_val_lens)
                    f.write(struct.pack(">i", len(comp_vals)) + comp_vals)

            elif is_compressed and not is_block_compressed:
                # Record Compression: Each record value is individually compressed
                for idx, row in enumerate(records):
                    if idx > 0 and idx % 2000 == 0:
                        f.write(struct.pack(">i", -1))  # Sync escape marker
                        f.write(sync_marker)

                    key_bytes = struct.pack(">q", idx)
                    val_str = json.dumps(row, default=str)
                    raw_val_bytes = val_str.encode("utf-8")
                    comp_val_bytes = self._compress_bytes(raw_val_bytes, codec_name)

                    rec_len = len(key_bytes) + len(comp_val_bytes)
                    key_len = len(key_bytes)

                    f.write(struct.pack(">i", rec_len))
                    f.write(struct.pack(">i", key_len))
                    f.write(key_bytes)
                    f.write(comp_val_bytes)

            else:
                # Uncompressed: Raw key-value pairs with periodic sync markers
                for idx, row in enumerate(records):
                    if idx > 0 and idx % 2000 == 0:
                        f.write(struct.pack(">i", -1))  # Sync escape marker
                        f.write(sync_marker)

                    key_bytes = struct.pack(">q", idx)
                    val_str = json.dumps(row, default=str)
                    val_bytes = val_str.encode("utf-8")

                    rec_len = len(key_bytes) + len(val_bytes)
                    key_len = len(key_bytes)

                    f.write(struct.pack(">i", rec_len))
                    f.write(struct.pack(">i", key_len))
                    f.write(key_bytes)
                    f.write(val_bytes)

        elapsed_seconds = time.perf_counter() - start_time
        metrics = monitor.stop()

        file_size_bytes = output_file.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        throughput_mb_s = (file_size_mb / elapsed_seconds) if elapsed_seconds > 0 else 0.0
        rows_per_second = (num_rows / elapsed_seconds) if elapsed_seconds > 0 else 0.0

        return {
            "format": "sequencefile",
            "codec": mode,
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
