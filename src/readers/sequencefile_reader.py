"""
SequenceFile Format Reader Module.
Parses Hadoop SequenceFile (SEQ6) binary structures for Uncompressed,
Record-Compressed, and Block-Compressed formats.
"""

import time
import os
import struct
import zlib
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, Generator
from src.utils import ResourceMonitor

try:
    import cramjam
    HAS_SNAPPY = True
    def decompress_snappy(b: bytes) -> bytes:
        return bytes(cramjam.snappy.decompress(b))
except ImportError:
    try:
        import snappy
        HAS_SNAPPY = True
        def decompress_snappy(b: bytes) -> bytes:
            return snappy.uncompress(b)
    except ImportError:
        HAS_SNAPPY = False
        def decompress_snappy(b: bytes) -> bytes:
            return zlib.decompress(b)

def read_vint(f) -> int:
    """Reads a Hadoop Variable-length Integer (VInt) from stream."""
    first_byte = f.read(1)
    if not first_byte:
        raise EOFError()
    b = struct.unpack("b", first_byte)[0]
    if b >= -112:
        return b
    elif b >= -120:
        length = -111 - b
        data = f.read(length)
        if len(data) < length:
            raise EOFError()
        val = int.from_bytes(data, byteorder="big", signed=True)
        return val
    else:
        length = -119 - b
        data = f.read(length)
        if len(data) < length:
            raise EOFError()
        val = int.from_bytes(data, byteorder="big", signed=True)
        return val

def read_hadoop_text(f) -> str:
    """Reads a Hadoop Text string: VInt length + UTF-8 payload."""
    length = read_vint(f)
    data = f.read(length)
    return data.decode("utf-8", errors="replace")

class SequenceFileReader:
    def __init__(self, file_path: Path):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"SequenceFile not found: {self.file_path}")

    def _decompress(self, data: bytes, codec: str) -> bytes:
        if "snappy" in codec.lower() and HAS_SNAPPY:
            try:
                return decompress_snappy(data)
            except Exception:
                pass
        return zlib.decompress(data)

    def _stream_records(self) -> Generator[Dict[str, Any], None, None]:
        """Generator that iterates through SEQ6 file and yields dictionary records."""
        with open(self.file_path, "rb") as f:
            magic = f.read(4)
            if magic != b"SEQ\x06":
                raise ValueError(f"Invalid SequenceFile magic header: {magic}")

            key_class = read_hadoop_text(f)
            val_class = read_hadoop_text(f)
            is_compressed = struct.unpack("?", f.read(1))[0]
            is_block_compressed = struct.unpack("?", f.read(1))[0]

            codec_name = "deflate"
            if is_compressed:
                codec_name = read_hadoop_text(f)

            # Metadata count
            meta_count = struct.unpack(">i", f.read(4))[0]
            for _ in range(meta_count):
                _ = read_hadoop_text(f)
                _ = read_hadoop_text(f)

            # Header Sync Marker (16 bytes)
            header_sync = f.read(16)

            if is_block_compressed:
                while True:
                    sync_peek = f.read(16)
                    if not sync_peek or len(sync_peek) < 16:
                        break
                    try:
                        num_records = read_vint(f)
                    except EOFError:
                        break

                    # Read 4 compressed buffers: key_lens, keys, val_lens, vals
                    try:
                        len_comp_kl = struct.unpack(">i", f.read(4))[0]
                        comp_kl = f.read(len_comp_kl)

                        len_comp_k = struct.unpack(">i", f.read(4))[0]
                        comp_k = f.read(len_comp_k)

                        len_comp_vl = struct.unpack(">i", f.read(4))[0]
                        comp_vl = f.read(len_comp_vl)

                        len_comp_v = struct.unpack(">i", f.read(4))[0]
                        comp_v = f.read(len_comp_v)
                    except Exception:
                        break

                    # Decompress value buffer
                    decomp_vals = self._decompress(comp_v, codec_name)
                    # Values are stored as newline-delimited JSON
                    lines = decomp_vals.split(b"\n")
                    for line in lines:
                        line = line.strip()
                        if line:
                            try:
                                yield json.loads(line.decode("utf-8"))
                            except Exception:
                                pass

            else:
                # Uncompressed or Record-compressed
                while True:
                    len_bytes = f.read(4)
                    if not len_bytes or len(len_bytes) < 4:
                        break
                    rec_len = struct.unpack(">i", len_bytes)[0]
                    if rec_len == -1:  # Sync escape marker
                        f.read(16)
                        continue

                    key_len_bytes = f.read(4)
                    if not key_len_bytes or len(key_len_bytes) < 4:
                        break
                    key_len = struct.unpack(">i", key_len_bytes)[0]

                    key_data = f.read(key_len)
                    val_len = rec_len - key_len
                    if val_len < 0:
                        break
                    val_data = f.read(val_len)

                    if is_compressed:
                        decomp_val = self._decompress(val_data, codec_name)
                    else:
                        decomp_val = val_data

                    try:
                        yield json.loads(decomp_val.decode("utf-8"))
                    except Exception:
                        pass

    def read_full_scan(self) -> Dict[str, Any]:
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        records = list(self._stream_records())
        row_count = len(records)

        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "full_scan",
            "format": "sequencefile",
            "elapsed_sec": round(elapsed, 4),
            "rows_read": row_count,
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_column_subset(self, columns: List[str]) -> Dict[str, Any]:
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        projected = []
        for r in self._stream_records():
            projected.append({col: r.get(col) for col in columns})

        row_count = len(projected)
        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "column_subset",
            "format": "sequencefile",
            "elapsed_sec": round(elapsed, 4),
            "rows_read": row_count,
            "columns_requested": columns,
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_filtered(self, col: str = "PULocationID", val: Any = 132) -> Dict[str, Any]:
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        matched = []
        for r in self._stream_records():
            if r.get(col) == val:
                matched.append(r)

        matched_rows = len(matched)
        elapsed = time.perf_counter() - start
        metrics = monitor.stop()

        return {
            "query_type": "filtered_query",
            "format": "sequencefile",
            "elapsed_sec": round(elapsed, 4),
            "matched_rows": matched_rows,
            "filter": f"{col} == {val}",
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }

    def read_aggregation(self, group_by: str = "payment_type", agg_col: str = "fare_amount", agg_func: str = "mean") -> Dict[str, Any]:
        monitor = ResourceMonitor()
        monitor.start()
        start = time.perf_counter()

        sums = {}
        counts = {}
        for r in self._stream_records():
            g_val = r.get(group_by)
            a_val = r.get(agg_col)
            if g_val is not None and a_val is not None:
                try:
                    f_val = float(a_val)
                    sums[g_val] = sums.get(g_val, 0.0) + f_val
                    counts[g_val] = counts.get(g_val, 0) + 1
                except (ValueError, TypeError):
                    pass

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
            "format": "sequencefile",
            "elapsed_sec": round(elapsed, 4),
            "groups_computed": len(result),
            "aggregation_result": result,
            "peak_memory_mb": metrics["peak_memory_mb"],
            "avg_cpu_percent": metrics["avg_cpu_percent"]
        }
