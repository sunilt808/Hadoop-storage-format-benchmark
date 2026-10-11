"""
Utility functions for benchmarking: timers, memory/CPU tracking with psutil,
formatting helpers, and statistical calculations.
"""

import time
import os
import psutil
import threading
import numpy as np
from typing import Dict, Any, Tuple, Callable

class ResourceMonitor:
    """Monitors CPU and RAM utilization in a background thread during benchmark execution."""
    def __init__(self, interval: float = 0.05):
        self.interval = interval
        self.process = psutil.Process(os.getpid())
        self.stop_flag = threading.Event()
        self.thread = None
        self.cpu_samples = []
        self.ram_samples_mb = []
        self.initial_ram_mb = 0.0

    def _monitor(self):
        while not self.stop_flag.is_set():
            try:
                # Process specific CPU percent and memory info
                cpu = self.process.cpu_percent(interval=None)
                ram = self.process.memory_info().rss / (1024 * 1024)
                self.cpu_samples.append(cpu)
                self.ram_samples_mb.append(ram)
            except Exception:
                pass
            time.sleep(self.interval)

    def start(self):
        self.stop_flag.clear()
        self.cpu_samples = []
        self.ram_samples_mb = []
        self.initial_ram_mb = self.process.memory_info().rss / (1024 * 1024)
        self.process.cpu_percent(interval=None)  # prime cpu calculation
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()

    def stop(self) -> Dict[str, float]:
        self.stop_flag.set()
        if self.thread:
            self.thread.join(timeout=1.0)
        
        avg_cpu = float(np.mean(self.cpu_samples)) if self.cpu_samples else 0.0
        max_cpu = float(np.max(self.cpu_samples)) if self.cpu_samples else 0.0
        peak_ram = float(np.max(self.ram_samples_mb)) if self.ram_samples_mb else self.initial_ram_mb
        delta_ram = max(0.0, peak_ram - self.initial_ram_mb)

        return {
            "avg_cpu_percent": round(avg_cpu, 2),
            "max_cpu_percent": round(max_cpu, 2),
            "peak_memory_mb": round(peak_ram, 2),
            "memory_delta_mb": round(delta_ram, 2)
        }


def compute_statistics(values: list) -> Dict[str, float]:
    """Computes mean, std_dev, min, max, median for a list of numerical benchmark runs."""
    if not values:
        return {"mean": 0.0, "std_dev": 0.0, "median": 0.0, "min": 0.0, "max": 0.0}
    arr = np.array(values, dtype=np.float64)
    return {
        "mean": float(np.mean(arr)),
        "std_dev": float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0,
        "median": float(np.median(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr))
    }


def format_bytes(size_bytes: int) -> str:
    """Formats raw bytes into human-readable B, KB, MB, GB."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.2f} GB"


def format_time_ms(seconds: float) -> str:
    """Formats seconds into ms or seconds."""
    if seconds < 1.0:
        return f"{seconds * 1000:.2f} ms"
    else:
        return f"{seconds:.3f} s"
