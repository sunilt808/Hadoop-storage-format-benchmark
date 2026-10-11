"""
Dashboard Components Module.
Provides clean, dark-themed Plotly charts for Hadoop Storage Benchmarking.
"""

from typing import Any, Dict, List
import pandas as pd
import plotly.graph_objects as go

# Consistent color palette: Parquet = green, Avro = blue, SequenceFile = orange
COLOR_MAP = {
    "parquet": "#10b981",       # Emerald Green
    "avro": "#3b82f6",          # Sky Blue
    "sequencefile": "#f59e0b",  # Amber Orange
    "PARQUET": "#10b981",
    "AVRO": "#3b82f6",
    "SEQUENCEFILE": "#f59e0b"
}


def _extract_val(val: Any) -> float:
    """Extract numeric value from dict with 'mean', string with ± or %, or float."""
    if isinstance(val, dict):
        return float(val.get("mean", 0.0))
    if isinstance(val, str):
        clean = val.split("±")[0].split("+-")[0].replace("%", "").replace("x", "").strip()
        try:
            return float(clean)
        except ValueError:
            return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _common_layout(title: str, yaxis_title: str = "", xaxis_title: str = "") -> dict:
    """Common dark theme layout tokens for clean presentation."""
    return dict(
        title=dict(
            text=title,
            font=dict(family="Inter, sans-serif", size=14, color="#f8fafc")
        ),
        xaxis=dict(
            title=xaxis_title,
            gridcolor="rgba(255, 255, 255, 0.06)",
            tickfont=dict(family="Inter, sans-serif", size=11, color="#cbd5e1"),
            title_font=dict(family="Inter, sans-serif", size=11, color="#94a3b8")
        ),
        yaxis=dict(
            title=yaxis_title,
            gridcolor="rgba(255, 255, 255, 0.06)",
            tickfont=dict(family="Inter, sans-serif", size=11, color="#cbd5e1"),
            title_font=dict(family="Inter, sans-serif", size=11, color="#94a3b8")
        ),
        template="plotly_dark",
        paper_bgcolor="rgba(0, 0, 0, 0)",
        plot_bgcolor="rgba(30, 41, 59, 0.45)",
        font=dict(family="Inter, sans-serif", color="#cbd5e1"),
        margin=dict(l=30, r=30, t=45, b=35),
        height=360
    )


def plot_file_size(df_results: pd.DataFrame, raw_csv_mb: float = 329.31) -> go.Figure:
    """
    Horizontal bar chart of file sizes with raw CSV baseline reference.
    Operation: Compress & Storage
    """
    df = df_results.copy()
    if "config_label" not in df.columns:
        df["config_label"] = df["Format"].astype(str) + " (" + df["Codec"].astype(str) + ")"
    if "format" not in df.columns:
        df["format"] = df["Format"].str.lower()

    size_col = "file_size_mb" if "file_size_mb" in df.columns else "File Size (MB)"
    df["size_mb"] = [_extract_val(v) for v in df[size_col]]

    # Sort descending so smallest file is at top in horizontal layout
    df = df.sort_values(by="size_mb", ascending=False)
    colors = [COLOR_MAP.get(fmt.lower(), "#94a3b8") for fmt in df["format"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["config_label"],
        x=df["size_mb"],
        orientation="h",
        marker=dict(color=colors),
        text=[f"{s:.1f} MB" for s in df["size_mb"]],
        textposition="outside",
        hoverinfo="y+x"
    ))

    fig.add_vline(
        x=raw_csv_mb,
        line_dash="dash",
        line_color="#ef4444",
        line_width=1.5,
        annotation_text=f"Raw CSV Baseline ({raw_csv_mb:.1f} MB)",
        annotation_position="top right",
        annotation_font=dict(color="#f87171", size=11)
    )

    layout = _common_layout(
        title="<b>Compress & Storage: File Size (smaller is better)</b>",
        xaxis_title="Disk Size (MB)"
    )
    layout["height"] = max(340, len(df) * 30)
    layout["margin"] = dict(l=140, r=40, t=45, b=35)
    fig.update_layout(**layout)
    return fig


def plot_write_speed(df_results: pd.DataFrame) -> go.Figure:
    """
    Bar chart of write speed in MB/s.
    Operation: Write Operations
    """
    df = df_results.copy()
    if "config_label" not in df.columns:
        df["config_label"] = df["Format"].astype(str) + " (" + df["Codec"].astype(str) + ")"
    if "format" not in df.columns:
        df["format"] = df["Format"].str.lower()

    speed_col = "write_throughput_mb_s" if "write_throughput_mb_s" in df.columns else "Write Throughput (MB/s)"
    df["speed"] = [_extract_val(v) for v in df[speed_col]]

    colors = [COLOR_MAP.get(fmt.lower(), "#94a3b8") for fmt in df["format"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["config_label"],
        y=df["speed"],
        marker=dict(color=colors),
        text=[f"{s:.1f} MB/s" for s in df["speed"]],
        textposition="outside",
        hoverinfo="x+y"
    ))

    layout = _common_layout(
        title="<b>Write Operations: Speed in MB/s (higher is better)</b>",
        yaxis_title="Write Speed (MB/s)"
    )
    layout["xaxis"]["tickangle"] = -25
    layout["height"] = 360
    fig.update_layout(**layout)
    return fig


def plot_query_speed(df_results: pd.DataFrame) -> go.Figure:
    """
    Grouped bar chart for query execution times with log-scale y-axis.
    Operation: Read Operations (Full Scan, Column Query, Filter Query, Aggregation)
    Colors: Parquet = green, Avro = blue, SequenceFile = orange.
    """
    df = df_results.copy()
    if "format" not in df.columns:
        df["format"] = df["Format"].str.lower()
    if "codec" not in df.columns:
        df["codec"] = df["Codec"].str.lower()

    scan_col = "full_scan_sec" if "full_scan_sec" in df.columns else "Full Scan (s)"
    col_col = "column_subset_sec" if "column_subset_sec" in df.columns else "Column Subset (s)"
    if col_col not in df.columns and "Column Query (s)" in df.columns:
        col_col = "Column Query (s)"
    filt_col = "filtered_query_sec" if "filtered_query_sec" in df.columns else "Filtered Query (s)"
    if filt_col not in df.columns and "Filter Query (s)" in df.columns:
        filt_col = "Filter Query (s)"
    agg_col = "aggregation_query_sec" if "aggregation_query_sec" in df.columns else "Aggregation (s)"

    queries = ["Full Scan", "Column Query", "Filter Query", "Aggregation"]
    fig = go.Figure()

    format_order = ["parquet", "avro", "sequencefile"]
    for fmt in format_order:
        sub = df[df["format"].str.lower() == fmt]
        if sub.empty:
            continue

        # Pick primary standard codec: snappy for parquet/avro, none for sequencefile
        row = sub[sub["codec"].str.lower() == "snappy"]
        if row.empty:
            row = sub[sub["codec"].str.lower() == "none"]
        if row.empty:
            row = sub.iloc[0:1]

        r = row.iloc[0]
        codec_name = r.get("codec", r.get("Codec", ""))
        vals = [
            _extract_val(r[scan_col]),
            _extract_val(r[col_col]),
            _extract_val(r[filt_col]),
            _extract_val(r[agg_col])
        ]

        text_labels = []
        for v in vals:
            if v < 1.0:
                text_labels.append(f"{v:.3f}s")
            elif v < 100.0:
                text_labels.append(f"{v:.1f}s")
            else:
                text_labels.append(f"{v:,.0f}s")

        fig.add_trace(go.Bar(
            x=queries,
            y=vals,
            name=f"{fmt.capitalize()} ({codec_name})",
            marker_color=COLOR_MAP.get(fmt, "#94a3b8"),
            text=text_labels,
            textposition="outside",
            hoverinfo="name+x+y"
        ))

    layout = _common_layout(
        title="<b>Read Operations: Query Speed (lower is better)</b>",
        yaxis_title="Execution Time in seconds (log scale)"
    )
    layout["yaxis"]["type"] = "log"
    layout["barmode"] = "group"
    layout["showlegend"] = True
    layout["legend"] = dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
        font=dict(size=11)
    )
    layout["height"] = 380
    fig.update_layout(**layout)
    return fig


def plot_resource_usage(df_results: pd.DataFrame) -> go.Figure:
    """
    Bar chart comparing Peak RAM memory footprint across formats.
    Operation: Other Operations (Resource Footprint)
    Colors: Parquet = green, Avro = blue, SequenceFile = orange.
    """
    df = df_results.copy()
    if "format" not in df.columns:
        df["format"] = df["Format"].str.lower()
    if "codec" not in df.columns:
        df["codec"] = df["Codec"].str.lower()

    ram_col = "write_peak_ram_mb" if "write_peak_ram_mb" in df.columns else "Peak RAM (MB)"

    fig = go.Figure()
    format_order = ["parquet", "avro", "sequencefile"]
    labels = []
    values = []
    colors = []

    for fmt in format_order:
        sub = df[df["format"].str.lower() == fmt]
        if sub.empty:
            continue
        row = sub[sub["codec"].str.lower() == "snappy"]
        if row.empty:
            row = sub[sub["codec"].str.lower() == "none"]
        if row.empty:
            row = sub.iloc[0:1]
        r = row.iloc[0]
        codec_name = r.get("codec", r.get("Codec", ""))
        val = _extract_val(r[ram_col])
        labels.append(f"{fmt.capitalize()} ({codec_name})")
        values.append(val)
        colors.append(COLOR_MAP.get(fmt, "#94a3b8"))

    fig.add_trace(go.Bar(
        x=labels,
        y=values,
        marker=dict(color=colors),
        text=[f"{v:.1f} MB" for v in values],
        textposition="outside",
        hoverinfo="x+y"
    ))

    layout = _common_layout(
        title="<b>Other Operations: Memory Footprint - Peak RAM (lower is better)</b>",
        yaxis_title="Peak RAM (MB)"
    )
    layout["height"] = 360
    fig.update_layout(**layout)
    return fig
