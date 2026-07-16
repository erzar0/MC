"""Plot GPU metrics from ``nvidia-smi dmon`` output.

Parses dmon's whitespace-separated table (header lines starting with ``#``)
and renders one time-series panel per unit group: power (W), temperatures
(C), utilization (%), and clocks (MHz). By default the chart is drawn in the
terminal (plotext); pass ``-o file.png`` to save a PNG instead. With multiple
GPUs, each GPU gets its own block of panels.

Usage:
    # Plot a saved log in the terminal
    nvidia-smi dmon -d 1 > gpu.log   # ... later:
    python src/scripts/monitoring/gpu_dmon_plot.py gpu.log

    # Pipe directly
    nvidia-smi dmon -c 120 | python src/scripts/monitoring/gpu_dmon_plot.py

    # Collect live for 5 minutes, then plot
    python src/scripts/monitoring/gpu_dmon_plot.py --collect 300

    # Save a PNG instead of drawing in the terminal
    python src/scripts/monitoring/gpu_dmon_plot.py gpu.log -o gpu.png
"""

import argparse
import logging
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# Fixed categorical hue order (validated palette); color follows the metric,
# assigned per panel in this order.
SERIES_COLORS = ["#2a78d6", "#008300", "#e87ba4", "#eda100", "#1baf7a", "#eb6834"]
SURFACE = "#fcfcfb"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"

# Panels grouped by unit — one axis per unit, never mixed scales.
PANELS: list[tuple[str, str, list[str]]] = [
    ("Power", "W", ["pwr"]),
    ("Temperature", "°C", ["gtemp", "mtemp"]),
    ("Utilization", "%", ["sm", "mem", "enc", "dec", "jpg", "ofa"]),
    ("Clocks", "MHz", ["mclk", "pclk"]),
]


@dataclass
class DmonLog:
    """Parsed dmon samples: per-GPU column arrays."""

    columns: list[str]
    # {gpu_idx: {column: np.ndarray}}
    gpus: dict[int, dict[str, np.ndarray]]


def parse_dmon(lines: list[str]) -> DmonLog:
    """Parse ``nvidia-smi dmon`` text output.

    Reads column names from the first ``#``-prefixed header line and collects
    numeric rows per GPU index. Non-numeric cells (``-``) become NaN.
    """
    columns: list[str] = []
    rows: dict[int, list[list[float]]] = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            fields = line.lstrip("#").split()
            # First header line has names; second has units (W, C, %, MHz).
            if not columns and fields and fields[0].lower() == "gpu":
                columns = [f.lower() for f in fields]
            continue
        parts = line.split()
        if not columns or len(parts) != len(columns):
            continue
        try:
            gpu_idx = int(parts[0])
        except ValueError:
            continue
        values = [float(p) if p != "-" else float("nan") for p in parts[1:]]
        rows.setdefault(gpu_idx, []).append(values)

    if not columns or not rows:
        raise ValueError("No dmon samples found — is this `nvidia-smi dmon` output?")

    gpus = {
        idx: {col: np.array([r[i] for r in samples]) for i, col in enumerate(columns[1:])}
        for idx, samples in rows.items()
    }
    return DmonLog(columns=columns[1:], gpus=gpus)


def collect_dmon(seconds: int, interval: int) -> list[str]:
    """Run ``nvidia-smi dmon`` for the given duration and return its output lines."""
    count = max(1, seconds // interval)
    cmd = ["nvidia-smi", "dmon", "-d", str(interval), "-c", str(count)]
    logger.info("Collecting %d samples: %s", count, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.splitlines()


def _panel_series(data: dict[str, np.ndarray], metrics: list[str]) -> list[tuple[str, str, np.ndarray]]:
    """Select the (metric, color, values) series a panel should draw.

    Extra channels that never move off zero (enc/dec/jpg/ofa idle) are
    skipped; the first two (e.g. sm/mem) are always drawn.
    """
    series = []
    for i, metric in enumerate(metrics):
        y = data[metric]
        if i >= 2 and np.nansum(np.abs(y)) == 0:
            continue
        series.append((metric, SERIES_COLORS[i], y))
    return series


def _filtered_panels(log: DmonLog) -> list[tuple[str, str, list[str]]]:
    panels = [(title, unit, [m for m in metrics if m in log.columns]) for title, unit, metrics in PANELS]
    return [p for p in panels if p[2]]


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    return tuple(int(color[i : i + 2], 16) for i in (1, 3, 5))


def plot_terminal(log: DmonLog, interval: float) -> None:
    """Draw one 2x2 block of unit panels per GPU in the terminal."""
    import plotext as pltx

    gpu_ids = sorted(log.gpus)
    panels = _filtered_panels(log)
    block_rows = (len(panels) + 1) // 2

    pltx.subplots(block_rows * len(gpu_ids), 2)
    for g, gpu_idx in enumerate(gpu_ids):
        data = log.gpus[gpu_idx]
        t = (np.arange(len(next(iter(data.values())))) * interval).tolist()
        for p, (title, unit, metrics) in enumerate(panels):
            sub = pltx.subplot(g * block_rows + p // 2 + 1, p % 2 + 1)
            sub.theme("clear")
            prefix = f"GPU {gpu_idx} · " if len(gpu_ids) > 1 else ""
            sub.title(f"{prefix}{title} ({unit})")
            sub.xlabel("time (s)")
            if unit == "%":
                sub.ylim(0, 100)
            series = _panel_series(data, metrics)
            for metric, color, y in series:
                sub.plot(
                    t,
                    y.tolist(),
                    color=_hex_to_rgb(color),
                    label=metric if len(series) > 1 else None,
                )
    pltx.show()


def plot_png(log: DmonLog, output: Path, interval: float) -> None:
    """Render one row of unit panels per GPU and save to ``output``."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    gpu_ids = sorted(log.gpus)
    panels = _filtered_panels(log)

    n_rows, n_cols = len(gpu_ids), len(panels)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4.2 * n_cols, 2.9 * n_rows), squeeze=False)
    fig.patch.set_facecolor(SURFACE)

    for row, gpu_idx in enumerate(gpu_ids):
        data = log.gpus[gpu_idx]
        t = np.arange(len(next(iter(data.values())))) * interval
        for col, (title, unit, metrics) in enumerate(panels):
            ax = axes[row][col]
            ax.set_facecolor(SURFACE)
            ax.grid(True, color=GRIDLINE, linewidth=0.75)
            ax.set_axisbelow(True)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)
            for side in ("left", "bottom"):
                ax.spines[side].set_color(BASELINE)
            ax.tick_params(colors=INK_MUTED, labelsize=8)
            series = _panel_series(data, metrics)
            for metric, color, y in series:
                ax.plot(t, y, color=color, linewidth=2, label=metric)
            prefix = f"GPU {gpu_idx} · " if len(gpu_ids) > 1 else ""
            ax.set_title(f"{prefix}{title} ({unit})", fontsize=10, loc="left")
            if row == n_rows - 1:
                ax.set_xlabel("time (s)", fontsize=8, color=INK_MUTED)
            if unit == "%":
                ax.set_ylim(-2, 102)
            if len(series) > 1:
                ax.legend(fontsize=8, frameon=False, labelcolor="#52514e")

    fig.tight_layout()
    fig.savefig(output, dpi=150, facecolor=SURFACE)
    logger.info("Saved %s", output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "logfile",
        nargs="?",
        help="dmon log file to plot (omit to read stdin, or use --collect)",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="save a PNG to this path instead of drawing in the terminal",
    )
    parser.add_argument(
        "--collect",
        type=int,
        metavar="SECONDS",
        help="run nvidia-smi dmon for this many seconds instead of reading a log",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="seconds between dmon samples (dmon -d value; default 1)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.collect:
        lines = collect_dmon(args.collect, int(args.interval))
    elif args.logfile:
        lines = Path(args.logfile).read_text().splitlines()
    else:
        if sys.stdin.isatty():
            parser.error("no input: pass a logfile, pipe dmon output, or use --collect")
        lines = sys.stdin.read().splitlines()

    log = parse_dmon(lines)
    if args.output:
        plot_png(log, Path(args.output), args.interval)
    else:
        plot_terminal(log, args.interval)


if __name__ == "__main__":
    main()
