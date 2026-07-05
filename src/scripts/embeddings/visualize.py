"""Visualize trained block2vec embeddings as RGB colors.

Reduces embeddings to 3D with PaCMAP, maps the axes to RGB, and renders
interactive Plotly HTML plus static SVG plots. Also saves the resulting
per-block RGB lookup table (`block_embeddings_rgb.npy`).
"""

import argparse
import logging
import random
import re
import sys
import warnings
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pacmap
import plotly.graph_objects as go
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler, QuantileTransformer

plt.switch_backend("Agg")

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src import config

logger = logging.getLogger(__name__)


def find_default_checkpoint() -> Path | None:
    """Returns the newest block-embedding checkpoint under tmp/checkpoints, if any."""
    ckpt_dir = config.PROJECT_ROOT / "tmp" / "checkpoints"
    candidates = sorted(ckpt_dir.glob("block_embeddings_ckpt_*.npy"), key=lambda p: p.stat().st_mtime)
    return candidates[-1] if candidates else None


def load_data(checkpoint_path: Path, states_path: Path):
    logger.info(f"Loading embeddings from {checkpoint_path}...")
    embeddings = np.load(checkpoint_path)

    logger.info(f"Loading block names from {states_path}...")
    with open(states_path, "r") as f:
        block_names = [line.strip() for line in f.readlines()]

    vocab_size_observed = len(block_names)
    embeddings = embeddings[:vocab_size_observed]

    return embeddings, block_names


def reduce_dimensions(embeddings):
    logger.info("Reducing dimensions using PaCMAP to 3D...")
    reducer = pacmap.PaCMAP(n_components=3, n_neighbors=None, MN_ratio=0.5, FP_ratio=2.0)
    return reducer.fit_transform(embeddings, init="pca")


def get_group_key(full_name):
    """Extract base name + material property for grouping.
    e.g. 'universal_minecraft:stairs[facing="south",material="quartz",shape="straight"]'
      -> 'universal_minecraft:stairs|quartz'
    """
    base = re.split(r"[\[\{]", full_name)[0]
    mat = re.search(r'material="([^"]*)"', full_name)
    if mat:
        return f"{base}|{mat.group(1)}"
    color = re.search(r'color="([^"]*)"', full_name)
    if color:
        return f"{base}|{color.group(1)}"
    return base


def get_representative_colors(block_names, reduced_scaled):
    """Groups blocks by base name and colors each group by a random representative state."""
    groups = defaultdict(list)
    for i, name in enumerate(block_names):
        groups[get_group_key(name)].append(i)

    base_to_representative_color = {base: reduced_scaled[random.choice(indices)] for base, indices in groups.items()}

    return np.array([base_to_representative_color[get_group_key(name)] for name in block_names])


def visualize_3d_plotly(
    data, names, colors, output_dir: Path, title="3D Embedding Visualization", filename="output.html"
):
    logger.info(f"Creating 3D Plotly visualization: {title}...")
    fig = go.Figure(
        data=[
            go.Scatter3d(
                x=data[:, 0],
                y=data[:, 1],
                z=data[:, 2],
                text=names,
                mode="markers",
                marker=dict(
                    size=2,
                    color=[
                        "rgb({},{},{})".format(int(r * 255), int(g * 255), int(b * 255))
                        for r, g, b in np.clip(colors, 0, 1)
                    ],
                    opacity=0.6,
                ),
            )
        ]
    )

    # RGB gradient indicators along axis edges
    n_grad = 40
    t = np.linspace(0, 1, n_grad)
    zeros = np.zeros(n_grad)
    axis_cfg = [
        # (x, y, z, R, G, B) — each axis gets its channel ramping, others at 0
        (t, zeros, zeros, t, zeros, zeros),  # X=Red
        (zeros, t, zeros, zeros, t, zeros),  # Y=Green
        (zeros, zeros, t, zeros, zeros, t),  # Z=Blue
    ]
    labels = ["R", "G", "B"]
    for (gx, gy, gz, gr, gg, gb), label in zip(axis_cfg, labels, strict=True):
        fig.add_trace(
            go.Scatter3d(
                x=gx,
                y=gy,
                z=gz,
                mode="markers",
                marker=dict(
                    size=4,
                    color=[
                        "rgb({},{},{})".format(int(r * 255), int(g * 255), int(b * 255))
                        for r, g, b in zip(gr, gg, gb, strict=True)
                    ],
                    opacity=1.0,
                ),
                name=label,
                hoverinfo="skip",
                showlegend=False,
            )
        )

    fig.update_layout(
        title=title,
        scene=dict(
            xaxis_title="R",
            yaxis_title="G",
            zaxis_title="B",
            xaxis=dict(backgroundcolor="white", gridcolor="lightgrey", showbackground=True),
            yaxis=dict(backgroundcolor="white", gridcolor="lightgrey", showbackground=True),
            zaxis=dict(backgroundcolor="white", gridcolor="lightgrey", showbackground=True),
        ),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font_color="black",
        margin=dict(l=0, r=0, b=0, t=40),
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    fig.write_html(output_path)
    logger.info(f"3D visualization saved to {output_path}")


def visualize_2d_seaborn(data, names, colors, output_dir: Path, title="2D Projection", filename="output.svg"):
    dims = random.sample(range(3), 2)
    d1, d2 = dims[1], dims[0]

    logger.info(f"Creating 2D SVG: {title} (Dimensions {d1 + 1} vs {d2 + 1})...")

    num_points = data.shape[0]
    label_indices = np.linspace(0, num_points - 1, 150, dtype=int)

    plt.figure(figsize=(20, 20), facecolor="white")
    ax = plt.gca()

    ax.scatter(data[:, d1], data[:, d2], c=np.clip(colors, 0, 1), s=15, alpha=0.5)

    for idx in label_indices:
        name_clean = get_group_key(names[idx]).replace("universal_minecraft:", "")
        ax.text(data[idx, d1], data[idx, d2], name_clean, fontsize=8, alpha=0.8)

    ax.set_facecolor("white")
    ax.set_xlabel(f"Dimension {d1 + 1}", fontsize=14)
    ax.set_ylabel(f"Dimension {d2 + 1}", fontsize=14)
    ax.tick_params(colors="black")

    plt.title(f"{title} - Dimensions {d1 + 1} vs {d2 + 1}", color="black", fontsize=20)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    plt.savefig(output_path, format="svg", facecolor="white")
    logger.info(f"2D plot saved to {output_path}")


def compute_rgb_coordinates(embeddings, raw_norms, squish_outliers: bool = True):
    """Maps embeddings to [0, 1]^3 RGB coordinates via PaCMAP + PCA + equalization.

    Returns:
        Tuple of (rgb coords for kept blocks, boolean keep mask over the vocabulary).
    """
    # Pre-process embeddings: mean subtraction
    logger.info("Centering raw embeddings...")
    embeddings = embeddings - embeddings.mean(axis=0)

    # PaCMAP 3D reduction on ALL embeddings (preserves global structure)
    pacmap_3d = reduce_dimensions(embeddings)

    # Filter out untrained blocks AFTER PaCMAP (using raw norms)
    keep_mask = raw_norms > 0.0
    n_filtered = (~keep_mask).sum()
    logger.info(f"Filtered {n_filtered} blocks (untrained). Keeping {keep_mask.sum()}/{len(keep_mask)}.")
    pacmap_3d = pacmap_3d[keep_mask]

    # Center the 3D embeddings
    logger.info("Centering 3D coordinates...")
    pacmap_3d = pacmap_3d - pacmap_3d.mean(axis=0)

    # 1. Align main point cloud variances to RGB axes via PCA.
    # This removes diagonal correlation, so the point cloud fills the axis-aligned RGB box.
    pacmap_3d = PCA(n_components=3).fit_transform(pacmap_3d)

    warnings.filterwarnings("ignore", category=UserWarning, module="sklearn.preprocessing._data")

    # 2. Adaptive squishing: histogram equalization maps the empirical CDF of each
    # axis to [0, 1]. Dense clusters expand, sparse outlier tails compress.
    if squish_outliers:
        logger.info("Applying Quantile Transformation (histogram equalization)...")
        scaler = QuantileTransformer(n_quantiles=100000, output_distribution="uniform", subsample=1000000)
        pacmap_3d = scaler.fit_transform(pacmap_3d)

    pacmap_3d = MinMaxScaler().fit_transform(pacmap_3d)
    return pacmap_3d, keep_mask


def main():
    parser = argparse.ArgumentParser(description="Visualize block2vec embeddings as RGB colors")
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=None,
        help="Path to a block_embeddings_*.npy checkpoint (default: newest under tmp/checkpoints)",
    )
    parser.add_argument(
        "--block-states",
        type=Path,
        default=config.PROJECT_ROOT / "assets" / "block_states.txt",
        help="Path to block_states.txt",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=config.PROJECT_ROOT / "tmp",
        help="Directory for plots and the RGB LUT",
    )
    parser.add_argument("--no-squish", action="store_true", help="Disable quantile-based outlier squishing")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    checkpoint = args.checkpoint or find_default_checkpoint()
    if checkpoint is None or not Path(checkpoint).exists():
        logger.error("No embeddings checkpoint found. Pass --checkpoint explicitly.")
        raise SystemExit(1)

    embeddings, names = load_data(Path(checkpoint), args.block_states)

    # Compute raw norms before normalization (for untrained block filtering)
    raw_norms = np.linalg.norm(embeddings, axis=1)

    pacmap_3d, keep_mask = compute_rgb_coordinates(embeddings, raw_norms, squish_outliers=not args.no_squish)
    names = [n for n, k in zip(names, keep_mask, strict=True) if k]

    # Reconstruct the full vocabulary array including the unfiltered blocks.
    # Untrained blocks default to [0, 0, 0] (black).
    full_colors = np.zeros((len(keep_mask), 3), dtype=np.float32)
    full_colors[keep_mask] = pacmap_3d

    # Save the mapped colors as a 0-255 uint8 NumPy array for use as a LUT
    lut_uint8 = (full_colors * 255).astype(np.uint8)
    lut_path = args.output_dir / "block_embeddings_rgb.npy"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    np.save(lut_path, lut_uint8)
    logger.info(f"Saved {len(lut_uint8)} RGB colors to {lut_path} for use as LUT.")

    # Mode 1: Individual Coloring (Every state is unique)
    logger.info("--- Mode 1: Individual State Colors ---")
    visualize_3d_plotly(
        pacmap_3d,
        names,
        pacmap_3d,
        args.output_dir,
        title="PaCMAP Individual Colors",
        filename="pacmap_individual_3d.html",
    )
    visualize_2d_seaborn(
        pacmap_3d,
        names,
        pacmap_3d,
        args.output_dir,
        title="PaCMAP Individual Colors",
        filename="pacmap_individual_2d.svg",
    )

    # Mode 2: Grouped Representative Coloring (Grouped by base name)
    logger.info("--- Mode 2: Grouped Representative Colors ---")
    grouped_colors = get_representative_colors(names, pacmap_3d)
    visualize_3d_plotly(
        pacmap_3d,
        names,
        grouped_colors,
        args.output_dir,
        title="PaCMAP Grouped Colors",
        filename="pacmap_grouped_3d.html",
    )
    visualize_2d_seaborn(
        pacmap_3d,
        names,
        grouped_colors,
        args.output_dir,
        title="PaCMAP Grouped Colors",
        filename="pacmap_grouped_2d.svg",
    )


if __name__ == "__main__":
    main()
