import numpy as np
import pandas as pd
import pacmap
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt
plt.switch_backend('Agg')
from pathlib import Path
import os

def load_data():
    project_root = Path(__file__).parent.parent.parent
    embeddings_path = project_root / "/home/kyre/repos/minecraft-world-generator/tmp/checkpoints/block_embeddings_ckpt_22000.npy"
    states_path = project_root / "assets/block_states.txt"

    print(f"Loading embeddings from {embeddings_path}...")
    embeddings = np.load(embeddings_path)
    
    # Load block names
    print(f"Loading block names from {states_path}...")
    with open(states_path, "r") as f:
        block_names = [line.strip() for line in f.readlines()]
    
    vocab_size_observed = len(block_names)
    embeddings = embeddings[:vocab_size_observed]
            
    return embeddings, block_names

def reduce_dimensions(embeddings):
    print("Reducing dimensions using PaCMAP to 3D...")
    reducer = pacmap.PaCMAP(n_components=3, n_neighbors=None, MN_ratio=0.5, FP_ratio=2.0)
    reduced = reducer.fit_transform(embeddings, init="pca")
    return reduced

def get_group_key(full_name):
    """Extract base name + material property for grouping.
    e.g. 'universal_minecraft:stairs[facing="south",material="quartz",shape="straight"]'
      -> 'universal_minecraft:stairs|quartz'
    """
    import re
    base = re.split(r'[\[\{]', full_name)[0]
    mat = re.search(r'material="([^"]*)"', full_name)
    if mat:
        return f"{base}|{mat.group(1)}"
    color = re.search(r'color="([^"]*)"', full_name)
    if color:
        return f"{base}|{color.group(1)}"
    return base

def get_representative_colors(block_names, reduced_scaled):
    """
    Groups blocks by base name and assigns a color based on a RANDOM 
    representative state for the whole group.
    """
    import random
    from collections import defaultdict
    
    # Group indices by base name
    groups = defaultdict(list)
    for i, name in enumerate(block_names):
        base = get_group_key(name)
        groups[base].append(i)
        
    base_to_representative_color = {}
    
    # Pick a random representative for each group
    for base, indices in groups.items():
        rep_idx = random.choice(indices)
        base_to_representative_color[base] = reduced_scaled[rep_idx]
            
    # Assign the group color to all members
    final_colors = []
    for i, name in enumerate(block_names):
        base = get_group_key(name)
        final_colors.append(base_to_representative_color[base])
        
    return np.array(final_colors)

def visualize_3d_plotly(data, names, colors, title="3D Embedding Visualization", filename="output.html"):
    print(f"Creating 3D Plotly visualization: {title}...")
    fig = go.Figure(data=[go.Scatter3d(
        x=data[:, 0],
        y=data[:, 1],
        z=data[:, 2],
        text=names,
        mode='markers',
        marker=dict(
            size=2,
            color=['rgb({},{},{})'.format(int(r*255), int(g*255), int(b*255)) for r, g, b in np.clip(colors, 0, 1)],
            opacity=0.6
        )
    )])
    
    # RGB gradient indicators along axis edges
    n_grad = 40
    t = np.linspace(0, 1, n_grad)
    axis_cfg = [
        # (x, y, z, R, G, B) — each axis gets its channel ramping, others at 0
        (t, np.zeros(n_grad), np.zeros(n_grad), t, np.zeros(n_grad), np.zeros(n_grad)),       # X=Red
        (np.zeros(n_grad), t, np.zeros(n_grad), np.zeros(n_grad), t, np.zeros(n_grad)),       # Y=Green
        (np.zeros(n_grad), np.zeros(n_grad), t, np.zeros(n_grad), np.zeros(n_grad), t),       # Z=Blue
    ]
    labels = ['R', 'G', 'B']
    for (gx, gy, gz, gr, gg, gb), label in zip(axis_cfg, labels):
        fig.add_trace(go.Scatter3d(
            x=gx, y=gy, z=gz,
            mode='markers',
            marker=dict(
                size=4,
                color=['rgb({},{},{})'.format(int(r*255), int(g*255), int(b*255)) for r, g, b in zip(gr, gg, gb)],
                opacity=1.0,
            ),
            name=label,
            hoverinfo='skip',
            showlegend=False,
        ))
    
    fig.update_layout(
        title=title,
        scene=dict(
            xaxis_title='R',
            yaxis_title='G',
            zaxis_title='B',
            xaxis=dict(backgroundcolor="white", gridcolor="lightgrey", showbackground=True),
            yaxis=dict(backgroundcolor="white", gridcolor="lightgrey", showbackground=True),
            zaxis=dict(backgroundcolor="white", gridcolor="lightgrey", showbackground=True),
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        font_color='black',
        margin=dict(l=0, r=0, b=0, t=40)
    )
    
    Path("tmp").mkdir(exist_ok=True)
    output_path = f"tmp/{filename}"
    fig.write_html(output_path)
    print(f"3D visualization saved to {output_path}")

def visualize_2d_seaborn(data, names, colors, title="2D Projection", filename="output.svg"):
    import random
    dims = random.sample(range(3), 2)
    d1, d2 = dims[1], dims[0]
    
    print(f"Creating 2D Seaborn SVG: {title} (Dimensions {d1+1} vs {d2+1})...")
    
    num_points = data.shape[0]
    label_indices = np.linspace(0, num_points - 1, 150, dtype=int)
    
    plt.figure(figsize=(20, 20), facecolor='white')
    ax = plt.gca()
    
    ax.scatter(data[:, d1], data[:, d2], c=np.clip(colors, 0, 1), s=15, alpha=0.5)
    
    for idx in label_indices:
        name_clean = get_group_key(names[idx]).replace('universal_minecraft:', '')
        ax.text(data[idx, d1], data[idx, d2], name_clean, fontsize=8, alpha=0.8)
    
    ax.set_facecolor('white')
    ax.set_xlabel(f'Dimension {d1+1}', fontsize=14)
    ax.set_ylabel(f'Dimension {d2+1}', fontsize=14)
    ax.tick_params(colors='black')
    
    plt.title(f"{title} - Dimensions {d1+1} vs {d2+1}", color='black', fontsize=20)
    
    Path("tmp").mkdir(exist_ok=True)
    output_path = f"tmp/{filename}"
    plt.savefig(output_path, format='svg', facecolor='white')
    print(f"2D plot saved to {output_path}")

def main():
    SQUISH_OUTLIERS = True

    try:
        embeddings, names = load_data()
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Compute raw norms before normalization (for untrained block filtering)
    raw_norms = np.linalg.norm(embeddings, axis=1)
    
    # Pre-process embeddings: mean subtraction
    print("Centering raw embeddings...")
    embeddings = embeddings - embeddings.mean(axis=0)
    
    # PaCMAP 3D reduction on ALL embeddings (preserves global structure)
    pacmap_3d = reduce_dimensions(embeddings)

    # Filter out untrained blocks AFTER PaCMAP (using raw norms)
    keep_mask = raw_norms > 0.0
    
    n_filtered = (~keep_mask).sum()
    print(f"Filtered {n_filtered} blocks (untrained). Keeping {keep_mask.sum()}/{len(names)}.")
    
    pacmap_3d = pacmap_3d[keep_mask]
    names = [n for n, k in zip(names, keep_mask) if k]
    
    # Center the 3D embeddings
    print("Centering 3D coordinates...")
    pacmap_3d = pacmap_3d - pacmap_3d.mean(axis=0)

    # 1. Align main point cloud variances to RGB axes via PCA
    # This removes diagonal correlation, so the point cloud fills the axis-aligned RGB box
    # print("Decorrelating axes with PCA to fill color space...")
    from sklearn.decomposition import PCA
    pacmap_3d = PCA(n_components=3).fit_transform(pacmap_3d)

    import warnings
    warnings.filterwarnings("ignore", category=UserWarning, module="sklearn.preprocessing._data")

    # 2. Advanced Adaptive Squishing: Histogram Equalization (QuantileTransformer)
    # This non-linear algorithm maps the empirical CDF of each axis perfectly to [0, 1].
    # Dense clusters are entirely expanded to reveal their internal structure, and 
    # sparse outlier tails are smoothly, tightly compressed, yielding ZERO empty space.
    if SQUISH_OUTLIERS:
        print("Applying advanced Quantile Transformation (Histogram Equalization)...")
        from sklearn.preprocessing import QuantileTransformer
        # High n_quantiles ensures smooth color gradients across the vocabulary
        scaler = QuantileTransformer(n_quantiles=100000, output_distribution='uniform', subsample=1000000)
        pacmap_3d = scaler.fit_transform(pacmap_3d)

    scaler = MinMaxScaler()
    pacmap_3d = scaler.fit_transform(pacmap_3d)

    # Reconstruct the full vocabulary array including the unfiltered blocks
    # Untrained blocks will default to [0, 0, 0] (black)
    full_colors = np.zeros((len(keep_mask), 3), dtype=np.float32)
    full_colors[keep_mask] = pacmap_3d
    
    # Save the mapped colors as a 0-255 uint8 NumPy array for use as a LUT
    lut_uint8 = (full_colors * 255).astype(np.uint8)
    lut_path = Path("tmp/block_embeddings_rgb.npy")
    np.save(lut_path, lut_uint8)
    print(f"Saved {len(lut_uint8)} RGB colors to {lut_path} for use as LUT.")
    # Mode 1: Individual Coloring (Every state is unique)
    print("\n--- Mode 1: Individual State Colors ---")
    visualize_3d_plotly(pacmap_3d, names, pacmap_3d, title="PaCMAP Individual Colors", filename="pacmap_individual_3d.html")
    visualize_2d_seaborn(pacmap_3d, names, pacmap_3d, title="PaCMAP Individual Colors", filename="pacmap_individual_2d.svg")
    
    # Mode 2: Grouped Representative Coloring (Grouped by base name)
    print("\n--- Mode 2: Grouped Representative Colors ---")
    grouped_colors = get_representative_colors(names, pacmap_3d)
    visualize_3d_plotly(pacmap_3d, names, grouped_colors, title="PaCMAP Grouped Colors", filename="pacmap_grouped_3d.html")
    visualize_2d_seaborn(pacmap_3d, names, grouped_colors, title="PaCMAP Grouped Colors", filename="pacmap_grouped_2d.svg")

if __name__ == "__main__":
    main()
