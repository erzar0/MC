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
    embeddings_path = project_root / "tmp/block_embeddings.npy"
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
        
    scaler = MinMaxScaler()
    reduced_scaled = scaler.fit_transform(reduced)
    return reduced_scaled

def get_base_name(full_name):
    # Handle both [ and { for base name extraction
    import re
    return re.split(r'[\[\{]', full_name)[0]

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
        base = get_base_name(name)
        groups[base].append(i)
        
    base_to_representative_color = {}
    
    # Pick a random representative for each group
    for base, indices in groups.items():
        rep_idx = random.choice(indices)
        base_to_representative_color[base] = reduced_scaled[rep_idx]
            
    # Assign the group color to all members
    final_colors = []
    for i, name in enumerate(block_names):
        base = get_base_name(name)
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
    
    fig.update_layout(
        title=title,
        scene=dict(
            xaxis_title='Dim 1',
            yaxis_title='Dim 2',
            zaxis_title='Dim 3',
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
        name_clean = get_base_name(names[idx]).replace('universal_minecraft:', '')
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
    try:
        embeddings, names = load_data()
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # PaCMAP 3D reduction
    pacmap_3d = reduce_dimensions(embeddings)

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
