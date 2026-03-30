import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
import pacmap
import plotly.graph_objects as go
from pathlib import Path
import os

def load_data():
    project_root = Path(__file__).parent.parent.parent
    embeddings_path = "/home/kyre/repos/minecraft-world-generator/tmp/checkpoints/block_embeddings_ckpt_600.npy"
    states_path = project_root / "assets/block_states.txt"

    print(f"Loading embeddings from {embeddings_path}...")
    embeddings = np.load(embeddings_path)
    
    print(f"Loading block names from {states_path}...")
    with open(states_path, "r") as f:
        block_names = [line.strip() for line in f.readlines()]
    
    # Trim embeddings to match vocab size if needed
    embeddings = embeddings[:len(block_names)]
            
    return embeddings, block_names

def visualize_raw_3d(data, names, title, filename):
    print(f"Creating 3D visualization for {title}...")
    
    # Raw coordinates as colors (normalized just for the color map)
    c_min, c_max = data.min(axis=0), data.max(axis=0)
    colors = (data - c_min) / (c_max - c_min + 1e-12)
    
    fig = go.Figure(data=[go.Scatter3d(
        x=data[:, 0],
        y=data[:, 1],
        z=data[:, 2],
        text=names,
        mode='markers',
        marker=dict(
            size=1.5,
            color=['rgb({},{},{})'.format(int(r*255), int(g*255), int(b*255)) for r, g, b in colors],
            opacity=0.4
        )
    )])
    
    fig.update_layout(
        title=dict(
            text=f"<b>{title}</b><br><sup>Raw Dimensionality Reduction (No Filtering/Scaling)</sup>",
            font=dict(size=20, color="white"),
            x=0.5,
            y=0.95
        ),
        template="plotly_dark",
        scene=dict(
            xaxis=dict(title='Dim 1', gridcolor="#444", zerolinecolor="#666"),
            yaxis=dict(title='Dim 2', gridcolor="#444", zerolinecolor="#666"),
            zaxis=dict(title='Dim 3', gridcolor="#444", zerolinecolor="#666"),
            bgcolor="black"
        ),
        paper_bgcolor='black',
        plot_bgcolor='black',
        margin=dict(l=0, r=0, b=0, t=60),
        hoverlabel=dict(bgcolor="#222", font_size=12, font_family="Inter")
    )
    
    output_dir = Path("tmp/visualizations")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    fig.write_html(str(output_path))
    print(f"Visualization saved to {output_path}")

def main():
    try:
        embeddings, names = load_data()
    except Exception as e:
        print(f"Error: {e}")
        return

    print(f"Processing {len(names)} total blocks...")

    # 1. PCA (The most 'raw' reduction)
    print("Running PCA (3D)...")
    pca = PCA(n_components=3)
    pca_3d = pca.fit_transform(embeddings)
    visualize_raw_3d(pca_3d, names, "Raw PCA Reduction", "pca_raw_3d.html")

    # 2. PaCMAP (With standard settings, no post-reduction scaling)
    print("Running PaCMAP (3D)...")
    reducer = pacmap.PaCMAP(n_components=3)
    pacmap_3d = reducer.fit_transform(embeddings, init="pca")
    visualize_raw_3d(pacmap_3d, names, "Raw PaCMAP Reduction", "pacmap_raw_3d.html")

if __name__ == "__main__":
    main()
