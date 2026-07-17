import sys
import numpy as np
import blosc2
from PIL import Image
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.block_colors import load_id2rgb

def main():
    volume_path = "tmp/processed_worlds/cleansed/3863/volumes/r.0.0.b2frame"
    out_png = "tmp/sample_heightmap.png"
    
    if not Path(volume_path).exists():
        print(f"Error: {volume_path} not found. Make sure you are running from the repo root.")
        sys.exit(1)
        
    print(f"Loading voxel volume from {volume_path}...")
    with open(volume_path, "rb") as f:
        grid = blosc2.unpack_array2(f.read())  # Shape: (X, Z, Y) uint16
        
    x_size, z_size, y_size = grid.shape
    print(f"Voxel grid shape: {grid.shape}")
    
    print("Loading unquantized block colors...")
    id2rgb, air_ids = load_id2rgb()
    air_set = set(air_ids.tolist())
    
    # 1. Compute heights and base colors
    heights = np.zeros((z_size, x_size), dtype=np.float32)
    colors = np.zeros((z_size, x_size, 3), dtype=np.uint8)
    
    for z in range(z_size):
        for x in range(x_size):
            # Scan down from the top of the Y-axis to find the first solid block
            for y in range(y_size - 1, -1, -1):
                block_id = grid[x, z, y]
                if block_id not in air_set:
                    colors[z, x] = id2rgb[block_id]
                    heights[z, x] = float(y)
                    break
                    
    # 2. Render with height-based and hillshading (light source from North-West / top-left)
    img_data = np.zeros((z_size, x_size, 3), dtype=np.uint8)
    
    for z in range(z_size):
        for x in range(x_size):
            if np.all(colors[z, x] == 0):
                # Sky/void background
                img_data[z, x] = [100, 149, 237] # Cornflower blue sky color for void
                continue
                
            # Compute slopes relative to North and West neighbors
            dz = heights[z, x] - heights[max(0, z-1), x]
            dx = heights[z, x] - heights[z, max(0, x-1)]
            slope = dx + dz
            
            # Shading multiplier (0.5 to 1.3) based on slopes
            shading = 0.8 + 0.3 * np.clip(slope / 2.0, -1.0, 1.0)
            
            # Height-based brightness factor (higher = slightly brighter)
            height_factor = 0.8 + 0.2 * (heights[z, x] / max(y_size - 1, 1))
            
            # Combine shading and color
            final_color = np.clip(colors[z, x] * shading * height_factor, 0, 255).astype(np.uint8)
            img_data[z, x] = final_color
            
    # Save as PNG
    img = Image.fromarray(img_data)
    img.save(out_png)
    print(f"Sample region heightmap visualization saved to {out_png}")

    # 3. Generate MP4 of Y layers
    mp4_path = Path("tmp/sample_y_layers.mp4")
    try:
        import imageio.v2 as imageio
        print(f"Generating video of Y layers at {mp4_path}...")
        
        # id2rgb shape: (vocab_size, 3)
        # grid shape: (X, Z, Y)
        # Map grid block IDs to colors: (X, Z, Y, 3)
        rgb = id2rgb[grid]
        
        writer = imageio.get_writer(str(mp4_path), fps=16, macro_block_size=1)
        try:
            for y in range(y_size):
                frame = rgb[:, :, y, :]
                # Upscale by 2x for visualization visibility
                frame = np.repeat(np.repeat(frame, 2, axis=0), 2, axis=1)
                writer.append_data(frame)
        finally:
            writer.close()
        print(f"Sample Y-layers video saved to {mp4_path}")
    except ImportError:
        print("Warning: imageio not installed. Skipping video generation.")

if __name__ == "__main__":
    main()
