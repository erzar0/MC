import sys
import numpy as np
from PIL import Image
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.block_colors import load_id2rgb

def main():
    npy_path = "tmp/generated_check.npy"
    out_png = "tmp/generated_check_heightmap.png"
    
    if not Path(npy_path).exists():
        print(f"Error: {npy_path} not found.")
        sys.exit(1)
        
    print(f"Loading voxel grid from {npy_path}...")
    grid = np.load(npy_path)  # Shape: (X, Z, Y)
    x_size, z_size, y_size = grid.shape
    print(f"Voxel grid shape: {grid.shape}")
    
    print("Loading block colors...")
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
                # Black background for void/air
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
    print(f"Voxel heightmap visualization saved to {out_png}")

if __name__ == "__main__":
    main()
