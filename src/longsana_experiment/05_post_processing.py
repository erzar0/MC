import numpy as np

def map_rgb_to_blocks(spatial_cube: np.ndarray, lut: list):
    """
    Stage 5: Maps the continuous float floating-point RGB colors 
    predicted by the diffusion model back to valid discrete Minecraft block IDs 
    using the predefined Look-Up Table and a KDTree.
    """
    from scipy.spatial import cKDTree
    print("\\n[POST-PROCESSING] Mapping RGB values back to discrete Minecraft blocks...")
    
    # Convert LUT list parsing to numpy array (N, 3)
    lut_array = np.array([
        [int(val) for val in row.split("|")] for row in lut
    ], dtype=np.uint8)
    
    flat_cube = spatial_cube.reshape(-1, 3)
    
    # Create KDTree for fast nearest-neighbor search to snap generated colors to closest game palette colors
    tree = cKDTree(lut_array)
    distances, block_ids = tree.query(flat_cube)
    
    # Reshape back to categorical array of shape (256, 256, H)
    categorical_cube = block_ids.reshape(spatial_cube.shape[:3])
    
    print(f"Final discrete categorical voxel grid shape (X, Z, Y): {categorical_cube.shape}")
    return categorical_cube

if __name__ == "__main__":
    # Mock usage:
    # mock_spatial_cube = np.random.randint(0, 255, (256, 256, 90, 3), dtype=np.uint8)
    # mock_lut = ["114|84|56", "255|0|0", "46|46|46", "125|125|125"]
    # map_rgb_to_blocks(mock_spatial_cube, mock_lut)
    pass
