import numpy as np
import os
import cv2
import json

def prepare_data(rgb_cube: np.ndarray, prompt: str, output_dir: str, name: str):
    """
    Stage 1: Converts a Minecraft 3D rgb_cube of shape (X, Z, Y, 3) 
    into a Video tensor of shape (Time, Height, Width, Channels)
    where Time = Y, Height = X, Width = Z.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Transpose to (Y, X, Z, 3) where Y is Time/Frames
    video_frames = np.transpose(rgb_cube, (2, 0, 1, 3))
    
    # Save as numpy array for training
    npy_path = os.path.join(output_dir, f"{name}.npy")
    np.save(npy_path, video_frames)
    
    # Save as MP4 for visualization
    video_path = os.path.join(output_dir, f"{name}.mp4")
    out = cv2.VideoWriter(video_path, cv2.VideoWriter_fourcc(*'mp4v'), 1, (video_frames.shape[2], video_frames.shape[1]))
    for frame in video_frames:
        out.write(cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
    out.release()
    print(f"Saved visualization MP4 (1 FPS) to {video_path}")
    
    # Save text conditioning prompt
    metadata = {"prompt": prompt, "frames": video_frames.shape[0]}
    with open(os.path.join(output_dir, f"{name}.json"), "w") as f:
        json.dump(metadata, f)
        
    print(f"Saved {name} with {video_frames.shape[0]} frames.")

if __name__ == "__main__":
    # Mock usage:
    # mock_rgb_cube = np.random.randint(0, 255, (256, 256, 80, 3), dtype=np.uint8)
    # prepare_castle_data(mock_rgb_cube, "A stone castle.", "./data", "castle_001")
    pass
