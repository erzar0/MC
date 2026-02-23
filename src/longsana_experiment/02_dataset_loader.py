import os
import json
import numpy as np
import torch
from torch.utils.data import Dataset

class MinecraftVideoDataset(Dataset):
    """
    Stage 2: PyTorch Dataset for loading the NPY arrays and text captions.
    Adapted for the LongSANA training script.
    """
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.files = [f for f in os.listdir(data_dir) if f.endswith(".npy")]

    def __len__(self):
        return len(self.files)

    def __getitem__(self, idx):
        name = self.files[idx].replace(".npy", "")
        
        # Load Video (T, H, W, 3) -> Convert to PyTorch format (C, T, H, W)
        frames = np.load(os.path.join(self.data_dir, f"{name}.npy"))
        
        # Normalize to 0.0 - 1.0 (or -1.0 to 1.0 depending on VAE bounds)
        frames_tensor = torch.tensor(frames, dtype=torch.float32).permute(3, 0, 1, 2) / 255.0
        
        # Load text prompt
        with open(os.path.join(self.data_dir, f"{name}.json"), "r") as f:
            prompt = json.load(f)["prompt"]
            
        return frames_tensor, prompt

if __name__ == "__main__":
    # Mock usage:
    # dataset = MinecraftVideoDataset("./data")
    # frames, prompt = dataset[0]
    # print(frames.shape, prompt)
    pass
