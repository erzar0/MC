"""Dataset for SANA-Video fine-tuning on Minecraft voxel volumes.

Loads `.b2frame` block-ID volumes listed in a JSONL manifest (built by
`prepare_sana_video_dataset.py`), maps block IDs to RGB colors and exposes
each region as a pseudo-video tensor of shape (C, F, H, W) in [-1, 1]:
Y-layers become frames, the X/Z plane becomes the spatial dimensions.

The frame/spatial layout matches `inference_sana_video.py`, which transposes
generated video (F, H, W, 3) back to a (X, Z, Y, 3) spatial grid.
"""

import json
import random
import sys
from pathlib import Path
from typing import Optional

import blosc2
import numpy as np
import torch
from torch.utils.data import Dataset

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.common.block_colors import load_id2rgb


class MinecraftVideoDataset(Dataset):
    """Pseudo-video dataset over `.b2frame` region volumes.

    Each item returns:
        frames: float32 tensor (3, max_frames, crop, crop) in [-1, 1]
        prompt: caption string (random orientation among the available ones)

    Args:
        manifest_path: JSONL manifest with `volume_path` and `captions` fields.
        spatial_crop_size: Side length of the random spatial crop (<= 512).
        max_frames: Number of Y-layers per sample. Must satisfy
            (max_frames - 1) % 4 == 0 for the Wan VAE temporal compression.
        surface_margin: Extra layers kept above the detected surface (0 crops
            the empty sky right at the highest content layer).
        content_threshold: Minimum non-air fraction for a layer to count as
            "surface" when locating the frame window.
    """

    def __init__(
        self,
        manifest_path: str,
        spatial_crop_size: int = 512,
        max_frames: int = 65,
        surface_margin: int = 0,
        content_threshold: float = 0.01,
        block_states_path: Optional[str] = None,
        block_state2rgb_path: Optional[str] = None,
    ):
        if (max_frames - 1) % 4 != 0:
            raise ValueError(f"max_frames must be 4n+1 for the Wan VAE temporal compression, got {max_frames}")
        self.spatial_crop_size = spatial_crop_size
        self.max_frames = max_frames
        self.surface_margin = surface_margin
        self.content_threshold = content_threshold

        # Relative volume_path entries are resolved against the manifest's own
        # directory, so a packaged dataset bundle (relative manifest + volumes)
        # is portable to any machine. Absolute paths are still honored as-is.
        self._manifest_dir = Path(manifest_path).resolve().parent

        self.entries = []
        with open(manifest_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    self.entries.append(json.loads(line))
        if not self.entries:
            raise ValueError(f"Manifest {manifest_path} is empty")

        self.id2rgb, self.air_ids = load_id2rgb(block_states_path, block_state2rgb_path)
        # Boolean mask over the vocab: True where the block is a kind of air
        self.is_air = np.zeros(len(self.id2rgb), dtype=bool)
        self.is_air[self.air_ids] = True

    def __len__(self) -> int:
        return len(self.entries)

    def _select_frame_window(self, volume_yxz: np.ndarray) -> np.ndarray:
        """Picks a max_frames-layer window whose top is the highest content layer.

        Empty sky above the topmost non-air layer is cropped so no frames are
        wasted on blank air. If the window is shorter than ``max_frames`` it is
        padded *below* (underground) with air, keeping the surface at the top of
        the stack. A fully-air region crops to nothing and becomes all air.
        """
        num_layers = volume_yxz.shape[0]
        non_air = ~self.is_air[volume_yxz]  # (Y, X, Z) bool
        layer_content = non_air.mean(axis=(1, 2))  # non-air fraction per Y layer

        content_layers = np.nonzero(layer_content >= self.content_threshold)[0]
        if len(content_layers) > 0:
            surface = int(content_layers[-1])
            end = min(num_layers, surface + 1 + self.surface_margin)
        else:
            # Nothing but air: crop it all away (the window fills with air below).
            end = 0

        start = max(0, end - self.max_frames)
        window = volume_yxz[start:end]

        if window.shape[0] < self.max_frames:
            # Pad below (underground) with air so the surface stays at the top of
            # the stack and no blank sky is reintroduced above it.
            pad = self.max_frames - window.shape[0]
            air_id = self.air_ids[0] if len(self.air_ids) > 0 else 0
            padding = np.full((pad, *window.shape[1:]), air_id, dtype=window.dtype)
            window = np.concatenate([padding, window], axis=0)
        return window

    def __getitem__(self, idx: int):
        entry = self.entries[idx]

        volume_path = Path(entry["volume_path"])
        if not volume_path.is_absolute():
            volume_path = self._manifest_dir / volume_path

        with open(volume_path, "rb") as f:
            volume = blosc2.unpack_array2(f.read())  # (X, Z, Y) uint16

        # Y-layers become frames: (X, Z, Y) -> (Y, X, Z)
        volume_yxz = np.ascontiguousarray(np.transpose(volume, (2, 0, 1)))
        window = self._select_frame_window(volume_yxz)  # (F, X, Z)

        # Random 90-degree rotation about the vertical (Y) axis, i.e. in the
        # X/Z spatial plane. Combined with the random caption orientation below
        # this yields 4 rotations x 4 orientations = 16 augmentation variants.
        k = random.randint(0, 3)
        if k:
            window = np.rot90(window, k=k, axes=(1, 2))

        # Random spatial crop
        crop = self.spatial_crop_size
        _, h, w = window.shape
        if h > crop or w > crop:
            y0 = random.randint(0, max(0, h - crop))
            x0 = random.randint(0, max(0, w - crop))
            window = window[:, y0 : y0 + crop, x0 : x0 + crop]

        # Block IDs -> RGB pseudo-video, normalized to [-1, 1] for the VAE
        rgb = self.id2rgb[window]  # (F, H, W, 3) uint8
        frames = torch.from_numpy(rgb).float().permute(3, 0, 1, 2) / 127.5 - 1.0

        # Random caption orientation
        captions = entry["captions"]
        prompt = captions[random.choice(list(captions.keys()))]
        return frames, prompt
