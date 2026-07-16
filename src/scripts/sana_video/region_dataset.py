"""Minecraft region dataset with the upstream Sana video dataset interface.

Wraps :class:`MinecraftVideoDataset` (manifest loading, .b2frame decode, RGB
mapping, rotation/crop augmentation) into the 8-tuple contract that
``Sana/train_video_scripts/train_video_ivjoint.py`` expects from
``diffusion/data/datasets/video/sana_video_data.py::getdata``:

    (vframes (F, C, H, W) float [-1, 1], caption str,
     attention_mask (1, 1, max_length) int16, data_info dict, idx,
     caption_type str, {"height", "width"}, 0.0)

Every sample uses one fixed frame count (``num_frames``, 4n+1 for the Wan
VAE) — taller regions are top-cropped, shorter ones air-padded above — so the
upstream plain (non-multi-scale) sampler path works without height bucketing.
"""

import sys
from pathlib import Path

import torch

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src.scripts.sana_video.dataset import MinecraftVideoDataset
except ImportError:
    from dataset import MinecraftVideoDataset


class MinecraftRegionVideoDataset(MinecraftVideoDataset):
    """Fixed-frame-count region dataset returning the upstream 8-tuple.

    Args:
        manifest_path: JSONL manifest (see MinecraftVideoDataset).
        num_frames: Fixed frame count for every sample (must be 4n+1).
        image_size: Side length of the random spatial crop.
        max_length: Text token length for the attention-mask placeholder
            (300 for gemma in the upstream config).
    """

    def __init__(
        self,
        manifest_path: str,
        num_frames: int = 129,
        image_size: int = 256,
        max_length: int = 300,
        **kwargs,
    ):
        super().__init__(
            manifest_path,
            spatial_crop_size=image_size,
            max_frames=num_frames,
            bucket_step=4,
            **kwargs,
        )
        self.num_frames = num_frames
        self.max_length = max_length
        # Force every sample to the same frame count (no bucketing).
        self.frame_counts = [num_frames] * len(self.entries)

    def __getitem__(self, idx: int):
        frames, prompt = super().__getitem__(idx)  # (C, F, H, W), str
        vframes = frames.permute(1, 0, 2, 3).contiguous()  # (F, C, H, W)
        h, w = float(vframes.shape[-2]), float(vframes.shape[-1])

        data_info = {
            "cache_key": f"region_{idx}",
            "key": str(Path(self.entries[idx]["volume_path"]).stem),
            "dataset_name": "minecraft_regions",
            "img_hw": torch.tensor([h, w], dtype=torch.float32),
            "aspect_ratio": h / w,
        }
        attention_mask = torch.ones(1, 1, self.max_length, dtype=torch.int16)
        return vframes, prompt, attention_mask, data_info, idx, "caption", {"height": h, "width": w}, 0.0
