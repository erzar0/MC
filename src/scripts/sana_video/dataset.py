"""Dataset for SANA-Video fine-tuning on Minecraft voxel volumes.

Loads `.b2frame` block-ID volumes listed in a JSONL manifest (built by
`prepare_dataset.py`), maps block IDs to RGB colors and exposes each region as a
pseudo-video tensor of shape (C, F, H, W) in [-1, 1]: Y-layers become frames,
the X/Z plane becomes the spatial dimensions.

The `.b2frame` volumes are already trimmed to their content height, so every
region has a *different* number of Y-layers. Rather than pad them all up to one
global length (mostly air), we **bucket** by height: each region's frame count
is snapped up to the nearest valid Wan-VAE length (`4n+1`), and `BucketBatchSampler`
groups equal-length regions into the same batch. This trains only on real blocks
and roughly halves the frames processed vs padding everything to 385.

The frame/spatial layout matches `inference.py`, which transposes generated
video (F, H, W, 3) back to a (X, Z, Y, 3) spatial grid.
"""

import json
import math
import random
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

import blosc2
import numpy as np
import torch
from torch.utils.data import Dataset, Sampler

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.common.block_colors import load_id2rgb


def snap_to_bucket(height: int, step: int, cap: int) -> int:
    """Snaps a content height up to the nearest valid Wan-VAE frame count.

    Buckets have the form ``1 + step * k`` (so ``(bucket - 1) % 4 == 0`` as long
    as ``step`` is a multiple of 4). The result is clamped to ``[5, cap]``.
    """
    bucket = 1 + step * math.ceil(max(height - 1, 0) / step)
    return int(min(max(bucket, 5), cap))


class MinecraftVideoDataset(Dataset):
    """Pseudo-video dataset over content-trimmed `.b2frame` region volumes.

    Each item returns:
        frames: float32 tensor (3, F, crop, crop) in [-1, 1], where F is the
            region's height snapped up to a bucket (use BucketBatchSampler so a
            batch shares one F).
        prompt: caption string (random orientation among the available ones)

    Args:
        manifest_path: JSONL manifest with `volume_path`, `captions` and
            `height` fields (height = the region's content layer count).
        spatial_crop_size: Side length of the random spatial crop (<= 512).
        max_frames: Cap on frames per sample (largest bucket). Must be 4n+1.
        bucket_step: Height quantization granularity (multiple of 4). Smaller =
            less air padding but more distinct buckets. 4 pads ~1.5 layers on
            average; 32 pads ~15.
    """

    def __init__(
        self,
        manifest_path: str,
        spatial_crop_size: int = 128,
        max_frames: int = 385,
        bucket_step: int = 4,
        block_states_path: Optional[str] = None,
        block_state2rgb_path: Optional[str] = None,
    ):
        if (max_frames - 1) % 4 != 0:
            raise ValueError(f"max_frames must be 4n+1 for the Wan VAE temporal compression, got {max_frames}")
        if bucket_step % 4 != 0:
            raise ValueError(f"bucket_step must be a multiple of 4 to keep buckets 4n+1, got {bucket_step}")
        self.spatial_crop_size = spatial_crop_size
        self.max_frames = max_frames
        self.bucket_step = bucket_step

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
        if "height" not in self.entries[0]:
            raise ValueError(
                f"Manifest {manifest_path} entries lack a 'height' field required for bucketing. "
                "Rebuild it with the updated prepare_dataset.py / package_dataset.py."
            )

        # Precompute each sample's bucket (frame count). BucketBatchSampler reads
        # the same list, so getitem and the sampler always agree on batch shape.
        self.frame_counts = [snap_to_bucket(int(e["height"]), bucket_step, max_frames) for e in self.entries]

        self.id2rgb, self.air_ids = load_id2rgb(block_states_path, block_state2rgb_path)
        self.air_id = int(self.air_ids[0]) if len(self.air_ids) > 0 else 0

    def __len__(self) -> int:
        return len(self.entries)

    def _fit_to_frames(self, volume_yxz: np.ndarray, target: int) -> np.ndarray:
        """Forces the (Y, X, Z) volume to exactly ``target`` layers.

        Volumes are content-trimmed (top layer is the surface), so if the volume
        is taller than the bucket we crop the deepest underground layers, and if
        it is shorter we pad air *below*, keeping the surface at the top.
        """
        num_layers = volume_yxz.shape[0]
        if num_layers >= target:
            return volume_yxz[num_layers - target :]
        pad = target - num_layers
        padding = np.full((pad, *volume_yxz.shape[1:]), self.air_id, dtype=volume_yxz.dtype)
        return np.concatenate([padding, volume_yxz], axis=0)

    def __getitem__(self, idx: int):
        entry = self.entries[idx]

        volume_path = Path(entry["volume_path"])
        if not volume_path.is_absolute():
            volume_path = self._manifest_dir / volume_path

        with open(volume_path, "rb") as f:
            volume = blosc2.unpack_array2(f.read())  # (X, Z, Y) uint16

        # Y-layers become frames: (X, Z, Y) -> (Y, X, Z)
        volume_yxz = np.ascontiguousarray(np.transpose(volume, (2, 0, 1)))
        window = self._fit_to_frames(volume_yxz, self.frame_counts[idx])  # (F, X, Z)

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


class BucketBatchSampler(Sampler):
    """Yields batches whose samples all share one frame-count bucket.

    Grouping equal-length regions keeps every batch a clean fixed-shape tensor
    (no cross-size padding) while letting different batches use different frame
    counts. Buckets and batch order are reshuffled every epoch.

    Args:
        frame_counts: Per-sample bucket size (``dataset.frame_counts``).
        batch_size: Samples per batch.
        shuffle: Reshuffle within buckets and across batches each epoch.
        drop_last: Drop the trailing partial batch of each bucket.
        seed: Base RNG seed.
    """

    def __init__(self, frame_counts, batch_size: int, shuffle: bool = True, drop_last: bool = False, seed: int = 0):
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.drop_last = drop_last
        self._rng = random.Random(seed)

        self._buckets = defaultdict(list)
        for idx, fc in enumerate(frame_counts):
            self._buckets[fc].append(idx)

        self._num_batches = 0
        for idxs in self._buckets.values():
            n = len(idxs)
            self._num_batches += n // batch_size if drop_last else (n + batch_size - 1) // batch_size

    def __len__(self) -> int:
        return self._num_batches

    def __iter__(self):
        batches = []
        for idxs in self._buckets.values():
            order = idxs[:]
            if self.shuffle:
                self._rng.shuffle(order)
            for i in range(0, len(order), self.batch_size):
                batch = order[i : i + self.batch_size]
                if self.drop_last and len(batch) < self.batch_size:
                    continue
                batches.append(batch)
        if self.shuffle:
            self._rng.shuffle(batches)
        return iter(batches)
