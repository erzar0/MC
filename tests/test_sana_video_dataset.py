"""Tests for the SANA-Video dataset: height bucketing, batch sampler, windowing.

Uses tiny synthesized `.b2frame` volumes (real block-colour assets) so the tests
are fast and don't need the full pipeline output.
"""

import json
from collections import Counter

import blosc2
import numpy as np
import pytest
import torch
from torch.utils.data import DataLoader

from src.scripts.sana_video.dataset import (
    BucketBatchSampler,
    MinecraftVideoDataset,
    snap_to_bucket,
)

AIR_ID = 0  # index 0 == universal_minecraft:air
SOLID_ID = 2  # bedrock — a non-air block


def _make_volume(path, height, block=SOLID_ID, xz=8):
    """Writes a (X, Z, Y) volume of the given content height as a .b2frame."""
    arr = np.full((xz, xz, height), block, dtype=np.uint16)
    path.write_bytes(blosc2.pack_array2(arr))


def _make_dataset(tmp_path, heights, captions=None, drop_height=False, **kwargs):
    """Builds a manifest + volumes under tmp_path and returns a dataset."""
    vol_dir = tmp_path / "volumes"
    vol_dir.mkdir(exist_ok=True)
    manifest = tmp_path / "manifest.jsonl"
    caps = captions or {"ne": "a north-east view", "sw": "a south-west view"}
    with open(manifest, "w") as f:
        for i, h in enumerate(heights):
            rel = f"volumes/r.{i}.0.b2frame"
            _make_volume(tmp_path / rel, h)
            entry = {"volume_path": rel, "captions": caps}
            if not drop_height:
                entry["height"] = h
            f.write(json.dumps(entry) + "\n")
    return MinecraftVideoDataset(str(manifest), spatial_crop_size=4, **kwargs)


# --------------------------------------------------------------------------- #
# snap_to_bucket
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("height", [1, 5, 6, 10, 14, 128, 162, 384, 500])
def test_snap_bucket_is_valid_vae_length(height):
    b = snap_to_bucket(height, step=4, cap=385)
    assert (b - 1) % 4 == 0, "bucket must be 4n+1 for the Wan VAE"


@pytest.mark.parametrize("height", [6, 10, 14, 100, 200])
def test_snap_bucket_covers_height(height):
    # Snapping up must never lose content (bucket >= height), within the cap.
    assert snap_to_bucket(height, step=4, cap=385) >= height


def test_snap_bucket_respects_cap():
    assert snap_to_bucket(1000, step=4, cap=385) == 385


def test_snap_bucket_has_floor():
    assert snap_to_bucket(1, step=4, cap=385) == 5
    assert snap_to_bucket(2, step=4, cap=385) == 5


def test_snap_bucket_coarse_step_still_valid():
    b = snap_to_bucket(100, step=32, cap=385)
    assert b == 129  # 1 + 32*ceil(99/32) = 1 + 32*4
    assert (b - 1) % 4 == 0


# --------------------------------------------------------------------------- #
# BucketBatchSampler
# --------------------------------------------------------------------------- #


def test_sampler_batches_share_one_bucket():
    frame_counts = [5, 5, 9, 9, 9, 13]
    sampler = BucketBatchSampler(frame_counts, batch_size=2, seed=0)
    for batch in sampler:
        buckets = {frame_counts[i] for i in batch}
        assert len(buckets) == 1, f"batch mixes buckets: {buckets}"


def test_sampler_covers_all_indices_once():
    frame_counts = [5, 5, 9, 9, 9, 13]
    sampler = BucketBatchSampler(frame_counts, batch_size=2, seed=1)
    seen = [i for batch in sampler for i in batch]
    assert sorted(seen) == list(range(len(frame_counts)))


def test_sampler_len_matches_yielded_batches():
    frame_counts = [5, 5, 9, 9, 9, 13]
    sampler = BucketBatchSampler(frame_counts, batch_size=2, seed=2)
    assert len(sampler) == len(list(sampler))


def test_sampler_drop_last_drops_partial_batches():
    # buckets: 5->2 (1 batch), 9->3 (1 full + drop 1), 13->1 (dropped)
    frame_counts = [5, 5, 9, 9, 9, 13]
    sampler = BucketBatchSampler(frame_counts, batch_size=2, drop_last=True, seed=0)
    batches = list(sampler)
    assert all(len(b) == 2 for b in batches)
    assert len(batches) == 2


def test_sampler_reshuffles_but_preserves_content():
    frame_counts = [5, 5, 5, 5, 9, 9, 9, 9]
    sampler = BucketBatchSampler(frame_counts, batch_size=2, shuffle=True, seed=0)
    epoch1 = [tuple(b) for b in sampler]
    epoch2 = [tuple(b) for b in sampler]
    # Same set of indices both epochs...
    assert sorted(i for b in epoch1 for i in b) == sorted(i for b in epoch2 for i in b)
    # ...but the order differs across epochs (shuffled).
    assert epoch1 != epoch2


# --------------------------------------------------------------------------- #
# MinecraftVideoDataset
# --------------------------------------------------------------------------- #


def test_dataset_frame_counts_are_bucketed(tmp_path):
    ds = _make_dataset(tmp_path, heights=[10, 20, 33], bucket_step=4)
    assert ds.frame_counts == [13, 21, 33]


def test_dataset_item_shape_and_range(tmp_path):
    ds = _make_dataset(tmp_path, heights=[10], bucket_step=4)
    frames, prompt = ds[0]
    assert frames.shape == (3, 13, 4, 4)  # (C, bucket, crop, crop)
    assert frames.dtype == torch.float32
    assert frames.min() >= -1.0 and frames.max() <= 1.0
    assert isinstance(prompt, str) and prompt


def test_dataset_requires_height_field(tmp_path):
    with pytest.raises(ValueError, match="height"):
        _make_dataset(tmp_path, heights=[10], drop_height=True)


def test_fit_to_frames_pads_air_below(tmp_path):
    ds = _make_dataset(tmp_path, heights=[10], bucket_step=4)
    vol = np.full((10, 4, 4), SOLID_ID, dtype=np.uint16)  # (Y, X, Z), all content
    out = ds._fit_to_frames(vol, target=13)
    assert out.shape == (13, 4, 4)
    # 3 air layers padded below (indices 0..2), content stays at the top.
    assert np.all(out[:3] == AIR_ID)
    assert np.all(out[3:] == SOLID_ID)


def test_fit_to_frames_crops_from_bottom(tmp_path):
    ds = _make_dataset(tmp_path, heights=[10], bucket_step=4)
    vol = np.arange(20, dtype=np.uint16).reshape(20, 1, 1)  # distinct per layer
    out = ds._fit_to_frames(vol, target=13)
    assert out.shape == (13, 1, 1)
    # Keeps the top 13 layers (surface), drops the deepest 7.
    assert np.array_equal(out[:, 0, 0], np.arange(7, 20))


def test_dataloader_batches_are_uniform(tmp_path):
    heights = [10, 10, 20, 20, 33, 33]
    ds = _make_dataset(tmp_path, heights=heights, bucket_step=4)
    sampler = BucketBatchSampler(ds.frame_counts, batch_size=2, seed=0)
    loader = DataLoader(ds, batch_sampler=sampler, num_workers=0)
    seen_buckets = Counter()
    for frames, _prompts in loader:
        # If a batch mixed frame counts, default collate would have raised.
        assert frames.shape[0] == 2
        seen_buckets[frames.shape[2]] += 1
    assert set(seen_buckets) == {13, 21, 33}


if __name__ == "__main__":
    pytest.main([__file__])
