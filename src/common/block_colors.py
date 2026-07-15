"""Shared block-ID <-> RGB color mapping utilities.

Single source of truth for the palette used by both SANA-Video training
(`src/scripts/sana_video/dataset.py`) and inference decoding
(`src/scripts/sana_video/inference.py`).

Colors come from the block2vec embedding RGB LUT (``block_embeddings_rgb.npy``,
produced by ``src/scripts/embeddings/visualize.py`` from ``block_embeddings.npy``),
so semantically similar blocks get similar colors. On top of the LUT:
  - every air variant is forced to pure black (0, 0, 0), and no other block may
    come near black, so black always decodes to air;
  - duplicate LUT colors are disambiguated by a deterministic minimal
    perturbation, so every non-air state has a unique color and the inference
    KD-tree snap is unambiguous.
"""

import hashlib
from pathlib import Path
from typing import Optional, Tuple

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BLOCK_STATES_PATH = PROJECT_ROOT / "assets" / "block_states.txt"
DEFAULT_BLOCK_EMBEDDINGS_RGB_PATH = PROJECT_ROOT / "assets" / "block_embeddings_rgb.npy"

# Non-air colors are kept at least this far (L1) from black so noisy dark pixels
# keep snapping to air (any pixel with R+G+B below half this always decodes to air).
_MIN_L1_FROM_BLACK = 48

# Bump to invalidate cached palettes when the assignment algorithm changes.
_PALETTE_VERSION = 2


def load_block_states(block_states_path: Optional[str] = None) -> list:
    """Loads the global block-state list (line index == global block ID)."""
    path = block_states_path or DEFAULT_BLOCK_STATES_PATH
    with open(path, "r") as f:
        return [line.strip() for line in f]


def _nearest_free_color(base: Tuple[int, int, int], used: set) -> Tuple[int, int, int]:
    """Finds the unused color closest to ``base`` (deterministic ring search).

    Colors within ``_MIN_L1_FROM_BLACK`` of black are skipped so near-black stays
    reserved for air.
    """
    r0, g0, b0 = (int(c) for c in base)
    for radius in range(0, 256):
        best = None
        for dr in range(-radius, radius + 1):
            for dg in range(-radius, radius + 1):
                for db in range(-radius, radius + 1):
                    if max(abs(dr), abs(dg), abs(db)) != radius:
                        continue  # only the shell of this radius
                    c = (r0 + dr, g0 + dg, b0 + db)
                    if not all(0 <= v <= 255 for v in c):
                        continue
                    if sum(c) < _MIN_L1_FROM_BLACK:
                        continue
                    if c in used:
                        continue
                    key = (abs(dr) + abs(dg) + abs(db), c)
                    if best is None or key < best[0]:
                        best = (key, c)
        if best is not None:
            return best[1]
    raise RuntimeError("RGB space exhausted — cannot assign a unique color")


def load_id2rgb(
    block_states_path: Optional[str] = None,
    embeddings_rgb_path: Optional[str] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Builds the global-block-ID -> RGB lookup table from the embedding LUT.

    Every non-air state gets a distinct color: the base color is its block2vec
    embedding color from ``block_embeddings_rgb.npy``, with duplicate colors
    resolved by a deterministic nearest-free-color search. All air variants map
    to pure black (0, 0, 0), which no other state may use or approach, so black
    always decodes to air.

    Args:
        block_states_path: Path to block_states.txt (defaults to assets/).
        embeddings_rgb_path: Path to the block2vec RGB LUT
            (defaults to tmp/block_embeddings_rgb.npy).

    Returns:
        Tuple of (id2rgb (vocab_size, 3) uint8 array, air block ID int64 array).

    Raises:
        FileNotFoundError: If the embedding LUT is missing.
        ValueError: If the LUT vocabulary size does not match block_states.txt.
    """
    states = load_block_states(block_states_path)
    lut_path = Path(embeddings_rgb_path or DEFAULT_BLOCK_EMBEDDINGS_RGB_PATH)
    if not lut_path.exists():
        raise FileNotFoundError(
            f"Embedding RGB LUT not found at {lut_path}. Generate it with "
            "src/scripts/embeddings/visualize.py (or copy it to this machine)."
        )
    lut = np.load(lut_path)
    if lut.shape != (len(states), 3):
        raise ValueError(f"LUT shape {lut.shape} does not match {len(states)} block states — regenerate the LUT")

    # The unique-color assignment takes a few seconds; cache it on disk keyed by
    # the source files and algorithm version.
    cache_dir = PROJECT_ROOT / "tmp"
    src_files = [Path(block_states_path or DEFAULT_BLOCK_STATES_PATH), lut_path]
    cache_key = f"v{_PALETTE_VERSION}-" + "-".join(f"{p.stat().st_size}.{int(p.stat().st_mtime)}" for p in src_files)
    cache_path = cache_dir / f"id2rgb_cache_{hashlib.md5(cache_key.encode()).hexdigest()[:16]}.npz"
    if cache_path.exists():
        cached = np.load(cache_path)
        return cached["id2rgb"], cached["air_ids"]

    id2rgb = np.zeros((len(states), 3), dtype=np.uint8)
    air_ids = []
    used: set = {(0, 0, 0)}
    for idx, state in enumerate(states):
        base_name = state.split("[")[0]
        if base_name.endswith(("air", "void_air", "cave_air")):
            air_ids.append(idx)  # stays (0, 0, 0)
            continue
        color = _nearest_free_color(tuple(int(c) for c in lut[idx]), used)
        used.add(color)
        id2rgb[idx] = color

    air_ids_arr = np.asarray(air_ids, dtype=np.int64)
    cache_dir.mkdir(parents=True, exist_ok=True)
    np.savez(cache_path, id2rgb=id2rgb, air_ids=air_ids_arr)
    return id2rgb, air_ids_arr


def load_snap_palette(
    block_states_path: Optional[str] = None,
    embeddings_rgb_path: Optional[str] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Builds the palette for snapping generated RGB back to block IDs.

    Since ``load_id2rgb`` assigns every non-air state a unique color, all states
    are snap targets. Air (global ID 0) is the single entry at pure black — the
    other air variants are aliases of the same color and are omitted — so black
    (and near-black) pixels always translate to air.

    Args:
        block_states_path: Path to block_states.txt (defaults to assets/).
        embeddings_rgb_path: Path to the block2vec RGB LUT
            (defaults to tmp/block_embeddings_rgb.npy).

    Returns:
        Tuple of (rgb (K, 3) uint8 array, global block IDs (K,) int64 array),
        aligned by row.
    """
    id2rgb, air_ids = load_id2rgb(block_states_path, embeddings_rgb_path)

    air_id = 0  # universal_minecraft:air is always line 0 of block_states.txt
    mask = np.ones(len(id2rgb), dtype=bool)
    mask[air_ids] = False
    ids_arr = np.concatenate(([air_id], np.where(mask)[0])).astype(np.int64)
    return id2rgb[ids_arr], ids_arr
