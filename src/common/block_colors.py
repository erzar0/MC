"""Shared block-ID <-> RGB color mapping utilities.

Single source of truth for the palette used by both SANA-Video training
(`src/scripts/sana_video/dataset.py`) and inference decoding
(`src/scripts/sana_video/inference.py`).

Colors come from the block2vec embedding RGB LUT (``block_embeddings_rgb.npy``,
produced by ``src/scripts/embeddings/visualize.py`` from ``block_embeddings.npy``),
so semantically similar blocks get similar colors. On top of the LUT:
  - every air variant is forced to pure black (0, 0, 0), and no other block may
    come near black, so black always decodes to air;
  - colors are quantized to a coarse RGB grid (``_COLOR_GRID_STEP``) so distinct
    palette colors are guaranteed at least one grid step apart — wide enough to
    survive the Wan VAE's reconstruction error. Many block states intentionally
    share one quantized color; decoding returns the *first* state (in
    block_states.txt order) that uses it.
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

# RGB quantization grid. Distinct palette colors are >= one step apart in every
# channel they differ in, so the KD-snap tolerates per-channel errors up to half
# a step — comfortably above the Wan VAE round-trip error (~5.5 mean / 11.7 p90).
_COLOR_GRID_STEP = 16

# Bump to invalidate cached palettes when the assignment algorithm changes.
_PALETTE_VERSION = 3


def load_block_states(block_states_path: Optional[str] = None) -> list:
    """Loads the global block-state list (line index == global block ID)."""
    path = block_states_path or DEFAULT_BLOCK_STATES_PATH
    with open(path, "r") as f:
        return [line.strip() for line in f]


def _quantize_color(base: np.ndarray) -> Tuple[int, int, int]:
    """Snaps a color to the RGB grid, keeping non-air colors away from black.

    Quantized colors whose channel sum is below ``_MIN_L1_FROM_BLACK`` are pushed
    away from black by bumping the largest channel one grid step at a time, so
    the black ball stays reserved for air. The push is deterministic, and states
    sharing a quantized color share the pushed color too.
    """
    step = _COLOR_GRID_STEP
    c = [int(min(max(round(v / step), 0), 255 // step)) * step for v in base]
    while sum(c) < _MIN_L1_FROM_BLACK:
        c[int(np.argmax(c))] += step
    return (c[0], c[1], c[2])


def load_id2rgb(
    block_states_path: Optional[str] = None,
    embeddings_rgb_path: Optional[str] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Builds the global-block-ID -> RGB lookup table from the embedding LUT.

    Each non-air state's block2vec color (from ``block_embeddings_rgb.npy``) is
    quantized to the ``_COLOR_GRID_STEP`` RGB grid; states whose colors fall in
    the same grid cell share the same quantized color. All air variants map to
    pure black (0, 0, 0), which no other state may use or approach, so black
    always decodes to air.

    Args:
        block_states_path: Path to block_states.txt (defaults to assets/).
        embeddings_rgb_path: Path to the block2vec RGB LUT
            (defaults to assets/block_embeddings_rgb.npy).

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

    # Cache the palette on disk keyed by the source files and algorithm version.
    cache_dir = PROJECT_ROOT / "tmp"
    src_files = [Path(block_states_path or DEFAULT_BLOCK_STATES_PATH), lut_path]
    cache_key = f"v{_PALETTE_VERSION}-" + "-".join(f"{p.stat().st_size}.{int(p.stat().st_mtime)}" for p in src_files)
    cache_path = cache_dir / f"id2rgb_cache_{hashlib.md5(cache_key.encode()).hexdigest()[:16]}.npz"
    if cache_path.exists():
        cached = np.load(cache_path)
        return cached["id2rgb"], cached["air_ids"]

    id2rgb = np.zeros((len(states), 3), dtype=np.uint8)
    air_ids = []
    for idx, state in enumerate(states):
        base_name = state.split("[")[0]
        if base_name.endswith(("air", "void_air", "cave_air")):
            air_ids.append(idx)  # stays (0, 0, 0)
            continue
        id2rgb[idx] = _quantize_color(lut[idx])

    air_ids_arr = np.asarray(air_ids, dtype=np.int64)
    cache_dir.mkdir(parents=True, exist_ok=True)
    np.savez(cache_path, id2rgb=id2rgb, air_ids=air_ids_arr)
    return id2rgb, air_ids_arr


def load_snap_palette(
    block_states_path: Optional[str] = None,
    embeddings_rgb_path: Optional[str] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Builds the palette for snapping generated RGB back to block IDs.

    Quantized colors are shared by multiple states, so each unique color is
    listed once and owned by the *first* state (lowest global block ID, i.e.
    first line in block_states.txt) that uses it — generated voxels decode to
    that canonical state. Air (global ID 0) is the single entry at pure black,
    so black (and near-black) pixels always translate to air.

    Args:
        block_states_path: Path to block_states.txt (defaults to assets/).
        embeddings_rgb_path: Path to the block2vec RGB LUT
            (defaults to assets/block_embeddings_rgb.npy).

    Returns:
        Tuple of (rgb (K, 3) uint8 array, global block IDs (K,) int64 array),
        aligned by row.
    """
    id2rgb, air_ids = load_id2rgb(block_states_path, embeddings_rgb_path)

    air_id = 0  # universal_minecraft:air is always line 0 of block_states.txt
    color_to_first_id = {(0, 0, 0): air_id}
    air_id_set = set(air_ids.tolist())
    for idx, color in enumerate(id2rgb):
        if idx in air_id_set:
            continue
        color_to_first_id.setdefault(tuple(int(c) for c in color), idx)

    ids_arr = np.array(sorted(color_to_first_id.values()), dtype=np.int64)
    return id2rgb[ids_arr], ids_arr
