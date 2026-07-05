"""Shared block-ID <-> RGB color mapping utilities.

Single source of truth for the palette used by both SANA-Video training
(`src/scripts/sana_video_dataset.py`) and inference decoding
(`src/scripts/inference_sana_video.py`).
"""

import csv
from pathlib import Path
from typing import Optional, Tuple

import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_BLOCK_STATES_PATH = PROJECT_ROOT / "assets" / "block_states.txt"
DEFAULT_BLOCK_STATE2RGB_PATH = PROJECT_ROOT / "assets" / "block_state2rgb.csv"


def load_block_states(block_states_path: Optional[str] = None) -> list:
    """Loads the global block-state list (line index == global block ID)."""
    path = block_states_path or DEFAULT_BLOCK_STATES_PATH
    with open(path, "r") as f:
        return [line.strip() for line in f]


def load_id2rgb(
    block_states_path: Optional[str] = None,
    block_state2rgb_path: Optional[str] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Builds the global-block-ID -> RGB lookup table.

    Args:
        block_states_path: Path to block_states.txt (defaults to assets/).
        block_state2rgb_path: Path to block_state2rgb.csv (defaults to assets/).

    Returns:
        Tuple of (id2rgb (vocab_size, 3) uint8 array, air block ID int64 array).
        Blocks without an RGB entry map to (0, 0, 0).
    """
    states = load_block_states(block_states_path)

    state2rgb = {}
    with open(block_state2rgb_path or DEFAULT_BLOCK_STATE2RGB_PATH, "r") as f:
        reader = csv.reader(f)
        next(reader)  # header
        for row in reader:
            if len(row) >= 2:
                state2rgb[row[0]] = [int(val) for val in row[1].split("|")]

    id2rgb = np.zeros((len(states), 3), dtype=np.uint8)
    air_ids = []
    for idx, state in enumerate(states):
        if state in state2rgb:
            id2rgb[idx] = state2rgb[state]
        base_name = state.split("[")[0]
        if base_name.endswith(("air", "void_air", "cave_air")):
            air_ids.append(idx)

    return id2rgb, np.asarray(air_ids, dtype=np.int64)
