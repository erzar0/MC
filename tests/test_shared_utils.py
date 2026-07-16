"""Tests for the shared utility modules extracted during the refactor."""

import json
from pathlib import Path

import numpy as np
import pytest

from src.common.block_colors import load_block_states, load_id2rgb
from src.common.llm_utils import strip_thinking_tags
from src.common.resumable_state import JsonStateStore

PROJECT_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# block_colors
# ---------------------------------------------------------------------------


def test_load_id2rgb_shape_and_air_ids():
    id2rgb, air_ids = load_id2rgb()
    states = load_block_states()
    assert id2rgb.shape == (len(states), 3)
    assert id2rgb.dtype == np.uint8
    # air, cave_air and void_air must all be detected
    air_names = {states[i].split("[")[0] for i in air_ids}
    assert air_names == {
        "universal_minecraft:air",
        "universal_minecraft:cave_air",
        "universal_minecraft:void_air",
    }
    # Plain air (id 0) maps to black
    assert 0 in air_ids
    assert (id2rgb[0] == 0).all()


def test_palette_quantization_and_first_seen_ownership():
    from src.common.block_colors import _COLOR_GRID_STEP, _MIN_L1_FROM_BLACK, load_snap_palette

    id2rgb, air_ids = load_id2rgb()
    non_air = np.ones(len(id2rgb), dtype=bool)
    non_air[air_ids] = False

    # All non-air colors sit on the quantization grid and away from black
    colors = id2rgb[non_air].astype(np.int64)
    assert (colors % _COLOR_GRID_STEP == 0).all()
    assert (colors.sum(axis=1) >= _MIN_L1_FROM_BLACK).all()

    # Snap palette: one entry per unique color, owned by the first (lowest) ID
    palette_rgb, palette_ids = load_snap_palette()
    seen = {tuple(c) for c in palette_rgb}
    assert len(seen) == len(palette_rgb)  # colors are unique
    assert palette_ids[0] == 0 and (palette_rgb[0] == 0).all()  # black -> air
    color_to_owner = {tuple(c): i for c, i in zip(palette_rgb, palette_ids, strict=True)}
    for idx in np.where(non_air)[0][:2000]:
        owner = color_to_owner[tuple(id2rgb[idx])]
        assert owner <= idx  # owner is the first state with this color


# ---------------------------------------------------------------------------
# llm_utils
# ---------------------------------------------------------------------------


def test_strip_thinking_tags():
    assert strip_thinking_tags("<think>reasoning</think>answer") == "answer"
    assert strip_thinking_tags("  plain text  ") == "plain text"
    # Unclosed tag is left as-is (minus surrounding whitespace)
    assert strip_thinking_tags("<think>oops") == "<think>oops"


# ---------------------------------------------------------------------------
# resumable_state
# ---------------------------------------------------------------------------


class _DownloadLikeState(JsonStateStore):
    DEFAULT_ENTRY = {"status": "pending", "attempted_urls": [], "error": None, "file": None}


def test_json_state_store_roundtrip(tmp_path):
    path = tmp_path / "state.json"
    store = _DownloadLikeState(path)
    entry = store._ensure("123")
    entry["status"] = "done"
    entry["file"] = "tmp/downloads/123/map.zip"
    store.save()

    reloaded = _DownloadLikeState(path)
    assert reloaded.is_done("123")
    assert not reloaded.is_failed("123")
    assert reloaded.get("123")["file"] == "tmp/downloads/123/map.zip"


def test_json_state_store_default_entry_not_shared(tmp_path):
    store = _DownloadLikeState(tmp_path / "state.json")
    store._ensure("a")["attempted_urls"].append("http://x")
    assert store._ensure("b")["attempted_urls"] == []
    assert _DownloadLikeState.DEFAULT_ENTRY["attempted_urls"] == []


def test_json_state_store_corrupt_file_starts_fresh(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json")
    store = _DownloadLikeState(path)
    assert store.get("anything") == {}


@pytest.mark.parametrize(
    "state_file,required_keys",
    [
        ("map_download_state.json", {"status", "attempted_urls", "error", "file"}),
        ("process_state.json", {"status", "error", "output_dir"}),
    ],
)
def test_golden_state_schemas(state_file, required_keys):
    """The real data/pipeline/ state files must match the schemas the classes write."""
    path = PROJECT_ROOT / "data" / "pipeline" / state_file
    if not path.exists():
        pytest.skip(f"{state_file} not present")
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    assert isinstance(data, dict)
    # Sample a few entries; they must contain at least the schema keys
    for _, entry in list(data.items())[:20]:
        assert required_keys <= set(entry.keys())


def test_download_state_matches_real_schema(tmp_path):
    """DownloadState (via JsonStateStore) writes entries shaped like the real file."""
    from src.scripts.downloading.map_downloader import DownloadState

    path = tmp_path / "map_download_state.json"
    state = DownloadState(path)
    state.mark_downloading("42", "http://example.com/a.zip")
    state.mark_failed("42", "boom")
    state.save()

    written = json.loads(path.read_text())
    assert set(written["42"].keys()) == {"status", "attempted_urls", "error", "file"}
    assert written["42"]["status"] == "failed"
    assert written["42"]["attempted_urls"] == ["http://example.com/a.zip"]


def test_process_state_matches_real_schema(tmp_path):
    from src.scripts.processing.process_worlds import ProcessState

    path = tmp_path / "process_state.json"
    state = ProcessState(path)
    state.mark_done("7", "tmp/processed_worlds/cleansed/7")
    state.save()

    written = json.loads(path.read_text())
    assert set(written["7"].keys()) == {"status", "error", "output_dir"}
    assert written["7"]["status"] == "done"


if __name__ == "__main__":
    pytest.main([__file__])
