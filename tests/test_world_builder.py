from pathlib import Path

import amulet
import numpy as np
import pytest

from src.common.block_colors import load_block_states, load_id2rgb
from src.world.fast_volume_extractor import FastVolumeParser
from src.world.world_builder import build_world
from src.world.world_wrapper import BlockStates

BASE_Y = -64


@pytest.fixture(scope="module")
def states():
    return load_block_states()


@pytest.fixture(scope="module")
def block_ids(states):
    _, air_ids = load_id2rgb()
    return {
        "stone": states.index("universal_minecraft:stone"),
        "air": int(air_ids[0]),
    }


@pytest.fixture(scope="module")
def built_world(tmp_path_factory, states, block_ids):
    # 32x32x8 grid spanning chunks (0,0)-(1,1): stone with an air gap layer
    grid = np.full((32, 32, 8), block_ids["stone"], dtype=np.uint16)
    grid[:, :, 4] = block_ids["air"]
    grid[10, 20, :] = block_ids["air"]  # air column
    world_dir = tmp_path_factory.mktemp("generated") / "world"
    build_world(grid, world_dir, states=states)
    return world_dir


def test_world_files_exist(built_world):
    assert (built_world / "level.dat").exists()
    assert (built_world / "region" / "r.0.0.mca").exists()


def test_blocks_round_trip(built_world):
    level = amulet.load_level(str(built_world))
    try:

        def java_block(x, y, z):
            block, _ = level.get_version_block(x, y, z, "minecraft:overworld", ("java", (1, 19, 2)))
            return block.namespaced_name

        # Stone everywhere except the air layer (y index 4) and the air column
        assert java_block(0, BASE_Y, 0) == "minecraft:stone"
        assert java_block(31, BASE_Y + 7, 31) == "minecraft:stone"  # chunk (1,1)
        assert java_block(5, BASE_Y + 4, 5) == "minecraft:air"
        assert java_block(10, BASE_Y + 2, 20) == "minecraft:air"
        # Above the grid remains air
        assert java_block(0, BASE_Y + 8, 0) == "minecraft:air"
    finally:
        level.close()


def test_chunk_data_version(built_world):
    parser = FastVolumeParser(built_world / "region" / "r.0.0.mca", BlockStates())
    try:
        nbt = parser.get_chunk_nbt(0, 0)
        assert nbt is not None
        assert nbt.compound["DataVersion"].py_int == 3120
    finally:
        parser.close()


def test_level_dat_patched(built_world):
    import amulet_nbt

    data = amulet_nbt.load(str(built_world / "level.dat")).compound.get_compound("Data")
    assert data["LevelName"].py_str == "Generated Structure"
    assert data["GameType"].py_int == 1
    gen = data.get_compound("WorldGenSettings").get_compound("dimensions").get_compound("minecraft:overworld")
    assert gen.get_compound("generator")["type"].py_str == "minecraft:flat"


def test_rejects_bad_shapes(block_ids):
    with pytest.raises(ValueError):
        build_world(np.zeros((10, 16, 4), dtype=np.uint16), Path("/nonexistent"))
    with pytest.raises(ValueError):
        build_world(np.zeros((16, 16, 400), dtype=np.uint16), Path("/nonexistent"))


if __name__ == "__main__":
    pytest.main([__file__])
