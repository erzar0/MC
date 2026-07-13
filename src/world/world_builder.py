"""Build a playable Minecraft Java world from a voxel block-ID grid.

Takes a ``(X, Z, Y)`` grid of global block IDs (indices into
``assets/block_states.txt``, universal blockstate strings) and writes a Java
1.19.2 world (``level.dat`` + ``region/*.mca``) using amulet-core, which
translates the universal blockstates to Java format on save.
"""

import logging
import sys
from pathlib import Path

import numpy as np

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
try:
    from src.common.block_colors import load_block_states, load_id2rgb
except ImportError:
    from common.block_colors import load_block_states, load_id2rgb

import amulet
import amulet_nbt
from amulet.api.block import Block, UniversalAirBlock
from amulet.level.formats.anvil_world.format import AnvilFormat

logger = logging.getLogger(__name__)

OVERWORLD = "minecraft:overworld"
TARGET_VERSION = (1, 19, 2)
CHUNK_SIZE = 16
PLAINS_BIOME = "universal_minecraft:plains"


def build_world(
    grid: np.ndarray,
    world_dir: Path,
    states: list[str] | None = None,
    air_ids: np.ndarray | None = None,
    base_y: int = -64,
    level_name: str = "Generated Structure",
    overwrite: bool = True,
) -> Path:
    """Write a Java 1.19.2 world containing the given voxel grid.

    Voxel ``(x, z, 0)`` is placed at world ``(x, base_y, z)``; air-ID voxels are
    left as air. The grid's X/Z extents must be multiples of 16 (chunk size).

    Args:
        grid: ``(X, Z, Y)`` integer array of global block IDs.
        world_dir: Directory to create the world in.
        states: Universal blockstate strings indexed by global ID. Defaults to
            ``load_block_states()``.
        air_ids: Global IDs to treat as air. Defaults to the air IDs from
            ``load_id2rgb()``.
        base_y: World y coordinate of the grid's bottom layer.
        level_name: Display name written to ``level.dat``.
        overwrite: Whether to overwrite an existing world at ``world_dir``.

    Returns:
        The world directory path.

    Raises:
        ValueError: If the grid shape or vertical placement is invalid.
    """
    if grid.ndim != 3:
        raise ValueError(f"Expected a (X, Z, Y) grid, got shape {grid.shape}")
    size_x, size_z, size_y = grid.shape
    if size_x % CHUNK_SIZE or size_z % CHUNK_SIZE:
        raise ValueError(f"Grid X/Z extents must be multiples of {CHUNK_SIZE}, got {size_x}x{size_z}")
    if base_y < -64 or base_y + size_y > 320:
        raise ValueError(f"Grid [{base_y}, {base_y + size_y}) exceeds world bounds [-64, 320)")

    if states is None:
        states = load_block_states()
    if air_ids is None:
        _, air_ids = load_id2rgb()
    air_set = set(np.asarray(air_ids).tolist())

    world_dir = Path(world_dir)
    world_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Creating Java %s world at %s", ".".join(map(str, TARGET_VERSION)), world_dir)
    wrapper = AnvilFormat(str(world_dir))
    wrapper.create_and_open("java", TARGET_VERSION, overwrite=overwrite)
    wrapper.close()

    # Patch level.dat before opening for editing: amulet's minimal level.dat lacks
    # WorldGenSettings, so the overworld would open with pre-1.18 bounds [0, 256)
    # and negative-y sections would be clipped on save.
    spawn = (size_x // 2, base_y + size_y, size_z // 2)
    _patch_level_dat(world_dir, level_name, spawn)

    level = amulet.load_level(str(world_dir))

    try:
        # Palette index 0 must be air so untouched voxels stay empty.
        air_index = level.block_palette.get_add_block(UniversalAirBlock)
        lut = np.full(len(states), air_index, dtype=np.uint32)
        for gid in np.unique(grid):
            gid = int(gid)
            if gid in air_set:
                continue
            block = Block.from_snbt_blockstate(states[gid])
            lut[gid] = level.block_palette.get_add_block(block)

        for cx in range(size_x // CHUNK_SIZE):
            for cz in range(size_z // CHUNK_SIZE):
                chunk = level.create_chunk(cx, cz, OVERWORLD)
                slab = grid[
                    cx * CHUNK_SIZE : (cx + 1) * CHUNK_SIZE,
                    cz * CHUNK_SIZE : (cz + 1) * CHUNK_SIZE,
                    :,
                ]
                # (x, z, y) -> (x, y, z) as expected by chunk.blocks
                chunk.blocks[0:CHUNK_SIZE, base_y : base_y + size_y, 0:CHUNK_SIZE] = lut[slab].transpose(0, 2, 1)
                chunk.biomes.convert_to_3d()
                chunk.biomes[:, :, :] = chunk.biome_palette.get_add_biome(PLAINS_BIOME)
                chunk.changed = True

        logger.info("Saving %d chunks...", (size_x // CHUNK_SIZE) * (size_z // CHUNK_SIZE))
        level.save()
    finally:
        level.close()

    logger.info("World written to %s (spawn at %s)", world_dir, spawn)
    return world_dir


def _patch_level_dat(world_dir: Path, level_name: str, spawn: tuple[int, int, int]) -> None:
    """Patch the minimal amulet-generated ``level.dat`` for vanilla playability.

    Sets the level name, creative mode, peaceful difficulty, spawn point, and a
    superflat-void generator so chunks outside the grid stay empty.
    """
    from amulet_nbt import ByteTag, CompoundTag, IntTag, ListTag, LongTag, StringTag

    level_dat_path = world_dir / "level.dat"
    root = amulet_nbt.load(str(level_dat_path))
    data = root.compound.get_compound("Data")

    data["LevelName"] = StringTag(level_name)
    data["GameType"] = IntTag(1)  # creative
    data["Difficulty"] = ByteTag(0)  # peaceful
    data["allowCommands"] = ByteTag(1)
    data["SpawnX"] = IntTag(spawn[0])
    data["SpawnY"] = IntTag(spawn[1])
    data["SpawnZ"] = IntTag(spawn[2])
    data["DayTime"] = LongTag(6000)
    data["WorldGenSettings"] = CompoundTag(
        {
            "bonus_chest": ByteTag(0),
            "generate_features": ByteTag(0),
            "seed": LongTag(0),
            "dimensions": CompoundTag(
                {
                    "minecraft:overworld": CompoundTag(
                        {
                            "type": StringTag("minecraft:overworld"),
                            "generator": CompoundTag(
                                {
                                    "type": StringTag("minecraft:flat"),
                                    "settings": CompoundTag(
                                        {
                                            "layers": ListTag([]),
                                            "biome": StringTag("minecraft:plains"),
                                            "features": ByteTag(0),
                                            "lakes": ByteTag(0),
                                        }
                                    ),
                                }
                            ),
                        }
                    )
                }
            ),
        }
    )

    root.save_to(str(level_dat_path), compressed=True)
