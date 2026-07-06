import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import amulet
import anvil
import numpy as np
from amulet.api.registry import BlockManager
from tqdm import tqdm

logger = logging.getLogger(__name__)

try:
    from .fast_volume_extractor import FastVolumeParser
except ImportError:
    from fast_volume_extractor import FastVolumeParser


class WorldWrapper:
    """A wrapper around the Amulet library to extract region data from Minecraft worlds.

    This class provides a high-level interface to load Minecraft worlds, identify valid
    regions, and extract structured data such as 3D block volumes, heightmaps,
    and player inhabited times. It also handles translation of block and biome IDs
    to consistent global IDs.
    """

    def __init__(self, world_path: Path) -> None:
        """Initializes the WorldWrapper and discovers valid regions.

        Args:
            world_path: Path to the Minecraft world directory (containing level.dat).
        """
        self._world = amulet.load_level(world_path)

        bounds = self._world.bounds("minecraft:overworld")
        height = bounds.max_y - bounds.min_y
        if height > 384:
            raise ValueError("World height must be less than or equal to 384")

        region_dir = Path(world_path) / "region"
        if not region_dir.exists():
            raise FileNotFoundError(f"Region directory not found at {region_dir}")

        self._mca_files = [f.as_posix() for f in region_dir.iterdir() if f.is_file() and f.suffix == ".mca"]
        # Build the coord -> path mapping directly so files that don't match the
        # r.X.Z.mca pattern can't misalign the pairing.
        self._mca_coord_to_path = {}
        for path in self._mca_files:
            match = re.search(r"r\.(-?\d+)\.(-?\d+)\.mca", os.path.basename(path))
            if match:
                self._mca_coord_to_path[(int(match.group(1)), int(match.group(2)))] = path

        self._mca_coords = set(self._mca_coord_to_path)
        self._mca_coords_rejected = set()
        self._metadata = {"total_regions": len(self._mca_files), "extracted_regions": {}}
        self._chunks_coords = set(self._world.all_chunk_coords("minecraft:overworld"))

        # Filter out regions that are missing any chunks (Stage 1 rejection)
        pbar = tqdm(tuple(self._mca_coords), desc="Checking regions")
        for mca_coord in pbar:
            pbar.set_postfix({"region": f"r.{mca_coord[0]}.{mca_coord[1]}"})
            reg_x_base, reg_z_base = mca_coord[0] * 32, mca_coord[1] * 32
            if any(
                (reg_x_base + cx, reg_z_base + cz) not in self._chunks_coords for cz in range(32) for cx in range(32)
            ):
                self.reject_region(mca_coord[0], mca_coord[1])

        self._blockstates = BlockStates()
        self._biomes = Biomes()

    @property
    def mca_coords(self) -> List[Tuple[int, int]]:
        """Returns a list of (region_x, region_z) tuples for all valid regions."""
        return list(self._mca_coords - self._mca_coords_rejected)

    @property
    def metadata(self) -> dict:
        """Returns the metadata for the world."""
        return self._metadata

    @property
    def mca_paths(self, include_rejected=False):
        if include_rejected:
            return list(self._mca_coord_to_path.values())
        return [path for coord, path in self._mca_coord_to_path.items() if coord not in self._mca_coords_rejected]

    @property
    def chunk_coords(self) -> List[Tuple[int, int]]:
        """Returns a list of (chunk_x, chunk_z) tuples for all chunks in the world."""
        return list(self._chunks_coords)

    def misc_keys(self) -> Any:
        """Utility method to inspect misc keys in a sample chunk for debugging.

        Returns:
            The keys present in the 'misc' attribute of the first available chunk.
        """
        if not self._chunks_coords:
            return []
        x, z = list(self._chunks_coords)[0]
        return self._world.get_chunk(x, z, "minecraft:overworld").misc.keys()

    def reject_region(self, region_x: int, region_z: int) -> None:
        """Marks a region as rejected and removes it from processing.

        This is used to filter out regions that don't meet quality criteria
        (e.g., incomplete regions at the edge of the world).

        Args:
            region_x: The X coordinate of the region.
            region_z: The Z coordinate of the region.
        """
        if (region_x, region_z) in self._mca_coords:
            self._mca_coords_rejected.add((region_x, region_z))
            self._metadata["rejected_regions"] = self._metadata.get("rejected_regions", 0) + 1

    def _post_process_volume(self, volume: np.ndarray) -> Tuple[np.ndarray, int]:
        """Transposes, reshapes and trims vertical space to only include non-air blocks.

        Returns:
            Tuple of (trimmed_volume, start_y_offset)
        """
        data = volume.transpose(0, 3, 1, 5, 2, 4)
        data = data.reshape(512, 512, -1)

        has_content = np.any(data != 0, axis=(0, 1))
        if not np.any(has_content):
            return data[:, :, :0], 0

        indices = np.where(has_content)[0]
        start_y = int(indices[0])
        end_y = int(indices[-1] + 1)

        trimmed = data[:, :, start_y:end_y]
        return trimmed, start_y

    def get_region_biomes(self, region_x: int, region_z: int) -> np.ndarray:
        """Extracts a 2D array of global biome IDs for a specified region.

        Args:
            region_x: X coordinate of the region.
            region_z: Z coordinate of the region.

        Returns:
            A 2D numpy array (uint16) of shape (512, 512).
        """
        if (region_x, region_z) not in self._mca_coords or (region_x, region_z) in self._mca_coords_rejected:
            raise ValueError(f"Region ({region_x}, {region_z}) not found or was rejected.")

        biomes = np.zeros((512, 512), dtype=np.uint16)
        pbar = tqdm(total=1024, desc=f"Biomes r.{region_x}.{region_z}", leave=False)
        for rx in range(32):
            for rz in range(32):
                pbar.update(1)
                chunk_coords = self.to_chunk_coords(region_x, region_z, rx, rz)
                try:
                    chunk = self._world.get_chunk(chunk_coords["x"], chunk_coords["z"], "minecraft:overworld")
                    chunk.biomes.convert_to_2d()
                    if chunk.biomes._2d is not None and chunk.biome_palette is not None:
                        x_start, x_end = rx * 16, (rx + 1) * 16
                        z_start, z_end = rz * 16, (rz + 1) * 16
                        tmp = self._biomes.to_global_ids(chunk.biomes._2d, chunk.biome_palette)
                        biomes[x_start:x_end, z_start:z_end] = tmp
                except Exception as e:
                    logger.debug(f"Skipping biomes for chunk {chunk_coords}: {e}")
                    continue
        pbar.close()
        return biomes

    def get_region_volume(self, region_x: int, region_z: int, use_fast_extractor: bool = True) -> np.ndarray:
        """Extracts a 3D volume of global block IDs for a specified region.

        The volume is returned as a 512x512xH array, where H is the trimmed height
        of the world content.

        Args:
            region_x: X coordinate of the region.
            region_z: Z coordinate of the region.
            use_fast_extractor: Whether to use the FastVolumeParser for speed.

        Returns:
            A 3D numpy array (uint16) of shape (512, 512, height).

        Raises:
            ValueError: If the region coordinates are not valid for this world.
        """
        if (region_x, region_z) not in self._mca_coords or (region_x, region_z) in self._mca_coords_rejected:
            raise ValueError(f"Region ({region_x}, {region_z}) not found or was rejected.")

        bounds = self._world.bounds("minecraft:overworld")

        if use_fast_extractor:
            volume_6d = self._extract_via_fast_parser(region_x, region_z, bounds)
        else:
            volume_6d = self._extract_via_amulet(region_x, region_z, bounds)

        data, start_y_offset = self._post_process_volume(volume_6d)
        self._validate_region_content(region_x, region_z, data)
        avg_surface_local = self._compute_surface_elevation(region_x, region_z, data)

        actual_min_y = bounds.min_y + start_y_offset
        avg_surface_pos = round(avg_surface_local + actual_min_y, 2)

        self._metadata["extracted_regions"][f"{region_x},{region_z}"] = {
            "y_range": (-64, min(320, int(data.shape[2]) + actual_min_y)),
            "blocks": {str(k): int(v) for k, v in zip(*np.unique(data, return_counts=True), strict=True)},
            "avg_surface_y": avg_surface_pos,
        }

        return data

    def _extract_via_fast_parser(self, region_x: int, region_z: int, bounds) -> np.ndarray:
        """Extracts the raw 6D section volume using FastVolumeParser.

        Returns:
            A (32, 32, sections, 16, 16, 16) uint16 array of global block IDs.

        Raises:
            FileNotFoundError: If the region has no backing MCA file.
            RuntimeError: If the fast parser fails; the region is rejected.
        """
        mca_path = self._mca_coord_to_path.get((region_x, region_z))
        if not mca_path:
            self.reject_region(region_x, region_z)
            raise FileNotFoundError(f"MCA file not found for region ({region_x}, {region_z}).")

        try:
            parser = FastVolumeParser(Path(mca_path), self._blockstates)
            volume_6d = parser.extract_volume(min_y=bounds.min_y, height=384 + 16)
            parser.close()
            return volume_6d
        except Exception as e:
            self.reject_region(region_x, region_z)
            raise RuntimeError(f"FastVolumeParser failed for region ({region_x}, {region_z}): {e}") from e

    def _extract_via_amulet(self, region_x: int, region_z: int, bounds) -> np.ndarray:
        """Extracts the raw 6D section volume chunk-by-chunk via Amulet (slow path).

        Returns:
            A (32, 32, sections, 16, 16, 16) uint16 array of global block IDs.
        """
        height = bounds.max_y - bounds.min_y
        max_sections = (height // 16 + 1) + 1  # Add 1 and check later if region is too high
        volume_6d = np.zeros((32, 32, max_sections, 16, 16, 16), dtype=np.uint16)

        pbar = tqdm(total=1024, desc=f"Region r.{region_x}.{region_z}", leave=False)
        for rx in range(32):
            for rz in range(32):
                pbar.update(1)
                chunk_coords = self.to_chunk_coords(region_x, region_z, rx, rz)
                try:
                    chunk = self._world.get_chunk(chunk_coords["x"], chunk_coords["z"], "minecraft:overworld")
                    y_offset_sections = -(bounds.min_y // 16)
                    for y in sorted(chunk.blocks.sections):
                        sec_idx = y + y_offset_sections
                        if 0 <= sec_idx < max_sections:
                            sub_chunk = chunk.blocks.get_sub_chunk(y)
                            volume_6d[rx, rz, sec_idx] = self._blockstates.to_global_ids(
                                sub_chunk, chunk._block_palette
                            )
                except Exception as e:
                    logger.debug(f"Skipping blocks for chunk {chunk_coords}: {e}")
                    continue
        return volume_6d

    def _validate_region_content(self, region_x: int, region_z: int, data: np.ndarray) -> None:
        """Rejects regions that are too thin, too tall, or mostly empty.

        Raises:
            ValueError: If the region fails any content check; the region is rejected.
        """
        # Reject if the non-air span is too thin
        if data.shape[2] < 5:
            self.reject_region(region_x, region_z)
            raise ValueError(
                f"Region ({region_x}, {region_z}) rejected: non-air span is only {data.shape[2]} blocks high."
            )

        # Reject if the non-air span is too high
        if data.shape[2] > 384:
            self.reject_region(region_x, region_z)
            raise ValueError(f"Region ({region_x}, {region_z}) rejected: height exceeds 384 blocks.")

        # Reject if more than 90% of the top-down view is air/sponge
        content_ratio = np.sum(np.any(data > 1, axis=2)) / (512 * 512)
        if content_ratio < 0.10:
            self.reject_region(region_x, region_z)
            raise ValueError(
                f"Region ({region_x}, {region_z}) rejected: {(1.0 - content_ratio) * 100:.1f}% filled with air/sponge."
            )

    def _compute_surface_elevation(self, region_x: int, region_z: int, data: np.ndarray) -> float:
        """Computes the median surface elevation (local Y index) of a region.

        Raises:
            ValueError: If the heightmap is perfectly flat and uniform (e.g. an
                untouched superflat world); the region is rejected.
        """
        mask = data > 1
        has_solid = np.any(mask, axis=2)

        if not np.any(has_solid):
            return 320

        # Find the highest block by looking from the top down (reversing the Y axis)
        flipped_mask = mask[:, :, ::-1]
        highest_indices = data.shape[2] - 1 - np.argmax(flipped_mask, axis=2)

        # Average the elevation ONLY for columns that have at least one block
        # USING MEDIAN prevents tall structures or deep holes from skewing the plane
        solid_heights = highest_indices[has_solid]

        # Reject if the height map is completely flat AND uniform (e.g. superflat or artificial platform)
        if np.ptp(solid_heights) == 0:
            # Since it's perfectly flat, all surface blocks are at the exact same vertical index
            h = solid_heights[0]
            surface_blocks = data[:, :, h][has_solid]

            # Only reject if every single block on the flat surface is identical
            if np.ptp(surface_blocks) == 0:
                self.reject_region(region_x, region_z)
                raise ValueError(f"Region ({region_x}, {region_z}) rejected: heightmap is completely flat and uniform.")

        return float(np.median(solid_heights))

    def get_heightmap(self, region_x: int, region_z: int, transparent_ids: Optional[List[int]] = None) -> np.ndarray:
        """Generates a 512x512 heightmap for a given region.

        The heightmap represents the highest non-transparent block at each (x, z)
        coordinate.

        Args:
            region_x: X coordinate of the region.
            region_z: Z coordinate of the region.
            transparent_ids: List of global IDs to treat as transparent (default [0, 1] for air/sponge).

        Returns:
            A 2D numpy array (uint16) of shape (512, 512) representing heights.
        """
        if transparent_ids is None:
            transparent_ids = [0, 1]
        volume = self.get_region_volume(region_x, region_z, use_fast_extractor=True)

        is_solid = ~np.isin(volume, transparent_ids)

        flipped_mask = is_solid[:, :, ::-1]

        heights = flipped_mask.shape[2] - np.argmax(flipped_mask, axis=2)

        no_solid_blocks = ~np.any(is_solid, axis=2)
        heights[no_solid_blocks] = 0

        return heights.astype(np.uint16)

    def mca_inhabited_times(self, region_x: int, region_z: int) -> np.ndarray:
        """Extracts a 32x32 array of inhabited times (in seconds) for a region.

        Inhabited time is the total time in ticks that players have spent in a chunk.
        This method uses the `anvil` library to read data directly from .mca files.

        Args:
            region_x: X coordinate of the region.
            region_z: Z coordinate of the region.

        Returns:
            A 32x32 numpy array (int64) of inhabited times in seconds.
        """
        path = self._mca_coord_to_path.get((region_x, region_z))
        if not path or (region_x, region_z) in self._mca_coords_rejected:
            return np.zeros((32, 32), dtype=np.int64)

        data = np.zeros((32, 32), dtype=np.int64)

        region = anvil.Region.from_file(path)

        for cz in range(32):
            for cx in range(32):
                try:
                    chunk = region.chunk_data(cx, cz)
                    if chunk:
                        it_value = 0
                        if "InhabitedTime" in chunk:
                            it_value = chunk["InhabitedTime"].value
                        elif "Level" in chunk and "InhabitedTime" in chunk["Level"]:
                            it_value = chunk["Level"]["InhabitedTime"].value

                        data[cx, cz] = it_value
                except Exception as e:
                    logger.debug(f"Skipping inhabited time for chunk ({cx}, {cz}): {e}")
                    continue

        return data / 20  # Convert ticks to seconds

    def chunk_inhabited_time(self, x: int, z: int) -> int:
        """Retrieves the inhabited time for a specific chunk.

        Args:
            x: X coordinate of the chunk.
            z: Z coordinate of the chunk.

        Returns:
            The inhabited time in ticks for the specified chunk.
        """
        return self._world.get_chunk(x, z, "minecraft:overworld").misc.get("InhabitedTime", 0)

    def unload(self) -> None:
        """Unloads the world from memory to free up resources."""
        self._world.unload()

    @staticmethod
    def to_mca_coords(chunk_x: int, chunk_z: int) -> Dict[str, int]:
        """Converts chunk coordinates to region (MCA) coordinates and offsets.

        Args:
            chunk_x: X coordinate of the chunk.
            chunk_z: Z coordinate of the chunk.

        Returns:
            Dictionary with keys: 'x', 'z', 'x_offset', 'z_offset'.
        """
        region_x = chunk_x // 32
        region_z = chunk_z // 32
        region_x_offset = chunk_x % 32
        region_z_offset = chunk_z % 32
        return {"x": region_x, "z": region_z, "x_offset": region_x_offset, "z_offset": region_z_offset}

    @staticmethod
    def to_chunk_coords(region_x: int, region_z: int, region_x_offset: int, region_z_offset: int) -> Dict[str, int]:
        """Converts region (MCA) coordinates and offsets back to chunk coordinates.

        Args:
            region_x: X coordinate of the region.
            region_z: Z coordinate of the region.
            region_x_offset: X offset within the region (0-31).
            region_z_offset: Z offset within the region (0-31).

        Returns:
            Dictionary with keys: 'x', 'z'.
        """
        chunk_x = region_x * 32 + region_x_offset
        chunk_z = region_z * 32 + region_z_offset
        return {"x": chunk_x, "z": chunk_z}


class _StringRegistry:
    """Append-only mapping between strings and global numerical IDs.

    Loads the existing mapping from a text file (line index == global ID) and
    appends new entries to that file as they are encountered, so IDs stay
    consistent across runs. Base class for `BlockStates` and `Biomes`.
    """

    #: File name (inside the asset directory) holding the persisted mapping.
    REGISTRY_FILENAME: str = ""
    #: String returned for out-of-bounds ID lookups.
    FALLBACK_ENTRY: str = ""

    def __init__(self, asset_path: Optional[Path] = None) -> None:
        """Initializes the registry.

        Args:
            asset_path: Optional path to the directory where the registry file is
                        stored. Defaults to the 'assets' directory in the project root.
        """
        if asset_path is None:
            asset_path = Path(__file__).resolve().parents[2] / "assets"

        asset_path.mkdir(parents=True, exist_ok=True)
        self._file_path = asset_path / self.REGISTRY_FILENAME
        self._file_path.touch(exist_ok=True)

        with open(self._file_path, "r") as f:
            lines = f.read().splitlines()
            self._entries = lines
            self._entries_dict = {entry: i for i, entry in enumerate(lines)}

    def _add_entry(self, entry: str) -> int:
        """Assigns a new global ID to an entry and persists it.

        Args:
            entry: The string to add.

        Returns:
            The newly assigned global ID.
        """
        new_id = len(self._entries)
        self._entries.append(entry)
        self._entries_dict[entry] = new_id

        with open(self._file_path, "a") as f:
            f.write(f"{entry}\n")
        return new_id

    def _get_entry_by_id(self, id: int) -> str:
        """Returns the string for a global ID, or FALLBACK_ENTRY when out of bounds."""
        if 0 <= id < len(self._entries):
            return self._entries[id]
        return self.FALLBACK_ENTRY

    def _get_id_by_entry(self, entry: str) -> int:
        """Returns the global ID for a string, registering it first if unknown."""
        if entry not in self._entries_dict:
            return self._add_entry(entry)
        return self._entries_dict[entry]


class BlockStates(_StringRegistry):
    """Manages the mapping between Minecraft blockstate strings and global IDs.

    This class ensures that blockstates are consistently mapped to numerical IDs
    across different chunks and regions. The mapping is persisted to a text file.
    """

    REGISTRY_FILENAME = "block_states.txt"
    FALLBACK_ENTRY = 'universal_minecraft:sponge[wet="false"]'

    def to_global_ids(self, blocks_array: np.ndarray, block_palette: BlockManager) -> np.ndarray:
        """Translates a local chunk palette to global IDs.

        Args:
            blocks_array: 3D numpy array of palette indices.
            block_palette: Amulet BlockManager containing the palette for the chunk.

        Returns:
            A numpy array of the same shape as blocks_array, containing global IDs.
        """
        palette_translation = []

        marker_block = 'universal_minecraft:sponge[wet="false"]'

        for i in range(len(block_palette)):
            block_obj = block_palette._index_to_block[i]
            block_str = str(block_obj)

            # Filter out numerical blocks (legacy format) and use a marker
            if "minecraft:numerical" in block_str:
                logger.warning(f"Intercepted legacy block: {block_str}")
                global_id = self.get_global_id_by_block(marker_block)
            else:
                global_id = self.get_global_id_by_block(block_str)

            palette_translation.append(global_id)

        palette_translation = np.array(palette_translation, dtype=np.uint16)
        return palette_translation[blocks_array]

    def get_block_by_global_id(self, id: int) -> str:
        """Retrieves the blockstate string corresponding to a given global ID.

        Args:
            id: The global ID of the blockstate.

        Returns:
            The blockstate string, or a default 'sponge' block if the ID is out of bounds.
        """
        return self._get_entry_by_id(id)

    def _sanitize_mod_block(self, block_str: str) -> str:
        """Converts modded blocks into a distinct vanilla marker block."""
        if block_str.startswith("universal_minecraft:"):
            return block_str
        return 'universal_minecraft:sponge[wet="false"]'

    def get_global_id_by_block(self, blockstate: str) -> int:
        """Retrieves the global ID corresponding to a given blockstate string.

        If the blockstate is not found in the mapping, it is added as a new entry
        and assigned a new global ID.

        Args:
            blockstate: The blockstate string.

        Returns:
            The global ID for the blockstate.
        """
        return self._get_id_by_entry(self._sanitize_mod_block(blockstate))


class Biomes(_StringRegistry):
    """Manages the mapping between Minecraft biome strings and global IDs.

    Similar to BlockStates, this class provides consistent numerical IDs for biomes
    and persists them to a text file.
    """

    REGISTRY_FILENAME = "biomes.txt"
    FALLBACK_ENTRY = "universal_minecraft:plains"

    def to_global_ids(self, biome_indices: np.ndarray, biome_palette: list) -> np.ndarray:
        """Translates local biome indices to global IDs.

        Args:
            biome_indices: 2D numpy array of biome indices.
            biome_palette: List of biome objects in the local palette.

        Returns:
            A numpy array of the same shape as biome_indices, containing global IDs.
        """
        palette_translation = []

        for biome_obj in biome_palette:
            biome_str = str(biome_obj)
            global_id = self.get_global_id_by_biome(biome_str)
            palette_translation.append(global_id)

        palette_translation = np.array(palette_translation, dtype=np.uint16)
        return palette_translation[biome_indices]

    def get_biome_by_global_id(self, id: int) -> str:
        """Retrieves the biome string corresponding to a given global ID.

        Args:
            id: The global ID of the biome.

        Returns:
            The biome string, or a default 'plains' biome if the ID is out of bounds.
        """
        return self._get_entry_by_id(id)

    def get_global_id_by_biome(self, biome_str: str) -> int:
        """Retrieves the global ID corresponding to a given biome string.

        If the biome string is not found in the mapping, it is added as a new entry
        and assigned a new global ID.

        Args:
            biome_str: The biome string.

        Returns:
            The global ID for the biome string.
        """
        return self._get_id_by_entry(biome_str)
