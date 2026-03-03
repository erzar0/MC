from amulet.api.block import Block
from amulet.api.registry import BlockManager
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
import amulet
import os
import glob 
import numpy as np
import re
from tqdm import tqdm


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

        Raises:
            ValueError: If the world height exceeds 384 blocks.
            FileNotFoundError: If the region directory is missing.
        """
        self._world = amulet.load_level(world_path) 

        bounds = self._world.bounds("minecraft:overworld")
        height = (bounds.max_y - bounds.min_y)
        if height > 384:
            raise ValueError("World height must be less than or equal to 384")

        region_dir = Path(world_path) / "region"
        if not region_dir.exists():
            raise FileNotFoundError(f"Region directory not found at {region_dir}")

        self._mca_files = [f.as_posix() for f in region_dir.iterdir() if f.is_file() and f.suffix == ".mca"]
        self._mca_coords = []
        for path in self._mca_files:
            match = re.search(r"r\.(-?\d+)\.(-?\d+)\.mca", os.path.basename(path))
            if match:
                self._mca_coords.append((int(match.group(1)), int(match.group(2))))
        
        self._mca_coord_to_path = {(x, z): path for (x, z), path in zip(self._mca_coords, self._mca_files)}
        self._mca_coords = set(self._mca_coords)
        self._mca_coords_rejected = set()
        self._metadata = {"total_regions": len(self._mca_files), "extracted_regions": {}} 
        self._chunks_coords = set(self._world.all_chunk_coords("minecraft:overworld"))

        # Filter out regions that are incomplete (missing chunks)
        pbar = tqdm(tuple(self._mca_coords), desc="Checking regions")
        for mca_coord in pbar:
            pbar.set_postfix({"region": f"r.{mca_coord[0]}.{mca_coord[1]}"})
            exit_loop = False
            for cz in range(32):
                for cx in range(32):
                    chunk_coords = self.to_chunk_coords(mca_coord[0], mca_coord[1], cx, cz)
                    if (chunk_coords["x"], chunk_coords["z"]) not in self._chunks_coords:
                        self.reject_region(mca_coord[0], mca_coord[1])
                        exit_loop = True
                        break
                if exit_loop:
                    break
        
        self._blockstates = BlockStates()
        self._biomes = Biomes()
    
    @property
    def mca_coords(self) -> List[Tuple[int, int]]:
        """Returns a list of (region_x, region_z) tuples for all valid regions."""
        return list(self._mca_coords)
    
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
    
    def get_region_volume(self, region_x: int, region_z: int, get_biomes: bool = False) -> Tuple[np.ndarray, Optional[np.ndarray]]:
        """Extracts a 3D volume of global block IDs for a specified region.

        The volume is returned as a 512x512xH array, where H is the trimmed height
        of the world content (up to 384).

        Args:
            region_x: X coordinate of the region.
            region_z: Z coordinate of the region.
            get_biomes: Whether to also extract and return biome data.

        Returns:
            A tuple (volume, biomes). 
            - volume: A 3D numpy array (uint16) of shape (512, 512, height).
            - biomes: A 2D numpy array (uint16) of shape (512, 512) if get_biomes is True, else None.

        Raises:
            ValueError: If the region coordinates are not valid for this world.
        """
        def _trim_y_axis(volume: np.ndarray) -> np.ndarray:
            """Helper to trim vertical space to only include non-air blocks, max 384."""
            has_content = np.any(volume != 0, axis=(0, 1))
            if not np.any(has_content):
                return volume[:,:,-384:]
            
            indices = np.where(has_content)[0]
            return volume[:, :, indices[0] : indices[-1] + 1][:,:,-384:]

        if (region_x, region_z) not in self._mca_coords:
            raise ValueError(f"Region ({region_x}, {region_z}) not found or was rejected.")

        bounds = self._world.bounds("minecraft:overworld")
        height = (bounds.max_y - bounds.min_y)
        max_sections = (height // 16 + 1)
        volume_6d = np.zeros((32, 32, max_sections, 16, 16, 16), dtype=np.uint16)
        biomes = np.zeros((512, 512), dtype=np.uint16) if get_biomes else None

        # chunk_a = self.to_chunk_coords(region_x, region_z, 0, 0)
        # chunk_b = self.to_chunk_coords(region_x, region_z, 31, 31)
        # x_a, z_a = chunk_a["x"] * 16, chunk_a["z"] * 16
        # x_b, z_b = chunk_b["x"] * 16 + 16, chunk_b["z"] * 16 + 16
        # print(f"-from {x_a} {z_a} -to {x_b} {z_b}")

        pbar = tqdm(total=1024, desc=f"Region r.{region_x}.{region_z}", leave=False)
        for rx in range(32):
            for rz in range(32):
                pbar.update(1)
                chunk_coords = self.to_chunk_coords(region_x, region_z, rx, rz)
                try:
                    chunk = self._world.get_chunk(chunk_coords["x"], chunk_coords["z"], "minecraft:overworld")
                except Exception:
                    continue

                y_sections = sorted(chunk.blocks.sections)
                for i, y in enumerate(y_sections[:max_sections]):
                    sub_chunk = chunk.blocks.get_sub_chunk(y)
                    palette = chunk._block_palette
                    volume_6d[rx, rz, i] = self._blockstates.to_global_ids(sub_chunk, palette)

                if get_biomes:
                    chunk.biomes.convert_to_2d()
                    if chunk.biomes._2d is not None and chunk.biome_palette is not None:
                        x_start, x_end = rx * 16, (rx + 1) * 16
                        z_start, z_end = rz * 16, (rz + 1) * 16
                        tmp = self._biomes.to_global_ids(chunk.biomes._2d, chunk.biome_palette)
                        biomes[x_start:x_end, z_start:z_end] = tmp
        pbar.close()
                    
        # Transpose and reshape 6D array to 3D (512, 512, height)
        data = volume_6d.transpose(0, 3, 1, 5, 2, 4)
        data = _trim_y_axis(data.reshape(512, 512, -1))

        self._metadata["extracted_regions"][f"{region_x},{region_z}"] = {
            "y_range": (bounds.min_y, int(data.shape[2]) + bounds.min_y),
            "blocks": {str(k): int(v) for k, v in zip(*np.unique(data, return_counts=True))},
        }

        return data, biomes
    
    def get_heightmap(self, region_x: int, region_z: int, transparent_ids: List[int] = [0]) -> np.ndarray:
        """Generates a 512x512 heightmap for a given region.

        The heightmap represents the highest non-transparent block at each (x, z) 
        coordinate.

        Args:
            region_x: X coordinate of the region.
            region_z: Z coordinate of the region.
            transparent_ids: List of global IDs to treat as transparent (default [0] for air).

        Returns:
            A 2D numpy array (uint16) of shape (512, 512) representing heights.
        """
        volume, _ = self.get_region_volume(region_x, region_z, False)
        
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
        if not path:
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
                except Exception:
                    continue

        return data / 20 # Convert ticks to seconds

    def chunk_inhibited_time(self, x: int, z: int) -> int:
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
    

class BlockStates:
    """Manages the mapping between Minecraft blockstate strings and global IDs.

    This class ensures that blockstates are consistently mapped to numerical IDs 
    across different chunks and regions. The mapping is persisted to a text file.
    """

    def __init__(self, asset_path: Optional[Path] = None) -> None:
        """Initializes the BlockStates manager.

        Args:
            asset_path: Optional path to the directory where block_states.txt is stored.
                        Defaults to the 'assets' directory in the project root.
        """
        if asset_path is None:
            asset_path = Path(__file__).parent.parent / "assets"
        
        asset_path.mkdir(parents=True, exist_ok=True)
        self._file_path = asset_path / "block_states.txt"
        self._file_path.touch(exist_ok=True)

        with open(self._file_path, "r") as f:
            lines = f.read().splitlines()
            self._blockstates = lines
            self._blockstates_dict = {state: i for i, state in enumerate(lines)}

    def _add_blockstate(self, blockstate: str) -> int:
        """Assigns a new global ID to a blockstate and persists it.

        Args:
            blockstate: The blockstate string to add.

        Returns:
            The newly assigned global ID.
        """
        new_id = len(self._blockstates)
        self._blockstates.append(blockstate)
        self._blockstates_dict[blockstate] = new_id
        
        with open(self._file_path, "a") as f:
            f.write(f"{blockstate}\n") 
        return new_id

    def to_global_ids(self, blocks_array: np.ndarray, block_palette: BlockManager) -> np.ndarray:
        """Translates a local chunk palette to global IDs.

        Args:
            blocks_array: 3D numpy array of palette indices.
            block_palette: Amulet BlockManager containing the palette for the chunk.

        Returns:
            A numpy array of the same shape as blocks_array, containing global IDs.
        """
        palette_translation = []
        
        marker_block = 'universal_minecraft:wool[color="magenta"]'

        for i in range(len(block_palette)):
            block_obj = block_palette._index_to_block[i]
            block_str = str(block_obj)
            
            # Filter out numerical blocks (legacy format) and use a marker
            if "minecraft:numerical" in block_str:
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
            The blockstate string, or a default 'air' block if the ID is out of bounds.
        """
        if 0 <= id < len(self._blockstates):
            return self._blockstates[id]
        return 'universal_minecraft:wool[color="magenta"]'
    
    def get_global_id_by_block(self, blockstate: str) -> int:
        """Retrieves the global ID corresponding to a given blockstate string.

        If the blockstate is not found in the mapping, it is added as a new entry
        and assigned a new global ID.

        Args:
            blockstate: The blockstate string.

        Returns:
            The global ID for the blockstate.
        """
        if blockstate not in self._blockstates_dict:
            return self._add_blockstate(blockstate)
        return self._blockstates_dict[blockstate]

class Biomes:
    """Manages the mapping between Minecraft biome strings and global IDs.

    Similar to BlockStates, this class provides consistent numerical IDs for biomes
    and persists them to a text file.
    """

    def __init__(self, asset_path: Optional[Path] = None) -> None:
        """Initializes the Biomes manager.

        Args:
            asset_path: Optional path to the directory where biomes.txt is stored.
                        Defaults to the 'assets' directory in the project root.
        """
        if asset_path is None:
            asset_path = Path(__file__).parent.parent / "assets"

        asset_path.mkdir(parents=True, exist_ok=True)
        self._file_path = asset_path / "biomes.txt"
        self._file_path.touch(exist_ok=True)
        
        with open(self._file_path, "r") as f:
            lines = f.read().splitlines()
            self._biomes = lines
            self._biomes_dict = {biome: i for i, biome in enumerate(lines)}

    def _add_biome(self, biome_str: str) -> int:
        """Assigns a new global ID to a biome and persists it.

        Args:
            biome_str: The biome string to add.

        Returns:
            The newly assigned global ID.
        """
        new_id = len(self._biomes)
        self._biomes.append(biome_str)
        self._biomes_dict[biome_str] = new_id
        
        with open(self._file_path, "a") as f:
            f.write(f"{biome_str}\n") 
        return new_id

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
            The biome string, or a default 'ocean' biome if the ID is out of bounds.
        """
        if 0 <= id < len(self._biomes):
            return self._biomes[id]
        return 'universal_minecraft:plains'
    
    def get_global_id_by_biome(self, biome_str: str) -> int:
        """Retrieves the global ID corresponding to a given biome string.

        If the biome string is not found in the mapping, it is added as a new entry
        and assigned a new global ID.

        Args:
            biome_str: The biome string.

        Returns:
            The global ID for the biome string.
        """
        if biome_str not in self._biomes_dict:
            return self._add_biome(biome_str)
        return self._biomes_dict[biome_str]
