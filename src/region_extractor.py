from amulet.api.block import Block
from amulet.api.registry import BlockManager
from pathlib import Path
from typing import List
import amulet
import os
import anvil
import glob 
import numpy as np
import re



class WorldWrapper:
    """
    A wrapper around the Amulet library to extract region data from Minecraft
    worlds. It loads the world, identifies valid regions, and provides methods
    to extract region volumes, heightmaps, and inhabited times. It also handles
    blockstate and biome ID translation to global IDs.
    """
    def __init__(self, world_path: Path) -> None:
        self._world = amulet.load_level(world_path) 

        bounds = self._world.bounds("minecraft:overworld")
        height = (bounds.max_y - bounds.min_y)
        if height > 384:
            raise ValueError("World height must be less than or equal to 384")

        self._mca_files = [f.as_posix() for f in (world_path / "region").iterdir() if f.is_file() and f.suffix == ".mca"]
        self._mca_coords = tuple(
            (int(re.search(r"r\.(-?\d+)\.(-?\d+)\.mca", os.path.basename(path)).group(1)),
             int(re.search(r"r\.(-?\d+)\.(-?\d+)\.mca", os.path.basename(path)).group(2)))
            for path in self._mca_files
        )
        self._mca_coord_to_path = {(x, z): path for (x, z), path in zip(self._mca_coords, self._mca_files)}
        self._mca_coords = set(self._mca_coords)
        self._metadata = {"total_regions": len(self._mca_files)} 
        self._chunks_coords = set(self._world.all_chunk_coords("minecraft:overworld"))

        for mca_coord in tuple(self._mca_coords):
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
    def mca_coords(self) -> List[tuple]:
        """
        Returns a list of (region_x, region_z) tuples for all valid regions in the world.
        """
        return self._mca_coords

    @property
    def chunk_coords(self) -> List[tuple]:
        """
        Returns a list of (chunk_x, chunk_z) tuples for all chunks in the world.
        """
        return self._chunks_coords
    
    def misc_keys(self) -> None:
        """
        Utility method to inspect misc keys in a chunk for debugging purposes.
        """
        x, z = list(self._chunks_coords)[0]
        return self._world.get_chunk(x, z, "minecraft:overworld").misc.keys()
    
    def reject_region(self, region_x: int, region_z: int) -> None:
        """
        Marks a region as rejected by removing it from the valid regions set and
        updating metadata. This is used to filter out regions that don't meet
        certain criteria (e.g., missing chunks).
        """
        if (region_x, region_z) in self._mca_coords:
            del self._mca_coord_to_path[(region_x, region_z)]
            self._mca_coords.remove((region_x, region_z))
            self._metadata["rejected_regions"] = self._metadata.get("rejected_regions", 0) + 1
    
    def get_region_volume(self, region_x: int, region_z: int, get_biomes=False) -> np.array:
        """
        Extracts a 3D volume of block global IDs for a given region, along with
        the corresponding biome IDs.
        """
        def _trim_y_axis(volume: np.array) -> np.array:
            has_content = np.any(volume != 0, axis=(0, 1))
            if not np.any(has_content):
                return volume[:,:,-384:]
            
            indices = np.where(has_content)[0]
            return volume[:, :, indices[0] : indices[-1] + 1][:,:,-384:]

        if (region_x, region_z) not in self._mca_coords:
            raise ValueError(f"Region ({region_x}, {region_z}) not found in world.")

        bounds = self._world.bounds("minecraft:overworld")
        height = (bounds.max_y - bounds.min_y)
        max_sections = (height // 16 + 1)
        volume_6d = np.zeros((32, 32, max_sections, 16, 16, 16), dtype=np.uint16)
        biomes = np.zeros((512, 512), dtype=np.uint16) if get_biomes else None

        chunk_a = self.to_chunk_coords(region_x, region_z, 0, 0)
        chunk_b = self.to_chunk_coords(region_x, region_z, 31, 31)
        x_a, z_a = chunk_a["x"] * 16, chunk_a["z"] * 16
        x_b, z_b = chunk_b["x"] * 16 + 16, chunk_b["z"] * 16 + 16
        print(f"-from {x_a} {z_a} -to {x_b} {z_b}")

        for rx in range(32):
            for rz in range(32):
                chunk_coords = self.to_chunk_coords(region_x, region_z, rx, rz)
                try:
                    chunk = self._world.get_chunk(chunk_coords["x"], chunk_coords["z"], "minecraft:overworld")
                except:
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
                    
        self._world.unload()
        data = volume_6d.transpose(0, 3, 1, 5, 2, 4)
        return _trim_y_axis(data.reshape(512, 512, -1)), biomes
    
    def get_heightmap(self, region_x: int, region_z: int, transparent_ids: list = [0]) -> np.array:
        """
        Generates a 512x512 heightmap for a given region.
        :param transparent_ids: List of global IDs to ignore (defaults to 0 for air).
        :return: 2D numpy array of heights.
        """
        volume, _ = self.get_region_volume(region_x, region_z, False)
        
        is_solid = ~np.isin(volume, transparent_ids)
        
        flipped_mask = is_solid[:, :, ::-1]
        
        heights = flipped_mask.shape[2] - np.argmax(flipped_mask, axis=2)
        
        no_solid_blocks = ~np.any(is_solid, axis=2)
        heights[no_solid_blocks] = 0
        
        return heights.astype(np.uint16)

    def mca_inhabited_times(self, region_x: int, region_z: int) -> np.array:
        """
        Extracts a 32x32 array of inhabited times for each chunk in the specified region.
        Inhabited time is the total time in ticks that players have spent in the chunk.
        Returns the inhabited times in seconds by dividing the tick count by 20.
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

        return data / 20 # Convert to seconds

    def chunk_inhibited_time(self, x: int, z: int) -> int:
        return self._world.get_chunk(x, z, "minecraft:overworld").misc.get("InhabitedTime", 0)

    @staticmethod
    def to_mca_coords(chunk_x: int, chunk_z: int) -> dict:
        """
        Converts chunk coordinates to region (MCA) coordinates and offsets.
        Returns a dictionary with region_x, region_z, region_x_offset, and region_z_offset.
        """
        region_x = chunk_x // 32
        region_z = chunk_z // 32
        region_x_offset = chunk_x % 32
        region_z_offset = chunk_z % 32
        return {"x": region_x, "z": region_z, "x_offset": region_x_offset, "z_offset": region_z_offset}
    
    @staticmethod
    def to_chunk_coords(region_x: int, region_z: int, region_x_offset: int, region_z_offset: int) -> dict:
        """
        Converts region (MCA) coordinates and offsets back to chunk coordinates.
        Returns a dictionary with chunk_x and chunk_z.
        """
        chunk_x = region_x * 32 + region_x_offset
        chunk_z = region_z * 32 + region_z_offset
        return {"x": chunk_x, "z": chunk_z}
    

class BlockStates:
    """
    Manages the mapping between blockstate strings and global IDs. It maintains
    a list of blockstates and a dictionary for quick lookup. When a new
    blockstate is encountered, it is added to the list and the mapping is
    updated. The blockstates are persisted in a text file to maintain
    consistency across runs. This allows for efficient translation of blockstate
    palettes to global IDs when extracting region volumes.
    """
    def __init__(self) -> None:
        Path("../../assets/").mkdir(parents=True, exist_ok=True)
        Path("../../assets/block_states.txt").touch(exist_ok=True)
        with open("../../assets/block_states.txt", "r") as f:
            lines = f.read().splitlines()
            self._blockstates = lines
            self._blockstates_dict = {state: i for i, state in enumerate(lines)}

    def _add_blockstate(self, blockstate: str) -> int:
        """
        Adds a new blockstate to the list and dictionary, assigns it a new
        global ID, and persists it to the blockstates.txt file. Returns the
        global ID assigned to the new blockstate.
        """
        new_id = len(self._blockstates)
        self._blockstates.append(blockstate)
        self._blockstates_dict[blockstate] = new_id
        
        with open("../../assets/block_states.txt", "a") as f:
            f.write(f"{blockstate}\n") 
        return new_id

    def to_global_ids(self, blocks_array: np.array, block_palette: BlockManager) -> np.array:
        """
        Translates a 3D array of block indices (from a chunk's sub-chunk) and
        its corresponding block palette into a 3D array of global block IDs. It
        uses the block palette to look up the blockstate string for each index,
        then translates that blockstate into a global ID using the internal
        mapping. If a blockstate is not found in the mapping, it is added as a
        new entry. This allows for consistent global ID assignment across
        different chunks and regions.
        """
        palette_translation = []
        
        marker_block = 'universal_minecraft:wool[color="magenta"]'

        for i in range(len(block_palette)):
            block_obj = block_palette._index_to_block[i]
            block_str = str(block_obj)
            
            if "minecraft:numerical" in block_str:
                global_id = self.get_global_id_by_block(marker_block)
            else:
                global_id = self.get_global_id_by_block(block_str)
                
            palette_translation.append(global_id)

        palette_translation = np.array(palette_translation, dtype=np.uint16)
        return palette_translation[blocks_array]

    def get_block_by_global_id(self, id: int) -> str:
        """
        Retrieves the blockstate string corresponding to a given global ID. If
        the ID is out of bounds, it returns a marker blockstate string to
        indicate an unknown block.
        """
        return self._blockstates[id]
    
    def get_global_id_by_block(self, blockstate: str) -> int:
        """
        Retrieves the global ID corresponding to a given blockstate string. If
        the blockstate is not found in the mapping, it is added as a new entry
        and assigned a new global ID.
        """
        if blockstate not in self._blockstates_dict:
            return self._add_blockstate(blockstate)
        return self._blockstates_dict[blockstate]

class Biomes:
    """
    Manages the mapping between biome strings and global IDs, similar to the
    BlockStates class. It maintains a list of biomes and a dictionary for quick
    lookup. When a new biome is encountered, it is added to the list and the
    mapping is updated. The biomes are persisted in a text file to maintain
    consistency across runs. This allows for efficient translation of biome
    palettes to global IDs when extracting region volumes.
    """
    def __init__(self) -> None:
        """
        Initializes the Biomes manager by loading existing biomes from a text
        file or creating the file if it doesn't exist. It maintains a list of
        biome strings and a dictionary mapping biome strings to global IDs for
        efficient lookup. This setup allows for consistent global ID assignment
        for biomes across different regions and runs of the extractor.
        """
        Path("../../assets/").mkdir(parents=True, exist_ok=True)
        self._file_path = "../../assets/biomes.txt"
        Path(self._file_path).touch(exist_ok=True)
        
        with open(self._file_path, "r") as f:
            lines = f.read().splitlines()
            self._biomes = lines
            self._biomes_dict = {biome: i for i, biome in enumerate(lines)}

    def _add_biome(self, biome_str: str) -> int:
        """
        Adds a new biome to the list and dictionary, assigns it a new global ID,
        and persists it to the biomes.txt file. Returns the global ID assigned
        to the new biome.
        """
        new_id = len(self._biomes)
        self._biomes.append(biome_str)
        self._biomes_dict[biome_str] = new_id
        
        with open(self._file_path, "a") as f:
            f.write(f"{biome_str}\n") 
        return new_id

    def to_global_ids(self, biome_indices: np.array, biome_palette: list) -> np.array:
        """
        Translates a 2D array of biome indices (from a chunk's biome layer) and
        its corresponding biome palette into a 2D array of global biome IDs. It
        uses the biome palette to look up the biome string for each index, then
        translates that biome string into a global ID using the internal
        mapping. If a biome string is not found in the mapping, it is added as a
        new entry. This allows for consistent global ID assignment for biomes
        across different regions and runs of the extractor.
        """
        palette_translation = []
        
        for biome_obj in biome_palette:
            biome_str = str(biome_obj)
            
            global_id = self.get_global_id_by_biome(biome_str)
            palette_translation.append(global_id)

        palette_translation = np.array(palette_translation, dtype=np.uint16)
        
        return palette_translation[biome_indices]

    def get_biome_by_global_id(self, id: int) -> str:
        """
        Retrieves the biome string corresponding to a given global ID. If the ID
        is out of bounds, it returns a marker biome string to indicate an
        unknown biome.
        """
        return self._biomes[id]
    
    def get_global_id_by_biome(self, biome_str: str) -> int:
        """
        Retrieves the global ID corresponding to a given biome string. If the
        biome string is not found in the mapping, it is added as a new entry and
        assigned a new global ID.
        """
        if biome_str not in self._biomes_dict:
            return self._add_biome(biome_str)
        return self._biomes_dict[biome_str]

# class Region:
#     def __init__(self, x: int, z: int, region_cube: np.array, inhabited_time: int) -> None:
#         self.x = x
#         self.z = z
#         self.region = region_cube
#         self.inhabited_time = inhabited_time

# class MinecraftRegionExtractor:
#     def __init__(self, directory_path: Path, output_dir: Path) -> None:
#         self.path = directory_path 
#         self.dir_name = directory_path.stem
#         self.world_path = glob.glob(f"{self.path}/**/level.dat", recursive=True)[0]
#         self.world = WorldWrapper(Path(self.world_path))
#         self.output_dir = output_dir
    
#     def extract_regions(self):
#         for world in self.worlds:
#             for coords in world.mca_coords:
#                 inhabited_times = world.mca_inhabited_times(coords[0], coords[1]) 
#                 volume, _ = world.get_region_volume(coords[0], coords[1])
#                 compressed_region = blosc2.pack_array2(np.ascontiguousarray(volume), chunksize=512**3)
#                 with open(self.output_dir / self.dir_name / f"r.{coords[0]}.{coords[1]}.b2frame", "wb") as f:
#                     np.save(self.output_dir / self.dir_name / f"t.{coords[0]}.{coords[1]}.npy", inhabited_times)
#                     f.write(compressed_region)








if __name__ == "__main__":
    extractor = WorldWrapper("/home/erzar/repos/MC/data/warty miasto v13 regular/warty miasto v13 regular")

