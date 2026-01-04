from amulet.api.block import Block
from amulet.api.registry import BlockManager
from itertools import product
from networkx import volume
from pathlib import Path
from typing import List
import amulet
import anvil
import glob 
import numpy as np
import re



class WorldWrapper:
    def __init__(self, world_path: Path) -> None:
        self._world = amulet.load_level(world_path)
        self._mca_files = glob.glob(f"{world_path}/**/region/*.mca", recursive=True)
        self._mca_coords = tuple(tuple(int(val) for val in file.stem.split(".")[-2:]) for file in map(Path, self._mca_files))
        self._mca_coord_to_path = {(x, z): path for (x, z), path in zip(self._mca_coords, self._mca_files)}
        self._chunk_coords = tuple(self._world.all_chunk_coords("minecraft:overworld"))
        self._blockstates = BlockStates()
    
    @property
    def mca_coords(self) -> List[tuple]:
        return self._mca_coords

    @property
    def chunk_coords(self) -> List[tuple]:
        return self._chunk_coords
    
    def misc_keys(self) -> None:
        x, z = list(self._chunk_coords)[0]
        return self._world.get_chunk(x, z, "minecraft:overworld").misc.keys()
    
    def get_region_volume(self, region_x: int, region_z: int) -> np.array:
        if (region_x, region_z) not in self._mca_coords:
            raise ValueError(f"Region ({region_x}, {region_z}) not found in world.")

        bounds = self._world.bounds("minecraft:overworld")
        height = (bounds.max_y - bounds.min_y)

        volume_6d = np.zeros((32, 32, height // 16 + 1, 16, 16, 16), dtype=np.uint16)

        for rx in range(32):
            for rz in range(32):
                chunk_coords = self.to_chunk_coords(region_x, region_z, rx, rz)
                try:
                    chunk = self._world.get_chunk(chunk_coords["x"], chunk_coords["z"], "minecraft:overworld")
                except:
                    continue

                y_sections = sorted(chunk.blocks.sections)
                for i, y in enumerate(y_sections[:24]):
                    sub_chunk = chunk.blocks.get_sub_chunk(y)
                    palette = chunk._block_palette
                    volume_6d[rx, rz, i] = self._blockstates.to_global_ids(sub_chunk, palette)
                    
        data = volume_6d.transpose(0, 3, 1, 5, 2, 4)
        return self._trim_y_axis(data.reshape(512, 512, -1))

    def _trim_y_axis(self, volume: np.array) -> np.array:
        has_content = np.any(volume != 0, axis=(0, 1))
        if not np.any(has_content):
            return volume
        
        indices = np.where(has_content)[0]
        return volume[:, :, indices[0] : indices[-1] + 1]
    
    def mca_inhabited_times(self, region_x: int, region_z: int) -> np.array:
        region = anvil.Region.from_file(self._mca_coord_to_path[(region_x, region_z)])

        chunks = [region.chunk_data(x, z) for x, z in product(range(32), range(32))]

        inhabited_times = np.array([chunk["Level"]["InhabitedTime"].value if chunk is not None else 0 for chunk in chunks]).reshape(32, 32)

        return inhabited_times

    def chunk_inhibited_time(self, x: int, z: int) -> int:
        return self._world.get_chunk(x, z, "minecraft:overworld").misc.get("InhabitedTime", 0)

    @staticmethod
    def to_mca_coords(chunk_x: int, chunk_z: int) -> dict:
        region_x = chunk_x // 32
        region_z = chunk_z // 32
        region_x_offset = chunk_x % 32
        region_z_offset = chunk_z % 32
        return {"x": region_x, "z": region_z, "x_offset": region_x_offset, "z_offset": region_z_offset}
    
    @staticmethod
    def to_chunk_coords(region_x: int, region_z: int, region_x_offset: int, region_z_offset: int) -> dict:
        chunk_x = region_x * 32 + region_x_offset
        chunk_z = region_z * 32 + region_z_offset
        return {"x": chunk_x, "z": chunk_z}

class BlockStates:
    def __init__(self) -> None:
        Path("../../assets/").mkdir(parents=True, exist_ok=True)
        Path("../../assets/blockstates.txt").touch(exist_ok=True)
        with open("../../assets/blockstates.txt", "r") as f:
            lines = f.read().splitlines()
            self._blockstates = lines
            self._blockstates_dict = {state: i for i, state in enumerate(lines)}

    def _add_blockstate(self, blockstate: str) -> int:
        new_id = len(self._blockstates)
        self._blockstates.append(blockstate)
        self._blockstates_dict[blockstate] = new_id
        
        with open("../../assets/blockstates.txt", "a") as f:
            f.write(f"{blockstate}\n") 
        return new_id

    def to_global_ids(self, blocks_array: np.array, block_palette: BlockManager) -> np.array:
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
        return self._blockstates[id]
    
    def get_global_id_by_block(self, blockstate: str) -> int:
        if blockstate not in self._blockstates_dict:
            return self._add_blockstate(blockstate)
        return self._blockstates_dict[blockstate]

class MinecraftRegionExtractor:
    def __init__(self, directory_path) -> None:
        self.path = directory_path 
        self.world_paths = glob.glob(f"{self.path}/**/level.dat", recursive=True)
        self.worlds = [WorldWrapper(Path(p).parent) for p in self.world_paths]

class Region:
    def __init__(self, x: int, z: int, region_cube: np.array, inhabited_time: int) -> None:
        self.x = x
        self.z = z
        self.region = region_cube
        self.inhabited_time = inhabited_time

if __name__ == "__main__":
    extractor = WorldWrapper("/home/erzar/repos/MC/data/warty miasto v13 regular/warty miasto v13 regular")

