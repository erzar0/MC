from concurrent.futures import as_completed
from pathlib import Path
from typing import List
from tqdm import tqdm
import glob 
import pympler
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from itertools import product
from batching import batch_n
import amulet
import numpy as np
from pprint import pprint

class Region:
    def __init__(self, x: int, z: int, region_cube: np.array, inhabited_time: int) -> None:
        self.x = x
        self.z = z
        self.region = region_cube
        self.inhabited_time = inhabited_time

class WorldWrapper:
    def __init__(self, world_path: Path) -> None:
        self.world = amulet.load_level(world_path)
        self.mca_files = glob.glob(f"{world_path}/**/region/*.mca", recursive=True)
        self.mca_coords = [file.stem.split(".")[-2:] for file in map(Path, self.mca_files)]
        self.chunk_coords = self.world.all_chunk_coords("minecraft:overworld")
    
    def misc_keys(self) -> None:
        x, z = list(self.chunk_coords)[0]
        return self.world.get_chunk(x, z, "minecraft:overworld").misc.keys()
    
    def get_region_volume(self, region_x: int, region_z: int) -> np.array:
        sections = []
        num_sections_per_chunk = 0
        
        for rx in range(32):
            for rz in range(32):
                chunk_coords = self._to_chunk_coords(region_x, region_z, rx, rz)
                chunk = self.world.get_chunk(chunk_coords["x"], chunk_coords["z"], "minecraft:overworld")
                
                sub_indices = sorted(chunk.blocks.sub_chunks)
                num_sections_per_chunk = len(sub_indices)
                
                for y in sub_indices:
                    sections.append(chunk.blocks.get_sub_chunk(y))

        data = np.array(sections)
        
        data = data.reshape(32, 32, num_sections_per_chunk, 16, 16, 16)
        
        data = data.transpose(0, 3, 1, 4, 2, 5)
        
        height = num_sections_per_chunk * 16
        return data.reshape(512, 512, height)
    
    def get_inhabited_times(self, region_x: int, region_z: int) -> List[int]:
        for x, z in self.mca_coords:
            if int(x) == region_x and int(z) == region_z:
                return self._mca_inhabited_times(region_x, region_z)
    
    def _mca_inhabited_times(self, region_x: int, region_z: int) -> List[int]:
        inhabited_times = [[0 for _ in range(32)] for _ in range(32)]
        for region_x_offset in range(32):
            for region_z_offset in range(32):
                chunk_coords = self._to_chunk_coords(region_x, region_z, region_x_offset, region_z_offset)
                chunk_x, chunk_z = chunk_coords["x"], chunk_coords["z"]
                if (chunk_x, chunk_z) in self.chunk_coords:
                    inhabited_times[region_x_offset][region_z_offset] += self._chunk_inhibited_time(chunk_x, chunk_z)
        return inhabited_times

    def _chunk_inhibited_time(self, x: int, z: int) -> int:
        chunk = self.world.get_chunk(x, z, "minecraft:overworld")
        if not chunk.misc:
            return 0
        return chunk.misc.get("inhabited_time")
    
    @staticmethod
    def _to_mca_coords(chunk_x: int, chunk_z: int) -> dict:
        region_x = chunk_x // 32
        region_z = chunk_z // 32
        region_x_offset = chunk_x % 32
        region_z_offset = chunk_z % 32
        return {"x": region_x, "z": region_z, "x_offset": region_x_offset, "z_offset": region_z_offset}
    
    @staticmethod
    def _to_chunk_coords(region_x: int, region_z: int, region_x_offset: int, region_z_offset: int) -> dict:
        chunk_x = region_x * 32 + region_x_offset
        chunk_z = region_z * 32 + region_z_offset
        return {"x": chunk_x, "z": chunk_z}


class MinecraftRegionExtractor:
    def __init__(self, directory_path) -> None:
        self.path = directory_path 
        self.world_paths = glob.glob(f"{self.path}/**/level.dat", recursive=True)
        self.worlds = [WorldWrapper(Path(p).parent) for p in self.world_paths]


if __name__ == "__main__":
    extractor = WorldWrapper("/home/erzar/repos/MC/data/warty miasto v13 regular/warty miasto v13 regular")

