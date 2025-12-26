from concurrent.futures import as_completed
from pathlib import Path
from typing import List
from tqdm import tqdm
import glob 
import anvil
import pympler
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from itertools import product
from batching import batch_n


class RegionWrapper:
    def __init__(self, path: Path) -> None:
        x, z = map(int, path.stem.split('.')[1:3])
        region = anvil.Region.from_file(path.as_posix())
        self.x = x
        self.z = z
        self.region = region
        self.path = path

    def print_keys(self) -> None:
        print(self.region.chunk_data(0, 0).pretty_tree())

    def get_inhabited_times(self) -> List[int]:
        return [self._get_inhabited_time(x, z) for x, z in product(range(32), range(32))]

    def get_size(self, unit="MB") -> float:
        if unit == "MB":
            return pympler.asizeof.asizeof(self) / (1024 ** 2)
        else:
            return pympler.asizeof.asizeof(self)

    def _get_inhabited_time(self, x: int, z: int) -> int:
        self.region.get_chunk(0, 0)
        chunk = self.region.chunk_data(x, z)
        if chunk is None:
            return 0
        return chunk["Level"]["InhabitedTime"].value or 0


class MinecraftRegionExtractor:
    def __init__(self, path) -> None:
        self.path = path

    def get_regions(self, workers: int = multiprocessing.cpu_count()) -> list[RegionWrapper]:
        region_files = self.extract_region_paths()

        results = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(MinecraftRegionExtractor._load_region_worker, batch) for batch in batch_n(region_files, workers)]

            with tqdm(total=len(region_files), desc="Loading regions") as pbar:
                for future in as_completed(futures):
                    batch_result = future.result()
                    results.extend(batch_result)
                    pbar.update(len(batch_result))

        return results

    def extract_region_paths(self) -> list[Path]:
        return [
            Path(path)
            for path in glob.glob(f"{self.path}/**/region/*.mca", recursive=True)
        ]

    @staticmethod
    def _load_region_worker(region_file_paths: list[Path]) -> list[tuple[int, int, anvil.Region]]:
        """Worker for processing a batch of region files."""

        results = []
        for region_file_path in region_file_paths:
            results.append(RegionWrapper(region_file_path))
        return results


if __name__ == "__main__":
    extractor = MinecraftRegionExtractor("/home/kyre/repos/minecraft-world-generator/data/RUNETALE Converged Realms")
    regions = extractor.get_regions()
    for region in regions:
        inhibited_times = region.get_inhabited_times()
        print(f"Region ({region.x}, {region.z}) - Average Inhabited Time: {sum(inhibited_times)/len(inhibited_times)}")

