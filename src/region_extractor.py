import anvil 
import glob 
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

def _load_region_worker(region_file_str: str):
    from pathlib import Path
    import anvil

    region_file = Path(region_file_str)
    x, z = map(int, region_file.stem.split('.')[1:3])

    region = anvil.Region.from_file(region_file.as_posix())
    return (x, z, region) 


class RegionWrapper():
    def __init__(self, x:int, z: int, region: anvil.Region):
        self.x = x
        self.z = z
        self.region = region
    
    def print_keys(self):
        print(self.region.chunk_data(0, 0).pretty_tree())
    
    def _get_inhabited_time(self, x: int, z: int) -> int:
        chunk = self.region.chunk_data(x, z)
        if chunk is None:
            return 0
        return chunk["Level"]["InhabitedTime"].value or 0
    
    def get_inhabited_times(self) -> list[int]:
        return [self._get_inhabited_time(cx, cz) for cx in range(32) for cz in range(32)]


class MinecraftRegionExtractor():
    def __init__(self, path):
        self.path = path
    
    def _extract_region_paths(self) -> list[Path]:
        return [
            Path(path)
            for path in glob.glob(f"{self.path}/**/region/*.mca", recursive=True)
        ]
    
    def get_regions(self, workers: int | None = None) -> list["RegionWrapper"]:
        region_files = sorted(self._extract_region_paths())

        region_paths = [str(p) for p in region_files]

        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_load_region_worker, region_paths))

        regions = [RegionWrapper(x, z, region) for (x, z, region) in results]
        return regions

if __name__ == "__main__":
    extractor = MinecraftRegionExtractor("/home/kyre/repos/minecraft-world-generator/data/RUNETALE Converged Realms")
    regions = extractor.get_regions()
    for region in regions:
        inhibited_times = region.get_inhabited_times()
        print(f"Region ({region.x}, {region.z}) - Average Inhabited Time: {sum(inhibited_times)/len(inhibited_times)}")

