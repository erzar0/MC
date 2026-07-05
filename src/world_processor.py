import json
import logging
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Tuple, Union

import blosc2
import numpy as np
from PIL import Image
from tqdm import tqdm

try:
    # Registers the JXL codec with PIL as an import side effect
    import pillow_jxl  # noqa: F401
except ImportError:
    pass
# Ensure src is in path for imports if run as main
import sys

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src import config
from src.world_wrapper import WorldWrapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# An MCA region spans 512x512 blocks
REGION_BLOCKS = 512
# Post-1.18 Java worlds span y in [-64, 320], i.e. 384 blocks
MAX_WORLD_HEIGHT = 384
# Give the 1.6.4 conversion server at most 30 minutes to start up
SERVER_STARTUP_TIMEOUT_S = 1800


class WorldProcessor:
    """
    Processes Minecraft worlds through a conversion and extraction pipeline:
    1. MCR to MCA conversion (really old versions -> Minecraft 1.6.4)
    2. Version update to 1.19.2 (Chunker)
    3. Extraction of MCA, level.data, metadata and 3D volumes (WorldWrapper)
    4. Rendering screenshots (mcmap)
    """

    def __init__(
        self,
        server_dir: Union[str, Path] = config.SERVER_DIR,
        chunker_bin: Union[str, Path] = config.CHUNKER_BIN,
        mcmap_bin: Union[str, Path] = config.MCMAP_BIN,
        output_dir: Union[str, Path] = config.PROCESSED_WORLDS_DIR,
    ):
        self.project_root = config.PROJECT_ROOT
        self.server_dir = Path(server_dir).absolute()
        self.chunker_bin = Path(chunker_bin).absolute()
        self.mcmap_bin = Path(mcmap_bin).absolute()
        self.output_dir = Path(output_dir).absolute()

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_world(self, world_path: Path, world_name: str, remove_tmp_dirs: bool = False):
        """Orchestrates the processing of a single world."""
        world_path = Path(world_path).absolute()
        logger.info(f"--- Starting processing for world: {world_name} ---")

        try:
            # 1. Convert MCR to MCA using 1.6.4 server
            converted_world_path = self._convert_mcr_to_mca(world_path, world_name)

            # 2. Update version to 1.19.2 using Chunker
            version_updated_path = self.output_dir / "versions" / world_name / "1_19_2"
            self._update_with_chunker(converted_world_path, version_updated_path)

            # 3. Extract regions and volumes
            cleansed_dir = self.output_dir / "cleansed" / world_name
            extracted_regions, world_metadata = self._extract_world_data(version_updated_path, cleansed_dir)

            # 4. Generate screenshots
            screenshots_dir = cleansed_dir / "screenshots"
            screenshots_dir.mkdir(parents=True, exist_ok=True)
            self._generate_screenshots(
                cleansed_dir, screenshots_dir, extracted_regions, world_metadata["extracted_regions"]
            )

            volumes_count = (
                len(list((cleansed_dir / "volumes").glob("*.b2frame"))) if (cleansed_dir / "volumes").exists() else 0
            )
            screenshots_count = len(list(screenshots_dir.glob("*.png"))) if screenshots_dir.exists() else 0

            if remove_tmp_dirs:
                if converted_world_path != world_path:
                    shutil.rmtree(converted_world_path)
                shutil.rmtree(version_updated_path.parent)

            logger.info(f"--- Finished processing world: {world_name} ---")
            return {
                "status": "success",
                "world_name": world_name,
                "output_dir": str(cleansed_dir),
                "cleansed_dir": str(cleansed_dir),
                "volumes_count": volumes_count,
                "screenshots_count": screenshots_count,
            }
        except Exception as e:
            logger.error(f"Failed to process world {world_name}: {e}")
            return {"status": "failed", "world_name": world_name, "error": str(e)}

    def _convert_mcr_to_mca(self, world_path: Path, world_name: str) -> Path:
        """Copies world to server dir, converts it, and returns the path to converted world."""
        logger.info(f"Step 1: Converting {world_name} from MCR to MCA using 1.6.4 server...")

        if not list(world_path.glob("region/*.mcr")):
            logger.info(f"World {world_name} does not contain .mcr files. Skipping conversion.")
            return world_path

        if not (self.server_dir / "server.jar").exists():
            logger.warning(f"server.jar not found in {self.server_dir}. Skipping MCR to MCA conversion.")
            return world_path

        # Target path in server directory
        server_world_path = self.server_dir / world_name
        if server_world_path.exists():
            shutil.rmtree(server_world_path)

        # Ensure server directory exists before copying
        self.server_dir.mkdir(parents=True, exist_ok=True)
        shutil.copytree(world_path, server_world_path)

        # Update server.properties
        props_path = self.server_dir / "server.properties"
        if props_path.exists():
            with open(props_path, "r") as f:
                content = f.read()

            # Replace level-name
            content = re.sub(r"level-name=.*", f"level-name={world_name}", content)

            with open(props_path, "w") as f:
                f.write(content)
        else:
            logger.warning(f"server.properties not found at {props_path}")

        # Run server
        # nogui is essential. We monitor logs for completion.
        process = subprocess.Popen(
            ["java", "-Xmx1G", "-Xms1G", "-jar", "server.jar", "nogui"],
            cwd=self.server_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        logger.info("Server started, waiting for conversion signal...")
        try:
            # We wait for "Done" or "Preparing spawn area"
            # 1.6.4 often shows "Preparing spawn area" or "Done (X.Xs)!"
            # We timeout after 30 minutes just in case.
            start_time = time.time()
            for line in iter(process.stdout.readline, ""):
                line_str = line.strip()
                if line_str:
                    logger.info(f"[Server] {line_str}")

                if "Done" in line_str or "Preparing spawn area" in line_str:
                    logger.info("Conversion trigger detected. Stopping server.")
                    break

                if (
                    "This crash report has been saved to" in line_str
                    or "Encountered an unexpected exception" in line_str
                ):
                    raise RuntimeError(f"Minecraft server crashed during MCR to MCA conversion: {line_str}")

                if time.time() - start_time > SERVER_STARTUP_TIMEOUT_S:
                    logger.warning("Server startup timed out. Proceeding anyway.")
                    break
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

        return server_world_path

    def _update_with_chunker(self, input_dir: Path, output_dir: Path):
        """Updates world version to 1.19.2 using chunker-cli."""
        logger.info(f"Step 2: Updating world version to 1.19.2 at {output_dir}...")
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        settings = {
            "customIdentifiers": False,
            "blockConnections": False,
            "itemConversion": False,
            "lootTableConversion": False,
            "mapConversion": False,
        }

        if not self.chunker_bin.exists():
            logger.warning(
                f"Chunker CLI not found at {self.chunker_bin}. Skipping version update and copying files directly."
            )
            shutil.copytree(input_dir, output_dir, dirs_exist_ok=True)
            return

        cmd = [
            str(self.chunker_bin),
            "--inputDirectory",
            str(input_dir),
            "--outputDirectory",
            str(output_dir),
            "--outputFormat",
            "JAVA_1_19_2",
            "--converterSettings",
            json.dumps(settings, separators=(",", ":")),
        ]

        logger.info(f"Running Chunker: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Chunker failed: {result.stderr}")
            raise RuntimeError(f"Chunker failed with exit code {result.returncode}")

        logger.info("Chunker conversion successful.")

    def _extract_world_data(self, world_path: Path, output_dir: Path) -> Tuple[List[Tuple[int, int]], dict]:
        """Extracts MCA files and region volumes.

        Returns:
            Tuple of (list of extracted region coordinates, world metadata dict).
        """
        logger.info(f"Step 3: Extracting world data (MCA and volumes) to {output_dir}...")

        # 1. Extract volumes using WorldWrapper
        extracted_regions = []
        try:
            wrapper = WorldWrapper(world_path)
            volumes_dir = output_dir / "volumes"
            volumes_dir.mkdir(parents=True, exist_ok=True)

            coords = wrapper.mca_coords
            logger.info(f"Found {len(coords)} regions to extract.")

            pbar = tqdm(coords, desc="Extracting regions")
            for rx, rz in pbar:
                pbar.set_postfix({"region": f"r.{rx}.{rz}"})
                try:
                    volume = wrapper.get_region_volume(rx, rz, True)

                    # Save volume
                    compressed_region = blosc2.pack_array2(np.ascontiguousarray(volume), chunksize=512**3)
                    logger.debug(
                        f"Compressed size for r.{rx}.{rz}: {len(compressed_region) / (1024**2):.2f} MB. Compression ratio: {volume.nbytes / len(compressed_region):.2f}x. Shape: {volume.shape}. Array type: {volume.dtype}. "
                    )
                    with open(volumes_dir / f"r.{rx}.{rz}.b2frame", "wb") as f:
                        f.write(compressed_region)

                    extracted_regions.append((rx, rz))

                    # Clear Amulet chunk cache to prevent OOM
                    wrapper.unload()
                except Exception as ve:
                    logger.error(f"Failed to extract region ({rx}, {rz}): {ve}")

            wrapper.unload()
        except Exception as e:
            logger.error(f"Error initializing WorldWrapper: {e}")
            raise

        # 2. Extract MCA and level.dat to cleansed directory (for mcmap)
        region_dir = world_path / "region"
        cleansed_mca_dir = output_dir / "region"
        cleansed_mca_dir.mkdir(parents=True, exist_ok=True)

        if region_dir.exists():
            for mca_file in wrapper.mca_paths:
                shutil.copy(mca_file, cleansed_mca_dir)
        else:
            logger.warning(f"Region directory not found in updated world: {region_dir}")

        level_dat_path = world_path / "level.dat"
        if level_dat_path.exists():
            shutil.copy(level_dat_path, output_dir / "level.dat")
        else:
            logger.warning(f"level.dat not found in updated world: {level_dat_path}")

        # 3. Save world metadata
        with open(output_dir / "metadata.json", "w") as f:
            json.dump(wrapper.metadata, f, indent=4)

        return extracted_regions, wrapper.metadata

    ORIENTATIONS = ["nw", "ne", "se", "sw"]

    def _generate_screenshots(
        self, world_path: Path, output_dir: Path, region_coords: List[Tuple[int, int]], regions_metadata: dict
    ):
        """Generates screenshots for regions using mcmap in all 4 orientations."""
        total = len(region_coords) * len(self.ORIENTATIONS)
        logger.info(
            f"Step 4: Generating screenshots for {len(region_coords)} regions x {len(self.ORIENTATIONS)} orientations ({total} total)..."
        )

        if not self.mcmap_bin.exists():
            logger.warning(f"mcmap binary not found at {self.mcmap_bin}. Skipping screenshot generation.")
            return

        pbar = tqdm(region_coords, desc="Generating screenshots")
        for rx, rz in pbar:
            # mcmap takes coordinates in blocks
            x_from, z_from = rx * REGION_BLOCKS, rz * REGION_BLOCKS
            x_to, z_to = x_from + REGION_BLOCKS, z_from + REGION_BLOCKS
            y_from, y_to = regions_metadata[f"{rx},{rz}"]["y_range"]

            for orientation in self.ORIENTATIONS:
                pbar.set_postfix({"region": f"r.{rx}.{rz}", "orientation": orientation})

                screenshot_path = output_dir / f"r.{rx}.{rz}.{orientation}.png"

                cmd = [
                    str(self.mcmap_bin),
                    f"-{orientation}",
                    "-from",
                    str(x_from),
                    str(z_from),
                    "-to",
                    str(x_to),
                    str(z_to),
                    "-min",
                    str(y_from),
                    "-max",
                    str(MAX_WORLD_HEIGHT),
                    "-fragment",
                    str(REGION_BLOCKS),
                    "-padding",
                    "0",
                    "-dim",
                    "overworld",
                    "-nobeacons",
                    "-shading",
                    "-lighting",
                    "-file",
                    str(screenshot_path),
                    str(world_path),
                ]

                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    logger.warning(f"mcmap failed for r.{rx}.{rz} ({orientation}): {result.stderr}")
                    continue

                # Crop transparent pixels from top/bottom and Compress to JXL
                try:
                    with Image.open(screenshot_path) as img:
                        base_img = img.convert("RGBA")

                    # Crop transparent pixels from top and bottom
                    alpha = base_img.split()[-1]
                    bbox = alpha.getbbox()
                    if bbox:
                        _, upper, _, lower = bbox
                        base_img = base_img.crop((0, upper, base_img.width, lower))

                    jxl_path = screenshot_path.with_suffix(".jxl")
                    # Using JXL for ML Datasets
                    base_img.save(jxl_path, quality=70, effort=3)

                    # Delete the original PNG image
                    if screenshot_path.exists():
                        os.remove(screenshot_path)
                except Exception as e:
                    logger.warning(f"Failed to process screenshot for r.{rx}.{rz} ({orientation}): {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process a Minecraft world.")
    parser.add_argument("world_source_path", help="Path to the world source directory.")
    parser.add_argument("world_name", help="Name of the world.", nargs="?", default=config.DEFAULT_WORLD_NAME)
    args = parser.parse_args()

    world_name = args.world_name or Path(args.world_source_path).name
    processor = WorldProcessor()
    processor.process_world(Path(args.world_source_path), world_name, remove_tmp_dirs=True)
