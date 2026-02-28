from pathlib import Path
import os
import sys

# Ensure src is in the path for importing
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src import config
from src.world_processor import WorldProcessor
from src.generate_captions import process_screenshots_folder

def main():
    # 1. Process World using WorldProcessor
    world_source_path = config.DEFAULT_WORLD_SOURCE
    world_name = config.DEFAULT_WORLD_NAME
    
    processor = WorldProcessor()
    print(f"Starting world processing for {world_name}...")
    result = processor.process_world(world_source_path, world_name)
    
    if result.get("status") == "success":
        cleansed_dir = Path(result["cleansed_dir"])
        screenshots_dir = cleansed_dir / "screenshots"
        captions_dir = cleansed_dir / "captions"
        
        # 2. Generate Captions
        if screenshots_dir.exists():
            print(f"Processing screenshots from: {screenshots_dir}")
            print(f"Saving captions to: {captions_dir}")
            
            process_screenshots_folder(str(screenshots_dir), str(captions_dir))
        else:
            print(f"Screenshots directory not found at {screenshots_dir}. Skipping caption generation.")
    else:
        print(f"World processing failed: {result.get('error')}")

if __name__ == "__main__":
    main()

