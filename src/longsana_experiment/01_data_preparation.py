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
    downloads_dir = config.DOWNLOADS_DIR
    if not downloads_dir.exists():
        print(f"Downloads directory not found: {downloads_dir}")
        return

    processor = WorldProcessor()
    
    for world_dir in downloads_dir.iterdir():
        if not world_dir.is_dir():
            continue
            
        world_name = world_dir.name
        print(f"\n" + "="*50)
        print(f"Starting pipeline for world: {world_name}")
        print("="*50)
        
        # 1. Process World using WorldProcessor
        result = processor.process_world(world_dir, world_name)
        
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
            print(f"World processing failed for {world_name}: {result.get('error')}")

if __name__ == "__main__":
    main()

