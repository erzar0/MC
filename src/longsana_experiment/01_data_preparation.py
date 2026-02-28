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
from src.processing_stats import ProcessingStats

def get_dir_size_mb(path: Path) -> float:
    """Calculate the total size of a directory in Megabytes."""
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total += os.path.getsize(fp)
    return total / (1024 * 1024)

def main():
    downloads_dir = config.DOWNLOADS_DIR
    if not downloads_dir.exists():
        print(f"Downloads directory not found: {downloads_dir}")
        return

    processor = WorldProcessor()
    stats = ProcessingStats()
    
    for world_dir in downloads_dir.iterdir():
        if not world_dir.is_dir():
            continue
            
        world_name = world_dir.name
        
        # Calculate world size before processing
        world_size_mb = get_dir_size_mb(world_dir)
        
        print(f"\n" + "="*50)
        print(f"Starting pipeline for world: {world_name} ({world_size_mb:.2f} MB)")
        print("="*50)
        
        # 1. Process World using WorldProcessor
        result = processor.process_world(world_dir, world_name)
        
        caption_stats = {
            "total": 0, "generated": 0, "skipped": 0, "failed": 0
        }
        
        if result.get("status") == "success":
            cleansed_dir = Path(result["cleansed_dir"])
            screenshots_dir = cleansed_dir / "screenshots"
            captions_dir = cleansed_dir / "captions"
            
            # 2. Generate Captions
            if screenshots_dir.exists():
                print(f"Processing screenshots from: {screenshots_dir}")
                print(f"Saving captions to: {captions_dir}")
                
                caption_stats = process_screenshots_folder(str(screenshots_dir), str(captions_dir))
            else:
                print(f"Screenshots directory not found at {screenshots_dir}. Skipping caption generation.")
        else:
            print(f"World processing failed for {world_name}: {result.get('error')}")

        stats.record_world(
            world_name=world_name,
            status=result.get("status", "failed"),
            error=result.get("error", ""),
            world_size_mb=world_size_mb,
            volumes_count=result.get("volumes_count", 0),
            screenshots_count=result.get("screenshots_count", 0),
            captions_total=caption_stats.get("total", 0),
            captions_generated=caption_stats.get("generated", 0),
            captions_skipped=caption_stats.get("skipped", 0),
            captions_failed=caption_stats.get("failed", 0)
        )

    # Print final summary table
    stats.print_summary()

if __name__ == "__main__":
    main()

