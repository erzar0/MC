"""Build a training manifest (JSONL) for SANA-Video voxel grid training.

Scans all cleansed worlds under `tmp/processed_worlds/cleansed/` and pairs each
volume (.b2frame) with its available caption orientations (.txt).
Writes a single `sana_video_manifest.jsonl` file.
"""

import os
import json
from pathlib import Path

def build_manifest(cleansed_dir: str, output_path: str):
    cleansed_path = Path(cleansed_dir)
    if not cleansed_path.exists():
        print(f"Directory {cleansed_dir} does not exist.")
        return

    manifest_entries = []
    
    # 1. Iterate over all cleansed world folders (e.g. 1015758, etc.)
    world_dirs = [d for d in cleansed_path.iterdir() if d.is_dir() and d.name != "0_regions"]
    print(f"Scanning {len(world_dirs)} world directories...")

    for world_dir in world_dirs:
        volumes_dir = world_dir / "volumes"
        captions_dir = world_dir / "captions"
        
        if not volumes_dir.exists() or not captions_dir.exists():
            continue
            
        b2frames = list(volumes_dir.glob("*.b2frame"))
        for b2_path in b2frames:
            # Filename is e.g. 'r.-1.-1.b2frame'
            region_name = b2_path.stem  # 'r.-1.-1'
            
            # Find matching captions for the 4 orientations
            orientations = ["ne", "nw", "se", "sw"]
            captions = {}
            
            for orient in orientations:
                cap_file = captions_dir / f"{region_name}.{orient}.txt"
                if cap_file.exists():
                    try:
                        with open(cap_file, "r") as f:
                            cap_text = f.read().strip()
                            if cap_text:
                                captions[orient] = cap_text
                    except Exception as e:
                        print(f"Error reading {cap_file}: {e}")
            
            # If we have at least one caption, keep it
            if captions:
                # Store absolute paths to avoid resolving path relative issues in dataloader
                entry = {
                    "volume_path": str(b2_path.resolve()),
                    "captions": captions
                }
                manifest_entries.append(entry)

    # Write JSONL manifest
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w") as f:
        for entry in manifest_entries:
            f.write(json.dumps(entry) + "\n")
            
    print(f"Manifest written successfully to {output_path}")
    print(f"Total regions in manifest: {len(manifest_entries)}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate JSONL manifest for SANA-Video voxel grid training")
    parser.add_argument("--cleansed_dir", type=str, default="tmp/processed_worlds/cleansed", help="Cleansed data directory")
    parser.add_argument("--output", type=str, default="tmp/sana_video_manifest.jsonl", help="Output JSONL manifest path")
    args = parser.parse_args()

    build_manifest(args.cleansed_dir, args.output)
