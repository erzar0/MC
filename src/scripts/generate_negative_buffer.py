import json
import os
from collections import Counter
from pathlib import Path

import numpy as np
import torch


def aggregate_frequencies(root_dir):
    total_counts = Counter()
    metadata_files = list(Path(root_dir).rglob("metadata.json"))
    print(f"Found {len(metadata_files)} metadata.json files.")

    for meta_path in metadata_files:
        try:
            with open(meta_path, "r") as f:
                data = json.load(f)
                extracted_regions = data.get("extracted_regions", {})
                for region_coords, region_info in extracted_regions.items():
                    blocks = region_info.get("blocks", {})
                    for block_id, count in blocks.items():
                        total_counts[int(block_id)] += int(count)
        except Exception as e:
            print(f"Error processing {meta_path}: {e}")

    return total_counts


def generate_negative_buffer(counts, buffer_size=100_000_000, power=0.75):
    # Convert counts to arrays for sampling
    ids = np.array(list(counts.keys()))
    freqs = np.array(list(counts.values()), dtype=np.float64)

    # Apply power law (Word2Vec standard)
    freqs = freqs**power
    probs = freqs / freqs.sum()

    print(f"Sampling {buffer_size} IDs based on frequency distribution...")
    # Using numpy for initial sampling
    negative_buffer = np.random.choice(ids, size=buffer_size, p=probs)

    return torch.from_numpy(negative_buffer.astype(np.int32))


if __name__ == "__main__":
    root = "/home/kyre/repos/minecraft-world-generator/tmp/processed_worlds/cleansed"
    output_path = "/home/kyre/repos/minecraft-world-generator/tmp/negative_sampling_buffer.pt"

    counts = aggregate_frequencies(root)
    if not counts:
        print("No frequencies found!")
    else:
        buffer = generate_negative_buffer(counts)
        print(f"Saving buffer to {output_path}")
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        torch.save(buffer, output_path)
        print("Done.")
