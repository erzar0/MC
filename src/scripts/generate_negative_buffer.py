"""Build the negative-sampling buffer for block2vec.

Aggregates block frequencies from all `metadata.json` files under the
processed-worlds directory and samples a large buffer of block IDs following
the Word2Vec unigram^0.75 distribution.
"""

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import torch

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src import config

logger = logging.getLogger(__name__)

# Word2Vec-standard smoothing exponent for the unigram distribution
DEFAULT_POWER = 0.75
DEFAULT_BUFFER_SIZE = 100_000_000


def aggregate_frequencies(root_dir: Path) -> Counter:
    """Sums per-block counts from every metadata.json under `root_dir`."""
    total_counts = Counter()
    metadata_files = list(Path(root_dir).rglob("metadata.json"))
    logger.info(f"Found {len(metadata_files)} metadata.json files.")

    for meta_path in metadata_files:
        try:
            with open(meta_path, "r") as f:
                data = json.load(f)
            for region_info in data.get("extracted_regions", {}).values():
                for block_id, count in region_info.get("blocks", {}).items():
                    total_counts[int(block_id)] += int(count)
        except (OSError, json.JSONDecodeError, ValueError) as e:
            logger.error(f"Error processing {meta_path}: {e}")

    return total_counts


def generate_negative_buffer(
    counts: Counter, buffer_size: int = DEFAULT_BUFFER_SIZE, power: float = DEFAULT_POWER
) -> torch.Tensor:
    """Samples `buffer_size` block IDs proportional to frequency^power."""
    ids = np.array(list(counts.keys()))
    freqs = np.array(list(counts.values()), dtype=np.float64)

    freqs = freqs**power
    probs = freqs / freqs.sum()

    logger.info(f"Sampling {buffer_size} IDs based on frequency distribution...")
    negative_buffer = np.random.choice(ids, size=buffer_size, p=probs)

    return torch.from_numpy(negative_buffer.astype(np.int32))


def main():
    parser = argparse.ArgumentParser(description="Generate the block2vec negative-sampling buffer")
    parser.add_argument(
        "--volumes-dir",
        type=Path,
        default=config.PROCESSED_WORLDS_DIR / "cleansed",
        help="Directory scanned recursively for metadata.json files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=config.PROJECT_ROOT / "tmp" / "negative_sampling_buffer.pt",
        help="Output .pt path for the sampled ID buffer",
    )
    parser.add_argument("--buffer-size", type=int, default=DEFAULT_BUFFER_SIZE, help="Number of IDs to sample")
    parser.add_argument("--power", type=float, default=DEFAULT_POWER, help="Unigram distribution smoothing exponent")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    counts = aggregate_frequencies(args.volumes_dir)
    if not counts:
        logger.error(f"No frequencies found under {args.volumes_dir}!")
        raise SystemExit(1)

    buffer = generate_negative_buffer(counts, buffer_size=args.buffer_size, power=args.power)
    logger.info(f"Saving buffer to {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    torch.save(buffer, args.output)
    logger.info("Done.")


if __name__ == "__main__":
    main()
