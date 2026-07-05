"""Generate Minecraft 3D voxel grid volumes from text prompts using SANA-Video and LoRA.

Usage:
    .venv/bin/python src/scripts/inference_sana_video.py \\
        --prompt "A medieval watchtower built of cobblestone and oak wood" \\
        --lora_path tmp/sana_video_lora/checkpoint-epoch-3 \\
        --output_npy tmp/generated_tower.npy
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import torch
from diffusers import SanaVideoPipeline
from scipy.spatial import cKDTree

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.common.block_colors import load_block_states, load_id2rgb


def parse_args():
    parser = argparse.ArgumentParser(description="Generate 3D Minecraft voxel grids from text using SANA-Video")
    parser.add_argument("--prompt", type=str, required=True, help="Text prompt describing the voxel structure")
    parser.add_argument(
        "--lora_path", type=str, default=None, help="Path to trained LoRA adapter directory (lora mode checkpoints)"
    )
    parser.add_argument(
        "--transformer_path",
        type=str,
        default=None,
        help="Path to fully fine-tuned transformer directory (full mode checkpoints)",
    )
    parser.add_argument(
        "--pretrained_model",
        type=str,
        default="Efficient-Large-Model/SANA-Video_2B_480p_diffusers",
        help="Base SANA-Video model",
    )
    parser.add_argument(
        "--output_npy",
        type=str,
        default="tmp/generated_structure.npy",
        help="Path to save the snapped block ID voxel grid",
    )
    parser.add_argument("--height", type=int, default=512, help="Spatial Height resolution")
    parser.add_argument("--width", type=int, default=512, help="Spatial Width resolution")
    parser.add_argument("--frames", type=int, default=64, help="Voxel vertical layers (Time/Frames axis)")
    parser.add_argument("--block_states", type=str, default=None, help="Path to block_states.txt")
    parser.add_argument("--block_state2rgb", type=str, default=None, help="Path to block_state2rgb.csv")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    return parser.parse_args()


def main():
    args = parse_args()
    torch.manual_seed(args.seed)

    # 1. Load pre-trained SANA-Video pipeline
    print(f"Loading base pipeline from {args.pretrained_model}...")
    pipe = SanaVideoPipeline.from_pretrained(args.pretrained_model, torch_dtype=torch.bfloat16)
    pipe.vae.to(torch.float32)
    pipe.text_encoder.to(torch.bfloat16)
    pipe.to("cuda")

    # 2. Load fine-tuned weights if provided (full transformer or LoRA adapter)
    if args.transformer_path and os.path.exists(args.transformer_path):
        print(f"Loading fully fine-tuned transformer from {args.transformer_path}...")
        transformer_cls = type(pipe.transformer)
        pipe.transformer = transformer_cls.from_pretrained(args.transformer_path, torch_dtype=torch.bfloat16).to("cuda")
    elif args.lora_path and os.path.exists(args.lora_path):
        print(f"Loading LoRA weights adapter from {args.lora_path}...")
        pipe.load_lora_weights(args.lora_path)
    else:
        print("Running with base SANA-Video model weights (no fine-tuned weights).")

    # 3. Run generation to get pseudo-video: (Frames, H, W, 3)
    # Scale inputs, guidance scale, steps
    print(f"Generating video slices for prompt: '{args.prompt}'...")
    with torch.no_grad():
        video = pipe(
            prompt=args.prompt,
            height=args.height,
            width=args.width,
            frames=args.frames,
            guidance_scale=6.0,
            use_resolution_binning=False,  # Keep exact crop dimensions
            num_inference_steps=30,
            generator=torch.Generator(device="cuda").manual_seed(args.seed),
            output_type="np.array",
        ).frames[0]

    # SANA-Video returns numpy array normalized to [0, 1] (float) or [0, 255] (uint8)
    if video.dtype == np.float32 or video.dtype == np.float64:
        video = (video * 255).astype(np.uint8)

    print(f"Generated raw video shape: {video.shape}")  # (Frames, H, W, 3)

    # 4. Spatial Transpose back to Minecraft grid format: (X, Z, Y, 3)
    # Input video shape is (Time=Y, Height=X, Width=Z, 3) -> Transpose to (X, Z, Y, 3)
    spatial_cube = np.transpose(video, (1, 2, 0, 3))
    print(f"Transposed to spatial grid shape: {spatial_cube.shape}")

    # 5. Snap continuous RGB colors back to discrete Minecraft Block IDs
    print("Mapping generated colors back to discrete block IDs via KDTree...")
    states = load_block_states(args.block_states)
    id2rgb, _ = load_id2rgb(args.block_states, args.block_state2rgb)

    # Perform KDTree nearest neighbor snapping
    flat_cube = spatial_cube.reshape(-1, 3)
    tree = cKDTree(id2rgb)
    _, block_ids = tree.query(flat_cube)

    # Reshape back to (X, Z, Y) categorical matrix
    categorical_grid = block_ids.reshape(spatial_cube.shape[:3]).astype(np.uint16)

    # 6. Save the categorical grid
    os.makedirs(os.path.dirname(args.output_npy), exist_ok=True)
    np.save(args.output_npy, categorical_grid)
    print(f"Voxel grid saved to {args.output_npy} (shape={categorical_grid.shape})")

    # Print top generated block states as a simple verification
    unique_ids, counts = np.unique(categorical_grid, return_counts=True)
    print("\nTop generated blocks:")
    sorted_indices = np.argsort(-counts)
    for i in sorted_indices[:10]:
        block_id = unique_ids[i]
        block_count = counts[i]
        block_name = states[block_id] if block_id < len(states) else f"Unknown ({block_id})"
        print(f"  - {block_name}: {block_count} voxels")


if __name__ == "__main__":
    main()
