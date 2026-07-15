"""End-to-end generation: prompt -> voxel grid -> playable world -> render -> Google Drive.

Runs SANA-Video inference from a trained checkpoint (or loads an existing .npy
grid), builds a playable Java 1.19.2 world from the block-ID grid, renders a PNG
with mcmap, zips the world, and uploads the zip + render to Google Drive via
rclone (same remote convention as deploy/remote_train.sh).

Usage:
    .venv/bin/python src/scripts/sana_video/generate_world.py \\
        --prompt "A medieval watchtower built of cobblestone and oak wood" \\
        --lora_path tmp/sana_video_ft/checkpoint-epoch-3

    # Reuse an existing grid, no upload:
    .venv/bin/python src/scripts/sana_video/generate_world.py \\
        --npy tmp/generated_structure.npy --skip-upload
"""

import argparse
import json
import logging
import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src import config
from src.common.block_colors import load_id2rgb
from src.world.world_builder import build_world

logger = logging.getLogger(__name__)

DEFAULT_REMOTE = "gdrive:minecraft-training/generated"


def parse_args():
    parser = argparse.ArgumentParser(description="Generate a playable Minecraft world from a text prompt")
    parser.add_argument("--prompt", type=str, default=None, help="Text prompt (required unless --npy is given)")
    parser.add_argument("--npy", type=str, default=None, help="Existing block-ID grid .npy; skips inference")
    parser.add_argument("--lora_path", type=str, default=None, help="Trained LoRA adapter directory")
    parser.add_argument("--transformer_path", type=str, default=None, help="Fully fine-tuned transformer directory")
    parser.add_argument(
        "--pretrained_model",
        type=str,
        default="Efficient-Large-Model/SANA-Video_2B_480p_diffusers",
        help="Base SANA-Video model",
    )
    parser.add_argument("--height", type=int, default=512, help="Spatial Height resolution (X)")
    parser.add_argument("--width", type=int, default=512, help="Spatial Width resolution (Z)")
    parser.add_argument("--frames", type=int, default=65, help="Voxel vertical layers (Y); must be 4n+1")
    parser.add_argument("--guidance_scale", type=float, default=6.0, help="CFG scale (1.0 disables CFG)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--name", type=str, default=None, help="Run name (default: generated-<timestamp>)")
    parser.add_argument(
        "--output_dir",
        type=str,
        default=str(config.PROJECT_ROOT / "tmp" / "generated"),
        help="Parent directory for run outputs",
    )
    parser.add_argument("--base_y", type=int, default=-64, help="World y of the grid's bottom layer")
    parser.add_argument("--skip-render", action="store_true", help="Skip the mcmap render")
    parser.add_argument("--skip-upload", action="store_true", help="Skip the Google Drive upload")
    parser.add_argument("--gdrive-remote", type=str, default=DEFAULT_REMOTE, help="rclone destination for uploads")
    parser.add_argument(
        "--mcmap-lighting",
        action="store_true",
        help="Render with -lighting (generated worlds have no light data, so this darkens the render; off by default)",
    )
    args = parser.parse_args()
    if not args.prompt and not args.npy:
        parser.error("either --prompt or --npy is required")
    # Fail fast, before any GPU time is spent
    if not args.npy and (args.frames - 1) % 4 != 0:
        parser.error(f"--frames must be 4n+1 for the Wan VAE, got {args.frames}")
    if not args.npy and args.base_y + args.frames > 320:
        parser.error(f"--base_y {args.base_y} + --frames {args.frames} exceeds the world build limit (y=320)")
    return args


def load_or_generate_grid(args, out_dir: Path) -> np.ndarray:
    """Load the grid from --npy or run SANA-Video inference and save it locally."""
    if args.npy:
        logger.info("Loading existing grid from %s", args.npy)
        return np.load(args.npy)

    # Lazy import so --npy runs don't need torch/diffusers
    from src.scripts.sana_video.inference import generate_grid

    grid = generate_grid(
        args.prompt,
        lora_path=args.lora_path,
        transformer_path=args.transformer_path,
        pretrained_model=args.pretrained_model,
        height=args.height,
        width=args.width,
        frames=args.frames,
        guidance_scale=args.guidance_scale,
        seed=args.seed,
    )
    npy_path = out_dir / "grid.npy"
    np.save(npy_path, grid)
    logger.info("Grid saved to %s (shape=%s)", npy_path, grid.shape)
    return grid


def render_world(world_dir: Path, png_path: Path, base_y: int, grid_shape: tuple, lighting: bool = False) -> bool:
    """Render the generated area to a PNG with mcmap. Returns True on success."""
    mcmap_bin = Path(config.MCMAP_BIN)
    if not mcmap_bin.exists():
        logger.warning("mcmap binary not found at %s. Skipping render.", mcmap_bin)
        return False

    size_x, size_z, size_y = grid_shape
    # fmt: off
    cmd = [
        str(mcmap_bin),
        "-nw",
        "-from", "0", "0",
        "-to", str(size_x), str(size_z),
        "-min", str(base_y),
        "-max", str(base_y + size_y),
        "-fragment", "512",
        "-padding", "0",
        "-dim", "overworld",
        "-nobeacons",
        "-shading",
        *(["-lighting"] if lighting else []),
        "-file", str(png_path),
        str(world_dir),
    ]
    # fmt: on
    logger.info("Rendering with mcmap: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning("mcmap failed: %s", result.stderr)
        return False
    logger.info("Render saved to %s", png_path)
    return True


def write_video(grid: np.ndarray, mp4_path: Path, fps: int = 24, scale: int = 4) -> bool:
    """Write the grid as an mp4: one frame per Y-layer, bottom to top.

    Each (X, Z) layer is colored via the block RGB palette (the same pseudo-video
    representation the model was trained on) and nearest-neighbor upscaled by
    ``scale`` for visibility. Returns True on success.
    """
    try:
        import imageio.v2 as imageio
    except ImportError:
        logger.warning("imageio not available. Skipping mp4.")
        return False

    id2rgb, _ = load_id2rgb()
    rgb = id2rgb[grid]  # (X, Z, Y, 3) uint8
    writer = imageio.get_writer(str(mp4_path), fps=fps, macro_block_size=1)
    try:
        for y in range(rgb.shape[2]):
            frame = rgb[:, :, y, :]
            frame = np.repeat(np.repeat(frame, scale, axis=0), scale, axis=1)
            writer.append_data(frame)
    finally:
        writer.close()
    logger.info("Video saved to %s (%d frames)", mp4_path, rgb.shape[2])
    return True


def upload_to_gdrive(files: list[Path], remote: str) -> None:
    """Upload files to the rclone remote (validates the remote name first)."""
    remote_name = remote.split(":", 1)[0]
    remotes = subprocess.run(["rclone", "listremotes"], capture_output=True, text=True)
    if remotes.returncode != 0 or f"{remote_name}:" not in remotes.stdout.split():
        raise RuntimeError(
            f"rclone remote '{remote_name}:' not configured. "
            "Copy your rclone.conf to ~/.config/rclone/rclone.conf or run 'rclone config'."
        )
    for f in files:
        dest = f"{remote.rstrip('/')}/{f.name}"
        logger.info("Uploading %s -> %s", f, dest)
        subprocess.run(["rclone", "copyto", "--progress", str(f), dest], check=True)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()

    # Always timestamp the run name so repeated runs never collide (locally or on Drive)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    name = f"{args.name}-{stamp}" if args.name else f"generated-{stamp}"
    out_dir = Path(args.output_dir) / name
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Run '%s' -> %s", name, out_dir)

    # Record what produced this run, so outputs stay identifiable among many runs
    meta_path = out_dir / "meta.json"
    meta_path.write_text(json.dumps({"run": name, **vars(args)}, indent=2, default=str))

    # 1. Grid: load or generate
    grid = load_or_generate_grid(args, out_dir)

    # 2. World: grid -> playable Java 1.19.2 world
    world_dir = build_world(grid, out_dir / "world", base_y=args.base_y, level_name=name)

    # 3. Render
    png_path = out_dir / f"{name}.nw.png"
    rendered = False
    if args.skip_render:
        logger.info("Skipping render (--skip-render).")
    else:
        rendered = render_world(world_dir, png_path, args.base_y, grid.shape, lighting=args.mcmap_lighting)

    # 4. mp4 of the raw output (Y-layers as frames, bottom to top)
    mp4_path = out_dir / f"{name}.mp4"
    has_video = write_video(grid, mp4_path)

    # 5. Zip the world (zip contains a world/ folder, droppable into saves/)
    zip_path = Path(shutil.make_archive(str(out_dir / name), "zip", root_dir=out_dir, base_dir="world"))
    logger.info("World zipped to %s", zip_path)

    # 6. Upload
    if args.skip_upload:
        logger.info("Skipping upload (--skip-upload).")
    else:
        uploads = [zip_path, meta_path] + ([png_path] if rendered else []) + ([mp4_path] if has_video else [])
        # Per-run subdirectory on the remote keeps runs grouped and collision-free
        upload_to_gdrive(uploads, f"{args.gdrive_remote.rstrip('/')}/{name}")

    logger.info("Done. Outputs in %s", out_dir)


if __name__ == "__main__":
    main()
