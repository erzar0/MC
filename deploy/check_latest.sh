#!/usr/bin/env bash
# deploy/check_latest.sh — Find the latest .pth checkpoint, run inference,
# and print generated blocks to verify if the model is learning.
set -euo pipefail

# Resolve repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

VENV=.venv/bin/python
if [ ! -f "$VENV" ]; then
    VENV=python3
fi

# Find the directory containing checkpoints
CKPT_DIR="/ephemeral/sana_video_ft/checkpoints"
if [ ! -d "$CKPT_DIR" ]; then
    CKPT_DIR="tmp/sana_video_ft/checkpoints"
fi

if [ ! -d "$CKPT_DIR" ]; then
    echo "ERROR: Checkpoint directory not found at $CKPT_DIR."
    exit 1
fi

# Resolve the latest checkpoint
if [ -f "$CKPT_DIR/latest.pth" ]; then
    LATEST="$CKPT_DIR/latest.pth"
else
    # Find the newest modified .pth file
    LATEST=$(find "$CKPT_DIR" -name "epoch_*.pth" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -f2- -d' ' || true)
fi

if [ -z "$LATEST" ] || [ ! -f "$LATEST" ]; then
    echo "ERROR: No .pth checkpoints found in $CKPT_DIR."
    exit 1
fi

# Resolve HF Cache to ephemeral if present
if [ -d "/ephemeral/cache/huggingface" ]; then
    export HF_HOME="/ephemeral/cache/huggingface"
fi

# Prompt to generate (can be overridden as first argument)
PROMPT="${1:-[Region]: A small island covered in dense green forests with sandy beaches along its shores. A small wooden house sits near the coast.}"

DIFFUSERS_DIR="tmp/check_render_diffusers"

echo "=========================================================="
echo "Converting checkpoint to Diffusers format..."
echo "=========================================================="
$VENV src/scripts/sana_video/convert_pth_to_diffusers.py \
    --pth "$LATEST" \
    --output "$DIFFUSERS_DIR" \
    --dtype bf16

echo "=========================================================="
echo "Running inference with converted checkpoint: $DIFFUSERS_DIR"
echo "Prompt: $PROMPT"
echo "=========================================================="

# Run inference
$VENV src/scripts/sana_video/inference.py \
    --prompt "$PROMPT" \
    --transformer_path "$DIFFUSERS_DIR" \
    --height 256 \
    --width 256 \
    --frames 65 \
    --no_chi \
    --output_npy "tmp/generated_check.npy"

echo "=========================================================="
echo "Categorical block IDs saved to tmp/generated_check.npy"
echo "Rendering 3D volume visualization using generate_world.py..."
echo "=========================================================="

$VENV src/scripts/sana_video/generate_world.py \
    --npy tmp/generated_check.npy \
    --skip-upload \
    --name check_render

echo "=========================================================="
echo "Visualizations generated in tmp/generated/check_render/ !"
echo "Check the directory for: "
echo "  - check_render.png (3D Isometric Voxel Render)"
echo "  - check_render.mp4 (Layer-by-layer video)"
echo "  - check_render.zip (Playable Minecraft world)"
echo "=========================================================="

