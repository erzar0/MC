#!/usr/bin/env bash
#
# remote_train.sh — bootstrap a fresh GPU box, download the training dataset from
# Google Drive (rclone), run SANA-Video fine-tuning, and stream checkpoints back
# to Google Drive as they are written.
#
# This script assumes it is run from inside a checkout of this repo (that is how
# you got the script). It installs uv + rclone if missing, then does everything
# else. rclone must be able to reach your Drive: either copy your rclone.conf to
# ~/.config/rclone/rclone.conf on this box, or point $RCLONE_CONFIG at it.
#
# Configure via environment variables (all have defaults shown):
#
#   DATASET_REMOTE   rclone source for the dataset tar
#                    (default: gdrive:minecraft-training/sana_video_dataset.tar)
#   CKPT_REMOTE      rclone dest for checkpoints
#                    (default: gdrive:minecraft-training/checkpoints)
#   OUTPUT_DIR       local checkpoint dir (default: tmp/sana_video_ft)
#   BUNDLE_DIR       where the dataset is extracted (default: data/train_bundle)
#   SAVE_EVERY       --save_every_steps passed to train.py (default: 1000)
#   UPLOAD_INTERVAL  seconds between checkpoint-upload passes (default: 60)
#   TRAIN_ARGS       extra args forwarded to train.py, e.g.
#                    "--mode lora --spatial_crop_size 256 --max_frames 33 --epochs 3"
#   ACCELERATE_ARGS  extra args for `accelerate launch` (e.g. "--num_processes 2")
#
# For wandb logging, add "--report_to wandb" to TRAIN_ARGS and export WANDB_API_KEY
# (or run `wandb login`) so the headless box can authenticate.
#
# Example:
#   DATASET_REMOTE=gdrive:mc/sana_video_dataset.tar \
#   CKPT_REMOTE=gdrive:mc/checkpoints \
#   TRAIN_ARGS="--mode lora --spatial_crop_size 512 --max_frames 65 --epochs 3" \
#   bash deploy/remote_train.sh
#
set -euo pipefail

# --- Resolve repo root (parent of this deploy/ dir) --------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# --- Config ------------------------------------------------------------------
DATASET_REMOTE="${DATASET_REMOTE:-gdrive:minecraft-training/sana_video_dataset.tar}"
CKPT_REMOTE="${CKPT_REMOTE:-gdrive:minecraft-training/checkpoints}"
OUTPUT_DIR="${OUTPUT_DIR:-tmp/sana_video_ft}"
BUNDLE_DIR="${BUNDLE_DIR:-data/train_bundle}"
SAVE_EVERY="${SAVE_EVERY:-1000}"
UPLOAD_INTERVAL="${UPLOAD_INTERVAL:-60}"
TRAIN_ARGS="${TRAIN_ARGS:---mode lora --spatial_crop_size 512 --max_frames 65 --epochs 3}"
ACCELERATE_ARGS="${ACCELERATE_ARGS:-}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# --- 1. Prerequisites --------------------------------------------------------
if ! command -v rclone >/dev/null 2>&1; then
    log "Installing rclone..."
    curl -fsSL https://rclone.org/install.sh | sudo bash
fi
if ! command -v uv >/dev/null 2>&1; then
    log "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

# Verify rclone can see the configured remote before doing anything expensive.
REMOTE_NAME="${DATASET_REMOTE%%:*}"
if ! rclone listremotes | grep -q "^${REMOTE_NAME}:"; then
    log "ERROR: rclone remote '${REMOTE_NAME}:' not configured."
    log "Copy your rclone.conf to ~/.config/rclone/rclone.conf (or set \$RCLONE_CONFIG),"
    log "or run 'rclone config' to create the '${REMOTE_NAME}' Google Drive remote."
    exit 1
fi

# --- 2. Python env -----------------------------------------------------------
log "Syncing Python environment with uv..."
uv sync

# --- 3. Dataset download + extract ------------------------------------------
mkdir -p "$BUNDLE_DIR"
if [ ! -f "$BUNDLE_DIR/manifest.jsonl" ]; then
    TAR_LOCAL="tmp/$(basename "$DATASET_REMOTE")"
    mkdir -p tmp
    log "Downloading dataset: $DATASET_REMOTE -> $TAR_LOCAL"
    rclone copyto --progress "$DATASET_REMOTE" "$TAR_LOCAL"
    log "Extracting to $BUNDLE_DIR ..."
    tar -xf "$TAR_LOCAL" -C "$BUNDLE_DIR"
    log "Extracted $(wc -l < "$BUNDLE_DIR/manifest.jsonl") manifest entries."
else
    log "Dataset already present at $BUNDLE_DIR (skipping download)."
fi

# --- 4. Background checkpoint uploader --------------------------------------
# Uploads any checkpoint-*/ that has a .complete marker (written by train.py once
# the checkpoint is fully flushed) and hasn't been uploaded yet. The .complete /
# .uploaded markers are not themselves pushed to Drive.
sync_checkpoints_once() {
    shopt -s nullglob
    for d in "$OUTPUT_DIR"/checkpoint-*; do
        [ -d "$d" ] || continue
        [ -f "$d/.complete" ] || continue
        [ -f "$d/.uploaded" ] && continue
        local name; name="$(basename "$d")"
        log "[uploader] syncing $name -> $CKPT_REMOTE/$name"
        if rclone copy --exclude ".complete" --exclude ".uploaded" "$d" "$CKPT_REMOTE/$name"; then
            touch "$d/.uploaded"
        else
            log "[uploader] WARNING: upload of $name failed; will retry next pass"
        fi
    done
}

mkdir -p "$OUTPUT_DIR"
UPLOADER_PID=""
start_uploader() {
    ( while true; do sync_checkpoints_once; sleep "$UPLOAD_INTERVAL"; done ) &
    UPLOADER_PID=$!
    log "Checkpoint uploader started (pid $UPLOADER_PID, every ${UPLOAD_INTERVAL}s)."
}
stop_uploader() {
    [ -n "$UPLOADER_PID" ] && kill "$UPLOADER_PID" 2>/dev/null || true
}
trap stop_uploader EXIT
start_uploader

# --- 5. Train ----------------------------------------------------------------
log "Launching training. Checkpoints -> $OUTPUT_DIR (every $SAVE_EVERY samples)."
# shellcheck disable=SC2086
uv run accelerate launch $ACCELERATE_ARGS src/scripts/sana_video/train.py \
    --manifest "$BUNDLE_DIR/manifest.jsonl" \
    --output_dir "$OUTPUT_DIR" \
    --save_every_steps "$SAVE_EVERY" \
    $TRAIN_ARGS

# --- 6. Final sync -----------------------------------------------------------
log "Training finished. Final checkpoint sync..."
stop_uploader
sync_checkpoints_once
log "Done. Checkpoints are in $CKPT_REMOTE"
