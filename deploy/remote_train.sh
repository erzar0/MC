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
#   MANIFEST_REMOTE  optional rclone path to a manifest.jsonl that overwrites the
#                    bundled one (e.g. height-augmented manifest for bucketing)
#   CKPT_REMOTE      rclone dest for checkpoints
#                    (default: gdrive:minecraft-training/checkpoints)
#   INIT_FROM        (ivjoint only) path to a .pth used as weights-only init:
#                    skips the Drive resume download, moves any local
#                    checkpoints aside, and starts at step 0 with a fresh
#                    optimizer and full lr warmup. Point CKPT_REMOTE at a new
#                    folder too, or the next relaunch resumes the OLD run.
#   OUTPUT_DIR       local checkpoint dir (default: tmp/sana_video_ft)
#   BUNDLE_DIR       where the dataset is extracted (default: data/train_bundle)
#   SAVE_EVERY       --save_every_steps passed to train.py (default: 1000)
#   UPLOAD_INTERVAL  seconds between checkpoint-upload passes (default: 60)
#   TRAIN_ARGS       extra args forwarded to train.py, e.g.
#                    "--mode lora --spatial_crop_size 128 --max_frames 385 --epochs 3"
#                    (128px full-height: max_frames 385 covers a 384-tall region)
#   ACCELERATE_ARGS  extra args for `accelerate launch` (e.g. "--num_processes 2")
#   MIN_DISK_GB      abort if the working filesystem has less free space than this
#                    (default: 90 — 38 GB tar + 38 GB extracted + model weights)
#   KEEP_TAR         set to 1 to keep the downloaded tar after extraction
#                    (default: unset — the tar is deleted to reclaim ~38 GB)
#   HF_TOKEN         Hugging Face token, exported for gated model downloads
#
# This installs ONLY the training dependencies (deploy/requirements-train.txt),
# not the full project env — it never builds amulet, so it works on minimal
# boxes (e.g. Brev / bare CUDA instances).
#
# For wandb logging, add "--report_to wandb" to TRAIN_ARGS and export WANDB_API_KEY
# (or run `wandb login`) so the headless box can authenticate.
#
# Example:
#   DATASET_REMOTE=gdrive:mc/sana_video_dataset.tar \
#   CKPT_REMOTE=gdrive:mc/checkpoints \
#   TRAIN_ARGS="--mode lora --spatial_crop_size 128 --max_frames 385 --epochs 3" \
#   bash deploy/remote_train.sh
#
set -euo pipefail

# --- Resolve repo root (parent of this deploy/ dir) --------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# --- Config ------------------------------------------------------------------
DATASET_REMOTE="${DATASET_REMOTE:-gdrive:minecraft-training/sana_video_dataset.tar}"
# Optional: rclone path to a manifest.jsonl to overwrite the bundled one with
# (e.g. a height-augmented manifest for bucketing). Leave empty to use the
# manifest inside the tar.
MANIFEST_REMOTE="${MANIFEST_REMOTE:-}"
CKPT_REMOTE="${CKPT_REMOTE:-gdrive:minecraft-training/checkpoints}"

# Use /ephemeral partition for large files by default if it exists (e.g. on Brev boxes)
if [ -d "/ephemeral" ]; then
    OUTPUT_DIR="${OUTPUT_DIR:-/ephemeral/sana_video_ft}"
    BUNDLE_DIR="${BUNDLE_DIR:-/ephemeral/train_bundle}"
    export HF_HOME="${HF_HOME:-/ephemeral/cache/huggingface}"
else
    OUTPUT_DIR="${OUTPUT_DIR:-tmp/sana_video_ft}"
    BUNDLE_DIR="${BUNDLE_DIR:-data/train_bundle}"
    export HF_HOME="${HF_HOME:-tmp/cache/huggingface}"
fi

# Optimize rclone Google Drive upload speeds by increasing the chunk size from the 8M default
export RCLONE_DRIVE_CHUNK_SIZE="${RCLONE_DRIVE_CHUNK_SIZE:-1024M}"

SAVE_EVERY="${SAVE_EVERY:-1000}"
UPLOAD_INTERVAL="${UPLOAD_INTERVAL:-60}"
# TRAIN_SCRIPT: "train" (diffusers-based train.py, default) or "ivjoint"
# (ported upstream trainer train_ivjoint.py + configs/sana_video_minecraft.yaml,
# saves .pth checkpoints; convert with convert_pth_to_diffusers.py for inference).
TRAIN_SCRIPT="${TRAIN_SCRIPT:-train}"
IVJOINT_CONFIG="${IVJOINT_CONFIG:-configs/sana_video_minecraft.yaml}"
# TRAIN_ARGS semantics depend on TRAIN_SCRIPT: train.py argparse flags for
# "train", pyrallis overrides (e.g. "--train.num_epochs 50") for "ivjoint".
if [ "$TRAIN_SCRIPT" = "ivjoint" ]; then
    TRAIN_ARGS="${TRAIN_ARGS:-}"
else
    TRAIN_ARGS="${TRAIN_ARGS:---mode lora --spatial_crop_size 128 --max_frames 385 --epochs 3}"
fi
ACCELERATE_ARGS="${ACCELERATE_ARGS:-}"
MIN_DISK_GB="${MIN_DISK_GB:-90}"
VENV_DIR="${VENV_DIR:-.venv}"

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

# Export Hugging Face token if provided (needed for gated base-model downloads).
if [ -n "${HF_TOKEN:-}" ]; then
    export HF_TOKEN
    export HUGGING_FACE_HUB_TOKEN="$HF_TOKEN"
    log "HF_TOKEN detected; exported for model downloads."
fi

# Disk preflight: need room for the tar + its extraction + model weights.
AVAIL_GB="$(df -Pk . | awk 'NR==2 {print int($4 / 1024 / 1024)}')"
log "Free disk on working filesystem: ${AVAIL_GB} GB (need >= ${MIN_DISK_GB} GB)."
if [ "$AVAIL_GB" -lt "$MIN_DISK_GB" ]; then
    log "ERROR: not enough free disk. Provision a larger volume, lower MIN_DISK_GB,"
    log "or set KEEP_TAR=0 (default) so the tar is deleted right after extraction."
    exit 1
fi

# --- 2. Python env (training-only; no amulet) --------------------------------
if [ ! -d "$VENV_DIR" ]; then
    log "Creating training venv at $VENV_DIR..."
    uv venv "$VENV_DIR"
else
    log "Python venv already exists at $VENV_DIR (skipping creation)."
fi
log "Installing/updating training deps..."
uv pip install --python "$VENV_DIR/bin/python" -r deploy/requirements-train.txt
if [ "$TRAIN_SCRIPT" = "ivjoint" ]; then
    log "Installing upstream-trainer deps (ivjoint)..."
    uv pip install --python "$VENV_DIR/bin/python" -r deploy/requirements-ivjoint.txt
    # mmcv 1.7.2's sdist build needs pkg_resources (setuptools<81, installed above).
    MMCV_WITH_OPS=0 uv pip install --python "$VENV_DIR/bin/python" --no-build-isolation "mmcv==1.7.2"
fi

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
    if [ "${KEEP_TAR:-0}" != "1" ]; then
        log "Deleting $TAR_LOCAL to reclaim disk (set KEEP_TAR=1 to keep it)."
        rm -f "$TAR_LOCAL"
    fi
else
    log "Dataset already present at $BUNDLE_DIR (skipping download)."
fi

# Optionally overwrite the bundled manifest (e.g. with a height-augmented one
# for bucketing) without re-downloading the volume tar.
if [ -n "$MANIFEST_REMOTE" ]; then
    log "Fetching manifest override: $MANIFEST_REMOTE -> $BUNDLE_DIR/manifest.jsonl"
    rclone copyto --progress "$MANIFEST_REMOTE" "$BUNDLE_DIR/manifest.jsonl"
    log "Manifest now has $(wc -l < "$BUNDLE_DIR/manifest.jsonl") entries."
fi

# --- 4. Background checkpoint uploader --------------------------------------
# Uploads any checkpoint-*/ that has a .complete marker (written by train.py once
# the checkpoint is fully flushed) and hasn't been uploaded yet. The .complete /
# .uploaded markers are not themselves pushed to Drive.
sync_checkpoints_once() {
    if [ "$TRAIN_SCRIPT" = "ivjoint" ]; then
        # ivjoint saves flat .pth files under $OUTPUT_DIR/checkpoints/ with no
        # completion marker; --min-age skips files still being written and
        # latest.pth is a symlink to the newest epoch_*.pth.
        if [ -d "$OUTPUT_DIR/checkpoints" ]; then
            rclone copy --min-age 2m --exclude "latest.pth" "$OUTPUT_DIR/checkpoints" "$CKPT_REMOTE" \
                || log "[uploader] WARNING: .pth upload failed; will retry next pass"
        fi
        return
    fi
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

# Check if there is an existing checkpoint on Google Drive to resume from
if [ "$TRAIN_SCRIPT" = "ivjoint" ]; then
    mkdir -p "$OUTPUT_DIR/checkpoints"
    if [ -n "${INIT_FROM:-}" ]; then
        # Fresh start: use $INIT_FROM as weights-only init (fresh optimizer,
        # step 0, full lr warmup). Any local .pth would make --resume_from
        # latest do a full resume instead, so move them aside.
        [ -f "$INIT_FROM" ] || { log "ERROR: INIT_FROM=$INIT_FROM does not exist."; exit 1; }
        if compgen -G "$OUTPUT_DIR/checkpoints/*.pth" > /dev/null; then
            BAK="$OUTPUT_DIR/checkpoints.bak-$(date +%s)"
            log "INIT_FROM set: moving existing checkpoints to $BAK"
            mv "$OUTPUT_DIR/checkpoints" "$BAK"
            mkdir -p "$OUTPUT_DIR/checkpoints"
        fi
        log "Fresh start from $INIT_FROM (weights only; optimizer/lr/step reset)."
    else
        # Pull the newest .pth from Drive (if any) into work_dir/checkpoints;
        # the trainer's --resume_from latest then picks it up (or falls back to
        # the pretrained load_from when the dir is empty).
        # sort -V so step_3200 ranks above step_800 (plain sort is lexicographic)
        LATEST_PTH="$(rclone lsf --files-only "$CKPT_REMOTE" 2>/dev/null | grep '\.pth$' | sort -V | tail -1 || true)"
        if [ -n "$LATEST_PTH" ] && [ ! -f "$OUTPUT_DIR/checkpoints/$LATEST_PTH" ]; then
            log "Downloading $LATEST_PTH from Drive..."
            rclone copyto --progress "$CKPT_REMOTE/$LATEST_PTH" "$OUTPUT_DIR/checkpoints/$LATEST_PTH"
        fi
    fi
elif [[ "$TRAIN_ARGS" == *"--resume_from_checkpoint"* ]]; then
    log "Resuming from checkpoint specified in TRAIN_ARGS."
else
    log "Checking for existing checkpoints on Google Drive at $CKPT_REMOTE..."
    LATEST_CKPT=""
    if rclone lsf "$CKPT_REMOTE" >/dev/null 2>&1; then
        LATEST_CKPT=$(rclone lsjson --dirs-only "$CKPT_REMOTE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    ckpts = [d for d in data if d['Name'].startswith('checkpoint-')]
    if ckpts:
        ckpts.sort(key=lambda x: x['ModTime'])
        print(ckpts[-1]['Name'])
except Exception:
    pass
")
    fi

    if [ -n "$LATEST_CKPT" ]; then
        log "Found latest checkpoint on Google Drive: $LATEST_CKPT"
        LOCAL_CKPT_PATH="$OUTPUT_DIR/$LATEST_CKPT"
        if [ ! -d "$LOCAL_CKPT_PATH" ]; then
            log "Downloading $LATEST_CKPT from Drive..."
            mkdir -p "$LOCAL_CKPT_PATH"
            rclone copy --progress "$CKPT_REMOTE/$LATEST_CKPT" "$LOCAL_CKPT_PATH"
        else
            log "Checkpoint $LATEST_CKPT already present locally."
        fi
        # Append --resume_from_checkpoint to TRAIN_ARGS
        TRAIN_ARGS="$TRAIN_ARGS --resume_from_checkpoint $LOCAL_CKPT_PATH"
    else
        log "No checkpoints found on Google Drive. Starting training from scratch."
    fi
fi

# --- 5. Train ----------------------------------------------------------------
# Run accelerate from the training venv directly. We avoid `uv run`, which would
# re-sync the full project env (and try to build amulet) before launching.
log "Launching training. Checkpoints -> $OUTPUT_DIR (every $SAVE_EVERY samples)."
if [ "$TRAIN_SCRIPT" = "ivjoint" ]; then
    # Save cadence, batch size etc. come from $IVJOINT_CONFIG; TRAIN_ARGS may
    # add pyrallis overrides (e.g. --train.train_batch_size 4).
    # shellcheck disable=SC2086
    "$VENV_DIR/bin/python" src/scripts/sana_video/train_ivjoint.py \
        --config "$IVJOINT_CONFIG" \
        --work_dir "$OUTPUT_DIR" \
        --data.data_dir "{minecraft: $BUNDLE_DIR/manifest.jsonl}" \
        --resume_from latest \
        ${INIT_FROM:+--model.load_from="$INIT_FROM"} \
        $TRAIN_ARGS
else
    # shellcheck disable=SC2086
    "$VENV_DIR/bin/accelerate" launch $ACCELERATE_ARGS src/scripts/sana_video/train.py \
        --manifest "$BUNDLE_DIR/manifest.jsonl" \
        --output_dir "$OUTPUT_DIR" \
        --save_every_steps "$SAVE_EVERY" \
        $TRAIN_ARGS
fi

# --- 6. Final sync -----------------------------------------------------------
log "Training finished. Final checkpoint sync..."
stop_uploader
sync_checkpoints_once
log "Done. Checkpoints are in $CKPT_REMOTE"
