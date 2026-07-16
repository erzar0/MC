#!/usr/bin/env bash
#
# train_h200.sh — launch config for SANA-Video full fine-tuning on an H200 box.
# Fill in WANDB_API_KEY (or put the key in ~/.netrc as
# `machine api.wandb.ai login user password <key>`) before running.
#
# Usage (from the repo root on the training box):
#   bash deploy/train_h200.sh
#
set -euo pipefail

# Leave empty if the key is already in ~/.netrc
export WANDB_API_KEY="${WANDB_API_KEY:-}"

# Working dirs on the instance's ephemeral volume
export HF_HOME="${HF_HOME:-/ephemeral/cache/huggingface}"
export OUTPUT_DIR="${OUTPUT_DIR:-/ephemeral/sana_video_ft}"
export BUNDLE_DIR="${BUNDLE_DIR:-/ephemeral/train_bundle}"

export DATASET_REMOTE="MinecraftDataset:minecraft-training/sana_video_dataset.tar"
# The tar's bundled manifest lacks the `height` field; this height-augmented
# manifest on Drive overwrites it (required for height bucketing).
export MANIFEST_REMOTE="MinecraftDataset:minecraft-training/manifest.jsonl"
export CKPT_REMOTE="MinecraftDataset:minecraft-training/checkpoints"

# batch_size must stay <= 4: at 8, the tallest height bucket (385 frames ->
# 97 latent frames) overflows conv2d's 32-bit index limit (canUse32BitIndexMath).
# Note: train.py now defaults to the upstream recipe (logit-normal sigma with
# training flow_shift 3.0, sqrt-auto-scaled lr = 5e-5*sqrt(effective_bs/256)
# (1.25e-5 at effective batch 16), grad clip 0.1, 500-step warmup, CHI-prefixed
# captions); resuming an older run picks these up, so expect the loss level to
# jump at the switch.
# These are train.py argparse flags — they only apply to the default trainer.
# For TRAIN_SCRIPT=ivjoint, TRAIN_ARGS are pyrallis overrides on
# configs/sana_video_minecraft.yaml (e.g. "--train.num_epochs=50") and default
# to empty; whatever you export is passed through untouched.
if [ "${TRAIN_SCRIPT:-train}" != "ivjoint" ]; then
    export TRAIN_ARGS="${TRAIN_ARGS:---mode full --spatial_crop_size 256 --batch_size 4 --gradient_accumulation_steps 4 --max_frames 385 --epochs 100 --num_workers 24 --report_to wandb}"
fi

# To run the ported upstream trainer instead (.pth checkpoints -> convert with
# convert_pth_to_diffusers.py):
#   export TRAIN_SCRIPT=ivjoint

bash "$(dirname "${BASH_SOURCE[0]}")/remote_train.sh"
