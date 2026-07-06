# Start training — copy/paste runbook

Zero-to-training on a fresh GPU box (H200). Values below are pre-filled for this
project's Drive remote (`MinecraftDataset:`) and the uploaded dataset. Only the
two secrets/paths in **Step 0** need your input.

---

## Step 0 — one-time secrets (fill these in)

```bash
# Your Weights & Biases API key (https://wandb.ai/authorize). Leave unset to
# train without wandb (drop --report_to wandb from TRAIN_ARGS below).
export WANDB_API_KEY=PASTE_YOUR_KEY_HERE

# Only if the SANA-Video base model is gated for your account:
# export HF_TOKEN=PASTE_YOUR_HF_TOKEN
```

> **Brev / minimal boxes:** this works on a plain CUDA instance — the bootstrap
> installs only the training deps (no `amulet` build) into a fresh `.venv`.
> Provision **≥ ~90 GB disk** (dataset tar + extraction + model weights); the
> script aborts early via `MIN_DISK_GB` if there isn't enough, and deletes the
> tar after extraction by default (`KEEP_TAR=1` to keep it).

rclone needs Drive access on the box. From **your local machine**, copy the
config you already made over to the remote (headless boxes can't do the browser
OAuth):

```bash
# run LOCALLY, replace user@remote with your box
scp ~/.config/rclone/rclone.conf user@remote:~/.config/rclone/rclone.conf
```

---

## Step 1 — get the repo onto the box

```bash
git clone git@github.com:erzar0/MC.git
cd MC
```

> If the box has no SSH key for GitHub, use HTTPS instead:
> `git clone https://github.com/erzar0/MC.git`

---

## Step 2 — start training (one command)

The bootstrap script installs `uv` + `rclone`, syncs the env, downloads +
extracts the dataset from Drive, launches training, and streams checkpoints back
to Drive as they are written.

```bash
DATASET_REMOTE=MinecraftDataset:minecraft-training/sana_video_dataset.tar \
CKPT_REMOTE=MinecraftDataset:minecraft-training/checkpoints \
TRAIN_ARGS="--mode lora --spatial_crop_size 128 --max_frames 385 --epochs 3 \
    --batch_size 4 --gradient_accumulation_steps 2 \
    --report_to wandb --wandb_project minecraft-sana-video" \
bash deploy/remote_train.sh
```

That's it — it runs to completion and leaves checkpoints in
`MinecraftDataset:minecraft-training/checkpoints`.

### Run it detached (survives SSH disconnects)

```bash
DATASET_REMOTE=MinecraftDataset:minecraft-training/sana_video_dataset.tar \
CKPT_REMOTE=MinecraftDataset:minecraft-training/checkpoints \
TRAIN_ARGS="--mode lora --spatial_crop_size 128 --max_frames 385 --epochs 3 \
    --batch_size 4 --gradient_accumulation_steps 2 \
    --report_to wandb --wandb_project minecraft-sana-video" \
nohup bash deploy/remote_train.sh > train.log 2>&1 &

tail -f train.log        # watch progress
```

---

## Tuning knobs

- **OOM?** Lower `--batch_size` (4 → 2 → 1). Memory scales with batch size.
- **GPU underused?** Raise `--batch_size` and drop `--gradient_accumulation_steps`
  to keep the effective batch (`batch_size × grad_accum`) where you want it.
- **No wandb:** delete `--report_to wandb --wandb_project ...` and skip the
  `WANDB_API_KEY` export.
- **Full fine-tuning:** swap `--mode lora` → `--mode full` (needs much more VRAM;
  start at `--batch_size 1`).

## After training

Checkpoints (`checkpoint-step-<N>/`, `checkpoint-epoch-<N>/`) are already on
Drive. Pull them anywhere with:

```bash
rclone copy --progress MinecraftDataset:minecraft-training/checkpoints ./checkpoints
```

Generate a voxel grid from a trained LoRA:

```bash
.venv/bin/python src/scripts/sana_video/inference.py \
    --prompt "a medieval castle on a hill" \
    --lora_path checkpoints/checkpoint-epoch-3 \
    --height 128 --width 128 --frames 385 \
    --output_npy tmp/generated.npy
```

> Match `--height`/`--width`/`--frames` to what you trained on (128 / 128 / 385).
