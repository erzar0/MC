# SANA-Video Voxel Training (`ivjoint`) Runbook

Zero-to-training on a fresh H200 GPU box using the `train_ivjoint.py` pipeline (flat `.pth` files). 

---

## Step 0 — one-time secrets (fill these in)

```bash
# Your Weights & Biases API key (https://wandb.ai/authorize). Leave unset to
# train without wandb (use --report_to none in TRAIN_ARGS below).
export WANDB_API_KEY=PASTE_YOUR_KEY_HERE

# Only if the SANA-Video base model is gated for your account:
# export HF_TOKEN=PASTE_YOUR_HF_TOKEN
```

> **Disk Space & Egress**: 
> * The script automatically detects `/ephemeral` (on Brev/GPU instances) and points your checkpoints (`OUTPUT_DIR`), dataset (`BUNDLE_DIR`), and Hugging Face downloads cache (`HF_HOME`) there to avoid running out of disk space.
> * It automatically sets `RCLONE_DRIVE_CHUNK_SIZE=1024M` to optimize Google Drive upload speeds.

Ensure `rclone` has Drive access. From **your local machine**, copy your configured `rclone.conf` over to the remote GPU instance (running `mkdir -p` first so the destination folder exists):

```bash
# Run LOCALLY, replacing user@remote with your box's ssh address
ssh user@remote "mkdir -p ~/.config/rclone"
scp ~/.config/rclone/rclone.conf user@remote:~/.config/rclone/rclone.conf
```

---

## Step 1 — get the repo onto the box

```bash
git clone https://github.com/erzar0/MC.git
cd MC
```

---

## Step 1.5 — one-time: refresh the manifest for height bucketing (local)

Training buckets regions by content height (trains only on real blocks, ~2× faster), which needs a `height` field. Upload a height-augmented manifest once from your local development machine:

```bash
# Run LOCALLY, from your local repo checkout
python deploy/package_dataset.py --manifest-only \
    --rclone-dest MinecraftDataset:minecraft-training/
```

---

## Step 2 — start training (One Command)

Running `remote_train.sh` with `TRAIN_SCRIPT=ivjoint` will automatically:
1. Detect and download the newest `.pth` checkpoint from Google Drive (if one exists).
2. Start or resume training from it seamlessly using `--resume_from latest`.
3. Stream newly completed epoch checkpoints back to Google Drive in the background.

```bash
# Configure script, config targets, and keys
export TRAIN_SCRIPT=ivjoint
export IVJOINT_CONFIG=configs/sana_video_minecraft.yaml
export WANDB_API_KEY=PASTE_YOUR_KEY_HERE
export HF_TOKEN=PASTE_YOUR_HF_TOKEN

# Run detached in the background
DATASET_REMOTE=MinecraftDataset:minecraft-training/sana_video_dataset.tar \
MANIFEST_REMOTE=MinecraftDataset:minecraft-training/manifest.jsonl \
CKPT_REMOTE=MinecraftDataset:minecraft-training/checkpoints \
nohup bash deploy/remote_train.sh > train.log 2>&1 &

# Watch live progress
tail -f train.log
```

---

## Tuning Knobs (Pyrallis Overrides)

Since `ivjoint` is driven by a YAML config file, you can override any parameter on the command line by prepending extra arguments to `TRAIN_ARGS`:

* **OOM / Out of Memory?** Decrease the batch size:
  `TRAIN_ARGS="--train.train_batch_size 1"`
* **Adjust Gradient Accumulation**:
  `TRAIN_ARGS="--train.gradient_accumulation_steps 8"`
* **Disable WandB tracker**:
  `TRAIN_ARGS="--report_to none"`
* **Change Learning Rate**:
  `TRAIN_ARGS="--train.optimizer.lr 2.5e-5"`

*Example running with custom overrides:*
```bash
export TRAIN_SCRIPT=ivjoint
export IVJOINT_CONFIG=configs/sana_video_minecraft.yaml

DATASET_REMOTE=MinecraftDataset:minecraft-training/sana_video_dataset.tar \
MANIFEST_REMOTE=MinecraftDataset:minecraft-training/manifest.jsonl \
CKPT_REMOTE=MinecraftDataset:minecraft-training/checkpoints \
TRAIN_ARGS="--train.train_batch_size 1 --train.gradient_accumulation_steps 8 --report_to none" \
nohup bash deploy/remote_train.sh > train.log 2>&1 &
```

---

## After training & Inference

Checkpoints (`epoch_X_step_Y.pth`) are stored on Google Drive. Pull them down locally with:

```bash
rclone copy --progress MinecraftDataset:minecraft-training/checkpoints ./checkpoints
```

Generate a 3D voxel grid from a `.pth` checkpoint:

```bash
.venv/bin/python src/scripts/sana_video/inference.py \
    --prompt "[Region]: A medieval castle with tall stone towers stands on a green plain." \
    --transformer_path checkpoints/epoch_1_step_3200.pth \
    --height 256 --width 256 --frames 385 --no_chi \
    --output_npy tmp/generated.npy
```

> [!IMPORTANT]
> Because `train_ivjoint.py` disables the CHI system prompt encoder, you **must** pass `--no_chi` during inference to match the trained checkpoint weights.
