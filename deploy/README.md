# Remote training deployment

Package the SANA-Video training dataset, ship it to Google Drive, and run
training on a fresh GPU box with checkpoints streamed back to Drive.

## What gets packaged

Only the files training actually needs (~38 GB for the full dataset):

- the `.b2frame` volumes referenced by the manifest,
- a manifest rewritten with **relative** volume paths (captions are already
  inline in the manifest, so caption `.txt` files are *not* needed),
- the block-colour lookup assets (`block_states.txt`, `block_state2rgb.csv`).

Bundle layout inside the tar:

```
manifest.jsonl
assets/block_states.txt
assets/block_state2rgb.csv
volumes/<world_id>/<region>.b2frame
```

`src/scripts/sana_video/dataset.py` resolves relative volume paths against the
manifest's directory, so the bundle is portable to any machine.

## One-time: configure rclone → Google Drive

On both your local machine and the remote box, rclone needs access to Drive:

```bash
rclone config          # create a remote named e.g. "gdrive" (type: drive)
```

For the **remote box** (often headless), the simplest path is to copy the
`rclone.conf` you created locally:

```bash
scp ~/.config/rclone/rclone.conf  user@remote:~/.config/rclone/rclone.conf
```

(For fully unattended cloud runs, use a Google service account instead and
share the target Drive folder with the service account email.)

## Step 1 — package + upload the dataset (local)

```bash
# build the bundle and upload in one shot
python deploy/package_dataset.py \
    --output tmp/sana_video_dataset.tar \
    --rclone-dest gdrive:minecraft-training/

# ...or build now, upload later
python deploy/package_dataset.py --output tmp/sana_video_dataset.tar
rclone copy --progress tmp/sana_video_dataset.tar gdrive:minecraft-training/
```

Smoke-test the whole pipeline cheaply with a tiny bundle first:

```bash
python deploy/package_dataset.py --limit 8 --output tmp/mini_dataset.tar
```

## Step 2 — train on the remote box

Get the repo onto the box (clone or `scp`), ensure `rclone.conf` is in place,
then:

```bash
DATASET_REMOTE=gdrive:minecraft-training/sana_video_dataset.tar \
CKPT_REMOTE=gdrive:minecraft-training/checkpoints \
TRAIN_ARGS="--mode lora --spatial_crop_size 512 --max_frames 65 --epochs 3" \
bash deploy/remote_train.sh
```

The script:

1. installs `uv` + `rclone` if missing and runs `uv sync`,
2. downloads + extracts the dataset bundle,
3. launches training (`--save_every_steps 1000` by default — a checkpoint every
   1k samples),
4. runs a background uploader that pushes each completed checkpoint to
   `CKPT_REMOTE` as it is written (so a preempted/crashed box never loses a
   finished checkpoint), and does a final sync at the end.

### How checkpoint upload stays safe

`train.py` writes each checkpoint to `checkpoint-step-<N>/` and then touches a
`.complete` marker. The uploader only syncs directories that have `.complete`
and marks them `.uploaded`, so partially-written checkpoints are never pushed
and nothing is uploaded twice.

## Knobs (env vars for `remote_train.sh`)

| Var | Default | Meaning |
|-----|---------|---------|
| `DATASET_REMOTE` | `gdrive:minecraft-training/sana_video_dataset.tar` | rclone source for the dataset tar |
| `CKPT_REMOTE` | `gdrive:minecraft-training/checkpoints` | rclone dest for checkpoints |
| `OUTPUT_DIR` | `tmp/sana_video_ft` | local checkpoint dir |
| `BUNDLE_DIR` | `data/train_bundle` | dataset extraction dir |
| `SAVE_EVERY` | `1000` | `--save_every_steps` (samples between checkpoints) |
| `UPLOAD_INTERVAL` | `60` | seconds between upload passes |
| `TRAIN_ARGS` | `--mode lora --spatial_crop_size 512 --max_frames 65 --epochs 3` | forwarded to `train.py` |
| `ACCELERATE_ARGS` | (empty) | forwarded to `accelerate launch` (e.g. `--num_processes 2`) |

> VRAM note: 512×512 LoRA needs a large GPU; on 16 GB it OOMs — drop to
> `--spatial_crop_size 256 --max_frames 33`. Full fine-tuning (`--mode full`)
> needs substantially more than 16 GB.

### Logging to Weights & Biases

Add `--report_to wandb` to `TRAIN_ARGS` and provide credentials so the (usually
headless) box can authenticate:

```bash
WANDB_API_KEY=<your-key> \
TRAIN_ARGS="--mode lora --spatial_crop_size 512 --max_frames 65 --epochs 3 \
    --report_to wandb --wandb_project minecraft-sana-video" \
bash deploy/remote_train.sh
```

`train.py` logs per-step loss, learning rate, epoch, and samples-seen, plus an
epoch-average loss. Locally, just add the same `--report_to wandb` flag after
`wandb login`.
