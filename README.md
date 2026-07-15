# 🌍 Minecraft World Generator

## 📂 Repository Structure

```text
├── src/
│   ├── config.py                # Central path config (PROJECT_ROOT-relative).
│   ├── block2vec.py             # Block embedding training (SGNS + Triton kernel).
│   ├── common/                  # Shared utilities (batching, block_colors, llm_utils, resumable_state).
│   ├── world/                   # World data extraction core:
│   │   ├── world_wrapper.py     #   Amulet wrapper: 3D volumes, biomes, metadata.
│   │   ├── fast_volume_extractor.py  # Fast NBT/region parsing (numba-accelerated).
│   │   ├── world_processor.py   #   Full conversion + extraction + render pipeline.
│   │   └── world_builder.py     #   Voxel block-ID grid -> playable Java 1.19.2 world.
│   └── scripts/                 # CLI entrypoints, one package per pipeline stage:
│       ├── crawling/            #   listing_crawler.py, detail_crawler.py (PMC)
│       ├── downloading/         #   map_downloader.py
│       ├── processing/          #   process_worlds.py (batch WorldProcessor driver)
│       ├── captioning/          #   cleanse_descriptions.py, vision_captions.py (vLLM)
│       ├── embeddings/          #   negative_buffer.py, benchmark.py, visualize.py
│       └── sana_video/          #   dataset.py, prepare_dataset.py, train.py, inference.py, generate_world.py
├── assets/                      # Immutable reference data (block states, palettes).
├── data/pipeline/               # Mutable crawl/download state + CSVs.
├── notebooks/                   # Interactive exploration and visualization.
├── tests/                       # pytest suite (run: .venv/bin/python -m pytest).
└── pyproject.toml               # Project dependencies + ruff/pytest config.
```

## ⚡ Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
# Clone the repository
git clone https://github.com/erzar0/minecraft-world-generator.git
cd minecraft-world-generator

# Install dependencies and create a virtual environment
uv sync
```

## Third party tools

- [mcmap](https://github.com/spoutn1k/mcmap) - Rendering of Minecraft maps
- [chunker](https://www.chunker.app/) - Translation of Minecraft world versions

## 🏗️ Generating Playable Worlds from a Trained Checkpoint

`src/scripts/sana_video/generate_world.py` runs the whole generation pipeline: SANA-Video
inference from a fine-tuned checkpoint → KD-tree snap to block IDs → playable Java 1.19.2
world (`level.dat` + `region/*.mca`, built by `src/world/world_builder.py`) → isometric PNG
render (mcmap) + mp4 of the raw output (Y-layers as frames) → upload of the world zip,
render, and mp4 to Google Drive (rclone).

```bash
# Full run: prompt -> world -> render -> upload
.venv/bin/python src/scripts/sana_video/generate_world.py \
    --prompt "A medieval watchtower built of cobblestone and oak wood" \
    --lora_path tmp/sana_video_ft/checkpoint-epoch-3 \
    --gdrive-remote "gdrive:minecraft-training/generated"

# Reuse an existing inference grid (.npy), skip the upload
.venv/bin/python src/scripts/sana_video/generate_world.py \
    --npy tmp/generated_structure.npy --skip-upload
```

Notes:
- Outputs land in `tmp/generated/<name>/`: `grid.npy`, `world/`, `<name>.nw.png`, `<name>.mp4`, `<name>.zip`.
  The zip contains a `world/` folder you can drop straight into Minecraft `saves/`.
- Use `--transformer_path` instead of `--lora_path` for full fine-tune checkpoints.
- `--gdrive-remote` must name a configured rclone remote (`rclone listremotes`); the same
  convention as `deploy/remote_train.sh`.
- Renders default to no `-lighting` (generated worlds carry no light data, which would
  darken the image); opt in with `--mcmap-lighting`.

## 🖼️ Caption Generation (vLLM)

To generate vision captions for JXL screenshots, we use a separate vLLM environment to prevent package bloat and JIT compilation issues:

### 1. Setup the separate vLLM environment
```bash
cd vllm_env
uv sync
cd ..
```

### 2. Run the vLLM Server
Run the local vLLM server with PyTorch's native sampler enabled (bypassing FlashInfer JIT compatibility bugs on WSL) and optimized memory bounds for the RTX 4070 Ti SUPER:
```bash
CUDA_HOME=/usr FLASHINFER_NVCC_THREADS=8 VLLM_USE_FLASHINFER_SAMPLER=0 .vllm_venv/bin/vllm serve Qwen/Qwen3-VL-8B-Instruct-FP8 --max-model-len 6144 --gpu-memory-utilization 0.90 --port 8000
```

### 3. Run the Captioning Script
Once the server finishes loading and starts listening, run the captioning script using your main project virtual environment:
```bash
.venv/bin/python src/scripts/captioning/vision_captions.py --model Qwen/Qwen3-VL-8B-Instruct-FP8 --base-url http://localhost:8000/v1 --concurrency 64
```


## 📦 Tech Stack

- **Core**: Python 3.12+
- **Map Interaction**: `amulet-core`, `amulet-map-editor`, `pymctranslate`
- **Machine Learning**: `torch`, `diffusers`, `transformers`, `accelerate`
- **Automation**: `selenium`, `undetected-chromedriver`
- **Data Handling**: `numpy`, `pandas`
- **Visualization**: `napari`, `matplotlib`

## ⚖️ License

TWOJA STARA