# 🌍 Minecraft World Generator

## 📂 Repository Structure

```text
├── src/
│   ├── config.py                  # Central path config (PROJECT_ROOT-relative).
│   ├── world_wrapper.py           # Core engine: Extracts 3D volumes, biomes, and metadata.
│   ├── fast_volume_extractor.py   # Fast NBT/region volume parsing (numba-accelerated).
│   ├── world_processor.py         # Full conversion + extraction + render pipeline.
│   ├── block2vec.py               # Block embedding training (SGNS + Triton kernel).
│   ├── block_colors.py            # Shared block-ID <-> RGB palette utilities.
│   ├── scripts/                   # Runnable entrypoints (see AGENTS.md for the full table).
│   │   ├── pmc_data_crawler.py    # Scrapes Planet Minecraft project listings.
│   │   ├── pmc_detail_crawler.py  # Deep extraction of map metadata.
│   │   ├── map_downloader.py      # Downloads and extracts map archives.
│   │   ├── process_downloads.py   # Batch world processing driver.
│   │   └── train_sana_video.py    # SANA-Video fine-tuning (LoRA or full).
│   └── notebooks/                 # Interactive exploration and visualization.
├── assets/                        # Global state and data.
├── tests/                         # pytest suite (run: .venv/bin/python -m pytest).
└── pyproject.toml                 # Project dependencies + ruff/pytest config.
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
.venv/bin/python src/scripts/generate_vision_captions.py --model Qwen/Qwen3-VL-8B-Instruct-FP8 --base-url http://localhost:8000/v1 --concurrency 64
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