# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project overview

**Minecraft World Generator** — a pipeline for collecting Minecraft maps, extracting
3D block volumes / biomes / metadata, and training generative models (Sana / LongSana
video LoRA, block2vec embeddings) on the resulting data.

The workflow is roughly: crawl & download maps → convert/upgrade world versions →
extract 3D volumes and render screenshots → generate captions → build datasets → train
and run inference.

## Environment & tooling

- **Python 3.12+** (`.python-version` pins `3.12`).
- **uv** manages dependencies and the virtualenv. Install with `uv sync`. The main env
  lives in `.venv/`.
- Run code with `uv run python <script>` or `.venv/bin/python <script>`.
- **Separate vLLM env** (`vllm_env/`, `.vllm_venv/`) is intentional — it isolates vLLM
  to avoid package bloat and JIT compatibility issues. Do not merge it into the main
  env. See README for the captioning server command.
- **Git submodules**: `Sana/` (NVlabs/Sana). Treat it as upstream vendored code —
  don't edit it as part of normal tasks. (The former `DC-Gen/` submodule was removed.)
- **Linting/formatting**: `ruff` (config in `pyproject.toml`; run via `uvx ruff check`
  and `uvx ruff format`). Keep new code clean under these settings.
- **Third-party binaries** (`mcmap`, `chunker-cli`) live under `tmp/third-party/` and
  are configured in `src/config.py`. Paths differ per-OS (macOS vs Windows blocks).

## Repository layout

```
src/
  config.py                 # Central path config (PROJECT_ROOT, ASSETS_DIR, PIPELINE_DATA_DIR, ...)
  block2vec.py              # Block embedding training (SGNS + Triton kernel); also a CLI
  common/                   # Shared utilities
    batching.py             #   batch_n() iterable splitter
    block_colors.py         #   block-ID <-> RGB palette (single source of truth)
    llm_utils.py            #   vLLM client factories + strip_thinking_tags()
    resumable_state.py      #   JsonStateStore base for resumable scripts
  world/                    # World data extraction core
    world_wrapper.py        #   Amulet-based high-level world/region access
    fast_volume_extractor.py#   Fast NBT/region volume parsing (numba-accelerated)
    world_processor.py      #   Full conversion + extraction + render pipeline (also a CLI)
  scripts/                  # CLI entrypoints, one package per pipeline stage
    crawling/               #   listing_crawler.py, detail_crawler.py (Planet Minecraft)
    downloading/            #   map_downloader.py
    processing/             #   process_worlds.py (batch WorldProcessor driver)
    captioning/             #   cleanse_descriptions.py, vision_captions.py (vLLM)
    embeddings/             #   negative_buffer.py, benchmark.py, visualize.py
    sana_video/             #   dataset.py, prepare_dataset.py, train.py, inference.py
assets/                     # Immutable reference data (block states, biomes, RGB palettes, grid.png)
data/pipeline/              # Mutable crawl/download state JSON + CSVs (tracked)
data/ (rest), tmp/          # Generated data, downloads, processed worlds (not source)
notebooks/                  # Jupyter notebooks
tests/                      # pytest suite + tests/resources/test_world fixture
```

## Conventions

- **Imports**: modules import via the `src` package (e.g. `from src import config`,
  `from src.world.world_wrapper import WorldWrapper`). Several modules add the project
  root to `sys.path` and use `try/except ImportError` to support both package import and
  direct script execution — preserve this pattern when editing those files. Scripts in
  `src/scripts/<stage>/` use `Path(__file__).resolve().parents[3]` for the project root.
- **Paths**: derive everything from `src/config.py` (`PROJECT_ROOT`-relative;
  `ASSETS_DIR` for reference data, `PIPELINE_DATA_DIR` for mutable state/CSVs). Don't
  hardcode absolute paths.
- **Type hints + docstrings**: core modules (`world_wrapper.py`, etc.) use typing and
  Google-style docstrings. Match that style in core code.
- **Logging**: use the `logging` module (see `world_processor.py`), not bare prints, in
  library/pipeline code.
- `src/scripts/` files are CLI entrypoints (commonly argparse-based) — keep them runnable
  standalone.

## Testing

- Run tests with `.venv/bin/python -m pytest`. (`uv run pytest` currently fails because
  `amulet-faulthandler` does not build under uv — use the existing venv interpreter.)
- pytest is configured in `pyproject.toml` with `testpaths = ["tests"]`.
- Tests live in `tests/` and use the `tests/resources/test_world` Minecraft world as a
  fixture. Test files are also runnable directly (`pytest.main([__file__])`).
- When changing extraction logic (`world_wrapper.py`, `fast_volume_extractor.py`), run
  the relevant tests — they assert concrete volume shapes (e.g. 512×512).

## File reference

Pipeline-ordered. Scripts are run from the repo root; the canonical interpreter is
`.venv/bin/python` (or `uv run python`). Many scripts hardcode `assets/` and `tmp/`
paths internally.

### Core library (`src/`)

| File | Description | Usage |
|------|-------------|-------|
| `config.py` | Central path config. Defines `PROJECT_ROOT`, `ASSETS_DIR`, `PIPELINE_DATA_DIR` and derived paths (`SERVER_DIR`, `CHUNKER_BIN`, `MCMAP_BIN`, `PROCESSED_WORLDS_DIR`, `DOWNLOADS_DIR`). Has commented-out Windows binary paths. | Imported as `from src import config`. Edit the OS block to switch macOS/Windows binaries. |
| `world/world_wrapper.py` | `WorldWrapper` — high-level Amulet wrapper to load a world, discover valid `.mca` regions, and extract 3D block volumes, heightmaps, and inhabited times. Also the `_StringRegistry`-based `BlockStates` and `Biomes` registries mapping block/biome strings to global IDs. Enforces world height ≤ 384. | `from src.world.world_wrapper import WorldWrapper`, then `get_region_volume(rx, rz)`. |
| `world/fast_volume_extractor.py` | `FastVolumeParser` — fast, low-level region/NBT decoder that unpacks block-state palettes directly (numba-accelerated via `_unpack_numba` when available, pure-Python fallback). The performance path behind `WorldWrapper`. | Used internally by `world_wrapper.py`; not a standalone entrypoint. |
| `world/world_processor.py` | `WorldProcessor` — full per-world pipeline: MCR→MCA conversion → version upgrade to 1.19.2 (Chunker) → volume/metadata extraction (WorldWrapper) → screenshot render (mcmap). | `python src/world/world_processor.py <world_source_path> [world_name]` (defaults name to `greenfield`). |
| `block2vec.py` | Block2Vec skip-gram-with-negative-sampling embeddings over voxel volumes, with a fused Triton update kernel (`_sgns_kernel`), `SpatialMinecraftDataset`, and `train_block2vec_from_volumes()`. | `python src/block2vec.py --neg_buffer <ids.pt> [--volumes DIR] [--dim 128] [--epochs N] [--neighbor_mode face6\|cube26] [--save_dir tmp/checkpoints] [--seed N]`. `--neg_buffer` is required. |
| `common/batching.py` | `batch_n(iterable, batch_count)` utility — splits an iterable into ~even batches (generators supported). | Imported helper. |
| `common/block_colors.py` | Shared block-ID <-> RGB palette: `load_block_states()` and `load_id2rgb()` (also returns air block IDs). Single source of truth for SANA-Video training and inference decoding. | `from src.common.block_colors import load_id2rgb`. |
| `common/llm_utils.py` | Shared vLLM helpers: `strip_thinking_tags()`, `make_vllm_client()`, `make_async_vllm_client()`. | Imported by the `captioning/` scripts. |
| `common/resumable_state.py` | `JsonStateStore` — flat `{id: entry}` JSON persistence base for resumable scripts. `DownloadState` (map_downloader) and `ProcessState` (process_worlds) subclass it. | Imported helper. |

### Scripts (`src/scripts/<stage>/`) — pipeline-ordered

| File | Description | Usage |
|------|-------------|-------|
| `crawling/listing_crawler.py` | Selenium/undetected-chromedriver crawler over Planet Minecraft listings → `data/pipeline/pmc_data.csv`. Resumable via cursor state JSON. | `python src/scripts/crawling/listing_crawler.py [--platform 1\|2] [--years ...] [--output ...] [--chrome-version N]`. |
| `crawling/detail_crawler.py` | Deep per-project metadata crawler. Reads `data/pipeline/pmc_data.csv` → `data/pipeline/pmc_details.csv`, resumable via `pmc_details_crawl_state.json` (404s recorded there, input CSV untouched). | `python src/scripts/crawling/detail_crawler.py [--input ...] [--output ...] [--chrome-version N]`. |
| `downloading/map_downloader.py` | Downloads map archives from `pmc_data_cleansed.csv`. Pluggable `UrlResolver`s (MediaFire, Dropbox, PlanetMinecraft, direct), archive extraction (zip/rar/7z/tar), resumable state in `data/pipeline/map_download_state.json`, output to `tmp/downloads/<id>/`. | `python src/scripts/downloading/map_downloader.py [--limit N] [--ids ...] [--year/-from/-to] [--retry-failed] [--no-skip-done] [--debug]`. |
| `processing/process_worlds.py` | Batch driver that runs `WorldProcessor` over downloaded maps, with multiprocessing and resumable state in `data/pipeline/process_state.json`. | `python src/scripts/processing/process_worlds.py [--limit N] [--ids ...] [--workers K] [--retry-failed] [--debug]`. |
| `captioning/cleanse_descriptions.py` | Cleans titles/tags/descriptions from crawled CSVs via a local vLLM (OpenAI-compatible) server, one focused call per field. | `python src/scripts/captioning/cleanse_descriptions.py [--batch-size 64] [--concurrency 16] [--max-rows 0] [--resume] [--model ...] [--base-url ...]`. Needs a running vLLM server. |
| `captioning/vision_captions.py` | Async caption generator for JXL screenshots via a local vLLM vision model (AsyncOpenAI). | `.venv/bin/python src/scripts/captioning/vision_captions.py [--model ...] [--base-url ...] [--concurrency 128] [--max-images 0] [--delay 0]`. See README for the server command. |
| `embeddings/negative_buffer.py` | Aggregates block frequencies from all `metadata.json` files to build the negative-sampling buffer (`.pt`) consumed by `block2vec.py`. | `python src/scripts/embeddings/negative_buffer.py [--volumes-dir ...] [--output ...]`. |
| `embeddings/benchmark.py` | Throughput benchmark for `Block2Vec` / `SpatialMinecraftDataset` across batch sizes. | `python src/scripts/embeddings/benchmark.py`. |
| `embeddings/visualize.py` | Loads trained block embeddings + `block_states.txt`, reduces with PaCMAP/PCA, renders Plotly/matplotlib plots and an RGB LUT. | `python src/scripts/embeddings/visualize.py [--checkpoint ...]` (default: newest ckpt in `tmp/checkpoints`). |
| `sana_video/prepare_dataset.py` | Builds a JSONL manifest pairing each `.b2frame` volume with caption orientations from `tmp/processed_worlds/cleansed/`. | `python src/scripts/sana_video/prepare_dataset.py [--cleansed_dir ...] [--output tmp/sana_video_manifest.jsonl]`. |
| `sana_video/dataset.py` | `MinecraftVideoDataset` — manifest-based dataset turning `.b2frame` block-ID volumes into RGB pseudo-video tensors (C, F, H, W) in [-1, 1]; Y-layers are frames, surface-aware frame windowing, random spatial crop and caption orientation. | Imported by `sana_video/train.py`; not a standalone entrypoint. |
| `sana_video/train.py` | Fine-tunes pretrained SANA-Video 2B on voxel "pseudo-video" volumes via Accelerate. `--mode lora` (PEFT adapters, default) or `--mode full` (all transformer weights). `max_frames` must be 4n+1 (Wan VAE). | `accelerate launch src/scripts/sana_video/train.py --manifest ... [--mode lora\|full] [--lora_rank 8] [--epochs 3] [--spatial_crop_size 512] [--max_frames 65] [--output_dir tmp/sana_video_ft] [--report_to wandb] [--wandb_project ...]`. |
| `sana_video/inference.py` | Generates a 3D voxel grid from a text prompt with SANA-Video + fine-tuned weights (LoRA adapter or full transformer), snaps to nearest block via KD-tree, saves `.npy`. | `.venv/bin/python src/scripts/sana_video/inference.py --prompt "..." [--lora_path ...] [--transformer_path ...] [--output_npy ...] [--frames 64]`. |

### Data directories

- **`assets/`** — immutable reference data: `block_states.txt`, `biomes.txt`,
  `block_state2rgb.csv`, `block_type2rgb.json`, `grid.png`. The block/biome registries
  append to these files when new states are encountered — don't edit them by hand.
- **`data/pipeline/`** — mutable, tracked pipeline state and outputs: crawl/download
  **state JSON** (`*_crawl_state.json`, `map_download_state.json`, `process_state.json`)
  and **data CSVs** (`pmc_data*.csv`, `pmc_details.csv`, `pmc_descriptions_cleaned.csv`).
  Scripts resume from these — be careful editing them by hand.

### Notebooks (`notebooks/`)

`main.ipynb` — interactive exploration / visualization. Open in Jupyter.

### Tests (`tests/`)

`test_world_wrapper.py`, `test_fast_volume_extractor.py`, `test_pipeline.py`, run against
the `tests/resources/test_world` fixture. Run with `.venv/bin/python -m pytest`.
Also: `test_batching.py` and `test_shared_utils.py` (shared utility + state-schema tests).

## Notes for agents

- `numba` is an optional accel dependency (`HAS_NUMBA` guard) — code must work without it.
- `numpy` is pinned `<2`; don't introduce numpy 2.x-only APIs.
- Large generated artifacts (`tmp/`, `data/`, `*.mp4`, downloaded worlds) are not source —
  don't commit them, and check `.gitignore` before adding files.
- Only commit or push when explicitly asked.
