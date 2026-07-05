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
- **Git submodules**: `Sana/` (NVlabs/Sana) and `DC-Gen/` (dc-ai-projects/DC-Gen).
  Treat these as upstream vendored code — don't edit them as part of normal tasks.
- **Third-party binaries** (`mcmap`, `chunker-cli`) live under `tmp/third-party/` and
  are configured in `src/config.py`. Paths differ per-OS (macOS vs Windows blocks).

## Repository layout

```
src/
  config.py                 # Central path config (PROJECT_ROOT-relative)
  world_wrapper.py          # Amulet-based high-level world/region access
  fast_volume_extractor.py  # Fast NBT/region volume parsing (numba-accelerated)
  world_processor.py        # Full conversion + extraction + render pipeline
  block2vec.py, batching.py # Embedding training utilities
  scripts/                  # Runnable entrypoints (crawling, downloading, training, inference)
  longsana_experiment/      # Numbered experiment stages (01_..05_)
  playground/               # Experimental / throwaway scripts
assets/                     # Global state JSON, block state lists
data/, tmp/                 # Generated data, downloads, processed worlds (not source)
tests/                      # pytest suite + tests/resources/test_world fixture
```

## Conventions

- **Imports**: modules import via the `src` package (e.g. `from src import config`,
  `from src.world_wrapper import WorldWrapper`). Several modules add the project root to
  `sys.path` and use `try/except ImportError` to support both package import and direct
  script execution — preserve this pattern when editing those files.
- **Paths**: derive everything from `src/config.py` (`PROJECT_ROOT`-relative). Don't
  hardcode absolute paths.
- **Type hints + docstrings**: core modules (`world_wrapper.py`, etc.) use typing and
  Google-style docstrings. Match that style in core code.
- **Logging**: use the `logging` module (see `world_processor.py`), not bare prints, in
  library/pipeline code.
- `src/scripts/` files are CLI entrypoints (commonly argparse-based) — keep them runnable
  standalone.

## Testing

- Run tests with `uv run pytest` (or `.venv/bin/python -m pytest`).
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
| `config.py` | Central path config. Defines `PROJECT_ROOT` and all derived paths (`SERVER_DIR`, `CHUNKER_BIN`, `MCMAP_BIN`, `PROCESSED_WORLDS_DIR`, `DOWNLOADS_DIR`). Has commented-out Windows binary paths. | Imported as `from src import config`. Edit the OS block to switch macOS/Windows binaries. |
| `world_wrapper.py` | `WorldWrapper` — high-level Amulet wrapper to load a world, discover valid `.mca` regions, and extract 3D block volumes, heightmaps, and inhabited times. Also `BlockStates` and `Biomes` registries that map block/biome strings to global IDs. Enforces world height ≤ 384. | `WorldWrapper(world_path)` then `get_region_volume(rx, rz)`. |
| `fast_volume_extractor.py` | `FastVolumeParser` — fast, low-level region/NBT decoder that unpacks block-state palettes directly (numba-accelerated via `_unpack_numba` when available, pure-Python fallback). The performance path behind `WorldWrapper`. | Used internally by `world_wrapper.py`; not a standalone entrypoint. |
| `world_processor.py` | `WorldProcessor` — full per-world pipeline: MCR→MCA conversion → version upgrade to 1.19.2 (Chunker) → volume/metadata extraction (WorldWrapper) → screenshot render (mcmap). | `python src/world_processor.py <world_source_path> [world_name]` (defaults name to `greenfield`). |
| `block2vec.py` | Block2Vec skip-gram-with-negative-sampling embeddings over voxel volumes, with a fused Triton update kernel (`_sgns_kernel`), `SpatialMinecraftDataset`, and `train_block2vec_from_volumes()`. | `python src/block2vec.py --neg_buffer <ids.pt> [--volumes DIR] [--dim 128] [--epochs N] [--neighbor_mode face6\|cube26] [--save_dir tmp/checkpoints]`. `--neg_buffer` is required. |
| `batching.py` | `batch_n(iterable, n)` utility — splits an iterable into ~even batches. | Imported helper. |
| `notebooks/main.ipynb` | Interactive exploration / visualization notebook. | Open in Jupyter. |

### Scripts (`src/scripts/`) — data collection → processing → training → inference

| File | Description | Usage |
|------|-------------|-------|
| `pmc_data_crawler.py` | Selenium/undetected-chromedriver crawler over Planet Minecraft listings → `assets/pmc_data*.csv`. Resumable via state JSON; `PLATFORM=2` (bedrock). | `python src/scripts/pmc_data_crawler.py` (config via constants at top of file). |
| `pmc_detail_crawler.py` | Deep per-project metadata crawler. Reads `assets/pmc_data.csv` → `assets/pmc_details.csv`, resumable via `pmc_details_crawl_state.json`. | `python src/scripts/pmc_detail_crawler.py`. |
| `cleanse_descriptions.py` | Cleans titles/tags/descriptions from crawled CSVs via a local vLLM (OpenAI-compatible) server, one focused call per field. | `python -m src.scripts.cleanse_descriptions [--batch-size 64] [--concurrency 16] [--max-rows 0] [--resume] [--model ...] [--base-url ...]`. Needs a running vLLM server. |
| `map_downloader.py` | Downloads map archives from `pmc_data_cleansed.csv`. Pluggable `UrlResolver`s (MediaFire, Dropbox, PlanetMinecraft, direct), archive extraction (zip/rar/7z/tar), resumable state in `assets/map_download_state.json`, output to `tmp/downloads/<id>/`. | `python src/scripts/map_downloader.py [--limit N] [--ids ...] [--year/-from/-to] [--retry-failed] [--no-skip-done] [--debug]`. |
| `process_downloads.py` | Batch driver that runs `WorldProcessor` over downloaded maps, with multiprocessing and resumable state. | `python src/scripts/process_downloads.py [--limit N] [--ids ...] [--workers K] [--retry-failed] [--debug]`. |
| `generate_vision_captions.py` | Async caption generator for JXL screenshots via a local vLLM vision model (AsyncOpenAI). | `.venv/bin/python src/scripts/generate_vision_captions.py [--model ...] [--base-url ...] [--concurrency 128] [--max-images 0] [--delay 0]`. See README for the server command. |
| `generate_negative_buffer.py` | Aggregates block frequencies from all `metadata.json` files to build the negative-sampling buffer (`.pt`) consumed by `block2vec.py`. | `python src/scripts/generate_negative_buffer.py`. |
| `benchmark_block2vec.py` | Throughput benchmark for `Block2Vec` / `SpatialMinecraftDataset` across batch sizes. | `python src/scripts/benchmark_block2vec.py`. |
| `visualize_embeddings.py` | Loads trained block embeddings + `block_states.txt`, reduces with PaCMAP/PCA, renders Plotly/matplotlib plots. | `python src/scripts/visualize_embeddings.py` (edit hardcoded checkpoint path near top). |
| `prepare_sana_video_dataset.py` | Builds a JSONL manifest pairing each `.b2frame` volume with caption orientations from `tmp/processed_worlds/cleansed/`. | `python src/scripts/prepare_sana_video_dataset.py [--cleansed_dir ...] [--output tmp/sana_video_manifest.jsonl]`. |
| `sana_video_dataset.py` | `MinecraftVideoDataset` — manifest-based dataset turning `.b2frame` block-ID volumes into RGB pseudo-video tensors (C, F, H, W) in [-1, 1]; Y-layers are frames, surface-aware frame windowing, random spatial crop and caption orientation. | Imported by `train_sana_video.py`; not a standalone entrypoint. |
| `train_sana_video.py` | Fine-tunes pretrained SANA-Video 2B on voxel "pseudo-video" volumes via Accelerate. `--mode lora` (PEFT adapters, default) or `--mode full` (all transformer weights). `max_frames` must be 4n+1 (Wan VAE). | `accelerate launch src/scripts/train_sana_video.py --manifest ... [--mode lora\|full] [--lora_rank 8] [--epochs 3] [--spatial_crop_size 512] [--max_frames 65] [--output_dir tmp/sana_video_ft]`. |
| `inference_sana_video.py` | Generates a 3D voxel grid from a text prompt with SANA-Video + fine-tuned weights (LoRA adapter or full transformer), snaps to nearest block via KD-tree, saves `.npy`. | `.venv/bin/python src/scripts/inference_sana_video.py --prompt "..." [--lora_path ...] [--transformer_path ...] [--output_npy ...] [--frames 64]`. |

### Assets (`assets/`)

Crawl/download/process **state JSON** (`*_crawl_state.json`, `map_download_state.json`,
`process_state.json`) and **data CSVs** (`pmc_data*.csv`, `pmc_details.csv`,
`pmc_descriptions_cleaned.csv`). Lookup tables: `block_states.txt`, `biomes.txt`,
`block_state2rgb.csv`, `block_type2rgb.json`. These are checked-in pipeline state — be
careful editing them by hand.

### Tests (`tests/`)

`test_world_wrapper.py`, `test_fast_volume_extractor.py`, `test_pipeline.py`, run against
the `tests/resources/test_world` fixture. Run with `uv run pytest`.

> Note: the git-status snapshot at session start referenced `src/longsana_experiment/`,
> `src/playground/`, and `src/scripts/train.py`, which are **not present** on disk now.
> The tables above describe the actual current tree.

## Notes for agents

- `numba` is an optional accel dependency (`HAS_NUMBA` guard) — code must work without it.
- `numpy` is pinned `<2`; don't introduce numpy 2.x-only APIs.
- Large generated artifacts (`tmp/`, `data/`, `*.mp4`, downloaded worlds) are not source —
  don't commit them, and check `.gitignore` before adding files.
- Only commit or push when explicitly asked.
