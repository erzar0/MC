# 🌍 Minecraft World Generator

## 📂 Repository Structure

```text
├── src/
│   ├── region_extractor.py    # Core engine: Extracts 3D volumes, biomes, and metadata.
│   ├── scripts/               # Web automation for data collection.
│   │   ├── data_crawler.py    # Scrapes Planet Minecraft project listings.
│   │   └── detail_crawler.py  # Deep extraction of map metadata.
│   ├── playground/            # Experimental scripts.
│   └── notebooks/             # Interactive exploration and visualization.
├── assets/                    # Global state and data.
└── pyproject.toml             # Project dependencies.
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

## Third party tools (not included in code)

- [mcmap](https://github.com/spoutn1k/mcmap) - Rendering of Minecraft maps
- [chunker](https://www.chunker.app/) - Translation of Minecraft world versions

## 📦 Tech Stack

- **Core**: Python 3.12+
- **Map Interaction**: `amulet-core`, `amulet-map-editor`, `pymctranslate`
- **Machine Learning**: `torch`, `diffusers`, `transformers`, `accelerate`
- **Automation**: `selenium`, `undetected-chromedriver`
- **Data Handling**: `numpy`, `pandas`
- **Visualization**: `napari`, `matplotlib`

## ⚖️ License

TWOJA STARA
