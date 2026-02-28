from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).parent.parent

# Third-party tools
SERVER_DIR = PROJECT_ROOT / "tmp" / "third-party" / "server"
CHUNKER_BIN = PROJECT_ROOT / "tmp" / "third-party" / "chunker-cli" / "chunker-cli.exe"
MCMAP_BIN = PROJECT_ROOT / "tmp" / "third-party" / "mcmap" / "mcmap.exe"

# Data Processing
PROCESSED_WORLDS_DIR = PROJECT_ROOT / "tmp" / "processed_worlds"

# Experiment Settings
DOWNLOADS_DIR = PROJECT_ROOT / "tmp" / "downloads"
DEFAULT_WORLD_SOURCE = DOWNLOADS_DIR / "3918278"
DEFAULT_WORLD_NAME = "spectre_village_beta_v1_1_1"
