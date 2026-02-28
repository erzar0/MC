from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).parent.parent

# Third-party tools
SERVER_DIR = PROJECT_ROOT / "tmp" / "third-party" / "server"

# MacOS
CHUNKER_BIN = PROJECT_ROOT / "tmp" / "third-party" / "chunker-cli" / "bin" / "chunker-cli"
MCMAP_BIN = PROJECT_ROOT / "tmp" / "third-party" / "mcmap" / "build" / "bin" / "mcmap"

# Windows
# CHUNKER_BIN = PROJECT_ROOT / "tmp" / "third-party" / "chunker-cli" / "chunker-cli.exe"
# MCMAP_BIN = PROJECT_ROOT / "tmp" / "third-party" / "mcmap" / "mcmap.exe"

# Data Processing
PROCESSED_WORLDS_DIR = PROJECT_ROOT / "tmp" / "processed_worlds"

# Experiment Settings
DOWNLOADS_DIR = PROJECT_ROOT / "tmp" / "downloads"
