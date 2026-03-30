import os
import sys
import json
import logging
import argparse
import signal
from pathlib import Path
from typing import Optional, Dict

# Ensure src is in path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src import config
from src.world_processor import WorldProcessor

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("process_downloads")

STATE_FILE = project_root / "assets" / "process_state.json"
DOWNLOADS_DIR = config.DOWNLOADS_DIR


class ProcessState:
    """Manages the process_state.json file for tracking progress."""
    def __init__(self, path: Path = STATE_FILE):
        self._path = path
        self._data: Dict[str, dict] = self._load()

    def _load(self) -> dict:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception as e:
                log.warning(f"Failed to load state file: {e}. Starting fresh.")
        return {}

    def _ensure(self, map_id: str) -> dict:
        return self._data.setdefault(map_id, {
            "status": "pending",
            "error": None,
            "output_dir": None
        })

    def is_done(self, map_id: str) -> bool:
        return self._ensure(map_id).get("status") == "done"

    def is_failed(self, map_id: str) -> bool:
        return self._ensure(map_id).get("status") == "failed"

    def mark_done(self, map_id: str, output_dir: str) -> None:
        entry = self._ensure(map_id)
        entry["status"] = "done"
        entry["output_dir"] = output_dir
        entry["error"] = None

    def mark_failed(self, map_id: str, reason: str) -> None:
        entry = self._ensure(map_id)
        entry["status"] = "failed"
        entry["error"] = reason
        entry["output_dir"] = None

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)


class DownloadProcessor:
    """Iterates through downloads and processes them via WorldProcessor."""

    def __init__(self):
        self.state = ProcessState()
        self.processor = WorldProcessor()
        self.running = True
        
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def _handle_exit(self, signum, frame):
        if self.running:
            log.info("Interrupt received – finishing current world before exit...")
            self.running = False
        else:
            log.warning("Second interrupt – exiting immediately.")
            sys.exit(1)

    def _find_world_root(self, map_dir: Path) -> Optional[Path]:
        """Locates the directory containing level.dat"""
        for p in map_dir.rglob("level.dat*"):
            if p.is_file():
                return p.parent
        return None

    def process_all(self, limit: Optional[int] = None, retry_failed: bool = False):
        """Processes all valid map directories found in tmp/downloads."""
        if not DOWNLOADS_DIR.exists():
            log.error(f"Downloads directory not found: {DOWNLOADS_DIR}")
            return

        map_dirs = [d for d in DOWNLOADS_DIR.iterdir() if d.is_dir()]
        log.info(f"Found {len(map_dirs)} downloaded maps in {DOWNLOADS_DIR.name}")

        self._process_list(map_dirs, limit=limit, retry_failed=retry_failed)

    def process_ids(self, map_ids: list[str], retry_failed: bool = False):
        """Processes only specific map IDs."""
        map_dirs = []
        for map_id in map_ids:
            map_dir = DOWNLOADS_DIR / map_id
            if not map_dir.exists():
                log.warning(f"Downloaded map ID {map_id} not found: {map_dir}")
                continue
            map_dirs.append(map_dir)

        self._process_list(map_dirs, limit=None, retry_failed=retry_failed)

    def _process_list(self, map_dirs: list[Path], limit: Optional[int], retry_failed: bool):
        processed = done = failed = skipped = 0

        for map_dir in map_dirs:
            if not self.running or (limit and processed >= limit):
                break

            map_id = map_dir.name

            # State check
            if self.state.is_done(map_id):
                log.debug(f"[{map_id}] already processed, skipping.")
                skipped += 1
                continue

            if self.state.is_failed(map_id) and not retry_failed:
                log.debug(f"[{map_id}] previously failed, skipping (use --retry-failed to bypass).")
                skipped += 1
                continue

            # Identify world root containing level.dat
            world_root = self._find_world_root(map_dir)
            if not world_root:
                log.warning(f"[{map_id}] ✗ Failed – No level.dat found inside map directory.")
                self.state.mark_failed(map_id, "No level.dat found inside directory")
                self.state.save()
                failed += 1
                processed += 1
                continue

            log.info(f"[{map_id}] Starting Processing on: {world_root.relative_to(DOWNLOADS_DIR)}")
            
            # Execute processor
            result = self.processor.process_world(world_root, map_id, remove_tmp_dirs=True)

            if result["status"] == "success":
                import shutil
                output_dir = Path(result["output_dir"])
                screenshots_dir = output_dir / "screenshots"
                
                if not screenshots_dir.exists() or not any(screenshots_dir.iterdir()):
                    zero_regions_dir = output_dir.parent / "0_regions" / map_id
                    zero_regions_dir.parent.mkdir(parents=True, exist_ok=True)
                    if zero_regions_dir.exists():
                        shutil.rmtree(zero_regions_dir)
                    shutil.move(str(output_dir), str(zero_regions_dir))
                    
                    self.state.mark_failed(map_id, "0 screenshots generated")
                    log.warning(f"[{map_id}] ✗ Moved to 0_regions due to empty screenshots.")
                    failed += 1
                else:
                    self.state.mark_done(map_id, result["output_dir"])
                    log.info(f"[{map_id}] ✓ Successfully processed")
                    done += 1
            else:
                self.state.mark_failed(map_id, result["error"])
                log.error(f"[{map_id}] ✗ Failed to process: {result['error']}")
                failed += 1

            self.state.save()
            processed += 1

        log.info(
            f"\nFinished processing. done={done}  failed={failed}  "
            f"skipped(already-processed)={skipped}"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process downloaded Minecraft maps.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N maps (for testing)")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Attempt to process maps that previously failed")
    parser.add_argument("--ids", nargs="+",
                        help="Only process specific map IDs")
    parser.add_argument("--debug", action="store_true",
                        help="Enable DEBUG log level")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    orchestrator = DownloadProcessor()

    if args.ids:
        orchestrator.process_ids(args.ids, retry_failed=args.retry_failed)
    else:
        orchestrator.process_all(limit=args.limit, retry_failed=args.retry_failed)
