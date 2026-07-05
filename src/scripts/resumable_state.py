"""Shared JSON-backed resumable state store.

Generalizes the per-item state tracking used by the download/process/crawl
scripts: a flat JSON object mapping item IDs to small status dicts, loaded
on start and saved after mutations, so interrupted runs can resume.

Subclasses set `DEFAULT_ENTRY` to their per-item schema and add domain
mark_* helpers. The serialized JSON schema is exactly whatever the entries
contain — this base class adds no wrapper keys, keeping existing state
files (assets/*.json) fully compatible.
"""

import json
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)


class JsonStateStore:
    """Flat `{item_id: entry_dict}` JSON persistence with resume support."""

    #: Template for a fresh entry; subclasses override.
    DEFAULT_ENTRY: Dict = {"status": "pending", "error": None}

    def __init__(self, path: Path):
        self._path = Path(path)
        self._data: Dict[str, dict] = self._load()

    def _load(self) -> dict:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except (OSError, json.JSONDecodeError) as e:
                logger.warning(f"Failed to load state file {self._path}: {e}. Starting fresh.")
        return {}

    def _ensure(self, item_id: str) -> dict:
        """Returns the entry for `item_id`, creating it from DEFAULT_ENTRY if missing."""
        return self._data.setdefault(item_id, json.loads(json.dumps(self.DEFAULT_ENTRY)))

    def get(self, item_id: str) -> dict:
        return self._data.get(item_id, {})

    def status(self, item_id: str) -> str:
        return self.get(item_id).get("status", "pending")

    def is_done(self, item_id: str) -> bool:
        return self.status(item_id) == "done"

    def is_failed(self, item_id: str) -> bool:
        return self.status(item_id) == "failed"

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)
