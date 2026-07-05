"""
map_downloader.py
-----------------
Downloads Minecraft world maps described in pmc_data_cleansed.csv.

Architecture
    UrlResolver         – base class; one subclass per supported host
    Extractor           – extracts zip / rar / 7z / tar archives
    DownloadState       – loads & saves per-map state to JSON
    MapDownloader       – orchestrates the full pipeline

Supported resolvers
    MediaFireResolver   – resolves MediaFire page → CDN direct link
    DropboxResolver     – rewrites dl=0 → dl=1
    PlanetMinecraftResolver – appends /download/ to project URLs
    DirectResolver      – passes direct archive URLs through unchanged

State file:  assets/map_download_state.json
Downloads:   tmp/downloads/<map_id>/<filename>
"""

from __future__ import annotations

import argparse
import csv
import gzip
import logging
import re
import shutil
import signal
import sys
import tarfile
import time
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

import py7zr
import rarfile
import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.scripts.resumable_state import JsonStateStore

# ---------------------------------------------------------------------------
# Paths & logging
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
ASSETS_DIR = REPO_ROOT / "assets"
INPUT_CSV = ASSETS_DIR / "pmc_data_cleansed.csv"
STATE_FILE = ASSETS_DIR / "map_download_state.json"
DOWNLOAD_DIR = REPO_ROOT / "tmp" / "downloads"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("map_downloader")

ARCHIVE_EXTENSIONS = (".zip", ".rar", ".7z", ".tar.gz", ".tgz", ".tar.bz2", ".tar", ".gz")

# Stop downloading when tmp/downloads exceeds this size
MAX_DOWNLOAD_DIR_BYTES = 50 * 1024**3

# Only these PMC categories contain worlds worth extracting
ALLOWED_CATEGORIES = {
    "Environment | Landscaping Map",
    "Complex Map",
    "Complex",
    "Land Structure Map",
    "Land Structure",
    "Underground Structure Map",
    "Underground Structure",
    "Water Structure Map",
    "Water Structure",
}


def _dir_size(path: Path) -> int:
    """Total size in bytes of all files under `path` (0 if it doesn't exist)."""
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


# ---------------------------------------------------------------------------
# Shared HTTP session
# ---------------------------------------------------------------------------


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


SESSION = _build_session()

# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

_MIRROR_RE = re.compile(r"\(https?://[^)]+\)")


def extract_urls(mirror_field: str) -> list[str]:
    """Return all raw URLs from a ``download_mirrors`` CSV cell."""
    return [m.group(0)[1:-1].strip() for m in _MIRROR_RE.finditer(mirror_field)]


def _netloc(url: str) -> str:
    return urlparse(url).netloc.lower()


def _is_archive_path(path: str) -> bool:
    path = path.lower()
    return any(path.endswith(ext) for ext in ARCHIVE_EXTENSIONS)


# ---------------------------------------------------------------------------
# URL Resolvers
# ---------------------------------------------------------------------------


class UrlResolver(ABC):
    """Converts a share/page URL into a direct downloadable URL."""

    @abstractmethod
    def can_handle(self, url: str) -> bool:
        """Return True if this resolver supports the given URL."""

    @abstractmethod
    def resolve(self, url: str) -> Optional[str]:
        """Return a direct download URL, or None on failure."""


class MediaFireResolver(UrlResolver):
    """
    Resolves MediaFire page/share URLs to direct CDN download links.

    Tries several extraction strategies because MediaFire's page structure
    has changed over the years.
    """

    _CDN_PATTERNS = [
        # Modern: explicit CDN anchor
        r'href=["\']?(https?://download\d*\.mediafire\.com/[^"\'>\s&]+)',
        # JSON-embedded download_url field
        r'"download_url"\s*:\s*"(https?://[^"]+)"',
        # downloadButton anchor (two attribute orderings)
        r'id=["\']downloadButton["\'][^>]*href=["\']([^"\']+)["\']',
        r'href=["\']([^"\']+)["\'][^>]*id=["\']downloadButton["\']',
        # JS redirect with archive extension
        r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+\.(?:zip|rar|7z|tar|gz))',
        # Broad fallback: any download sub-domain
        r'(https?://download[a-z0-9]*\.mediafire\.com/[^\s"\'<>&]+)',
    ]

    def can_handle(self, url: str) -> bool:
        return "mediafire.com" in _netloc(url)

    def resolve(self, url: str) -> Optional[str]:
        page_url = self._normalise_page_url(url)
        try:
            resp = SESSION.get(page_url, timeout=25, allow_redirects=True)
            resp.raise_for_status()
        except Exception as exc:
            log.debug(f"MediaFire: page fetch failed ({exc})")
            return None

        # Strategy 1 – search HTML for CDN link
        html = resp.text
        for pat in self._CDN_PATTERNS:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                direct = m.group(1).replace("&amp;", "&").replace("\\u002F", "/")
                log.debug(f"MediaFire: resolved via pattern '{pat[:40]}'")
                return direct

        # Strategy 2 – redirect history
        for r in resp.history:
            loc = r.headers.get("Location", "")
            if "download" in loc and "mediafire" in loc:
                return loc

        # Strategy 3 – final URL is itself a direct download
        if _is_archive_path(urlparse(resp.url).path):
            return resp.url

        log.debug(f"MediaFire: could not resolve {url}")
        return None

    @staticmethod
    def _normalise_page_url(url: str) -> str:
        if re.search(r"/file/[^/]+", url):
            return re.sub(r"/file$", "", url.rstrip("/")) + "/file"
        if re.search(r"/\?[a-z0-9]+$", url, re.IGNORECASE):
            return url  # old-style /?key short link
        return url.rstrip("/") + "/file"


class DropboxResolver(UrlResolver):
    """Rewrites Dropbox share URLs to force a direct download."""

    def can_handle(self, url: str) -> bool:
        return "dropbox.com" in _netloc(url)

    def resolve(self, url: str) -> Optional[str]:
        if "dl=0" in url:
            return url.replace("dl=0", "dl=1")
        if "dl=1" in url:
            return url
        sep = "&" if "?" in url else "?"
        return url + sep + "dl=1"


class PlanetMinecraftResolver(UrlResolver):
    """Appends /download/ to PlanetMinecraft project URLs and resolves redirects."""

    def __init__(self):
        self.chain = None  # Set by ResolverChain later

    def can_handle(self, url: str) -> bool:
        return "planetminecraft.com" in _netloc(url)

    def resolve(self, url: str) -> Optional[str]:
        if "/download/" not in url:
            url = url.rstrip("/") + "/download/"

        try:
            # PMC download URLs often redirect to a third-party host (like MediaFire)
            # Or they serve the file directly if hosted on PMC.
            # Using GET because HEAD sometimes returns 403 or doesn't include the Location header
            resp = SESSION.get(url, timeout=15, allow_redirects=False)
            if resp.status_code in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location")
                if loc and "planetminecraft.com" not in _netloc(loc):
                    # Redirects to another host! Let the chain handle it if possible.
                    if self.chain:
                        return self.chain.resolve(loc)
                    return loc
        except Exception as e:
            log.debug(f"PlanetMinecraft resolve error: {e}")
            pass

        return url


class DirectResolver(UrlResolver):
    """Passes URLs that already point directly to an archive file."""

    def can_handle(self, url: str) -> bool:
        return _is_archive_path(urlparse(url).path)

    def resolve(self, url: str) -> Optional[str]:
        return url


class GoogleDriveResolver(UrlResolver):
    """Resolves Google Drive URLs."""

    def can_handle(self, url: str) -> bool:
        return "drive.google.com" in _netloc(url)

    def resolve(self, url: str) -> Optional[str]:
        file_id = None
        match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
        if match:
            file_id = match.group(1)
        else:
            match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
            if match:
                file_id = match.group(1)
            else:
                match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
                if match:
                    file_id = match.group(1)

        if not file_id:
            return None

        base_url = f"https://drive.google.com/uc?export=download&id={file_id}"

        try:
            resp = SESSION.get(base_url, stream=True, timeout=25, allow_redirects=False)
            if resp.status_code in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location")
                if loc:
                    return loc
            elif resp.status_code == 200:
                for key, value in resp.cookies.items():
                    if key.startswith("download_warning"):
                        return f"{base_url}&confirm={value}"
                return base_url
        except Exception as e:
            log.debug(f"Google Drive resolve error: {e}")
            pass

        return base_url


class ResolverChain:
    """
    Tries each registered resolver in order and returns the first
    successful direct URL, or None if none could handle the input.
    """

    def __init__(self, resolvers: list[UrlResolver]):
        self._resolvers = resolvers
        for r in self._resolvers:
            if hasattr(r, "chain"):
                r.chain = self

    def resolve(self, url: str, depth: int = 0) -> Optional[str]:
        if depth > 5:
            return None
        for resolver in self._resolvers:
            if resolver.can_handle(url):
                result = resolver.resolve(url)
                if result:
                    return result
                # resolver matched but failed – don't try others for this host
                return None
        log.debug(f"No resolver matched URL: {url}")
        return None


# Default chain used by MapDownloader
DEFAULT_RESOLVER_CHAIN = ResolverChain(
    [
        MediaFireResolver(),
        DropboxResolver(),
        PlanetMinecraftResolver(),
        GoogleDriveResolver(),
        DirectResolver(),
    ]
)

# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


class Extractor:
    """Extracts archives into a destination directory."""

    def extract(self, archive: Path, dest_dir: Path) -> bool:
        """Return True on success, False on unsupported type or error."""
        name = archive.name.lower()
        try:
            if name.endswith(".zip"):
                self._extract_zip(archive, dest_dir)
            elif name.endswith(".rar"):
                self._extract_rar(archive, dest_dir)
            elif name.endswith(".7z"):
                self._extract_7z(archive, dest_dir)
            elif name.endswith((".tar.gz", ".tgz", ".tar.bz2", ".tar")):
                self._extract_tar(archive, dest_dir)
            elif name.endswith(".gz"):
                self._extract_gz(archive, dest_dir)
            else:
                log.warning(f"  Extractor: unknown archive type '{archive.suffix}'")
                return False
            log.info(f"  Extracted → {dest_dir}")
            return True
        except Exception as exc:
            log.warning(f"  Extraction failed for {archive.name}: {exc}")
            return False

    @staticmethod
    def _extract_zip(archive: Path, dest: Path) -> None:
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(dest)

    @staticmethod
    def _extract_rar(archive: Path, dest: Path) -> None:
        with rarfile.RarFile(archive) as rf:
            rf.extractall(dest)

    @staticmethod
    def _extract_7z(archive: Path, dest: Path) -> None:
        with py7zr.SevenZipFile(archive, mode="r") as szf:
            szf.extractall(dest)

    @staticmethod
    def _extract_tar(archive: Path, dest: Path) -> None:
        with tarfile.open(archive) as tf:
            tf.extractall(dest)

    @staticmethod
    def _extract_gz(archive: Path, dest: Path) -> None:
        out_path = dest / archive.stem
        with gzip.open(archive, "rb") as gz_in, open(out_path, "wb") as fh:
            shutil.copyfileobj(gz_in, fh)


# ---------------------------------------------------------------------------
# DownloadState
# ---------------------------------------------------------------------------


class DownloadState(JsonStateStore):
    """
    Persists per-map download state to a JSON file.

    Schema per entry::

        {
            "status":        "pending" | "downloading" | "done" | "failed",
            "attempted_urls": [...],
            "error":         "<message>" | null,
            "file":          "<relative path>" | null
        }
    """

    DEFAULT_ENTRY = {
        "status": "pending",
        "attempted_urls": [],
        "error": None,
        "file": None,
    }

    def __init__(self, path: Path = STATE_FILE):
        super().__init__(path)

    def get_attempted_urls(self, map_id: str) -> list[str]:
        return self.get(map_id).get("attempted_urls", [])

    def mark_downloading(self, map_id: str, url: str) -> None:
        entry = self._ensure(map_id)
        entry["status"] = "downloading"
        if url not in entry["attempted_urls"]:
            entry["attempted_urls"].append(url)

    def mark_done(self, map_id: str, file_path: Path) -> None:
        entry = self._ensure(map_id)
        entry["status"] = "done"
        entry["file"] = str(file_path.relative_to(REPO_ROOT))
        entry["error"] = None

    def mark_failed(self, map_id: str, reason: str) -> None:
        entry = self._ensure(map_id)
        entry["status"] = "failed"
        entry["error"] = reason
        entry["file"] = None


# ---------------------------------------------------------------------------
# Downloader (file fetcher)
# ---------------------------------------------------------------------------


class FileDownloader:
    """Streams a direct URL to disk with a progress bar."""

    CHUNK_SIZE = 1 << 17  # 128 KB

    def download(self, url: str, dest_dir: Path) -> Optional[Path]:
        """Return the Path of the saved file, or None on failure."""
        try:
            with SESSION.get(url, stream=True, timeout=60, allow_redirects=True) as resp:
                resp.raise_for_status()
                fname = self._filename(resp)
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest = dest_dir / fname
                total = int(resp.headers.get("Content-Length", 0)) or None
                log.info(f"  Downloading → {fname} ({total} bytes)")
                with open(dest, "wb") as fh:
                    with tqdm(total=total, unit="B", unit_scale=True, desc=fname, leave=False) as pbar:
                        for chunk in resp.iter_content(chunk_size=self.CHUNK_SIZE):
                            if chunk:
                                fh.write(chunk)
                                pbar.update(len(chunk))
                return dest
        except Exception as exc:
            log.warning(f"  Download error: {exc}")
            raise exc

    @staticmethod
    def _filename(resp: requests.Response) -> str:
        # Try Content-Disposition first
        cd = resp.headers.get("Content-Disposition", "")
        m = re.search(r'filename\*?=["\']?([^"\';\r\n]+)', cd)
        if m:
            return unquote(m.group(1).strip().strip("\"'"))

        # Fall back to URL path
        name = unquote(Path(urlparse(resp.url).path).name) or "download"

        # Ensure a sensible archive extension
        if not any(name.lower().endswith(e) for e in ARCHIVE_EXTENSIONS):
            ext = FileDownloader._ext_from_content_type(resp.headers.get("Content-Type", ""))
            name += ext
        return name

    @staticmethod
    def _ext_from_content_type(ct: str) -> str:
        ct = ct.lower()
        if "zip" in ct:
            return ".zip"
        if "rar" in ct:
            return ".rar"
        if "7z" in ct:
            return ".7z"
        if "gzip" in ct:
            return ".gz"
        if "tar" in ct:
            return ".tar"
        return ".bin"


# ---------------------------------------------------------------------------
# MapDownloader  – main orchestrator
# ---------------------------------------------------------------------------


class MapDownloader:
    """
    Reads maps from the CSV, attempts to download each one, and tracks
    progress in a JSON state file.

    Parameters
    ----------
    resolver_chain : ResolverChain
        Chain of URL resolvers to use.  Defaults to ``DEFAULT_RESOLVER_CHAIN``.
    state : DownloadState
        State tracker.  Defaults to a new instance backed by ``STATE_FILE``.
    extractor : Extractor
        Archive extractor.  Defaults to a new ``Extractor``.
    file_downloader : FileDownloader
        File fetcher.  Defaults to a new ``FileDownloader``.
    """

    def __init__(
        self,
        resolver_chain: ResolverChain = DEFAULT_RESOLVER_CHAIN,
        state: DownloadState = None,
        extractor: Extractor = None,
        file_downloader: FileDownloader = None,
    ):
        self.resolver = resolver_chain
        self.state = state or DownloadState()
        self.extractor = extractor or Extractor()
        self.downloader = file_downloader or FileDownloader()
        self.running = True
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        skip_done: bool = True,
        limit: Optional[int] = None,
        year: Optional[int] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        retry_failed: bool = False,
    ) -> None:
        """Process all maps from the CSV."""
        if not INPUT_CSV.exists():
            log.error(f"Input CSV not found: {INPUT_CSV}")
            sys.exit(1)

        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        rows = self._load_candidate_rows()

        done = failed = skipped = processed = 0

        for row in rows:
            if not self.running or (limit and processed >= limit):
                break

            current_dir_size = _dir_size(DOWNLOAD_DIR)
            if current_dir_size > MAX_DOWNLOAD_DIR_BYTES:
                log.warning(
                    f"Downloads directory exceeded 50GB limit (Current size: {current_dir_size / 1024**3:.2f}GB). Stopping."
                )
                break

            map_id = row["id"].strip()
            mirrors = row.get("download_mirrors", "").strip()

            if not self._matches_year_filter(row, year, year_from, year_to):
                continue

            if self._should_skip(map_id, skip_done=skip_done, retry_failed=retry_failed):
                skipped += 1
                continue

            if not mirrors:
                self.state.mark_failed(map_id, "no mirrors")
                continue

            status = self._process_map(map_id, mirrors, skip_done=skip_done, retry_failed=retry_failed)
            self.state.save()
            processed += 1

            if status == "done":
                done += 1
                new_size = _dir_size(DOWNLOAD_DIR)
                log.info(f"[{map_id}] Current downloads directory size: {new_size / 1024**3:.2f}GB / 50.00GB")
            else:
                failed += 1

            if self.running:
                time.sleep(0.5)

        log.info(f"\nFinished. done={done}  failed={failed}  skipped(already-done)={skipped}")

    def _load_candidate_rows(self) -> list[dict]:
        """Loads CSV rows, keeps allowed categories, sorts by download count (desc)."""
        with open(INPUT_CSV, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))

        rows = [r for r in rows if r.get("category", "").strip() in ALLOWED_CATEGORIES]

        def get_downloads(r):
            try:
                return int(r.get("downloads", 0) or 0)
            except ValueError:
                return 0

        rows.sort(key=get_downloads, reverse=True)
        log.info(f"Loaded {len(rows)} rows from {INPUT_CSV.name} after filtering and sorting")
        return rows

    @staticmethod
    def _matches_year_filter(row: dict, year: Optional[int], year_from: Optional[int], year_to: Optional[int]) -> bool:
        """Applies the optional updated_at year filters to a CSV row."""
        if year is None and year_from is None and year_to is None:
            return True

        updated_at = row.get("updated_at", "").strip()
        if not updated_at:
            return False
        try:
            # Parse YYYY from YYYY-MM-DD HH:MM:SS
            row_year = int(updated_at[:4])
        except ValueError:
            return False

        if year is not None and row_year != year:
            return False
        if year_from is not None and row_year < year_from:
            return False
        if year_to is not None and row_year > year_to:
            return False
        return True

    def _should_skip(self, map_id: str, *, skip_done: bool, retry_failed: bool) -> bool:
        """Decides whether to skip a map based on its recorded state.

        Also resets attempted URLs when re-processing done/failed entries.
        """
        status = self.state.get(map_id).get("status")

        if skip_done and status == "done":
            return True
        if status == "failed" and not retry_failed:
            return True

        if not skip_done and status == "done":
            self.state.get(map_id)["attempted_urls"] = []
        if retry_failed and status == "failed":
            self.state.get(map_id)["attempted_urls"] = []
        return False

    def run_ids(self, map_ids: list[str], retry_failed: bool = False, skip_done: bool = True) -> None:
        """Process only the specified map IDs."""
        with open(INPUT_CSV, newline="", encoding="utf-8") as fh:
            rows = {r["id"]: r for r in csv.DictReader(fh)}

        for map_id in map_ids:
            if not self.running:
                break
            row = rows.get(map_id)
            if row is None:
                log.warning(f"ID {map_id} not found in CSV")
                continue

            if self.state.is_done(map_id) and skip_done:
                log.info(f"[{map_id}] already done, skipping.")
                continue

            if self._should_skip(map_id, skip_done=skip_done, retry_failed=retry_failed):
                continue

            self._process_map(map_id, row.get("download_mirrors", ""), skip_done=skip_done, retry_failed=retry_failed)
            self.state.save()

    # ------------------------------------------------------------------
    # Internal pipeline
    # ------------------------------------------------------------------

    def _process_map(self, map_id: str, mirror_field: str, skip_done: bool = True, retry_failed: bool = False) -> str:
        """Try every URL; return 'done' or 'failed'."""
        status = self.state.get(map_id).get("status")

        if skip_done and status == "done":
            return "done"

        if status == "failed" and not retry_failed:
            return "failed"

        urls = extract_urls(mirror_field)
        if not urls:
            self.state.mark_failed(map_id, "no parseable URLs")
            return "failed"

        dest_dir = DOWNLOAD_DIR / str(map_id)
        tried = set(self.state.get_attempted_urls(map_id))

        last_error = "No URLs attempted"

        for url in urls:
            if url in tried:
                continue

            log.info(f"[{map_id}] Trying: {url}")
            self.state.mark_downloading(map_id, url)

            direct = self.resolver.resolve(url)
            if direct is None:
                log.info("  → No direct link resolved (unsupported/failed)")
                last_error = "Unsupported host or resolver failed"
                continue

            # Clear destination directory for a fresh attempt
            self._clean_directory(dest_dir)

            log.info(f"  → Resolved: {direct[:100]}{'…' if len(direct) > 100 else ''}")
            try:
                archive = self.downloader.download(direct, dest_dir)
            except Exception as e:
                log.info("  → Download failed, trying next URL")
                last_error = f"Download error: {str(e)}"
                continue

            # Extract the main archive and delete it
            self.extractor.extract(archive, dest_dir)
            try:
                archive.unlink(missing_ok=True)
            except OSError:
                pass

            # Recursively extract any nested archives
            self._extract_recursive(dest_dir)

            # Check for level.dat
            if not self._has_level_dat(dest_dir):
                log.info("  → Invalid map (no level.dat found), trying next URL")
                last_error = "No level.dat found after extraction"
                continue

            self.state.mark_done(map_id, dest_dir)
            log.info(f"[{map_id}] ✓ Done")
            return "done"

        if dest_dir.exists():
            shutil.rmtree(dest_dir, ignore_errors=True)

        self.state.mark_failed(map_id, last_error)
        log.warning(f"[{map_id}] ✗ Failed – {last_error}")
        return "failed"

    def _extract_recursive(self, dest_dir: Path) -> None:
        """Recursively extract all archives in the directory. Delete them after extraction."""
        while True:
            extracted_something = False
            archives = []
            for p in dest_dir.rglob("*"):
                if p.is_file() and _is_archive_path(p.name):
                    archives.append(p)

            if not archives:
                break

            for archive in archives:
                dest = archive.parent
                log.info(f"  Recursive extract: {archive.relative_to(dest_dir)}")
                success = self.extractor.extract(archive, dest)
                try:
                    archive.unlink(missing_ok=True)
                except Exception as e:
                    log.warning(f"  Could not delete {archive.name}: {e}")

                if success:
                    extracted_something = True

            if not extracted_something:
                # Prevent infinite loop if we found archives but couldn't extract any
                break

    def _has_level_dat(self, dest_dir: Path) -> bool:
        """Check if level.dat or level.dat_old exists in any subdirectory."""
        for p in dest_dir.rglob("level.dat*"):
            if p.is_file():
                return True
        return False

    def _clean_directory(self, dest_dir: Path) -> None:
        """Deletes all files and folders in the given directory."""
        if dest_dir.exists():
            shutil.rmtree(dest_dir, ignore_errors=True)

    def _handle_exit(self, signum, frame) -> None:
        log.info("Interrupt received – saving state and exiting…")
        self.state.save()
        self.running = False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Minecraft maps from pmc_data_cleansed.csv")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N maps (for testing)")
    parser.add_argument("--no-skip-done", action="store_true", help="Re-attempt maps already marked 'done'")
    parser.add_argument(
        "--retry-failed", action="store_true", help="Clear attempted URLs for failed maps so they get a fresh run"
    )
    parser.add_argument("--ids", nargs="+", help="Only process specific map IDs")
    parser.add_argument("--year", type=int, default=None, help="Download maps from this exact year (e.g. 2017)")
    parser.add_argument("--year-from", type=int, default=None, help="Download maps starting from this year (inclusive)")
    parser.add_argument("--year-to", type=int, default=None, help="Download maps up to this year (inclusive)")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG log level")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    downloader = MapDownloader()

    if args.ids:
        downloader.run_ids(args.ids, retry_failed=args.retry_failed, skip_done=not args.no_skip_done)
    else:
        downloader.run(
            skip_done=not args.no_skip_done,
            limit=args.limit,
            year=args.year,
            year_from=args.year_from,
            year_to=args.year_to,
            retry_failed=args.retry_failed,
        )
