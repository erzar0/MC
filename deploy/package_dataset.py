"""Package the minimal SANA-Video training dataset into a portable tar bundle.

Only the files needed to train are included:
  - the ``.b2frame`` volumes actually referenced by the manifest
  - a rewritten manifest with volume paths *relative* to the bundle root
    (captions are already stored inline in the manifest, so the caption
    ``.txt`` files are not needed)
  - the block-colour lookup assets (``block_states.txt``, ``block_state2rgb.csv``)

Volumes are streamed straight into the tar (no multi-GB staging copy). The
resulting layout inside the tar::

    manifest.jsonl
    assets/block_states.txt
    assets/block_state2rgb.csv
    volumes/<world_id>/<region>.b2frame

which `src/scripts/sana_video/dataset.py` loads directly (it resolves relative
volume paths against the manifest's directory).

Usage::

    # build the bundle
    python deploy/package_dataset.py --output tmp/sana_video_dataset.tar

    # build and upload to Google Drive via a configured rclone remote
    python deploy/package_dataset.py --output tmp/sana_video_dataset.tar \
        --rclone-dest gdrive:minecraft-training/
"""

import argparse
import json
import logging
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

# Repo root, so we can find assets/ regardless of where the script is called from
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "tmp" / "sana_video_manifest.jsonl"
DEFAULT_OUTPUT = REPO_ROOT / "tmp" / "sana_video_dataset.tar"
ASSET_FILES = [
    REPO_ROOT / "assets" / "block_states.txt",
    REPO_ROOT / "assets" / "block_state2rgb.csv",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("package_dataset")


def _arcname_for(volume_path: Path) -> str:
    """Maps an absolute volume path to a collision-free bundle-relative name.

    ``.../cleansed/<world_id>/volumes/<region>.b2frame`` -> ``volumes/<world_id>/<region>.b2frame``.
    Region filenames repeat across worlds, so the world id must namespace them.
    """
    world_id = volume_path.parent.parent.name
    return f"volumes/{world_id}/{volume_path.name}"


def build_bundle(manifest_path: Path, output_path: Path, limit: int | None = None) -> Path:
    entries = []
    with open(manifest_path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    if limit:
        entries = entries[:limit]
    if not entries:
        log.error(f"Manifest {manifest_path} is empty (or --limit 0).")
        sys.exit(1)

    for asset in ASSET_FILES:
        if not asset.exists():
            log.error(f"Required asset missing: {asset}")
            sys.exit(1)

    # Rewrite each entry's volume_path to its bundle-relative arcname, tracking
    # the unique set of source files to add to the tar.
    rel_entries = []
    to_add: dict[str, Path] = {}  # arcname -> source path
    missing = 0
    for entry in entries:
        src = Path(entry["volume_path"])
        if not src.exists():
            missing += 1
            continue
        arc = _arcname_for(src)
        prev = to_add.get(arc)
        if prev is not None and prev != src:
            log.error(f"Arcname collision: {arc} <- {prev} and {src}")
            sys.exit(1)
        to_add[arc] = src
        rel_entries.append({"volume_path": arc, "captions": entry["captions"]})

    if missing:
        log.warning(f"{missing} referenced volumes were missing on disk and skipped.")
    if not rel_entries:
        log.error("No volumes found on disk; nothing to package.")
        sys.exit(1)

    total_bytes = sum(p.stat().st_size for p in to_add.values())
    log.info(
        f"Packaging {len(rel_entries)} entries / {len(to_add)} unique volumes "
        f"({total_bytes / 1024**3:.2f} GB) -> {output_path}"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as mf:
        for e in rel_entries:
            mf.write(json.dumps(e) + "\n")
        manifest_tmp = Path(mf.name)

    try:
        # Plain tar (no compression): .b2frame volumes are already blosc2-compressed,
        # so gzip would burn CPU for almost no size gain.
        with tarfile.open(output_path, "w") as tar:
            tar.add(manifest_tmp, arcname="manifest.jsonl")
            for asset in ASSET_FILES:
                tar.add(asset, arcname=f"assets/{asset.name}")
            for i, (arc, src) in enumerate(sorted(to_add.items()), 1):
                tar.add(src, arcname=arc)
                if i % 2000 == 0:
                    log.info(f"  ...added {i}/{len(to_add)} volumes")
    finally:
        manifest_tmp.unlink(missing_ok=True)

    size_gb = output_path.stat().st_size / 1024**3
    log.info(f"Bundle written: {output_path} ({size_gb:.2f} GB)")
    return output_path


def upload_rclone(bundle: Path, dest: str) -> None:
    cmd = ["rclone", "copy", "--progress", str(bundle), dest]
    log.info(f"Uploading via rclone: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    log.info(f"Upload complete -> {dest}")


def main():
    parser = argparse.ArgumentParser(description="Package the minimal SANA-Video training dataset into a tar bundle")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="Source JSONL manifest")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output tar path")
    parser.add_argument(
        "--limit", type=int, default=None, help="Only package the first N manifest entries (for testing)"
    )
    parser.add_argument(
        "--rclone-dest",
        type=str,
        default=None,
        help="If set, rclone copy the bundle here after building (e.g. gdrive:minecraft-training/)",
    )
    args = parser.parse_args()

    bundle = build_bundle(args.manifest, args.output, limit=args.limit)
    if args.rclone_dest:
        upload_rclone(bundle, args.rclone_dest)
    else:
        log.info("Skipped upload (no --rclone-dest). To upload later:")
        log.info(f"  rclone copy --progress {bundle} gdrive:minecraft-training/")


if __name__ == "__main__":
    main()
