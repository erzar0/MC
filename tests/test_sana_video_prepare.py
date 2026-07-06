"""Tests for prepare_dataset.build_manifest: caption pairing + height extraction."""

import json

import pytest

from src.scripts.sana_video.prepare_dataset import build_manifest


def _make_world(cleansed, world_id, region, y_range=None, orientations=("ne", "nw")):
    """Creates a minimal cleansed world (volume + captions + optional metadata)."""
    wdir = cleansed / world_id
    (wdir / "volumes").mkdir(parents=True)
    (wdir / "captions").mkdir(parents=True)
    (wdir / "volumes" / f"{region}.b2frame").touch()  # content is never read here
    for o in orientations:
        (wdir / "captions" / f"{region}.{o}.txt").write_text(f"{o} caption")
    if y_range is not None:
        region_key = region.replace("r.", "", 1).replace(".", ",")
        (wdir / "metadata.json").write_text(json.dumps({"extracted_regions": {region_key: {"y_range": y_range}}}))
    return wdir


def _read_manifest(path):
    return [json.loads(line) for line in open(path) if line.strip()]


def test_manifest_includes_height_and_captions(tmp_path):
    cleansed = tmp_path / "cleansed"
    _make_world(cleansed, "123", "r.0.0", y_range=[-64, 100])
    out = tmp_path / "manifest.jsonl"

    build_manifest(str(cleansed), str(out))
    entries = _read_manifest(out)

    assert len(entries) == 1
    e = entries[0]
    assert e["height"] == 164  # 100 - (-64)
    assert set(e["captions"]) == {"ne", "nw"}
    assert e["volume_path"].endswith("123/volumes/r.0.0.b2frame")


def test_manifest_handles_negative_region_coords(tmp_path):
    cleansed = tmp_path / "cleansed"
    _make_world(cleansed, "w", "r.-1.-2", y_range=[-64, 60])
    out = tmp_path / "manifest.jsonl"

    build_manifest(str(cleansed), str(out))
    entries = _read_manifest(out)

    # region key "-1,-2" must match metadata so height is found
    assert entries[0]["height"] == 124


def test_manifest_omits_height_when_metadata_missing(tmp_path):
    cleansed = tmp_path / "cleansed"
    _make_world(cleansed, "w", "r.0.0", y_range=None)
    out = tmp_path / "manifest.jsonl"

    build_manifest(str(cleansed), str(out))
    entries = _read_manifest(out)

    assert len(entries) == 1
    assert "height" not in entries[0]


def test_manifest_skips_regions_without_captions(tmp_path):
    cleansed = tmp_path / "cleansed"
    _make_world(cleansed, "w", "r.0.0", y_range=[0, 10], orientations=())
    out = tmp_path / "manifest.jsonl"

    build_manifest(str(cleansed), str(out))
    entries = _read_manifest(out)

    assert entries == []


if __name__ == "__main__":
    pytest.main([__file__])
