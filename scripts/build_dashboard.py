#!/usr/bin/env python3
"""
Build the dashboard with cache-busted asset names.

Workflow:
1) Copy dashboard/site into dashboard/site/dist (excluding backups/temp files).
2) Hash .js and .css files by content and rename them with the hash.
3) Rewrite HTML files to point to the hashed asset names.
4) Emit manifest.json mapping original -> hashed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

TARGET_EXTS = {".js", ".css"}
EXCLUDE_NAMES = {"dist", "backup", "README.md", "manifest.json", ".DS_Store"}


def hash_file(path: Path, digest_length: int = 12) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()[:digest_length]


def copy_source(src_dir: Path, build_dir: Path) -> None:
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)

    for item in src_dir.iterdir():
        if item.name in EXCLUDE_NAMES:
            continue
        if item.is_file():
            shutil.copy2(item, build_dir / item.name)
        elif item.is_dir():
            shutil.copytree(item, build_dir / item.name)


def fingerprint_assets(build_dir: Path) -> dict[str, str]:
    manifest: dict[str, str] = {}
    for path in sorted(build_dir.iterdir()):
        if not path.is_file() or path.suffix not in TARGET_EXTS:
            continue

        digest = hash_file(path)
        hashed_name = f"{path.stem}.{digest}{path.suffix}"
        hashed_path = path.with_name(hashed_name)
        path.replace(hashed_path)
        manifest[path.name] = hashed_path.name

    return manifest


def rewrite_html(build_dir: Path, manifest: dict[str, str]) -> None:
    if not manifest:
        return

    for html_file in build_dir.rglob("*.html"):
        content = html_file.read_text(encoding="utf-8")
        for original, hashed in manifest.items():
            content = content.replace(original, hashed)
        html_file.write_text(content, encoding="utf-8")


def write_manifest(build_dir: Path, manifest: dict[str, str]) -> Path:
    manifest_path = build_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dashboard assets with hashed filenames")
    default_source = Path(__file__).resolve().parents[1] / "dashboard" / "site"
    parser.add_argument(
        "--source",
        type=Path,
        default=default_source,
        help="Path to dashboard/site (source assets)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output directory for built assets (default: <source>/dist)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_dir: Path = args.source
    build_dir: Path = args.out or source_dir / "dist"

    if not source_dir.exists():
        print(f"Source directory not found: {source_dir}", file=sys.stderr)
        return 1

    copy_source(source_dir, build_dir)
    manifest = fingerprint_assets(build_dir)

    if not manifest:
        print("No assets were fingerprinted (.js/.css not found)", file=sys.stderr)
        return 1

    rewrite_html(build_dir, manifest)
    manifest_path = write_manifest(build_dir, manifest)

    print(f"Built dashboard to: {build_dir}")
    print("Hashed assets:")
    for original, hashed in manifest.items():
        print(f"  {original} -> {hashed}")
    print(f"Manifest written to: {manifest_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
