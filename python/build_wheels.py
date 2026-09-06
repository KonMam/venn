#!/usr/bin/env python3
"""Build platform wheels for venn-bin from goreleaser's dist/ output.

The binary is static and CGO-free, so there is nothing to compile here: each
wheel is the right prebuilt binary plus metadata, dropped into the wheel's
scripts/ directory so pip installs it onto PATH. One wheel per platform tag.

    goreleaser release --snapshot --clean
    python python/build_wheels.py --version 0.1.0

Wheels land in python/dist/.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import re
import sys
import tarfile
import zipfile
from pathlib import Path

DIST = "dist"
NAME = "venn_bin"
DISPLAY_NAME = "venn-bin"

# goreleaser (goos, goarch) -> the wheel platform tags it satisfies. A static
# Go binary runs on both glibc and musl, so linux gets both.
PLATFORMS: dict[tuple[str, str], list[str]] = {
    ("linux", "amd64"): ["manylinux2014_x86_64", "musllinux_1_1_x86_64"],
    ("linux", "arm64"): ["manylinux2014_aarch64", "musllinux_1_1_aarch64"],
    ("darwin", "amd64"): ["macosx_10_9_x86_64"],
    ("darwin", "arm64"): ["macosx_11_0_arm64"],
    ("windows", "amd64"): ["win_amd64"],
    ("windows", "arm64"): ["win_arm64"],
}


def find_source(dist: Path, version: str, goos: str, goarch: str) -> Path | None:
    """Locate the built binary for one platform in goreleaser's output.

    Handles both layouts: `goreleaser release` writes archives, `goreleaser
    build` writes bare binaries under venn_<goos>_<goarch>*/. Snapshot builds
    put a generated version in the archive name, so the version is matched
    loosely and taken from --version instead.
    """
    ext = "zip" if goos == "windows" else "tar.gz"
    exact = dist / f"venn_{version}_{goos}_{goarch}.{ext}"
    if exact.exists():
        return exact
    archives = sorted(dist.glob(f"venn_*_{goos}_{goarch}.{ext}"))
    if archives:
        return archives[0]
    # `goreleaser build` layout: dist/venn_linux_amd64_v1/venn
    exe = "venn.exe" if goos == "windows" else "venn"
    bare = sorted(dist.glob(f"venn_{goos}_{goarch}*/{exe}"))
    return bare[0] if bare else None


def extract_binary(src: Path, goos: str) -> bytes:
    member = "venn.exe" if goos == "windows" else "venn"
    if src.is_file() and src.name == member:
        return src.read_bytes()
    if src.suffix == ".zip":
        with zipfile.ZipFile(src) as z:
            return z.read(member)
    with tarfile.open(src) as t:
        f = t.extractfile(member)
        if f is None:
            raise SystemExit(f"{src}: {member} not found in archive")
        return f.read()


def metadata(version: str) -> str:
    long_desc = (Path(__file__).parent / "README.md").read_text()
    return (
        "Metadata-Version: 2.1\n"
        f"Name: {DISPLAY_NAME}\n"
        f"Version: {version}\n"
        "Summary: Row-level keyed diff of tabular datasets: files, directories and lake tables\n"
        "Home-page: https://github.com/KonMam/venn\n"
        "Author: KonMam\n"
        "License: MIT\n"
        "Project-URL: Source, https://github.com/KonMam/venn\n"
        "Project-URL: Issues, https://github.com/KonMam/venn/issues\n"
        "Classifier: License :: OSI Approved :: MIT License\n"
        "Classifier: Programming Language :: Python :: 3\n"
        "Classifier: Topic :: Database\n"
        "Classifier: Topic :: Software Development :: Testing\n"
        "Classifier: Topic :: Utilities\n"
        "Requires-Python: >=3.8\n"
        "Description-Content-Type: text/markdown\n"
        "\n" + long_desc
    )


def wheel_metadata(tag: str) -> str:
    return (
        "Wheel-Version: 1.0\n"
        "Generator: venn build_wheels.py\n"
        "Root-Is-Purelib: false\n"
        f"Tag: {tag}\n"
    )


def urlsafe_b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def build_wheel(out_dir: Path, version: str, plat_tag: str, binary: bytes, goos: str) -> Path:
    tag = f"py3-none-{plat_tag}"
    dist_info = f"{NAME}-{version}.dist-info"
    data_dir = f"{NAME}-{version}.data/scripts"
    exe = "venn.exe" if goos == "windows" else "venn"

    records: list[tuple[str, str, int]] = []

    def entry(path: str, data: bytes) -> tuple[str, bytes, int]:
        digest = hashlib.sha256(data).digest()
        records.append((path, "sha256=" + urlsafe_b64(digest), len(data)))
        return path, data, len(data)

    files = [
        entry(f"{data_dir}/{exe}", binary),
        entry(f"{dist_info}/METADATA", metadata(version).encode()),
        entry(f"{dist_info}/WHEEL", wheel_metadata(tag).encode()),
        entry(f"{dist_info}/top_level.txt", b""),
    ]

    record_lines = io.StringIO()
    w = csv.writer(record_lines, lineterminator="\n")
    for path, digest, size in records:
        w.writerow([path, digest, size])
    w.writerow([f"{dist_info}/RECORD", "", ""])

    out_dir.mkdir(parents=True, exist_ok=True)
    whl = out_dir / f"{NAME}-{version}-{tag}.whl"
    with zipfile.ZipFile(whl, "w", zipfile.ZIP_DEFLATED) as z:
        for path, data, _ in files:
            info = zipfile.ZipInfo(path)
            info.date_time = (1980, 1, 1, 0, 0, 0)  # reproducible
            # the S_IFREG bits matter: without them pip reads the mode as
            # unset and installs the binary without its executable bit
            mode = 0o755 if path.endswith(exe) else 0o644
            info.external_attr = (mode | 0o100000) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, data)
        info = zipfile.ZipInfo(f"{dist_info}/RECORD")
        info.date_time = (1980, 1, 1, 0, 0, 0)
        info.external_attr = (0o644 | 0o100000) << 16
        z.writestr(info, record_lines.getvalue())
    return whl


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--version", required=True, help="release version, without the leading v")
    ap.add_argument("--dist", default=DIST, help="goreleaser output directory")
    ap.add_argument("--out", default=str(Path(__file__).parent / "dist"))
    args = ap.parse_args()

    version = args.version.lstrip("v")
    if not re.match(r"^\d+\.\d+\.\d+", version):
        # PEP 440 needs a normalizable version; a snapshot string is not one
        print(f"warning: {version!r} may not be a valid PEP 440 version", file=sys.stderr)

    dist = Path(args.dist)
    if not dist.is_dir():
        print(f"{dist} not found; run goreleaser first", file=sys.stderr)
        return 1

    out = Path(args.out)
    built = []
    for (goos, goarch), tags in PLATFORMS.items():
        src = find_source(dist, version, goos, goarch)
        if src is None:
            print(f"skip {goos}/{goarch}: nothing built in {dist}", file=sys.stderr)
            continue
        binary = extract_binary(src, goos)
        for tag in tags:
            whl = build_wheel(out, version, tag, binary, goos)
            built.append(whl)
            print(f"{whl.name}  ({len(binary):,} bytes from {src.name})")

    if not built:
        print("no wheels built", file=sys.stderr)
        return 1
    print(f"\n{len(built)} wheels in {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
