#!/usr/bin/env python3
"""Copy Markdown files from one directory to another, stripping frontmatter.

Frontmatter is a block at the very top of the file fenced by a line containing
only ``---`` before and after a set of ``tag: value`` lines, e.g.::

    ---
    title: My Note
    tags: a, b, c
    ---
    # Body starts here

Only a fence that is the *first* line of the file is treated as frontmatter, so
a ``---`` used mid-document as a horizontal rule is left alone. If an opening
fence has no matching closing fence, the file is copied unchanged (and a warning
is printed) rather than risk deleting real content.

Usage:
    strip_frontmatter.py SRC DST [-r] [--dry-run]

Example:
    strip_frontmatter.py ./notes ./published --recursive
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

FENCE = "---"
MARKDOWN_SUFFIXES = {".md", ".markdown"}


def strip_frontmatter(text: str) -> tuple[str, str]:
    """Return ``(new_text, status)`` for a document's contents.

    status is one of:
      * ``"stripped"``     — a leading frontmatter block was removed
      * ``"none"``         — the file had no leading frontmatter
      * ``"unterminated"`` — an opening fence with no closing fence; left as-is
    """
    # Preserve a UTF-8 BOM if the file has one.
    bom = ""
    if text.startswith("﻿"):
        bom, text = "﻿", text[1:]

    # keepends=True so we round-trip the original newline style (\n, \r\n, \r).
    lines = text.splitlines(keepends=True)

    # Frontmatter only counts if the very first line is exactly the fence.
    if not lines or lines[0].strip() != FENCE:
        return bom + text, "none"

    # Find the closing fence.
    for i in range(1, len(lines)):
        if lines[i].strip() == FENCE:
            body = lines[i + 1 :]
            # Drop blank lines left between the frontmatter and the real content.
            while body and body[0].strip() == "":
                body.pop(0)
            return bom + "".join(body), "stripped"

    # Opening fence but no close: not valid frontmatter — don't touch it.
    return bom + text, "unterminated"


def iter_markdown(src: Path, recursive: bool):
    """Yield Markdown files under ``src`` (recursively if requested)."""
    entries = src.rglob("*") if recursive else src.iterdir()
    for path in sorted(entries):
        if path.is_file() and path.suffix.lower() in MARKDOWN_SUFFIXES:
            yield path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Copy Markdown files from SRC to DST, stripping leading "
        "frontmatter (a --- fenced block of tag: value lines).",
    )
    parser.add_argument("src", type=Path, help="source directory")
    parser.add_argument("dst", type=Path, help="destination directory")
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="recurse into subdirectories (structure is preserved under DST)",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="report what would happen without writing anything",
    )
    args = parser.parse_args(argv)

    src: Path = args.src
    dst: Path = args.dst

    if not src.is_dir():
        parser.error(f"source directory does not exist: {src}")
    if dst.exists() and not dst.is_dir():
        parser.error(f"destination exists but is not a directory: {dst}")
    if src.resolve() == dst.resolve():
        parser.error("source and destination are the same directory; refusing "
                     "to overwrite originals in place")

    copied = stripped = unterminated = 0
    for path in iter_markdown(src, args.recursive):
        try:
            text = path.read_bytes().decode("utf-8")
        except UnicodeDecodeError as exc:
            print(f"warning: skipping non-UTF-8 file {path}: {exc}", file=sys.stderr)
            continue

        new_text, status = strip_frontmatter(text)
        if status == "unterminated":
            print(
                f"warning: {path} opens with '---' but has no closing '---'; "
                "copied unchanged",
                file=sys.stderr,
            )
            unterminated += 1

        rel = path.relative_to(src)
        out = dst / rel
        action = "would write" if args.dry_run else "wrote"
        note = " (stripped frontmatter)" if status == "stripped" else ""
        if not args.dry_run:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(new_text.encode("utf-8"))

        print(f"{action} {out}{note}")
        copied += 1
        if status == "stripped":
            stripped += 1

    summary = (
        f"\n{'(dry run) ' if args.dry_run else ''}"
        f"{copied} file(s) copied, {stripped} had frontmatter stripped"
    )
    if unterminated:
        summary += f", {unterminated} had an unterminated fence (left unchanged)"
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
