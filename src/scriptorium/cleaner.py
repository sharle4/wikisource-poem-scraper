"""
Corpus Cleaning and Deduplication Script.

This script processes a results file `poems.jsonl.gz` to:
1. Clean poem titles.
2. Remove duplicate poems based on their `page_id`, keeping
   the most complete version (the one with collection information).
3. Remove unused metadata fields.

Usage:
  python -m scriptorium clean --input <input_file.jsonl.gz> --output <output_file.jsonl.gz>
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Any

from .utils import iter_jsonl, open_maybe_gzip


def clean_title(title: str) -> str:
    """Returns the last segment after '/' and removes any content in parentheses."""
    if not isinstance(title, str):
        return title
    segment = title.split("/")[-1]
    segment = re.sub(r"\s*\([^)]*\)", "", segment)
    segment = re.sub(r"\s+", " ", segment).strip()
    return segment if segment else title.strip()

def process_poem(poem: Dict[str, Any]) -> Dict[str, Any]:
    """Applies all transformations to a poem object."""
    if "title" in poem:
        poem["title"] = clean_title(poem["title"])

    if "metadata" in poem and isinstance(poem["metadata"], dict):
        poem["metadata"].pop("license_name", None)

    return poem

def main(argv: list[str] | None = None) -> int:
    """Main entry point for the cleaning logic."""
    parser = argparse.ArgumentParser(
        description="Cleans and deduplicates a poems.jsonl.gz results file.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--input", "-i", type=Path, required=True, help="Input file (.jsonl or .jsonl.gz)")
    parser.add_argument("--output", "-o", type=Path, required=True, help="Output file (.jsonl or .jsonl.gz)")
    args = parser.parse_args(argv)

    input_path: Path = args.input
    output_path: Path = args.output

    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path}", file=sys.stderr)
        return 1

    if output_path.exists():
        print(f"[WARNING] Output file {output_path} already exists and will be overwritten.", file=sys.stderr)

    best_poems: Dict[int, Dict[str, Any]] = {}
    total_read = 0

    print(f"[*] Processing {input_path}...")
    print("[*] Phase 1: Reading and selecting the best version for each poem...")

    for poem in iter_jsonl(input_path):
        total_read += 1
        page_id = poem.get("page_id")

        if page_id is None:
            print(f"[WARNING] Poem without page_id found (line ~{total_read}), skipped.", file=sys.stderr)
            continue

        cleaned_poem = process_poem(poem)

        existing_poem = best_poems.get(page_id)

        if not existing_poem or (cleaned_poem.get("collection_page_id") is not None and existing_poem.get("collection_page_id") is None):
            best_poems[page_id] = cleaned_poem

    print(f"[*] Phase 2: Writing {len(best_poems)} unique and optimal poems to {output_path}...")

    written_count = 0
    with open_maybe_gzip(output_path, "wt") as fout:
        for poem in best_poems.values():
            fout.write(json.dumps(poem, ensure_ascii=False) + "\n")
            written_count += 1

    duplicates_removed = total_read - written_count

    print("\n" + "="*50)
    print(" " * 15 + "CLEANING REPORT")
    print("="*50)
    print(f"{'Total poems read:':<35} {total_read}")
    print(f"{'Duplicates (by page_id) removed:':<35} {duplicates_removed}")
    print("-" * 50)
    print(f"{'Total unique poems written:':<35} {written_count}")
    print(f"[OK] Cleaned file saved to: {output_path}")
    print("="*50)

    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
