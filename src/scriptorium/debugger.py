"""
Debug tool to extract poems from unidentified collections.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tqdm import tqdm

from .utils import iter_jsonl, open_maybe_gzip


def extract_unidentified_collections(input_path: Path, output_path: Path):
    """
    Reads a poem data file and extracts those that have a collection_title
    but no collection_page_id, to facilitate analysis.
    """
    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path}", file=sys.stderr)
        return 1

    print(f"[*] Analyzing '{input_path}' to extract poems from unidentified collections...")

    unidentified_count = 0

    total_lines = sum(1 for _ in open_maybe_gzip(input_path, "rt"))

    with open_maybe_gzip(output_path, "wt") as fout, tqdm(total=total_lines, desc="Analyzing poems", unit=" poem") as pbar:
        for poem in iter_jsonl(input_path):
            has_title = poem.get("collection_title") is not None
            has_id = poem.get("collection_page_id") is not None

            if has_title and not has_id:
                fout.write(json.dumps(poem, ensure_ascii=False) + "\n")
                unidentified_count += 1
            pbar.update(1)

    print("\n" + "="*50)
    print(" " * 15 + "DEBUG EXTRACTION REPORT")
    print("="*50)
    print(f"{'Total poems read:':<40} {total_lines}")
    print(f"{'Poems extracted (unidentified collection):':<40} {unidentified_count}")
    print(f"[OK] Debug file saved to: {output_path}")
    print("="*50)

    return 0

def main(argv: list[str] | None = None) -> int:
    """Entry point for the debug script."""
    parser = argparse.ArgumentParser(
        description="Extracts poems belonging to unidentified collections for debugging.",
    )
    parser.add_argument("--input", "-i", type=Path, required=True, help="Input file (e.g., data/poems.enriched.jsonl.gz).")
    parser.add_argument("--output", "-o", type=Path, required=True, help="Output file for extracted poems (e.g., data/debug.unidentified.jsonl.gz).")
    args = parser.parse_args(argv)

    return extract_unidentified_collections(args.input, args.output)

if __name__ == "__main__":
    sys.exit(main())
