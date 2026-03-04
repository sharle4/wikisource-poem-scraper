from __future__ import annotations

import gzip
import io
import json
import sys
import statistics
import argparse
from pathlib import Path
from collections import Counter, defaultdict
from typing import Iterator, Dict, Any

def is_gz(path: Path) -> bool:
    """Checks if a file is Gzip-compressed."""
    return path.suffix == ".gz" or path.name.endswith(".jsonl.gz")

def open_maybe_gzip(path: Path, mode: str):
    """Opens a file, transparently handling Gzip decompression."""
    if "b" in mode:
        return gzip.open(path, mode) if is_gz(path) else open(path, mode)
    if is_gz(path):
        gz = gzip.open(path, mode.replace("t", "b"))
        return io.TextIOWrapper(gz, encoding="utf-8")
    return open(path, mode, encoding="utf-8")

def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    """Iterates over lines of a JSONL file, handling parsing errors."""
    with open_maybe_gzip(path, "rt") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                print(f"[ERROR] JSON decoding error at line {line_num}", file=sys.stderr)
                continue

class CorpusAnalyzer:
    """
    Orchestrates a comprehensive and detailed analysis of the poetry corpus,
    extracting statistics on structure, content, and metadata,
    and rigorously distinguishing structured data from inferred data.
    """

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.total_poems = 0

        # Metadata completeness counters
        self.poems_with_author = 0
        self.poems_with_date = 0
        self.poems_with_publisher = 0
        self.poems_with_translator = 0

        # Structural counters
        self.poems_with_identified_collection = 0
        self.poems_with_unidentified_collection = 0
        self.poems_with_section = 0
        self.poems_with_order = 0

        # Content analysis data
        self.total_stanzas = 0
        self.total_verses = 0
        self.poem_lengths_data: list[dict] = []

        # Entity analysis structures
        self.authors_data = defaultdict(lambda: {"poem_count": 0, "collection_ids": set()})
        self.collections_by_id: Dict[int, Dict[str, Any]] = defaultdict(
            lambda: {"poem_count": 0, "titles": set(), "sections": set(), "authors": set()}
        )
        self.collections_by_title_only: Counter[str] = Counter()
        self.collection_titles_to_ids: Dict[str, set] = defaultdict(set)
        self.hubs_data = defaultdict(lambda: {"version_count": 0, "title": "", "poem_ids": set()})
        self.poems_in_multiversions = 0
        self.checksum_counts: Counter[str] = Counter()

    def analyze_and_report(self):
        """Launches the analysis process and displays the final report."""
        print(f"[*] Starting detailed analysis of {self.filepath}...")

        for poem in iter_jsonl(self.filepath):
            self._process_poem(poem)

        print("[*] Analysis complete. Generating comprehensive report...")
        self._print_report()

    def _process_poem(self, poem: dict[str, Any]):
        """Processes a single poem and updates all statistical metrics."""
        self.total_poems += 1

        # --- Metadata Analysis ---
        metadata = poem.get("metadata", {})
        author = metadata.get("author")
        if author:
            self.poems_with_author += 1
            self.authors_data[author]["poem_count"] += 1

        if metadata.get("publication_date"): self.poems_with_date += 1
        if metadata.get("publisher"): self.poems_with_publisher += 1
        if metadata.get("translator"): self.poems_with_translator += 1

        # --- Structural Analysis (Collections and Sections) ---
        collection_page_id = poem.get("collection_page_id")
        collection_title = poem.get("collection_title")

        if collection_page_id:
            self.poems_with_identified_collection += 1
            collection_entry = self.collections_by_id[collection_page_id]
            collection_entry["poem_count"] += 1
            if collection_title:
                collection_entry["titles"].add(collection_title)
                self.collection_titles_to_ids[collection_title].add(collection_page_id)
            if author:
                collection_entry["authors"].add(author)
                self.authors_data[author]["collection_ids"].add(collection_page_id)

            section_title = poem.get("section_title")
            if section_title:
                self.poems_with_section += 1
                collection_entry["sections"].add(section_title)

        elif collection_title:
            self.poems_with_unidentified_collection += 1
            self.collections_by_title_only[collection_title] += 1

        if poem.get("poem_order") is not None:
            self.poems_with_order += 1

        # --- Hub Analysis (Multi-versions) ---
        hub_id = poem.get("hub_page_id")
        poem_id = poem.get("page_id")
        if hub_id is not None:
            self.hubs_data[hub_id]["version_count"] += 1
            if poem_id is not None:
                self.hubs_data[hub_id]["poem_ids"].add(poem_id)
            if not self.hubs_data[hub_id]["title"]:
                self.hubs_data[hub_id]["title"] = poem.get("hub_title") or f"Standalone poem: {poem.get('title', 'N/A')}"

        # --- Content Analysis ---
        structure = poem.get("structure", {})
        stanzas = structure.get("stanzas", [])
        num_verses = sum(len(s) for s in stanzas)

        self.total_stanzas += len(stanzas)
        self.total_verses += num_verses

        self.poem_lengths_data.append({
            "verses": num_verses,
            "title": poem.get("title", "Unknown title"),
            "author": metadata.get("author", "Unknown author")
        })

        # --- Technical Analysis ---
        checksum = poem.get("checksum_sha256")
        if checksum: self.checksum_counts[checksum] += 1

    def _print_report(self):
        """Displays the final statistical report in a structured and professional format."""

        def print_header(title):
            print("\n" + "="*80)
            print(f"    {title.upper()}")
            print("="*80)

        def print_stat(label, value, total=None, indent=0):
            prefix = " " * indent
            label_formatted = f"{prefix}{label:<45}"
            value_str = f"{value}"
            if total is not None and total > 0:
                percent = (value / total) * 100
                print(f"{label_formatted} {value_str:<10} ({percent:.2f}%)")
            else:
                print(f"{label_formatted} {value_str}")

        print_header("Comprehensive Poetic Corpus Analysis Report")

        # --- Section 1: Overview ---
        print_header("Corpus Overview")
        print_stat("Total unique poems", self.total_poems)
        print_stat("Total unique authors", len(self.authors_data))
        print_stat("Total unique IDENTIFIED collections", len(self.collections_by_id))
        print_stat("Number of UNIDENTIFIED collection titles", len(self.collections_by_title_only))

        # --- Section 2: Metadata Quality ---
        print_header("Metadata Quality and Completeness")
        print_stat("Poems with an identified author", self.poems_with_author, self.total_poems)
        print_stat("Poems with a publication date", self.poems_with_date, self.total_poems)
        print_stat("Poems with a publisher", self.poems_with_publisher, self.total_poems)
        print_stat("Poems with a translator", self.poems_with_translator, self.total_poems)

        # --- Section 3: Collection Structural Analysis ---
        print_header("Collection Structural Analysis")
        print_stat("Poems linked to an IDENTIFIED collection (by ID)", self.poems_with_identified_collection, self.total_poems)
        print_stat("Poems linked to an UNIDENTIFIED collection (title)", self.poems_with_unidentified_collection, self.total_poems)
        total_in_collection = self.poems_with_identified_collection + self.poems_with_unidentified_collection
        print_stat("Total poems in a collection (all types)", total_in_collection, self.total_poems)
        print_stat("Poems with an ordinal position", self.poems_with_order, total_in_collection)
        print_stat("Poems with a section title", self.poems_with_section, self.poems_with_identified_collection)

        collections_with_sections = sum(1 for data in self.collections_by_id.values() if data["sections"])
        if self.collections_by_id:
            print_stat("IDENTIFIED collections structured into sections", collections_with_sections, len(self.collections_by_id))

        # --- Section 4: Quantitative Content Analysis ---
        print_header("Quantitative Poetic Content Analysis")
        print_stat("Total stanzas", self.total_stanzas)
        print_stat("Total verses", self.total_verses)
        if self.total_poems > 0:
            print_stat("Average stanzas per poem", f"{self.total_stanzas / self.total_poems:.2f}")
            print_stat("Average verses per poem", f"{self.total_verses / self.total_poems:.2f}")

        poem_lengths_verses = [p['verses'] for p in self.poem_lengths_data]
        if poem_lengths_verses:
            print_stat("Median poem length (in verses)", f"{statistics.median(poem_lengths_verses):.0f}")
            print_stat("Longest poem (in verses)", max(poem_lengths_verses))
            print_stat("Shortest poem (in verses)", min(poem_lengths_verses))

        # --- Section 5: Version and Duplicate Analysis ---
        print_header("Version and Duplicate Analysis")
        real_hubs = {k: v for k, v in self.hubs_data.items() if v["version_count"] > 1}
        print_stat("Real multi-version hubs (>1 poem)", len(real_hubs))

        # Count poems in multi-version hubs
        poems_in_multiversions = sum(len(v["poem_ids"]) for v in real_hubs.values())
        print_stat("Poems in multi-version hubs", poems_in_multiversions, self.total_poems)

        exact_duplicates = sum(count - 1 for count in self.checksum_counts.values() if count > 1)
        print_stat("Strictly identical wikitext content (duplicates)", exact_duplicates)

        # Analyze collections sharing the same title
        duplicate_titles = {title: ids for title, ids in self.collection_titles_to_ids.items() if len(ids) > 1}
        print_stat("Collection titles shared across multiple IDs", len(duplicate_titles))
        total_collections_with_duplicate_titles = sum(len(ids) for ids in duplicate_titles.values())
        print_stat("Total collections affected", total_collections_with_duplicate_titles, len(self.collections_by_id))

        # --- Section 6: Rankings (Top 10) ---
        print_header("Rankings (Top 10)")

        # Authors
        print("\n  Most prolific authors (by poem count):")
        top_authors = sorted(self.authors_data.items(), key=lambda item: item[1]['poem_count'], reverse=True)[:10]
        for author, data in top_authors:
            print(f"    - {author:<40} {data['poem_count']} poems")

        # Collections by size
        print("\n  Largest IDENTIFIED collections (by poem count):")
        top_collections = sorted(self.collections_by_id.items(), key=lambda item: item[1]['poem_count'], reverse=True)[:10]
        for cid, data in top_collections:
            title = next(iter(data['titles']), f"ID: {cid}")
            print(f"    - {title:<40} {data['poem_count']} poems")

        print("\n  Most frequent UNIDENTIFIED collection titles:")
        for title, count in self.collections_by_title_only.most_common(10):
            print(f"    - {title:<40} {count} poems")

        # Collections by structure
        print("\n  Best structured collections (by section count):")
        top_structured = sorted(self.collections_by_id.items(), key=lambda item: len(item[1]['sections']), reverse=True)[:10]
        for cid, data in top_structured:
            title = next(iter(data['titles']), f"ID: {cid}")
            print(f"    - {title:<40} {len(data['sections'])} sections")

        # Hubs
        print("\n  Hubs with the most versions:")
        top_hubs = sorted(real_hubs.items(), key=lambda item: item[1]['version_count'], reverse=True)[:10]
        if top_hubs:
            for hub_id, data in top_hubs:
                hub_title_display = data['title'] if data['title'] and 'Standalone' not in data['title'] else f"Hub ID {hub_id}"
                print(f"    - {hub_title_display:<40} {data['version_count']} versions")
        else:
            print("    No multi-version hubs found.")

        # Poems by length
        print("\n  Longest poems (by verse count):")
        top_longest = sorted(self.poem_lengths_data, key=lambda p: p['verses'], reverse=True)[:10]
        for poem in top_longest:
            display = f"\"{poem['title']}\" ({poem['author']})"
            print(f"    - {display:<60} {poem['verses']} verses")

        print("\n  Shortest poems (by verse count):")
        top_shortest = sorted(self.poem_lengths_data, key=lambda p: p['verses'])[:10]
        for poem in top_shortest:
            display_title = f"\"{poem['title']}\" ({poem['author']})"
            print(f"    - {display_title:<60} {poem['verses']} verses")

        print("\n" + "="*80)


def main(argv: list[str] | None = None):
    """Entry point: finds a data file and launches the analysis."""
    parser = argparse.ArgumentParser(description="Analyzes a poem data file.")
    parser.add_argument("filepath", type=Path, nargs='?', default=None, help="Path to the file to analyze.")
    args = parser.parse_args(argv)

    if args.filepath:
        target = args.filepath
        if not target.exists():
             print(f"[ERROR] The specified file '{target}' was not found.", file=sys.stderr)
             sys.exit(1)
    else:
        repo_root = Path(__file__).resolve().parents[2]
        candidates = sorted(repo_root.glob("data/*.jsonl.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
        target = next((p for p in candidates), None)

    if target is None:
        print("[ERROR] No data file found. Specify a path or place a .jsonl.gz file in data/", file=sys.stderr)
        sys.exit(1)

    analyzer = CorpusAnalyzer(target)
    analyzer.analyze_and_report()

if __name__ == "__main__":
    main()
