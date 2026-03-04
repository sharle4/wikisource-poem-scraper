"""Corpus Merger module for reconciling two JSONL poem files into a Golden Record.

This module provides the CorpusMerger class, which reads two JSONL (or JSONL.GZ)
files produced by the online and offline scraping pipelines, identifies unique
poems by page_id, resolves duplicates using a configurable strategy, and writes
a single consolidated output file.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Literal

from .utils import iter_jsonl, open_maybe_gzip

logger = logging.getLogger(__name__)

Strategy = Literal["keep_a", "keep_b", "keep_richest"]

METADATA_SCORED_FIELDS = ("author", "publication_date", "source_collection", "publisher", "translator")


def calculate_richness_score(poem: Dict[str, Any]) -> int:
    """Calculate a deterministic richness score for a poem record.

    The scoring algorithm rewards contextual completeness, metadata density,
    and text length. It is used by the ``keep_richest`` conflict-resolution
    strategy to decide which version of a duplicate poem to retain.

    Scoring breakdown:
        - **Collection context (+50):** ``collection_page_id`` is not null.
        - **Section context (+20):** ``section_title`` is not null.
        - **Hub context (+30):** ``hub_page_id`` is present AND differs from
          ``page_id`` (proper multiversion grouping).
        - **Metadata completeness (+10 each):** For every non-null, non-empty
          string among ``author``, ``publication_date``, ``source_collection``,
          ``publisher``, ``translator``.
        - **Text length (tie-breaker):** +1 per 100 characters of
          ``normalized_text``.

    Args:
        poem: A parsed poem dictionary (one JSON line from a JSONL file).

    Returns:
        An integer score (higher is richer).
    """
    score = 0

    # Collection context (+50)
    if poem.get("collection_page_id") is not None:
        score += 50

    # Section context (+20)
    if poem.get("section_title") is not None:
        score += 20

    # Hub context (+30)
    hub_page_id = poem.get("hub_page_id")
    page_id = poem.get("page_id")
    if hub_page_id is not None and hub_page_id != page_id:
        score += 30

    # Metadata completeness (+10 each)
    metadata = poem.get("metadata")
    if isinstance(metadata, dict):
        for field in METADATA_SCORED_FIELDS:
            value = metadata.get(field)
            if value is not None and isinstance(value, str) and value.strip():
                score += 10

    # Text length tie-breaker (+1 per 100 chars)
    normalized_text = poem.get("normalized_text", "")
    if isinstance(normalized_text, str):
        score += len(normalized_text) // 100

    return score


class CorpusMerger:
    """Merges two JSONL poem files into a single deduplicated Golden Record.

    The merger streams both files line-by-line to remain memory-efficient,
    stores parsed records in an in-memory dictionary keyed by ``page_id``,
    and resolves duplicates according to the chosen strategy.

    Args:
        file_a: Path to the first input file (.jsonl or .jsonl.gz).
        file_b: Path to the second input file (.jsonl or .jsonl.gz).
        output: Path to the output file (.jsonl or .jsonl.gz).
        strategy: Conflict resolution strategy — one of ``keep_a``,
            ``keep_b``, or ``keep_richest``.
    """

    def __init__(
        self,
        file_a: Path,
        file_b: Path,
        output: Path,
        strategy: Strategy = "keep_richest",
    ) -> None:
        self.file_a = file_a
        self.file_b = file_b
        self.output = output
        self.strategy: Strategy = strategy

        # Counters for the summary report.
        self._total_a: int = 0
        self._total_b: int = 0
        self._duplicates: int = 0
        self._a_wins: int = 0
        self._b_wins: int = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_conflict(
        self,
        existing: Dict[str, Any],
        incoming: Dict[str, Any],
    ) -> bool:
        """Decide whether *incoming* (from file B) should replace *existing* (from file A).

        Args:
            existing: The poem dict already stored (originally from file A).
            incoming: The poem dict from file B that shares the same page_id.

        Returns:
            ``True`` if *incoming* should replace *existing*, ``False`` otherwise.
        """
        if self.strategy == "keep_a":
            return False

        if self.strategy == "keep_b":
            return True

        # keep_richest
        score_a = calculate_richness_score(existing)
        score_b = calculate_richness_score(incoming)
        # Tie goes to A (score_a >= score_b → keep A).
        return score_b > score_a

    def _ingest_file_a(self, merged: Dict[int, Dict[str, Any]]) -> None:
        """Stream file A into the merged corpus dictionary.

        Every record from file A is unconditionally stored (first pass).

        Args:
            merged: The in-memory corpus dictionary to populate.
        """
        logger.info("Reading file A: %s", self.file_a)
        for poem in iter_jsonl(self.file_a):
            page_id = poem.get("page_id")
            if page_id is None:
                logger.warning("Skipping record without page_id in file A.")
                continue
            merged[page_id] = poem
            self._total_a += 1

        logger.info("File A ingested: %d records.", self._total_a)

    def _ingest_file_b(self, merged: Dict[int, Dict[str, Any]]) -> None:
        """Stream file B and merge into the corpus with conflict resolution.

        For each record in file B:
        - If the page_id is new, it is added directly.
        - If the page_id already exists (duplicate), the conflict-resolution
          strategy is applied.

        Args:
            merged: The in-memory corpus dictionary to update.
        """
        logger.info("Reading file B: %s", self.file_b)
        for poem in iter_jsonl(self.file_b):
            page_id = poem.get("page_id")
            if page_id is None:
                logger.warning("Skipping record without page_id in file B.")
                continue
            self._total_b += 1

            if page_id not in merged:
                merged[page_id] = poem
            else:
                self._duplicates += 1
                if self._resolve_conflict(merged[page_id], poem):
                    merged[page_id] = poem
                    self._b_wins += 1
                else:
                    self._a_wins += 1

        logger.info("File B ingested: %d records.", self._total_b)

    def _write_output(self, merged: Dict[int, Dict[str, Any]]) -> None:
        """Stream the merged corpus to the output file.

        Automatically compresses with gzip when the output path ends in ``.gz``.

        Args:
            merged: The final merged corpus dictionary.
        """
        logger.info("Writing merged output to: %s", self.output)
        self.output.parent.mkdir(parents=True, exist_ok=True)

        with open_maybe_gzip(self.output, "wt") as fout:
            for poem in merged.values():
                fout.write(json.dumps(poem, ensure_ascii=False) + "\n")

    def _log_summary(self, total_output: int, elapsed: float) -> None:
        """Log a detailed summary of the merge operation.

        Args:
            total_output: Number of unique poems written to the output file.
            elapsed: Wall-clock time for the entire merge, in seconds.
        """
        logger.info("=" * 60)
        logger.info("MERGE SUMMARY")
        logger.info("=" * 60)
        logger.info("  File A records       : %d", self._total_a)
        logger.info("  File B records       : %d", self._total_b)
        logger.info("  Duplicates found     : %d", self._duplicates)
        logger.info("  Unique poems in output: %d", total_output)
        logger.info("  Strategy used        : %s", self.strategy)
        if self.strategy == "keep_richest":
            logger.info("  Conflicts won by A   : %d", self._a_wins)
            logger.info("  Conflicts won by B   : %d", self._b_wins)
        elif self.strategy == "keep_a":
            logger.info("  Conflicts won by A   : %d (all)", self._duplicates)
        elif self.strategy == "keep_b":
            logger.info("  Conflicts won by B   : %d (all)", self._duplicates)
        logger.info("  Elapsed time         : %.2f s", elapsed)
        logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Execute the full merge pipeline.

        Steps:
            1. Stream and store every record from file A.
            2. Stream file B, merging with conflict resolution.
            3. Write the deduplicated corpus to the output file.
            4. Log a detailed summary.

        Returns:
            ``0`` on success, ``1`` on failure.
        """
        start = time.perf_counter()

        logger.info(
            "Starting merge: A=%s, B=%s → %s [strategy=%s]",
            self.file_a,
            self.file_b,
            self.output,
            self.strategy,
        )

        for label, path in [("A", self.file_a), ("B", self.file_b)]:
            if not path.exists():
                logger.error("Input file %s not found: %s", label, path)
                return 1

        merged: Dict[int, Dict[str, Any]] = {}

        self._ingest_file_a(merged)
        self._ingest_file_b(merged)
        self._write_output(merged)

        elapsed = time.perf_counter() - start
        self._log_summary(len(merged), elapsed)

        return 0
