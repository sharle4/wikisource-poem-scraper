"""
Data enrichment module to fix missing `collection_page_id` values.

This script runs a three-step process to improve an existing data file:
1.  **Analysis and caching**: It reads the input file once to build a cache
    of known `collection_title` -> `collection_page_id` mappings and identifies
    all collection titles that need an ID lookup.
2.  **API retrieval**: For all titles without an ID, it queries the MediaWiki API
    asynchronously and at scale to find the corresponding `pageid` values,
    handling redirects.
3.  **Enrichment and writing**: It re-reads the input file and writes a new
    output file, adding the `collection_page_id` values that were found.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, Set, List, Any

from tqdm import tqdm

from .api_client import WikiAPIClient
from .utils import iter_jsonl, open_maybe_gzip

logger = logging.getLogger(__name__)


class PoemEnricher:
    """Orchestrates the poem data enrichment process."""

    def __init__(self, input_path: Path, output_path: Path, lang: str, workers: int, bot_username: str | None = None, bot_password: str | None = None):
        self.input_path = input_path
        self.output_path = output_path
        self.lang = lang
        self.workers = workers
        self.api_endpoint = f"https://{lang}.wikisource.org/w/api.php"
        self.title_to_id_cache: Dict[str, int] = {}
        self.bot_username = bot_username
        self.bot_password = bot_password

    async def run(self):
        """Executes the complete enrichment workflow."""
        logger.info(f"Starting enrichment process for '{self.input_path}'.")

        if not self.input_path.exists():
            logger.critical(f"Input file '{self.input_path}' not found.")
            return

        # --- Step 1: Build the initial cache and identify missing titles ---
        titles_to_fetch = await self._build_initial_cache_and_identify_missing()

        # --- Step 2: Retrieve missing IDs via the API ---
        if titles_to_fetch:
            logger.info(f"Fetching {len(titles_to_fetch)} missing collection IDs via the API...")
            async with WikiAPIClient(self.api_endpoint, self.workers, self.bot_username, self.bot_password) as client:
                await self._fetch_missing_ids_from_api(client, list(titles_to_fetch))
        else:
            logger.info("No missing collection IDs to fetch. Cache is complete.")

        # --- Step 3: Enrich the original file and write the new one ---
        await self._enrich_and_write_file()

        logger.info(f"Process complete. Enriched file saved to '{self.output_path}'.")

    async def _build_initial_cache_and_identify_missing(self) -> Set[str]:
        """
        Reads the file once to create a cache of known IDs and list the titles to look up.
        This is an optimization to avoid unnecessary API calls.
        """
        logger.info("Phase 1: Analyzing file to build initial cache...")
        titles_needing_id = set()

        total_lines = sum(1 for _ in open_maybe_gzip(self.input_path, "rt"))

        with tqdm(total=total_lines, desc="Analyzing poems", unit=" poem") as pbar:
            for poem in iter_jsonl(self.input_path):
                collection_title = poem.get("collection_title")
                collection_id = poem.get("collection_page_id")

                if collection_title:
                    if collection_id is not None:
                        self.title_to_id_cache.setdefault(collection_title, collection_id)
                    else:
                        titles_needing_id.add(collection_title)
                pbar.update(1)

        titles_to_fetch = titles_needing_id - set(self.title_to_id_cache.keys())
        logger.info(f"Analysis complete. {len(self.title_to_id_cache)} IDs found in cache. "
                    f"{len(titles_to_fetch)} unique IDs to fetch.")
        return titles_to_fetch

    async def _fetch_missing_ids_from_api(self, client: WikiAPIClient, titles: List[str]):
        """
        Queries the MediaWiki API in batches to find IDs for collection titles.
        """
        batch_size = 1
        tasks = []
        for i in range(0, len(titles), batch_size):
            batch = titles[i:i + batch_size]
            tasks.append(client.get_page_info_and_redirects(batch))

        found_count = 0
        with tqdm(total=len(tasks), desc="API calls", unit=" batch") as pbar:
            for future in asyncio.as_completed(tasks):
                try:
                    query_result = await future
                    if query_result:
                        self._process_api_result(query_result)
                        found_count += len(query_result.get("pages", []))
                except Exception as e:
                    logger.error(f"An API batch call failed: {e}", exc_info=True)
                pbar.update(1)

        logger.info(f"{len(self.title_to_id_cache) - found_count} IDs were added to the cache via the API.")

    def _process_api_result(self, query_result: Dict[str, Any]):
        """Processes an API call result to update the `title_to_id` cache."""
        pages = {p['title']: p for p in query_result.get("pages", []) if "missing" not in p}
        redirects = {r['from']: r['to'] for r in query_result.get("redirects", [])}

        for title, page_info in pages.items():
            page_id = page_info.get("pageid")
            if page_id:
                self.title_to_id_cache[title] = page_id

        for from_title, to_title in redirects.items():
            if to_title in self.title_to_id_cache:
                self.title_to_id_cache[from_title] = self.title_to_id_cache[to_title]

    async def _enrich_and_write_file(self):
        """Reads the input file a second time, enriches the data, and writes the output file."""
        logger.info("Phase 2: Enriching and writing the new file...")
        enriched_count = 0
        total_lines = sum(1 for _ in open_maybe_gzip(self.input_path, "rt"))

        with open_maybe_gzip(self.output_path, "wt") as fout:
            with tqdm(total=total_lines, desc="Writing poems", unit=" poem") as pbar:
                for poem in iter_jsonl(self.input_path):
                    if poem.get("collection_page_id") is None:
                        title = poem.get("collection_title")
                        if title and title in self.title_to_id_cache:
                            poem["collection_page_id"] = self.title_to_id_cache[title]
                            enriched_count += 1

                    fout.write(json.dumps(poem, ensure_ascii=False) + "\n")
                    pbar.update(1)

        logger.info(f"Writing complete. {enriched_count} poems were enriched with a `collection_page_id`.")
