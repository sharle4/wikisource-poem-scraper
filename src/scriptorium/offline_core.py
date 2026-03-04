"""
Offline pipeline orchestrator for processing Wikimedia dump files.

Replaces the async producer/consumer architecture of core.py with a
sequential, multi-phase pipeline that processes local dump files to
extract poems, producing the same output as the online ScraperOrchestrator.
"""
import gzip
import hashlib
import json
import logging
import logging.handlers
import shutil
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import mwparserfromhell
from bs4 import BeautifulSoup
from tqdm import tqdm

from .classifier import PageClassifier, PageType
from .cleaner import process_poem
from .database import DatabaseManager
from .dump_index import DumpIndexBuilder
from .dump_readers import iter_ndjson_pages, iter_xml_pages
from .exceptions import PoemParsingError
from .log_manager import LogManager
from .processors import PoemProcessor
from .schemas import Collection, PoemInfo, PoemSchema, Section
from .tree_logger import HierarchicalLogger

logger = logging.getLogger(__name__)

collection_log = logging.getLogger("collection_processing")
collection_log.propagate = False

# Expected dump files
EXPECTED_SQL_FILES = [
    "frwikisource-latest-page.sql",
    "frwikisource-latest-categorylinks.sql",
    "frwikisource-latest-linktarget.sql",
    "frwikisource-latest-redirect.sql",
]


def _classify_page_worker(args: Tuple) -> Optional[Dict[str, Any]]:
    """
    Worker function for ProcessPoolExecutor.
    Parses HTML with BeautifulSoup, classifies the page, and returns
    a serializable result dict (NOT soup objects — those can't cross
    process boundaries).

    Returns None if the page should be skipped.
    """
    (
        page_id,
        revision_id,
        title,
        url,
        html,
        date_modified,
        categories_set,
        lang,
    ) = args

    try:
        soup = BeautifulSoup(html, "lxml")
        wikicode = mwparserfromhell.parse("")

        page_data = {
            "pageid": page_id,
            "title": title,
            "ns": 0,
            "fullurl": url,
            "categories": [{"title": f"Catégorie:{cat}"} for cat in categories_set],
            "revisions": [{"revid": revision_id, "content": ""}],
        }

        classifier = PageClassifier(page_data, soup, lang, wikicode)
        page_type, reason = classifier.classify()

        result = {
            "page_id": page_id,
            "revision_id": revision_id,
            "title": title,
            "url": url,
            "html": html,
            "date_modified": date_modified,
            "page_type": page_type.name,
            "reason": reason,
            "page_data": page_data,
        }

        # Extract collection links or hub sub-pages if applicable
        if page_type == PageType.POETIC_COLLECTION:
            ordered_links = classifier.extract_ordered_collection_links()
            result["ordered_links"] = [
                (link_title, lt.name) for link_title, lt in ordered_links
            ]
        elif page_type == PageType.MULTI_VERSION_HUB:
            sub_titles = classifier.extract_hub_sub_pages()
            result["sub_titles"] = list(sub_titles)

        return result

    except Exception as e:
        logger.warning(f"Worker failed for page {page_id} ('{title}'): {e}")
        return None


class OfflineOrchestrator:
    """
    Offline pipeline that processes Wikimedia dump files to extract poems.
    Produces the same output as the online ScraperOrchestrator.
    """

    def __init__(self, config, log_manager: LogManager):
        self.config = config
        self.log_manager = log_manager
        self.lang = config.lang
        self.category = config.category
        self.output_dir = Path(config.output_dir)
        self.dumps_dir = Path(config.dumps_dir)
        self.limit = getattr(config, "limit", None)
        self.write_cleaned = str(getattr(config, "cleaned", "true")).lower() == "true"
        self.num_workers = getattr(config, "workers", 4)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "poems.jsonl.gz"
        self.cleaned_output_file = self.output_dir / "poems.cleaned.jsonl.gz"
        self.db_path = self.output_dir / "poems_index.sqlite"
        self.index_db_path = self.output_dir / "dump_index.sqlite"

        if not collection_log.hasHandlers():
            coll_log_path = self.output_dir / "logs" / "collection_processing.log"
            coll_log_path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.handlers.RotatingFileHandler(
                coll_log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
            )
            handler.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )
            collection_log.addHandler(handler)
            collection_log.setLevel(logging.DEBUG)

        self.db_manager = DatabaseManager(self.db_path)
        self.processor = PoemProcessor()
        self.index_builder = DumpIndexBuilder(self.dumps_dir, self.index_db_path)

        self.tree_logger: Optional[HierarchicalLogger] = None
        if getattr(config, "tree_log", False):
            tree_log_dir = self.output_dir / "logs" / "tree-logs"
            self.tree_logger = HierarchicalLogger(tree_log_dir)

        self.processed_counter = 0
        self.skipped_counter = 0

    def run(self):
        """Main execution method. Fully synchronous."""
        logger.info("=" * 60)
        logger.info("OFFLINE MODE — Scriptorium v5.1.0")
        logger.info("=" * 60)
        logger.info(f"Language: {self.lang}")
        logger.info(f"Root category: {self.category}")
        logger.info(f"Dumps directory: {self.dumps_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Workers: {self.num_workers}")

        self._validate_dump_files()
        self._check_disk_space()

        # Resume support
        already_processed: Set[int] = set()
        if getattr(self.config, "resume", False) and self.db_path.exists():
            already_processed = self.db_manager.get_all_processed_ids_sync()
            logger.info(
                f"Resume mode: loaded {len(already_processed)} already processed page IDs."
            )

        try:
            # ── PHASE 1: BUILD INDEX ──
            logger.info("")
            logger.info("=" * 40)
            logger.info("PHASE 1: Building dump index")
            logger.info("=" * 40)

            if self.index_db_path.exists() and getattr(self.config, "resume", False):
                logger.info("Index already exists. Reusing it (resume mode).")
                index_conn = sqlite3.connect(str(self.index_db_path))
            else:
                index_conn = self.index_builder.build()

            # Find target page_ids via category tree
            target_page_ids = self.index_builder.find_pages_in_category_tree(
                index_conn, self.category, self.lang
            )

            # Also identify collection pages (Recueils de poèmes)
            collection_page_ids = self.index_builder.find_pages_in_category_tree(
                index_conn, "Recueils de poèmes", self.lang
            )

            # Merge: we need to process both poems and collections
            all_target_ids = target_page_ids | collection_page_ids

            # Remove already-processed pages if resuming
            if already_processed:
                all_target_ids -= already_processed
                logger.info(
                    f"After resume filtering: {len(all_target_ids)} pages remaining."
                )

            if self.limit and len(all_target_ids) > self.limit:
                all_target_ids = set(sorted(all_target_ids)[: self.limit])

            # Build title→page_id index for collection inference
            logger.info("Building title→page_id index...")
            title_to_id = self.index_builder.build_title_to_id_index(index_conn, namespace=0)

            logger.info(
                f"Index built: {len(target_page_ids)} poem-category pages, "
                f"{len(collection_page_ids)} collection pages, "
                f"{len(title_to_id)} ns:0 title mappings."
            )

            # ── PHASE 2: PROCESS NDJSON ──
            logger.info("")
            logger.info("=" * 40)
            logger.info("PHASE 2: Processing NDJSON dumps")
            logger.info("=" * 40)

            # Data structures for results
            poems_pending: Dict[int, Dict[str, Any]] = {}
            collections: Dict[int, Dict[str, Any]] = {}
            hubs: Dict[int, Dict[str, Any]] = {}
            discovered_page_ids: Set[int] = set()

            self._process_ndjson_pass(
                index_conn,
                all_target_ids,
                title_to_id,
                collection_page_ids,
                poems_pending,
                collections,
                hubs,
                discovered_page_ids,
                already_processed,
                pass_number=1,
            )

            # Second pass for newly discovered pages
            if discovered_page_ids:
                discovered_page_ids -= already_processed
                discovered_page_ids -= set(poems_pending.keys())
                if discovered_page_ids:
                    logger.info(
                        f"Second NDJSON pass for {len(discovered_page_ids)} "
                        f"discovered pages (from collections/hubs)."
                    )
                    self._process_ndjson_pass(
                        index_conn,
                        discovered_page_ids,
                        title_to_id,
                        collection_page_ids,
                        poems_pending,
                        collections,
                        hubs,
                        set(),  # no further discovery
                        already_processed,
                        pass_number=2,
                    )

            logger.info(
                f"NDJSON processing complete: {len(poems_pending)} poems pending, "
                f"{len(collections)} collections, {len(hubs)} hubs."
            )

            # ── PHASE 3: COLLECTION & HUB ENRICHMENT ──
            logger.info("")
            logger.info("=" * 40)
            logger.info("PHASE 3: Collection & hub enrichment")
            logger.info("=" * 40)

            poem_collection_context = self._build_collection_context(
                collections, hubs, poems_pending, title_to_id, index_conn
            )

            # ── PHASE 4: WIKITEXT ENRICHMENT (XML) ──
            logger.info("")
            logger.info("=" * 40)
            logger.info("PHASE 4: Extracting wikitext from XML dumps")
            logger.info("=" * 40)

            wikitext_page_ids = set(poems_pending.keys())
            wikitext_dict: Dict[int, str] = {}

            for record in iter_xml_pages(
                self.dumps_dir,
                target_page_ids=wikitext_page_ids,
                target_namespace=0,
            ):
                wikitext_dict[record["page_id"]] = record["wikitext"]

            logger.info(
                f"XML extraction complete: found wikitext for "
                f"{len(wikitext_dict)}/{len(wikitext_page_ids)} poems."
            )

            # ── PHASE 5: FINALIZE AND WRITE ──
            logger.info("")
            logger.info("=" * 40)
            logger.info("PHASE 5: Finalizing and writing output")
            logger.info("=" * 40)

            self._finalize_and_write(
                poems_pending,
                poem_collection_context,
                wikitext_dict,
                collections,
            )

        except KeyboardInterrupt:
            logger.info("Interrupted by user. Flushing partial output...")
        finally:
            if self.tree_logger:
                self.tree_logger.write_log_files()
            try:
                index_conn.close()
            except Exception:
                pass

            logger.info("")
            logger.info("=" * 40)
            logger.info("OFFLINE SCRAPING COMPLETE")
            logger.info(f"Total poems processed and saved: {self.processed_counter}")
            logger.info(f"Total pages skipped: {self.skipped_counter}")
            logger.info("=" * 40)

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def _validate_dump_files(self):
        """Validates that required dump files exist."""
        missing = []
        for sql_file in EXPECTED_SQL_FILES:
            if not (self.dumps_dir / sql_file).exists():
                missing.append(sql_file)

        ndjson_files = list(self.dumps_dir.glob("frwikisource_namespace_0_*.ndjson"))
        if not ndjson_files:
            missing.append("frwikisource_namespace_0_*.ndjson (at least one)")

        xml_files = list(
            self.dumps_dir.glob("frwikisource-latest-pages-articles1.xml-*")
        )
        if not xml_files:
            missing.append("frwikisource-latest-pages-articles1.xml-* (at least one)")

        if missing:
            msg = "Missing required dump files:\n" + "\n".join(
                f"  - {f}" for f in missing
            )
            logger.critical(msg)
            raise FileNotFoundError(msg)

    def _check_disk_space(self):
        """Checks that the output directory has at least 1 GB free."""
        usage = shutil.disk_usage(self.output_dir)
        free_gb = usage.free / (1024**3)
        if free_gb < 1.0:
            logger.warning(
                f"Low disk space: only {free_gb:.2f} GB free in {self.output_dir}. "
                f"At least 1 GB recommended."
            )

    # ------------------------------------------------------------------
    # PHASE 2: NDJSON processing
    # ------------------------------------------------------------------

    def _process_ndjson_pass(
        self,
        index_conn: sqlite3.Connection,
        target_ids: Set[int],
        title_to_id: Dict[str, int],
        collection_page_ids: Set[int],
        poems_pending: Dict[int, Dict[str, Any]],
        collections: Dict[int, Dict[str, Any]],
        hubs: Dict[int, Dict[str, Any]],
        discovered_page_ids: Set[int],
        already_processed: Set[int],
        pass_number: int = 1,
    ):
        """
        Single pass through the NDJSON files, classifying pages and
        routing them to the appropriate storage dict.
        """
        # Pre-fetch categories for all target pages in batch
        logger.info(f"Pre-fetching categories for {len(target_ids)} target pages...")
        page_categories: Dict[int, Set[str]] = {}
        target_list = list(target_ids)
        for i in range(0, len(target_list), 500):
            chunk = target_list[i : i + 500]
            placeholders = ",".join("?" * len(chunk))
            rows = index_conn.execute(
                f"""
                SELECT cl.cl_from, lt.lt_title
                FROM categorylinks cl
                JOIN linktarget lt ON cl.cl_target_id = lt.lt_id
                WHERE cl.cl_from IN ({placeholders}) AND lt.lt_namespace = 14
                """,
                chunk,
            ).fetchall()
            for page_id, cat_title in rows:
                page_categories.setdefault(page_id, set()).add(
                    cat_title.replace("_", " ")
                )

        # Build worker argument batches
        batch: List[Tuple] = []
        batch_size = max(1, self.num_workers * 4)

        def _process_batch(batch_args: List[Tuple]):
            """Submit batch to pool and process results."""
            with ProcessPoolExecutor(max_workers=self.num_workers) as pool:
                futures = {pool.submit(_classify_page_worker, args): args[0] for args in batch_args}
                for future in as_completed(futures):
                    result = future.result()
                    if result is None:
                        self.skipped_counter += 1
                        continue
                    self._route_classification_result(
                        result,
                        index_conn,
                        title_to_id,
                        collection_page_ids,
                        poems_pending,
                        collections,
                        hubs,
                        discovered_page_ids,
                        already_processed,
                    )

        for record in iter_ndjson_pages(self.dumps_dir, target_page_ids=target_ids):
            page_id = record["page_id"]
            if page_id in already_processed:
                continue

            categories = page_categories.get(page_id, set())

            worker_args = (
                page_id,
                record["revision_id"],
                record["title"],
                record["url"],
                record["html"],
                record["date_modified"],
                categories,
                self.lang,
            )
            batch.append(worker_args)

            if len(batch) >= batch_size:
                _process_batch(batch)
                batch.clear()

        # Process remaining batch
        if batch:
            _process_batch(batch)
            batch.clear()

    def _route_classification_result(
        self,
        result: Dict[str, Any],
        index_conn: sqlite3.Connection,
        title_to_id: Dict[str, int],
        collection_page_ids: Set[int],
        poems_pending: Dict[int, Dict[str, Any]],
        collections: Dict[int, Dict[str, Any]],
        hubs: Dict[int, Dict[str, Any]],
        discovered_page_ids: Set[int],
        already_processed: Set[int],
    ):
        """Route a classified page to the appropriate storage."""
        page_id = result["page_id"]
        page_type_name = result["page_type"]
        title = result["title"]
        reason = result["reason"]
        url = result["url"]
        timestamp = datetime.now(timezone.utc)

        collection_log.info(
            f"CLASSIFIED page '{title}' (id:{page_id}) as {page_type_name}. Reason: {reason}"
        )

        if self.tree_logger:
            page_type_enum = PageType[page_type_name]
            self.tree_logger.add_node(
                f"Catégorie:{self.category}",
                f"Catégorie:{self.category}",
                title,
                page_type_enum,
                reason,
                timestamp,
            )

        if page_type_name == PageType.POEM.name:
            # Store for later processing (Phase 5)
            # Store raw HTML, NOT soup objects (memory saving)
            poems_pending[page_id] = {
                "page_data": result["page_data"],
                "html": result["html"],
            }

        elif page_type_name == PageType.POETIC_COLLECTION.name:
            ordered_links = result.get("ordered_links", [])

            collection_log.info(
                f"COLLECTION '{title}' (id:{page_id}) has {len(ordered_links)} items."
            )

            self.log_manager.log_collection(
                timestamp.isoformat(),
                title,
                url,
                f"Catégorie:{self.category}",
                "is_poetic_collection",
                len([l for l in ordered_links if l[1] == PageType.POEM.name]),
            )

            collections[page_id] = {
                "title": title,
                "url": url,
                "ordered_links": ordered_links,
            }

            # Resolve poem titles to page_ids; discover new pages
            for link_title, link_type_name in ordered_links:
                if link_type_name == PageType.POEM.name:
                    resolved_id = title_to_id.get(link_title)
                    if resolved_id is None:
                        # Try with underscores
                        resolved_id = title_to_id.get(
                            link_title.replace(" ", "_")
                        )
                    if resolved_id is not None:
                        if (
                            resolved_id not in poems_pending
                            and resolved_id not in already_processed
                        ):
                            discovered_page_ids.add(resolved_id)
                    else:
                        collection_log.warning(
                            f"Could not resolve poem title '{link_title}' "
                            f"from collection '{title}'."
                        )

            self.skipped_counter += 1

        elif page_type_name == PageType.MULTI_VERSION_HUB.name:
            sub_titles = result.get("sub_titles", [])

            self.log_manager.log_hub(
                timestamp.isoformat(),
                title,
                url,
                f"Catégorie:{self.category}",
                reason,
                len(sub_titles),
            )

            hubs[page_id] = {
                "title": title,
                "sub_page_ids": set(),
            }

            for sub_title in sub_titles:
                resolved_id = title_to_id.get(sub_title)
                if resolved_id is None:
                    resolved_id = title_to_id.get(sub_title.replace(" ", "_"))
                if resolved_id is not None:
                    hubs[page_id]["sub_page_ids"].add(resolved_id)
                    if (
                        resolved_id not in poems_pending
                        and resolved_id not in already_processed
                    ):
                        discovered_page_ids.add(resolved_id)

            self.skipped_counter += 1

        else:
            # OTHER, AUTHOR, DISAMBIGUATION
            self.log_manager.log_other(
                timestamp.isoformat(),
                title,
                url,
                f"Catégorie:{self.category}",
                reason,
            )
            self.skipped_counter += 1

    # ------------------------------------------------------------------
    # PHASE 3: Collection & hub enrichment
    # ------------------------------------------------------------------

    def _build_collection_context(
        self,
        collections: Dict[int, Dict[str, Any]],
        hubs: Dict[int, Dict[str, Any]],
        poems_pending: Dict[int, Dict[str, Any]],
        title_to_id: Dict[str, int],
        index_conn: sqlite3.Connection,
    ) -> Dict[int, Dict[str, Any]]:
        """
        Build a mapping: poem_page_id → collection/hub context.
        Returns dict with keys:
          - collection_page_id, collection_title, section_title, poem_order
          - hub_title, hub_page_id
          - is_first_poem_in_collection
          - collection_obj (Collection object, only for first poem)
        """
        context: Dict[int, Dict[str, Any]] = {}

        # Process collections
        for coll_page_id, coll_data in collections.items():
            coll_title = coll_data["title"]
            coll_url = coll_data["url"]
            ordered_links = coll_data.get("ordered_links", [])

            # Build Collection object
            collection_obj = Collection(
                page_id=coll_page_id,
                title=coll_title,
                url=coll_url,
            )

            current_section: Optional[Section] = None
            poem_order = 0
            is_first = True

            for link_title, link_type_name in ordered_links:
                if link_type_name == PageType.SECTION_TITLE.name:
                    current_section = Section(title=link_title)
                    collection_obj.content.append(current_section)
                elif link_type_name == PageType.POEM.name:
                    resolved_id = title_to_id.get(link_title)
                    if resolved_id is None:
                        resolved_id = title_to_id.get(link_title.replace(" ", "_"))

                    if resolved_id is not None and resolved_id in poems_pending:
                        encoded_title = quote(link_title.replace(" ", "_"))
                        poem_info = PoemInfo(
                            title=link_title,
                            page_id=resolved_id,
                            url=f"https://{self.lang}.wikisource.org/wiki/{encoded_title}",
                        )

                        if current_section:
                            current_section.poems.append(poem_info)
                        else:
                            collection_obj.content.append(poem_info)

                        section_title = (
                            current_section.title if current_section else None
                        )

                        context[resolved_id] = {
                            "collection_page_id": coll_page_id,
                            "collection_title": coll_title,
                            "section_title": section_title,
                            "poem_order": poem_order,
                            "is_first_poem_in_collection": is_first,
                            "collection_obj": collection_obj if is_first else None,
                        }

                        poem_order += 1
                        is_first = False

            collection_log.info(
                f"Collection '{coll_title}' (id:{coll_page_id}): "
                f"enriched {poem_order} poems."
            )

        # Process hubs
        for hub_page_id, hub_data in hubs.items():
            hub_title = hub_data["title"]
            for sub_id in hub_data.get("sub_page_ids", set()):
                if sub_id in poems_pending:
                    ctx = context.setdefault(sub_id, {})
                    ctx["hub_title"] = hub_title
                    ctx["hub_page_id"] = hub_page_id

        # For poems with metadata.source_collection but no collection context:
        # look up the title in title→page_id index
        for page_id, pending in poems_pending.items():
            if page_id in context and context[page_id].get("collection_page_id"):
                continue
            # Check if the poem title suggests a collection (has a "/" separator)
            poem_title = pending["page_data"].get("title", "")
            if "/" in poem_title:
                parent_title = poem_title.split("/")[0].strip()
                parent_id = title_to_id.get(parent_title)
                if parent_id is not None and parent_id in collections:
                    # This poem belongs to a known collection
                    ctx = context.setdefault(page_id, {})
                    if not ctx.get("collection_page_id"):
                        ctx["collection_page_id"] = parent_id
                        ctx["collection_title"] = collections[parent_id]["title"]

        logger.info(
            f"Enrichment complete: {len(context)} poems have collection/hub context."
        )
        return context

    # ------------------------------------------------------------------
    # PHASE 5: Finalize and write
    # ------------------------------------------------------------------

    def _finalize_and_write(
        self,
        poems_pending: Dict[int, Dict[str, Any]],
        poem_collection_context: Dict[int, Dict[str, Any]],
        wikitext_dict: Dict[int, str],
        collections: Dict[int, Dict[str, Any]],
    ):
        """Final phase: process each poem and write output."""
        # Initialize output database
        db_conn, db_cursor = self.db_manager.initialize_sync()

        sorted_page_ids = sorted(poems_pending.keys())
        seen_cleaned: Set[int] = set()

        cleaned_fp = None
        if self.write_cleaned:
            cleaned_fp = gzip.open(self.cleaned_output_file, "at", encoding="utf-8")

        try:
            with gzip.open(self.output_file, "at", encoding="utf-8") as f_gz:
                for page_id in tqdm(
                    sorted_page_ids, desc="Writing poems", unit=" poem"
                ):
                    pending = poems_pending[page_id]
                    page_data = pending["page_data"]
                    raw_html = pending["html"]

                    # Get wikitext from XML
                    wikitext = wikitext_dict.get(page_id, "")
                    page_data["revisions"][0]["content"] = wikitext
                    wikicode = (
                        mwparserfromhell.parse(wikitext)
                        if wikitext
                        else mwparserfromhell.parse("")
                    )

                    # Re-parse HTML
                    soup = BeautifulSoup(raw_html, "lxml")

                    # Get context
                    ctx = poem_collection_context.get(page_id, {})
                    collection_page_id = ctx.get("collection_page_id")
                    collection_title = ctx.get("collection_title")
                    section_title = ctx.get("section_title")
                    poem_order = ctx.get("poem_order")
                    is_first = ctx.get("is_first_poem_in_collection", False)
                    collection_obj = ctx.get("collection_obj")
                    hub_title = ctx.get("hub_title")
                    hub_page_id_ctx = ctx.get("hub_page_id")

                    # Build hub_info dict if available
                    hub_info = None
                    if hub_title is not None and hub_page_id_ctx is not None:
                        hub_info = {
                            "title": hub_title,
                            "page_id": hub_page_id_ctx,
                        }

                    # Build collection_context (Collection object) if needed
                    collection_context_obj = None
                    if collection_page_id is not None:
                        if is_first and collection_obj is not None:
                            collection_context_obj = collection_obj
                        elif collection_page_id in collections:
                            # Not the first poem — pass a Collection without
                            # the structure (only for page_id and title reference)
                            coll_data = collections[collection_page_id]
                            collection_context_obj = Collection(
                                page_id=collection_page_id,
                                title=coll_data["title"],
                                url=coll_data["url"],
                            )

                    try:
                        poem_data = self.processor.process(
                            page_data,
                            soup,
                            self.lang,
                            wikicode,
                            hub_info=hub_info,
                            collection_context=collection_context_obj,
                            order_in_collection=poem_order,
                            section_title_in_collection=section_title,
                            is_first_poem_in_collection=is_first,
                        )

                        # Override provenance
                        poem_data.provenance = "dump"

                        # Write to output
                        json_str = poem_data.model_dump_json(exclude_none=True)
                        f_gz.write(json_str + "\n")

                        if cleaned_fp is not None and page_id not in seen_cleaned:
                            seen_cleaned.add(page_id)
                            poem_dict = poem_data.model_dump(
                                mode="json", exclude_none=True
                            )
                            cleaned_poem = process_poem(poem_dict)
                            cleaned_fp.write(
                                json.dumps(cleaned_poem, ensure_ascii=False) + "\n"
                            )

                        self.db_manager.add_poem_index_sync(poem_data, db_cursor)
                        self.processed_counter += 1

                    except PoemParsingError as e:
                        logger.warning(
                            f"Poem parsing failed for '{page_data.get('title', page_id)}': {e}"
                        )
                        self.skipped_counter += 1
                    except Exception as e:
                        logger.error(
                            f"Error processing poem {page_id} "
                            f"('{page_data.get('title', 'N/A')}'): {e}"
                        )
                        self.skipped_counter += 1

        finally:
            if cleaned_fp is not None:
                try:
                    cleaned_fp.close()
                except Exception:
                    pass
            db_conn.commit()
            db_conn.close()
