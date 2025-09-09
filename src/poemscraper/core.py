import asyncio
import functools
import gzip
import queue
import functools
import logging
import queue
import threading
from typing import Set

import mwparserfromhell
from bs4 import BeautifulSoup
from tqdm import tqdm

from .api_client import WikiAPIClient, get_localized_category_prefix
from .classifier import PageClassifier, PageType
from .database import DatabaseManager, connect_sync_db
from .processors import PoemProcessor
from .exceptions import PageProcessingError, PoemParsingError
from .processors import PoemProcessor
from .schemas import PoemSchema
from .classifier import PageClassifier, PageType
from .tree_logger import HierarchicalLogger

logger = logging.getLogger(__name__)

class ScraperOrchestrator:
    """Orchestre le workflow de scraping intelligent et hiérarchique."""

    def __init__(self, config):
        self.config = config
        self.api_endpoint = f"https://{config.lang}.wikisource.org/w/api.php"

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.config.output_dir / "poems.jsonl.gz"
        self.db_path = self.config.output_dir / "poems_index.sqlite"

        self.db_manager = DatabaseManager(self.db_path)
        self.processor = PoemProcessor()

        self.tree_logger = None
        if self.config.tree_log:
            log_dir = self.config.output_dir / "logs"
            self.tree_logger = HierarchicalLogger(log_dir)
            logger.info(f"Hierarchical logging enabled. Logs will be saved to {log_dir}")
        
        self.processed_ids: Set[int] = set()
        self.processed_counter = 0
        self.skipped_counter = 0

    async def run(self):
        """Méthode d'exécution principale."""
        logger.info(
            f"Starting intelligent scraper for '{self.config.lang}.wikisource.org'"
        )
        logger.info(f"Root category: '{self.config.category}'")

        await self.db_manager.initialize()
        if self.config.resume:
            self.processed_ids = await self.db_manager.get_all_processed_ids()
            logger.info(
                f"Resume mode: Loaded {len(self.processed_ids)} already processed page IDs."
            )

        page_queue = asyncio.Queue(maxsize=self.config.workers * 5)
        writer_sync_queue = queue.Queue(maxsize=self.config.workers * 2)

        writer_thread = threading.Thread(
            target=self._sync_writer, args=(writer_sync_queue,), daemon=True
        )
        writer_thread.start()

        async with WikiAPIClient(self.api_endpoint, self.config.workers) as client:
            producer_task = asyncio.create_task(self._producer(client, page_queue))

            with tqdm(
                desc="Processing pages", unit=" page", dynamic_ncols=True
            ) as pbar:
                consumer_tasks = [
                    asyncio.create_task(
                        self._consumer(client, page_queue, writer_sync_queue, pbar)
                    )
                    for _ in range(self.config.workers)
                ]

                await producer_task

                await page_queue.join()

                for _ in consumer_tasks:
                    await page_queue.put(None)
                await asyncio.gather(*consumer_tasks)

                writer_sync_queue.put(None)
                writer_thread.join()

        if self.tree_logger:
            self.tree_logger.write_log_files()
        
        await self.db_manager.close()
        logger.info("Scraping finished.")
        logger.info(f"Total poems processed and saved: {self.processed_counter}")
        logger.info(
            f"Total pages skipped (non-poem, collection, etc.): {self.skipped_counter}"
        )

    async def _producer(self, client: WikiAPIClient, queue: asyncio.Queue):
        """Découvre et met en file les pages initiales pour le traitement."""
        logger.info(f"Normalizing root category title '{self.config.category}'...")
        cat_prefix = get_localized_category_prefix(self.config.lang)
        full_cat_title = f"{cat_prefix}:{self.config.category}"

        page_info = await client.get_page_info([full_cat_title])

        if not page_info or not page_info.get("pages"):
            logger.critical(
                f"Root category '{self.config.category}' query failed. Aborting."
            )
            return

        page_details = page_info["pages"][0]

        if "missing" in page_details:
            logger.warning(
                f"Category '{full_cat_title}' not found with exact title. Attempting search fallback..."
            )
            corrected_title = await client.search_for_page(
                self.config.category, namespace=14
            )

            if not corrected_title:
                logger.critical(
                    f"Root category '{self.config.category}' does not exist and could not be found via search. Aborting."
                )
                return

            logger.info(
                f"Found likely match via search: '{corrected_title}'. Using this title."
            )
            page_info = await client.get_page_info([corrected_title])
            page_details = page_info["pages"][0]
            if "missing" in page_details:
                logger.critical(
                    f"Corrected category '{corrected_title}' is also missing. Aborting."
                )
                return

        canonical_category_title = page_details["title"].split(":", 1)[1]
        if canonical_category_title != self.config.category:
            logger.info(
                f"Normalized category title from '{self.config.category}' to '{canonical_category_title}'"
            )

        logger.info(
            f"Phase 1: Discovering author subcategories in '{canonical_category_title}'..."
        )
        author_cat_titles = [
            cat["title"].split(":", 1)[1]
            async for cat in client.get_subcategories_generator(
                canonical_category_title, self.config.lang
            )
        ]

        logger.info(
            f"Found {len(author_cat_titles)} potential author categories. Checking which are non-empty..."
        )

        non_empty_author_cats = []
        if author_cat_titles:
            for i in range(0, len(author_cat_titles), 50):
                batch_titles = author_cat_titles[i : i + 50]
                info = await client.get_category_info(batch_titles, self.config.lang)
                for title, cat_info in info.items():
                    if cat_info.get("pages", 0) > 0:
                        non_empty_author_cats.append(title.split(":", 1)[1])

        logger.info(
            f"Found {len(non_empty_author_cats)} non-empty author categories. Discovering pages..."
        )

        enqueued_count = 0
        if non_empty_author_cats:
            with tqdm(
                total=len(non_empty_author_cats),
                desc="Discovering pages",
                unit=" author_cat",
            ) as pbar:
                for author_cat in non_empty_author_cats:
                    author_cat_full_title = f"{cat_prefix}:{author_cat}"
                    async for page in client.get_pages_in_category_generator(
                        author_cat, self.config.lang
                    ):
                        if self.config.limit and enqueued_count >= self.config.limit:
                            break
                        if page["pageid"] not in self.processed_ids:
                            await queue.put({
                                'page_info': page,
                                'parent_title': author_cat_full_title,
                                'author_cat': author_cat_full_title
                            })
                            enqueued_count += 1
                    pbar.update(1)
                    if self.config.limit and enqueued_count >= self.config.limit:
                        logger.info(f"Scrape limit ({self.config.limit}) reached.")
                        break

        logger.info(
            f"Producer finished. Enqueued {enqueued_count} initial pages for processing."
        )

    async def _consumer(
        self,
        client: WikiAPIClient,
        page_queue: asyncio.Queue,
        writer_queue: queue.Queue,
        pbar: tqdm,
    ):
        """
        Récupère une page, la classifie, et agit en conséquence :
        - POEM: traite et met en file pour écriture.
        - COLLECTION: extrait les liens et les remet dans la file de traitement.
        - OTHER: ignore.
        """
        loop = asyncio.get_running_loop()
        while True:
            queue_item = await page_queue.get()
            if queue_item is None:
                page_queue.task_done()
                break

            page_info = queue_item['page_info']
            parent_title = queue_item['parent_title']
            author_cat = queue_item['author_cat']

            page_id = page_info["pageid"]
            page_title = page_info.get('title', 'N/A')
            
            if page_id in self.processed_ids:
                logger.debug(f"Skipping page ID {page_id} already processed.")
                pbar.update(1)
                page_queue.task_done()
                continue

            try:
                page_html_task = client.get_rendered_html(page_id)
                page_data_task = client.get_page_data_by_id(page_id)
                page_html, page_data = await asyncio.gather(
                    page_html_task, page_data_task
                )

                if not page_data or not page_html:
                    raise PageProcessingError("Missing page data or rendered HTML.")

                soup = BeautifulSoup(page_html, "lxml")

                wikitext = page_data.get("revisions", [{}])[0].get("content", "")
                if not wikitext:
                    raise PageProcessingError("Page has no wikitext content.")

                wikicode = mwparserfromhell.parse(wikitext)

                classifier = PageClassifier(
                    page_data, soup, self.config.lang, wikicode
                )
                page_type = classifier.classify()

                if self.tree_logger:
                    self.tree_logger.add_node(author_cat, parent_title, page_title, page_type)
                
                if page_type == PageType.POEM:
                    try:
                        poem_data = self.processor.process(
                            page_data, soup, self.config.lang, wikicode
                        )
                        await loop.run_in_executor(
                            None, functools.partial(writer_queue.put, poem_data)
                        )
                        self.processed_ids.add(page_id)
                    except PoemParsingError as e:
                        logger.warning(
                            f"Page {page_info.get('title', 'N/A')} (ID: {page_id}) failed parsing: {e}"
                        )
                        self.skipped_counter += 1

                elif page_type == PageType.COLLECTION:
                    logger.info(
                        f"Page '{page_info.get('title')}' is a collection. Extracting sub-pages."
                    )
                    sub_titles = classifier.extract_sub_page_titles()
                    if sub_titles:
                        await self._enqueue_new_titles(
                            client, page_queue, list(sub_titles), pbar, page_title, author_cat
                        )
                    self.processed_ids.add(page_id)
                    self.skipped_counter += 1

                else:
                    logger.debug(
                        f"Skipping page '{page_info.get('title')}' classified as {page_type.name}."
                    )
                    self.processed_ids.add(page_id)
                    self.skipped_counter += 1

            except Exception as e:
                logger.error(
                    f"Error processing page {page_info.get('title', 'N/A')} (ID: {page_id}): {e}",
                    exc_info=False,
                )
                self.skipped_counter += 1
            finally:
                pbar.update(1)
                page_queue.task_done()

    async def _enqueue_new_titles(
        self, client: WikiAPIClient, queue: asyncio.Queue, titles: list[str], pbar: tqdm,
        current_collection_title: str, author_cat: str
    ):
        """Résout une liste de titres en informations de page et les ajoute à la file."""
        logger.debug(f"Resolving {len(titles)} titles discovered from '{current_collection_title}'.")
        for i in range(0, len(titles), 50):
            batch = titles[i : i + 50]
            query_result = await client.get_page_info_and_redirects(batch)

            if not query_result or not query_result.get("pages"):
                continue
            
            pages_by_norm_title = {
                p.get("title", "").lower(): p for p in query_result.get("pages", [])
            }
            redirects = {
                r["from"].lower(): r["to"] for r in query_result.get("redirects", [])
            }

            for p_info in query_result.get("pages", []):
                if p_info.get("missing"): continue
                page_id = p_info.get("pageid")
                if page_id and page_id not in self.processed_ids:
                    pbar.total += 1
                    pbar.refresh()
                    await queue.put(
                        {
                            'page_info': p_info,
                            'parent_title': current_collection_title,
                            'author_cat': author_cat
                        }
                    )


    def _sync_writer(self, writer_queue: queue.Queue):
        """Tâche synchrone dans un thread dédié pour gérer toutes les I/O disque."""
        db_conn, db_cursor = connect_sync_db(self.db_path)
        with gzip.open(self.output_file, "at", encoding="utf-8") as f_gz:
            while True:
                result = writer_queue.get()
                if result is None:
                    writer_queue.task_done()
                    break

                if isinstance(result, PoemSchema):
                    f_gz.write(result.model_dump_json() + "\n")
                    self.db_manager.add_poem_index_sync(result, db_cursor)
                    self.processed_counter += 1

                writer_queue.task_done()

        db_conn.commit()
        db_conn.close()