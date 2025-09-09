import asyncio
import functools
import gzip
import json
import logging
import queue
import threading
from typing import Set, List

import mwparserfromhell
from bs4 import BeautifulSoup
from tqdm import tqdm

from .api_client import WikiAPIClient, get_localized_category_prefix
from .classifier import PageClassifier, PageType
from .database import DatabaseManager, connect_sync_db
from .exceptions import PageProcessingError, PoemParsingError
from .processors import PoemProcessor
from .schemas import PoemSchema
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
        logger.info(f"Starting intelligent scraper for '{self.config.lang}.wikisource.org'")
        logger.info(f"Root category: '{self.config.category}'")
        
        await self.db_manager.initialize()

        if self.config.resume:
            self.processed_ids = await self.db_manager.get_all_processed_ids()
            logger.info(f"Resume mode: Loaded {len(self.processed_ids)} already processed page IDs.")

        page_queue = asyncio.Queue(maxsize=self.config.workers * 10)
        writer_sync_queue = queue.Queue(maxsize=self.config.workers * 2)
        
        writer_thread = threading.Thread(
            target=self._sync_writer, args=(writer_sync_queue,), daemon=True
        )
        writer_thread.start()

        async with WikiAPIClient(self.api_endpoint, self.config.workers) as client:
            producer_task = asyncio.create_task(self._producer(client, page_queue))

            with tqdm(desc="Processing pages", unit=" page", dynamic_ncols=True) as pbar:
                consumer_tasks = [
                    asyncio.create_task(self._consumer(client, page_queue, writer_sync_queue, pbar))
                    for _ in range(self.config.workers)
                ]
                
                await producer_task
                await page_queue.join()

                for task in consumer_tasks:
                    task.cancel()
                await asyncio.gather(*consumer_tasks, return_exceptions=True)
                
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
        """Trouve et met en file les pages initiales des catégories d'auteurs."""
        logger.info(f"Normalizing root category title '{self.config.category}'...")
        cat_prefix = get_localized_category_prefix(self.config.lang)
        full_cat_title = f"{cat_prefix}:{self.config.category}"
        
        page_info = await client.get_page_info_and_redirects([full_cat_title])

        if not page_info or not page_info.get("pages") or "missing" in page_info["pages"][0]:
            logger.warning(f"Category '{full_cat_title}' not found. Attempting search...")
            corrected_title = await client.search_for_page(full_cat_title, namespace=14)
            if not corrected_title:
                logger.critical(f"Root category '{self.config.category}' not found. Aborting.")
                return
            logger.info(f"Found likely match: '{corrected_title}'. Using this title.")
            full_cat_title = corrected_title

        logger.info(f"Phase 1: Discovering author subcategories in '{full_cat_title}'...")
        
        author_cat_titles = [
            cat['title']
            async for cat in client.get_subcategories_generator(full_cat_title.split(":")[-1], self.config.lang)
        ]
        logger.info(f"Found {len(author_cat_titles)} potential author categories. Checking which are non-empty...")

        non_empty_author_cats = []
        if author_cat_titles:
            for i in range(0, len(author_cat_titles), 50):
                batch_titles = author_cat_titles[i:i + 50]
                info = await client.get_category_info([t.split(":")[-1] for t in batch_titles], self.config.lang)
                for title, cat_info in info.items():
                    if cat_info.get('pages', 0) > 0 or cat_info.get('subcats', 0) > 0:
                        non_empty_author_cats.append(title)

        logger.info(f"Found {len(non_empty_author_cats)} non-empty author categories. Discovering pages...")
        
        enqueued_count = 0
        if non_empty_author_cats:
            with tqdm(total=len(non_empty_author_cats), desc="Discovering pages", unit=" author_cat") as pbar:
                for author_cat_full_title in non_empty_author_cats:
                    cat_name = author_cat_full_title.split(":")[-1]
                    async for page in client.get_pages_in_category_generator(cat_name, self.config.lang):
                        if self.config.limit and enqueued_count >= self.config.limit: break
                        if page['pageid'] not in self.processed_ids:
                            await queue.put({
                                'page_info': page,
                                'parent_title': author_cat_full_title,
                                'author_cat': author_cat_full_title 
                            })
                            enqueued_count += 1
                    pbar.update(1)
                    if self.config.limit and enqueued_count >= self.config.limit: break
        
        logger.info(f"Producer finished. Enqueued {enqueued_count} initial pages for processing.")

    async def _consumer(
        self,
        client: WikiAPIClient,
        page_queue: asyncio.Queue,
        writer_queue: queue.Queue,
        pbar: tqdm,
    ):
        """Consomme les pages de la file d'attente et les délègue pour traitement."""
        while True:
            try:
                queue_item = await page_queue.get()
                await self._process_page(client, page_queue, writer_queue, pbar, queue_item)
                page_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.critical("Critical error in consumer loop", exc_info=True)


    async def _process_page(
        self,
        client: WikiAPIClient,
        page_queue: asyncio.Queue,
        writer_queue: queue.Queue,
        pbar: tqdm,
        queue_item: dict,
    ):
        """Récupère, classifie et traite une seule page."""
        page_info = queue_item['page_info']
        parent_title = queue_item['parent_title']
        author_cat = queue_item['author_cat']
        page_id = page_info["pageid"]
        page_title = page_info.get('title', 'N/A')

        if page_id in self.processed_ids:
            return

        try:
            page_data = await client.get_page_data_by_id(page_id)
            if not page_data:
                raise PageProcessingError("API did not return page data.")
            
            page_html = await client.get_rendered_html(page_id)
            if not page_html:
                raise PageProcessingError("API did not return rendered HTML.")

            soup = BeautifulSoup(page_html, "lxml")
            wikitext = page_data.get("revisions", [{}])[0].get("content", "")
            wikicode = mwparserfromhell.parse(wikitext)
            
            classifier = PageClassifier(page_data, soup, self.config.lang, wikicode)
            page_type, reason = classifier.classify()

            if self.tree_logger:
                self.tree_logger.add_node(author_cat, parent_title, page_title, page_type, reason)

            if page_type == PageType.POEM:
                try:
                    poem_data = self.processor.process(page_data, soup, self.config.lang, wikicode)
                    logger.debug(f"CLASSIFIED: '{page_title}' as POEM ({reason}). Processing.")
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, functools.partial(writer_queue.put, poem_data))
                except PoemParsingError as e:
                    logger.warning(f"Page '{page_title}' (classified as POEM due to '{reason}') failed parsing: {e}")
                    self.skipped_counter += 1
            
            else:
                logger.info(f"CLASSIFIED: '{page_title}' as {page_type.name} ({reason}). Exploring for sub-pages.")
                sub_titles = classifier.extract_sub_page_titles()
                if sub_titles:
                    logger.debug(f"Found {len(sub_titles)} sub-pages in '{page_title}'. Enqueuing.")
                    await self._enqueue_new_titles(client, page_queue, list(sub_titles), pbar, current_parent_title=page_title, author_cat=author_cat)
                else:
                    logger.debug(f"No sub-pages found in '{page_title}'.")
                self.skipped_counter += 1

        except Exception as e:
            logger.error(f"Error processing page {page_title} (ID: {page_id})", exc_info=True)
            self.skipped_counter += 1
        finally:
            self.processed_ids.add(page_id)
            pbar.update(1)


    async def _enqueue_new_titles(
        self, client: WikiAPIClient, queue: asyncio.Queue, titles: List[str], pbar: tqdm,
        current_parent_title: str, author_cat: str
    ):
        """Met en file de nouveaux titres à traiter après les avoir résolus via l'API."""
        logger.debug(f"Resolving {len(titles)} titles found in '{current_parent_title}'.")
        for i in range(0, len(titles), 50):
            batch = titles[i : i + 50]
            query_result = await client.get_page_info_and_redirects(batch)

            if not query_result or not query_result.get("pages"):
                continue

            new_pages_to_enqueue = []
            for p_info in query_result.get("pages", []):
                if "missing" in p_info:
                    continue
                
                page_id = p_info.get("pageid")
                if page_id and page_id not in self.processed_ids:
                    new_pages_to_enqueue.append(p_info)
            
            if new_pages_to_enqueue:
                pbar.total = (pbar.total or 0) + len(new_pages_to_enqueue)
                pbar.refresh()
                for p_info in new_pages_to_enqueue:
                    await queue.put({
                        'page_info': p_info,
                        'parent_title': current_parent_title,
                        'author_cat': author_cat
                    })

    def _sync_writer(self, writer_queue: queue.Queue):
        """Tâche synchrone pour gérer toutes les E/S disque."""
        db_conn, db_cursor = connect_sync_db(self.db_path)
        try:
            with gzip.open(self.output_file, "at", encoding='utf-8') as f_gz:
                while True:
                    result = writer_queue.get()
                    if result is None:
                        break
                    
                    if isinstance(result, PoemSchema):
                        f_gz.write(result.model_dump_json() + "\n")
                        self.db_manager.add_poem_index_sync(result, db_cursor)
                        self.processed_counter += 1
                    
                    writer_queue.task_done()
        finally:
            writer_queue.task_done()
            db_conn.commit()
            db_conn.close()