import asyncio
import logging
import gzip
import json
import queue
import functools
import threading

from tqdm import tqdm

from .api_client import WikiAPIClient, get_localized_category_prefix
from .database import DatabaseManager, connect_sync_db
from .processors import PoemProcessor
from .exceptions import PageProcessingError, PoemParsingError
from .schemas import PoemSchema

logger = logging.getLogger(__name__)

class ScraperOrchestrator:
    """Manages the hierarchical scraping workflow."""
    def __init__(self, config):
        self.config = config
        self.api_endpoint = f"https://{config.lang}.wikisource.org/w/api.php"
        
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.config.output_dir / "poems.jsonl.gz"
        self.db_path = self.config.output_dir / "poems_index.sqlite"

        self.db_manager = DatabaseManager(self.db_path)
        self.processor = PoemProcessor()
        self.processed_counter = 0
        self.skipped_counter = 0

    async def run(self):
        """Main execution method."""
        logger.info(f"Starting hierarchical scraper for '{self.config.lang}.wikisource.org'")
        logger.info(f"Root category: '{self.config.category}'")
        
        await self.db_manager.initialize()

        page_queue = asyncio.Queue(maxsize=self.config.workers * 2)
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

                for _ in consumer_tasks: await page_queue.put(None)
                await asyncio.gather(*consumer_tasks)
                
                writer_sync_queue.put(None)
                writer_thread.join()
        
        await self.db_manager.close()
        logger.info("Scraping finished.")
        logger.info(f"Total poems processed and saved: {self.processed_counter}")
        logger.info(f"Total pages skipped (non-poem, etc.): {self.skipped_counter}")

    async def _producer(self, client: WikiAPIClient, queue: asyncio.Queue):
        """Finds and enqueues pages for processing."""
        processed_ids = set()
        if self.config.resume:
            processed_ids = await self.db_manager.get_all_processed_ids()
            logger.info(f"Resume mode: Loaded {len(processed_ids)} already processed page IDs.")
        
        logger.info(f"Normalizing root category title '{self.config.category}'...")
        cat_prefix = get_localized_category_prefix(self.config.lang)
        full_cat_title = f"{cat_prefix}:{self.config.category}"
        
        page_info = await client.get_page_info([full_cat_title])

        if not page_info or not page_info.get("pages"):
            logger.critical(f"Root category '{self.config.category}' query failed. Aborting.")
            return

        page_details = page_info["pages"][0]
        
        # --- NOUVELLE LOGIQUE DE RECHERCHE ET DE CORRECTION ---
        if "missing" in page_details:
            logger.warning(f"Category '{full_cat_title}' not found with exact title. Attempting search fallback...")
            # The namespace for "Category" is 14
            corrected_title = await client.search_for_page(full_cat_title, namespace=14)
            
            if not corrected_title:
                 logger.critical(f"Root category '{self.config.category}' does not exist and could not be found via search. Aborting.")
                 return

            logger.info(f"Found likely match via search: '{corrected_title}'. Using this title.")
            page_info = await client.get_page_info([corrected_title])
            page_details = page_info["pages"][0]
            if "missing" in page_details:
                logger.critical(f"Corrected category '{corrected_title}' is also missing. Aborting.")
                return

        canonical_category_title = page_details["title"].split(":", 1)[1]
        if canonical_category_title != self.config.category:
            logger.info(f"Normalized category title from '{self.config.category}' to '{canonical_category_title}'")
        
        logger.info(f"Phase 1: Discovering author subcategories in '{canonical_category_title}'...")
        author_cat_titles = [
            cat['title'].split(":", 1)[1]
            async for cat in client.get_subcategories_generator(canonical_category_title, self.config.lang)
        ]
        
        logger.info(f"Found {len(author_cat_titles)} potential author categories. Checking which are non-empty...")
        
        non_empty_author_cats = []
        if author_cat_titles:
            for i in range(0, len(author_cat_titles), 50):
                 batch_titles = author_cat_titles[i:i+50]
                 info = await client.get_category_info(batch_titles, self.config.lang)
                 for title, cat_info in info.items():
                     if cat_info.get('pages', 0) > 0:
                         non_empty_author_cats.append(title.split(":", 1)[1])
        
        logger.info(f"Found {len(non_empty_author_cats)} non-empty author categories. Discovering pages...")
        
        enqueued_count = 0
        if non_empty_author_cats:
            with tqdm(total=len(non_empty_author_cats), desc="Discovering pages", unit=" author_cat") as pbar:
                for author_cat in non_empty_author_cats:
                    async for page in client.get_pages_in_category_generator(author_cat, self.config.lang):
                        if self.config.limit and enqueued_count >= self.config.limit: break
                        if page['pageid'] not in processed_ids:
                            await queue.put(page)
                            enqueued_count += 1
                    pbar.update(1)
                    if self.config.limit and enqueued_count >= self.config.limit:
                        logger.info(f"Scrape limit ({self.config.limit}) reached.")
                        break
        
        logger.info(f"Producer finished. Enqueued {enqueued_count} new pages for processing.")

    async def _consumer(self, client: WikiAPIClient, page_queue: asyncio.Queue, writer_queue: queue.Queue, pbar: tqdm):
        """Fetches, processes, and queues results for writing."""
        loop = asyncio.get_running_loop()
        while True:
            page_info = await page_queue.get()
            if page_info is None:
                page_queue.task_done()
                break
            
            page_id = page_info['pageid']
            try:
                page_html_task = client.get_rendered_html(page_id)
                page_data_task = client.get_page_data_by_id(page_id)
                page_html, page_data = await asyncio.gather(page_html_task, page_data_task)

                if not page_data or not page_html:
                    raise PageProcessingError("Missing page data or rendered HTML.")

                poem_data = self.processor.process(page_data, page_html, self.config.lang)
                await loop.run_in_executor(None, functools.partial(writer_queue.put, poem_data))

            except PoemParsingError:
                self.skipped_counter += 1
            except Exception as e:
                logger.error(f"Error processing page {page_info.get('title', 'N/A')} (ID: {page_id}): {e}", exc_info=False)
                self.skipped_counter += 1
            finally:
                pbar.update(1)
                page_queue.task_done()

    def _sync_writer(self, writer_queue: queue.Queue):
        """Synchronous task running in a dedicated thread to handle all disk I/O."""
        db_conn, db_cursor = connect_sync_db(self.db_path)
        with gzip.open(self.output_file, "wb") as f_gz:
            while True:
                result = writer_queue.get()
                if result is None:
                    writer_queue.task_done()
                    break
                
                if isinstance(result, PoemSchema):
                    json_line_bytes = (result.model_dump_json() + "\n").encode('utf-8')
                    f_gz.write(json_line_bytes)
                    self.db_manager.add_poem_index_sync(result, db_cursor)
                    self.processed_counter += 1
                
                writer_queue.task_done()
        
        db_conn.commit()
        db_conn.close()