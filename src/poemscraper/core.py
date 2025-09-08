import asyncio
import logging
from collections import deque

from tqdm import tqdm

from .api_client import WikiAPIClient
from .database import DatabaseManager
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
        
        async with WikiAPIClient(self.api_endpoint, self.config.workers) as client:
            producer_task = asyncio.create_task(
                self._producer(client, page_queue)
            )

            with tqdm(desc="Processing pages", unit=" page", dynamic_ncols=True) as pbar:
                consumer_tasks = [
                    asyncio.create_task(self._consumer(client, page_queue, pbar))
                    for _ in range(self.config.workers)
                ]
                
                await producer_task
                await page_queue.join()

                for task in consumer_tasks:
                    task.cancel()
                
                await asyncio.gather(*consumer_tasks, return_exceptions=True)
        
        await self.db_manager.close()
        logger.info("Scraping finished.")
        logger.info(f"Total poems processed and saved: {self.processed_counter}")
        logger.info(f"Total pages skipped (already processed, non-poem, etc.): {self.skipped_counter}")

    async def _producer(self, client: WikiAPIClient, queue: asyncio.Queue):
        """
        Hierarchical producer:
        1. Finds all non-empty author subcategories.
        2. Finds all pages within each author subcategory.
        3. Enqueues pages for processing.
        """
        processed_ids = set()
        if self.config.resume:
            processed_ids = await self.db_manager.get_all_processed_ids()
            logger.info(f"Resume mode: Loaded {len(processed_ids)} already processed page IDs.")
            
        logger.info(f"Phase 1: Discovering author subcategories in '{self.config.category}'...")
        
        author_cat_titles = [
            cat['title'].replace("Category:", "")
            async for cat in client.get_subcategories_generator(self.config.category)
        ]
        
        logger.info(f"Found {len(author_cat_titles)} potential author categories. Checking which are non-empty...")
        
        non_empty_author_cats = []
        for i in range(0, len(author_cat_titles), 50)
             batch_titles = author_cat_titles[i:i+50]
             info = await client.get_category_info(batch_titles)
             for title, cat_info in info.items():
                 if cat_info.get('pages', 0) > 0:
                     non_empty_author_cats.append(title.replace("Category:", ""))
        
        logger.info(f"Found {len(non_empty_author_cats)} non-empty author categories. Discovering pages...")
        
        enqueued_count = 0
        with tqdm(total=len(non_empty_author_cats), desc="Discovering pages", unit=" author_cat") as pbar:
            for author_cat in non_empty_author_cats:
                async for page in client.get_pages_in_category_generator(author_cat):
                    if self.config.limit and enqueued_count >= self.config.limit:
                        break
                    if page['pageid'] not in processed_ids:
                        await queue.put(page)
                        enqueued_count += 1
                pbar.update(1)
                if self.config.limit and enqueued_count >= self.config.limit:
                    logger.info(f"Scrape limit ({self.config.limit}) reached. Stopping discovery.")
                    break
        
        logger.info(f"Producer finished. Enqueued {enqueued_count} new pages for processing.")


    async def _consumer(self, client: WikiAPIClient, queue: asyncio.Queue, pbar: tqdm):
        """Fetches pages from queue, gets HTML and wikitext, processes, and saves."""
        while True:
            try:
                page_info = await queue.get()
                page_id = page_info['pageid']
                
                try:
                    page_html_task = client.get_rendered_html(page_id)
                    page_data_task = client.get_page_data_by_id(page_id)
                    
                    page_html, page_data = await asyncio.gather(page_html_task, page_data_task)

                    if not page_data or not page_html:
                        raise PageProcessingError("Missing page data or rendered HTML.")

                    poem_data = self.processor.process(page_data, page_html, self.config.lang)
                    await self.save_result(poem_data)
                    self.processed_counter += 1

                except PoemParsingError:
                    self.skipped_counter += 1
                except Exception as e:
                    logger.error(f"Unexpected error processing page {page_info.get('title', 'N/A')} (ID: {page_id}): {e}", exc_info=False)
                    self.skipped_counter += 1
                finally:
                    pbar.update(1)
                    queue.task_done()
            except asyncio.CancelledError:
                break

    async def save_result(self, poem_data: PoemSchema):
        """Saves a validated poem to the NDJSON file and the SQLite index."""
        pass
