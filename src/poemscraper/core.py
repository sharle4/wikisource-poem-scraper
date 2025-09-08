import asyncio
import logging
from pathlib import Path
import gzip
import json

import aiofiles
from tqdm.asyncio import tqdm

from .api_client import WikiAPIClient
from .database import DatabaseManager
from .processors import PoemProcessor
from .exceptions import PoemParsingError, PageProcessingError
from .schemas import PoemSchema

logger = logging.getLogger(__name__)

class ScraperOrchestrator:
    """Manages the entire scraping workflow."""
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
        logger.info(f"Starting scraper for '{self.config.lang}.wikisource.org'")
        logger.info(f"Configuration: {self.config}")
        
        await self.db_manager.initialize()

        page_queue = asyncio.Queue(maxsize=self.config.workers * 2)

        async with WikiAPIClient(self.api_endpoint, self.config.workers) as client:
            producer_task = asyncio.create_task(
                self._producer(client, page_queue)
            )

            consumer_tasks = [
                asyncio.create_task(self._consumer(client, page_queue))
                for _ in range(self.config.workers)
            ]
            
            await producer_task
            logger.info("Producer finished: All pages have been enqueued.")

            await page_queue.join()
            logger.info("Consumer queue is empty. All tasks are completing.")

            for task in consumer_tasks:
                task.cancel()
            
            await asyncio.gather(*consumer_tasks, return_exceptions=True)

        await self.db_manager.close()
        logger.info("Scraping finished.")
        logger.info(f"Total poems processed and saved: {self.processed_counter}")
        logger.info(f"Total pages skipped (already processed or non-poem): {self.skipped_counter}")

    async def _producer(self, client: WikiAPIClient, queue: asyncio.Queue):
        """
        Crawls the specified category recursively and adds page_ids to the queue.
        """
        processed_ids = set()
        if self.config.resume:
            processed_ids = await self.db_manager.get_all_processed_ids()
            logger.info(f"Resume mode: Loaded {len(processed_ids)} already processed page IDs.")
            
        logger.info(f"Starting crawl of category '{self.config.category}'...")
        
        page_generator = client.get_pages_in_category_generator(self.config.category)
        
        pbar = tqdm(desc="Crawling category pages", unit=" pages")
        
        async for page in page_generator:
            pbar.update(1)
            page_id = page['pageid']

            if self.config.limit and (self.processed_counter + self.skipped_counter) >= self.config.limit:
                logger.info(f"Reached scrape limit of {self.config.limit}. Stopping producer.")
                break

            if self.config.resume and page_id in processed_ids:
                self.skipped_counter += 1
                continue
            
            if self.config.dry_run:
                logger.info(f"[DRY-RUN] Would process page: {page['title']} (ID: {page_id})")
                self.processed_counter += 1
                continue

            await queue.put(page)
        
        pbar.close()
        logger.info(f"Producer has finished crawling. Total potential pages found: {pbar.n}")

    async def _consumer(self, client: WikiAPIClient, queue: asyncio.Queue):
        """
        Continuously fetches pages from the queue and processes them.
        """
        while True:
            try:
                page_info = await queue.get()
                
                try:
                    poem_data = await self.process_page(client, page_info)
                    if poem_data:
                        await self.save_result(poem_data)
                        self.processed_counter += 1
                    else:
                        self.skipped_counter += 1
                except PageProcessingError as e:
                    logger.warning(f"Skipping page {page_info.get('title')}: {e}")
                    self.skipped_counter += 1
                except Exception as e:
                    logger.error(f"Unexpected error processing page {page_info.get('title')}: {e}", exc_info=True)
                    self.skipped_counter += 1
                finally:
                    queue.task_done()
            except asyncio.CancelledError:
                break

    async def process_page(self, client: WikiAPIClient, page_info: dict) -> PoemSchema | None:
        """
        Full processing pipeline for a single page.
        """
        page_id = page_info['pageid']
        title = page_info['title']
        
        page_data = await client.get_page_data_by_id(page_id)
        if not page_data or 'revisions' not in page_data:
            raise PageProcessingError(f"No content found for page '{title}' (ID: {page_id})")
        
        try:
            poem_schema = self.processor.process(
                page_data=page_data,
                lang=self.config.lang
            )
            return poem_schema
        except PoemParsingError as e:
            logger.debug(f"Could not parse poem structure for '{title}': {e}")
            return None

    async def save_result(self, poem_data: PoemSchema):
        """Saves a validated poem to the NDJSON file and the SQLite index."""
        async with aiofiles.open(self.output_file, "ab") as f:
            json_str = poem_data.model_dump_json() + "\n"
            await f.write(json_str.encode("utf-8"))

        await self.db_manager.insert_poem(poem_data)