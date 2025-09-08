import asyncio
import logging
from pathlib import Path
import gzip
import json

import aiofiles
from tqdm import tqdm

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
        logger.info(f"Configuration: {vars(self.config)}")
        
        await self.db_manager.initialize()

        page_queue = asyncio.Queue(maxsize=self.config.workers * 2)
        
        self.progress_bar = tqdm(desc="Processing pages", unit=" poem", total=0, dynamic_ncols=True)

        async with WikiAPIClient(self.api_endpoint, self.config.workers) as client:
            producer_task = asyncio.create_task(
                self._producer(client, page_queue)
            )

            consumer_tasks = [
                asyncio.create_task(self._consumer(client, page_queue))
                for _ in range(self.config.workers)
            ]
            
            await producer_task

            await page_queue.join()

            for task in consumer_tasks:
                task.cancel()
            
            await asyncio.gather(*consumer_tasks, return_exceptions=True)
        
        self.progress_bar.close()
        await self.db_manager.close()
        logger.info("Scraping finished.")
        logger.info(f"Total poems processed and saved: {self.processed_counter}")
        logger.info(f"Total pages skipped (already processed or non-poem): {self.skipped_counter}")

    async def _producer(self, client: WikiAPIClient, queue: asyncio.Queue):
        """Crawls the category and puts page_ids into the queue."""
        processed_ids = set()
        if self.config.resume:
            processed_ids = await self.db_manager.get_all_processed_ids()
            logger.info(f"Resume mode: Loaded {len(processed_ids)} already processed page IDs.")
            
        logger.info(f"Starting crawl of category '{self.config.category}'...")
        
        page_generator = client.get_pages_in_category_generator(self.config.category)
        
        pages_found = 0
        async for page in page_generator:
            pages_found += 1

            if self.config.limit and (self.processed_counter + self.skipped_counter) >= self.config.limit:
                logger.info(f"Reached scrape limit of {self.config.limit}. Stopping producer.")
                break

            if self.config.resume and page['pageid'] in processed_ids:
                self.skipped_counter += 1
                continue
            
            if self.config.dry_run:
                logger.info(f"[DRY-RUN] Would process page: {page['title']} (ID: {page['pageid']})")
                self.processed_counter += 1
                continue

            await queue.put(page)
            self.progress_bar.total += 1
        
        logger.info(f"Producer has finished crawling. Found {pages_found} potential pages.")

    async def _consumer(self, client: WikiAPIClient, queue: asyncio.Queue):
        """Fetches pages from the queue, processes them, and updates progress."""
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
                    logger.warning(f"Skipping page {page_info.get('title', 'N/A')}: {e}")
                    self.skipped_counter += 1
                except Exception as e:
                    logger.error(f"Unexpected error processing page {page_info.get('title', 'N/A')}: {e}", exc_info=True)
                    self.skipped_counter += 1
                finally:
                    self.progress_bar.update(1)
                    queue.task_done()
            except asyncio.CancelledError:
                break

    async def process_page(self, client: WikiAPIClient, page_info: dict) -> PoemSchema | None:
        """Full processing pipeline for a single page."""
        page_id = page_info['pageid']
        title = page_info['title']
        
        page_data = await client.get_page_data_by_id(page_id)
        if not page_data or 'revisions' not in page_data:
            raise PageProcessingError(f"No content found for page '{title}' (ID: {page_id})")
        
        try:
            return self.processor.process(page_data=page_data, lang=self.config.lang)
        except PoemParsingError as e:
            logger.debug(f"Could not parse poem structure for '{title}': {e}")
            return None

    async def save_result(self, poem_data: PoemSchema):
        """Saves a validated poem to the NDJSON file and the SQLite index."""
        async with aiofiles.open(self.output_file, "ab") as f:
            json_str = poem_data.model_dump_json() + "\n"
            await f.write(json_str.encode("utf-8"))

        await self.db_manager.insert_poem(poem_data)