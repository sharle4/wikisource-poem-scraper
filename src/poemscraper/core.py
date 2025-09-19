import asyncio
import functools
import gzip
import json
import logging
import queue
import threading
from typing import Set, Optional, Dict, Any, List
from datetime import datetime, timezone
import logging.handlers

import re
import mwparserfromhell
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.parse import unquote

from .api_client import WikiAPIClient, get_localized_category_prefix
from .classifier import PageClassifier, PageType
from .database import DatabaseManager, connect_sync_db
from .exceptions import PageProcessingError, PoemParsingError
from .processors import PoemProcessor
from .schemas import PoemSchema, Collection, Section, PoemInfo, CollectionComponent
from .cleaner import process_poem
from .tree_logger import HierarchicalLogger
from .log_manager import LogManager

collection_log = logging.getLogger('collection_processing')
collection_log.propagate = False

logger = logging.getLogger(__name__)

class ScraperOrchestrator:
    """Orchestre le workflow de scraping intelligent et hiérarchique."""

    def __init__(self, config, log_manager: LogManager):
        self.config = config
        self.log_manager = log_manager
        self.api_endpoint = f"https://{config.lang}.wikisource.org/w/api.php"
        self.write_cleaned = str(getattr(config, "cleaned", "true")).lower() == "true"

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.config.output_dir / "poems.jsonl.gz"
        self.cleaned_output_file = self.config.output_dir / "poems.cleaned.jsonl.gz"
        self.db_path = self.config.output_dir / "poems_index.sqlite"
        
        if not collection_log.hasHandlers():
            collection_log_path = self.config.output_dir / "logs" / "collection_processing.log"
            collection_log_path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.handlers.RotatingFileHandler(
                collection_log_path, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
            )
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            collection_log.addHandler(handler)
            collection_log.setLevel(logging.DEBUG)
            collection_log.info("Logger de traitement des recueils initialisé.")


        self.db_manager = DatabaseManager(self.db_path)
        self.processor = PoemProcessor()
        
        self.tree_logger: Optional[HierarchicalLogger] = None
        if self.config.tree_log:
            tree_log_dir = self.config.output_dir / "logs" / "tree-logs"
            self.tree_logger = HierarchicalLogger(tree_log_dir)
            logger.info(f"Hierarchical tree logging enabled. Logs will be saved to {tree_log_dir}")
        if self.write_cleaned:
            logger.info(f"Cleaned output enabled. A second file will be written to: {self.cleaned_output_file}")

        self.processed_ids: Set[int] = set()
        self.scheduled_or_processed_ids: Set[int] = set()
        self.processed_counter = 0
        self.skipped_counter = 0
        self._net_timeout_seconds = 25
        self._net_retries = 2
        self._backoff_base = 0.5
        
    async def run(self):
        """Méthode d'exécution principale avec gestion propre de l'arrêt."""
        logger.info(f"Starting intelligent scraper for '{self.config.lang}.wikisource.org'")
        logger.info(f"Root category: '{self.config.category}'")
        
        await self.db_manager.initialize()

        if self.config.resume:
            self.processed_ids = await self.db_manager.get_all_processed_ids()
            self.scheduled_or_processed_ids.update(self.processed_ids)
            logger.info(f"Resume mode: Loaded {len(self.processed_ids)} already processed page IDs.")

        page_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        writer_sync_queue: queue.Queue[Optional[PoemSchema]] = queue.Queue(maxsize=self.config.workers * 2)

        writer_thread = threading.Thread(
            target=self._sync_writer, args=(writer_sync_queue,), daemon=True
        )
        writer_thread.start()

        try:
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

        finally:
            logger.info("Shutdown sequence initiated. Finalizing operations...")
            
            writer_sync_queue.put(None)
            writer_thread.join()
            logger.info("Writer thread finished processing remaining items.")
            
            if self.tree_logger:
                self.tree_logger.write_log_files()
            
            await self.db_manager.close()
            
            logger.info("Scraping finished.")
            logger.info(f"Total poems processed and saved: {self.processed_counter}")
            logger.info(
                f"Total pages skipped (non-poem, collection, etc.): {self.skipped_counter}"
            )

    async def _queue_page_if_new(self, queue: asyncio.Queue, page_item: Dict[str, Any]):
        """
        CORRECTIF ANTI-BOUCLE: Ajoute une page à la file d'attente uniquement si son ID
        n'a pas déjà été programmé ou traité.
        """
        page_id = page_item['page_info']['pageid']
        if page_id not in self.scheduled_or_processed_ids:
            self.scheduled_or_processed_ids.add(page_id)
            await queue.put(page_item)
            return True
        else:
            logger.debug(f"Skipping enqueue for page ID {page_id} as it's already scheduled or processed.")
            return False

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
            cat['title'].split(":", 1)[1]
            async for cat in client.get_subcategories_generator(full_cat_title.split(":", 1)[1], self.config.lang)
        ]
        logger.info(f"Found {len(author_cat_titles)} potential author categories. Checking which are non-empty...")

        non_empty_author_cats = []
        if author_cat_titles:
            for i in range(0, len(author_cat_titles), 50):
                batch_titles = author_cat_titles[i:i + 50]
                info = await client.get_category_info(batch_titles, self.config.lang)
                for title, cat_info in info.items():
                    if cat_info.get('pages', 0) > 0 or cat_info.get('subcats', 0) > 0:
                        non_empty_author_cats.append(title.split(":", 1)[1])

        logger.info(f"Found {len(non_empty_author_cats)} non-empty author categories. Discovering pages...")
        
        enqueued_count = 0
        if non_empty_author_cats:
            with tqdm(total=len(non_empty_author_cats), desc="Discovering pages", unit=" author_cat") as pbar:
                for author_cat in non_empty_author_cats:
                    author_cat_full_title = f"{cat_prefix}:{author_cat}"
                    async for page in client.get_pages_in_category_generator(author_cat, self.config.lang):
                        if self.config.limit and enqueued_count >= self.config.limit: break
                        
                        page_item = {
                            'page_info': page,
                            'parent_title': author_cat_full_title,
                            'author_cat': author_cat_full_title
                        }
                        if await self._queue_page_if_new(queue, page_item):
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
        """Consomme les pages de la file, les classifie et délègue le traitement."""
        while True:
            try:
                queue_item = await page_queue.get()
                await self._process_single_page(client, writer_queue, pbar, queue_item)
            except asyncio.CancelledError:
                break
            finally:
                page_queue.task_done()

    async def _process_single_page(
        self, client: WikiAPIClient, writer_queue: queue.Queue, pbar: tqdm, queue_item: Dict[str, Any]
    ):
        """Logique de traitement pour une seule page, extraite pour être réutilisable."""
        page_info = queue_item['page_info']
        page_id = page_info["pageid"]
        page_title = page_info.get('title', 'N/A')
        
        collection_log.debug(f"PROCESSING page '{page_title}' (id:{page_id}). Context received: {json.dumps({k: v if not isinstance(v, Collection) else v.model_dump(exclude={'content'}) for k, v in queue_item.items() if k != 'page_info'}, default=str)}")

        parent_title = queue_item['parent_title']
        author_cat = queue_item['author_cat']
        hub_info = queue_item.get("hub_info")
        
        collection_context = queue_item.get("collection_context")
        order_in_collection = queue_item.get("order_in_collection")
        section_title_in_collection = queue_item.get("section_title_in_collection")
        is_first_poem_in_collection = queue_item.get("is_first_poem_in_collection", False)

        try:
            page_data = await self._retry_call(
                lambda: client.get_resolved_page_data(page_id=page_id),
                op_name="get_resolved_page_data",
                ctx=f"page_id={page_id}"
            )
            if not page_data:
                raise PageProcessingError(f"API call for page ID {page_id} returned no data.")

            final_page_id = page_data["pageid"]
            if final_page_id != page_id and final_page_id in self.processed_ids:
                self.processed_ids.add(page_id)
                collection_log.debug(f"SKIPPING page '{page_title}' (id:{page_id}) because its redirect target (id:{final_page_id}) was already processed.")
                self.scheduled_or_processed_ids.add(page_id)
                return

            timestamp = datetime.now(timezone.utc)
            page_title = page_data.get('title', 'N/A')
            page_html = await self._retry_call(
                lambda: client.get_rendered_html(final_page_id),
                op_name="get_rendered_html",
                ctx=f"page_id={final_page_id}"
            )
            if not page_html:
                raise PageProcessingError(f"API did not return HTML for final page ID {final_page_id}.")

            soup = BeautifulSoup(page_html, "lxml")
            wikitext = page_data.get("revisions", [{}])[0].get("content", "")
            wikicode = mwparserfromhell.parse(wikitext)
            page_url = page_data.get("fullurl", f"https://{self.config.lang}.wikisource.org/wiki/{page_title.replace(' ', '_')}")
            
            classifier = PageClassifier(page_data, soup, self.config.lang, wikicode)
            page_type, classification_reason = classifier.classify()
            
            collection_log.info(f"CLASSIFIED page '{page_title}' (id:{final_page_id}) as {page_type.name}. Reason: {classification_reason}")

            if self.tree_logger:
                self.tree_logger.add_node(author_cat, parent_title, page_title, page_type, classification_reason, timestamp)

            if page_type == PageType.POEM:
                try:
                    poem_data = self.processor.process(
                        page_data, soup, self.config.lang, wikicode, hub_info=hub_info,
                        collection_context=collection_context,
                        order_in_collection=order_in_collection,
                        section_title_in_collection=section_title_in_collection,
                        is_first_poem_in_collection=is_first_poem_in_collection
                    )
                    await self._writer_put(writer_queue, poem_data)
                except PoemParsingError as e:
                    logger.warning(f"Page '{page_title}' looked like a poem but failed parsing: {e}")
                    self.skipped_counter += 1
            
            elif page_type == PageType.POETIC_COLLECTION:
                await self._process_collection(client, writer_queue, pbar, {
                    'page_data': page_data, 'soup': soup, 'author_cat': author_cat, 'hub_info': hub_info,
                    'parent_title': parent_title, 'timestamp': timestamp
                })
                self.skipped_counter += 1

            elif page_type == PageType.POETIC_COLLECTION:
                await self._process_collection(client, page_queue, pbar, {
                    'page_data': page_data, 'soup': soup, 'author_cat': author_cat, 'hub_info': hub_info,
                    'parent_title': parent_title, 'timestamp': timestamp
                })
                self.skipped_counter += 1

            elif page_type == PageType.MULTI_VERSION_HUB:
                logger.info(f"Page '{page_title}' is a MULTI_VERSION_HUB ({classification_reason}). Extracting sub-pages.")
                sub_titles = classifier.extract_hub_sub_pages()
                self.log_manager.log_hub(timestamp.isoformat(), page_title, page_url, parent_title, classification_reason, len(sub_titles))
                if sub_titles:
                    new_hub_info = {"title": page_title, "page_id": final_page_id}
                    await self._enqueue_new_titles(client, page_queue, pbar, list(sub_titles), current_parent_title=page_title, author_cat=author_cat, hub_info=new_hub_info)
                self.skipped_counter += 1
            
            else:
                logger.debug(f"Skipping page '{page_title}' classified as {page_type.name} ({classification_reason}).")
                self.log_manager.log_other(timestamp.isoformat(), page_title, page_url, parent_title, classification_reason)
                self.skipped_counter += 1

        except Exception as e:
            logger.error(f"Error processing page {page_title} (ID: {page_id}): {e}", exc_info=False)
            collection_log.error(f"CRITICAL FAILURE processing page '{page_title}' (id:{page_id}): {e}", exc_info=True)
            self.skipped_counter += 1
        finally:
            self.processed_ids.add(page_id)
            if 'final_page_id' in locals():
                self.processed_ids.add(final_page_id)
                self.scheduled_or_processed_ids.add(final_page_id)
            pbar.update(1)

    async def _process_collection(
        self, client: WikiAPIClient, page_queue: asyncio.Queue, pbar: tqdm, context: Dict[str, Any]
    ):
        """
        Orchestre le traitement d'une page de recueil.
        Utilise maintenant _queue_page_if_new pour éviter les boucles.
        """
        page_data = context['page_data']
        page_id = page_data['pageid']
        page_title = page_data['title']
        page_url = page_data.get('fullurl', '')
        author_cat = context['author_cat']
        soup = context['soup']

        collection_log.info(f"--- Starting processing for POETIC_COLLECTION: '{page_title}' (id:{page_id}) ---")
        
        try:
            classifier = PageClassifier(page_data, soup, self.config.lang, mwparserfromhell.parse(""))
            ordered_links = classifier.extract_ordered_collection_links()
            collection_log.info(f"Found {len(ordered_links)} ordered items (poems/sections) in '{page_title}'.")
            
            if not ordered_links:
                logger.warning(f"Collection '{page_title}' did not yield any ordered links.")
                collection_log.warning(f"Extraction from '{page_title}' yielded no links. Aborting processing for this collection.")
                self.log_manager.log_collection(datetime.now(timezone.utc).isoformat(), page_title, page_url, context.get('parent_title'), "no_links_found", 0)
                return

            collection_obj = Collection(
                page_id=page_id,
                title=page_title,
                url=page_url,
                author=author_cat.split(':')[-1]
            )
            
            poem_titles_to_resolve = [title for title, type in ordered_links if type == PageType.POEM]
            collection_log.debug(f"Resolving {len(poem_titles_to_resolve)} poem titles: {poem_titles_to_resolve}")
            resolved_pages = await self._resolve_titles_to_pages(client, poem_titles_to_resolve)
            collection_log.info(f"Successfully resolved {len(resolved_pages)} out of {len(poem_titles_to_resolve)} titles.")
            
            self.log_manager.log_collection(
                datetime.now(timezone.utc).isoformat(), page_title, page_url, 
                context.get('parent_title'), "is_poetic_collection", len(resolved_pages)
            )

            current_section_obj: Optional[Section] = None
            poem_counter_in_collection = 0
            is_first_poem_processed = True

            for title, item_type in ordered_links:
                if item_type == PageType.SECTION_TITLE:
                    collection_log.debug(f"Identified SECTION: '{title}' in '{page_title}'.")
                    current_section_obj = Section(title=title)
                    collection_obj.content.append(current_section_obj)
                
                elif item_type == PageType.POEM:
                    if title in resolved_pages:
                        page_info = resolved_pages[title]
                        poem_info = PoemInfo(title=title, page_id=page_info['pageid'], url=f"https://{self.config.lang}.wikisource.org/?curid={page_info['pageid']}")
                        
                        if current_section_obj:
                            current_section_obj.poems.append(poem_info)
                        else:
                            collection_obj.content.append(poem_info)
                        
                        section_title_for_poem = current_section_obj.title if current_section_obj else None
                        
                        queue_payload = {
                            'page_info': page_info,
                            'parent_title': page_title,
                            'author_cat': author_cat,
                            'hub_info': context.get('hub_info'),
                            'collection_context': collection_obj,
                            'order_in_collection': poem_counter_in_collection,
                            'section_title_in_collection': section_title_for_poem,
                            'is_first_poem_in_collection': is_first_poem_processed
                        }
                        
                        collection_log.debug(f"QUEUING poem '{title}' (order:{poem_counter_in_collection}) from section '{section_title_for_poem}' with full collection context.")
                        
                        if await self._queue_page_if_new(page_queue, queue_payload):
                            poem_counter_in_collection += 1
                            is_first_poem_processed = False
                        else:
                             collection_log.warning(f"Poem '{title}' from collection '{page_title}' was not queued as it's already scheduled/processed.")

                    else:
                        collection_log.warning(f"Could not resolve title '{title}' from collection '{page_title}'. It will be skipped.")
            
            collection_log.info(f"--- Finished processing for POETIC_COLLECTION: '{page_title}'. Processed {poem_counter_in_collection} poems. ---")
        
        except Exception as e:
            collection_log.error(f"CRITICAL FAILURE during _process_collection for '{page_title}': {e}", exc_info=True)


    async def _resolve_titles_to_pages(self, client: WikiAPIClient, titles: List[str]) -> Dict[str, Dict]:
        """Résout une liste de titres en objets page_info."""
        resolved = {}
        if not titles:
            return resolved
            
        for i in range(0, len(titles), 50):
            batch = titles[i:i+50]
            try:
                query_result = await client.get_page_info_and_redirects(batch)
                if not query_result or not query_result.get("pages"):
                    collection_log.warning(f"API call to resolve titles returned no pages for batch: {batch}")
                    continue
                
                redirect_map = {r['from']: r['to'] for r in query_result.get('redirects', [])}
                
                for p_info in query_result.get("pages", []):
                    if p_info.get("missing"):
                        collection_log.warning(f"Title '{p_info['title']}' is marked as missing by API.")
                        continue
                    resolved[p_info['title']] = p_info

                for from_title, to_title in redirect_map.items():
                    if to_title in resolved:
                        resolved[from_title] = resolved[to_title]
            except Exception as e:
                logger.error(f"Failed to resolve batch of titles: {batch}. Error: {e}")
                collection_log.error(f"API error resolving titles batch: {e}", exc_info=True)

        return resolved

    async def _retry_call(self, coro_factory, op_name: str = "operation", ctx: str = ""):
        """Execute an async operation with timeout and limited retries with exponential backoff."""
        attempt = 0
        while True:
            try:
                return await asyncio.wait_for(coro_factory(), timeout=self._net_timeout_seconds)
            except (asyncio.TimeoutError, Exception) as e:
                if attempt >= self._net_retries:
                    logger.error(f"{op_name} failed after {attempt+1} attempts ({ctx}): {e}")
                    return None
                delay = self._backoff_base * (2 ** attempt)
                logger.warning(f"{op_name} error ({ctx}), retry {attempt+1}/{self._net_retries} in {delay:.1f}s: {e}")
                await asyncio.sleep(delay)
                attempt += 1

    async def _writer_put(self, writer_queue: queue.Queue, item: PoemSchema):
        """Non-blocking put with backoff to avoid deadlocks if writer thread stalls temporarily."""
        while True:
            try:
                writer_queue.put_nowait(item)
                return
            except queue.Full:
                await asyncio.sleep(0.05)

    async def _enqueue_new_titles(
        self, client: WikiAPIClient, page_queue: asyncio.Queue, pbar: tqdm, titles: list[str],
        current_parent_title: str, author_cat: str, hub_info: Optional[dict] = None
    ):
        """
        Version modifiée pour mettre en file d'attente les enfants d'un hub.
        Utilise maintenant _queue_page_if_new pour éviter les boucles.
        """
        logger.debug(f"Resolving {len(titles)} titles from hub '{current_parent_title}'.")
        resolved_pages = await self._resolve_titles_to_pages(client, titles)
        
        enqueued_count = 0
        for title, page_info in resolved_pages.items():
            page_item = {
                'page_info': page_info,
                'parent_title': current_parent_title,
                'author_cat': author_cat,
                'hub_info': hub_info
            }
            if await self._queue_page_if_new(page_queue, page_item):
                enqueued_count += 1
        logger.debug(f"Enqueued {enqueued_count} new pages from hub '{current_parent_title}'.")

    def _sync_writer(self, writer_queue: queue.Queue):
        """Tâche synchrone pour gérer toutes les E/S disque."""
        db_conn, db_cursor = connect_sync_db(self.db_path)
        cleaned_fp = None
        seen_cleaned_page_ids: set[int] = set()

        if self.write_cleaned:
            cleaned_fp = gzip.open(self.cleaned_output_file, "at", encoding='utf-8')

        with gzip.open(self.output_file, "at", encoding='utf-8') as f_gz:
            while True:
                result = writer_queue.get()
                if result is None:
                    writer_queue.task_done()
                    break
                try:
                    if isinstance(result, PoemSchema):
                        json_str = result.model_dump_json(exclude_none=True)
                        f_gz.write(json_str + "\n")

                        if cleaned_fp is not None:
                            page_id = result.page_id
                            if page_id not in seen_cleaned_page_ids:
                                seen_cleaned_page_ids.add(page_id)
                                
                                poem_dict = result.model_dump(mode="json", exclude_none=True)
                                cleaned_poem = process_poem(poem_dict)
                                
                                cleaned_fp.write(json.dumps(cleaned_poem, ensure_ascii=False) + "\n")

                        self.db_manager.add_poem_index_sync(result, db_cursor)
                        self.processed_counter += 1

                except Exception as e:
                    logger.error(f"Writer thread failed to persist a record: {e}")
                finally:
                    writer_queue.task_done()
        
        if cleaned_fp is not None:
            try:
                cleaned_fp.close()
            except Exception:
                pass
        
        db_conn.commit()
        db_conn.close()