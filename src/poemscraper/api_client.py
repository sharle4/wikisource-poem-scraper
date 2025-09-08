import asyncio
import logging
from typing import Dict, Any, Optional, AsyncGenerator

import aiohttp
import backoff

logger = logging.getLogger(__name__)

WIKIMEDIA_USER_AGENT = (
    "WikisourcePoemScraper/1.0 (https://github.com/sharle4/wikisource-poem-scraper; "
    "charleskayssieh@gmail.com) aiohttp/" + aiohttp.__version__
)

class WikiAPIClient:
    def __init__(self, api_endpoint: str, max_concurrent_requests: int = 5):
        self.api_endpoint = api_endpoint
        self.headers = {"User-Agent": WIKIMEDIA_USER_AGENT}
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @staticmethod
    def _is_retryable(e: Exception) -> bool:
        if isinstance(e, aiohttp.ClientResponseError):
            return e.status in [429, 500, 502, 503, 504]
        return isinstance(e, (aiohttp.ClientConnectionError, asyncio.TimeoutError))

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=5,
        giveup=lambda e: not WikiAPIClient._is_retryable(e),
        logger=logger,
    )
    async def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.session:
            raise RuntimeError("ClientSession not initialized.")
        
        params.update({"format": "json", "formatversion": "2"})
        
        async with self.semaphore:
            logger.debug(f"Requesting API with params: {params}")
            try:
                async with self.session.get(self.api_endpoint, params=params, timeout=30) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if "error" in data:
                        logger.error(f"MediaWiki API error: {data['error']}")
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Network/Server error during API call ({type(e).__name__}). Retrying...")
                raise

    async def get_page_data_by_id(self, page_id: int) -> Optional[Dict[str, Any]]:
        params = {
            "action": "query",
            "pageids": page_id,
            "prop": "info|revisions|categories",
            "inprop": "url",
            "rvprop": "ids|timestamp|content",
            "cllimit": "max",
        }
        data = await self._make_request(params)
        page_data = data.get("query", {}).get("pages", [{}])[0]
        if page_data.get("missing") or "invalid" in page_data:
            logger.info(f"Page ID {page_id} is missing or invalid.")
            return None
        return page_data

    async def get_pages_in_category_generator(self, category_name: str) -> AsyncGenerator[Dict, None]:
        """
        **Scalable Method**: Yields pages from a category and its subcategories.
        
        This uses an API generator (`categorymembers`) to fetch pages in chunks,
        avoiding loading thousands of page titles into memory at once.
        """
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category_name}",
            "cmlimit": "500",
            "cmprop": "ids|title|type",
        }
        cmcontinue = None
        while True:
            if cmcontinue:
                params["cmcontinue"] = cmcontinue
            
            data = await self._make_request(params)
            
            members = data.get("query", {}).get("categorymembers", [])
            for member in members:
                if member['type'] == 'subcat':
                    async for sub_page in self.get_pages_in_category_generator(member['title'].split(':', 1)[1]):
                        yield sub_page
                elif member['type'] == 'page':
                    yield member

            if "continue" in data:
                cmcontinue = data["continue"]["cmcontinue"]
            else:
                break