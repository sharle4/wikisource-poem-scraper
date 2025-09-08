import asyncio
import logging
from typing import Dict, Any, Optional, AsyncGenerator

import aiohttp
import backoff

logger = logging.getLogger(__name__)

WIKIMEDIA_USER_AGENT = (
    "WikisourcePoemScraper/2.2 (https://votre_projet; email@example.com) "
    "aiohttp/" + aiohttp.__version__
)

def get_localized_category_prefix(lang: str) -> str:
    """
    Returns the localized 'Category:' prefix for a given language.
    """
    prefixes = {
        "fr": "Catégorie", "en": "Category", "de": "Kategorie",
        "es": "Categoría", "it": "Categoria",
    }
    return prefixes.get(lang, "Category")

class WikiAPIClient:
    """
    Client API MediaWiki asynchrone, respectueux des règles.
    """
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
    def _should_retry(e: Exception) -> bool:
        if isinstance(e, aiohttp.ClientResponseError):
            return e.status in [429, 500, 502, 503, 504]
        return isinstance(e, (aiohttp.ClientConnectionError, asyncio.TimeoutError))

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError),
                          max_tries=5, giveup=lambda e: not WikiAPIClient._should_retry(e),
                          logger=logger)
    async def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.session: raise RuntimeError("ClientSession not initialized.")
        params.update({"format": "json", "formatversion": "2"})
        async with self.semaphore:
            logger.debug(f"API Request: {params}")
            async with self.session.get(self.api_endpoint, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                if "error" in data: logger.error(f"MediaWiki API Error: {data['error']}")
                return data

    async def get_page_info(self, page_titles: list[str]) -> Optional[dict]:
        """Gets basic info for pages, resolving redirects."""
        params = {"action": "query", "prop": "info", "titles": "|".join(page_titles), "redirects": 1}
        data = await self._make_request(params)
        return data.get("query")

    async def search_for_page(self, search_term: str, namespace: int) -> Optional[str]:
        """
        Uses opensearch to find the most likely page title for a search term in a given namespace.
        Returns the canonical title of the best match, or None.
        """
        params = {
            "action": "opensearch", "search": search_term, "limit": 1, "namespace": namespace
        }
        data = await self._make_request(params)
        # opensearch returns [searchTerm, [results], [descriptions], [urls]]
        if isinstance(data, list) and len(data) == 4 and data[1]:
            return data[1][0]
        return None

    async def get_subcategories_generator(self, category_title: str, lang: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Lists all subcategories of a given category."""
        cmcontinue = None
        cat_prefix = get_localized_category_prefix(lang)
        while True:
            params = {
                "action": "query", "list": "categorymembers",
                "cmtitle": f"{cat_prefix}:{category_title}", "cmtype": "subcat",
                "cmlimit": "max", "cmprop": "title|ids",
            }
            if cmcontinue: params["cmcontinue"] = cmcontinue
            data = await self._make_request(params)
            for member in data.get("query", {}).get("categorymembers", []): yield member
            if "continue" in data: cmcontinue = data["continue"]["cmcontinue"]
            else: break

    async def get_pages_in_category_generator(self, category_title: str, lang: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Lists all pages in a given category."""
        cmcontinue = None
        cat_prefix = get_localized_category_prefix(lang)
        while True:
            params = {
                "action": "query", "list": "categorymembers",
                "cmtitle": f"{cat_prefix}:{category_title}", "cmtype": "page",
                "cmlimit": "max", "cmprop": "title|ids",
            }
            if cmcontinue: params["cmcontinue"] = cmcontinue
            data = await self._make_request(params)
            for member in data.get("query", {}).get("categorymembers", []): yield member
            if "continue" in data: cmcontinue = data["continue"]["cmcontinue"]
            else: break

    async def get_rendered_html(self, page_id: int) -> Optional[str]:
        """Fetches the rendered HTML of a page."""
        params = {"action": "parse", "pageid": page_id, "prop": "text", "disabletoc": True, "disableeditsection": True}
        data = await self._make_request(params)
        return data.get("parse", {}).get("text")

    async def get_page_data_by_id(self, page_id: int) -> Optional[Dict[str, Any]]:
        """Fetches raw wikitext and metadata for a page."""
        params = {"action": "query", "pageids": page_id, "prop": "info|revisions", "rvprop": "ids|timestamp|content", "inprop": "url"}
        data = await self._make_request(params)
        if not data.get("query", {}).get("pages"): return None
        page_data = data["query"]["pages"][0]
        if page_data.get("missing") or "invalid" in page_data: return None
        return page_data

    async def get_category_info(self, category_titles: list[str], lang: str) -> dict:
        """Checks if a list of categories are empty."""
        cat_prefix = get_localized_category_prefix(lang)
        params = {"action": "query", "prop": "categoryinfo", "titles": "|".join([f"{cat_prefix}:{title}" for title in category_titles])}
        data = await self._make_request(params)
        return {p['title']: p.get('categoryinfo', {}) for p in data.get("query", {}).get("pages", [])}