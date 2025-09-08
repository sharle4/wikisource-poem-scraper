import asyncio
import logging
from typing import Dict, Any, Optional, AsyncGenerator

import aiohttp
import backoff

logger = logging.getLogger(__name__)

WIKIMEDIA_USER_AGENT = (
    "WikisourcePoemScraper/2.0 (https://github.com/sharle4/wikisource-poem-scraper; charleskayssieh@gmail.com) "
    "aiohttp/" + aiohttp.__version__
)

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
        if not self.session:
            raise RuntimeError("ClientSession not initialized.")

        params.update({"format": "json", "formatversion": "2"})
        async with self.semaphore:
            logger.debug(f"API Request: {params}")
            async with self.session.get(self.api_endpoint, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                if "error" in data:
                    logger.error(f"MediaWiki API Error: {data['error']}")
                return data

    async def get_subcategories_generator(self, category_title: str) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Générateur asynchrone pour lister toutes les sous-catégories d'une catégorie donnée.
        Filtre les catégories vides en se basant sur les informations de l'API.
        """
        cmcontinue = None
        while True:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": f"Category:{category_title}",
                "cmtype": "subcat",
                "cmlimit": "max",
                "cmprop": "title|ids",
                "cmcontinue": cmcontinue,
            }
            data = await self._make_request(params)
            
            members = data.get("query", {}).get("categorymembers", [])
            for member in members:
                yield member

            if "continue" in data:
                cmcontinue = data["continue"]["cmcontinue"]
            else:
                break

    async def get_pages_in_category_generator(self, category_title: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Générateur asynchrone pour lister toutes les pages (pas les sous-catégories) d'une catégorie."""
        cmcontinue = None
        while True:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": f"Category:{category_title}",
                "cmtype": "page",
                "cmlimit": "max",
                "cmprop": "title|ids",
                "cmcontinue": cmcontinue,
            }
            data = await self._make_request(params)
            members = data.get("query", {}).get("categorymembers", [])
            for member in members:
                yield member

            if "continue" in data:
                cmcontinue = data["continue"]["cmcontinue"]
            else:
                break

    async def get_rendered_html(self, page_id: int) -> Optional[str]:
        """Récupère le HTML rendu d'une page pour l'extraction de métadonnées structurées."""
        params = {
            "action": "parse",
            "pageid": page_id,
            "prop": "text",
            "disabletoc": True,
            "disableeditsection": True,
        }
        data = await self._make_request(params)
        return data.get("parse", {}).get("text")

    async def get_page_data_by_id(self, page_id: int) -> Optional[Dict[str, Any]]:
        """Récupère les données brutes (wikitext, révision, etc.) d'une page."""
        params = {
            "action": "query",
            "pageids": page_id,
            "prop": "info|revisions",
            "rvprop": "ids|timestamp|content",
            "inprop": "url",
        }
        data = await self._make_request(params)
        if not data.get("query", {}).get("pages"):
            return None
        page_data = data["query"]["pages"][0]
        if page_data.get("missing") or "invalid" in page_data:
            return None
        return page_data

    async def get_category_info(self, category_titles: list[str]) -> dict:
        """Vérifie si une liste de catégories sont vides."""
        params = {
            "action": "query",
            "prop": "categoryinfo",
            "titles": "|".join([f"Category:{title}" for title in category_titles]),
        }
        data = await self._make_request(params)
        return {p['title']: p.get('categoryinfo', {}) for p in data.get("query", {}).get("pages", [])}