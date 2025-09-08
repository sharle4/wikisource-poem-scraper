import asyncio
import logging
from typing import Dict, Any, Optional
import aiohttp
import backoff

logger = logging.getLogger(__name__)

WIKIMEDIA_USER_AGENT = (
    "WikisourcePoemScraper/1.0 (https://github.com/sharle4/wikisource-poem-scraper; "
    "charleskayssieh@gmail.com) aiohttp/" + aiohttp.__version__
)

class WikiAPIClient:
    """
    Client API MediaWiki asynchrone, respectueux des règles, avec gestion
    des sémaphores, des retries et du User-Agent.
    """
    def __init__(self, api_endpoint: str, max_concurrent_requests: int = 5):
        self.api_endpoint = api_endpoint
        self.headers = {"User-Agent": WIKIMEDIA_USER_AGENT}
        
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Initialise la session aiohttp."""
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ferme la session aiohttp."""
        if self.session:
            await self.session.close()

    def _should_retry(e: Exception) -> bool:
        """Détermine si une exception doit déclencher un retry (ex: erreurs réseau, 503)."""
        if isinstance(e, aiohttp.ClientResponseError):
            return e.status in [429, 500, 502, 503, 504]
        if isinstance(e, (aiohttp.ClientConnectionError, asyncio.TimeoutError)):
            return True
        return False

    @backoff.on_exception(backoff.expo,
                          (aiohttp.ClientError, asyncio.TimeoutError),
                          max_tries=5,
                          giveup=_should_retry,
                          logger=logger)
    async def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Méthode de requête interne, gérée par le sémaphore et backoff."""
        if not self.session:
            raise RuntimeError("ClientSession non initialisée. Utiliser 'async with WikiAPIClient(...)'.")

        params.update({"format": "json", "formatversion": "2"})

        async with self.semaphore:
            logger.debug(f"Début requête API : {params.get('action')}, page : {params.get('titles') or params.get('pageids')}")
            try:
                async with self.session.get(self.api_endpoint, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    if "error" in data:
                        logger.error(f"Erreur API MediaWiki : {data['error']}")
                    
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Erreur réseau/serveur pendant l'appel API ({e}). Tentative de retry...")
                raise

    async def get_page_data_by_id(self, page_id: int) -> Optional[Dict[str, Any]]:
        """
        Récupère toutes les données nécessaires pour une page (wikitext, révision, catégories)
        """
        params = {
            "action": "query",
            "pageids": page_id,
            "prop": "info|revisions|categories|templates",
            "rvprop": "ids|timestamp|content",
            "cllimit": "max",
            "tllimit": "max",
        }
        data = await self._make_request(params)
        
        if not data.get("query", {}).get("pages"):
             logger.warning(f"Page non trouvée ou réponse invalide pour pageid: {page_id}")
             return None
             
        page_data = data["query"]["pages"][0]
        
        if page_data.get("missing") or "invalid" in page_data:
            logger.info(f"Page marquée comme 'missing' ou 'invalid' : {page_id}")
            return None
            
        return page_data

    async def get_rendered_html(self, page_id: int) -> Optional[str]:
        """Récupère le HTML rendu (optionnel)."""
        params = {
            "action": "parse",
            "pageid": page_id,
            "prop": "text",
        }
        data = await self._make_request(params)
        if data and "parse" in data and "text" in data["parse"]:
            return data["parse"]["text"]
        return None