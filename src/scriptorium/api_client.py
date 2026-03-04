import asyncio
import logging
import time
import collections
from typing import Dict, Any, Optional, AsyncGenerator

import aiohttp
import backoff

logger = logging.getLogger(__name__)

WIKIMEDIA_USER_AGENT = (
    "Scriptorium/5.1.0 (https://github.com/sharle4/scriptorium; charleskayssieh@gmail.com) "
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
    Asynchronous MediaWiki API client, compliant with usage policies.
    """
    def __init__(self, api_endpoint: str, max_concurrent_requests: int = 3, bot_username: Optional[str] = None, bot_password: Optional[str] = None):
        self.api_endpoint = api_endpoint
        self.headers = {"User-Agent": WIKIMEDIA_USER_AGENT}
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.session: Optional[aiohttp.ClientSession] = None
        self.bot_username = bot_username
        self.bot_password = bot_password
        self._request_lock = asyncio.Lock()
        self._request_times = collections.deque(maxlen=10)

    async def __aenter__(self):
        cookie_jar = aiohttp.CookieJar(unsafe=True)
        self.session = aiohttp.ClientSession(headers=self.headers, cookie_jar=cookie_jar)

        if self.bot_username and self.bot_password:
            await self._login()

        return self

    async def _login(self):
        """Authenticates with the MediaWiki API using a bot password."""
        logger.info(f"Attempting bot login for user: {self.bot_username}")

        # 1. Retrieve a login token
        token_params = {
            "action": "query",
            "meta": "tokens",
            "type": "login",
            "format": "json"
        }

        async with self.session.get(self.api_endpoint, params=token_params) as resp:
            resp.raise_for_status()
            data = await resp.json()
            login_token = data.get("query", {}).get("tokens", {}).get("logintoken")

        if not login_token:
            logger.error("Failed to retrieve login token.")
            return

        # 2. Authenticate with the token
        login_params = {
            "action": "login",
            "lgname": self.bot_username,
            "lgpassword": self.bot_password,
            "lgtoken": login_token,
            "format": "json"
        }

        async with self.session.post(self.api_endpoint, data=login_params) as resp:
            resp.raise_for_status()
            data = await resp.json()

            login_result = data.get("login", {}).get("result")
            if login_result == "Success":
                logger.info("Bot login successful.")
            else:
                logger.error(f"Bot login failed. Result: {data.get('login', {})}")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @staticmethod
    def _should_retry(e: Exception) -> bool:
        if isinstance(e, aiohttp.ClientResponseError):
            return e.status in [500, 502, 503, 504]
        return isinstance(e, (aiohttp.ClientConnectionError, asyncio.TimeoutError))

    async def _throttle(self):
        async with self._request_lock:
            now = time.time()
            if len(self._request_times) == 10:
                elapsed_since_10th = now - self._request_times[0]
                if elapsed_since_10th < 1.0:
                    sleep_time = 1.0 - elapsed_since_10th
                    logger.debug(f"Rate limiting requests to 10 RPS, sleeping for {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                    now = time.time()
            self._request_times.append(now)

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError),
                          max_tries=5, giveup=lambda e: not WikiAPIClient._should_retry(e),
                          logger=logger)
    async def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.session: raise RuntimeError("ClientSession not initialized.")

        sanitized_params = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, bool):
                sanitized_params[key] = str(value).lower()
            else:
                sanitized_params[key] = value

        sanitized_params.update({"format": "json", "formatversion": "2"})

        async with self.semaphore:
            await self._throttle()
            logger.debug(f"API Request: {sanitized_params}")
            while True:
                start_time = time.time()
                async with self.session.get(self.api_endpoint, params=sanitized_params) as response:
                    elapsed = time.time() - start_time
                    if response.status == 429:
                        cookies = self.session.cookie_jar.filter_cookies(self.api_endpoint)
                        cookie_names = list(cookies.keys())
                        retry_after = response.headers.get("Retry-After")

                        logger.warning(f"Rate limited (429). Found cookies: {cookie_names}")

                        if retry_after:
                            try:
                                wait_time = int(retry_after)
                                logger.warning(f"Respecting Retry-After: waiting {wait_time}s")
                            except ValueError:
                                wait_time = 5
                                logger.warning(f"Invalid Retry-After '{retry_after}', waiting {wait_time}s")
                        else:
                            wait_time = 5
                            logger.warning(f"No Retry-After header provided, waiting {wait_time}s")

                        await asyncio.sleep(wait_time)
                        continue

                    if response.status == 403:
                        body = await response.text()
                        logger.error(f"403 Forbidden. Response body:\n{body}")

                    response.raise_for_status()
                    data = await response.json()

                    if elapsed > 1.0:
                        logger.warning(f"Action API request took {elapsed:.2f}s (>1s limit). Waiting 5 seconds to respect expensive endpoint policy.")
                        await asyncio.sleep(5)

                    if "error" in data: logger.error(f"MediaWiki API Error: {data['error']}")
                    return data

    async def get_page_info_and_redirects(self, page_titles: list[str]) -> Optional[dict]:
        """Gets basic info for pages, resolving redirects."""
        params = {"action": "query", "prop": "info", "titles": "|".join(page_titles), "redirects": 1}
        data = await self._make_request(params)
        return data.get("query")

    async def search_for_page(self, search_term: str, namespace: int) -> Optional[str]:
        """
        Uses opensearch to find the most likely page title for a search term.
        """
        params = {"action": "opensearch", "search": search_term, "limit": 1, "namespace": namespace}
        data = await self._make_request(params)
        if isinstance(data, list) and len(data) >= 2 and data[1]:
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
                "cmlimit": "max", "cmprop": "title|ids", "cmcontinue": cmcontinue,
            }
            data = await self._make_request(params)
            for member in data.get("query", {}).get("categorymembers", []):
                yield member
            if "continue" in data:
                cmcontinue = data["continue"]["cmcontinue"]
            else:
                break

    async def get_pages_in_category_generator(self, category_title: str, lang: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Lists all pages in a given category."""
        cmcontinue = None
        cat_prefix = get_localized_category_prefix(lang)
        while True:
            params = {
                "action": "query", "list": "categorymembers",
                "cmtitle": f"{cat_prefix}:{category_title}", "cmtype": "page",
                "cmlimit": "max", "cmprop": "title|ids", "cmcontinue": cmcontinue,
            }
            data = await self._make_request(params)
            for member in data.get("query", {}).get("categorymembers", []):
                yield member
            if "continue" in data:
                cmcontinue = data["continue"]["cmcontinue"]
            else:
                break

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError),
                          max_tries=5, giveup=lambda e: not WikiAPIClient._should_retry(e),
                          logger=logger)
    async def get_rendered_html(self, page_title: str) -> Optional[str]:
        """Fetches the rendered HTML of a page using the standard website URLs."""
        if not self.session: raise RuntimeError("ClientSession not initialized.")
        base_url = self.api_endpoint.replace("/w/api.php", "")
        import urllib.parse
        encoded_title = urllib.parse.quote(page_title.replace(" ", "_"))
        url = f"{base_url}/wiki/{encoded_title}"

        async with self.semaphore:
            await self._throttle()
            while True:
                start_time = time.time()
                async with self.session.get(url, headers={"User-Agent": WIKIMEDIA_USER_AGENT + " (Live Site HTML Fetch)"}) as response:
                    elapsed = time.time() - start_time
                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 5
                        await asyncio.sleep(wait_time)
                        continue
                    if response.status == 404:
                        logger.warning(f"Website returned 404 for '{page_title}'.")
                        return None
                    response.raise_for_status()
                    html_content = await response.text()

                    if elapsed > 1.0:
                        logger.warning(f"Live website request took {elapsed:.2f}s (>1s limit). Waiting 5 seconds...")
                        await asyncio.sleep(5)

                    return html_content

    async def get_resolved_page_data(self, page_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves the final page data from its ID, automatically resolving redirects.
        This is the preferred method for fetching page content as it uses a single efficient API call.
        Returns the final page data object, or None if the page is missing or invalid.
        """
        params = {
            "action": "query",
            "prop": "info|revisions|categories",
            "rvprop": "ids|timestamp|content",
            "inprop": "url",
            "cllimit": "max",
            "redirects": 1,
            "pageids": page_id
        }
        try:
            data = await self._make_request(params)

            if not data.get("query", {}).get("pages"):
                return None

            page_data = data["query"]["pages"][0]
            if page_data.get("missing") or "invalid" in page_data:
                return None

            return page_data
        except Exception as e:
            logger.error(f"Failed to resolve page data for id={page_id}: {e}")
            return None

    async def get_page_data_by_id(self, page_id: int) -> Optional[Dict[str, Any]]:
        """Fetches raw wikitext, categories, and metadata for a page."""
        params = {
            "action": "query",
            "pageids": page_id,
            "prop": "info|revisions|categories",
            "rvprop": "ids|timestamp|content",
            "inprop": "url",
            "cllimit": "max"
        }
        data = await self._make_request(params)
        if not data.get("query", {}).get("pages"): return None
        page_data = data["query"]["pages"][0]
        if page_data.get("missing") or "invalid" in page_data: return None
        return page_data

    async def get_category_info(self, category_titles: list[str], lang: str) -> dict:
        """Checks if a list of categories are empty."""
        cat_prefix = get_localized_category_prefix(lang)
        params = {
            "action": "query",
            "prop": "categoryinfo",
            "titles": "|".join([f"{cat_prefix}:{title}" for title in category_titles])
        }
        data = await self._make_request(params)
        results = {}
        pages = data.get("query", {}).get("pages", [])
        for p in pages:
            if "missing" not in p:
                results[p['title']] = p.get('categoryinfo', {})
        return results
