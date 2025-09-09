import logging
import re
from enum import Enum, auto
from typing import Set, Tuple
from urllib.parse import unquote

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

from .parsing import PoemParser

logger = logging.getLogger(__name__)


class PageType(Enum):
    """Énumération granulaire des types de pages pour une classification précise."""
    POEM = auto()
    POETIC_COLLECTION = auto()
    MULTI_VERSION_HUB = auto()
    AUTHOR = auto()
    DISAMBIGUATION = auto()
    OTHER = auto()


def get_localized_prefix(lang: str, prefix_type: str) -> str:
    """Retourne le préfixe localisé."""
    prefixes = {
        "fr": {"category": "Catégorie", "author": "Auteur"},
        "en": {"category": "Category", "author": "Author"},
    }
    return prefixes.get(lang, {}).get(prefix_type, prefix_type.capitalize())


class PageClassifier:
    """Analyse les données d'une page pour la classifier avec une logique experte."""

    def __init__(
        self,
        page_data: dict,
        soup: BeautifulSoup,
        lang: str,
        wikicode: mwparserfromhell.wikicode.Wikicode,
    ):
        self.page_data = page_data
        self.soup = soup
        self.lang = lang
        self.wikicode = wikicode
        self.title = page_data.get("title", "")
        self.ns = page_data.get("ns", -1)
        self.categories = {
            c["title"].split(":")[-1] for c in page_data.get("categories", [])
        }
        self._sub_page_titles_cache: Set[str] | None = None

    def _get_page_signals(self) -> dict:
        """Analyse la page une seule fois pour extraire des signaux forts."""
        sub_page_titles = self.extract_sub_page_titles()
        num_sub_pages = len(sub_page_titles)

        return {
            "is_recueil_cat": "Recueils de poèmes" in self.categories,
            "is_multiversion_cat": "Éditions multiples" in self.categories,
            "has_wikidata_link": bool(
                self.soup.find("a", title=re.compile(r"^d:Q\d+$"))
            ),
            "has_editions_header": bool(
                self.soup.find(["h2", "h3"], string=re.compile(r"Éditions", re.I))
            ),
            "has_ws_summary": bool(self.soup.select_one("div.ws-summary")),
            "has_poem_structure": PoemParser.extract_poem_structure(self.soup)
            is not None,
            "has_toc": bool(self.soup.find("div", id="toc")),
            "has_many_sub_links": num_sub_pages > 3,
            "sub_page_titles": sub_page_titles,
        }

    def classify(self) -> Tuple[PageType, str]:
        """Détermine le type de page et la raison de la classification."""
        if self.ns != 0:
            if self.title.startswith(get_localized_prefix(self.lang, "author") + ":"):
                return (PageType.AUTHOR, "author_namespace")
            return (PageType.OTHER, f"namespace_{self.ns}")

        signals = self._get_page_signals()

        if signals["has_poem_structure"] and not signals["has_many_sub_links"]:
            return (PageType.POEM, "has_poem_structure")

        collection_signals = {
            "is_recueil_cat": signals["is_recueil_cat"],
            "has_ws_summary": signals["has_ws_summary"],
            "has_editions_header": signals["has_editions_header"],
            "has_toc": signals["has_toc"],
            "has_many_sub_links": signals["has_many_sub_links"],
        }
        true_collection_signal = next(
            (k for k, v in collection_signals.items() if v), None
        )

        if true_collection_signal:
            if signals["is_multiversion_cat"]:
                return (PageType.MULTI_VERSION_HUB, "is_multiversion_cat")
            if signals["has_wikidata_link"]:
                return (PageType.MULTI_VERSION_HUB, "has_wikidata_link")
            return (PageType.POETIC_COLLECTION, true_collection_signal)

        if signals["has_poem_structure"]:
            return (PageType.POEM, "has_poem_structure_fallback")

        return (PageType.OTHER, "no_strong_signals")

    def extract_sub_page_titles(self) -> Set[str]:
        """Extrait les titres des pages liées avec une logique de sélection robuste."""
        if self._sub_page_titles_cache is not None:
            return self._sub_page_titles_cache

        content_area = self.soup.select_one("#mw-content-text .mw-parser-output")
        if not content_area:
            self._sub_page_titles_cache = set()
            return self._sub_page_titles_cache

        links = content_area.select("ul a, ol a")
        if not links:
            links = content_area.find_all("a", href=True)

        self._sub_page_titles_cache = self._filter_and_extract_titles_from_links(links)
        return self._sub_page_titles_cache

    def _filter_and_extract_titles_from_links(self, links: list[Tag]) -> Set[str]:
        """Factorisation de l'extraction et du filtrage de liens."""
        titles: Set[str] = set()
        ignored_prefixes = (
            f":{get_localized_prefix(self.lang, 'author')}:",
            f":{get_localized_prefix(self.lang, 'category')}:",
            "Portail:", "Aide:", "Wikisource:", "Fichier:", "Spécial:", "Catégorie:",
        )

        for link in links:
            href = link.get("href", "")
            if (
                not href.startswith("/wiki/")
                or "action=edit" in href
                or "redlink=1" in href
                or (link.get("class") and "new" in link.get("class"))
                or href.startswith(ignored_prefixes)
            ):
                continue
            try:
                raw_title = href.split("/")[-1].split("#")[0]
                title = unquote(raw_title).replace("_", " ")
                if title and title != self.title:
                    titles.add(title)
            except Exception as e:
                logger.warning(f"Impossible d'extraire le titre de l'URL '{href}': {e}")

        logger.debug(f"Extracted {len(titles)} sub-page titles from '{self.title}'.")
        return titles