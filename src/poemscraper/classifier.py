import logging
import re
from enum import Enum, auto
from typing import Set
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

    def _get_page_signals(self) -> dict:
        """Analyse la page une seule fois pour extraire des signaux forts."""
        sub_page_titles = self.extract_sub_page_titles()
        num_sub_pages = len(sub_page_titles)

        return {
            "is_recueil_cat": "Recueils de poèmes" in self.categories,
            "is_multiversion_cat": "Éditions multiples" in self.categories,
            "has_wikidata_link": bool(self.soup.find("a", title=re.compile(r"^d:Q\d+$"))),
            "has_editions_header": bool(self.soup.find(["h2", "h3"], string=re.compile(r"Éditions", re.I))),
            "has_ws_summary": bool(self.soup.select_one("div.ws-summary")),
            "has_poem_structure": PoemParser.extract_poem_structure(self.soup) is not None,
            "has_toc": bool(self.soup.find("div", id="toc")),
            "has_many_sub_links": num_sub_pages > 3,
            "sub_page_titles": sub_page_titles,
        }

    def classify(self) -> PageType:
        """Détermine le type de page en appliquant une logique de priorisation stricte."""
        if self.ns != 0:
            return PageType.AUTHOR if self.title.startswith(get_localized_prefix(self.lang, "author") + ":") else PageType.OTHER
        
        signals = self._get_page_signals()

        if signals["has_poem_structure"]:
            if not signals["has_many_sub_links"]:
                 return PageType.POEM

        if signals["is_recueil_cat"] or signals["has_ws_summary"] or \
           signals["has_editions_header"] or signals["has_toc"] or signals["has_many_sub_links"]:
            
            if signals["is_multiversion_cat"] or signals["has_wikidata_link"]:
                return PageType.MULTI_VERSION_HUB
            return PageType.POETIC_COLLECTION
        
        if signals["has_poem_structure"]:
            return PageType.POEM
            
        return PageType.OTHER

    def extract_sub_page_titles(self) -> Set[str]:
        """
        Extrait les titres des pages liées avec une logique de sélection prioritaire et robuste.
        """
        content_area = self.soup.select_one("#mw-content-text .mw-parser-output")
        if not content_area:
            return set()

        links = content_area.select("ul a, ol a")

        if not links:
            links = content_area.find_all("a", href=True)

        return self._filter_and_extract_titles_from_links(links)

    def _filter_and_extract_titles_from_links(self, links: list[Tag]) -> Set[str]:
        """
        Factorisation de l'extraction et du filtrage de liens depuis une liste d'éléments <a>.
        """
        titles: Set[str] = set()
        
        ignored_prefixes = (
            f":{get_localized_prefix(self.lang, 'author')}:",
            f":{get_localized_prefix(self.lang, 'category')}:",
            "Portail:", "Aide:", "Wikisource:", "Fichier:", "Spécial:",
            "Catégorie:",
        )

        for link in links:
            href = link.get("href", "")
            if (
                not href.startswith("/wiki/")
                or "action=edit" in href
                or "redlink=1" in href
                or link.get("class") and "new" in link.get("class")
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