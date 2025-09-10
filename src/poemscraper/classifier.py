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
    """
    Analyse les données d'une page pour la classifier avec une logique experte.
    """

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
        """Analyse la page une seule fois pour extraire des signaux booléens."""
        content_area = self.soup.select_one("#mw-content-text .mw-parser-output") or self.soup
        
        # Category signals
        is_recueil_cat = "Recueils de poèmes" in self.categories
        is_multiversion_cat = "Éditions multiples" in self.categories
        
        # Structure signals
        has_poem_structure = PoemParser.extract_poem_structure(self.soup) is not None
        has_ws_summary = bool(content_area.select_one("div.ws-summary"))
        has_toc = bool(content_area.select_one("div#toc"))
        has_editions_header = bool(content_area.find(["h2", "h3"], string=re.compile(r"^\s*Éditions\s*$", re.I)))
        
        # Content signals
        links_in_lists = content_area.select('#mw-content-text li a[href^="/wiki/"], #mw-content-text .tableItem a[href^="/wiki/"]')
        has_significant_links_in_lists = len(links_in_lists) > 3

        return {
            "is_recueil_cat": is_recueil_cat,
            "is_multiversion_cat": is_multiversion_cat,
            "has_poem_structure": has_poem_structure,
            "has_ws_summary": has_ws_summary,
            "has_toc": has_toc,
            "has_editions_header": has_editions_header,
            "has_significant_links_in_lists": has_significant_links_in_lists,
        }

    def classify(self) -> PageType:
        """Détermine le type de page en appliquant une logique de priorisation stricte."""
        if self.ns != 0:
            return PageType.AUTHOR if self.title.startswith(get_localized_prefix(self.lang, "author") + ":") else PageType.OTHER
        
        signals = self._get_page_signals()

        if signals["is_multiversion_cat"]:
            return PageType.MULTI_VERSION_HUB
        
        if signals["is_recueil_cat"]:
            return PageType.POETIC_COLLECTION

        if signals["has_editions_header"]:
             return PageType.MULTI_VERSION_HUB

        if signals["has_ws_summary"] or signals["has_toc"]:
            return PageType.POETIC_COLLECTION
            
        if signals["has_poem_structure"]:
            return PageType.POEM

        if signals["has_significant_links_in_lists"]:
            return PageType.POETIC_COLLECTION

        return PageType.OTHER

    def extract_sub_page_titles(self) -> Set[str]:
        """
        Extrait les titres des sous-pages (poèmes, éditions) de manière robuste.
        Cette méthode utilise plusieurs stratégies pour trouver les liens pertinents.
        """
        toc_element = self.soup.select_one("div.ws-summary, div#toc, div.ws_summary")
        if toc_element:
            logger.debug(f"'{self.title}': Élément TOC trouvé. Extraction des liens.")
            return self._extract_links_from_element(toc_element)

        editions_header = self.soup.find(["h2", "h3"], string=re.compile(r"^\s*Éditions\s*$", re.I))
        if editions_header:
            next_element = editions_header.find_next_sibling()
            if next_element and next_element.name in ['ul', 'ol', 'dl']:
                logger.debug(f"'{self.title}': En-tête 'Éditions' trouvé. Extraction des liens de la liste suivante.")
                return self._extract_links_from_element(next_element, check_title_attr=True)

        content_area = self.soup.select_one("#mw-content-text .mw-parser-output")
        if content_area:
            logger.debug(f"'{self.title}': Aucun conteneur spécifique trouvé. Recherche dans toutes les listes de #mw-content-text.")
            return self._extract_links_from_element(content_area)
            
        return set()

    def _extract_links_from_element(self, element: Tag, check_title_attr: bool = False) -> Set[str]:
        """
        Factorisation de l'extraction de liens depuis un élément BeautifulSoup.
        Extrait les liens internes valides depuis des structures de listes (<li>) ou des tables des matières (div.tableItem).
        """
        titles: Set[str] = set()
        author_prefix = get_localized_prefix(self.lang, "author")
        category_prefix = get_localized_prefix(self.lang, "category")
        
        links = element.select('li a[href], .tableItem a[href]')
        
        if not links:
            links = element.select('a[href]')

        for link in links:
            href = link.get("href", "")
            
            if not href or not href.startswith("/wiki/"):
                continue

            if any(
                href.startswith(f"/wiki/{prefix}:")
                for prefix in [category_prefix, author_prefix, "Portail", "Aide", "Wikisource", "Fichier", "Spécial", "Livre"]
            ) or "action=edit" in href:
                continue

            link_title_attr = link.get('title', '')
            if check_title_attr and self.title not in link_title_attr:
                continue

            try:
                raw_title = href.split("/")[-1].split("#")[0]
                title = unquote(raw_title).replace("_", " ")
                if title and title != self.title:
                    titles.add(title)
            except Exception as e:
                logger.warning(f"Impossible d'extraire le titre de l'URL '{href}': {e}")
                
        logger.info(f"Extrait {len(titles)} titres de sous-pages de '{self.title}'.")
        return titles