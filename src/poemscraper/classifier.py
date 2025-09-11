import logging
import re
from enum import Enum, auto
from typing import Set, Tuple, Optional
from urllib.parse import unquote

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

from .parsing import PoemParser
from .log_manager import LogManager

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
        log_manager: Optional[LogManager] = None,
    ):
        self.page_data = page_data
        self.soup = soup
        self.lang = lang
        self.wikicode = wikicode
        self.title = page_data.get("title", "")
        self.page_id = page_data.get("pageid", 0)
        self.ns = page_data.get("ns", -1)
        self.categories = {
            c["title"].split(":")[-1] for c in page_data.get("categories", [])
        }
        self.log_manager = log_manager

    def _get_page_signals(self) -> dict:
        """Analyse la page une seule fois pour extraire des signaux booléens."""
        
        is_recueil_cat = "Recueils de poèmes" in self.categories
        is_multiversion_cat = "Éditions multiples" in self.categories

        has_donnees_structurees = bool(self.soup.find("a", title=re.compile(r"^d:Q\d+$")))
        has_editions_header = bool(self.soup.find(["h2", "h3"], string=re.compile(r"Éditions", re.I)))

        has_ws_summary = bool(self.soup.select_one("div.ws-summary"))
        has_toc = bool(self.soup.find("div", id="toc"))
        has_poem_structure = PoemParser.extract_poem_structure(self.soup) is not None

        return {
            "is_recueil_cat": is_recueil_cat,
            "is_multiversion_cat": is_multiversion_cat,
            "has_donnees_structurees": has_donnees_structurees,
            "has_editions_header": has_editions_header,
            "has_ws_summary": has_ws_summary,
            "has_toc": has_toc,
            "has_poem_structure": has_poem_structure,
        }

    def classify(self) -> Tuple[PageType, str]:
        """Détermine le type de page et la raison de la classification."""
        if self.ns != 0:
            reason = "is_author_page" if self.title.startswith(get_localized_prefix(self.lang, "author") + ":") else "is_other_namespace"
            page_type = PageType.AUTHOR if reason == "is_author_page" else PageType.OTHER
            return page_type, reason
        
        signals = self._get_page_signals()

        if signals["is_recueil_cat"]:
            return PageType.POETIC_COLLECTION, "is_recueil_cat"
        if signals["is_multiversion_cat"]:
            return PageType.MULTI_VERSION_HUB, "is_multiversion_cat"
        
        for key in ("has_ws_summary", "has_toc", "has_editions_header"):
            if signals[key]:
                if signals["has_donnees_structurees"]:
                    return PageType.MULTI_VERSION_HUB, f"has_donnees_structurees and {key}"
                return PageType.POETIC_COLLECTION, key

        if signals["has_poem_structure"]:
            return PageType.POEM, "has_poem_structure"

        if signals["has_donnees_structurees"] and self.soup.select("ul > li"):
            return PageType.MULTI_VERSION_HUB, "has_donnees_structurees and has_list_items"
            
        return PageType.OTHER, "no_signals_matched"

    def extract_hub_sub_pages(self) -> Set[str]:
        """
        Extrait les titres des sous-pages pour un MULTI_VERSION_HUB.
        Cette fonction emploie une stratégie à plusieurs niveaux pour identifier de manière robuste
        les liens vers différentes éditions ou versions d'une même œuvre sur une page hub.
        """
        titles: Set[str] = set()
        
        content_area = self.soup.select_one("#mw-content-text .mw-parser-output")
        if not content_area:
            logger.debug(f"'{self.title}': Le sélecteur strict a échoué. Tentative avec le sélecteur de fallback '.mw-parser-output'.")
            content_area = self.soup.select_one(".mw-parser-output")

        if not content_area:
            logger.warning(
                f"'{self.title}': Impossible de trouver la zone de contenu principal, même avec le fallback. La structure HTML est atypique."
            )
            if self.log_manager:
                self.log_manager.log_debug_html(self.title, self.page_id, self.soup.prettify())
            return titles

        search_scope = None
        
        homonymy_box = content_area.select_one("#homonymie-editions, .homonymie")
        if homonymy_box:
            logger.debug(f"'{self.title}': Stratégie de hub 3a - Boîte d'homonymie trouvée.")
            search_scope = homonymy_box

        if not search_scope:
            editions_header = content_area.find(["h2", "h3"], string=re.compile(r"^\s*Éditions\s*$", re.I))
            if editions_header:
                next_element = editions_header.find_next_sibling()
                if next_element and next_element.name in ['ul', 'ol', 'dl']:
                    logger.debug(f"'{self.title}': Stratégie de hub 3b - Liste sous en-tête 'Éditions' trouvée.")
                    search_scope = next_element
        
        if not search_scope:
            logger.debug(f"'{self.title}': Aucune portée ciblée trouvée. Passage à la recherche globale sur le contenu.")
            search_scope = content_area

        author_prefix = get_localized_prefix(self.lang, "author")
        category_prefix = get_localized_prefix(self.lang, "category")
        ignored_prefixes = [
            f"/wiki/{p}:" for p in [
                category_prefix, author_prefix, "Portail", "Aide", 
                "Wikisource", "Fichier", "Spécial", "Livre", "Modèle"
            ]
        ]

        for link in search_scope.select('a[href]'):
            href = link.get("href", "")
            
            if not href.startswith("/wiki/"):
                continue
            
            if any(href.startswith(prefix) for prefix in ignored_prefixes):
                continue
            
            if "action=edit" in href or "redlink=1" in href:
                continue

            try:
                path = href.split("wiki/", 1)[1]
                raw_title = path.split("#", 1)[0]
                decoded_title = unquote(raw_title).replace("_", " ").strip()

                if decoded_title and decoded_title != self.title and self.title in decoded_title:
                    titles.add(decoded_title)
            
            except IndexError:
                continue
            except Exception as e:
                logger.warning(f"Impossible de parser le lien '{href}' sur la page '{self.title}': {e}", exc_info=False)
                continue
                
        if titles:
            logger.info(f"Extraction réussie de {len(titles)} titres de version depuis le hub '{self.title}'.")
        else:
            logger.warning(f"Aucun titre de version trouvé pour le hub '{self.title}'. La structure de la page est peut-être atypique.")
            
        return titles

    def extract_collection_sub_pages(self) -> Set[str]:
        """
        Extrait les titres des sous-pages (poèmes, éditions) pour une POETIC_COLLECTION.
        Cette méthode utilise des stratégies basées sur la structure (TOC, en-têtes).
        """
        toc_element = self.soup.select_one("div.ws-summary, div#toc, div.ws_summary")
        if toc_element:
            logger.debug(f"'{self.title}': Élément TOC trouvé. Extraction des liens.")
            return self._extract_links_from_collection_element(toc_element)

        editions_header = self.soup.find(["h2", "h3"], string=re.compile(r"^\s*Éditions\s*$", re.I))
        if editions_header:
            next_element = editions_header.find_next_sibling()
            if next_element and next_element.name in ['ul', 'ol', 'dl']:
                logger.debug(f"'{self.title}': En-tête 'Éditions' trouvé. Extraction des liens de la liste suivante.")
                return self._extract_links_from_collection_element(next_element, check_title_attr=True)

        content_area = self.soup.select_one("#mw-content-text .mw-parser-output")
        if content_area:
            logger.debug(f"'{self.title}': Aucun conteneur spécifique trouvé. Recherche dans toutes les listes de #mw-content-text.")
            return self._extract_links_from_collection_element(content_area)
            
        return set()

    def _extract_links_from_collection_element(self, element: Tag, check_title_attr: bool = False) -> Set[str]:
        """
        Factorisation de l'extraction de liens depuis un élément BeautifulSoup pour une collection.
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
                path = href.split("wiki/", 1)[1]
                raw_title = path.split("#", 1)[0]
                title = unquote(raw_title).replace("_", " ")
                if title and title != self.title:
                    titles.add(title)
            except Exception as e:
                logger.warning(f"Impossible d'extraire le titre de l'URL '{href}': {e}")
                
        logger.info(f"Extrait {len(titles)} titres de sous-pages de la collection '{self.title}'.")
        return titles