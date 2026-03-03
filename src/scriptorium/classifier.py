import logging
import re
from enum import Enum, auto
from typing import Set, Tuple, List
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
    SECTION_TITLE = auto()


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
        self.internal_prefixes_to_ignore = [
            get_localized_prefix(lang, "category"),
            get_localized_prefix(lang, "author"),
            "Portail", "Aide", "Wikisource", "Fichier", "Spécial",
            "Livre", "Discussion", "Modèle", "Projet"
        ]

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
        Extrait les titres des sous-pages pour un MULTI_VERSION_HUB de manière exhaustive et fiable.
        """
        titles: Set[str] = set()
        
        links = self.soup.select('a[href][title]')

        normalized_self_title = re.sub(r"\s*\([^)]*\)", "", self.title or "")
        normalized_self_title = re.sub(r"\s+", " ", normalized_self_title).strip().lower()

        for link in links:
            href = link.get("href", "")
            link_title = link.get('title', '')

            if not href.startswith("/wiki/"):
                continue
            
            if any(link_title.startswith(f"{prefix}:") for prefix in self.internal_prefixes_to_ignore) or \
               "action=edit" in href or "&redlink=1" in href:
                continue

            is_a_version = False
            
            normalized_link_title = re.sub(r"\s*\([^)]*\)", "", link_title or "")
            normalized_link_title = re.sub(r"\s+", " ", normalized_link_title).strip().lower()
            if normalized_self_title in normalized_link_title:
                is_a_version = True
            
            else:
                try:
                    path = href.split("wiki/", 1)[1]
                    decoded_title_from_href = unquote(path.split("#", 1)[0]).replace("_", " ")
                    if decoded_title_from_href.startswith(self.title + "/"):
                        is_a_version = True
                except IndexError:
                    continue
            
            if is_a_version:
                titles.add(link_title)
                
        logger.info(f"Extrait {len(titles)} titres de version depuis la page hub '{self.title}'.")
        return titles

    def _is_valid_poem_link(self, link: Tag) -> bool:
        """Vérifie si un tag <a> est un lien plausible vers un poème."""
        if not isinstance(link, Tag) or link.name != 'a':
            return False
        href = link.get('href', '')
        title = link.get('title', '')
        if not title or not href:
            return False
        if not href.startswith('/wiki/') or '&redlink=1' in href or 'action=edit' in href:
            return False
        if any(title.startswith(f"{prefix}:") for prefix in self.internal_prefixes_to_ignore):
            return False
        if href.startswith('#'):
            return False
        if link.find('img'):
            return False
        if title == self.title:
            return False
        return True

    def _is_section_title_element(self, element: Tag) -> bool:
        """
        Détermine si un élément agit comme un titre de section.
        Logique améliorée pour plus de précision.
        """
        if not isinstance(element, Tag):
            return False
        
        if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            return True
            
        if element.name == 'dt':
            return True

        has_valid_link = element.find(self._is_valid_poem_link)
        if has_valid_link:
            return False

        text = element.get_text(strip=True)
        if not text or len(text) <= 1 or len(text) > 150:
            return False
        
        if element.find(['b', 'strong', 'i', 'em']):
            return True
            
        if element.name in ['li', 'p'] and not has_valid_link:
            return True

        return False

    def extract_ordered_collection_links(self) -> List[Tuple[str, PageType]]:
        """
        MOTEUR DE PARSING STRUCTUREL EXPERT v2
        Extrait les liens et titres de section en analysant la structure sémantique du document.
        """
        ordered_items: List[Tuple[str, PageType]] = []
        content_area = self.soup.select_one(".mw-parser-output")

        if not content_area:
            logger.warning(f"Impossible de trouver la zone de contenu '.mw-parser-output' pour '{self.title}'.")
            return []

        logger.debug(f"Début de l'analyse structurelle experte pour '{self.title}'.")
        
        candidate_elements = content_area.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'ul', 'ol', 'dl', 'div'])

        last_added_title = None

        for element in candidate_elements:
            if any(parent == element for parent in element.find_parents(['ul', 'ol', 'dl'])):
                 continue

            if self._is_section_title_element(element):
                title_text = element.get_text(strip=True)
                if title_text != last_added_title:
                    logger.debug(f"Élément '{element.name}' identifié comme SECTION_TITLE: '{title_text}'")
                    ordered_items.append((title_text, PageType.SECTION_TITLE))
                    last_added_title = title_text
                continue

            if element.name in ['ul', 'ol', 'dl']:
                logger.debug(f"Analyse du conteneur de liste '{element.name}'.")
                list_item_tags = 'dd' if element.name == 'dl' else 'li'
                for item in element.find_all(list_item_tags, recursive=False):
                    link = item.find(self._is_valid_poem_link)
                    if link and link.get('title'):
                        title = link['title']
                        if title != last_added_title:
                            logger.debug(f"  Poème trouvé dans '{list_item_tags}': '{title}'")
                            ordered_items.append((title, PageType.POEM))
                            last_added_title = title
                    elif self._is_section_title_element(item):
                         title_text = item.get_text(strip=True)
                         if title_text != last_added_title:
                            logger.debug(f"  Section trouvée dans '{list_item_tags}': '{title_text}'")
                            ordered_items.append((title_text, PageType.SECTION_TITLE))
                            last_added_title = title_text
                continue
            
            link = element.find(self._is_valid_poem_link)
            if link and link.get('title'):
                title = link['title']
                if title != last_added_title:
                    logger.debug(f"Poème trouvé dans un conteneur simple '{element.name}': '{title}'")
                    ordered_items.append((title, PageType.POEM))
                    last_added_title = title
        
        final_items = []
        seen_titles = set()
        for title, page_type in ordered_items:
            if title not in seen_titles:
                final_items.append((title, page_type))
                seen_titles.add(title)

        logger.info(f"Analyse structurelle terminée pour '{self.title}'. {len(final_items)} éléments uniques extraits.")
        return final_items