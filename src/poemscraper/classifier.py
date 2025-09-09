import logging
from enum import Enum, auto
from typing import List, Set
from urllib.parse import unquote

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

from .parsing import PoemParser

logger = logging.getLogger(__name__)


class PageType(Enum):
    """Énumération des types de pages possibles sur Wikisource."""

    POEM = auto()
    COLLECTION = auto()
    AUTHOR = auto()
    DISAMBIGUATION = auto()
    OTHER = auto()


def get_localized_prefix(lang: str, prefix_type: str) -> str:
    """Retourne le préfixe localisé pour différentes fonctionnalités MediaWiki."""
    prefixes = {
        "fr": {"category": "Catégorie", "author": "Auteur"},
        "en": {"category": "Category", "author": "Author"},
        "de": {"category": "Kategorie", "author": "Autor"},
    }
    return prefixes.get(lang, {}).get(prefix_type, prefix_type.capitalize())


class PageClassifier:
    """
    Analyse les données brutes d'une page (wikitext, HTML) pour la classifier
    et extraire des informations contextuelles.
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

    def _has_poem_structure(self) -> bool:
        """Détermine si la page contient une structure de poème HTML identifiable."""
        structure = PoemParser.extract_poem_structure(self.soup)
        return structure is not None and len(structure.stanzas) > 0

    def _is_explicit_collection(self) -> bool:
        """
        Détermine si une page est un recueil de manière très probable.
        Signal fort : présence d'une table des matières (TDM/TOC).
        Signal moyen : présence d'une liste à puces avec de nombreux liens.
        """
        if self.soup.find("div", id="toc"):
            return True
        
        list_items = self.soup.select("ul > li")
        if len(list_items) > 5:
            links_in_list = sum(1 for li in list_items if li.find("a", href=True))
            if links_in_list / len(list_items) > 0.7:
                 return True

        return False

    def classify(self) -> PageType:
        """
        Détermine le type de page en appliquant une série d'heuristiques ordonnées
        pour une précision maximale.
        """
        if self.ns != 0:
            if self.title.startswith(get_localized_prefix(self.lang, "author") + ":"):
                return PageType.AUTHOR
            return PageType.OTHER

        disambiguation_templates = {"homonymie", "disambig", "homonymes"}
        if any(
            t.name.strip().lower() in disambiguation_templates
            for t in self.wikicode.filter_templates()
        ):
            return PageType.DISAMBIGUATION

        has_poem = self._has_poem_structure()
        is_collection = self._is_explicit_collection()

        if is_collection:
            return PageType.COLLECTION
        
        if has_poem:
            return PageType.POEM

        return PageType.OTHER

    def extract_sub_page_titles(self) -> Set[str]:
        """Extrait les titres des pages liées depuis un recueil."""
        titles: Set[str] = set()
        author_prefix = get_localized_prefix(self.lang, "author")
        category_prefix = get_localized_prefix(self.lang, "category")
        
        link_elements: List[Tag] = self.soup.select("#toc a, #mw-content-text a")

        for link in link_elements:
            href = link.get("href", "")
            if not href or not href.startswith("/wiki/"):
                continue

            if any(
                href.startswith(f"/wiki/{prefix}:")
                for prefix in [
                    category_prefix, author_prefix, "Portail", "Aide",
                    "Wikisource", "Fichier",
                ]
            ):
                continue

            try:
                raw_title = href.split("/")[-1].split("#")[0]
                title = unquote(raw_title).replace("_", " ")
                if title and title != self.title:
                    titles.add(title)
            except Exception as e:
                logger.warning(f"Impossible d'extraire le titre de l'URL '{href}': {e}")
        return titles