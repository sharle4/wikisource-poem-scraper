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
        """
        Analyse la page une seule fois pour extraire des signaux booléens plus riches,
        en combinant HTML et wikitext pour plus de robustesse.
        """
        is_recueil_cat = "Recueils de poèmes" in self.categories
        is_multiversion_cat = "Éditions multiples" in self.categories

        has_donnees_structurees = bool(self.soup.find("a", title=re.compile(r"^d:Q\d+$")))
        has_editions_header = bool(self.soup.find(["h2", "h3"], string=re.compile(r"Éditions|Versions", re.I)))
        has_ws_summary = bool(self.soup.select_one("div.ws-summary"))
        has_toc = bool(self.soup.find("div", id="toc"))
        has_poem_structure = PoemParser.extract_poem_structure(self.soup) is not None

        templates = [tpl.name.strip().lower() for tpl in self.wikicode.filter_templates()]
        has_editions_template = "éditions" in templates
        has_titre_template_with_parts = any(
            tpl.name.strip().lower() == 'titre' and tpl.has('parties') 
            for tpl in self.wikicode.filter_templates()
        )

        return {
            "is_recueil_cat": is_recueil_cat,
            "is_multiversion_cat": is_multiversion_cat,
            "has_donnees_structurees": has_donnees_structurees,
            "has_editions_header": has_editions_header,
            "has_ws_summary": has_ws_summary,
            "has_toc": has_toc,
            "has_poem_structure": has_poem_structure,
            "has_editions_template": has_editions_template,
            "has_titre_template_with_parts": has_titre_template_with_parts,
        }

    def classify(self) -> PageType:
        """
        Détermine le type de page en appliquant une logique de priorisation stricte
        et en utilisant les nouveaux signaux du wikitext.
        """
        if self.ns != 0:
            if self.title.startswith(get_localized_prefix(self.lang, "author") + ":"):
                return PageType.AUTHOR
            return PageType.OTHER
        
        signals = self._get_page_signals()
        
        if signals["is_multiversion_cat"] or signals["has_editions_template"]:
            return PageType.MULTI_VERSION_HUB

        if signals["is_recueil_cat"] or signals["has_ws_summary"] or signals["has_toc"] or signals["has_titre_template_with_parts"]:
            return PageType.POETIC_COLLECTION

        if signals["has_editions_header"]:
            return PageType.MULTI_VERSION_HUB

        if signals["has_poem_structure"]:
            return PageType.POEM

        if signals["has_donnees_structurees"] and self.soup.select("ul > li > a"):
            return PageType.MULTI_VERSION_HUB
            
        return PageType.OTHER

    def extract_sub_page_titles(self) -> Set[str]:
        """
        Combine plusieurs stratégies (HTML et Wikitext) pour une robustesse maximale.
        """
        html_links = self._extract_links_from_html()
        wikitext_links = self._extract_links_from_wikitext()
        
        return html_links.union(wikitext_links)

    def _extract_links_from_html(self) -> Set[str]:
        """Extrait les liens depuis des zones sémantiques précises du HTML rendu."""
        summary_div = self.soup.select_one("div.ws-summary")
        if summary_div:
            return self._parse_links_from_element(summary_div)

        editions_header = self.soup.find(["h2", "h3"], string=re.compile(r"Éditions|Versions", re.I))
        if editions_header:
            next_element = editions_header.find_next_sibling()
            if next_element and next_element.name == 'ul':
                return self._parse_links_from_element(next_element)
        
        content_area = self.soup.select_one("#mw-content-text")
        if content_area:
            return self._parse_links_from_element(content_area)
            
        return set()

    def _extract_links_from_wikitext(self) -> Set[str]:
        """Extrait les liens directement du wikitext, ce qui est très robuste."""
        titles: Set[str] = set()
        for link in self.wikicode.filter_wikilinks():
            title = link.title.strip_code().strip()
            
            if self._is_valid_link_title(title):
                title = title.split('#')[0]
                if title and title != self.title:
                    titles.add(title)
        return titles

    def _is_valid_link_title(self, title: str) -> bool:
        """Vérifie si un titre de lien est pertinent pour le scraping de poèmes."""
        title_lower = title.lower()
        prefixes_a_exclure = [
            get_localized_prefix(self.lang, "category").lower(),
            get_localized_prefix(self.lang, "author").lower(),
            "portail:", "aide:", "wikisource:", "fichier:", "spécial:", "modèle:", "template:"
        ]
        if any(title_lower.startswith(prefix) for prefix in prefixes_a_exclure):
            return False
        return True

    def _parse_links_from_element(self, element: Tag) -> Set[str]:
        """Factorisation de l'extraction de liens depuis un élément BeautifulSoup."""
        titles: Set[str] = set()
        for link in element.find_all("a", href=True):
            href = link.get("href", "")
            if not href.startswith("/wiki/"):
                continue
            
            try:
                raw_title = unquote(href.split("/wiki/")[-1]).replace("_", " ")
                
                if self._is_valid_link_title(raw_title):
                    final_title = raw_title.split("#")[0]
                    if final_title and final_title != self.title:
                        titles.add(final_title)
            except Exception as e:
                logger.warning(f"Impossible d'extraire le titre de l'URL '{href}': {e}")
        return titles