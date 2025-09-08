import logging
from enum import Enum, auto
from typing import List, Optional, Set
from urllib.parse import unquote

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


class PageType(Enum):
    """Énumération des types de pages possibles sur Wikisource."""

    POEM = auto()
    COLLECTION = auto()
    AUTHOR = auto()
    DISAMBIGUATION = auto()
    OTHER = auto()


class PageClassifier:
    """
    Analyse les données brutes d'une page (wikitext, HTML) pour la classifier
    et extraire des informations contextuelles.
    """

    def __init__(
        self,
        page_data: dict,
        page_html: str,
        lang: str,
        wikicode: mwparserfromhell.wikicode.Wikicode,
    ):
        self.page_data = page_data
        self.soup = BeautifulSoup(page_html, "lxml")
        self.lang = lang
        self.wikicode = wikicode
        self.title = page_data.get("title", "")
        self.ns = page_data.get("ns", -1)

    def classify(self) -> PageType:
        """
        Détermine le type de la page en appliquant une série d'heuristiques.

        Returns:
            PageType: Le type de page le plus probable.
        """
        if self.ns == 104:
            return PageType.AUTHOR

        disambiguation_templates = {"homonymie", "disambig", "homonymes"}
        if any(
            t.name.strip().lower() in disambiguation_templates
            for t in self.wikicode.filter_templates()
        ):
            return PageType.DISAMBIGUATION

        has_poem_tag = "<poem" in self.page_data.get("revisions", [{}])[0].get(
            "content", ""
        )
        has_toc = self.soup.find("div", id="toc") is not None

        if has_toc and not has_poem_tag:
            return PageType.COLLECTION

        content_links = self.soup.select("#mw-content-text a[href^='/wiki/']")
        if has_toc and len(content_links) > 5:
            return PageType.COLLECTION

        if has_poem_tag:
            return PageType.POEM

        return PageType.OTHER

    def extract_sub_page_titles(self) -> Set[str]:
        """
        Si la page est une collection, extrait les titres des pages liées
        depuis la table des matières ou le corps du texte.

        Returns:
            Set[str]: Un ensemble de titres de pages uniques.
        """
        titles: Set[str] = set()
        link_elements: List[Tag] = self.soup.select("#toc a, #mw-content-text a")

        for link in link_elements:
            href = link.get("href", "")
            if not href.startswith("/wiki/"):
                continue

            if any(
                href.startswith(f"/wiki/{prefix}:")
                for prefix in [
                    "Catégorie",
                    "Auteur",
                    "Portail",
                    "Aide",
                    "Wikisource",
                ]
            ):
                continue

            try:
                raw_title = href.split("/")[-1]
                raw_title = raw_title.split("#")[0]
                title = unquote(raw_title).replace("_", " ")
                if title:
                    titles.add(title)
            except Exception as e:
                logger.warning(
                    f"Impossible d'extraire le titre depuis l'URL '{href}': {e}"
                )

        return titles