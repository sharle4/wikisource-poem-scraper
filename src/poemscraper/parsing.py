import logging
from typing import List, Optional

from bs4 import BeautifulSoup

from .models import PoemStructure

logger = logging.getLogger(__name__)


class PoemParser:
    """
    Analyse le HTML rendu (via un objet BeautifulSoup) pour extraire les structures de poèmes.
    Cette approche est beaucoup plus fiable que l'analyse du wikitext pour les balises de présentation.
    """

    @staticmethod
    def extract_poem_structure(soup: BeautifulSoup) -> Optional[PoemStructure]:
        """
        Extrait les strophes et les vers du HTML en se basant sur les motifs courants de Wikisource
        comme <div class="poem"> ou la balise <poem>.
        """
        poem_blocks = soup.find_all(["div", "span"], class_="poem")

        if not poem_blocks:
            poem_blocks = soup.find_all("poem")

        if not poem_blocks:
            return None

        all_stanzas: List[List[str]] = []
        raw_markers: List[str] = []

        for block in poem_blocks:
            raw_markers.append(str(block.prettify().splitlines()[0]).strip())

            text_content = block.get_text(separator="\n", strip=True)

            raw_stanzas = text_content.split("\n\n")

            for raw_stanza in raw_stanzas:
                stanza_lines = raw_stanza.strip().split("\n")
                verses = [line.strip() for line in stanza_lines if line.strip()]
                if verses:
                    all_stanzas.append(verses)

        if not all_stanzas:
            return None

        return PoemStructure(stanzas=all_stanzas, raw_markers=raw_markers)

    @staticmethod
    def create_normalized_text(structure: PoemStructure) -> str:
        """
        Crée un texte plat normalisé à partir de la structure extraite.
        (Vers séparés par \n, Strophes séparées par \n\n)
        """
        stanza_texts = ["\n".join(stanza) for stanza in structure.stanzas]
        return "\n\n".join(stanza_texts)