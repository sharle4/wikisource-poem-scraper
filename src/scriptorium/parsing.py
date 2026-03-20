import copy
import logging
import re
from typing import List, Optional

from bs4 import BeautifulSoup

from .schemas import PoemStructure

logger = logging.getLogger(__name__)


class PoemParser:
    """
    Parses rendered HTML (via a BeautifulSoup object) to extract poem structures.
    """

    @staticmethod
    def extract_poem_structure(soup: BeautifulSoup) -> Optional[PoemStructure]:
        """
        Extracts stanzas and verses from HTML based on common Wikisource patterns
        such as <div class="poem"> or the <poem> tag.
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

            working_block = copy.copy(block)

            for pagenum in working_block.find_all("span", class_="pagenum"):
                pagenum.decompose()

            for br in working_block.find_all("br"):
                br.replace_with("\n")

            for p in working_block.find_all("p"):
                p.append("\n\n")

            for div in working_block.find_all("div"):
                div.append("\n")

            text_content = working_block.get_text(separator="")

            text_content = text_content.replace("\xa0", " ")

            text_content = "\n".join(line.strip() for line in text_content.split("\n"))
            
            text_content = re.sub(r'\n{2,}', '\n\n', text_content)

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
        Creates a flat normalized text from the extracted structure.
        (Verses separated by \n, stanzas separated by \n\n)
        """
        stanza_texts = ["\n".join(stanza) for stanza in structure.stanzas]
        return "\n\n".join(stanza_texts)
