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

        STANZA_DELIM = "===STANZA_DELIM==="
        VERSE_DELIM = "===VERSE_DELIM==="

        all_stanzas: List[List[str]] = []
        raw_markers: List[str] = []

        for block in poem_blocks:
            raw_markers.append(str(block.prettify().splitlines()[0]).strip())

            working_block = copy.copy(block)

            for noise in working_block.find_all(["span", "div"], class_=["pagenum", "ws-noexport", "mw-editsection"]):
                noise.decompose()

            has_br = bool(working_block.find("br"))
            has_p = bool(working_block.find("p"))

            if has_br or has_p:
                for p in working_block.find_all("p"):
                    p.append(f" {STANZA_DELIM} ")

                for div in working_block.find_all("div"):
                    div.append(f" {STANZA_DELIM} ")

                html_str = str(working_block)
                # Two or more consecutive <br> tags (with optional whitespace) denote a stanza break
                html_str = re.sub(r'(?:<br\s*/?>\s*){2,}', f' {STANZA_DELIM} ', html_str, flags=re.IGNORECASE)
                # A single <br> tag denotes a verse (line) break
                html_str = re.sub(r'<br\s*/?>', f' {VERSE_DELIM} ', html_str, flags=re.IGNORECASE)

                temp_soup = BeautifulSoup(html_str, "lxml")
                text_content = temp_soup.get_text(separator=" ")
                text_content = text_content.replace("\xa0", " ")

                raw_stanzas = text_content.split(STANZA_DELIM)
                for raw_stanza in raw_stanzas:
                    lines = raw_stanza.split(VERSE_DELIM)
                    verses = []
                    for line in lines:
                        for subline in line.split("\n"):
                            cleaned = re.sub(r"\s+", " ", subline).strip()
                            if cleaned:
                                verses.append(cleaned)
                    if verses:
                        all_stanzas.append(verses)
            else:
                # Wikitext or plain text fallback (e.g. raw text within <poem> tags)
                text_content = working_block.get_text()
                text_content = text_content.replace("\xa0", " ")
                raw_stanzas = re.split(r"\n\s*\n", text_content)
                for raw_stanza in raw_stanzas:
                    verses = [line.strip() for line in raw_stanza.splitlines() if line.strip()]
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
