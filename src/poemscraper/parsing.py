import re
from typing import Tuple, List, Optional
from .schemas import PoemStructure

POEM_TAG_REGEX = re.compile(r"(<poem.*?>)(.*?)</poem>", re.IGNORECASE | re.DOTALL)

class WikitextParser:
    """
    Analyse le Wikitext pour extraire les structures de poèmes.
    """

    @staticmethod
    def extract_poem_structure(wikitext: str) -> Optional[PoemStructure]:
        """
        Extrait toutes les strophes et tous les vers du wikitext basé sur les balises <poem>.
        Gère les poèmes multiples sur une page (via plusieurs balises <poem>) en les fusionnant
        en une seule structure.
        """
        
        all_stanzas: List[List[str]] = []
        all_markers: List[str] = []

        matches = POEM_TAG_REGEX.findall(wikitext)

        if not matches:
            return None

        for marker_tag, content in matches:
            all_markers.append(marker_tag.strip())
            
            content_cleaned = re.sub(r"", "", content, flags=re.DOTALL)
            content_cleaned = content_cleaned.replace("<nowiki>", "").replace("</nowiki>", "")
            
            raw_stanzas = re.split(r'\n\s*\n+', content_cleaned.strip())

            for raw_stanza in raw_stanzas:
                verses_in_stanza: List[str] = []
                stanza_text = raw_stanza.strip()
                
                if not stanza_text:
                    continue

                raw_lines = stanza_text.split('\n')
                
                for line in raw_lines:
                    verse = line.strip()
                    if verse:
                        verse_cleaned = re.sub(r"\{\{.*?\}\}", "", verse)
                        verse_cleaned = verse_cleaned.replace("'''", "").replace("''", "")
                        
                        if verse_cleaned.startswith(':'):
                            verse_cleaned = verse_cleaned.lstrip(':').lstrip()

                        if verse_cleaned:
                           verses_in_stanza.append(verse_cleaned)

                if verses_in_stanza:
                    all_stanzas.append(verses_in_stanza)

        if not all_stanzas:
            return None

        return PoemStructure(stanzas=all_stanzas, raw_markers=all_markers)

    @staticmethod
    def create_normalized_text(structure: PoemStructure) -> str:
        """
        Crée un texte plat normalisé à partir de la structure extraite.
        (Vers séparés par \n, Strophes séparées par \n\n)
        """
        stanza_texts = []
        for stanza in structure.stanzas:
            stanza_texts.append("\n".join(stanza))
        return "\n\n".join(stanza_texts)