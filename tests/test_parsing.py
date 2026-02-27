import pytest
from bs4 import BeautifulSoup
from src.poemscraper.parsing import PoemParser

SAMPLE_WIKITEXT_SIMPLE = """
Du texte avant.
<poem>
Vers 1 de la strophe 1
Vers 2 de la strophe 1

Vers 1 de la strophe 2
</poem>
Du texte après.
"""

SAMPLE_WIKITEXT_INDENTED = """
<poem class="fancy">
Strophe 1, Vers 1
:Vers 2 indenté
::Vers 3 doublement indenté

Strophe 2, Vers 1
</poem>
"""

SAMPLE_WIKITEXT_MULTI_BLOCK = """
<poem>Bloc 1, Vers 1</poem>
Bla bla.
<poem>Bloc 2, Vers 1</poem>
"""

SAMPLE_WIKITEXT_EMPTY = """
<poem>

</poem>
"""

SAMPLE_WIKITEXT_NO_POEM = "Juste du texte normal. [[Lien]]."


class TestPoemParser:

    def test_simple_poem_structure(self):
        soup = BeautifulSoup(SAMPLE_WIKITEXT_SIMPLE, "html.parser")
        structure = PoemParser.extract_poem_structure(soup)
        assert structure is not None
        assert len(structure.stanzas) == 2
        assert structure.stanzas[0] == ["Vers 1 de la strophe 1", "Vers 2 de la strophe 1"]
        assert structure.stanzas[1] == ["Vers 1 de la strophe 2"]
        assert structure.raw_markers == ["<poem>"]

    def test_indentation_stripping(self):
        soup = BeautifulSoup(SAMPLE_WIKITEXT_INDENTED, "html.parser")
        structure = PoemParser.extract_poem_structure(soup)
        assert structure is not None
        assert len(structure.stanzas) == 2
        assert structure.stanzas[0] == ["Strophe 1, Vers 1", ":Vers 2 indenté", "::Vers 3 doublement indenté"]
        assert structure.raw_markers[0] == '<poem class="fancy">'

    def test_multiple_poem_blocks_merged(self):
        soup = BeautifulSoup(SAMPLE_WIKITEXT_MULTI_BLOCK, "html.parser")
        structure = PoemParser.extract_poem_structure(soup)
        assert structure is not None
        assert len(structure.stanzas) == 2
        assert structure.stanzas[0] == ["Bloc 1, Vers 1"]
        assert structure.stanzas[1] == ["Bloc 2, Vers 1"]
        assert structure.raw_markers == ["<poem>", "<poem>"]

    def test_no_poem_tag(self):
        soup = BeautifulSoup(SAMPLE_WIKITEXT_NO_POEM, "html.parser")
        structure = PoemParser.extract_poem_structure(soup)
        assert structure is None

    def test_empty_poem_tag(self):
        soup = BeautifulSoup(SAMPLE_WIKITEXT_EMPTY, "html.parser")
        structure = PoemParser.extract_poem_structure(soup)
        assert structure is None

    def test_normalized_text_creation(self):
        soup = BeautifulSoup(SAMPLE_WIKITEXT_SIMPLE, "html.parser")
        structure = PoemParser.extract_poem_structure(soup)
        assert structure is not None
        normalized = PoemParser.create_normalized_text(structure)
        expected_text = "Vers 1 de la strophe 1\nVers 2 de la strophe 1\n\nVers 1 de la strophe 2"
        assert normalized == expected_text