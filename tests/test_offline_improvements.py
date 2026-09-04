import unittest
import mwparserfromhell
from bs4 import BeautifulSoup

from src.scriptorium.schemas import Collection, Section, PoemInfo
from src.scriptorium.classifier import (
    PageClassifier,
    PageType,
    clean_section_title,
    TOC_HEADER_PREFIX_RE,
    PAGE_NUM_OR_PUNCT_RE,
)
from src.scriptorium.processors import (
    PoemProcessor,
    parse_imprint,
    extract_page_metadata,
)


class TestImprintParser(unittest.TestCase):
    def test_standard_imprint(self):
        year, pub = parse_imprint("Michel Lévy frères, Paris, 1868")
        self.assertEqual(year, "1868")
        self.assertEqual(pub, "Michel Lévy frères")

    def test_city_first(self):
        year, pub = parse_imprint("Paris, Alphonse Lemerre, éditeur, 1893")
        self.assertEqual(year, "1893")
        self.assertEqual(pub, "Alphonse Lemerre")

    def test_publisher_and_year_only(self):
        year, pub = parse_imprint("Garnier Frères, 1888")
        self.assertEqual(year, "1888")
        self.assertEqual(pub, "Garnier Frères")

    def test_hyphenated_publisher(self):
        year, pub = parse_imprint("Poulet-Malassis et De Broise, 1857")
        self.assertEqual(year, "1857")
        self.assertEqual(pub, "Poulet-Malassis et De Broise")

    def test_year_only(self):
        year, pub = parse_imprint("1857")
        self.assertEqual(year, "1857")
        self.assertIsNone(pub)

    def test_empty_imprint(self):
        year, pub = parse_imprint("")
        self.assertIsNone(year)
        self.assertIsNone(pub)


class TestSectionTitleCleaning(unittest.TestCase):
    def test_section_title_cleaning(self):
        # Cleans trailing dots and page numbers
        self.assertEqual(clean_section_title("SPLEEN ET IDÉAL . . . . . . . . . . . . 15"), "SPLEEN ET IDÉAL")
        self.assertEqual(clean_section_title("Livre Premier — 12"), "Livre Premier")
        self.assertEqual(clean_section_title("Tableaux parisiens"), "Tableaux parisiens")
        # Empty when purely dots and numbers
        self.assertEqual(clean_section_title(". . . . . . . . . . . . . . . . . . . 356"), "")

    def test_section_title_element_filtering(self):
        classifier = PageClassifier(
            {"pageid": 1, "title": "Test", "ns": 0, "categories": []},
            BeautifulSoup("", "lxml"), "fr", mwparserfromhell.parse("")
        )
        # TOC headers should NOT be recognized as section titles
        soup = BeautifulSoup("<h2>TABLE DES MATIÈRES</h2><h3>TABLE</h3><p>. . . 356</p><h4>SPLEEN ET IDÉAL</h4>", "lxml")
        self.assertFalse(classifier._is_section_title_element(soup.find("h2")))
        self.assertFalse(classifier._is_section_title_element(soup.find("h3")))
        self.assertFalse(classifier._is_section_title_element(soup.find("p")))
        # Legitimate section title SHOULD be recognized
        self.assertTrue(classifier._is_section_title_element(soup.find("h4")))


class TestPageClassifierImprovements(unittest.TestCase):
    def test_parsoid_tableitem_collection(self):
        html = """
        <div class="mw-parser-output">
            <div class="headertemplate">
                <div class="headertemplate-title"><a href="./Mon_Recueil">Mon Recueil</a></div>
                <div class="headertemplate-author"><a href="./Auteur:Charles_Baudelaire">Charles Baudelaire</a></div>
            </div>
            <h2>Table des matières</h2>
            <div class="tableItem"><a href="./Mon_Recueil/Poeme_1">Poème 1</a></div>
            <div class="tableItem"><a href="./Mon_Recueil/Poeme_2">Poème 2</a></div>
            <div class="tableItem"><a href="./Mon_Recueil/Poeme_3">Poème 3</a></div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        page_data = {
            "pageid": 100,
            "title": "Mon Recueil",
            "ns": 0,
            "categories": [{"title": "Catégorie:Recueils de poèmes"}],
        }
        classifier = PageClassifier(page_data, soup, "fr", mwparserfromhell.parse(""))
        page_type, reason = classifier.classify()
        self.assertEqual(page_type, PageType.POETIC_COLLECTION)

        links = classifier.extract_ordered_collection_links()
        link_titles = [t for t, p in links if p == PageType.POEM]
        self.assertIn("Mon Recueil/Poeme 1", link_titles)
        self.assertIn("Mon Recueil/Poeme 2", link_titles)
        self.assertIn("Mon Recueil/Poeme 3", link_titles)
        # Header links should NOT be extracted as poems
        self.assertNotIn("Mon Recueil", link_titles)
        self.assertNotIn("Auteur:Charles Baudelaire", link_titles)


class TestPoemProcessorImprovements(unittest.TestCase):
    def setUp(self):
        self.processor = PoemProcessor()

    def test_metadata_extraction_from_html_and_wikitext(self):
        html = """
        <div class="mw-parser-output">
            <div class="headertemplate">
                <div class="headertemplate-title"><span>Les Fleurs du mal</span></div>
                <div class="headertemplate-author"><a href="./Auteur:Charles_Baudelaire">Charles Baudelaire</a></div>
                <div class="headertemplate-reference"><small>Michel Lévy frères, Paris, 1868</small></div>
            </div>
            <div class="poem">
                <p>Voici des vers magnifiques,<br/>Et des rimes dorées.</p>
            </div>
        </div>
        """
        wikitext = "{{titre|[[Les Fleurs du mal]]|[[Auteur:Charles Baudelaire|Charles Baudelaire]]|<small>Michel Lévy frères, Paris, 1868</small>}}\n<poem>\nVoici des vers magnifiques,\nEt des rimes dorées.\n</poem>"
        soup = BeautifulSoup(html, "lxml")
        wikicode = mwparserfromhell.parse(wikitext)
        meta = extract_page_metadata(soup, wikicode, "Les Fleurs du mal (1868)")

        self.assertEqual(meta.get("author"), "Charles Baudelaire")
        self.assertEqual(meta.get("source_collection"), "Les Fleurs du mal")
        self.assertEqual(meta.get("publication_date"), "1868")
        self.assertEqual(meta.get("publisher"), "Michel Lévy frères")

    def test_contextual_inheritance(self):
        # A subpage poem without author, date, or publisher in its own markup
        html = """
        <div class="mw-parser-output">
            <div class="poem">
                <p>Sois sage, ô ma Douleur, et tiens-toi plus tranquille.<br/>Tu réclamais le Soir ; il descend ; le voici :</p>
            </div>
        </div>
        """
        wikitext = "<poem>\nSois sage, ô ma Douleur, et tiens-toi plus tranquille.\nTu réclamais le Soir ; il descend ; le voici :\n</poem>"
        soup = BeautifulSoup(html, "lxml")
        wikicode = mwparserfromhell.parse(wikitext)
        page_data = {
            "pageid": 401202,
            "title": "Les Fleurs du mal (1868)/Recueillement",
            "fullurl": "https://fr.wikisource.org/wiki/Les_Fleurs_du_mal_(1868)/Recueillement",
            "revisions": [{"revid": 999, "content": wikitext}],
        }

        coll_context = Collection(
            page_id=150039,
            title="Les Fleurs du mal (1868)",
            url="https://fr.wikisource.org/wiki/Les_Fleurs_du_mal_(1868)",
            author="Charles Baudelaire",
            publication_date="1868",
            publisher="Michel Lévy frères",
        )

        poem = self.processor.process(
            page_data=page_data,
            soup=soup,
            lang="fr",
            wikicode=wikicode,
            collection_context=coll_context,
            order_in_collection=42,
            section_title_in_collection="SPLEEN ET IDÉAL",
            is_first_poem_in_collection=False,
        )

        self.assertEqual(poem.collection_page_id, 150039)
        self.assertEqual(poem.collection_title, "Les Fleurs du mal (1868)")
        self.assertEqual(poem.section_title, "SPLEEN ET IDÉAL")
        self.assertEqual(poem.poem_order, 42)
        # Inherited metadata
        self.assertEqual(poem.metadata.author, "Charles Baudelaire")
        self.assertEqual(poem.metadata.publication_date, "1868")
        self.assertEqual(poem.metadata.publisher, "Michel Lévy frères")
        self.assertEqual(poem.metadata.source_collection, "Les Fleurs du mal (1868)")


if __name__ == "__main__":
    unittest.main()
