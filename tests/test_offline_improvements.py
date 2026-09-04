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

    def test_admin_subpages_excluded_from_collection(self):
        admin_titles = [
            "Les Fleurs du mal/Éditions",
            "Les Fleurs du mal/editions",
            "Les Fleurs du mal/Texte entier",
            "Les Fleurs du mal/texte entier",
            "Les Fleurs du mal/Concordance",
            "Les Fleurs du mal/Table",
            "Les Fleurs du mal/Sommaire",
        ]
        html = '<div class="mw-parser-output"><div class="tableItem"><a href="./Poeme">Poème</a></div></div>'
        soup = BeautifulSoup(html, "lxml")
        for title in admin_titles:
            page_data = {"pageid": 999, "title": title, "ns": 0, "categories": []}
            classifier = PageClassifier(page_data, soup, "fr", mwparserfromhell.parse(""))
            page_type, reason = classifier.classify()
            self.assertEqual(page_type, PageType.OTHER, f"Expected {title} to be classified as OTHER, got {page_type}")
            self.assertEqual(reason, "is_admin_subpage")

    def test_hub_extract_sub_pages_filters_admin(self):
        html = """
        <div class="mw-parser-output">
            <ul>
                <li><a href="./Les_Fleurs_du_mal_(1857)">Les Fleurs du mal (1857)</a></li>
                <li><a href="./Les_Fleurs_du_mal_(1861)">Les Fleurs du mal (1861)</a></li>
                <li><a href="./Les_Fleurs_du_mal/%C3%89ditions">Les Fleurs du mal/Éditions</a></li>
                <li><a href="./Les_Fleurs_du_mal/Texte_entier">Les Fleurs du mal/Texte entier</a></li>
            </ul>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        page_data = {"pageid": 500, "title": "Les Fleurs du mal", "ns": 0, "categories": []}
        classifier = PageClassifier(page_data, soup, "fr", mwparserfromhell.parse(""))
        sub_pages = classifier.extract_hub_sub_pages()
        self.assertIn("Les Fleurs du mal (1857)", sub_pages)
        self.assertIn("Les Fleurs du mal (1861)", sub_pages)
        self.assertNotIn("Les Fleurs du mal/Éditions", sub_pages)
        self.assertNotIn("Les Fleurs du mal/Texte entier", sub_pages)

    def test_styled_div_and_centered_section_detection(self):
        html = """
        <div class="mw-parser-output">
            <div class="headertemplate ws-noexport"><a href="./Les_Fleurs_du_mal">Hub Parent</a></div>
            <div style="margin: 1em auto;text-align: center;font-weight: bold;">SPLEEN ET IDÉAL</div>
            <div class="tableItem"><a href="./Recueil/Poeme_1">Poème 1</a></div>
            <div class="tableItem"><a href="./Page:Scan_djvu/12">Page:Scan djvu/12</a></div>
            <div style="text-align: center;">TABLEAUX PARISIENS</div>
            <div class="tableItem"><a href="./Recueil/Poeme_2">Poème 2</a></div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        page_data = {"pageid": 100, "title": "Recueil", "ns": 0, "categories": []}
        classifier = PageClassifier(page_data, soup, "fr", mwparserfromhell.parse(""))
        links = classifier.extract_ordered_collection_links()

        sections = [t for t, k in links if k == PageType.SECTION_TITLE]
        poems = [t for t, k in links if k == PageType.POEM]

        self.assertEqual(sections, ["SPLEEN ET IDÉAL", "TABLEAUX PARISIENS"])
        self.assertEqual(poems, ["Recueil/Poeme 1", "Recueil/Poeme 2"])
        # ProofreadPage and header links should not be included
        self.assertNotIn("Page:Scan djvu/12", poems)
        self.assertNotIn("Les Fleurs du mal", poems)


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

    def test_end_to_end_sonnet_and_collection_section_enrichment(self):
        # 1. Collection with styled div section titles
        coll_html = """
        <div class="mw-parser-output">
            <div class="headertemplate ws-noexport">
                <a href="./Auteur:Charles_Baudelaire">Charles Baudelaire</a>
            </div>
            <div class="tableItem"><a href="./Les_Fleurs_du_mal_(1861)/Au_lecteur">Au lecteur</a></div>
            <div style="margin: 1em auto;text-align: center;font-weight: bold;">SPLEEN ET IDÉAL</div>
            <div class="tableItem"><a href="./Les_Fleurs_du_mal_(1861)/B%C3%A9n%C3%A9diction">Bénédiction</a></div>
            <div style="text-align: center;font-weight: bold;">TABLEAUX PARISIENS</div>
            <div class="tableItem"><a href="./Les_Fleurs_du_mal_(1861)/%C3%80_une_passante">À une passante</a></div>
        </div>
        """
        coll_soup = BeautifulSoup(coll_html, "lxml")
        classifier = PageClassifier({"pageid": 1642, "title": "Les Fleurs du mal (1861)", "ns": 0}, coll_soup, "fr", mwparserfromhell.parse(""))
        ordered_items = classifier.extract_ordered_collection_links()

        sections_map = {}
        curr_sec = None
        for item_title, kind in ordered_items:
            if kind == PageType.SECTION_TITLE:
                curr_sec = item_title
            elif kind == PageType.POEM:
                sections_map[item_title] = curr_sec

        self.assertIsNone(sections_map.get("Les Fleurs du mal (1861)/Au lecteur"))
        self.assertEqual(sections_map.get("Les Fleurs du mal (1861)/Bénédiction"), "SPLEEN ET IDÉAL")
        self.assertEqual(sections_map.get("Les Fleurs du mal (1861)/À une passante"), "TABLEAUX PARISIENS")

        # 2. Process sonnet À une passante with Parsoid HTML markup
        sonnet_html = """
        <div class="mw-parser-output">
            <div class="poem verse">
                <p><br/>
                La rue assourdissante autour de moi hurlait.<br/>
                Longue, mince, en grand deuil, douleur majestueuse,<br/>
                Une femme passa, d’une main fastueuse<br/>
                Soulevant, balançant le feston et l’ourlet ;<br/>
                <br/>
                Agile et noble, avec sa jambe de statue.<br/>
                Moi, je buvais, crispé comme un extravagant,<br/>
                Dans son œil, ciel livide où germe l’ouragan,<br/>
                La douceur qui fascine et le plaisir qui tue.
                </p>
            </div>
            <div class="poem verse">
                <p><br/>
                Un éclair… puis la nuit ! — Fugitive beauté<br/>
                Dont le regard m’a fait soudainement renaître,<br/>
                Ne te verrai-je plus que dans l’éternité ?<br/>
                <br/>
                Ailleurs, bien loin d’ici ! trop tard ! jamais peut-être !<br/>
                Car j’ignore où tu fuis, tu ne sais où je vais,<br/>
                Ô toi que j’eusse aimée, ô toi qui le savais !
                </p>
            </div>
        </div>
        """
        sonnet_soup = BeautifulSoup(sonnet_html, "lxml")
        coll_context = Collection(
            page_id=1642,
            title="Les Fleurs du mal (1861)",
            url="https://fr.wikisource.org/wiki/Les_Fleurs_du_mal_(1861)",
            author="Charles Baudelaire",
            publication_date="1861",
            publisher="Poulet-Malassis et de Broise",
        )

        poem_data = {
            "pageid": 11774,
            "title": "Les Fleurs du mal (1861)/À une passante",
            "fullurl": "https://fr.wikisource.org/wiki/Les_Fleurs_du_mal_(1861)/%C3%80_une_passante",
            "revisions": [{"revid": 15069894, "content": ""}],
        }

        poem = self.processor.process(
            page_data=poem_data,
            soup=sonnet_soup,
            lang="fr",
            wikicode=mwparserfromhell.parse(""),
            collection_context=coll_context,
            order_in_collection=92,
            section_title_in_collection=sections_map.get("Les Fleurs du mal (1861)/À une passante"),
            is_first_poem_in_collection=False,
        )

        self.assertEqual(poem.collection_page_id, 1642)
        self.assertEqual(poem.collection_title, "Les Fleurs du mal (1861)")
        self.assertEqual(poem.section_title, "TABLEAUX PARISIENS")
        self.assertEqual(poem.poem_order, 92)
        self.assertEqual(poem.metadata.author, "Charles Baudelaire")
        self.assertEqual(poem.metadata.source_collection, "Les Fleurs du mal (1861)")
        self.assertEqual(poem.metadata.publication_date, "1861")
        self.assertEqual(poem.metadata.publisher, "Poulet-Malassis et de Broise")

        # 4 stanzas of the sonnet
        self.assertEqual(len(poem.structure.stanzas), 4)
        self.assertEqual([len(s) for s in poem.structure.stanzas], [4, 4, 3, 3])


if __name__ == "__main__":
    unittest.main()
