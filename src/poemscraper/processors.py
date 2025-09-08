import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

from .schemas import Poem, PoemMetadata, PoemStructure
from .parsing import WikitextParser
from .exceptions import PoemParsingError

logger = logging.getLogger(__name__)

class PoemProcessor:
    """
    Transforme les données brutes d'une page MediaWiki et le HTML rendu
    en un objet Poem validé et nettoyé.
    """

    def process(
        self,
        page_data: dict,
        page_html: str,
        lang: str,
        wikicode: mwparserfromhell.wikicode.Wikicode,
    ) -> Poem:
        """Méthode de traitement principale pour une seule page."""
        wikitext = page_data["revisions"][0]["content"]

        structure = WikitextParser.extract_poem_structure(wikitext)
        if not structure or not structure.stanzas:
            raise PoemParsingError("No poem structure found or content is empty.")

        html_metadata = self._extract_html_metadata(page_html)
        wikitext_metadata = self._extract_wikitext_metadata(wikicode)

        final_meta_dict = wikitext_metadata
        final_meta_dict.update(html_metadata)

        metadata_obj = PoemMetadata(**final_meta_dict)

        normalized_text = WikitextParser.create_normalized_text(structure)

        poem_obj = Poem(
            page_id=page_data["pageid"],
            revision_id=page_data["revisions"][0]["revid"],
            title=page_data["title"],
            language=lang,
            wikisource_url=page_data.get(
                "fullurl",
                f"https://{lang}.wikisource.org/?curid={page_data['pageid']}",
            ),
            metadata=metadata_obj,
            raw_wikitext=wikitext,
            structure=structure,
            normalized_text=normalized_text,
            checksum_sha256=hashlib.sha256(wikitext.encode("utf-8")).hexdigest(),
            extraction_timestamp=datetime.now(timezone.utc),
        )
        return poem_obj

    def _extract_html_metadata(self, html: str) -> dict:
        """Extrait les métadonnées structurées (itemprop) du HTML rendu."""
        if not html:
            return {}

        soup = BeautifulSoup(html, "lxml")
        metadata = {}

        def get_itemprop(prop: str) -> Optional[str]:
            element: Optional[Tag] = soup.find(attrs={"itemprop": prop})
            if not element:
                return None
            return element.get_text(strip=True) or element.get("content", "").strip()

        itemprop_map = {
            "author": "author",
            "datePublished": "publication_date",
            "isPartOf": "source_collection",
            "publisher": "publisher",
            "translator": "translator",
            "license": "license_name",
        }

        for prop, key in itemprop_map.items():
            value = get_itemprop(prop)
            if value:
                metadata[key] = value

        return metadata

    def _extract_wikitext_metadata(
        self, parsed_wikicode: mwparserfromhell.wikicode.Wikicode
    ) -> dict:
        """
        Extrait des métadonnées de secours depuis les templates du wikitext.
        C'est utile quand les `itemprop` ne sont pas présents.
        """
        metadata = {}

        for template in parsed_wikicode.filter_templates():
            name = template.name.strip().lower()

            if name in ["auteur", "author"] and template.has(1):
                metadata.setdefault("author", template.get(1).value.strip())

            if name == "titre" and template.has("auteur"):
                metadata.setdefault("author", template.get("auteur").value.strip())

            if name == "infoédit":
                if template.has("AUTEUR"):
                    author_node = template.get("AUTEUR").value
                    metadata.setdefault(
                        "author", author_node.filter_wikilinks()[0].title.split(":")[-1]
                    )
                if template.has("ANNÉE"):
                    metadata.setdefault(
                        "publication_date", template.get("ANNÉE").value.strip()
                    )
                if template.has("RECUEIL"):
                    metadata.setdefault(
                        "source_collection", template.get("RECUEIL").value.strip()
                    )

        return metadata