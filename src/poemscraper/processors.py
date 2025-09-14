import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

from .schemas import PoemSchema, PoemMetadata
from .parsing import PoemParser
from .exceptions import PoemParsingError

logger = logging.getLogger(__name__)

class PoemProcessor:
    """
    Transforme les données brutes d'une page MediaWiki et le HTML rendu
    en un objet PoemSchema validé et nettoyé.
    """

    def process(
        self,
        page_data: dict,
        soup: BeautifulSoup,
        lang: str,
        wikicode: mwparserfromhell.wikicode.Wikicode,
        hub_info: Optional[dict] = None,
    ) -> PoemSchema:
        """Méthode de traitement principale pour une seule page."""
        wikitext = page_data["revisions"][0]["content"]

        structure = PoemParser.extract_poem_structure(soup)
        if not structure or not structure.stanzas:
            raise PoemParsingError(
                "Aucune structure de poème trouvée dans le HTML ou contenu vide."
            )

        html_meta = self._extract_html_metadata(soup)
        wikitext_meta = self._extract_wikitext_metadata(wikicode)
        
        final_meta_dict = {**wikitext_meta, **html_meta}

        if not final_meta_dict.get("author"):
            if "/" in page_data["title"]:
                parent_title = page_data["title"].split("/")[0].strip()
                if len(parent_title) < 50: 
                    final_meta_dict["source_collection"] = parent_title


        metadata_obj = PoemMetadata(**final_meta_dict)
        normalized_text = PoemParser.create_normalized_text(structure)

        if hub_info:
            hub_title = hub_info["title"]
            hub_page_id = hub_info["page_id"]
        else:
            hub_title = page_data["title"]
            hub_page_id = page_data["pageid"]

        poem_obj = PoemSchema(
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
            hub_title=hub_title,
            hub_page_id=hub_page_id,
        )
        return poem_obj

    def _extract_html_metadata(self, soup: BeautifulSoup) -> dict:
        """Extrait les métadonnées structurées (itemprop) du HTML rendu."""
        metadata = {}
        itemprop_map = {
            "author": "author",
            "datePublished": "publication_date",
            "isPartOf": "source_collection",
            "publisher": "publisher",
            "translator": "translator",
            "license": "license_name",
        }

        for prop, key in itemprop_map.items():
            element: Tag | None = soup.find(attrs={"itemprop": prop})
            if element:
                value = element.get_text(strip=True) or element.get(
                    "content", ""
                ).strip()
                if value:
                    metadata[key] = value
        return metadata

    def _extract_wikitext_metadata(
        self, parsed_wikicode: mwparserfromhell.wikicode.Wikicode
    ) -> dict:
        """Extrait des métadonnées de secours depuis les templates du wikitext."""
        metadata = {}
        for template in parsed_wikicode.filter_templates():
            name = template.name.strip().lower()

            if name in ["auteur", "author"] and template.has(1):
                metadata.setdefault("author", template.get(1).value.strip())

            if name == "titre":
                if template.has("auteur"):
                     metadata.setdefault("author", template.get("auteur").value.strip())
                if template.has("recueil"):
                     metadata.setdefault("source_collection", template.get("recueil").value.strip())

            if name == "infoédit":
                if template.has("AUTEUR"):
                    author_node = template.get("AUTEUR").value
                    if author_node.filter_wikilinks():
                        author_name = author_node.filter_wikilinks()[0].title.split(":")[-1]
                        metadata.setdefault("author", author_name)
                    else:
                         metadata.setdefault("author", author_node.strip())
                if template.has("ANNÉE"):
                    metadata.setdefault(
                        "publication_date", template.get("ANNÉE").value.strip()
                    )
                if template.has("RECUEIL"):
                    metadata.setdefault(
                        "source_collection", template.get("RECUEIL").value.strip()
                    )
        return metadata