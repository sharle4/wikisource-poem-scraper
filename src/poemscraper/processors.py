import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

from .schemas import PoemSchema, PoemMetadata, Collection
from .parsing import PoemParser
from .exceptions import PoemParsingError

logger = logging.getLogger(__name__)
collection_log = logging.getLogger('collection_processing')

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
        collection_context: Optional[Collection] = None,
        order_in_collection: Optional[int] = None,
        section_title_in_collection: Optional[str] = None,
        is_first_poem_in_collection: bool = False
    ) -> PoemSchema:
        """Méthode de traitement principale pour une seule page."""
        page_title = page_data.get("title", "N/A")
        page_id = page_data.get("pageid", -1)
        
        if collection_context:
            collection_log.debug(f"PoemProcessor received context for '{page_title}' (id:{page_id}): collection='{collection_context.title}' (id:{collection_context.page_id})")
        else:
            collection_log.debug(f"PoemProcessor received NO collection context for '{page_title}' (id:{page_id}). Will rely on metadata fallback.")

        wikitext = page_data["revisions"][0]["content"]

        structure = PoemParser.extract_poem_structure(soup)
        if not structure or not structure.stanzas:
            raise PoemParsingError(
                "Aucune structure de poème trouvée dans le HTML ou contenu vide."
            )

        html_meta = self._extract_html_metadata(soup)
        wikitext_meta = self._extract_wikitext_metadata(wikicode)
        
        final_meta_dict = {**wikitext_meta, **html_meta}

        if not final_meta_dict.get("source_collection"):
            if "/" in page_data["title"]:
                parent_title = page_data["title"].split("/")[0].strip()
                if len(parent_title) < 70: 
                    final_meta_dict["source_collection"] = parent_title

        metadata_obj = PoemMetadata(**final_meta_dict)
        normalized_text = PoemParser.create_normalized_text(structure)

        if hub_info:
            hub_title = hub_info.get("title")
            hub_page_id = hub_info.get("page_id")
        else:
            hub_title = None
            hub_page_id = page_data["pageid"]

        final_collection_page_id = collection_context.page_id if collection_context else None
        final_collection_title = collection_context.title if collection_context else metadata_obj.source_collection

        collection_log.info(f"FINALIZING poem '{page_title}' (id:{page_id}): collection_page_id={final_collection_page_id}, collection_title='{final_collection_title}'")

        poem_obj = PoemSchema(
            page_id=page_data["pageid"],
            revision_id=page_data["revisions"][0]["revid"],
            title=page_data["title"],
            language=lang,
            wikisource_url=page_data.get(
                "fullurl",
                f"https://{lang}.wikisource.org/?curid={page_data['pageid']}",
            ),
            collection_page_id=final_collection_page_id,
            collection_title=final_collection_title,
            section_title=section_title_in_collection,
            poem_order=order_in_collection,
            collection_structure=collection_context if is_first_poem_in_collection else None,
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
        }

        for prop, key in itemprop_map.items():
            element: Optional[Tag] = soup.find(attrs={"itemprop": prop})
            if not element:
                continue

            value = ""
            if prop == "isPartOf":
                link_tag = element.find("a")
                if link_tag:
                    name_span = link_tag.find("span", itemprop="name")
                    if name_span:
                        value = name_span.get_text(strip=True)
                    else:
                        value = link_tag.get_text(strip=True)
        
            if not value:
                value = element.get_text(strip=True) or element.get("content", "").strip()

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
                    wikilinks = author_node.filter_wikilinks()
                    if wikilinks:
                        author_name = wikilinks[0].title.split(":")[-1].strip()
                        metadata.setdefault("author", author_name)
                    else:
                         metadata.setdefault("author", author_node.strip_code().strip())
                if template.has("ANNÉE"):
                    metadata.setdefault(
                        "publication_date", template.get("ANNÉE").value.strip_code().strip()
                    )
                if template.has("RECUEIL"):
                    metadata.setdefault(
                        "source_collection", template.get("RECUEIL").value.strip_code().strip()
                    )
        return metadata