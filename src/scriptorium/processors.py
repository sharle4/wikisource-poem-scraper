import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

from .schemas import PoemSchema, PoemMetadata, Collection
from .parsing import PoemParser
from .exceptions import PoemParsingError
from .author_cleaner import clean_author_name

logger = logging.getLogger(__name__)
collection_log = logging.getLogger('collection_processing')

KNOWN_CITIES = {
    "paris", "lyon", "bruxelles", "genève", "geneve", "londres",
    "bordeaux", "marseille", "strasbourg", "rouen", "toulouse", "lille"
}


def parse_imprint(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts publication date and publisher from an imprint text string
    such as 'Michel Lévy frères, Paris, 1868' or 'Paris, Alphonse Lemerre, 1893'.
    """
    if not text:
        return None, None

    # 1. Extract year
    year = None
    year_matches = list(re.finditer(r"\b(1[5-9]\d{2}|20\d{2})\b", text))
    if year_matches:
        year = year_matches[-1].group(1)

    # 2. Extract publisher
    publisher = None
    cleaned_text = re.sub(r"\[.*?\]|\(.*?\)", "", text).strip()
    parts = [p.strip() for p in re.split(r"[,;\n]|\s+[—–-]\s+", cleaned_text) if p.strip()]

    candidate_publishers = []
    for part in parts:
        part_clean = re.sub(r"^\s*éditions?\s+(?:de\s+|d['’])?", "", part, flags=re.I).strip()
        part_lower = part_clean.lower()

        if re.fullmatch(r"\d+", part_clean):
            continue
        if part_lower in KNOWN_CITIES:
            continue
        if part_lower in {"in-8", "in-12", "in-16", "in-4", "tome", "vol", "volume", "éd", "édition", "nouvelle édition", "sans date", "s. d."}:
            continue
        if len(part_clean) >= 3 and (part_clean[0].isupper() or part_clean.startswith("éd") or part_clean.startswith("Éd")):
            candidate_publishers.append(part_clean)

    if candidate_publishers:
        for cand in candidate_publishers:
            if re.search(r"\b(frères|éditeurs?|librairie|imprimerie|garnier|lemerre|fasquelle|hachette|gallimard|mercure|plon|flammarion|albin)\b", cand, re.I):
                publisher = cand
                break
        if not publisher:
            publisher = candidate_publishers[0]

    return year, publisher


def extract_page_metadata(soup: BeautifulSoup, wikicode: mwparserfromhell.wikicode.Wikicode, title: str = "") -> Dict[str, Any]:
    """
    Unified metadata extraction from rendered HTML and wikitext.
    Used for both individual poems and collections.
    """
    processor = PoemProcessor()
    html_meta = processor._extract_html_metadata(soup)
    wikitext_meta = processor._extract_wikitext_metadata(wikicode)
    meta = {**wikitext_meta, **html_meta}

    if not meta.get("source_collection") and "/" in title:
        parent_title = title.split("/")[0].strip()
        if len(parent_title) < 70:
            meta["source_collection"] = parent_title

    if not meta.get("publication_date"):
        # Check title or source_collection for year in parentheses: e.g. "Les Fleurs du mal (1868)"
        for candidate_t in [title, meta.get("source_collection", "")]:
            m = re.search(r"\((1[5-9]\d{2}|20\d{2})\)", candidate_t)
            if m:
                meta["publication_date"] = m.group(1)
                break

    return meta


class PoemProcessor:
    """
    Transforms raw MediaWiki page data and rendered HTML
    into a validated and cleaned PoemSchema object.
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
        """Main processing method for a single page."""
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
                "No poem structure found in the HTML or content is empty."
            )

        final_meta_dict = extract_page_metadata(soup, wikicode, page_title)

        # Contextual inheritance from parent collection
        if collection_context:
            if collection_context.title:
                final_meta_dict["source_collection"] = collection_context.title
            if not final_meta_dict.get("author") and collection_context.author:
                final_meta_dict["author"] = collection_context.author
            if not final_meta_dict.get("publication_date") and collection_context.publication_date:
                final_meta_dict["publication_date"] = collection_context.publication_date
            if not final_meta_dict.get("publisher") and collection_context.publisher:
                final_meta_dict["publisher"] = collection_context.publisher

        # Fallback date extraction from source_collection title if still missing
        if not final_meta_dict.get("publication_date") and final_meta_dict.get("source_collection"):
            m = re.search(r"\((1[5-9]\d{2}|20\d{2})\)", final_meta_dict["source_collection"])
            if m:
                final_meta_dict["publication_date"] = m.group(1)

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
        """Extracts structured metadata from rendered HTML (microdata + header classes)."""
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
                metadata[key] = clean_author_name(value) if key == "author" else value

        # Header template classes (Parsoid and Wikisource header template conventions)
        if "author" not in metadata:
            author_el = soup.find(class_=re.compile(r"\b(headertemplate-author|ws-author|header_author)\b", re.I))
            if author_el:
                link = author_el.find("a")
                raw_author = link.get_text(strip=True) if link else author_el.get_text(strip=True)
                ca = clean_author_name(raw_author)
                if ca:
                    metadata["author"] = ca

        if "source_collection" not in metadata:
            title_el = soup.find(class_=re.compile(r"\b(headertemplate-title|ws-title|header_title)\b", re.I))
            if title_el:
                link = title_el.find("a")
                raw_title = link.get_text(strip=True) if link else title_el.get_text(strip=True)
                if raw_title and not raw_title.startswith("Catégorie:"):
                    metadata["source_collection"] = raw_title

        if "translator" not in metadata:
            trans_el = soup.find(class_=re.compile(r"\b(headertemplate-translator|ws-translator)\b", re.I))
            if trans_el:
                metadata["translator"] = trans_el.get_text(strip=True)

        # Imprint information (date and publisher) from reference/edition classes
        if "publication_date" not in metadata or "publisher" not in metadata:
            ref_el = soup.find(class_=re.compile(r"\b(headertemplate-reference|headertemplate-edition|ws-reference|ws-edition)\b", re.I))
            ref_text = ""
            if ref_el:
                small_tag = ref_el.find("small")
                ref_text = small_tag.get_text(" ", strip=True) if small_tag else ref_el.get_text(" ", strip=True)
            else:
                header_el = soup.find(class_=re.compile(r"\bheadertemplate\b", re.I))
                if header_el:
                    small_tag = header_el.find("small")
                    if small_tag:
                        ref_text = small_tag.get_text(" ", strip=True)

            if ref_text:
                year, pub = parse_imprint(ref_text)
                if year and "publication_date" not in metadata:
                    metadata["publication_date"] = year
                if pub and "publisher" not in metadata:
                    metadata["publisher"] = pub

        return metadata

    def _extract_wikitext_metadata(
        self, parsed_wikicode: mwparserfromhell.wikicode.Wikicode
    ) -> dict:
        """Extracts metadata from wikitext templates."""
        metadata = {}
        for template in parsed_wikicode.filter_templates():
            name = template.name.strip().lower()

            if name in ["auteur", "author"]:
                if template.has("nom"):
                    metadata.setdefault("author", clean_author_name(template.get("nom").value.strip_code().strip()))
                elif template.has(1):
                    metadata.setdefault("author", clean_author_name(template.get(1).value.strip_code().strip()))

            elif name == "titre":
                if template.has("auteur"):
                    a_val = template.get("auteur").value
                    wlinks = a_val.filter_wikilinks()
                    author_name = wlinks[0].title.split(":")[-1].strip() if wlinks else a_val.strip_code().strip()
                    metadata.setdefault("author", clean_author_name(author_name))
                elif template.has(2):
                    a_val = template.get(2).value
                    wlinks = a_val.filter_wikilinks()
                    author_name = wlinks[0].title.split(":")[-1].strip() if wlinks else a_val.strip_code().strip()
                    if author_name:
                        metadata.setdefault("author", clean_author_name(author_name))

                if template.has("recueil"):
                    metadata.setdefault("source_collection", template.get("recueil").value.strip_code().strip())
                elif template.has(1):
                    t_val = template.get(1).value
                    wlinks = t_val.filter_wikilinks()
                    t_name = wlinks[0].title.split(":")[-1].strip() if wlinks else t_val.strip_code().strip()
                    if t_name and len(t_name) < 100:
                        metadata.setdefault("source_collection", t_name)

                if template.has("année"):
                    metadata.setdefault("publication_date", template.get("année").value.strip_code().strip())
                elif template.has("date"):
                    metadata.setdefault("publication_date", template.get("date").value.strip_code().strip())

                if template.has("éditeur"):
                    metadata.setdefault("publisher", template.get("éditeur").value.strip_code().strip())

                if template.has(3) and ("publication_date" not in metadata or "publisher" not in metadata):
                    notes = template.get(3).value.strip_code().strip()
                    y, p = parse_imprint(notes)
                    if y and "publication_date" not in metadata:
                        metadata.setdefault("publication_date", y)
                    if p and "publisher" not in metadata:
                        metadata.setdefault("publisher", p)

            elif name == "infoédit":
                if template.has("AUTEUR"):
                    author_node = template.get("AUTEUR").value
                    wikilinks = author_node.filter_wikilinks()
                    if wikilinks:
                        author_name = wikilinks[0].title.split(":")[-1].strip()
                    else:
                        author_name = author_node.strip_code().strip()
                    metadata.setdefault("author", clean_author_name(author_name))
                if template.has("ANNÉE"):
                    metadata.setdefault(
                        "publication_date", template.get("ANNÉE").value.strip_code().strip()
                    )
                if template.has("RECUEIL"):
                    metadata.setdefault(
                        "source_collection", template.get("RECUEIL").value.strip_code().strip()
                    )
                if template.has("ÉDITEUR"):
                    metadata.setdefault(
                        "publisher", template.get("ÉDITEUR").value.strip_code().strip()
                    )

            elif name == "header":
                if template.has("author"):
                    metadata.setdefault("author", clean_author_name(template.get("author").value.strip_code().strip()))
                if template.has("title"):
                    metadata.setdefault("source_collection", template.get("title").value.strip_code().strip())
                if template.has("year"):
                    metadata.setdefault("publication_date", template.get("year").value.strip_code().strip())
                if template.has("publisher"):
                    metadata.setdefault("publisher", template.get("publisher").value.strip_code().strip())
                if template.has("translator"):
                    metadata.setdefault("translator", template.get("translator").value.strip_code().strip())

            elif name in ["édition", "edition"]:
                if template.has("éditeur"):
                    metadata.setdefault("publisher", template.get("éditeur").value.strip_code().strip())
                if template.has("année"):
                    metadata.setdefault("publication_date", template.get("année").value.strip_code().strip())
                elif template.has("date"):
                    metadata.setdefault("publication_date", template.get("date").value.strip_code().strip())

        return metadata
