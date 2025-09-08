import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import mwparserfromhell
from bs4 import BeautifulSoup, Tag

from .schemas import PoemSchema
from .parsing import WikitextParser
from .exceptions import PoemParsingError

logger = logging.getLogger(__name__)

class PoemProcessor:
    """
    Transforms raw MediaWiki page data and rendered HTML into a clean,
    validated PoemSchema object using a hybrid approach.
    """
    def process(self, page_data: dict, page_html: str, lang: str) -> PoemSchema:
        """
        Main processing method for a single page.
        """
        if page_data.get('ns') != 0:
            raise PoemParsingError(f"Page is not in main namespace (ns={page_data.get('ns')}).")

        wikitext = page_data['revisions'][0]['content']
        
        structure = WikitextParser.extract_poem_structure(wikitext)
        if not structure:
            raise PoemParsingError("No <poem> tags found or content is empty.")
            
        html_metadata = self._extract_html_metadata(page_html)
        
        parsed_wikicode = mwparserfromhell.parse(wikitext)
        wikitext_metadata = self._extract_wikitext_metadata(parsed_wikicode, lang)
        
        final_metadata = wikitext_metadata
        final_metadata.update(html_metadata)
        
        author = final_metadata.get("author")
        
        normalized_text = WikitextParser.create_normalized_text(structure)

        poem_obj = PoemSchema(
            page_id=page_data['pageid'],
            revision_id=page_data['revisions'][0]['revid'],
            title=page_data['title'],
            author=author,
            author_url=f"https://{lang}.wikisource.org/wiki/Auteur:{author.replace(' ', '_')}" if author else None,
            language=lang,
            wikisource_url=page_data.get('fullurl', f"https://{lang}.wikisource.org/?curid={page_data['pageid']}"),
            license={"name": final_metadata.get("license", "Not detected"), "url": None},
            metadata=final_metadata,
            raw_wikitext=wikitext,
            structure=structure,
            normalized_text=normalized_text,
            checksum_sha256=hashlib.sha256(wikitext.encode('utf-8')).hexdigest(),
            extraction_timestamp=datetime.now(timezone.utc),
            provenance="api"
        )
        return poem_obj

    def _extract_html_metadata(self, html: str) -> dict:
        """
        Extracts structured metadata (itemprop) from rendered HTML using BeautifulSoup.
        """
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
            "isPartOf": "publication_source",
            "publisher": "publisher",
            "translator": "translator",
            "license": "license",
        }
        
        for prop, key in itemprop_map.items():
            value = get_itemprop(prop)
            if value:
                metadata[key] = value

        return metadata

    def _extract_wikitext_metadata(self, parsed_wikicode: mwparserfromhell.wikicode.Wikicode, lang: str) -> dict:
        """
        Extracts fallback metadata (categories, templates) from wikitext.
        """
        metadata = {"categories": [], "templates": {}}
        
        category_namespaces = {"fr": "Catégorie", "en": "Category"}
        cat_prefix = category_namespaces.get(lang, "Category") + ":"

        for link in parsed_wikicode.filter_wikilinks():
            if link.title.startswith(cat_prefix):
                metadata["categories"].append(str(link.title).split(":", 1)[1])

        for template in parsed_wikicode.filter_templates():
            template_name = str(template.name).strip()
            params = {str(p.name).strip(): str(p.value).strip() for p in template.params}
            metadata["templates"][template_name] = params
            
            if template_name.lower() in ["auteur", "author"] and '1' in params:
                metadata.setdefault('author', params['1'])

        return metadata